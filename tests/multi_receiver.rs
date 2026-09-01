//! Multi-receiver protocol test.
//!
//! Real udpcast receivers on one host cannot both register with the sender
//! (participants are keyed by IP:port — same limitation as the C version),
//! so this test drives `spawn_net_sender` directly with two simulated
//! participants on distinct loopback addresses (127.0.0.2 / 127.0.0.3) and
//! verifies the multi-receiver contract:
//!
//!  * a slice is only freed once ALL participants have acked it
//!  * the pipeline (max 3 in flight) stalls while one receiver hasn't acked
//!  * a RETRANSMIT from one receiver triggers retransmission of only the
//!    missing blocks, and the other receiver's ack state is unaffected
//!  * data payload arrives intact for every block of every slice

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use udpcast::fifo::Fifo;
use udpcast::participants::ParticipantsDb;
use udpcast::protocol::{self, OkMsg, Reqack, Retransmit};
use udpcast::senddata::{self, Discovery, NetConfig, FLAG_SN};
use udpcast::statistics::SenderStats;

const BLOCK: usize = 512;
/// Blocks per slice. Must stay >= 32: the retransmit path clamps the sender's
/// slice size to a 32-block minimum (`senddata::send_reqack`, mirroring C
/// `senddata.c:458`), so a smaller slice size would make the sender grow its
/// slices mid-transfer instead of keeping the fixed layout asserted below.
const SLICE_BLOCKS: u32 = 32;
const SLICE_BYTES: i32 = (SLICE_BLOCKS as usize * BLOCK) as i32;
const NR_DATA_SLICES: i32 = 5;
const PORT_BASE: u16 = 9199;

fn source_byte(i: usize) -> u8 {
    ((i * 7 + 13) % 251) as u8
}

#[test]
fn two_receivers_gate_acks_and_retransmits() {
    let total = NR_DATA_SLICES as usize * SLICE_BYTES as usize;

    let fifo = Arc::new(Fifo::new(BLOCK));
    for i in 0..total {
        fifo.buffer_mut()[i] = source_byte(i);
    }
    // Mirrors local_reader_fifo: reserve free memory before publishing data.
    fifo.free_mem_queue.consume(total);
    fifo.free_mem_queue.consumed(total);
    fifo.data.produce(total);
    fifo.data.produce_end();

    let dest = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), PORT_BASE);
    let addr_a = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 2), PORT_BASE);
    let addr_b = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 3), PORT_BASE);

    let sender_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").unwrap());
    let sender_addr = sender_sock.local_addr().unwrap();
    let observer = UdpSocket::bind(dest).unwrap();
    observer.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
    let part_a = UdpSocket::bind(addr_a).unwrap();
    let part_b = UdpSocket::bind(addr_b).unwrap();
    part_a.set_read_timeout(Some(Duration::from_millis(10))).unwrap();
    part_b.set_read_timeout(Some(Duration::from_millis(10))).unwrap();

    let caps = protocol::CAP_BIG_ENDIAN | protocol::CAP_NEW_GEN;
    let db = Arc::new(Mutex::new(ParticipantsDb::new()));
    db.lock().unwrap().add_participant(&addr_a, caps, 0, false);
    db.lock().unwrap().add_participant(&addr_b, caps, 0, false);
    assert_eq!(db.lock().unwrap().nr_participants(), 2);

    let cfg: &'static mut NetConfig = Box::leak(Box::new(NetConfig {
        net_if: None,
        port_base: PORT_BASE,
        block_size: BLOCK as u32,
        slice_size: SLICE_BLOCKS,
        control_mcast_addr: dest,
        data_mcast_addr: dest,
        mcast_rdv: None,
        ttl: 0,
        flags: FLAG_SN,
        capabilities: caps,
        min_slice_size: 32,
        default_slice_size: SLICE_BLOCKS,
        max_slice_size: protocol::MAX_SLICE_SIZE as u32,
        rcvbuf: 0,
        rexmit_hello_interval: 0,
        autostart: 0,
        requested_buf_size: 0,
        min_receivers: 2,
        max_receivers_wait: 0,
        min_receivers_wait: 0,
        retries_until_drop: 1_000_000,
        rehello_offset: 0,
        start_timeout: 0,
        discovery: Discovery::Doubling,
        fec_stripes: 0,
        fec_redundancy: 0,
        fec_stripesize: 0,
        max_bitrate: None,
        autorate: false,
    }));
    let stats: &'static mut SenderStats =
        Box::leak(Box::new(SenderStats::new(0, 100, 100, false, true)));

    let fifo_c = fifo.clone();
    let sock_c = sender_sock.clone();
    let db_c = db.clone();
    let sender_thread = std::thread::spawn(move || {
        let buf: &[u8] = fifo_c.buffer();
        senddata::spawn_net_sender(
            &fifo_c.data,
            &fifo_c.free_mem_queue,
            buf,
            fifo_c.data_buf_size,
            &sock_c,
            &[],
            cfg,
            db_c,
            stats,
        );
    });

    let send_ok = |sock: &UdpSocket, slice_no: i32| {
        let _ = sock.send_to(&OkMsg { slice_no }.pack(), &sender_addr);
    };
    // Map of RECEIVED blocks (bit cleared = missing), only block 2 missing.
    let mut missing2_map = [0xffu8; 128];
    missing2_map[0] &= !(1 << 2);
    let send_rexmit = |rxmit: i32| {
        let m = Retransmit {
            slice_no: 1,
            rxmit,
            map: missing2_map,
        };
        let _ = part_b.send_to(&m.pack(), &sender_addr);
    };

    let mut buf = [0u8; 2048];
    // (slice,block) -> (count, first payload)
    let mut data_seen: HashMap<(i32, u16), (usize, Vec<u8>)> = HashMap::new();
    let mut reqack_seen: HashMap<i32, i32> = HashMap::new(); // slice -> bytes
    let mut reqack_log: Vec<(i32, i32, i32)> = Vec::new(); // (slice, bytes, rxmit)
    let mut initial_reqacks: Vec<i32> = Vec::new();

    let recv_reqack = |buf: &[u8]| Reqack::unpack(buf);

    // Phase A: wait until REQACKs for slices 0, 1, 2 have all arrived,
    // recording everything sent meanwhile.
    let deadline = Instant::now() + Duration::from_secs(10);
    while initial_reqacks.len() < 3 {
        assert!(Instant::now() < deadline, "timed out waiting for initial REQACKs");
        if let Ok((n, _)) = observer.recv_from(&mut buf) {
            let op = u16::from_be_bytes([buf[0], buf[1]]);
            if op == protocol::CMD_DATA && n >= 16 {
                let d = protocol::DataBlock::unpack(&buf);
                let e = data_seen.entry((d.slice_no, d.block_no)).or_insert((0, buf[16..n].to_vec()));
                e.0 += 1;
            } else if op == protocol::CMD_REQACK && n >= protocol::REQACK_SIZE {
                let r = recv_reqack(&buf);
                reqack_seen.insert(r.slice_no, r.bytes);
                reqack_log.push((r.slice_no, r.bytes, r.rxmit));
                if !initial_reqacks.contains(&r.slice_no) && r.rxmit == 0 {
                    initial_reqacks.push(r.slice_no);
                }
            }
        }
    }
    assert_eq!(initial_reqacks, vec![0, 1, 2]);

    // Receiver A acks everything; receiver B only retransmits slice 1 and
    // withholds its acks -> sender must NOT advance past 3 in-flight slices.
    send_ok(&part_a, 0);
    send_ok(&part_a, 1);
    send_ok(&part_a, 2);
    send_rexmit(0);

    // Phase B: for 500 ms, keep answering repeated REQACKs the same way.
    let end_b = Instant::now() + Duration::from_millis(500);
    while Instant::now() < end_b {
        if let Ok((n, _)) = observer.recv_from(&mut buf) {
            let op = u16::from_be_bytes([buf[0], buf[1]]);
            if op == protocol::CMD_DATA && n >= 16 {
                let d = protocol::DataBlock::unpack(&buf);
                let e = data_seen.entry((d.slice_no, d.block_no)).or_insert((0, buf[16..n].to_vec()));
                e.0 += 1;
            } else if op == protocol::CMD_REQACK && n >= protocol::REQACK_SIZE {
                let r = recv_reqack(&buf);
                reqack_seen.insert(r.slice_no, r.bytes);
                reqack_log.push((r.slice_no, r.bytes, r.rxmit));
                assert!(r.slice_no < 3, "pipeline advanced without acks from receiver B");
                send_ok(&part_a, r.slice_no);
                if r.slice_no == 1 {
                    send_rexmit(r.rxmit);
                }
            }
        }
    }
    let rexmit_count = data_seen.get(&(1, 2)).map(|e| e.0).unwrap_or(0);
    assert!(
        rexmit_count >= 2,
        "block 2 of slice 1 was not retransmitted (seen {} times)",
        rexmit_count
    );
    for (&s, _) in &reqack_seen {
        assert!(s < 3, "saw REQACK for slice {} before receiver B acked", s);
    }

    // Phase C: receiver B acks everything; transfer must now complete.
    send_ok(&part_b, 0);
    send_ok(&part_b, 1);
    send_ok(&part_b, 2);

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if sender_thread.is_finished() {
            break;
        }
        assert!(Instant::now() < deadline, "sender did not finish after all acks");
        if let Ok((n, _)) = observer.recv_from(&mut buf) {
            let op = u16::from_be_bytes([buf[0], buf[1]]);
            if op == protocol::CMD_DATA && n >= 16 {
                let d = protocol::DataBlock::unpack(&buf);
                let e = data_seen.entry((d.slice_no, d.block_no)).or_insert((0, buf[16..n].to_vec()));
                e.0 += 1;
            } else if op == protocol::CMD_REQACK && n >= protocol::REQACK_SIZE {
                let r = recv_reqack(&buf);
                reqack_seen.insert(r.slice_no, r.bytes);
                reqack_log.push((r.slice_no, r.bytes, r.rxmit));
                send_ok(&part_a, r.slice_no);
                send_ok(&part_b, r.slice_no);
            }
        }
    }
    sender_thread.join().unwrap();

    // REQACKs for all data slices plus the final 0-byte slice.
    for s in 0..=NR_DATA_SLICES {
        let bytes = reqack_seen.get(&s).unwrap_or_else(|| panic!("no REQACK for slice {}", s));
        let expect = if s == NR_DATA_SLICES { 0 } else { SLICE_BYTES };
        assert_eq!(*bytes, expect, "REQACK bytes mismatch for slice {} (log {:?})", s, reqack_log);
    }

    // Every data block arrived with intact payload; block 2 of slice 1 twice.
    for s in 0..NR_DATA_SLICES {
        for b in 0..SLICE_BLOCKS as u16 {
            let (count, payload) = data_seen
                .get(&(s, b))
                .unwrap_or_else(|| panic!("missing DATA slice {} block {}", s, b));
            let offset = (s as usize) * SLICE_BYTES as usize + (b as usize) * BLOCK;
            let expected: Vec<u8> = (offset..offset + BLOCK).map(source_byte).collect();
            assert_eq!(*payload, expected, "payload mismatch slice {} block {}", s, b);
            if (s, b) == (1, 2) {
                assert!(*count >= 2, "retransmitted block sent only {} time(s)", count);
            } else {
                assert_eq!(*count, 1, "slice {} block {} sent {} times", s, b, count);
            }
        }
    }
}
