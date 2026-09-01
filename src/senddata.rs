use std::net::{SocketAddrV4, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use crate::participants::ParticipantsDb;
use crate::protocol;
use crate::produconsum::Produconsum;
use crate::statistics::SenderStats;
use crate::socklib;

const MAX_SLICE_SIZE: usize = 1024;
const BITS_PER_CHAR: usize = 8;
const NR_SLICES: usize = 8;
const QUEUE_SIZE: usize = 256;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SliceState {
    Free,
    New,
    Xmitted,
    Acked,
}

struct Slice {
    base: usize,
    slice_no: i32,
    bytes: usize,
    next_block: usize,
    state: SliceState,
    rxmit_map: [u8; MAX_SLICE_SIZE / BITS_PER_CHAR],
    is_xmitted_map: [u8; MAX_SLICE_SIZE / BITS_PER_CHAR],
    rxmit_id: i32,
    ready_set: [u8; 128],
    answered_set: [u8; 128],
    nr_ready: usize,
    nr_answered: usize,
    need_rxmit: bool,
    last_good_block: usize,
    fec_data: Vec<u8>,
}

impl Slice {
    fn new(fec_buf_size: usize) -> Self {
        Self {
            base: 0,
            slice_no: 0,
            bytes: 0,
            next_block: 0,
            state: SliceState::Free,
            rxmit_map: [0; MAX_SLICE_SIZE / BITS_PER_CHAR],
            is_xmitted_map: [0; MAX_SLICE_SIZE / BITS_PER_CHAR],
            rxmit_id: 0,
            ready_set: [0; 128],
            answered_set: [0; 128],
            nr_ready: 0,
            nr_answered: 0,
            need_rxmit: false,
            last_good_block: 0,
            fec_data: vec![0u8; fec_buf_size],
        }
    }
}

pub struct NetConfig {
    pub net_if: Option<socklib::NetIf>,
    pub port_base: u16,
    pub block_size: u32,
    pub slice_size: u32,
    pub control_mcast_addr: SocketAddrV4,
    pub data_mcast_addr: SocketAddrV4,
    pub mcast_rdv: Option<String>,
    pub ttl: i32,
    pub flags: u32,
    pub capabilities: u32,
    pub min_slice_size: u32,
    pub default_slice_size: u32,
    pub max_slice_size: u32,
    pub rcvbuf: u32,
    pub rexmit_hello_interval: i32,
    pub autostart: i32,
    pub requested_buf_size: u32,
    pub min_receivers: i32,
    pub max_receivers_wait: i32,
    pub min_receivers_wait: i32,
    pub retries_until_drop: i32,
    pub rehello_offset: i32,
    pub start_timeout: i32,
    pub discovery: Discovery,
    pub fec_stripes: u32,
    pub fec_redundancy: u32,
    pub fec_stripesize: u32,
    pub max_bitrate: Option<String>,
    pub autorate: bool,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Discovery {
    Doubling,
    Reducing,
}

pub const FLAG_SN: u32 = 0x0001;
pub const FLAG_NOTSN: u32 = 0x0002;
pub const FLAG_ASYNC: u32 = 0x0004;
pub const FLAG_POINTOPOINT: u32 = 0x0008;
pub const FLAG_FEC: u32 = 0x0010;
pub const FLAG_BCAST: u32 = 0x0020;
pub const FLAG_NOPOINTOPOINT: u32 = 0x0040;
pub const FLAG_NOKBD: u32 = 0x0080;
pub const FLAG_STREAMING: u32 = 0x0100;
pub const FLAG_PASSIVE: u32 = 0x0010;
pub const FLAG_IGNORE_LOST_DATA: u32 = 0x0400;

/// The sender's capabilities word, as it is advertised in HELLO and in the
/// CONNECT_REPLY.
///
/// Base is `SENDER_CAPABILITIES` (CAP_NEW_GEN | CAP_BIG_ENDIAN) plus
/// CAP_ASYNC, exactly as C `udps-negotiate.c:390-392` assembles it. The
/// 2012 reference defines `CAP_FEC` (`udpc-protoc.h`, under
/// BB_FEATURE_UDPCAST_FEC) but never raises it, so C peers never rely on
/// it: the C receiver turns FEC on by the arrival of CMD_FEC packets and
/// only tests CAP_NEW_GEN / CAP_ASYNC from the advertised word. This port
/// additionally sets CAP_FEC while `-F` is in use, so the word tells the
/// truth; the bit is harmless to old peers for the same reason CAP_LATE_JOIN
/// is.
pub fn sender_capabilities(flags: u32) -> u32 {
    let mut caps = protocol::SENDER_CAPABILITIES;
    if flags & FLAG_FEC != 0 {
        caps |= protocol::CAP_FEC;
    }
    if flags & FLAG_ASYNC != 0 {
        caps |= protocol::CAP_ASYNC;
    }
    caps
}

pub const DEFAULT_STAT_PERIOD: i64 = 500_000;

pub fn send_hello(net_config: &NetConfig, sock: &UdpSocket, streaming: bool) {
    let opcode = if streaming {
        protocol::CMD_HELLO_STREAMING
    } else {
        protocol::CMD_HELLO_NEW
    };
    let hello = protocol::Hello {
        capabilities: net_config.capabilities,
        mcast: protocol::ip4_into_16(*net_config.data_mcast_addr.ip()),
        block_size: net_config.block_size as i32,
    };
    let packed = hello.pack(opcode);
    let _ = sock.send_to(&packed, &net_config.control_mcast_addr);
}

enum IncomingMsg {
    Ok { slice_no: i32, cl_no: i32 },
    Retransmit { slice_no: i32, cl_no: i32, map: [u8; 128], rxmit: i32 },
    Disconnect { cl_no: i32 },
}

struct ReturnChannel {
    incoming: Arc<Produconsum>,
    free_space: Arc<Produconsum>,
    messages: Vec<std::cell::UnsafeCell<IncomingMsg>>,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
    /// Diagnostics: packets read from the socket, messages enqueued and
    /// messages popped by the dispatcher.
    rx: Arc<AtomicU64>,
    enq: Arc<AtomicU64>,
    popped: Arc<AtomicU64>,
}

unsafe impl Send for ReturnChannel {}
unsafe impl Sync for ReturnChannel {}

impl ReturnChannel {
    fn new() -> Self {
        let incoming = Arc::new(Produconsum::new(QUEUE_SIZE, "rc:incoming"));
        let free_space = Arc::new(Produconsum::new(QUEUE_SIZE, "rc:free"));
        free_space.produce(QUEUE_SIZE);
        let messages = (0..QUEUE_SIZE)
            .map(|_| std::cell::UnsafeCell::new(IncomingMsg::Ok { slice_no: 0, cl_no: 0 }))
            .collect();
        Self {
            incoming,
            free_space,
            messages,
            stop: Arc::new(AtomicBool::new(false)),
            handle: None,
            rx: Arc::new(AtomicU64::new(0)),
            enq: Arc::new(AtomicU64::new(0)),
            popped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Reads answers from every socket the sender listens on. Receivers unicast
    /// their OK/retransmit messages to the sender's port, but depending on how
    /// the sockets are bound the kernel may hand such a datagram to any of them,
    /// so all of them have to be polled (C gets this for free because it does not
    /// set SO_REUSEPORT; we simply read from all sockets to be safe).
    fn start(
        &mut self,
        socks: Vec<UdpSocket>,
        port_base: u16,
        db: Arc<std::sync::Mutex<ParticipantsDb>>,
    ) {
        let incoming = self.incoming.clone();
        let free_space = self.free_space.clone();
        let stop = self.stop.clone();
        let rx = self.rx.clone();
        let enq = self.enq.clone();
        let msgs = &self.messages as *const Vec<std::cell::UnsafeCell<IncomingMsg>> as usize;

        let handle = std::thread::spawn(move || {
            use nix::sys::select::{select, FdSet};
            use std::os::unix::io::{AsRawFd, BorrowedFd};

            let msgs = unsafe { &*(msgs as *const Vec<std::cell::UnsafeCell<IncomingMsg>>) };
            for s in socks.iter() {
                s.set_read_timeout(Some(Duration::from_millis(200))).ok();
            }
            // Sockets reported readable by the last select; they may still hold
            // queued datagrams.
            let mut ready: Vec<i32> = Vec::new();
            let mut buf = [0u8; 512];
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }

                let fd = match ready.pop() {
                    Some(fd) => fd,
                    None => {
                        let mut read_set = FdSet::new();
                        let mut max_fd = 0i32;
                        for s in socks.iter() {
                            let fd = s.as_raw_fd();
                            read_set.insert(unsafe { BorrowedFd::borrow_raw(fd) });
                            if fd >= max_fd {
                                max_fd = fd + 1;
                            }
                        }
                        if max_fd == 0 {
                            break;
                        }
                        let mut tv = nix::sys::time::TimeVal::new(0, 200_000_000);
                        match select(max_fd, Some(&mut read_set), None, None, Some(&mut tv)) {
                            Ok(0) => continue,
                            Ok(_) => {
                                for s in socks.iter() {
                                    let fd = s.as_raw_fd();
                                    if read_set.contains(unsafe { BorrowedFd::borrow_raw(fd) }) {
                                        ready.push(fd);
                                    }
                                }
                            }
                            Err(nix::errno::Errno::EINTR) => continue,
                            Err(_) => continue,
                        }
                        continue;
                    }
                };

                let Some(sock) = socks.iter().find(|s| s.as_raw_fd() == fd) else {
                    continue;
                };
                let (n, from) = match sock.recv_from(&mut buf) {
                    Ok(r) => r,
                    // Drained (or interrupted): go back to select().
                    Err(_) => continue,
                };
                // There may be more datagrams waiting on this very socket.
                ready.push(fd);

                if n < 4 {
                    continue;
                }
                rx.fetch_add(1, Ordering::Relaxed);
                let from_v4 = match from {
                    std::net::SocketAddr::V4(v4) => v4,
                    _ => continue,
                };
                if from_v4.port() != socklib::RECEIVER_PORT(port_base) {
                    continue;
                }

                let db = db.lock().unwrap();
                let cl_no = db.lookup_participant(&from_v4);
                if cl_no < 0 {
                    continue;
                }
                drop(db);

                let opcode = u16::from_be_bytes([buf[0], buf[1]]);
                let msg = match opcode {
                    protocol::CMD_OK => {
                        if n < protocol::OK_MSG_SIZE { continue; }
                        let ok = protocol::OkMsg::unpack(&buf);
                        IncomingMsg::Ok { slice_no: ok.slice_no, cl_no }
                    }
                    protocol::CMD_RETRANSMIT => {
                        if n < protocol::RETRANSMIT_SIZE { continue; }
                        let rt = protocol::Retransmit::unpack(&buf);
                        IncomingMsg::Retransmit { slice_no: rt.slice_no, cl_no, map: rt.map, rxmit: rt.rxmit }
                    }
                    protocol::CMD_DISCONNECT => {
                        IncomingMsg::Disconnect { cl_no }
                    }
                    _ => continue,
                };

                free_space.consume(1);
                let pos = free_space.get_consumer_position();
                unsafe { *msgs[pos].get() = msg; }
                free_space.consumed(1);
                enq.fetch_add(1, Ordering::Relaxed);
                incoming.produce(1);
            }
        });
        self.handle = Some(handle);
    }

    fn get_waiting(&self) -> usize {
        self.incoming.get_waiting()
    }

    fn pop(&self) -> Option<IncomingMsg> {
        if self.incoming.get_waiting() == 0 {
            return None;
        }
        self.incoming.consume(1);
        let pos = self.incoming.get_consumer_position();
        let msg = unsafe { std::ptr::read(self.messages[pos].get()) };
        self.incoming.consumed(1);
        self.free_space.produce(1);
        self.popped.fetch_add(1, Ordering::Relaxed);
        Some(msg)
    }

    fn consume_with_timeout(&self, timeout: Duration) -> bool {
        self.incoming.consume_any_with_timeout(timeout) > 0
    }

    fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    fn join(&mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn bit_is_set(bitmap: &[u8], bit: usize) -> bool {
    (bitmap[bit / 8] & (1 << (bit % 8))) != 0
}

fn set_bit(bitmap: &mut [u8], bit: usize) {
    bitmap[bit / 8] |= 1 << (bit % 8);
}

/// `extra_socks` are the sender's additional listeners (broadcast and joined
/// multicast rendezvous sockets). Receiver answers have to be read from all
/// of them, not just from the socket used for sending.
pub fn spawn_net_sender(
    data: &Produconsum,
    free_mem_queue: &Produconsum,
    data_buffer: &[u8],
    data_buf_size: usize,
    sock: &UdpSocket,
    extra_socks: &[UdpSocket],
    net_config: &mut NetConfig,
    db: Arc<std::sync::Mutex<ParticipantsDb>>,
    stats: &mut SenderStats,
) {
    let fec_buf_size = if net_config.flags & FLAG_FEC != 0 {
        (net_config.fec_stripes * net_config.fec_redundancy * net_config.block_size) as usize
    } else {
        0
    };

    if net_config.flags & FLAG_FEC != 0 {
        crate::fec::fec_init();
    }

    let mut slices: Vec<Slice> = (0..NR_SLICES).map(|_| Slice::new(fec_buf_size)).collect();

    let mut rc = ReturnChannel::new();
    let mut ack_socks: Vec<UdpSocket> = Vec::with_capacity(1 + extra_socks.len());
    ack_socks.push(sock.try_clone().unwrap());
    for s in extra_socks {
        if let Ok(c) = s.try_clone() {
            ack_socks.push(c);
        }
    }
    rc.start(ack_socks, net_config.port_base, db.clone());

    let mut slice_no = 0i32;
    let mut xmit_slice: Option<usize> = None;
    let mut pending: Vec<usize> = Vec::new();
    let mut at_end = false;
    let mut wait_average: u64 = 10_000;
    // Consecutive select timeouts while waiting for acks; used for the
    // "Timeout notAnswered=..." diagnostic (C's nrWaited).
    let mut nr_waited: u32 = 0;
    // Late-join (CAP_LATE_JOIN): keep re-sending the CONNECT_REPLY to
    // participants that registered but never answered, until the first
    // slice is acked. While no slice is acked yet, every sent slice is
    // still in this ring, so a late joiner can re-request all of them and
    // catch up. Once the first slice is acked, any later joiner would miss
    // data forever, so the re-replies stop (a lost-reply receiver then
    // simply times out in its start phase).
    let mut first_slice_acked = false;
    let mut last_reply_rexmit = std::time::Instant::now();

    let max_in_flight = if net_config.flags & FLAG_NOTSN != 0 || net_config.flags & FLAG_SN == 0 {
        1
    } else {
        3
    };

    let mut rate_set = crate::rate::RateGovernorSet::new();
    if let Some(ref br) = net_config.max_bitrate {
        rate_set.add_max_bitrate(br);
    }
    if net_config.autorate {
        rate_set.add_autorate();
    }
    let sock_fd = {
        use std::os::unix::io::AsRawFd;
        sock.as_raw_fd()
    };

    if net_config.default_slice_size == 0 {
        if net_config.flags & FLAG_FEC != 0 {
            net_config.slice_size = net_config.fec_stripesize * net_config.fec_stripes;
        } else if net_config.flags & FLAG_ASYNC != 0 {
            net_config.slice_size = 1024;
        } else if net_config.flags & FLAG_SN != 0 {
            net_config.slice_size = 112;
        } else {
            net_config.slice_size = 130;
        }
        net_config.discovery = Discovery::Doubling;
    } else {
        net_config.slice_size = net_config.default_slice_size;
    }

    if net_config.flags & FLAG_FEC != 0 && net_config.slice_size > 128 * net_config.fec_stripes {
        net_config.slice_size = 128 * net_config.fec_stripes;
    }
    if net_config.flags & FLAG_FEC != 0 && net_config.max_slice_size > net_config.fec_stripes * 128 {
        net_config.max_slice_size = net_config.fec_stripes * 128;
    }
    if net_config.slice_size > net_config.max_slice_size {
        net_config.slice_size = net_config.max_slice_size;
    }

    loop {
        if rc.get_waiting() > 0 {
            handle_next_message(&mut rc, &mut slices, &db, stats);
            continue;
        }

        if !first_slice_acked
            && last_reply_rexmit.elapsed() >= std::time::Duration::from_secs(1)
        {
            last_reply_rexmit = std::time::Instant::now();
            let db_l = db.lock().unwrap();
            for i in 0..crate::participants::MAX_CLIENTS {
                if db_l.is_participant_valid(i)
                    && !db_l.ever_answered(i)
                    && db_l.get_participant_capabilities(i) & protocol::CAP_LATE_JOIN != 0
                {
                    if let Some(addr) = db_l.get_participant_ip(i) {
                        let reply = protocol::ConnectReply {
                            cl_nr: i as i32,
                            block_size: net_config.block_size as i32,
                            capabilities: net_config.capabilities,
                            mcast: protocol::ip4_into_16(*net_config.data_mcast_addr.ip()),
                        };
                        let _ = sock.send_to(&reply.pack(), *addr);
                    }
                }
            }
        }

        if !pending.is_empty() {
            let nr_part = db.lock().unwrap().nr_participants();
            let mut freed = false;
            for &idx in &pending {
                if slices[idx].state != SliceState::Acked && slices[idx].nr_ready >= nr_part {
                    ack_slice(&mut slices[idx], net_config, free_mem_queue, stats);
                    first_slice_acked = true;
                    free_slice(&mut slices[idx]);
                    freed = true;
                }
            }
            if freed {
                pending.retain(|&idx| slices[idx].state != SliceState::Free);
                continue;
            }
        }

        if !pending.is_empty() {
            let mut retransmitted = false;
            for &idx in &pending {
                if slices[idx].need_rxmit {
                    rexmit_slice(
                        &mut slices[idx],
                        data_buffer,
                        data_buf_size,
                        net_config,
                        sock,
                        stats,
                        &mut rate_set,
                        sock_fd,
                    );
                    retransmitted = true;
                }
            }
            if retransmitted {
                continue;
            }
        }

        if !pending.is_empty() {
            let nr_part = db.lock().unwrap().nr_participants();
            let mut freed = false;
            for &idx in &pending {
                if slices[idx].state != SliceState::Acked && slices[idx].nr_answered >= nr_part {
                    slices[idx].rxmit_id += 1;
                    send_reqack(&mut slices[idx], net_config, free_mem_queue, stats, sock);
                    if slices[idx].state == SliceState::Acked {
                        first_slice_acked = true;
                        free_slice(&mut slices[idx]);
                        freed = true;
                    }
                }
            }
            if freed {
                pending.retain(|&idx| slices[idx].state != SliceState::Free);
                continue;
            }
        }

        if xmit_slice.is_none() && pending.len() < max_in_flight && !at_end {
            let pos = match find_free_slice(&slices) {
                Some(p) => p,
                None => {
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
            };
            let slice = &mut slices[pos];
            slice.base = data.get_consumer_position();
            slice.slice_no = slice_no;
            slice_no += 1;
            let mut bytes = data.consume(10 * net_config.block_size as usize);
            if bytes > (net_config.block_size * net_config.slice_size) as usize {
                bytes = (net_config.block_size * net_config.slice_size) as usize;
            }
            if bytes > net_config.block_size as usize {
                bytes -= bytes % net_config.block_size as usize;
            }
            data.consumed(bytes);
            slice.bytes = bytes;
            slice.next_block = 0;
            slice.state = SliceState::New;
            slice.ready_set = [0; 128];
            slice.answered_set = [0; 128];
            slice.nr_ready = 0;
            slice.nr_answered = 0;
            slice.rxmit_map = [0; MAX_SLICE_SIZE / BITS_PER_CHAR];
            slice.is_xmitted_map = [0; MAX_SLICE_SIZE / BITS_PER_CHAR];
            slice.rxmit_id = 0;
            slice.need_rxmit = false;
            slice.last_good_block = 0;
            if bytes == 0 {
                at_end = true;
            }

            if net_config.flags & FLAG_FEC != 0 && bytes > 0 {
                fec_encode_slice(&mut slices[pos], data_buffer, data_buf_size, net_config);
            }

            xmit_slice = Some(pos);
        }

        if let Some(idx) = xmit_slice {
            if slices[idx].state == SliceState::New {
                if crate::util::dbg_on() {
                    crate::util::flprintf(&format!(
                        "DBG {:.3} tx DATA no={} bytes={} base={}\n",
                        crate::util::dbg_stamp(), slices[idx].slice_no,
                        slices[idx].bytes, slices[idx].base
                    ));
                }
                send_slice(
                    &slices[idx],
                    data_buffer,
                    data_buf_size,
                    net_config,
                    sock,
                    &mut rate_set,
                    sock_fd,
                );
                if net_config.flags & FLAG_FEC != 0 && slices[idx].bytes > 0 {
                    send_fec_blocks(&slices[idx], net_config, sock, &mut rate_set, sock_fd);
                }
                slices[idx].state = SliceState::Xmitted;
                send_reqack(&mut slices[idx], net_config, free_mem_queue, stats, sock);
                if slices[idx].state == SliceState::Acked {
                    first_slice_acked = true;
                    free_slice(&mut slices[idx]);
                } else {
                    pending.push(idx);
                }
                xmit_slice = None;
                continue;
            }
        }

        if at_end && pending.is_empty() && xmit_slice.is_none() {
            break;
        }

        if net_config.flags & FLAG_ASYNC != 0 {
            break;
        }

        // C: ``ts = now + 1.1 * waitAverage``, plus "after the tenth
        // retransmission, wait at least one second". That ramp is essential:
        // without it a sender whose average inter-message time has collapsed to
        // a few hundred microseconds (typical for a fast LAN) hammers out
        // REQACKs at ~100/s, burns through the --retries-until-drop budget in
        // about two seconds and then drops *every* participant that has not
        // answered this very round -- including healthy receivers that are only
        // milliseconds away from answering.
        let mut timeout_us = (wait_average as f64 * 1.1) as u64;
        if let Some(&idx) = pending.first() {
            if slices[idx].rxmit_id > 10 {
                timeout_us += 1_000_000;
            }
        }
        // Never spin on a zero-length wait.
        let timeout = Duration::from_micros(timeout_us.max(200));
        let start = std::time::Instant::now();
        if rc.consume_with_timeout(timeout) {
            // C: measured timeout, plus the previous average when we had
            // already waited; then an exponential average over 10 samples.
            let mut elapsed = start.elapsed().as_micros() as u64;
            if nr_waited > 0 {
                elapsed += wait_average;
            }
            wait_average = (wait_average * 9 + elapsed + 9) / 10;
            nr_waited = 0;
            continue;
        }
        nr_waited += 1;

        if let Some(&idx) = pending.first() {
            // Same diagnostic as C's mainDispatcher(): shows exactly which
            // participants stopped answering for the oldest unfinished slice.
            if nr_waited > 5 {
                let mut not_answered = String::new();
                let mut not_ready = String::new();
                {
                    let db_lock = db.lock().unwrap();
                    for i in 0..crate::participants::MAX_CLIENTS {
                        if !db_lock.is_participant_valid(i) {
                            continue;
                        }
                        if !bit_is_set(&slices[idx].answered_set, i) {
                            not_answered.push_str(&format!("{}", if not_answered.is_empty() { "" } else { "," }));
                            not_answered.push_str(&i.to_string());
                        }
                        if !bit_is_set(&slices[idx].ready_set, i) {
                            not_ready.push_str(&format!("{}", if not_ready.is_empty() { "" } else { "," }));
                            not_ready.push_str(&i.to_string());
                        }
                    }
                }
                crate::util::flprintf(&format!(
                    "Timeout notAnswered=[{}] notReady=[{}] nrAns={} nrRead={} nrPart={} avg={}\n",
                    not_answered,
                    not_ready,
                    slices[idx].nr_answered,
                    slices[idx].nr_ready,
                    db.lock().unwrap().nr_participants(),
                    wait_average / 1000
                ));
                nr_waited = 0;
            }
            // C: drop participants that have not acknowledged. The budget
            // is per participant: it starts at the round of that
            // participant's first answer for this slice (a late joiner's
            // missed-start rounds must not eat its --retries-until-drop
            // budget), falling back to the slice start for a participant
            // that never answered at all.
            if slices[idx].rxmit_id > net_config.retries_until_drop {
                let mut to_drop = Vec::new();
                {
                    let db_lock = db.lock().unwrap();
                    for i in 0..crate::participants::MAX_CLIENTS {
                        if db_lock.is_participant_valid(i)
                            && !bit_is_set(&slices[idx].ready_set, i)
                        {
                            let base = db_lock
                                .first_answered_round(i)
                                .filter(|(sn, _)| *sn == slices[idx].slice_no)
                                .map(|(_, r)| r)
                                .unwrap_or(0);
                            if crate::util::dbg_on() {
                                crate::util::flprintf(&format!(
                                    "DBG {:.3} drop-check cl={} slice={} rxmit={} first_round={:?} base={}\n",
                                    crate::util::dbg_stamp(),
                                    i,
                                    slices[idx].slice_no,
                                    slices[idx].rxmit_id,
                                    db_lock.first_answered_round(i),
                                    base
                                ));
                            }
                            if slices[idx].rxmit_id - base > net_config.retries_until_drop {
                                to_drop.push(i);
                            }
                        }
                    }
                }
                for i in to_drop {
                    crate::util::flprintf(&format!(
                        "Dropping client #{} because of timeout\n",
                        i
                    ));
                    db.lock().unwrap().remove_participant(i);
                }
                if db.lock().unwrap().nr_participants() == 0 {
                    break;
                }
            }
            slices[idx].rxmit_id += 1;
            send_reqack(&mut slices[idx], net_config, free_mem_queue, stats, sock);
        }
    }

    rc.stop();
    rc.join();
    free_mem_queue.produce_end();
}

fn handle_next_message(
    rc: &mut ReturnChannel,
    slices: &mut [Slice],
    db: &Arc<std::sync::Mutex<ParticipantsDb>>,
    stats: &mut SenderStats,
) {
    let msg = match rc.pop() {
        Some(m) => m,
        None => return,
    };
    match msg {
        IncomingMsg::Ok { slice_no, cl_no } => {
            let slice = find_slice(slices, slice_no);
            if let Some(idx) = slice {
                db.lock().unwrap().mark_answered(cl_no as usize, Some(slice_no), slices[idx].rxmit_id);
                if !bit_is_set(&slices[idx].ready_set, cl_no as usize) {
                    set_bit(&mut slices[idx].ready_set, cl_no as usize);
                    slices[idx].nr_ready += 1;
                    stats.set_answered(cl_no);
                }
                if !bit_is_set(&slices[idx].answered_set, cl_no as usize) {
                    set_bit(&mut slices[idx].answered_set, cl_no as usize);
                    slices[idx].nr_answered += 1;
                }
            } else {
                db.lock().unwrap().mark_answered(cl_no as usize, None, 0);
            }
        }
        IncomingMsg::Retransmit { slice_no, cl_no, map, rxmit } => {
            let slice = find_slice(slices, slice_no);
            if let Some(idx) = slice {
                db.lock().unwrap().mark_answered(cl_no as usize, Some(slice_no), slices[idx].rxmit_id);
                // C drops a RETR whose echoed rxmit predates the current
                // round. That optimisation is actively harmful when a
                // receiver misses consecutive REQACKs (as happens under
                // loss): it then answers with a stale rxmit and is silently
                // excluded from the retransmit union until it happens to see
                // a fresh REQACK again, which at 30%+ loss may take many
                // seconds. A stale map can only make us retransmit blocks
                // the receiver already has (its have-map drops them), never
                // the opposite, so always accept it for a slice that is
                // still pending; find_slice above already discards RETRs
                // for slices that are gone.
                if rxmit < slices[idx].rxmit_id && crate::util::dbg_on() {
                    crate::util::flprintf(&format!(
                        "DBG {:.3} tx RETR from #{} stale (echoed {} < {}) — accepting\n",
                        crate::util::dbg_stamp(),
                        cl_no,
                        rxmit,
                        slices[idx].rxmit_id
                    ));
                }
                for i in 0..MAX_SLICE_SIZE / BITS_PER_CHAR {
                    slices[idx].rxmit_map[i] |= !map[i];
                }
                slices[idx].need_rxmit = true;
                if !bit_is_set(&slices[idx].answered_set, cl_no as usize) {
                    set_bit(&mut slices[idx].answered_set, cl_no as usize);
                    slices[idx].nr_answered += 1;
                }
            } else {
                db.lock().unwrap().mark_answered(cl_no as usize, None, 0);
            }
        }
        IncomingMsg::Disconnect { cl_no } => {
            for s in slices.iter_mut() {
                if bit_is_set(&s.ready_set, cl_no as usize) {
                    s.ready_set[cl_no as usize / 8] &= !(1 << (cl_no as usize % 8));
                    s.nr_ready -= 1;
                }
                if bit_is_set(&s.answered_set, cl_no as usize) {
                    s.answered_set[cl_no as usize / 8] &= !(1 << (cl_no as usize % 8));
                    s.nr_answered -= 1;
                }
            }
            db.lock().unwrap().remove_participant(cl_no as usize);
        }
    }
}

fn find_slice(slices: &[Slice], slice_no: i32) -> Option<usize> {
    slices
        .iter()
        .position(|s| s.state != SliceState::Free && s.slice_no == slice_no)
}

fn append_ring(packet: &mut Vec<u8>, data_buffer: &[u8], data_buf_size: usize, start: usize, size: usize) {
    let first = std::cmp::min(size, data_buf_size - start);
    packet.extend_from_slice(&data_buffer[start..start + first]);
    if first < size {
        packet.extend_from_slice(&data_buffer[..size - first]);
    }
}

fn send_batch(
    sock: &UdpSocket,
    packets: &[Vec<u8>],
    dest: &SocketAddrV4,
    rate_set: &mut crate::rate::RateGovernorSet,
    fd: i32,
) {
    if packets.is_empty() {
        return;
    }
    let ip = u32::from(*dest.ip());

    let sockaddr = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: dest.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from(*dest.ip()).to_be(),
        },
        sin_zero: [0; 8],
    };
    let mut iovs: Vec<libc::iovec> = packets
        .iter()
        .map(|p| libc::iovec {
            iov_base: p.as_ptr() as *mut libc::c_void,
            iov_len: p.len(),
        })
        .collect();
    let mut hdrs: Vec<libc::mmsghdr> = iovs
        .iter_mut()
        .map(|iov| libc::mmsghdr {
            msg_hdr: libc::msghdr {
                msg_name: &sockaddr as *const _ as *mut libc::c_void,
                msg_namelen: std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                msg_iov: iov,
                msg_iovlen: 1,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            },
            msg_len: 0,
        })
        .collect();

    let mut offset = 0usize;
    while offset < hdrs.len() {
        let chunk = std::cmp::min(1024, hdrs.len() - offset);
        for p in &packets[offset..offset + chunk] {
            rate_set.wait_all(fd, ip, p.len() as i64);
        }
        let n = unsafe {
            libc::sendmmsg(fd, hdrs.as_mut_ptr().add(offset), chunk as libc::c_uint, 0)
        };
        if n <= 0 {
            for p in &packets[offset..] {
                let _ = sock.send_to(p, dest);
            }
            return;
        }
        offset += n as usize;
    }
}

fn send_slice(
    slice: &Slice,
    data_buffer: &[u8],
    data_buf_size: usize,
    net_config: &NetConfig,
    sock: &UdpSocket,
    rate_set: &mut crate::rate::RateGovernorSet,
    fd: i32,
) {
    let nr_blocks = (slice.bytes + net_config.block_size as usize - 1) / net_config.block_size as usize;
    let mut packets: Vec<Vec<u8>> = Vec::with_capacity(nr_blocks);
    for i in 0..nr_blocks {
        let size = if i * net_config.block_size as usize >= slice.bytes {
            0
        } else {
            std::cmp::min(net_config.block_size as usize, slice.bytes - i * net_config.block_size as usize)
        };
        let msg = protocol::DataBlock {
            slice_no: slice.slice_no,
            block_no: i as u16,
            bytes: slice.bytes as i32,
        };
        let header = msg.pack();
        let data_start = (slice.base + i * net_config.block_size as usize) % data_buf_size;
        let mut packet = Vec::with_capacity(header.len() + size);
        packet.extend_from_slice(&header);
        append_ring(&mut packet, data_buffer, data_buf_size, data_start, size);
        packets.push(packet);
    }
    send_batch(sock, &packets, &net_config.data_mcast_addr, rate_set, fd);
}

fn send_fec_blocks(
    slice: &Slice,
    net_config: &NetConfig,
    sock: &UdpSocket,
    rate_set: &mut crate::rate::RateGovernorSet,
    fd: i32,
) {
    let nr_fec = (net_config.fec_redundancy * net_config.fec_stripes) as usize;
    let mut packets: Vec<Vec<u8>> = Vec::with_capacity(nr_fec);
    for i in 0..nr_fec {
        let msg = protocol::FecBlock {
            stripes: net_config.fec_stripes as i32,
            slice_no: slice.slice_no,
            block_no: i as u16,
            bytes: slice.bytes as i32,
        };
        let header = msg.pack();
        let offset = i * net_config.block_size as usize;
        let end = std::cmp::min(offset + net_config.block_size as usize, slice.fec_data.len());
        let mut packet = Vec::with_capacity(header.len() + net_config.block_size as usize);
        packet.extend_from_slice(&header);
        packet.extend_from_slice(&slice.fec_data[offset..end]);
        while packet.len() < header.len() + net_config.block_size as usize {
            packet.push(0);
        }
        packets.push(packet);
    }
    send_batch(sock, &packets, &net_config.data_mcast_addr, rate_set, fd);
}

fn fec_encode_slice(slice: &mut Slice, data_buffer: &[u8], data_buf_size: usize, net_config: &NetConfig) {
    let block_size = net_config.block_size as usize;
    let stripes = net_config.fec_stripes as usize;
    let redundancy = net_config.fec_redundancy as usize;
    let nr_blocks = (slice.bytes + block_size - 1) / block_size;

    let mut last_block_data = vec![0u8; block_size];
    let left_over = slice.bytes % block_size;
    if left_over > 0 && nr_blocks > 0 {
        let last_start = (slice.base + (nr_blocks - 1) * block_size) % data_buf_size;
        for i in 0..left_over {
            last_block_data[i] = data_buffer[(last_start + i) % data_buf_size];
        }
    }

    for stripe in 0..stripes {
        let mut temp_blocks: Vec<Vec<u8>> = Vec::new();

        let mut i = stripe;
        while i < nr_blocks {
            let start = (slice.base + i * block_size) % data_buf_size;
            if i == nr_blocks - 1 && left_over > 0 {
                temp_blocks.push(last_block_data.clone());
            } else {
                let mut block = vec![0u8; block_size];
                let first = std::cmp::min(block_size, data_buf_size - start);
                block[..first].copy_from_slice(&data_buffer[start..start + first]);
                if first < block_size {
                    block[first..].copy_from_slice(&data_buffer[..block_size - first]);
                }
                temp_blocks.push(block);
            }
            i += stripes;
        }

        let data_ptrs: Vec<&[u8]> = temp_blocks.iter().map(|b| b.as_slice()).collect();

        let fec_offsets: Vec<(usize, usize)> = (0..redundancy)
            .map(|r| {
                let offset = (stripe + r * stripes) * block_size;
                let end = std::cmp::min(offset + block_size, slice.fec_data.len());
                (offset, end)
            })
            .collect();

        if !data_ptrs.is_empty() && !fec_offsets.is_empty() {
            let fec_data_ptr = slice.fec_data.as_mut_ptr();
            let mut fec_ptrs: Vec<&mut [u8]> = Vec::new();
            for &(offset, end) in &fec_offsets {
                let ptr = unsafe { fec_data_ptr.add(offset) };
                let len = end - offset;
                let slice_ref = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
                fec_ptrs.push(slice_ref);
            }
            crate::fec::fec_encode(block_size, &data_ptrs, &mut fec_ptrs);
        }
    }
}

fn rexmit_slice(
    slice: &mut Slice,
    data_buffer: &[u8],
    data_buf_size: usize,
    net_config: &NetConfig,
    sock: &UdpSocket,
    stats: &mut SenderStats,
    rate_set: &mut crate::rate::RateGovernorSet,
    fd: i32,
) {
    let nr_blocks = (slice.bytes + net_config.block_size as usize - 1) / net_config.block_size as usize;
    let mut retransmissions = 0u32;
    let mut packets: Vec<Vec<u8>> = Vec::new();

    for i in 0..nr_blocks {
        if !bit_is_set(&slice.rxmit_map, i) || bit_is_set(&slice.is_xmitted_map, i) {
            if i > slice.last_good_block {
                slice.last_good_block = i;
            }
            continue;
        }
        set_bit(&mut slice.is_xmitted_map, i);
        retransmissions += 2;

        let size = if i * net_config.block_size as usize >= slice.bytes {
            0
        } else {
            std::cmp::min(net_config.block_size as usize, slice.bytes - i * net_config.block_size as usize)
        };
        let msg = protocol::DataBlock {
            slice_no: slice.slice_no,
            block_no: i as u16,
            bytes: slice.bytes as i32,
        };
        let header = msg.pack();
        let data_start = (slice.base + i * net_config.block_size as usize) % data_buf_size;
        let mut packet = Vec::with_capacity(header.len() + size);
        packet.extend_from_slice(&header);
        append_ring(&mut packet, data_buffer, data_buf_size, data_start, size);
        // Send the block twice in the same round.  C retransmits a missing
        // block once per (typically one second) round; at 30%+ path loss a
        // one-block tail then needs many rounds to clear, and a receiver
        // that misses its final block (or whose OK gets lost) for
        // --retries-until-drop rounds is dropped despite being healthy.
        // Two attempts per round cut the per-block miss probability from
        // p to p^2 without meaningfully increasing the retransmit volume
        // (the missing set is small by the time retransmissions matter).
        // Duplicate blocks are dropped by the receiver's have-map, which
        // keeps C receivers happy too.
        packets.push(packet.clone());
        packets.push(packet);
    }

    if retransmissions > 0 {
        stats.add_retransmissions(retransmissions);
        send_batch(sock, &packets, &net_config.data_mcast_addr, rate_set, fd);
    }
    slice.need_rxmit = false;
}

fn ack_slice(slice: &mut Slice, net_config: &mut NetConfig, free_mem_queue: &Produconsum, stats: &mut SenderStats) {
    if slice.state == SliceState::Acked {
        return;
    }
    if net_config.flags & FLAG_SN == 0 {
        if net_config.discovery == Discovery::Doubling {
            net_config.slice_size += net_config.slice_size / 4;
            if net_config.slice_size >= net_config.max_slice_size {
                net_config.slice_size = net_config.max_slice_size;
                net_config.discovery = Discovery::Reducing;
            }
        }
    }
    slice.state = SliceState::Acked;
    free_mem_queue.produce(slice.bytes);
    stats.add_bytes(slice.bytes as u64);
    if slice.bytes > 0 {
        stats.display(net_config.block_size, net_config.slice_size, false);
    }
}

fn free_slice(slice: &mut Slice) {
    slice.state = SliceState::Free;
}

fn find_free_slice(slices: &[Slice]) -> Option<usize> {
    slices.iter().position(|s| s.state == SliceState::Free)
}

fn send_reqack(slice: &mut Slice, net_config: &mut NetConfig, free_mem_queue: &Produconsum, stats: &mut SenderStats, sock: &UdpSocket) {
    if net_config.flags & FLAG_ASYNC != 0 && slice.bytes != 0 {
        ack_slice(slice, net_config, free_mem_queue, stats);
        return;
    }

    if net_config.flags & FLAG_ASYNC == 0 && slice.rxmit_id != 0 {
        let nr_blocks = (slice.bytes + net_config.block_size as usize - 1) / net_config.block_size as usize;
        if slice.last_good_block != 0 && slice.last_good_block < nr_blocks {
            net_config.discovery = Discovery::Reducing;
            if slice.last_good_block < net_config.slice_size as usize / 2 {
                net_config.slice_size /= 2;
            } else {
                net_config.slice_size = slice.last_good_block as u32;
            }
            if net_config.slice_size < 32 {
                net_config.slice_size = 32;
            }
        }
    }

    if crate::util::dbg_on() {
        crate::util::flprintf(&format!(
            "DBG {:.3} tx REQACK no={} bytes={} nrReady={} rxmit={}\n",
            crate::util::dbg_stamp(), slice.slice_no, slice.bytes, slice.nr_ready, slice.rxmit_id
        ));
    }
    slice.last_good_block = 0;
    slice.answered_set.copy_from_slice(&slice.ready_set);
    slice.nr_answered = slice.nr_ready;
    slice.need_rxmit = false;
    slice.rxmit_map = [0; MAX_SLICE_SIZE / BITS_PER_CHAR];
    slice.is_xmitted_map = [0; MAX_SLICE_SIZE / BITS_PER_CHAR];

    let msg = protocol::Reqack {
        slice_no: slice.slice_no,
        bytes: slice.bytes as i32,
        rxmit: slice.rxmit_id,
    };
    let packed = msg.pack();
    let _ = sock.send_to(&packed, &net_config.data_mcast_addr);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{self, CAP_ASYNC, CAP_BIG_ENDIAN, CAP_FEC, CAP_NEW_GEN};

    /// The advertised word must match what C `udps-negotiate.c` builds:
    /// SENDER_CAPABILITIES (CAP_NEW_GEN | CAP_BIG_ENDIAN) plus CAP_ASYNC in
    /// async mode. C 2012 never sets CAP_FEC even with `-F`; this port adds
    /// it when FEC is in use (see sender_capabilities).
    #[test]
    fn sender_capabilities_matches_c() {
        assert_eq!(
            protocol::SENDER_CAPABILITIES,
            CAP_NEW_GEN | CAP_BIG_ENDIAN,
            "base word must equal C's SENDER_CAPABILITIES"
        );
        assert_eq!(sender_capabilities(0), protocol::SENDER_CAPABILITIES);
        assert_eq!(
            sender_capabilities(FLAG_ASYNC),
            CAP_NEW_GEN | CAP_BIG_ENDIAN | CAP_ASYNC
        );
        assert_eq!(
            sender_capabilities(FLAG_FEC),
            CAP_NEW_GEN | CAP_BIG_ENDIAN | CAP_FEC
        );
        assert_eq!(
            sender_capabilities(FLAG_FEC | FLAG_ASYNC),
            CAP_NEW_GEN | CAP_BIG_ENDIAN | CAP_FEC | CAP_ASYNC
        );
        // Unrelated flags must not change the word (C: only ASYNC/FEC do).
        assert_eq!(
            sender_capabilities(FLAG_SN | FLAG_POINTOPOINT | FLAG_BCAST),
            protocol::SENDER_CAPABILITIES
        );
    }
}
