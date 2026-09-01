use std::net::{SocketAddrV4, UdpSocket};
use std::os::unix::io::{AsFd, AsRawFd};
use std::time::{Duration, Instant};
use crate::fifo::Fifo;
use crate::protocol;
use crate::socklib;
use crate::statistics::ReceiverStats;

const MAX_SLICE_SIZE: usize = 1024;
const BITS_PER_CHAR: usize = 8;
const NR_SLICES: usize = 8;
const SLICEMAGIC: u32 = 0x41424344;

#[derive(Clone, Copy, PartialEq)]
enum SliceState {
    Free,
    Receiving,
    Done,
}

struct Slice {
    magic: u32,
    state: SliceState,
    base: usize,
    slice_no: i32,
    blocks_transferred: usize,
    data_blocks_transferred: usize,
    bytes: usize,
    bytes_known: bool,
    free_pos: usize,
    retransmit_map: [u8; MAX_SLICE_SIZE / BITS_PER_CHAR],
    fec_data: Vec<u8>,
    fec_block_nos: Vec<u32>,
    fec_blocks: Vec<Vec<u8>>,
    erased_blocks: Vec<u32>,
    fec_stripes: usize,
}

impl Slice {
    fn new(_block_size: u32) -> Self {
        Self {
            magic: SLICEMAGIC,
            state: SliceState::Free,
            base: 0,
            slice_no: -1,
            blocks_transferred: 0,
            data_blocks_transferred: 0,
            bytes: 0,
            bytes_known: false,
            free_pos: 0,
            retransmit_map: [0; MAX_SLICE_SIZE / BITS_PER_CHAR],
            fec_data: Vec::new(),
            fec_block_nos: Vec::new(),
            fec_blocks: Vec::new(),
            erased_blocks: Vec::new(),
            fec_stripes: 0,
        }
    }
}

pub struct ClientConfig {
    pub socks: Vec<Option<UdpSocket>>,
    pub server_addr: SocketAddrV4,
    /// The mcast/bcast control (rendezvous) address on the sender port —
    /// the destination of CONNECT_REQ. Also used as the DISCONNECT
    /// fallback when the sender's unicast address was never learned
    /// (e.g. its one-shot CONNECT_REPLY was lost).
    pub control_addr: SocketAddrV4,
    pub client_number: i32,
    pub is_started: bool,
    pub sender_is_newgen: bool,
    /// `-w/--exit-wait`: milliseconds to keep answering the sender after the
    /// last slice has been received (C `net_config->exitWait`).
    pub exit_wait_ms: u64,
    /// Slice numbers whose data has already been handed to the writer.  C
    /// keeps these in the slice ring until it wraps; we keep an explicit
    /// record so that a late REQACK for an already-delivered slice answers
    /// OK ("old slice => sending ok") instead of re-requesting the whole
    /// slice from scratch — re-requesting an old slice re-opens a completed
    /// region of the file and stalls the sender until it is dropped.
    pub completed_slices: Vec<bool>,
    /// Set once the zero-byte end marker has arrived while some earlier
    /// slice is still incomplete; suppresses repeated warnings.
    pub end_marker_seen: bool,
    /// `--receive-timeout`: seconds without any packet before the receiver
    /// gives up, 0 = wait forever (C `net_config->receiveTimeout`).
    pub receive_timeout_secs: u64,
    /// Late-join (CAP_LATE_JOIN): slices observed on the data multicast
    /// group during the start phase (i.e. before this receiver's CONNECT
    ///_REPLY arrived), as (slice_no, bytes, last seen rxmit). The net
    /// receiver seeds its slice table with them and re-requests all their
    /// blocks, so a receiver that missed its one-shot reply can still
    /// obtain a complete file. Empty for a normal (non-late) join.
    pub late_slices: Vec<(i32, i32, i32)>,
}

pub fn spawn_net_receiver(
    fifo: &Fifo,
    client_config: &mut ClientConfig,
    net_config: &crate::senddata::NetConfig,
    stats: &mut ReceiverStats,
) {
    let mut slices: Vec<Slice> = (0..NR_SLICES).map(|_| Slice::new(net_config.block_size)).collect();

    // Late-join: pre-seed the slice table with the slices the sender had
    // already sent when our (re)sent CONNECT_REPLY finally arrived. Each
    // seeded slice starts with an empty have-map, so the first REQACK for
    // it makes us send a retransmit request for the whole slice; the
    // sender still holds every un-acked slice and re-sends it. Slices are
    // filled and completed in slice-number order, which the sequential
    // file writer requires.
    for &(slice_no, bytes, rxmit) in &client_config.late_slices {
        let _ = rxmit; // retransmits are driven by the sender's fresh REQACKs
        let idx = match find_slice(&mut slices, None, 0, slice_no) {
            Some(idx) => idx,
            None => match new_slice(&mut slices, fifo, slice_no, net_config.block_size) {
                Some(idx) => idx,
                None => continue,
            },
        };
        slices[idx].bytes = bytes as usize;
        slices[idx].bytes_known = true;
        if crate::util::dbg_on() {
            crate::util::flprintf(&format!(
                "DBG {:.3} rx late-join: re-requesting slice {} ({} bytes)\n",
                crate::util::dbg_stamp(),
                slice_no,
                bytes
            ));
        }
    }
    let mut current_slice: Option<usize> = None;
    let mut current_slice_no: i32 = -1;
    let mut end_reached = false;

    let _block_size = net_config.block_size;

    // -w/--exit-wait: once the last slice is in, keep answering the sender's
    // REQACKs for this long before finishing (C waits exitWait per select
    // round in receivedata.c; every incoming packet re-arms the timer).
    let exit_wait = Duration::from_millis(client_config.exit_wait_ms);
    // --receive-timeout: abort when the sender stops delivering packets.
    let receive_timeout = Duration::from_secs(client_config.receive_timeout_secs);
    let mut drain_deadline: Option<Instant> = None;
    let mut last_activity = Instant::now();

    // The poller reads a socket as soon as select() says it is readable, so all
    // of them must be non-blocking (otherwise one quiet socket would stall the
    // loop and with it the timeouts below).
    for sock in client_config.socks.iter().flatten() {
        sock.set_nonblocking(true).ok();
    }
    let mut poller = RxPoller::new();
    let mut buf = vec![0u8; 4096];

    loop {
        if let Some(deadline) = drain_deadline {
            if Instant::now() >= deadline {
                break;
            }
        }

        let (n, _from) = match poller.next(&mut client_config.socks, &mut buf, net_config.port_base) {
            Some(r) => {
                last_activity = Instant::now();
                if end_reached {
                    drain_deadline = Some(last_activity + exit_wait);
                }
                r
            }
            None => {
                if client_config.receive_timeout_secs > 0
                    && client_config.is_started
                    && last_activity.elapsed() >= receive_timeout
                {
                    // Tell the sender to drop us now, so it does not stall
                    // every in-flight slice waiting for our ready/answer.
                    send_disconnect(client_config, 1);
                    crate::util::fatal(1, "Receiver timeout\n");
                }
                continue;
            }
        };

        if n < 4 {
            continue;
        }

        let opcode = u16::from_be_bytes([buf[0], buf[1]]);
        match opcode {
            protocol::CMD_DATA => {
                if n < protocol::DATA_BLOCK_SIZE {
                    continue;
                }
                let msg = protocol::DataBlock::unpack(&buf);
                if crate::util::dbg_on() && msg.block_no == 0 {
                    crate::util::flprintf(&format!(
                        "DBG {:.3} rx DATA no={} bytes={}\n",
                        crate::util::dbg_stamp(), msg.slice_no, msg.bytes
                    ));
                }
                stats.start_timer();
                client_config.is_started = true;
                process_data_block(&mut slices, &mut current_slice, &mut current_slice_no, fifo, net_config, client_config, &msg, &buf[protocol::DATA_BLOCK_SIZE..n]);
            }
            protocol::CMD_FEC => {
                if n < protocol::FEC_BLOCK_SIZE {
                    continue;
                }
                let msg = protocol::FecBlock::unpack(&buf);
                stats.start_timer();
                client_config.is_started = true;
                process_fec_block(&mut slices, fifo, net_config, client_config, &msg, &buf[protocol::FEC_BLOCK_SIZE..n]);
            }
            protocol::CMD_REQACK => {
                if n < protocol::REQACK_SIZE {
                    continue;
                }
                let msg = protocol::Reqack::unpack(&buf);
                stats.start_timer();
                client_config.is_started = true;
                process_reqack(&mut slices, fifo, net_config, client_config, &msg);
            }
            protocol::CMD_HELLO_NEW | protocol::CMD_HELLO_STREAMING | protocol::CMD_HELLO => {
                continue;
            }
            protocol::CMD_CONNECT_REPLY => {
                // A stale duplicate of the reply we already processed during
                // negotiation: the receiver retransmits CONNECT_REQ until it
                // gets a reply, so a reply to an earlier retransmit may still
                // be queued when the main loop starts. All of its fields were
                // applied by the first reply, so it is a no-op — ignore it
                // instead of treating it as a protocol error.
                continue;
            }
            _ => {
                crate::util::flprintf(&format!("Unexpected command {:04x}\n", opcode));
            }
        }

        if end_reached {
            continue;
        }

        for s in &slices {
            if s.state == SliceState::Done && s.bytes == 0 {
                end_reached = true;
                drain_deadline = Some(Instant::now() + exit_wait);
                break;
            }
        }
    }
}

/// Is `from` a packet from the sender (as opposed to some unrelated host that
/// happens to talk to one of our ports)? Mirrors the port check in C.
fn is_sender_packet(from: &std::net::SocketAddr, port_base: u16) -> bool {
    let std::net::SocketAddr::V4(v4) = from else {
        return false;
    };
    v4.port() == socklib::SENDER_PORT(port_base) || v4.port() == socklib::RECEIVER_PORT(port_base)
}

/// Waits for the next sender datagram on any of the receiver's sockets.
///
/// One `select()` reports which sockets are readable and those are then drained
/// one by one, so a burst of incoming data costs one select plus one recvfrom
/// per packet instead of a probe on every socket for every packet.
struct RxPoller {
    /// Socket indices reported readable by the last select; the tail is
    /// drained first.
    ready: Vec<usize>,
}

impl RxPoller {
    fn new() -> Self {
        Self { ready: Vec::new() }
    }

    fn next(
        &mut self,
        socks: &mut [Option<UdpSocket>],
        buf: &mut [u8],
        port_base: u16,
    ) -> Option<(usize, std::net::SocketAddr)> {
        use nix::sys::select::{select, FdSet};

        loop {
            while let Some(i) = self.ready.pop() {
                if i >= socks.len() {
                    continue;
                }
                let Some(sock) = &mut socks[i] else { continue };
                match sock.recv_from(buf) {
                    Ok((n, from)) => {
                        if is_sender_packet(&from, port_base) {
                            // More data may still be queued here.
                            self.ready.push(i);
                            return Some((n, from));
                        }
                    }
                    // Drained or interrupted: try the next ready socket.
                    Err(_) => {}
                }
            }

            let mut read_set = FdSet::new();
            let mut max_fd = 0i32;
            for (_i, sock) in socks
                .iter()
                .enumerate()
                .filter_map(|(i, s)| s.as_ref().map(|s| (i, s)))
            {
                read_set.insert(sock.as_fd());
                if sock.as_raw_fd() >= max_fd {
                    max_fd = sock.as_raw_fd() + 1;
                }
            }
            if max_fd == 0 {
                return None;
            }

            // Short timeout: the caller re-checks its own timeouts whenever
            // this returns None.
            let mut timeout = nix::sys::time::TimeVal::new(0, 100_000);
            match select(max_fd, Some(&mut read_set), None, None, Some(&mut timeout)) {
                Ok(0) => return None,
                Ok(_) => {
                    self.ready.clear();
                    for (i, sock) in socks
                        .iter()
                        .enumerate()
                        .filter_map(|(i, s)| s.as_ref().map(|s| (i, s)))
                    {
                        if read_set.contains(sock.as_fd()) {
                            self.ready.push(i);
                        }
                    }
                }
                Err(nix::errno::Errno::EINTR) => continue,
                Err(_) => return None,
            }
        }
    }
}

fn find_slice(slices: &mut [Slice], current_slice: Option<usize>, _current_slice_no: i32, slice_no: i32) -> Option<usize> {
    if let Some(idx) = current_slice {
        if slices[idx].slice_no == slice_no {
            return Some(idx);
        }
    }
    for (i, s) in slices.iter().enumerate() {
        if s.slice_no == slice_no && s.state != SliceState::Free {
            return Some(i);
        }
    }
    None
}

fn new_slice(slices: &mut [Slice], fifo: &Fifo, slice_no: i32, block_size: u32) -> Option<usize> {
    // Prefer a genuinely free slot; otherwise recycle a completed slice whose
    // data has already been handed to the writer.
    let idx = slices.iter().position(|s| s.state == SliceState::Free)
        .or_else(|| slices.iter().position(|s| s.state == SliceState::Done));
    let i = idx?;
    // Wait for enough free memory for a worst-case slice before claiming the
    // base position, like C's newSlice; otherwise concurrently in-flight
    // slices would share the same fifo region.
    fifo.free_mem_queue.consume(block_size as usize * MAX_SLICE_SIZE);
    let s = &mut slices[i];
    s.magic = SLICEMAGIC;
    s.state = SliceState::Receiving;
    s.blocks_transferred = 0;
    s.data_blocks_transferred = 0;
    s.retransmit_map = [0; MAX_SLICE_SIZE / BITS_PER_CHAR];
    s.free_pos = 0;
    s.bytes = 0;
    s.bytes_known = false;
    s.slice_no = slice_no;
    s.base = fifo.free_mem_queue.get_consumer_position();
    s.fec_data.clear();
    s.fec_block_nos.clear();
    s.fec_blocks.clear();
    s.erased_blocks.clear();
    s.fec_stripes = 0;
    Some(i)
}

fn process_data_block(
    slices: &mut [Slice],
    current_slice: &mut Option<usize>,
    current_slice_no: &mut i32,
    fifo: &Fifo,
    net_config: &crate::senddata::NetConfig,
    client_config: &mut ClientConfig,
    msg: &protocol::DataBlock,
    data: &[u8],
) {
    let slice_idx = match find_slice(slices, *current_slice, *current_slice_no, msg.slice_no) {
        Some(idx) => idx,
        None => {
            if let Some(idx) = new_slice(slices, fifo, msg.slice_no, net_config.block_size) {
                *current_slice = Some(idx);
                *current_slice_no = msg.slice_no;
                idx
            } else {
                return;
            }
        }
    };

    let slice = &mut slices[slice_idx];
    if slice.state == SliceState::Free || slice.state == SliceState::Done {
        return;
    }

    let block_no = msg.block_no as usize;
    if block_no >= MAX_SLICE_SIZE {
        return;
    }

    let byte_idx = block_no / 8;
    let bit_idx = block_no % 8;
    if (slice.retransmit_map[byte_idx] & (1 << bit_idx)) != 0 {
        return;
    }

    if msg.bytes != 0 {
        slice.bytes = msg.bytes as usize;
        slice.bytes_known = true;
    }

    let data_start = (slice.base + block_no * net_config.block_size as usize) % fifo.data_buf_size;
    let copy_len = std::cmp::min(data.len(), net_config.block_size as usize);
    fifo.write_at(data_start, &data[..copy_len]);

    slice.retransmit_map[byte_idx] |= 1 << bit_idx;
    slice.data_blocks_transferred += 1;
    slice.blocks_transferred += 1;

    check_slice_complete(slices, slice_idx, fifo, net_config, client_config);
}

fn process_fec_block(
    slices: &mut [Slice],
    fifo: &Fifo,
    net_config: &crate::senddata::NetConfig,
    client_config: &mut ClientConfig,
    msg: &protocol::FecBlock,
    data: &[u8],
) {
    let slice_idx = match find_slice(slices, None, 0, msg.slice_no) {
        Some(idx) => idx,
        None => {
            if let Some(idx) = new_slice(slices, fifo, msg.slice_no, net_config.block_size) {
                idx
            } else {
                return;
            }
        }
    };

    let slice = &mut slices[slice_idx];
    if slice.state == SliceState::Done {
        return;
    }

    if msg.bytes != 0 && !slice.bytes_known {
        slice.bytes = msg.bytes as usize;
        slice.bytes_known = true;
    }

    if msg.stripes > 0 {
        slice.fec_stripes = msg.stripes as usize;
    }

    slice.fec_block_nos.push(msg.block_no as u32);
    slice.fec_blocks.push(data.to_vec());
    slice.blocks_transferred += 1;

    check_slice_complete(slices, slice_idx, fifo, net_config, client_config);
}

fn check_slice_complete(slices: &mut [Slice], slice_idx: usize, fifo: &Fifo, net_config: &crate::senddata::NetConfig, client_config: &mut ClientConfig) {
    let slice = &mut slices[slice_idx];
    if !slice.bytes_known || slice.bytes == 0 {
        return;
    }

    let blocks_in_slice = (slice.bytes + net_config.block_size as usize - 1) / net_config.block_size as usize;

    if slice.data_blocks_transferred >= blocks_in_slice {
        complete_slice(slices, slice_idx, fifo, net_config, client_config);
        return;
    }

    if slice.fec_stripes > 0 && !slice.fec_blocks.is_empty() {
        let missing = blocks_in_slice - slice.data_blocks_transferred;
        if slice.fec_blocks.len() >= missing {
            try_fec_recover(slices, slice_idx, fifo, net_config, client_config);
        }
    }
}

fn try_fec_recover(slices: &mut [Slice], slice_idx: usize, fifo: &Fifo, net_config: &crate::senddata::NetConfig, client_config: &mut ClientConfig) {
    let block_size = net_config.block_size as usize;

    let slice = &mut slices[slice_idx];
    let stripes = slice.fec_stripes;
    let blocks_in_slice = (slice.bytes + block_size - 1) / block_size;

    if stripes == 0 {
        return;
    }

    for stripe in 0..stripes {
        let mut stripe_indices: Vec<usize> = Vec::new();
        let mut i = stripe;
        while i < blocks_in_slice {
            stripe_indices.push(i);
            i += stripes;
        }
        let nr_stripe = stripe_indices.len();

        let mut erased: Vec<u32> = Vec::new();
        for (j, &gi) in stripe_indices.iter().enumerate() {
            let byte_idx = gi / 8;
            let bit_idx = gi % 8;
            if (slice.retransmit_map[byte_idx] & (1 << bit_idx)) == 0 {
                erased.push(j as u32);
            }
        }
        if erased.is_empty() {
            continue;
        }
        let needed = erased.len();

        let mut fec_sel: Vec<&[u8]> = Vec::new();
        let mut fec_nos: Vec<u32> = Vec::new();
        for (k, &bno) in slice.fec_block_nos.iter().enumerate() {
            if (bno as usize) % stripes == stripe {
                fec_sel.push(slice.fec_blocks[k].as_slice());
                fec_nos.push(bno / stripes as u32);
            }
        }
        if fec_sel.len() < needed {
            continue;
        }
        fec_sel.truncate(needed);
        fec_nos.truncate(needed);

        let mut data_blocks: Vec<Vec<u8>> = vec![vec![0u8; block_size]; nr_stripe];
        for (j, &gi) in stripe_indices.iter().enumerate() {
            let byte_idx = gi / 8;
            let bit_idx = gi % 8;
            if (slice.retransmit_map[byte_idx] & (1 << bit_idx)) != 0 {
                let start = (slice.base + gi * block_size) % fifo.data_buf_size;
                fifo.read_at(start, &mut data_blocks[j]);
            }
        }
        let mut data_ptrs: Vec<&mut [u8]> = data_blocks.iter_mut().map(|b| b.as_mut_slice()).collect();

        let ok = crate::fec::fec_decode(block_size, &mut data_ptrs, &fec_sel, &fec_nos, &erased);
        if !ok {
            continue;
        }

        for &ej in &erased {
            let gi = stripe_indices[ej as usize];
            let data_start = (slice.base + gi * block_size) % fifo.data_buf_size;
            fifo.write_at(data_start, &data_blocks[ej as usize]);
            let byte_idx = gi / 8;
            let bit_idx = gi % 8;
            slice.retransmit_map[byte_idx] |= 1 << bit_idx;
            slice.data_blocks_transferred += 1;
            slice.blocks_transferred += 1;
        }
    }

    let blocks_in_slice = (slice.bytes + block_size - 1) / block_size;
    if slice.data_blocks_transferred >= blocks_in_slice {
        complete_slice(slices, slice_idx, fifo, net_config, client_config);
    }
}

fn complete_slice(slices: &mut [Slice], slice_idx: usize, fifo: &Fifo, _net_config: &crate::senddata::NetConfig, client_config: &mut ClientConfig) {
    let slice = &mut slices[slice_idx];
    if slice.state == SliceState::Done {
        return;
    }
    slice.state = SliceState::Done;
    // Remember that this slice number has been delivered, and proactively
    // tell the sender we are ready for it.  C only answers OK in response to
    // a REQACK, so a receiver that just completed a slice but happens to miss
    // every REQACK of the following rounds cannot get its OK through and is
    // eventually dropped even though it has the complete file.  A proactive
    // OK (retried by the REQACK-driven OKs as well) breaks that dependency;
    // the sender's OK handling is idempotent, so the extra copy is harmless
    // to C senders.
    if (slice.slice_no as usize) >= client_config.completed_slices.len() {
        client_config.completed_slices.resize(slice.slice_no as usize + 1, false);
    }
    client_config.completed_slices[slice.slice_no as usize] = true;
    if slice.bytes > 0 {
        fifo.free_mem_queue.consumed(slice.bytes);
        fifo.data.produce(slice.bytes);
    } else {
        // Zero-byte slice: the transfer end marker.  Only end the transfer
        // if every earlier slice completed; otherwise the file is
        // unrecoverable (the sender may have freed the missing slices) and
        // we must fail the transfer, not report a truncated file as
        // complete.
        let mut all_done = true;
        let mut missing = Vec::new();
        for j in 0..(slice.slice_no as usize) {
            if !*client_config.completed_slices.get(j).unwrap_or(&false) {
                all_done = false;
                missing.push(j);
            }
        }
        if all_done {
            fifo.data.produce_end();
        } else if !client_config.end_marker_seen {
            client_config.end_marker_seen = true;
            crate::util::flprintf(&format!(
                "End of transfer reached with incomplete slice(s): {:?} -- file is incomplete, waiting for receive timeout\n",
                missing
            ));
        }
    }
    let ok_msg = protocol::OkMsg { slice_no: slice.slice_no };
    let packed = ok_msg.pack();
    if let Some(sock) = &client_config.socks[0] {
        send_answer(sock, &packed, &client_config.server_addr, "OK");
    }
}

/// Sends an answer (OK or retransmit request) back to the sender. A failure to
/// deliver is what stalls a transfer, so it must never be swallowed silently.
fn send_answer(sock: &UdpSocket, packed: &[u8], dest: &SocketAddrV4, what: &str) {
    match sock.send_to(packed, dest) {
        Ok(_) => {}
        Err(e) => {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            if n < 5 || n % 100 == 0 {
                crate::util::flprintf(&format!(
                    "Failed to send {} to {}: {}
",
                    what,
                    dest,
                    e
                ));
            }
        }
    }
}

fn process_reqack(
    slices: &mut [Slice],
    fifo: &Fifo,
    net_config: &crate::senddata::NetConfig,
    client_config: &mut ClientConfig,
    msg: &protocol::Reqack,
) {
    let slice_idx = match find_slice(slices, None, 0, msg.slice_no) {
        Some(idx) => idx,
        None => {
            if client_config.completed_slices.get(msg.slice_no as usize) == Some(&true) {
                // C: "an old slice => send ok".  This slice's data is already
                // in the file; the sender is re-REQACKing it for other (late
                // or slow) receivers.  Answering OK keeps us out of the
                // retransmit union; re-creating the slice would re-request a
                // region of the file we already have.
                let ok_msg = protocol::OkMsg { slice_no: msg.slice_no };
                let packed = ok_msg.pack();
                if let Some(sock) = &client_config.socks[0] {
                    send_answer(sock, &packed, &client_config.server_addr, "OK");
                }
                return;
            }
            // A slice number we have never delivered: either the next new
            // slice or one a late joiner seeded; create it so our retransmit
            // request below carries a proper map (previously this path
            // re-opened already-completed slices after their ring slot was
            // recycled, growing the missing set every round).
            match new_slice(slices, fifo, msg.slice_no, net_config.block_size) {
                Some(idx) => idx,
                None => {
                    let ok_msg = protocol::OkMsg { slice_no: msg.slice_no };
                    let packed = ok_msg.pack();
                    if let Some(sock) = &client_config.socks[0] {
                        send_answer(sock, &packed, &client_config.server_addr, "OK");
                    }
                    return;
                }
            }
        }
    };

    {
        let slice = &mut slices[slice_idx];
        slice.bytes = msg.bytes as usize;
        slice.bytes_known = true;
    }

    let blocks_in_slice = (slices[slice_idx].bytes + net_config.block_size as usize - 1)
        / net_config.block_size as usize;

    if crate::util::dbg_on() {
        crate::util::flprintf(&format!(
            "DBG {:.3} rx REQACK no={} bytes={} have={}/{} -> {}\n",
            crate::util::dbg_stamp(), msg.slice_no, slices[slice_idx].bytes,
            slices[slice_idx].data_blocks_transferred, blocks_in_slice,
            if slices[slice_idx].data_blocks_transferred >= blocks_in_slice { "OK" } else { "RETR" }
        ));
    }

    if slices[slice_idx].data_blocks_transferred >= blocks_in_slice {
        let ok_msg = protocol::OkMsg { slice_no: msg.slice_no };
        let packed = ok_msg.pack();
        if let Some(sock) = &client_config.socks[0] {
            send_answer(sock, &packed, &client_config.server_addr, "OK");
        }
        complete_slice(slices, slice_idx, fifo, net_config, client_config);
    } else {
        let mut map = [0u8; 128];
        for i in 0..blocks_in_slice {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            if (slices[slice_idx].retransmit_map[byte_idx] & (1 << bit_idx)) != 0 {
                map[byte_idx] |= 1 << bit_idx;
            }
        }
        let retransmit = protocol::Retransmit {
            slice_no: msg.slice_no,
            rxmit: msg.rxmit,
            map,
        };
        let packed = retransmit.pack();
        if let Some(sock) = &client_config.socks[0] {
            send_answer(sock, &packed, &client_config.server_addr, "RETR");
        }
    }
}

pub fn send_connect_req(client_config: &mut ClientConfig, _net_config: &crate::senddata::NetConfig) {
    let msg = protocol::ConnectReq {
        capabilities: protocol::RECEIVER_CAPABILITIES,
        rcvbuf: crate::socklib::get_rcv_buf(client_config.socks[0].as_ref().unwrap()),
    };
    let packed = msg.pack();
    if let Some(sock) = &client_config.socks[0] {
        let _ = sock.send_to(&packed, &client_config.server_addr);
    }
}

pub fn send_go(client_config: &mut ClientConfig) {
    let packed = protocol::pack_go();
    if let Some(sock) = &client_config.socks[0] {
        let _ = sock.send_to(&packed, &client_config.server_addr);
    }
}

/// Number of DISCONNECT attempts on a failing exit. The notification is a
/// one-way datagram with no acknowledgement, so like the CONNECT_REPLY it can
/// be lost; a handful of retransmissions makes a lost notification a
/// vanishingly rare event.
pub const DISCONNECT_RETRIES: u32 = 5;

pub fn send_disconnect(client_config: &mut ClientConfig, exit_status: i32) {
    let packed = protocol::pack_disconnect();
    // Prefer the learned unicast sender address (C behaviour); when the
    // sender's address was never learned — the CONNECT_REPLY(s) were all
    // lost — use the mcast/bcast rendezvous address, which both the C and
    // the Rust sender listen on. When both are known, use both: more
    // chances for a datagram to survive.
    let mut dests: [Option<SocketAddrV4>; 2] = [None, None];
    let mut n_dests = 0;
    if !client_config.server_addr.ip().is_unspecified() {
        dests[n_dests] = Some(client_config.server_addr);
        n_dests += 1;
    }
    if !client_config.control_addr.ip().is_unspecified()
        && client_config.control_addr != client_config.server_addr
    {
        dests[n_dests] = Some(client_config.control_addr);
        n_dests += 1;
    }
    if let Some(sock) = &client_config.socks[0] {
        // A clean end of transfer needs exactly one notification (the
        // sender is shutting the transfer down anyway); a failing exit
        // retransmits, because the sender must drop us now or it would
        // stall every slice until its retry-until-drop budget runs out.
        let rounds = if exit_status == 0 { 1 } else { DISCONNECT_RETRIES };
        for i in 0..rounds {
            for k in 0..n_dests {
                let Some(d) = dests[k] else { continue };
                let _ = sock.send_to(&packed, d);
            }
            if i + 1 < rounds {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
    if exit_status == 0 {
        crate::util::flprintf("Transfer complete.\x07\n");
    }
}
