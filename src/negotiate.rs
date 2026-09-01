//! Sender-side rendezvous / negotiation with the receivers.
//!
//! Mirrors `udps-negotiate.c`. The important part for a one-sender / many-
//! receiver setup is that the sender listens on *all* the addresses a receiver
//! may announce itself on:
//!
//!  * the unicast address of the interface (directed CONNECT_REQ),
//!  * the subnet broadcast address (`makeSocket(ADDR_TYPE_BCAST, ...)`),
//!  * the multicast rendezvous group (`makeSocket(ADDR_TYPE_MCAST, ...)`) –
//!    a socket that did not join the group never sees packets sent to it.
//!
//! C keeps these in the `sock[]` array of `startSender()` and selects over all
//! of them; `SENDER_SOCKS` below is the same thing.

use std::net::UdpSocket;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::console::Console;
use crate::diskio::DiskConfig;
use crate::participants::ParticipantsDb;
use crate::protocol;
use crate::senddata::{self, NetConfig};
use crate::socklib;
use crate::util;

/// Interval between HELLO retransmissions while waiting for receivers, when
/// the user did not ask for a specific one (`-H`). HELLOs are what lets a
/// receiver that started before the sender (or that lost its CONNECT_REQ)
/// discover the transfer, so unlike the C version – where hello retransmission
/// is off unless `-H`/`-S`/`--async` is given – the Rust sender keeps asking
/// for participants at 1s intervals by default. `-H -1` turns it off.
pub const DEFAULT_HELLO_INTERVAL_MS: i32 = 1000;

/// Return value of a dispatch round.
const DISPATCH_WAIT: i32 = 0;
const DISPATCH_START: i32 = 1;
const DISPATCH_GIVE_UP: i32 = -1;

/// The sender's sockets, mirroring C's `sock[]` array in `startSender()`:
/// index 0 is the main (unicast) socket used for everything that goes out,
/// the others are additional listeners.
pub struct SenderSocks {
    pub main: UdpSocket,
    /// Receive-only sockets (broadcast + joined rendezvous group).
    pub extra: Vec<UdpSocket>,
}

impl SenderSocks {
    fn iter(&self) -> impl Iterator<Item = &UdpSocket> {
        std::iter::once(&self.main).chain(self.extra.iter())
    }
}

/// Open every socket the sender needs to talk to potential receivers.
pub fn open_sender_socks(
    net_config: &mut NetConfig,
    if_name: Option<&str>,
    disk_config: &DiskConfig,
    announce: bool,
) -> Option<SenderSocks> {
    net_config.net_if = Some(socklib::get_net_if(if_name));
    let net_if = net_config.net_if.as_ref().unwrap().clone();

    let receiver_port = socklib::RECEIVER_PORT(net_config.port_base);

    let main = socklib::make_socket(
        socklib::AddrType::Ucast,
        &net_if,
        None,
        socklib::SENDER_PORT(net_config.port_base),
    )?;

    // C: controlMcastAddr = 0; ttl==1 without -M means "use the subnet
    // broadcast address", anything else falls back to a rendezvous group.
    net_config.control_mcast_addr = socklib::clear_ip();
    if net_config.ttl == 1 && net_config.mcast_rdv.is_none() {
        net_config.control_mcast_addr = socklib::get_broadcast_address(&net_if, receiver_port);
    }
    let _ = socklib::set_socket_to_broadcast(&main);
    if net_config.control_mcast_addr.ip().is_unspecified() {
        net_config.control_mcast_addr =
            socklib::get_mcast_all_address(net_config.mcast_rdv.as_deref(), receiver_port);
        if socklib::is_mcast_address(&net_config.control_mcast_addr) {
            let _ = socklib::set_mcast_destination(&main, &net_if, &net_config.control_mcast_addr);
            let _ = socklib::set_ttl(&main, net_config.ttl);
        }
    }

    // Additional listeners. Receivers announce themselves with a CONNECT_REQ
    // aimed at the control address; a socket bound to the interface's unicast
    // address does not see broadcast or multicast packets, so bind (and join)
    // one socket per transport, exactly like C's startSender().
    let mut extra: Vec<UdpSocket> = Vec::new();

    if let Some(s) = socklib::make_socket(
        socklib::AddrType::Bcast,
        &net_if,
        None,
        socklib::SENDER_PORT(net_config.port_base),
    ) {
        extra.push(s);
    }

    if socklib::is_mcast_address(&net_config.control_mcast_addr) {
        if let Some(s) = socklib::make_socket(
            socklib::AddrType::Mcast,
            &net_if,
            Some(&net_config.control_mcast_addr),
            socklib::SENDER_PORT(net_config.port_base),
        ) {
            extra.push(s);
        }
    }

    if net_config.flags & senddata::FLAG_POINTOPOINT == 0
        && net_config.data_mcast_addr.ip().is_unspecified()
    {
        net_config.data_mcast_addr = socklib::get_default_mcast_address(&net_if);
        if announce {
            util::flprintf(&format!(
                "Using mcast address {}\n",
                net_config.data_mcast_addr.ip()
            ));
        }
    }

    // C: setPort(&dataMcastAddr, RECEIVER_PORT(portBase)) – unconditionally.
    // Getting this wrong (leaving port 0 behind when -m was given) makes every
    // data packet fail to send, silently.
    socklib::set_port(&mut net_config.data_mcast_addr, receiver_port);

    if announce {
        util::flprintf(&format!(
            "{}UDP sender for {} at {} on {}\n",
            if disk_config.pipe_name.is_some() {
                "Compressed "
            } else {
                ""
            },
            disk_config.file_name.as_deref().unwrap_or("(stdin)"),
            net_if.addr,
            net_if.name
        ));
        util::flprintf(&format!(
            "Broadcasting control to {}\n",
            net_config.control_mcast_addr.ip()
        ));
    }

    Some(SenderSocks { main, extra })
}

fn send_connection_reply(
    db: &Arc<std::sync::Mutex<ParticipantsDb>>,
    sock: &UdpSocket,
    net_config: &NetConfig,
    client: &std::net::SocketAddrV4,
    capabilities: u32,
    rcvbuf: u32,
) {
    let rcvbuf = if rcvbuf == 0 { 65536 } else { rcvbuf };
    if capabilities & protocol::CAP_BIG_ENDIAN == 0 {
        util::fatal(1, "Little endian protocol no longer supported");
    }
    let cl_nr = db.lock().unwrap().add_participant(
        client,
        capabilities,
        rcvbuf,
        net_config.flags & senddata::FLAG_POINTOPOINT != 0,
    );
    let reply = protocol::ConnectReply {
        cl_nr,
        block_size: net_config.block_size as i32,
        capabilities: net_config.capabilities,
        mcast: protocol::ip4_into_16(*net_config.data_mcast_addr.ip()),
    };
    if let Err(e) = sock.send_to(&reply.pack(), client) {
        util::flprintf(&format!("reply add new client: {}\n", e));
    }
}

pub fn send_hello(net_config: &NetConfig, sock: &UdpSocket, streaming: bool) {
    senddata::send_hello(net_config, sock, streaming);
}

/// Should the sender apply the `--min-receivers` / `--min-wait` / `--max-wait`
/// startup rules? The C version only enables them when at least one of the
/// three options was given (`firstConnectedP` stays NULL otherwise).
fn wait_for_receivers(net_config: &NetConfig) -> bool {
    net_config.min_receivers > 0
        || net_config.min_receivers_wait > 0
        || net_config.max_receivers_wait > 0
}

/// True when enough receivers have registered (and the minimum wait has
/// passed), or when `--max-wait` expired. C's `checkClientWait()`.
fn check_client_wait(
    db: &Arc<std::sync::Mutex<ParticipantsDb>>,
    net_config: &NetConfig,
    first_connected: Option<Instant>,
) -> bool {
    let fc = match first_connected {
        Some(t) => t,
        None => return false,
    };
    if db.lock().unwrap().nr_participants() == 0 {
        return false;
    }
    let elapsed = fc.elapsed();
    if net_config.max_receivers_wait > 0
        && elapsed >= Duration::from_secs(net_config.max_receivers_wait as u64)
    {
        return true;
    }
    if db.lock().unwrap().nr_participants() >= net_config.min_receivers.max(0) as usize {
        return net_config.min_receivers_wait <= 0
            || elapsed >= Duration::from_secs(net_config.min_receivers_wait as u64);
    }
    false
}

/// One round of C's `mainDispatcher()`: wait for receiver traffic / a key
/// stroke, with the periodic HELLO and the client-wait bookkeeping.
#[allow(clippy::too_many_arguments)]
fn dispatch_round(
    socks: &SenderSocks,
    db: &Arc<std::sync::Mutex<ParticipantsDb>>,
    net_config: &NetConfig,
    console: &mut Option<Console>,
    hello_interval: Option<Duration>,
    hello_tries: &mut i32,
    first_connected: &mut Option<Instant>,
    loop_start: Instant,
) -> i32 {
    use nix::sys::select::{select, FdSet};
    use nix::sys::time::TimeVal;
    use std::os::unix::io::{AsFd, AsRawFd};

    if first_connected.is_none() && db.lock().unwrap().nr_participants() > 0 {
        *first_connected = Some(Instant::now());
    }

    // Tick: how long to sleep before re-evaluating. Infinite only when there
    // is nothing that could expire.
    let tick: Option<Duration> = if let Some(iv) = hello_interval {
        Some(iv)
    } else if wait_for_receivers(net_config) || net_config.start_timeout > 0 {
        Some(Duration::from_secs(2))
    } else {
        None
    };

    let mut read_set = FdSet::new();
    let mut max_fd = 0i32;
    for s in socks.iter() {
        read_set.insert(s.as_fd());
        if s.as_raw_fd() >= max_fd {
            max_fd = s.as_raw_fd() + 1;
        }
    }

    let mut key_pressed = false;
    let ret = match console.as_mut() {
        Some(c) => match c.select_with_console(&mut max_fd, &read_set, tick.as_ref()) {
            Ok((n, kp)) => {
                key_pressed = kp;
                n
            }
            Err(nix::errno::Errno::EINTR) => 0,
            Err(_) => return DISPATCH_WAIT,
        },
        None => {
            let mut tv = tick.map(|d| TimeVal::new(d.as_secs() as i64, d.subsec_micros() as i64));
            match select(max_fd, Some(&mut read_set), None, None, tv.as_mut()) {
                Ok(n) => n,
                Err(nix::errno::Errno::EINTR) => 0,
                Err(_) => return DISPATCH_WAIT,
            }
        }
    };

    if ret == 0 && !key_pressed {
        // Nothing happened: HELLO time.
        if hello_interval.is_some() {
            send_hello(net_config, &socks.main, false);
            *hello_tries += 1;
            if net_config.autostart != 0 && *hello_tries > net_config.autostart {
                return DISPATCH_START;
            }
        }
        if check_client_wait(db, net_config, *first_connected) {
            return DISPATCH_START;
        }
        if net_config.start_timeout > 0
            && loop_start.elapsed() >= Duration::from_secs(net_config.start_timeout as u64)
        {
            util::flprintf("Start timeout: no transfer started\n");
            return DISPATCH_GIVE_UP;
        }
        return DISPATCH_WAIT;
    }

    if key_pressed {
        // select() reports the console readable on EOF too (e.g. when stdin is
        // /dev/null), so only a character that was really typed starts the
        // transfer. 'q' cancels, like C's restoreConsole(console, 1).
        let key = match console.as_mut() {
            Some(c) => c.take_key(),
            None => None,
        };
        match key {
            Some(b'q') => {
                util::flprintf("Cancelling transfer\n");
                std::process::exit(1);
            }
            Some(_) => return DISPATCH_START,
            None => {}
        }
    }

    // Drain whatever the receivers sent. One datagram per socket per round is
    // all C does, but reading until empty converges faster when many receivers
    // register at the same instant.
    for s in socks.iter() {
        if !read_set.contains(s.as_fd()) {
            continue;
        }
        let mut buf = [0u8; 256];
        for _ in 0..64 {
            match s.recv_from(&mut buf) {
                Ok((n, from)) => {
                    let from = match from {
                        std::net::SocketAddr::V4(v4) => v4,
                        _ => continue,
                    };
                    // C answers from `fd[0]`, the main (unicast) socket, and not
                    // from the socket that happened to receive the request: the
                    // broadcast/multicast listeners are bound to the broadcast /
                    // group address, so a reply sent from them would carry an
                    // unusable source address and the receiver would never accept
                    // it ("Start timeout: no sender found").
                    if handle_receiver_msg(&buf[..n], &from, &socks.main, db, net_config) {
                        return DISPATCH_START;
                    }
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.raw_os_error() == Some(libc::EINTR) =>
                {
                    break;
                }
                Err(_) => break,
            }
        }
    }

    // A disconnect may have emptied the participant list.
    // C calls checkClientWait whenever a receiver has connected; the
    // min/max receiver counts (default: min 0, no waits) decide when to start.
    if check_client_wait(db, net_config, *first_connected) {
        return DISPATCH_START;
    }
    DISPATCH_WAIT
}

/// Returns true when the transfer should start.
fn handle_receiver_msg(
    buf: &[u8],
    from: &std::net::SocketAddrV4,
    sock: &UdpSocket,
    db: &Arc<std::sync::Mutex<ParticipantsDb>>,
    net_config: &NetConfig,
) -> bool {
    if buf.len() < 4 {
        return false;
    }
    let opcode = u16::from_be_bytes([buf[0], buf[1]]);
    match opcode {
        protocol::CMD_CONNECT_REQ => {
            if buf.len() < protocol::CONNECT_REQ_SIZE {
                return false;
            }
            let req = protocol::ConnectReq::unpack(buf);
            let capabilities = protocol::CAP_BIG_ENDIAN | req.capabilities;
            send_connection_reply(db, sock, net_config, from, capabilities, req.rcvbuf);
            false
        }
        protocol::CMD_GO => true,
        protocol::CMD_DISCONNECT => {
            let mut db = db.lock().unwrap();
            let idx = db.lookup_participant(from);
            if idx >= 0 {
                db.remove_participant(idx as usize);
            }
            false
        }
        _ => false,
    }
}

/// True when data must be sent point-to-point instead of to the multicast
/// group. C's `isPointToPoint()`: explicit `-1`, or implicitly when there is
/// exactly one receiver and neither `-2` nor `--async` forbids it.
fn is_point_to_point(db: &Arc<std::sync::Mutex<ParticipantsDb>>, flags: u32) -> bool {
    if flags & senddata::FLAG_POINTOPOINT != 0 {
        return true;
    }
    if flags & (senddata::FLAG_NOPOINTOPOINT | senddata::FLAG_ASYNC) != 0 {
        return false;
    }
    db.lock().unwrap().nr_participants() == 1
}

pub fn start_sender(
    disk_config: &DiskConfig,
    net_config: &mut NetConfig,
    stat_config: &crate::sender::StatConfig,
    main_sock: &UdpSocket,
) -> i32 {
    let socks = SenderSocks {
        main: main_sock.try_clone().unwrap(),
        extra: Vec::new(),
    };
    start_sender_with_socks(disk_config, net_config, stat_config, socks)
}

pub fn start_sender_with_socks(
    disk_config: &DiskConfig,
    net_config: &mut NetConfig,
    stat_config: &crate::sender::StatConfig,
    mut socks: SenderSocks,
) -> i32 {
    let db = Arc::new(std::sync::Mutex::new(ParticipantsDb::new()));

    if net_config.requested_buf_size > 0 {
        socklib::set_send_buf(&socks.main, net_config.requested_buf_size);
    }

    // C assembles this in the sender start (`udps-negotiate.c:390`). HELLO
    // and every CONNECT_REPLY carry this pre-AND word; the per-participant
    // AND further down only feeds the endianness check and the debug print,
    // exactly like C's doTransfer.
    net_config.capabilities = senddata::sender_capabilities(net_config.flags);

    // A console is prepared unless -k/--nokbd, and it is stdin when the data
    // comes from a file (-f) /dev/tty otherwise (-p pipe or stdin data).
    let mut console = if net_config.flags & senddata::FLAG_NOKBD == 0 {
        Console::prepare(disk_config.file_name.is_some())
    } else {
        None
    };
    let nokbd = net_config.flags & senddata::FLAG_NOKBD != 0;

    for s in socks.iter() {
        s.set_nonblocking(true).ok();
    }

    send_hello(net_config, &socks.main, false);

    let hello_interval = if net_config.rexmit_hello_interval > 0 {
        Some(Duration::from_millis(
            net_config.rexmit_hello_interval as u64,
        ))
    } else if net_config.rexmit_hello_interval < 0 {
        None
    } else {
        Some(Duration::from_millis(DEFAULT_HELLO_INTERVAL_MS as u64))
    };

    let mut hello_tries = 0i32;
    let mut first_connected: Option<Instant> = None;
    let mut prompt_printed = false;
    let loop_start = Instant::now();

    let decision = loop {
        if !prompt_printed
            && !nokbd
            && (db.lock().unwrap().nr_participants() > 0
                || net_config.flags & senddata::FLAG_ASYNC != 0)
        {
            util::flprintf("Ready. Press any key to start sending data.\n");
            prompt_printed = true;
        }

        let r = dispatch_round(
            &socks,
            &db,
            net_config,
            &mut console,
            hello_interval,
            &mut hello_tries,
            &mut first_connected,
            loop_start,
        );

        if r != DISPATCH_WAIT {
            break r;
        }
    };

    // The keystroke that triggered the start (if any) has already been read,
    // and 'q' was handled there; all that is left is putting the terminal
    // back the way it was.
    if let Some(mut c) = console.take() {
        c.restore(false);
    }

    // Negotiation polled with non-blocking sockets; restore blocking mode for
    // the transfer phase so sends never fail with EAGAIN.
    socks.main.set_nonblocking(false).ok();

    // The extra listeners are not needed any more – and if the data channel
    // happens to be the group they joined, their presence would only make the
    // kernel hand copies of every data packet to a socket nobody reads.
    socks.extra.clear();

    if decision == DISPATCH_GIVE_UP {
        return 0;
    }

    if db.lock().unwrap().nr_participants() == 0 && net_config.flags & senddata::FLAG_ASYNC == 0 {
        util::flprintf("No participants... exiting\n");
        return 0;
    }

    do_transfer(
        disk_config,
        net_config,
        stat_config,
        &socks.main,
        &socks.extra,
        &db,
    );
    0
}

fn do_transfer(
    disk_config: &DiskConfig,
    net_config: &mut NetConfig,
    stat_config: &crate::sender::StatConfig,
    sock: &UdpSocket,
    extra_socks: &[UdpSocket],
    db: &Arc<std::sync::Mutex<ParticipantsDb>>,
) {
    let is_p2p = is_point_to_point(db, net_config.flags);

    if net_config.flags & senddata::FLAG_POINTOPOINT != 0 {
        let nr = db.lock().unwrap().nr_participants();
        if nr != 1 {
            util::fatal(
                1,
                &format!(
                    "pointopoint mode set, and {} participants instead of 1\n",
                    nr
                ),
            );
        }
    }

    net_config.rcvbuf = 0;
    {
        let db_lock = db.lock().unwrap();
        for i in 0..crate::participants::MAX_CLIENTS {
            if !db_lock.is_participant_valid(i) {
                continue;
            }
            if is_p2p {
                if let Some(ip) = db_lock.get_participant_ip(i) {
                    let port = net_config.data_mcast_addr.port();
                    net_config.data_mcast_addr = std::net::SocketAddrV4::new(*ip.ip(), port);
                }
            }
            net_config.capabilities &= db_lock.get_participant_capabilities(i);
            let p_rcvbuf = db_lock.get_participant_rcvbuf(i);
            if p_rcvbuf != 0 && (net_config.rcvbuf == 0 || net_config.rcvbuf > p_rcvbuf) {
                net_config.rcvbuf = p_rcvbuf;
            }
        }
    }

    if net_config.rcvbuf != 0 {
        socklib::set_send_buf(sock, net_config.rcvbuf);
    }

    if socklib::is_mcast_address(&net_config.data_mcast_addr) {
        if let Some(ref net_if) = net_config.net_if {
            let _ = socklib::set_mcast_destination(sock, net_if, &net_config.data_mcast_addr);
        }
    }

    util::flprintf(&format!(
        "Starting transfer: {:08x}\n",
        net_config.capabilities
    ));

    if net_config.capabilities & protocol::CAP_BIG_ENDIAN == 0 {
        util::fatal(1, "Peer with incompatible endianness");
    }

    let orig_in = crate::diskio::open_file(disk_config);

    // C udp-sender.c:635: the optional coprocess between the local reader
    // and the network sender (`-p`): the coprocess reads the input (or
    // stdin) and its stdout becomes our new input. C's open_pipe_sender
    // did fork+dup2+execvp; Command hands the child copies of the fds.
    let mut pipe = crate::diskio::open_pipe_sender(disk_config, &orig_in);
    let in_src = pipe.as_ref().map(|p| &p.read_end).unwrap_or(&orig_in);

    let print_uncompressed_pos = crate::statistics::should_print_uncompressed_pos(
        stat_config.print_uncompressed_pos,
        orig_in.raw_fd(),
        in_src.raw_fd(),
    );

    let mut stats = crate::statistics::SenderStats::new(
        orig_in.raw_fd(),
        stat_config.bw_period,
        stat_config.stat_period,
        print_uncompressed_pos,
        stat_config.no_progress,
    );

    let fifo = Arc::new(crate::fifo::Fifo::new(net_config.block_size as usize));

    let block_size = net_config.block_size;
    let slice_size = net_config.slice_size;

    // The network thread *borrows* the socket, the net config and the stats
    // (scoped thread) instead of being handed a `ptr::read` copy of them: the
    // raw copy left a dangling struct in this frame, which a second transfer
    // round (the old `-D` loop) would have read back as garbage - e.g. a
    // rendezvous address that silently turned into the default one.
    std::thread::scope(|s| {
        s.spawn(|| {
            senddata::spawn_net_sender(
                &fifo.data,
                &fifo.free_mem_queue,
                &fifo.buffer,
                fifo.data_buf_size,
                sock,
                extra_socks,
                net_config,
                db.clone(),
                &mut stats,
            );
        });
        crate::diskio::local_reader_fifo(&fifo, in_src);
    });

    if let Some(p) = &mut pipe {
        crate::process::wait_for_child(&mut p.child, "Pipe");
    }

    stats.display(block_size, slice_size, true);
    // The input handle (and the coprocess's pipe end) drops here, which
    // closes the file fd; stdin/stdout are the process's own handles.

    util::flprintf("Transfer complete.\x07\n");

    {
        let mut db_lock = db.lock().unwrap();
        for i in 0..crate::participants::MAX_CLIENTS {
            if db_lock.is_participant_valid(i) {
                db_lock.remove_participant(i);
            }
        }
    }
    eprintln!("\n");
}
