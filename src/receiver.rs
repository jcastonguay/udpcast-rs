use clap::Parser;
use std::net::{SocketAddrV4, UdpSocket};
use std::time::{Duration, Instant};

use crate::diskio::DiskConfig;
use crate::fifo::Fifo;
use crate::protocol;
use crate::receivedata::{self, ClientConfig};
use crate::senddata::NetConfig;
use crate::socklib;
use crate::statistics::ReceiverStats;

pub struct StatConfig {
    pub print_uncompressed_pos: i32,
    pub stat_period: i64,
    pub no_progress: bool,
}

const DEFAULT_STAT_PERIOD: i64 = 500_000;
const DEFAULT_RCVBUF: u32 = 1024 * 1024;

#[derive(Parser)]
#[command(name = "udp-receiver", about = "UDP file receiver")]
struct Cli {
    #[arg(short = 'f', long = "file")]
    file: Option<String>,

    #[arg(short = 'p', long = "pipe")]
    pipe: Option<String>,

    #[arg(short = 'P', long = "portbase", default_value = "9000")]
    port_base: u16,

    #[arg(short = 'i', long = "interface")]
    interface: Option<String>,

    #[arg(short = 't', long = "ttl", default_value = "1")]
    ttl: i32,

    #[arg(short = 'M', long = "mcast-rdv-address")]
    mcast_rdv_address: Option<String>,

    #[arg(short = 'd', long = "passive")]
    passive: bool,

    #[arg(short = 'n', long = "nosync")]
    nosync: bool,

    #[arg(short = 'y', long = "sync")]
    sync_mode: bool,

    #[arg(short = 'b', long = "rcvbuf")]
    rcvbuf: Option<String>,

    #[arg(short = 'k', long = "nokbd")]
    nokbd: bool,

    #[arg(short = 'w', long = "exit-wait", default_value = "500")]
    exit_wait: i32,

    #[arg(short = 's', long = "start-timeout", default_value = "0")]
    start_timeout: i32,

    #[arg(long = "receive-timeout", default_value = "0")]
    receive_timeout: i32,

    #[arg(short = 'l', long = "log")]
    log: Option<String>,

    #[arg(long = "no-progress")]
    no_progress: bool,

    #[arg(short = 'x', long = "print-uncompressed-position", default_value = "-1")]
    print_uncompressed_pos: i32,

    #[arg(short = 'z', long = "stat-period", default_value = "0")]
    stat_period: i32,

    #[arg(short = 'Z', long = "ignore-lost-data")]
    ignore_lost_data: bool,
}

pub fn run_receiver() {
    let cli = Cli::parse();

    if let Some(path) = cli.log.as_deref() {
        crate::util::init_log(path);
    }

    let mut disk_config = DiskConfig {
        orig_out_file: false,
        file_name: cli.file.clone(),
        pipe_name: cli.pipe.clone(),
        flags: 0,
    };

    if cli.nosync {
        disk_config.flags |= crate::diskio::FLAG_NOSYNC;
    }
    if cli.sync_mode {
        disk_config.flags |= crate::diskio::FLAG_SYNC;
    }

    let mut flags: u32 = 0;
    if cli.passive {
        flags |= crate::senddata::FLAG_PASSIVE;
    }
    if cli.nokbd {
        flags |= crate::senddata::FLAG_NOKBD;
    }
    if cli.ignore_lost_data {
        flags |= crate::senddata::FLAG_IGNORE_LOST_DATA;
    }

    let requested_buf_size = cli
        .rcvbuf
        .as_deref()
        .map(|s| socklib::parse_size(s) as u32)
        .unwrap_or(0);

    let mut stat_period = DEFAULT_STAT_PERIOD;
    if cli.stat_period > 0 {
        stat_period = cli.stat_period as i64 * 1000;
    }

    let stat_config = StatConfig {
        print_uncompressed_pos: cli.print_uncompressed_pos,
        stat_period,
        no_progress: cli.no_progress,
    };

    let mut net_config = NetConfig {
        net_if: None,
        port_base: cli.port_base,
        block_size: 1456,
        slice_size: 16,
        control_mcast_addr: socklib::clear_ip(),
        data_mcast_addr: socklib::clear_ip(),
        mcast_rdv: cli.mcast_rdv_address.clone(),
        ttl: cli.ttl,
        flags,
        capabilities: 0,
        min_slice_size: 16,
        default_slice_size: 0,
        max_slice_size: 1024,
        rcvbuf: requested_buf_size,
        rexmit_hello_interval: 0,
        autostart: 0,
        requested_buf_size,
        min_receivers: 0,
        max_receivers_wait: 0,
        min_receivers_wait: 0,
        retries_until_drop: 200,
        rehello_offset: 50,
        start_timeout: cli.start_timeout,
        discovery: crate::senddata::Discovery::Doubling,
        fec_stripes: 0,
        fec_redundancy: 0,
        fec_stripesize: 0,
        max_bitrate: None,
        autorate: false,
    };

    eprintln!("Udp-receiver 1.0.0\n");

    let ret = start_receiver(
        &disk_config,
        &mut net_config,
        &stat_config,
        cli.interface.as_deref(),
        cli.exit_wait,
        cli.receive_timeout,
    );
    if ret < 0 {
        eprintln!("Receiver error");
    }
    std::process::exit(ret);
}

fn recv_from_any_negotiate(
    socks: &[Option<UdpSocket>],
    buf: &mut [u8],
    port_base: u16,
    timeout: Option<Duration>,
) -> Option<(usize, std::net::SocketAddr)> {
    use std::os::unix::io::{AsRawFd, BorrowedFd};
    use nix::sys::select::{select, FdSet};
    use nix::sys::time::TimeVal;

    let fds: Vec<(usize, i32)> = socks.iter().enumerate()
        .filter_map(|(i, s)| s.as_ref().map(|s| (i, s.as_raw_fd())))
        .collect();
    if fds.is_empty() {
        return None;
    }

    loop {
        let mut read_set = FdSet::new();
        let mut max_fd = 0i32;
        for &(_, fd) in &fds {
            let bfd = unsafe { BorrowedFd::borrow_raw(fd) };
            read_set.insert(bfd);
            if fd >= max_fd {
                max_fd = fd + 1;
            }
        }
        let mut tv = timeout.map(|d| TimeVal::new(d.as_secs() as i64, d.subsec_micros() as i64));
        match select(max_fd, Some(&mut read_set), None, None, tv.as_mut()) {
            Ok(0) => return None,
            Ok(_) => {
                for &(idx, fd) in &fds {
                    let bfd = unsafe { BorrowedFd::borrow_raw(fd) };
                    if read_set.contains(bfd) {
                        if let Some(sock) = &socks[idx] {
                            sock.set_nonblocking(true).ok();
                            match sock.recv_from(buf) {
                                Ok((n, from)) => {
                                    let from_v4 = match from {
                                        std::net::SocketAddr::V4(v4) => v4,
                                        _ => continue,
                                    };
                                    if from_v4.port() == socklib::SENDER_PORT(port_base)
                                        || from_v4.port() == socklib::RECEIVER_PORT(port_base)
                                    {
                                        return Some((n, from));
                                    }
                                }
                                Err(_) => continue,
                            }
                        }
                    }
                }
            }
            Err(_) => return None,
        }
    }
}

fn start_receiver(
    disk_config: &DiskConfig,
    net_config: &mut NetConfig,
    stat_config: &StatConfig,
    if_name: Option<&str>,
    exit_wait: i32,
    receive_timeout: i32,
) -> i32 {
    net_config.net_if = Some(socklib::get_net_if(if_name));
    let net_if = net_config.net_if.as_ref().unwrap().clone();

    let s_ucast = match socklib::make_socket(
        socklib::AddrType::Ucast,
        &net_if,
        None,
        socklib::RECEIVER_PORT(net_config.port_base),
    ) {
        Some(s) => s,
        None => {
            eprintln!("Could not open unicast socket");
            return -1;
        }
    };

    let s_bcast = match socklib::make_socket(
        socklib::AddrType::Bcast,
        &net_if,
        None,
        socklib::RECEIVER_PORT(net_config.port_base),
    ) {
        Some(s) => s,
        None => {
            eprintln!("Could not open broadcast socket");
            return -1;
        }
    };

    let mut s_mcast_ctrl: Option<UdpSocket> = None;

    if net_config.ttl == 1 && net_config.mcast_rdv.is_none() {
        let bcast_addr =
            socklib::get_broadcast_address(&net_if, socklib::SENDER_PORT(net_config.port_base));
        net_config.control_mcast_addr = bcast_addr;
        let _ = socklib::set_socket_to_broadcast(&s_ucast);
    } else {
        let mcast_addr = socklib::get_mcast_all_address(
            net_config.mcast_rdv.as_deref(),
            socklib::SENDER_PORT(net_config.port_base),
        );
        net_config.control_mcast_addr = mcast_addr;
        if socklib::is_mcast_address(&mcast_addr) {
            let _ = socklib::set_mcast_destination(&s_ucast, &net_if, &mcast_addr);
            let _ = socklib::set_ttl(&s_ucast, net_config.ttl);
            s_mcast_ctrl = socklib::make_socket(
                socklib::AddrType::Mcast,
                &net_if,
                Some(&mcast_addr),
                socklib::RECEIVER_PORT(net_config.port_base),
            );
        }
    }

    net_config.data_mcast_addr = socklib::clear_ip();

    // Late-join sniffer (CAP_LATE_JOIN): join the default data multicast
    // group while negotiating. If our one-shot CONNECT_REPLY is lost, the
    // sender re-sends it (while the first slice is un-acked) -- but to
    // catch up we must know which slices were already sent, and that can
    // only be learned from the data-group traffic (REQACK/DATA carry the
    // slice number, size and rxmit id). The sniffer is closed again when
    // the transfer actually starts.
    let mut s_sniff: Option<UdpSocket> =
        if (net_config.flags & crate::senddata::FLAG_POINTOPOINT) == 0 {
            let data_addr = socklib::get_default_mcast_address(&net_if);
            if socklib::is_mcast_address(&data_addr) {
                socklib::make_socket(
                    socklib::AddrType::Mcast,
                    &net_if,
                    Some(&data_addr),
                    socklib::RECEIVER_PORT(net_config.port_base),
                )
            } else {
                None
            }
        } else {
            None
        };
    // The group the sniffer is currently joined to. The sender may use a
    // non-default data group (-m); the HELLO on the control channel says
    // which, so the sniffer is re-aimed when one arrives.
    let mut sniff_group = s_sniff
        .as_ref()
        .and_then(|s| s.local_addr().ok())
        .and_then(|a| match a {
            std::net::SocketAddr::V4(v4) => Some(*v4.ip()),
            _ => None,
        });

    crate::util::flprintf(&format!(
        "{}UDP receiver for {} at {} on {}\n",
        if disk_config.pipe_name.is_some() {
            "Compressed "
        } else {
            ""
        },
        disk_config
            .file_name
            .as_deref()
            .unwrap_or("(stdout)"),
        net_if.addr,
        net_if.name
    ));

    let mut client_config = ClientConfig {
        socks: vec![Some(s_ucast), Some(s_bcast), s_mcast_ctrl, s_sniff],
        server_addr: socklib::clear_ip(),
        control_addr: net_config.control_mcast_addr,
        client_number: 0,
        is_started: false,
        sender_is_newgen: false,
        exit_wait_ms: exit_wait.max(0) as u64,
        completed_slices: Vec::new(),
        end_marker_seen: false,
        receive_timeout_secs: receive_timeout.max(0) as u64,
        late_slices: Vec::new(),
    };

    let rcv_buf_size = if net_config.requested_buf_size > 0 {
        net_config.requested_buf_size
    } else {
        DEFAULT_RCVBUF
    };
    for sock_opt in &client_config.socks {
        if let Some(sock) = sock_opt {
            socklib::set_rcv_buf(sock, rcv_buf_size);
        }
    }

    // CONNECT_REQ is repeated once a second while the receiver has no reply:
    // a single request can be lost (and with `-H -1` the sender never asks
    // again), which would otherwise leave the receiver waiting forever.
    const CONNECT_REQ_RETRY: Duration = Duration::from_secs(1);
    let mut last_connect_req: Option<Instant> = None;
    let mut have_server_address = false;
    // -s/--start-timeout: give up when the sender stays silent this long
    // (C: udpc_selectSock() with startTimeout returns -1 -> "Receiver error").
    let start_timeout = net_config.start_timeout.max(0) as u64;
    let mut last_activity = Instant::now();
    // Slices observed on the data group before our reply arrived (late-join
    // catch-up list): (slice_no, bytes, last seen rxmit).
    let mut late_slices: Vec<(i32, i32, i32)> = Vec::new();

    loop {
        let need_connect_req = match last_connect_req {
            None => true,
            Some(t) => t.elapsed() >= CONNECT_REQ_RETRY,
        };
        if need_connect_req && (net_config.flags & crate::senddata::FLAG_PASSIVE) == 0 {
            let msg = protocol::ConnectReq {
                capabilities: protocol::RECEIVER_CAPABILITIES,
                rcvbuf: socklib::get_rcv_buf(client_config.socks[0].as_ref().unwrap()),
            };
            let packed = msg.pack();
            let sock = client_config.socks[0].as_ref().unwrap();
            if have_server_address {
                let _ = sock.send_to(&packed, &client_config.server_addr);
            } else {
                let _ = socklib::set_socket_to_broadcast(sock);
                let _ = sock.send_to(&packed, &net_config.control_mcast_addr);
            }
            last_connect_req = Some(Instant::now());
        }

        have_server_address = false;

        let mut buf = [0u8; 4096];

        // Wake up at least once a second (to repeat the CONNECT_REQ) but never
        // later than the remaining start timeout.
        let timeout = if start_timeout > 0 {
            Duration::from_secs(start_timeout)
                .saturating_sub(last_activity.elapsed())
                .min(CONNECT_REQ_RETRY)
        } else {
            CONNECT_REQ_RETRY
        };
        let timeout = Some(timeout);

        let (msglen, from) = match recv_from_any_negotiate(&client_config.socks, &mut buf, net_config.port_base, timeout) {
            Some(r) => {
                last_activity = Instant::now();
                r
            }
            None => {
                if start_timeout > 0
                    && last_activity.elapsed() >= Duration::from_secs(start_timeout)
                {
                    crate::util::flprintf("Start timeout: no sender found\n");
                    // The sender registered us from our CONNECT_REQ but its
                    // (re)sponses to us were all lost. Tell it to drop us,
                    // otherwise it would stall every slice of the whole
                    // transfer waiting for our ready/answer until the
                    // retry-until-drop budget runs out.
                    crate::receivedata::send_disconnect(&mut client_config, 1);
                    return -1;
                }
                continue;
            }
        };
        let from = match from {
            std::net::SocketAddr::V4(v4) => v4,
            _ => continue,
        };

        if from.port() != socklib::SENDER_PORT(net_config.port_base) {
            continue;
        }

        if msglen < 4 {
            continue;
        }

        let opcode = u16::from_be_bytes([buf[0], buf[1]]);

        match opcode {
            protocol::CMD_CONNECT_REPLY => {
                if msglen < protocol::CONNECT_REPLY_SIZE {
                    continue;
                }
                let reply = protocol::ConnectReply::unpack(&buf);
                client_config.client_number = reply.cl_nr;
                net_config.block_size = reply.block_size as u32;
                client_config.server_addr = from;

                crate::util::flprintf(&format!(
                    "received message, cap={:08x}\n",
                    reply.capabilities
                ));
                if reply.capabilities & protocol::CAP_NEW_GEN != 0 {
                    client_config.sender_is_newgen = true;
                    let ip = protocol::ip4_from_16(&reply.mcast);
                    net_config.data_mcast_addr = SocketAddrV4::new(ip, 0);
                }
                if client_config.client_number == -1 {
                    crate::util::fatal(1, "Too many clients already connected\n");
                }
                break;
            }
            protocol::CMD_HELLO_STREAMING | protocol::CMD_HELLO_NEW | protocol::CMD_HELLO => {
                // The sender is asking for participants: answer right away.
                last_connect_req = None;
                if opcode == protocol::CMD_HELLO_STREAMING {
                    net_config.flags |= crate::senddata::FLAG_STREAMING;
                }
                if msglen >= protocol::HELLO_SIZE {
                    let hello = protocol::Hello::unpack(&buf);
                    if hello.capabilities & protocol::CAP_NEW_GEN != 0 {
                        client_config.sender_is_newgen = true;
                        let ip = protocol::ip4_from_16(&hello.mcast);
                        net_config.data_mcast_addr = SocketAddrV4::new(ip, 0);
                        net_config.block_size = hello.block_size as u32;
                        if hello.capabilities & protocol::CAP_ASYNC != 0 {
                            net_config.flags |= crate::senddata::FLAG_PASSIVE;
                        }
                        if (net_config.flags & crate::senddata::FLAG_PASSIVE) != 0 {
                            break;
                        }
                    }
                }
                client_config.server_addr = from;
                have_server_address = true;
                // Re-aim the late-join sniffer at the data group the HELLO
                // advertises (it may differ from the default). The HELLO is
                // sent on the control channel, so it can arrive even when
                // the unicast CONNECT_REPLY is lost.
                let want = net_config.data_mcast_addr;
                if (net_config.flags & crate::senddata::FLAG_POINTOPOINT) == 0
                    && socklib::is_mcast_address(&want)
                    && sniff_group != Some(*want.ip())
                {
                    s_sniff = socklib::make_socket(
                        socklib::AddrType::Mcast,
                        &net_if,
                        Some(&want),
                        socklib::RECEIVER_PORT(net_config.port_base),
                    );
                    sniff_group = s_sniff
                        .as_ref()
                        .and_then(|s| s.local_addr().ok())
                        .and_then(|a| match a {
                            std::net::SocketAddr::V4(v4) => Some(*v4.ip()),
                            _ => None,
                        });
                    if client_config.socks.len() > 3 {
                        if let Some(sn) = s_sniff.as_ref() {
                            if let Ok(c) = sn.try_clone() {
                                client_config.socks[3] = Some(c);
                            }
                        }
                    }
                }
                continue;
            }
            protocol::CMD_CONNECT_REQ => {
                continue;
            }
            protocol::CMD_REQACK | protocol::CMD_DATA | protocol::CMD_FEC => {
                // Only reachable through the late-join sniffer: the
                // transfer is already in progress. Record what we see so
                // that when our (re)sent CONNECT_REPLY arrives we can
                // re-request every slice sent before we joined.
                let (slice_no, bytes, rxmit) = match opcode {
                    protocol::CMD_REQACK => {
                        let m = protocol::Reqack::unpack(&buf);
                        (m.slice_no, m.bytes, m.rxmit)
                    }
                    protocol::CMD_DATA => {
                        let m = protocol::DataBlock::unpack(&buf);
                        (m.slice_no, m.bytes, 0)
                    }
                    _ => {
                        let m = protocol::FecBlock::unpack(&buf);
                        (m.slice_no, m.bytes, 0)
                    }
                };
                if slice_no >= 0 {
                    match late_slices.iter_mut().find(|e| e.0 == slice_no) {
                        Some(e) => {
                            e.1 = bytes;
                            if rxmit > e.2 {
                                e.2 = rxmit;
                            }
                        }
                        None => late_slices.push((slice_no, bytes, rxmit)),
                    }
                }
                continue;
            }
            _ => {
                crate::util::fatal(
                    1,
                    &format!(
                        "Bad server reply {:04x}. Other transfer in progress?\n",
                        opcode
                    ),
                );
            }
        }
    }

    // Negotiation is over: close the late-join sniffer (the data phase opens
    // its own socket for the data group learned from the reply, which may
    // differ from the default group when the sender used -m). A duplicate
    // socket in the same group would make every data packet arrive twice.
    if client_config.socks.len() > 3 {
        client_config.socks.pop();
    }
    client_config.late_slices = std::mem::take(&mut late_slices);
    if !client_config.late_slices.is_empty() {
        crate::util::flprintf(&format!(
            "Late join: re-requesting {} slice(s) missed during start\n",
            client_config.late_slices.len()
        ));
    }

    crate::util::flprintf(&format!(
        "Connected as #{} to {}\n",
        client_config.client_number, client_config.server_addr
    ));

    let my_ip = socklib::get_my_address(&net_if);
    if !socklib::ip_is_zero(&net_config.data_mcast_addr)
        && net_config.data_mcast_addr.ip() != my_ip.ip()
        && (socklib::ip_is_zero(&net_config.control_mcast_addr)
            || net_config.data_mcast_addr.ip() != net_config.control_mcast_addr.ip())
    {
        crate::util::flprintf(&format!(
            "Listening to multicast on {}\n",
            net_config.data_mcast_addr.ip()
        ));
        let s_mcast_data = socklib::make_socket(
            socklib::AddrType::Mcast,
            &net_if,
            Some(&net_config.data_mcast_addr),
            socklib::RECEIVER_PORT(net_config.port_base),
        );
        if client_config.socks.len() > 2 {
            client_config.socks[2] = s_mcast_data;
        }
    }

    for sock_opt in &client_config.socks {
        if let Some(sock) = sock_opt {
            socklib::set_rcv_buf(sock, rcv_buf_size);
        }
    }

    let orig_out = crate::diskio::open_out_file(disk_config);
    let mut pipe_pid = 0i32;
    let piped_out = crate::diskio::open_pipe_receiver(orig_out, disk_config, &mut pipe_pid);

    let print_uncompressed_pos = crate::statistics::should_print_uncompressed_pos(
        stat_config.print_uncompressed_pos,
        orig_out,
        piped_out,
    );

    let mut stats = ReceiverStats::new(
        orig_out,
        stat_config.stat_period,
        print_uncompressed_pos,
        stat_config.no_progress,
    );

    let fifo = std::sync::Arc::new(Fifo::new(net_config.block_size as usize));
    let fifo_clone = fifo.clone();

    let mut client_config_for_thread = ClientConfig {
        socks: client_config.socks.iter().map(|s| s.as_ref().and_then(|orig| orig.try_clone().ok())).collect(),
        server_addr: client_config.server_addr,
        control_addr: client_config.control_addr,
        client_number: client_config.client_number,
        is_started: client_config.is_started,
        sender_is_newgen: client_config.sender_is_newgen,
        exit_wait_ms: client_config.exit_wait_ms,
        completed_slices: Vec::new(),
        end_marker_seen: false,
        receive_timeout_secs: client_config.receive_timeout_secs,
        late_slices: client_config.late_slices.clone(),
    };
    let net_config_clone = unsafe { std::ptr::read(net_config as *const NetConfig) };
    let stat_period = stat_config.stat_period;
    let no_progress = stat_config.no_progress;

    let receiver_thread = std::thread::spawn(move || {
        let mut stats_local = ReceiverStats::new(
            orig_out,
            stat_period,
            print_uncompressed_pos,
            no_progress,
        );
        receivedata::spawn_net_receiver(&fifo_clone, &mut client_config_for_thread, &net_config_clone, &mut stats_local);
        stats_local.display(true);
    });

    crate::diskio::writer(&fifo, piped_out);

    let _ = receiver_thread.join();

    if pipe_pid != 0 {
        let _ = crate::process::wait_for_process(pipe_pid, "Pipe");
    }

    stats.display(true);

    receivedata::send_disconnect(&mut client_config, 0);

    0
}
