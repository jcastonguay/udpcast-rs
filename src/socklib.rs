use crate::util;
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};

pub const RECEIVER_PORT: fn(u16) -> u16 = |x| x;
pub const SENDER_PORT: fn(u16) -> u16 = |x| x + 1;

#[derive(Debug, Clone)]
pub struct NetIf {
    pub addr: Ipv4Addr,
    pub bcast: Ipv4Addr,
    pub name: String,
    pub index: i32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AddrType {
    Ucast,
    Mcast,
    Bcast,
}

pub fn make_sock_addr(hostname: &str, port: u16) -> io::Result<SocketAddrV4> {
    let addr: Ipv4Addr = if hostname.is_empty() {
        Ipv4Addr::UNSPECIFIED
    } else {
        hostname
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid IP address"))?
    };
    Ok(SocketAddrV4::new(addr, port))
}

pub fn get_my_address(net_if: &NetIf) -> SocketAddrV4 {
    SocketAddrV4::new(net_if.addr, 0)
}

pub fn get_broadcast_address(net_if: &NetIf, port: u16) -> SocketAddrV4 {
    let mut addr = SocketAddrV4::new(net_if.bcast, port);
    if net_if.bcast.is_unspecified() {
        if (u32::from(net_if.addr) & 0xff000000) == 0x7f000000 {
            addr = SocketAddrV4::new(net_if.addr, port);
        }
    }
    addr
}

pub fn get_mcast_all_address(address: Option<&str>, port: u16) -> SocketAddrV4 {
    let ip: Ipv4Addr = match address {
        Some(a) if !a.is_empty() => a.parse().unwrap_or_else(|_| Ipv4Addr::new(224, 0, 0, 1)),
        _ => Ipv4Addr::new(224, 0, 0, 1),
    };
    SocketAddrV4::new(ip, port)
}

pub fn do_send(sock: &UdpSocket, message: &[u8], to: &SocketAddrV4) -> io::Result<usize> {
    sock.send_to(message, to)
}

pub fn do_receive(
    sock: &UdpSocket,
    buf: &mut [u8],
    port_base: u16,
) -> io::Result<(usize, SocketAddrV4)> {
    let (n, from) = sock.recv_from(buf)?;
    let port = from.port();
    if port != RECEIVER_PORT(port_base) && port != SENDER_PORT(port_base) {
        eprintln!("Bad message from port {}:{}", from.ip(), port);
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Bad port"));
    }
    let from_v4 = match from {
        std::net::SocketAddr::V4(v4) => v4,
        std::net::SocketAddr::V6(_) => {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected IPv4"));
        }
    };
    Ok((n, from_v4))
}

pub fn get_send_buf(sock: &UdpSocket) -> io::Result<u32> {
    let s: socket2::Socket = sock.try_clone()?.into();
    s.send_buffer_size().map(|v| v as u32)
}

/// Bytes currently queued in the socket's send buffer, or -1 on error.
/// Mirrors C `auto-rate.c`'s `getCurrentQueueLength()`.
///
/// TIOCOUTQ has no safe binding in any available crate (checked nix 0.31,
/// rustix 1.1, socket2 0.6 — rustix's own `ioctl()` is `unsafe fn`), so this
/// stays a minimal direct call with the unsafe confined to one block.
pub fn get_send_queue(sock: &UdpSocket) -> i32 {
    let mut length: libc::c_int = 0;
    // SAFETY: `TIOCOUTQ` is a read-only (getter) ioctl: the kernel only
    // writes the single `c_int` in `length`. `sock` is a valid, open UDP
    // socket for the whole call.
    let r = unsafe {
        use std::os::unix::io::AsRawFd;
        libc::ioctl(sock.as_raw_fd(), libc::TIOCOUTQ, &mut length)
    };
    if r < 0 {
        -1
    } else {
        length
    }
}

pub fn set_send_buf(sock: &UdpSocket, bufsize: u32) {
    match sock.try_clone() {
        Ok(cloned) => {
            let s: socket2::Socket = cloned.into();
            if let Err(e) = s.set_send_buffer_size(bufsize as usize) {
                eprintln!("Set send buffer: {}", e);
            }
        }
        Err(e) => eprintln!("Set send buffer: {}", e),
    }
}

pub fn get_rcv_buf(sock: &UdpSocket) -> u32 {
    match sock.try_clone() {
        Ok(cloned) => {
            let s: socket2::Socket = cloned.into();
            s.recv_buffer_size().unwrap_or(0) as u32
        }
        Err(_) => 0,
    }
}

pub fn set_rcv_buf(sock: &UdpSocket, bufsize: u32) {
    match sock.try_clone() {
        Ok(cloned) => {
            let s: socket2::Socket = cloned.into();
            if let Err(e) = s.set_recv_buffer_size(bufsize as usize) {
                eprintln!("Set receiver buffer: {}", e);
            }
        }
        Err(e) => eprintln!("Set receiver buffer: {}", e),
    }
}

pub fn set_socket_to_broadcast(sock: &UdpSocket) -> io::Result<()> {
    sock.set_broadcast(true)
}

pub fn set_ttl(sock: &UdpSocket, ttl: i32) -> io::Result<()> {
    sock.set_ttl(ttl as u32)
}

/// Pick the outbound interface for multicast packets (`IP_MULTICAST_IF`).
/// Mirrors C `socklib.c`'s `setMcastDestination()`.
///
/// The C original sends an `ip_mreqn` with `imr_ifindex` (the exact
/// interface). Here we use socket2's `set_multicast_if_v4`, which passes the
/// interface's IP address and lets the kernel resolve the device
/// (`ip_dev_find`) — equivalent whenever that IP is not shared by several
/// interfaces, and the whole thing is safe Rust; the kernel accepts the
/// 4-byte form on all modern kernels (verified in v4.4 … v7.1 sources and on
/// a live 7.1 kernel), and the resulting `mc_index` is what `udp.c` uses to
/// pick the egress device for multicast sends.
pub fn set_mcast_destination(
    sock: &UdpSocket,
    net_if: &NetIf,
    _addr: &SocketAddrV4,
) -> io::Result<()> {
    let s: socket2::Socket = sock.try_clone()?.into();
    s.set_multicast_if_v4(&net_if.addr)
}

pub fn is_full_duplex(_sock: &UdpSocket, _ifname: &str) -> i32 {
    -1
}

pub fn get_net_if(wanted: Option<&str>) -> NetIf {
    use nix::ifaddrs::getifaddrs;
    use std::collections::HashMap;

    let wanted: Option<String> = wanted
        .map(|s| s.to_string())
        .or_else(|| std::env::var("IFNAME").ok());

    let mut best_goodness = 0;
    let mut best_name = String::new();
    let mut best_addr = Ipv4Addr::UNSPECIFIED;
    let mut best_bcast = Ipv4Addr::UNSPECIFIED;
    let mut best_index = 0i32;

    let mut seen: HashMap<String, (Ipv4Addr, Ipv4Addr, i32)> = HashMap::new();

    let ifaddrs = match getifaddrs() {
        Ok(it) => it,
        Err(e) => {
            eprintln!("getifaddrs failed: {}", e);
            std::process::exit(1);
        }
    };
    for ifa in ifaddrs {
        let Some(addr) = ifa.address.as_ref().and_then(|a| a.as_sockaddr_in()) else {
            continue;
        };
        let bcast = ifa
            .broadcast
            .as_ref()
            .and_then(|b| b.as_sockaddr_in())
            .map(|s| s.ip())
            .unwrap_or(Ipv4Addr::UNSPECIFIED);
        let idx = nix::net::if_::if_nametoindex(ifa.interface_name.as_str())
            .map(|i| i as i32)
            .unwrap_or(0);
        seen.entry(ifa.interface_name)
            .or_insert((addr.ip(), bcast, idx));
    }

    if seen.is_empty() {
        eprintln!("getifaddrs failed: no IPv4 interfaces found");
        std::process::exit(1);
    }

    for (name, (addr, bcast, idx)) in &seen {
        let mut goodness;

        if let Some(ref w) = wanted {
            if let Ok(wa) = w.parse::<Ipv4Addr>() {
                if addr == &wa {
                    goodness = 8;
                } else {
                    continue;
                }
            } else if w == name {
                goodness = 12;
            } else if name.starts_with(w.as_str()) {
                goodness = 7;
            } else {
                continue;
            }
        } else {
            if addr.is_unspecified() {
                goodness = 1;
            } else if addr.is_loopback() {
                goodness = 2;
            } else if name == "eth0" || name == "en0" {
                goodness = 6;
            } else if name.starts_with("eth0:") {
                goodness = 5;
            } else if name.starts_with("eth") || name.starts_with("en") {
                goodness = 4;
            } else {
                goodness = 3;
            }
        }

        goodness *= 2;
        if !bcast.is_unspecified() {
            goodness += 1;
        }

        if goodness > best_goodness {
            best_goodness = goodness;
            best_name = name.clone();
            best_addr = *addr;
            best_bcast = *bcast;
            best_index = *idx;
        }
    }

    if best_name.is_empty() {
        eprintln!("No suitable network interface found");
        std::process::exit(1);
    }

    NetIf {
        addr: best_addr,
        bcast: best_bcast,
        name: best_name,
        index: best_index,
    }
}

pub fn make_socket(
    addr_type: AddrType,
    net_if: &NetIf,
    mcast_addr: Option<&SocketAddrV4>,
    port: u16,
) -> Option<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};

    let bind_addr = match addr_type {
        AddrType::Ucast => SocketAddrV4::new(net_if.addr, port),
        AddrType::Bcast => {
            let ba = get_broadcast_address(net_if, port);
            if ba.ip().is_unspecified() {
                return None;
            }
            ba
        }
        AddrType::Mcast => {
            let ip = mcast_addr.map(|a| *a.ip()).unwrap_or(Ipv4Addr::UNSPECIFIED);
            SocketAddrV4::new(ip, port)
        }
    };

    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).ok()?;
    // Just SO_REUSEADDR, like the C implementation. Enabling SO_REUSEPORT
    // unconditionally would be wrong: with several sockets bound to the same
    // port (the sender's unicast socket plus its broadcast and multicast
    // listeners) the kernel hashes incoming datagrams over all of them, so a
    // process that reads only one of these sockets silently loses receiver
    // acknowledgements. Only fall back to SO_REUSEPORT if the plain bind is
    // refused because this exact address/port is already in use.
    sock.set_reuse_address(true).ok()?;
    sock.set_nonblocking(false).ok();
    if sock.bind(&socket2::SockAddr::from(bind_addr)).is_err() {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            use nix::sys::socket::setsockopt;
            use nix::sys::socket::sockopt::ReusePort;
            let _ = setsockopt(&sock, ReusePort, &true);
        }
        sock.bind(&socket2::SockAddr::from(bind_addr)).ok()?;
    }

    let udp: UdpSocket = sock.into();

    if addr_type == AddrType::Mcast {
        let mcast_ip = bind_addr.ip();
        if mcast_ip.is_multicast() {
            let _ = udp.join_multicast_v4(mcast_ip, &net_if.addr);
        }
    }

    Some(udp)
}

pub fn print_my_ip(net_if: &NetIf) {
    eprint!("{}", net_if.addr);
}

pub fn ip_is_zero(addr: &SocketAddrV4) -> bool {
    addr.ip().is_unspecified()
}

pub fn set_port(addr: &mut SocketAddrV4, port: u16) {
    *addr = SocketAddrV4::new(*addr.ip(), port);
}

pub fn clear_ip() -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)
}

pub fn set_ip_from_string(ip: &str) -> SocketAddrV4 {
    let addr: Ipv4Addr = ip.parse().unwrap_or(Ipv4Addr::UNSPECIFIED);
    SocketAddrV4::new(addr, 0)
}

pub fn get_default_mcast_address(net_if: &NetIf) -> SocketAddrV4 {
    let mut ip = u32::from(net_if.addr);
    ip &= 0x07ffffff;
    ip |= 0xe8000000;
    SocketAddrV4::new(Ipv4Addr::from(ip), 0)
}

pub fn is_mcast_address(addr: &SocketAddrV4) -> bool {
    addr.ip().is_multicast()
}

pub fn zero_sock_array(nr: usize) -> Vec<Option<UdpSocket>> {
    (0..nr).map(|_| None).collect()
}

pub fn parse_size(size_string: &str) -> u64 {
    let s = size_string.trim();
    if s.is_empty() {
        return 0;
    }
    let (num_str, suffix) = match s.rfind(|c: char| c.is_alphabetic()) {
        Some(i) => (&s[..i], &s[i..]),
        None => (s, ""),
    };
    let val: u64 = num_str.trim().parse().unwrap_or(0);
    match suffix.to_uppercase().as_str() {
        "" => val,
        "K" => val * 1024,
        "M" => val * 1024 * 1024,
        _ => {
            util::fatal(1, &format!("Unit {} unsupported\n", suffix));
        }
    }
}
