use clap::Parser;

use crate::diskio::DiskConfig;
use crate::negotiate;
use crate::senddata::NetConfig;
use crate::socklib;

const DEFAULT_STAT_PERIOD: i64 = 500_000;

pub struct StatConfig {
    pub log: Option<String>,
    pub bw_period: i64,
    pub print_uncompressed_pos: i32,
    pub stat_period: i64,
    pub no_progress: bool,
}

#[derive(Parser)]
#[command(name = "udp-sender", about = "UDP file sender")]
struct Cli {
    #[arg(short = 'f', long = "file")]
    file: Option<String>,

    #[arg(short = 'p', long = "pipe")]
    pipe: Option<String>,

    #[arg(short = 'P', long = "portbase", alias = "port", default_value = "9000")]
    port_base: u16,

    #[arg(short = 'b', long = "blocksize", default_value = "1456")]
    block_size: u32,

    #[arg(short = 'i', long = "interface")]
    interface: Option<String>,

    // Long names accepted by the C sender, obsolete spellings included, so the
    // Rust binary stays a drop-in replacement for existing scripts.
    #[arg(
        short = 'm',
        long = "mcast-data-address",
        aliases = ["mcast-address", "mcast_address"]
    )]
    mcast_data_address: Option<String>,

    #[arg(
        short = 'M',
        long = "mcast-rdv-address",
        aliases = ["mcast-all-address", "mcast_all_address", "mcast_rdv_address"]
    )]
    mcast_rdv_address: Option<String>,

    #[arg(short = 'r', long = "max-bitrate", alias = "max_bitrate")]
    max_bitrate: Option<String>,

    #[arg(
        short = '1',
        long = "pointopoint",
        aliases = ["point-to-point", "point_to_point"]
    )]
    pointopoint: bool,

    #[arg(
        short = '2',
        long = "nopointopoint",
        aliases = ["nopoint-to-point", "nopoint_to_point"]
    )]
    nopointopoint: bool,

    #[arg(short = 'a', long = "async")]
    async_mode: bool,

    #[arg(short = 'c', long = "half-duplex")]
    half_duplex: bool,

    #[arg(short = 'd', long = "full-duplex")]
    full_duplex: bool,

    #[arg(short = 't', long = "ttl", default_value = "1")]
    ttl: i32,

    #[arg(short = 'l', long = "log")]
    log: Option<String>,

    #[arg(long = "no-progress")]
    no_progress: bool,

    #[arg(long = "min-slice-size", default_value = "16")]
    min_slice_size: u32,

    #[arg(long = "slice-size", long = "default-slice-size")]
    default_slice_size: Option<u32>,

    #[arg(long = "max-slice-size", default_value = "1024")]
    max_slice_size: u32,

    #[arg(short = 'H', long = "rexmit-hello-interval", default_value = "0")]
    rexmit_hello_interval: i32,

    #[arg(short = 'S', long = "autostart", default_value = "0")]
    autostart: i32,

    #[arg(short = 'B', long = "broadcast")]
    broadcast: bool,

    #[arg(short = 's', long = "sendbuf")]
    sendbuf: Option<String>,

    #[arg(
        short = 'C',
        long = "min-receivers",
        alias = "min-clients",
        default_value = "0"
    )]
    min_receivers: i32,

    #[arg(short = 'W', long = "max-wait", default_value = "0")]
    max_wait: i32,

    #[arg(short = 'w', long = "min-wait", default_value = "0")]
    min_wait: i32,

    #[arg(short = 'k', long = "nokbd")]
    nokbd: bool,

    #[arg(short = 'T', long = "start-timeout", default_value = "0")]
    start_timeout: i32,

    #[arg(
        short = 'R',
        long = "retries-until-drop",
        alias = "retriesUntilDrop",
        default_value = "200"
    )]
    retries_until_drop: i32,

    #[arg(short = 'I', long = "bw-period", default_value = "0")]
    bw_period: i64,

    #[arg(
        short = 'x',
        long = "print-uncompressed-position",
        default_value = "-1"
    )]
    print_uncompressed_pos: i32,

    #[arg(
        short = 'z',
        long = "stat-period",
        alias = "statistics-period",
        default_value = "0"
    )]
    stat_period: i32,

    #[arg(short = 'Z', long = "streaming")]
    streaming: bool,

    #[arg(short = 'Y', long = "rehello-offset", default_value = "50")]
    rehello_offset: i32,

    #[arg(short = 'F', long = "fec")]
    fec: Option<String>,

    /// C's `-L` prints the license of the FEC code (GPL for udpcast plus the
    /// BSD license of the Reed-Solomon code) and stops right out of the
    /// option-parsing loop, before any transfer setup (`udp-sender.c` case 'L').
    #[arg(short = 'L', long = "license")]
    license: bool,

    #[arg(short = 'A', long = "autorate")]
    autorate: bool,

    trailing: Vec<String>,
}

/// Parse the `--fec` spec `[stripes]x[redundancy][/stripesize]`.
/// Mirrors C `udp-sender.c` case 'F': without `x` the default is 8 stripes
/// and the whole string is the redundancy; without `/` the stripesize
/// defaults to 128. Returns (stripes, redundancy, stripesize).
pub(crate) fn parse_fec_spec(spec: &str) -> (u32, u32, u32) {
    let (stripes, rest) = match spec.find('x') {
        Some(pos) => {
            let s: u32 = spec[..pos]
                .parse()
                .unwrap_or_else(|_| crate::util::fatal(1, &format!("bad fec spec {}\n", spec)));
            (s, &spec[pos + 1..])
        }
        None => (8, spec),
    };
    let (redundancy, stripesize) = match rest.find('/') {
        Some(pos) => {
            let r: u32 = rest[..pos]
                .parse()
                .unwrap_or_else(|_| crate::util::fatal(1, &format!("bad fec spec {}\n", spec)));
            let ss: u32 = rest[pos + 1..]
                .parse()
                .unwrap_or_else(|_| crate::util::fatal(1, &format!("bad fec spec {}\n", spec)));
            (r, ss)
        }
        None => {
            let r: u32 = rest
                .parse()
                .unwrap_or_else(|_| crate::util::fatal(1, &format!("bad fec spec {}\n", spec)));
            (r, 128)
        }
    };
    (stripes, redundancy, stripesize)
}

pub fn run_sender() {
    let cli = Cli::parse();

    // C fires this straight out of the option-parsing loop, before the
    // sockets are opened or a transfer is set up: print and exit(0).
    if cli.license {
        crate::fec::fec_license();
    }

    let mut disk_config = DiskConfig {
        orig_out_file: false,
        file_name: cli.file.clone(),
        pipe_name: cli.pipe.clone(),
        flags: 0,
    };

    let mut data_mcast_addr = socklib::clear_ip();
    let mut _data_mcast_supplied = false;

    if let Some(ref addr_str) = cli.mcast_data_address {
        data_mcast_addr = socklib::set_ip_from_string(addr_str);
        _data_mcast_supplied = true;
    }

    let mut flags: u32 = 0;
    if cli.async_mode {
        flags |= crate::senddata::FLAG_ASYNC | crate::senddata::FLAG_SN;
    }
    if cli.half_duplex {
        flags &= !crate::senddata::FLAG_SN;
        flags |= crate::senddata::FLAG_NOTSN;
    }
    if cli.full_duplex {
        flags |= crate::senddata::FLAG_SN;
    }
    if cli.pointopoint {
        flags |= crate::senddata::FLAG_POINTOPOINT;
    }
    if cli.nopointopoint {
        flags |= crate::senddata::FLAG_NOPOINTOPOINT;
    }
    if cli.broadcast {
        flags |= crate::senddata::FLAG_BCAST;
    }
    if cli.nokbd {
        flags |= crate::senddata::FLAG_NOKBD;
    }
    if cli.streaming {
        flags |= crate::senddata::FLAG_STREAMING;
    }

    let mut fec_stripes = 0u32;
    let mut fec_redundancy = 0u32;
    let mut fec_stripesize = 0u32;
    if let Some(ref spec) = cli.fec {
        flags |= crate::senddata::FLAG_FEC;
        let (s, r, ss) = parse_fec_spec(spec);
        fec_stripes = s;
        fec_redundancy = r;
        fec_stripesize = ss;
        // Same stderr diagnostic as C's case 'F'.
        eprintln!("stripes={} redund={} stripesize={}", s, r, ss);
    }

    let mut block_size = cli.block_size;
    block_size -= block_size % 4;
    if block_size == 0 {
        crate::util::fatal(1, "block size too small\n");
    }

    // `-H 0` would mean "no HELLO retransmission" in the C sender. The Rust
    // one keeps announcing itself at DEFAULT_HELLO_INTERVAL_MS intervals by
    // default instead: without it, a receiver that starts after the single
    // initial HELLO never finds the transfer. Pass `-H -1` for the old
    // "announce once" behaviour.
    let mut rexmit_hello_interval = cli.rexmit_hello_interval;
    if rexmit_hello_interval == 0 {
        rexmit_hello_interval = negotiate::DEFAULT_HELLO_INTERVAL_MS;
    }

    let requested_buf_size = cli
        .sendbuf
        .as_deref()
        .map(|s| socklib::parse_size(s) as u32)
        .unwrap_or(0);

    let mut min_slice_size = cli.min_slice_size;
    let mut max_slice_size = cli.max_slice_size;
    let mut default_slice_size = cli.default_slice_size.unwrap_or(0);

    if min_slice_size < 1 {
        min_slice_size = 1;
    }
    if max_slice_size < min_slice_size {
        max_slice_size = min_slice_size;
    }
    if default_slice_size != 0 {
        if default_slice_size < min_slice_size {
            default_slice_size = min_slice_size;
        }
        if default_slice_size > max_slice_size {
            default_slice_size = max_slice_size;
        }
    }

    let mut stat_period = DEFAULT_STAT_PERIOD;
    if cli.stat_period > 0 {
        stat_period = cli.stat_period as i64 * 1000;
    }

    let stat_config = StatConfig {
        log: cli.log.clone(),
        bw_period: cli.bw_period,
        print_uncompressed_pos: cli.print_uncompressed_pos,
        stat_period,
        no_progress: cli.no_progress,
    };

    let mut net_config = NetConfig {
        net_if: None,
        port_base: cli.port_base,
        block_size: block_size,
        slice_size: if default_slice_size > 0 {
            default_slice_size
        } else {
            16
        },
        control_mcast_addr: socklib::clear_ip(),
        data_mcast_addr,
        mcast_rdv: cli.mcast_rdv_address.clone(),
        ttl: cli.ttl,
        flags,
        capabilities: crate::senddata::sender_capabilities(flags),
        min_slice_size,
        default_slice_size,
        max_slice_size,
        rcvbuf: requested_buf_size,
        rexmit_hello_interval,
        autostart: cli.autostart,
        requested_buf_size,
        min_receivers: cli.min_receivers,
        max_receivers_wait: cli.max_wait,
        min_receivers_wait: cli.min_wait,
        retries_until_drop: cli.retries_until_drop,
        rehello_offset: cli.rehello_offset,
        start_timeout: cli.start_timeout,
        discovery: crate::senddata::Discovery::Doubling,
        fec_stripes,
        fec_redundancy,
        fec_stripesize,
        max_bitrate: cli.max_bitrate.clone(),
        autorate: cli.autorate,
    };

    if let Some(ref fname) = cli.file {
        disk_config.file_name = Some(fname.clone());
    } else if !cli.trailing.is_empty() {
        disk_config.file_name = Some(cli.trailing[0].clone());
    }

    if (flags & crate::senddata::FLAG_POINTOPOINT) != 0
        && (flags & crate::senddata::FLAG_NOPOINTOPOINT) != 0
    {
        crate::util::fatal(1, "pointopoint and nopointopoint cannot be set both\n");
    }

    eprintln!("Udp-sender 1.0.0\n");

    let socks = match negotiate::open_sender_socks(
        &mut net_config,
        cli.interface.as_deref(),
        &disk_config,
        true,
    ) {
        Some(s) => s,
        None => {
            eprintln!("Could not open main sender socket");
            std::process::exit(1);
        }
    };

    let r = negotiate::start_sender_with_socks(&disk_config, &mut net_config, &stat_config, socks);
    std::process::exit(r);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// C `udp-sender.c` case 'F': `[stripes]x[redundancy][/stripesize]`.
    /// Default stripes 8, default stripesize 128; a bare number is the
    /// redundancy (with the default 8 stripes).
    #[test]
    fn parse_fec_spec_forms() {
        assert_eq!(parse_fec_spec("8x2/128"), (8, 2, 128));
        assert_eq!(parse_fec_spec("8x2"), (8, 2, 128));
        assert_eq!(parse_fec_spec("2"), (8, 2, 128));
        assert_eq!(parse_fec_spec("8"), (8, 8, 128));
        assert_eq!(parse_fec_spec("4x3/64"), (4, 3, 64));
        assert_eq!(parse_fec_spec("16x4/256"), (16, 4, 256));
        assert_eq!(parse_fec_spec("8x8/16"), (8, 8, 16));
    }
}
