//! Logging and command-line parsing utilities.
//!
//! Mirrors the behavior of the original `log.c` / `udpcast.c` helpers:
//! messages go to stderr by default, or to a log file when `-l` is given.

use std::fs::File;
use std::io::Write;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Global log file (if any). Replaces the C global `udpc_log`.
static LOG: Mutex<Option<File>> = Mutex::new(None);

/// A monotonically increasing "now" in microseconds, for compatibility
/// with code ported from the C `gettimeofday`-based timing.
pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Initialize the global log file. Replaces `initLog()`.
pub fn init_log(path: &str) {
    match File::options().append(true).create(true).open(path) {
        Ok(f) => {
            *LOG.lock().unwrap() = Some(f);
        }
        Err(e) => {
            eprintln!("cannot open log file `{path}`: {e}");
            std::process::exit(2);
        }
    }
}

/// Current time as `HH:MM:SS.ffffff` (local time), like the C logger.
fn time_stamp() -> String {
    let usecs = (now_us() % 1_000_000) as u32;
    let secs = now_us() / 1_000_000;
    let local = secs as i64 + local_tz_offset_secs();
    let ltod = local.rem_euclid(86400);
    format!(
        "{:02}:{:02}:{:02}.{:06}",
        ltod / 3600,
        (ltod % 3600) / 60,
        ltod % 60,
        usecs
    )
}

fn local_tz_offset_secs() -> i64 {
    // Best-effort local offset from the TZ environment variable
    // (forms like "+0200", "UTC+2"); falls back to UTC.
    let tz = match std::env::var("TZ") {
        Ok(t) => t,
        Err(_) => return 0,
    };
    parse_tz_offset(&tz).unwrap_or(0)
}

fn parse_tz_offset(tz: &str) -> Option<i64> {
    // Look for a "[+-]HH" or "[+-]HHMM" / "[+-]HH:MM" token in the string.
    let bytes = tz.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'+' || *b == b'-' {
            let sign: i64 = if *b == b'+' { 1 } else { -1 };
            let rest = &bytes[i + 1..];
            let mut v: i64 = 0;
            let mut n = 0;
            for c in rest.iter() {
                match c {
                    b'0'..=b'9' => {
                        v = v * 10 + (*c - b'0') as i64;
                        n += 1;
                    }
                    b':' => continue,
                    _ => break,
                }
            }
            if n >= 2 {
                // HH or HHMM
                let hh = v / 100;
                let mm = if n >= 4 { v % 100 } else { 0 };
                return Some(sign * (hh * 3600 + mm * 60));
            }
        }
    }
    None
}

/// Print to the log file (with timestamp) if logging is enabled, else to
/// stderr. This is the workhorse "flprintf" of the C code; Rust callers use
/// `flprintf!(format, ...)`-style pre-formatted strings.
pub fn flprintf(msg: &str) {
    let mut log = LOG.lock().unwrap();
    if let Some(f) = log.as_mut() {
        let _ = write!(f, "{} {}", time_stamp(), msg);
        if msg.ends_with('\n') || !msg.contains('\n') {
            let _ = f.flush();
        }
    } else {
        eprint!("{msg}");
        let _ = std::io::stderr().flush();
    }
}

/// Always print to the log file (never to stderr), like C `logprintf`.
pub fn logprintf(msg: &str) {
    let mut log = LOG.lock().unwrap();
    if let Some(f) = log.as_mut() {
        let _ = write!(f, "{} {}", time_stamp(), msg);
        let _ = f.flush();
    }
}

/// Print an error and exit the program, like C `fatal()`.
/// Returns `!` so it can be used in place of `panic!`.
#[allow(clippy::empty_loop)]
pub fn fatal(code: i32, msg: &str) -> ! {
    eprint!("{msg}");
    let _ = std::io::stderr().flush();
    std::process::exit(code);
}

/// Convenience: format + fatal.
#[macro_export]
macro_rules! fatal {
    ($code:expr, $($arg:tt)+) => {
        $crate::util::fatal($code, &format!($($arg)+))
    };
}

/// Format + print (flprintf with format args).
#[macro_export]
macro_rules! flprintf {
    ($($arg:tt)+) => {
        $crate::util::flprintf(&format!($($arg)+))
    };
}

/// Print a number with a K/M suffix, like C `printLongNum`.
pub fn print_long_num(x: u64) -> String {
    if x > 1_000_000_000_000 {
        format!("{:.1}M", x as f64 / 1_048_576.0)
    } else if x >= 1_000_000_000 {
        format!("{:.1}K", x as f64 / 1024.0)
    } else {
        format!("{x}")
    }
}

/// Parse a size with optional K/M/G suffix (case-insensitive),
/// e.g. "64k", "1.5M", "10G". Mirrors C `parseSize`.
pub fn parse_size(s: &str) -> f64 {
    let s = s.trim();
    let (num, suffix) = match s.rfind(|c: char| c.is_alphabetic()) {
        Some(i) => (s[..i].trim_end(), s[i..].to_ascii_uppercase()),
        None => (s.trim_end(), String::new()),
    };
    let val: f64 = match num.parse() {
        Ok(v) => v,
        Err(_) => return 0.0,
    };
    match suffix.as_str() {
        "" => val,
        "B" => val,
        "K" | "KI" => val * 1024.0,
        "M" | "MI" => val * 1024.0 * 1024.0,
        "G" | "GI" => val * 1024.0 * 1024.0 * 1024.0,
        "KB" => val * 1000.0,
        "MB" => val * 1000.0 * 1000.0,
        "GB" => val * 1000.0 * 1000.0 * 1000.0,
        _ => val,
    }
}

/// Parse a speed with optional K/M/G (decimal) suffix, e.g. "100K", "1.5M".
/// Mirrors C `parseSpeed`.
pub fn parse_speed(s: &str) -> f64 {
    let s = s.trim();
    let (num, suffix) = match s.rfind(|c: char| c.is_alphabetic()) {
        Some(i) => (s[..i].trim_end(), s[i..].to_ascii_uppercase()),
        None => (s.trim_end(), String::new()),
    };
    let val: f64 = match num.parse() {
        Ok(v) => v,
        Err(_) => return 0.0,
    };
    match suffix.as_str() {
        "" => val,
        "K" => val * 1000.0,
        "M" => val * 1000.0 * 1000.0,
        "G" => val * 1000.0 * 1000.0 * 1000.0,
        "B" => val,
        "BPS" => val,
        _ => val,
    }
}

/// Sleep `ms` milliseconds.
pub fn msleep(ms: u64) {
    std::thread::sleep(Duration::from_millis(ms));
}

/// Parse a command string like "/bin/gzip -c" into a Vec of CStrings
/// suitable for execvp.
pub fn parse_command(cmd: &str) -> Vec<std::ffi::OsString> {
    cmd.split_whitespace()
        .map(std::ffi::OsString::from)
        .collect()
}

/// Timestamp (seconds since process start) for debug logging.
pub fn dbg_stamp() -> f64 {
    use std::sync::OnceLock;
    static T0: OnceLock<std::time::Instant> = OnceLock::new();
    T0.get_or_init(std::time::Instant::now)
        .elapsed()
        .as_secs_f64()
}

/// `UDPC_DEBUG=1` switches on the verbose protocol tracing.
pub fn dbg_on() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("UDPC_DEBUG").is_ok())
}
