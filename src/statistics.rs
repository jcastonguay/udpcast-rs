use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Stats {
    pub fd: i32,
    pub last_printed_us: u64,
    pub stat_period: i64,
    pub print_uncompressed_pos: bool,
    pub no_progress: bool,
}

pub struct ReceiverStats {
    pub start_us: u64,
    pub total_bytes: u64,
    pub timer_started: bool,
    pub s: Stats,
}

pub struct SenderStats {
    pub total_bytes: u64,
    pub retransmissions: u64,
    pub cl_no: i32,
    pub period_bytes: u64,
    pub period_start_us: u64,
    pub bw_period: i64,
    pub s: Stats,
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn should_print(s: &mut Stats, now_us: u64, is_final: bool) -> bool {
    if is_final {
        return true;
    }
    let since_last = now_us.saturating_sub(s.last_printed_us);
    if since_last < s.stat_period as u64 {
        return false;
    }
    s.last_printed_us = now_us;
    true
}

fn print_file_position(fd: i32) {
    if fd < 0 {
        return;
    }
    let path = format!("/proc/self/fdinfo/{}", fd);
    if let Ok(contents) = std::fs::read_to_string(&path) {
        for line in contents.lines() {
            if line.starts_with("pos:") {
                if let Some(offset_str) = line.strip_prefix("pos:") {
                    if let Ok(offset) = offset_str.trim().parse::<u64>() {
                        crate::util::print_long_num(offset);
                    }
                }
                return;
            }
        }
    }
}

pub fn should_print_uncompressed_pos(default: i32, fd: i32, ref_fd: i32) -> bool {
    if default != -1 {
        return default != 0;
    }
    if ref_fd == fd {
        return false;
    }
    true
}

impl ReceiverStats {
    pub fn new(fd: i32, stat_period: i64, print_uncompressed_pos: bool, no_progress: bool) -> Self {
        let now = now_us();
        Self {
            start_us: now,
            total_bytes: 0,
            timer_started: false,
            s: Stats {
                fd,
                last_printed_us: now,
                stat_period,
                print_uncompressed_pos,
                no_progress,
            },
        }
    }

    pub fn start_timer(&mut self) {
        if !self.timer_started {
            self.start_us = now_us();
            self.timer_started = true;
        }
    }

    pub fn add_bytes(&mut self, bytes: u64) {
        self.total_bytes += bytes;
    }

    pub fn display(&mut self, is_final: bool) {
        if self.s.no_progress {
            return;
        }
        let now = now_us();
        if !should_print(&mut self.s, now, is_final) {
            return;
        }
        let time_passed = now.saturating_sub(self.start_us);
        eprint!("bytes=");
        crate::util::print_long_num(self.total_bytes);
        eprint!(" (");
        if time_passed != 0 {
            let mbps = (self.total_bytes * 800 / time_passed) as u32;
            eprint!("{:3}.{:02}", mbps / 100, mbps % 100);
        } else {
            eprint!("***.**");
        }
        eprint!(" Mbps)");
        if self.s.print_uncompressed_pos {
            print_file_position(self.s.fd);
        }
        eprint!("\r");
        let _ = std::io::stderr().flush();
    }
}

impl SenderStats {
    pub fn new(
        fd: i32,
        bw_period: i64,
        stat_period: i64,
        print_uncompressed_pos: bool,
        no_progress: bool,
    ) -> Self {
        let now = now_us();
        Self {
            total_bytes: 0,
            retransmissions: 0,
            cl_no: 0,
            period_bytes: 0,
            period_start_us: now,
            bw_period,
            s: Stats {
                fd,
                last_printed_us: now,
                stat_period,
                print_uncompressed_pos,
                no_progress,
            },
        }
    }

    pub fn add_bytes(&mut self, bytes: u64) {
        self.total_bytes += bytes;
        if self.bw_period > 0 {
            let now = now_us();
            self.period_bytes += bytes;
            let elapsed = now.saturating_sub(self.period_start_us);
            if elapsed >= (self.bw_period as u64) * 1_000_000 {
                let bw = self.period_bytes as f64 * 8.0 / elapsed as f64;
                self.period_bytes = 0;
                self.period_start_us = now;
                eprintln!("Inst BW={:.6}", bw);
            }
        }
    }

    pub fn add_retransmissions(&mut self, retransmissions: u32) {
        self.retransmissions += retransmissions as u64;
    }

    pub fn set_answered(&mut self, cl_no: i32) {
        self.cl_no = cl_no;
    }

    pub fn display(&mut self, block_size: u32, slice_size: u32, is_final: bool) {
        if self.s.no_progress {
            return;
        }
        let now = now_us();
        if !should_print(&mut self.s, now, is_final) {
            return;
        }
        let blocks = (self.total_bytes + block_size as u64 - 1) / block_size as u64;
        let percent = if blocks == 0 {
            0u32
        } else {
            ((1000u64 * self.retransmissions) / blocks) as u32
        };
        eprint!("bytes=");
        crate::util::print_long_num(self.total_bytes);
        eprint!(
            " re-xmits={:07} ({:3}.{:1}%) slice={:04} ",
            self.retransmissions,
            percent / 10,
            percent % 10,
            slice_size
        );
        if self.s.print_uncompressed_pos {
            print_file_position(self.s.fd);
        }
        eprint!("- {:3}\r", self.cl_no);
        let _ = std::io::stderr().flush();
    }
}
