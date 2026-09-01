use std::net::UdpSocket;
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
pub struct RateGovernor {
    name: String,
    data: Box<dyn RateGovernorData>,
}

pub trait RateGovernorData: Send {
    fn set_prop(&mut self, _key: &str, _value: &str) {}
    fn end_config(&mut self) {}
    fn wait(&mut self, _sock: &UdpSocket, _ip: u32, _size: i64) {}
    fn shutdown(&mut self) {}
}

pub struct MaxBitrateData {
    date_us: u64,
    bitrate: u64,
    queue_size: i64,
}

impl RateGovernorData for MaxBitrateData {
    fn set_prop(&mut self, key: &str, value: &str) {
        if key == "maxBitrate" {
            self.bitrate = parse_speed(value);
        }
    }

    fn wait(&mut self, _sock: &UdpSocket, _ip: u32, size: i64) {
        if self.bitrate == 0 {
            return;
        }
        let now = now_us();
        let elapsed = now.saturating_sub(self.date_us);
        let bits = elapsed * self.bitrate / 1_000_000;
        let size = size + 28;

        if bits >= (self.queue_size * 8) as u64 {
            self.queue_size = size;
            self.date_us = now;
            return;
        }

        self.queue_size -= (bits / 8) as i64;
        self.date_us += bits * 1_000_000 / self.bitrate;
        let sleep_time = self.queue_size * 8 * 1_000_000 / self.bitrate as i64;
        if sleep_time > 40000 || self.queue_size >= 100000 {
            let mut st = sleep_time - 10000;
            st -= st % 10000;
            if st > 0 {
                std::thread::sleep(std::time::Duration::from_micros(st as u64));
            }
        }
        self.queue_size += size;
    }
}

pub struct AutoRateData {
    is_initialized: bool,
    dir: i32,
    sendbuf: i32,
}

impl AutoRateData {
    pub fn new() -> Self {
        Self {
            is_initialized: false,
            dir: 0,
            sendbuf: 0,
        }
    }
}

impl RateGovernorData for AutoRateData {
    fn wait(&mut self, sock: &UdpSocket, _ip: u32, size: i64) {
        if !self.is_initialized {
            let q = crate::socklib::get_send_queue(sock);
            if q == 0 {
                self.dir = 0;
                self.sendbuf = crate::socklib::get_send_buf(sock)
                    .map(|v| v as i32)
                    .unwrap_or(0);
            } else {
                self.dir = 1;
                self.sendbuf = q;
            }
            self.is_initialized = true;
        }
        loop {
            let mut r = crate::socklib::get_send_queue(sock);
            if r < 0 {
                return;
            }
            if self.dir == 1 {
                r = self.sendbuf - r;
            }
            if (r as i64) < self.sendbuf as i64 / 2 - size {
                return;
            }
            std::thread::sleep(std::time::Duration::from_micros(2500));
        }
    }
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn parse_speed(speed_string: &str) -> u64 {
    let s = speed_string.trim();
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
        "K" => val * 1000,
        "M" => val * 1_000_000,
        "G" => val * 1_000_000_000,
        _ => val,
    }
}

pub struct RateGovernorSet {
    governors: Vec<RateGovernor>,
}

impl RateGovernorSet {
    pub fn new() -> Self {
        Self {
            governors: Vec::new(),
        }
    }

    pub fn add_max_bitrate(&mut self, bitrate_str: &str) {
        let mut data = MaxBitrateData {
            date_us: now_us(),
            bitrate: 0,
            queue_size: 0,
        };
        data.set_prop("maxBitrate", bitrate_str);
        self.governors.push(RateGovernor {
            name: "maxBitrate".to_string(),
            data: Box::new(data),
        });
    }

    pub fn add_autorate(&mut self) {
        self.governors.push(RateGovernor {
            name: "autorate".to_string(),
            data: Box::new(AutoRateData::new()),
        });
    }

    pub fn wait_all(&mut self, sock: &UdpSocket, ip: u32, size: i64) {
        for gov in &mut self.governors {
            gov.data.wait(sock, ip, size);
        }
    }

    pub fn shutdown_all(&mut self) {
        for gov in &mut self.governors {
            gov.data.shutdown();
        }
    }
}
