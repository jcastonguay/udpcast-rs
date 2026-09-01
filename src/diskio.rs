use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use crate::fifo::Fifo;

const BLOCKSIZE: usize = 4096;

pub struct DiskConfig {
    pub orig_out_file: bool,
    pub file_name: Option<String>,
    pub pipe_name: Option<String>,
    pub flags: u32,
}

pub const FLAG_NOSYNC: u32 = 0x0040;
pub const FLAG_SYNC: u32 = 0x0100;

pub fn open_file(config: &DiskConfig) -> RawFd {
    if let Some(ref fname) = config.file_name {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fname)
            .unwrap_or_else(|e| {
                crate::util::fatal(1, &format!("Could not open file {}: {}\n", fname, e));
            });
        let fd = f.as_raw_fd();
        std::mem::forget(f);
        fd
    } else {
        0
    }
}

pub fn open_out_file(config: &DiskConfig) -> RawFd {
    if let Some(ref fname) = config.file_name {
        use std::os::unix::fs::OpenOptionsExt;
        let mut opts = std::fs::OpenOptions::new();
        opts.create(true).write(true).truncate(true);
        if (config.flags & FLAG_SYNC) != 0 {
            opts.custom_flags(libc::O_SYNC);
        }
        let f = opts.open(fname).unwrap_or_else(|e| {
            crate::util::fatal(1, &format!("Could not open outfile {}: {}\n", fname, e));
        });
        let fd = f.as_raw_fd();
        std::mem::forget(f);
        fd
    } else {
        1
    }
}

pub fn open_pipe_sender(config: &DiskConfig, in_fd: RawFd, pid: &mut i32) -> RawFd {
    *pid = 0;
    if let Some(ref pipe_name) = config.pipe_name {
        use std::os::unix::io::IntoRawFd;
        let args = crate::util::parse_command(pipe_name);
        let (read_end, write_end) = nix::unistd::pipe().unwrap();
        *pid = crate::process::open2(in_fd, write_end.as_raw_fd(), &args, Some(read_end.as_raw_fd()));
        drop(write_end);
        read_end.into_raw_fd()
    } else {
        in_fd
    }
}

pub fn open_pipe_receiver(out_fd: RawFd, config: &DiskConfig, pid: &mut i32) -> RawFd {
    *pid = 0;
    if let Some(ref pipe_name) = config.pipe_name {
        use std::os::unix::io::IntoRawFd;
        let args = crate::util::parse_command(pipe_name);
        let (read_end, write_end) = nix::unistd::pipe().unwrap();
        *pid = crate::process::open2(read_end.as_raw_fd(), out_fd, &args, Some(write_end.as_raw_fd()));
        drop(read_end);
        write_end.into_raw_fd()
    } else {
        out_fd
    }
}

pub fn local_reader(fifo: &mut Fifo, in_fd: RawFd) {
    let mut file = unsafe { File::from_raw_fd(in_fd) };
    loop {
        let pos = fifo.free_mem_queue.get_consumer_position();
        let mut bytes = fifo.free_mem_queue.consume_contiguous_min_amount(BLOCKSIZE);
        let remainder = (pos + bytes) % BLOCKSIZE;
        if bytes > remainder && remainder != 0 {
            bytes -= remainder;
        }
        if bytes == 0 {
            break;
        }
        let mut buf = vec![0u8; bytes];
        match file.read(&mut buf) {
            Ok(0) => {
                fifo.data.produce_end();
                break;
            }
            Ok(n) => {
                fifo.free_mem_queue.consumed(n);
                fifo.write_at(pos, &buf[..n]);
                fifo.data.produce(n);
            }
            Err(e) => {
                eprintln!("read: {}", e);
                std::process::exit(1);
            }
        }
    }
    std::mem::forget(file);
}

pub fn local_reader_fifo(fifo: &Fifo, in_fd: RawFd) {
    let mut file = unsafe { File::from_raw_fd(in_fd) };
    loop {
        let pos = fifo.free_mem_queue.get_consumer_position();
        let mut bytes = fifo.free_mem_queue.consume_contiguous_min_amount(BLOCKSIZE);
        let remainder = (pos + bytes) % BLOCKSIZE;
        if bytes > remainder && remainder != 0 {
            bytes -= remainder;
        }
        if bytes == 0 {
            break;
        }
        let mut buf = vec![0u8; bytes];
        match file.read(&mut buf) {
            Ok(0) => {
                fifo.data.produce_end();
                break;
            }
            Ok(n) => {
                fifo.free_mem_queue.consumed(n);
                fifo.write_at(pos, &buf[..n]);
                fifo.data.produce(n);
            }
            Err(e) => {
                eprintln!("read: {}", e);
                std::process::exit(1);
            }
        }
    }
    std::mem::forget(file);
}

pub fn writer(fifo: &Fifo, out_fd: RawFd) {
    let mut file = unsafe { File::from_raw_fd(out_fd) };
    let fifo_size = fifo.data.get_size();
    if fifo_size % BLOCKSIZE != 0 {
        crate::util::fatal(1, "Fifo size not a multiple of block size\n");
    }
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let pos = fifo.data.get_consumer_position();
        let mut bytes = fifo.data.consume_contiguous_min_amount(BLOCKSIZE);
        if bytes == 0 {
            return;
        }
        let remainder = (pos + bytes) % BLOCKSIZE;
        if pos + bytes != fifo_size && bytes > remainder && remainder != 0 {
            bytes -= remainder;
        }
        buf.resize(bytes, 0);
        fifo.read_at(pos, &mut buf[..bytes]);
        match file.write_all(&buf[..bytes]) {
            Ok(()) => {
                fifo.data.consumed(bytes);
                fifo.free_mem_queue.produce(bytes);
            }
            Err(e) => {
                eprintln!("write: {}", e);
                std::process::exit(1);
            }
        }
    }
}
