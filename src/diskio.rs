//! Disk I/O: the local reader / writer threads and the optional
//! `-p` coprocess.
//!
//! C hands raw fds around here and never closes them (the process exit
//! does it for it). Instead this port keeps *owned* handles:
//! `InFile` / `OutFile` own their fd (or refer to the stdio handles)
//! and close it exactly once, on drop. Coprocesses are spawned with
//! `std::process::Command`, which dups the handles into the child for
//! us — no fork/dup2/execvp dance, and the child never keeps a handle
//! to a parent-only fd.

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::io::{AsFd, AsRawFd};

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

/// A read-side input source: a file, or standard input.
pub struct InFile {
    inner: InInner,
}

enum InInner {
    File(File),
    Stdin,
}

impl InFile {
    pub fn stdin() -> Self {
        Self {
            inner: InInner::Stdin,
        }
    }

    pub fn file(f: File) -> Self {
        Self {
            inner: InInner::File(f),
        }
    }

    /// The numeric fd, for callers that still speak raw fds (statistics
    /// and /proc introspection).
    pub fn raw_fd(&self) -> i32 {
        match &self.inner {
            InInner::File(f) => f.as_raw_fd(),
            InInner::Stdin => 0,
        }
    }

    /// The underlying `File`, when this is one (None for stdin).
    fn as_file(&self) -> Option<&File> {
        match &self.inner {
            InInner::File(f) => Some(f),
            InInner::Stdin => None,
        }
    }

    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        match &self.inner {
            // Same sequential read(2) the C code does; works through the
            // shared handle (no &mut File needed).
            InInner::File(f) => nix::unistd::read(f.as_raw_fd(), buf).map_err(io::Error::other),
            InInner::Stdin => {
                let mut sin = std::io::stdin();
                sin.read(buf)
            }
        }
    }
}

/// A write-side output destination: a file, or standard output.
pub struct OutFile {
    inner: OutInner,
}

enum OutInner {
    File(File),
    Stdout,
}

impl OutFile {
    pub fn stdout() -> Self {
        Self {
            inner: OutInner::Stdout,
        }
    }

    pub fn file(f: File) -> Self {
        Self {
            inner: OutInner::File(f),
        }
    }

    /// The numeric fd, for callers that still speak raw fds.
    pub fn raw_fd(&self) -> i32 {
        match &self.inner {
            OutInner::File(f) => f.as_raw_fd(),
            OutInner::Stdout => 1,
        }
    }

    /// The underlying `File`, when this is one (None for stdout).
    fn as_file(&self) -> Option<&File> {
        match &self.inner {
            OutInner::File(f) => Some(f),
            OutInner::Stdout => None,
        }
    }

    fn write_all(&self, buf: &[u8]) -> io::Result<()> {
        match &self.inner {
            OutInner::File(f) => {
                let mut off = 0;
                while off < buf.len() {
                    let n = nix::unistd::write(f.as_fd(), &buf[off..]).map_err(io::Error::other)?;
                    if n == 0 {
                        return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
                    }
                    off += n;
                }
                Ok(())
            }
            OutInner::Stdout => {
                let mut out = std::io::stdout();
                out.write_all(buf)
            }
        }
    }
}

pub fn open_file(config: &DiskConfig) -> InFile {
    if let Some(ref fname) = config.file_name {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fname)
            .unwrap_or_else(|e| {
                crate::util::fatal(1, &format!("Could not open file {}: {}\n", fname, e));
            });
        InFile::file(f)
    } else {
        InFile::stdin()
    }
}

pub fn open_out_file(config: &DiskConfig) -> OutFile {
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
        OutFile::file(f)
    } else {
        OutFile::stdout()
    }
}

/// A coprocess between the local reader and the network sender: the
/// child runs `config.pipe_name`, reading its stdin from a duplicate of
/// the input (or from the inherited stdin) and writing to a pipe whose
/// read end becomes the new input source.
pub struct PipeReader {
    pub child: std::process::Child,
    pub read_end: InFile,
}

/// C udp-sender: `open_pipe_sender` — fork+dup2+execvp of the `-p`
/// command between the local reader and the network sender. `std::
/// process::Command` does the dup2 for us: the child is given a
/// *copy* of the input (so the parent keeps its own) and its stdout is
/// the pipe; with a stdin input the child simply inherits fd 0.
pub fn open_pipe_sender(config: &DiskConfig, input: &InFile) -> Option<PipeReader> {
    let pipe_name = config.pipe_name.as_deref()?;
    let args = crate::util::parse_command(pipe_name);
    if args.is_empty() {
        crate::util::fatal(1, &format!("Empty coprocess command: {}\n", pipe_name));
    }
    let (read_end, write_end) = nix::unistd::pipe().unwrap_or_else(|e| {
        crate::util::fatal(1, &format!("Could not create pipe: {}\n", e));
    });
    let read_end = File::from(read_end);
    let write_end = File::from(write_end);

    let mut cmd = std::process::Command::new(&args[0]);
    cmd.args(&args[1..]);
    cmd.stdout(write_end);
    if let Some(f) = input.as_file() {
        let dup = f.try_clone().unwrap_or_else(|e| {
            crate::util::fatal(1, &format!("Could not duplicate input: {}\n", e));
        });
        cmd.stdin(dup);
    }

    let child = cmd.spawn().unwrap_or_else(|e| {
        crate::util::fatal(1, &format!("exec {}: {}\n", args[0].to_string_lossy(), e));
    });

    Some(PipeReader {
        child,
        read_end: InFile::file(read_end),
    })
}
/// A coprocess between the network receiver and the local writer: the
/// child runs `config.pipe_name`, reading from a pipe and writing to a
/// duplicate of the output (or to the inherited stdout).
pub struct PipeWriter {
    pub child: std::process::Child,
    pub write_end: OutFile,
}

/// C udp-receiver: `open_pipe_receiver` — the `-p` command between the
/// network receiver and the local writer.
pub fn open_pipe_receiver(output: &OutFile, config: &DiskConfig) -> Option<PipeWriter> {
    let pipe_name = config.pipe_name.as_deref()?;
    let args = crate::util::parse_command(pipe_name);
    if args.is_empty() {
        crate::util::fatal(1, &format!("Empty coprocess command: {}\n", pipe_name));
    }
    let (read_end, write_end) = nix::unistd::pipe().unwrap_or_else(|e| {
        crate::util::fatal(1, &format!("Could not create pipe: {}\n", e));
    });
    let read_end = File::from(read_end);
    let write_end = File::from(write_end);

    let mut cmd = std::process::Command::new(&args[0]);
    cmd.args(&args[1..]);
    cmd.stdin(read_end);
    match output.as_file() {
        Some(f) => {
            let dup = f.try_clone().unwrap_or_else(|e| {
                crate::util::fatal(1, &format!("Could not duplicate output: {}\n", e));
            });
            cmd.stdout(dup);
        }
        // With a stdout output the child simply inherits fd 1.
        None => {}
    }

    let child = cmd.spawn().unwrap_or_else(|e| {
        crate::util::fatal(1, &format!("exec {}: {}\n", args[0].to_string_lossy(), e));
    });

    Some(PipeWriter {
        child,
        write_end: OutFile::file(write_end),
    })
}

pub fn local_reader(fifo: &Fifo, input: &InFile) {
    local_reader_fifo(fifo, input);
}

/// C's local reader thread: pull input blocks into the fifo until the
/// input runs out.
pub fn local_reader_fifo(fifo: &Fifo, input: &InFile) {
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
        match input.read(&mut buf) {
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
}

/// C's local writer thread: drain the fifo into the output.
pub fn writer(fifo: &Fifo, output: &OutFile) {
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
        match output.write_all(&buf[..bytes]) {
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
