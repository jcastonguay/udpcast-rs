use std::os::unix::io::{BorrowedFd, RawFd};
use nix::sys::termios;
use nix::fcntl::OFlag;
use nix::unistd;
use nix::sys::select::FdSet;
use std::time::Duration;

pub struct Console {
    fd: RawFd,
    old_tio: Option<termios::Termios>,
    need_close: bool,
}

impl Drop for Console {
    fn drop(&mut self) {
        self.restore(false);
    }
}

impl Console {
    pub fn prepare(fd: Option<RawFd>) -> Option<Self> {
        let (fd, need_close) = match fd {
            Some(f) => (f, false),
            None => {
                match nix::fcntl::open("/dev/tty", OFlag::O_RDWR, nix::sys::stat::Mode::empty()) {
                    Ok(f) => (f, true),
                    Err(e) => {
                        eprintln!("Could not open keyboard: {}", e);
                        return None;
                    }
                }
            }
        };

        let borrowed_fd = unsafe { BorrowedFd::borrow_raw(fd) };
        let old_tio = termios::tcgetattr(borrowed_fd).ok();
        if let Some(ref old) = old_tio {
            let mut newtio = old.clone();
            newtio.local_flags.remove(termios::LocalFlags::ECHO | termios::LocalFlags::ICANON);
            newtio.control_chars[termios::SpecialCharacterIndices::VMIN as usize] = 1;
            newtio.control_chars[termios::SpecialCharacterIndices::VTIME as usize] = 0;
            if let Err(e) = termios::tcsetattr(borrowed_fd, termios::SetArg::TCSAFLUSH, &newtio) {
                eprintln!("Set terminal to raw: {}", e);
            }
        }

        Some(Console {
            fd,
            old_tio,
            need_close,
        })
    }

    pub fn select_with_console(
        &mut self,
        max_fd: &mut i32,
        read_set: &mut FdSet,
        timeout: Option<&Duration>,
    ) -> nix::Result<(i32, bool)> {
        use nix::sys::select;
        use nix::sys::time::TimeVal;

        let console_fd = self.fd;
        let borrowed_console = unsafe { BorrowedFd::borrow_raw(console_fd) };
        let mut fds = read_set.clone();
        fds.insert(borrowed_console);
        if console_fd >= *max_fd {
            *max_fd = console_fd + 1;
        }

        let mut tv = timeout.map(|d| {
            TimeVal::new(d.as_secs() as i64, d.subsec_micros() as i64)
        });
        let ret = select::select(*max_fd, Some(&mut fds), None, None, tv.as_mut())?;
        let key_pressed = fds.contains(borrowed_console);
        *read_set = fds;
        Ok((ret, key_pressed))
    }

    /// True when a keystroke can actually be read: a real terminal, or a pipe
    /// with data in it. A closed/redirected-away console (EOF, or /dev/null)
    /// must never look like "the user pressed a key".
    fn key_readable(&self) -> bool {
        use nix::poll::{poll, PollFd, PollFlags, PollTimeout};

        let borrowed_fd = unsafe { BorrowedFd::borrow_raw(self.fd) };
        let mut pfds = [PollFd::new(borrowed_fd, PollFlags::POLLIN)];
        match poll(&mut pfds, PollTimeout::ZERO) {
            Ok(n) if n > 0 => match pfds[0].revents() {
                Some(revents) => !revents.contains(PollFlags::POLLHUP),
                None => false,
            },
            _ => false,
        }
    }

    /// Consume a pending keystroke and return it: the console half of C's
    /// `selectWithConsole`. Returns None when nothing was typed, or when the
    /// console is merely at EOF (redirected from /dev/null) -- that must never
    /// be mistaken for "the user pressed a key", otherwise a sender without
    /// `-k` starts transfers on its own in a tight loop.
    pub fn take_key(&mut self) -> Option<u8> {
        if !self.key_readable() {
            return None;
        }
        let mut ch = [0u8; 1];
        // A zero-length read means EOF rather than a keystroke.
        match unistd::read(self.fd, &mut ch) {
            Ok(n) if n > 0 => Some(ch[0]),
            _ => None,
        }
    }

    /// Non-blocking check for a pending keystroke; the character is consumed.
    pub fn poll_key(&mut self) -> bool {
        self.take_key().is_some()
    }

    pub fn restore(&mut self, do_consume: bool) {
        if do_consume {
            let mut ch = [0u8; 1];
            let _ = unistd::read(self.fd, &mut ch);
            if ch[0] == b'q' {
                std::process::exit(1);
            }
        }

        if let Some(ref old) = self.old_tio {
            let borrowed_fd = unsafe { BorrowedFd::borrow_raw(self.fd) };
            let _ = termios::tcsetattr(borrowed_fd, termios::SetArg::TCSAFLUSH, old);
        }

        if self.need_close {
            let _ = unistd::close(self.fd);
            self.need_close = false;
        }
    }
}
