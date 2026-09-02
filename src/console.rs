// Keyboard console for the sender's interactive negotiation loop.
//
// Mirrors C's Console: either stdin (data comes from a file, -f) or
// /dev/tty (data comes from a pipe or the keyboard). The terminal is
// put into non-echo / non-canonical mode with VMIN=1 so a single key
// press is delivered whole, and the original termios is restored on
// drop or restore().

use std::os::unix::io::{AsFd, AsRawFd, OwnedFd, RawFd};
use std::time::Duration;

use nix::sys::select::FdSet;
use nix::sys::termios::{self, Termios};
use nix::unistd;

/// The keyboard the console listens to.
///
/// `Stdin` is the stdio handle (fd 0, nothing to close); `Tty` owns
/// /dev/tty and closes it when dropped. Both expose AsFd through the
/// inner type, so termios/select/poll are driven without raw-fd FFI.
enum ConsoleFd {
    Stdin(std::io::Stdin),
    Tty(OwnedFd),
}

impl ConsoleFd {
    fn as_fd(&self) -> std::os::unix::io::BorrowedFd<'_> {
        match self {
            ConsoleFd::Stdin(s) => s.as_fd(),
            ConsoleFd::Tty(f) => f.as_fd(),
        }
    }

    fn raw_fd(&self) -> RawFd {
        match self {
            ConsoleFd::Stdin(_) => 0,
            ConsoleFd::Tty(f) => f.as_raw_fd(),
        }
    }
}

pub struct Console {
    fd: ConsoleFd,
    old_tio: Option<Termios>,
}

impl Drop for Console {
    fn drop(&mut self) {
        self.restore(false);
    }
}

impl Console {
    /// Prepare the keyboard: `use_stdin` when the data source is a file
    /// (the keyboard is stdin), otherwise /dev/tty.
    pub fn prepare(use_stdin: bool) -> Option<Self> {
        let fd = if use_stdin {
            ConsoleFd::Stdin(std::io::stdin())
        } else {
            match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open("/dev/tty")
            {
                Ok(f) => ConsoleFd::Tty(OwnedFd::from(f)),
                Err(e) => {
                    eprintln!("Could not open keyboard: {}", e);
                    return None;
                }
            }
        };

        // Keep C's behaviour: if the terminal mode cannot be read the
        // console still works, it just is not put into raw mode.
        let old_tio = termios::tcgetattr(fd.as_fd()).ok().map(|mut old| {
            old.local_flags
                .remove(termios::LocalFlags::ECHO | termios::LocalFlags::ICANON);
            old.control_chars[termios::SpecialCharacterIndices::VMIN as usize] = 1;
            old.control_chars[termios::SpecialCharacterIndices::VTIME as usize] = 0;
            if let Err(e) = termios::tcsetattr(fd.as_fd(), termios::SetArg::TCSAFLUSH, &old) {
                eprintln!("Set terminal to raw: {}", e);
            }
            old
        });

        Some(Console { fd, old_tio })
    }

    /// C's selectWithConsole: add the keyboard fd to a copy of the
    /// caller's read-set and run one select; reports whether the keyboard
    /// is among the ready fds. The set is local: the console fd matches
    /// no socket, so the caller's set is left untouched.
    pub fn select_with_console(
        &mut self,
        max_fd: &mut i32,
        read_set: &FdSet,
        timeout: Option<&Duration>,
    ) -> nix::Result<(i32, bool)> {
        use nix::sys::select;
        use nix::sys::time::TimeVal;

        let console_fd = self.fd.raw_fd();
        let mut fds = read_set.clone();
        fds.insert(self.fd.as_fd());
        if console_fd >= *max_fd {
            *max_fd = console_fd + 1;
        }

        let mut tv = timeout.map(|d| TimeVal::new(d.as_secs() as i64, d.subsec_micros() as i64));
        let ret = select::select(*max_fd, Some(&mut fds), None, None, tv.as_mut())?;
        let key_pressed = fds.contains(self.fd.as_fd());
        Ok((ret, key_pressed))
    }

    /// True when a keystroke can actually be read: a real terminal, or a
    /// pipe with data in it. A closed/redirected-away console (EOF, or
    /// /dev/null) must never look like "the user pressed a key".
    fn key_readable(&self) -> bool {
        use nix::poll::{poll, PollFd, PollFlags, PollTimeout};

        let mut pfds = [PollFd::new(self.fd.as_fd(), PollFlags::POLLIN)];
        match poll(&mut pfds, PollTimeout::ZERO) {
            Ok(n) if n > 0 => match pfds[0].revents() {
                Some(revents) => !revents.contains(PollFlags::POLLHUP),
                None => false,
            },
            _ => false,
        }
    }

    /// Consume a pending keystroke and return it: the console half of
    /// C's `selectWithConsole`. Returns None when nothing was typed, or
    /// when the console is merely at EOF (redirected from /dev/null) --
    /// that must never be mistaken for "the user pressed a key",
    /// otherwise a sender without `-k` starts transfers on its own in a
    /// tight loop.
    pub fn take_key(&mut self) -> Option<u8> {
        if !self.key_readable() {
            return None;
        }
        let mut ch = [0u8; 1];
        // A zero-length read means EOF rather than a keystroke.
        match unistd::read(self.fd.as_fd(), &mut ch) {
            Ok(n) if n > 0 => Some(ch[0]),
            _ => None,
        }
    }

    /// Non-blocking check for a pending keystroke; the character is consumed.
    pub fn poll_key(&mut self) -> bool {
        self.take_key().is_some()
    }

    /// Restore the terminal mode and (optionally) consume one final key,
    /// exiting on 'q' like C's restoreConsole(console, 1). A Tty console
    /// closes /dev/tty when dropped; a Stdin console owns nothing.
    pub fn restore(&mut self, do_consume: bool) {
        if do_consume {
            if let Some(ch) = self.take_key() {
                if ch == b'q' {
                    std::process::exit(1);
                }
            }
        }

        if let Some(ref old) = self.old_tio {
            let _ = termios::tcsetattr(self.fd.as_fd(), termios::SetArg::TCSAFLUSH, old);
        }
    }
}
