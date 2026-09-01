//! Child-process helpers for the `-p` coprocesses.
//!
//! The coprocesses themselves are spawned with `std::process::Command`
//! (see `crate::diskio::open_pipe_sender` / `open_pipe_receiver`); this
//! module only waits for them.

use std::os::unix::process::ExitStatusExt;

/// C's wait_for_process: wait for the coprocess and report how it died.
///
/// Returns the process exit code when it exited on its own, 1 when it
/// was killed by a signal, and 0 when it terminated cleanly (or the
/// wait itself failed).
pub fn wait_for_child(child: &mut std::process::Child, message: &str) -> i32 {
    match child.wait() {
        Ok(status) => {
            if let Some(code) = status.code() {
                if code != 0 {
                    eprintln!("{} process died with code {}\n", message, code);
                    return code;
                }
            } else if let Some(sig) = status.signal() {
                eprintln!("{} process caught signal {}\n", message, sig);
                return 1;
            }
        }
        Err(_) => {}
    }
    0
}
