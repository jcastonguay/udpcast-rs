use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::{close, dup2, execvp, fork, ForkResult};
use std::ffi::CString;
use std::os::unix::io::RawFd;

pub fn open2(in_fd: RawFd, out_fd: RawFd, args: &[CString], close_fd: Option<RawFd>) -> i32 {
    match unsafe { fork() } {
        Ok(ForkResult::Child) => {
            dup_fd(in_fd, 0);
            dup_fd(out_fd, 1);
            if let Some(fd) = close_fd {
                let _ = close(fd);
            }
            let _ = execvp(&args[0], args);
            crate::util::fatal(1, &format!("exec {}: {}\n", args[0].to_string_lossy(), std::io::Error::last_os_error()));
        }
        Ok(ForkResult::Parent { child }) => child.as_raw() as i32,
        Err(e) => {
            eprintln!("fork: {}", e);
            -1
        }
    }
}

fn dup_fd(src: RawFd, target: RawFd) {
    if src != target {
        let _ = close(target);
        if let Err(e) = dup2(src, target) {
            crate::util::fatal(1, &format!("dup2 {}->{}: {}\n", src, target, e));
        }
        let _ = close(src);
    }
}

pub fn wait_for_process(pid: i32, message: &str) -> i32 {
    let nix_pid = nix::unistd::Pid::from_raw(pid);
    match waitpid(nix_pid, Some(WaitPidFlag::empty())) {
        Ok(WaitStatus::Exited(_, code)) => {
            if code != 0 {
                eprintln!("{} process died with code {}\n", message, code);
                return code;
            }
        }
        Ok(WaitStatus::Signaled(_, sig, _)) => {
            eprintln!("{} process caught signal {:?}\n", message, sig);
            return 1;
        }
        Ok(_) => {}
        Err(_) => {}
    }
    0
}
