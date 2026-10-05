#![cfg(all(target_os = "linux", target_arch = "x86_64", target_env = "gnu"))]

use hiway_uring::Driver;
use std::{
    io::{BufRead, BufReader, Write},
    mem::MaybeUninit,
    process::{Child, Command, Stdio},
    ptr::null_mut,
};

const COOP_TASKRUN: libc::c_long = 1 << 8;

struct Tracee {
    child: Child,
    pid: libc::pid_t,
    live: bool,
}
impl Tracee {
    fn pid(&self) -> libc::pid_t {
        self.pid
    }
}
impl Drop for Tracee {
    fn drop(&mut self) {
        if self.live {
            // SAFETY: ptrace can kill only this test's tracee, not a reused PID.
            unsafe {
                libc::ptrace(
                    libc::PTRACE_KILL,
                    self.pid(),
                    null_mut::<libc::c_void>(),
                    null_mut::<libc::c_void>(),
                );
                libc::waitpid(self.pid(), null_mut(), libc::__WALL);
            }
            let _ = self.child.wait();
        }
    }
}

fn registers(pid: libc::pid_t) -> libc::user_regs_struct {
    let mut registers = MaybeUninit::<libc::user_regs_struct>::uninit();
    // SAFETY: the stopped child is traced by this process; the output fits GETREGS.
    assert_eq!(
        unsafe {
            libc::ptrace(
                libc::PTRACE_GETREGS,
                pid,
                null_mut::<libc::c_void>(),
                registers.as_mut_ptr().cast::<libc::c_void>(),
            )
        },
        0
    );
    // SAFETY: successful GETREGS initialized the entire register structure.
    unsafe { registers.assume_init() }
}

fn set_registers(pid: libc::pid_t, registers: &libc::user_regs_struct) {
    // SAFETY: the stopped tracee accepts this complete register structure.
    assert_eq!(
        unsafe {
            libc::ptrace(
                libc::PTRACE_SETREGS,
                pid,
                null_mut::<libc::c_void>(),
                std::ptr::from_ref(registers)
                    .cast_mut()
                    .cast::<libc::c_void>(),
            )
        },
        0
    );
}

#[test]
fn cooperative_setup_falls_back_when_unsupported() {
    setup_with_fault(
        "cooperative_setup_falls_back_when_unsupported",
        libc::EINVAL,
    );
}

#[test]
fn cooperative_setup_preserves_permission_error() {
    setup_with_fault("cooperative_setup_preserves_permission_error", libc::EPERM);
}

// Keep tracee ownership and cleanup in one scope.
#[allow(clippy::too_many_lines)]
fn setup_with_fault(test: &str, error: i32) {
    const CHILD: &str = "HIWAY_URING_COOP_FALLBACK_CHILD";
    if std::env::var_os(CHILD).is_some() {
        // SAFETY: only this child's test thread requests tracing.
        assert_eq!(
            unsafe {
                libc::ptrace(
                    libc::PTRACE_TRACEME,
                    0,
                    null_mut::<libc::c_void>(),
                    null_mut::<libc::c_void>(),
                )
            },
            0
        );
        // SAFETY: gettid reads this thread's identifier without borrowing memory.
        println!("TRACE_TID={}", unsafe { libc::gettid() });
        std::io::stdout().flush().unwrap();
        // SAFETY: the tracee stops itself for its parent to configure tracing.
        assert_eq!(unsafe { libc::raise(libc::SIGSTOP) }, 0);
        let result = Driver::<(), 1, 64>::new(2);
        if error == libc::EINVAL {
            assert!(result.is_ok());
        } else {
            match result {
                Err(actual) => assert_eq!(actual.raw_os_error(), Some(error)),
                Ok(_) => panic!("setup denial was hidden"),
            }
        }
        return;
    }
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", test, "--nocapture"])
        .env(CHILD, "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = command.spawn().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let pid = loop {
        let mut line = String::new();
        assert!(output.read_line(&mut line).unwrap() > 0);
        if let Some(id) = line.strip_prefix("TRACE_TID=") {
            break id.trim().parse().unwrap();
        }
    };
    let mut tracee = Tracee {
        child,
        pid,
        live: true,
    };
    let pid = tracee.pid();
    let mut status = 0;
    // SAFETY: status is writable, and pid belongs to this child.
    assert_eq!(
        unsafe { libc::waitpid(pid, &raw mut status, libc::__WALL) },
        pid
    );
    assert!(libc::WIFSTOPPED(status));
    // SAFETY: options apply only to the stopped child owned by this tracer.
    assert_eq!(
        unsafe {
            libc::ptrace(
                libc::PTRACE_SETOPTIONS,
                pid,
                null_mut::<libc::c_void>(),
                (libc::PTRACE_O_TRACESYSGOOD | libc::PTRACE_O_EXITKILL) as usize
                    as *mut libc::c_void,
            )
        },
        0
    );
    let (mut rejected, mut plain, mut reject_exit, mut signal) = (0, 0, false, 0);
    // The child stopped at signal delivery after raise's syscall exit.
    // Its next syscall stop is an entry; TRACESYSGOOD stops alternate thereafter.
    let mut entering = true;
    loop {
        // SAFETY: resume only this test's tracee, preserving real delivery signals.
        assert_eq!(
            unsafe {
                libc::ptrace(
                    libc::PTRACE_SYSCALL,
                    pid,
                    null_mut::<libc::c_void>(),
                    usize::try_from(signal).unwrap() as *mut libc::c_void,
                )
            },
            0
        );
        // SAFETY: status is writable, and pid still belongs to this unreaped child.
        assert_eq!(
            unsafe { libc::waitpid(pid, &raw mut status, libc::__WALL) },
            pid
        );
        if libc::WIFEXITED(status) || libc::WIFSIGNALED(status) {
            tracee.live = false;
            assert!(libc::WIFEXITED(status));
            assert_eq!(libc::WEXITSTATUS(status), 0);
            break;
        }
        assert!(libc::WIFSTOPPED(status));
        signal = libc::WSTOPSIG(status);
        if signal != (libc::SIGTRAP | 0x80) {
            continue;
        }
        signal = 0;
        let mut registers = registers(pid);
        if entering && registers.orig_rax == libc::SYS_io_uring_setup as u64 {
            // Linux UAPI: flags follows two u32 fields in io_uring_params;
            // x86-64 syscall argument two (rsi) points to that structure.
            // SAFETY: PEEKDATA reads the stopped child's setup parameters, not our memory.
            let flags = unsafe {
                libc::ptrace(
                    libc::PTRACE_PEEKDATA,
                    pid,
                    (registers.rsi + 8) as *mut libc::c_void,
                    null_mut::<libc::c_void>(),
                )
            };
            assert_ne!(flags, -1);
            if flags & COOP_TASKRUN != 0 {
                rejected += 1;
                reject_exit = true;
                registers.orig_rax = u64::MAX;
                set_registers(pid, &registers);
            } else {
                plain += 1;
            }
        } else if !entering && reject_exit {
            registers.rax = (-i64::from(error)).cast_unsigned();
            set_registers(pid, &registers);
            reject_exit = false;
        }
        entering = !entering;
    }
    assert!(tracee.child.wait().unwrap().success());
    assert!(rejected > 0, "cooperative setup was not rejected");
    if error == libc::EINVAL {
        assert!(plain > 0, "no setup without the unsupported flag");
    } else {
        assert_eq!(plain, 0, "setup was retried after a permission error");
    }
}
