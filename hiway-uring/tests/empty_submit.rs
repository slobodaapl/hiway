#![cfg(target_os = "linux")]

use hiway::{EventId, EventSpec, SchemaRevision, StaticStream, WireCodec, WireError};
use hiway_uring::Driver;
use std::{os::unix::net::UnixStream, process::Command};

struct Byte;
impl EventSpec for Byte {
    type Payload = u8;
    const ID: EventId = EventId::from_name("uring.test.empty-submit");
}
impl WireCodec for Byte {
    fn encoded_len(_: &u8) -> usize {
        1
    }
    fn encode(value: &u8, output: &mut [u8]) -> Result<usize, WireError> {
        *output.first_mut().ok_or(WireError::InvalidPayload)? = *value;
        Ok(1)
    }
    fn decode(bytes: &[u8], _: SchemaRevision) -> Result<u8, WireError> {
        let [value] = bytes else {
            return Err(WireError::InvalidPayload);
        };
        Ok(*value)
    }
}

#[test]
// Keep the syscall-denial sequence within its isolated child.
#[allow(clippy::too_many_lines)]
fn ready_wait_and_empty_advance_do_not_enter_kernel() {
    const CHILD: &str = "HIWAY_URING_EMPTY_SUBMIT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "ready_wait_and_empty_advance_do_not_enter_kernel",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let mut driver = Driver::<(), 1, 64>::new(2).unwrap();
    let destination = StaticStream::<Byte, 1>::new();
    let (peer, data) = UnixStream::pair().unwrap();
    let (_peer_control, control) = UnixStream::pair().unwrap();
    let import = driver
        .import(destination.sender(), data, control, ())
        .unwrap();
    driver.advance().unwrap();
    drop(peer);
    driver.wait().unwrap();
    drop(import);
    let mut filter = [
        libc::sock_filter {
            code: u16::try_from(libc::BPF_LD | libc::BPF_W | libc::BPF_ABS).unwrap(),
            jt: 0,
            jf: 0,
            k: 0,
        },
        libc::sock_filter {
            code: u16::try_from(libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K).unwrap(),
            jt: 0,
            jf: 1,
            k: u32::try_from(libc::SYS_io_uring_enter).unwrap(),
        },
        libc::sock_filter {
            code: u16::try_from(libc::BPF_RET | libc::BPF_K).unwrap(),
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_ERRNO | u32::try_from(libc::EPERM).unwrap(),
        },
        libc::sock_filter {
            code: u16::try_from(libc::BPF_RET | libc::BPF_K).unwrap(),
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_ALLOW,
        },
    ];
    let program = libc::sock_fprog {
        len: u16::try_from(filter.len()).unwrap(),
        filter: filter.as_mut_ptr(),
    };
    // SAFETY: these calls change only the isolated child's execution policy.
    assert_eq!(
        unsafe {
            libc::prctl(
                libc::PR_SET_NO_NEW_PRIVS,
                1 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            )
        },
        0
    );
    // SAFETY: program and its four filter instructions remain valid for the call.
    assert_eq!(
        unsafe {
            libc::prctl(
                libc::PR_SET_SECCOMP,
                libc::c_ulong::from(libc::SECCOMP_MODE_FILTER),
                &raw const program,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            )
        },
        0
    );
    // SAFETY: an invalid descriptor cannot submit operations or access buffers.
    assert_eq!(
        unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                -1 as libc::c_long,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                std::ptr::null::<libc::sigset_t>(),
                0 as libc::c_ulong,
            )
        },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM)
    );
    assert_eq!(driver.wait().unwrap(), 0);
    assert_eq!(driver.advance().unwrap(), 1);
    assert_eq!(driver.active_links(), 0);
    assert_eq!(driver.advance().unwrap(), 0);
    assert_eq!(driver.wait().unwrap(), 0);
}
