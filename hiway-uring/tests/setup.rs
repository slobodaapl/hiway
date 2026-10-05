#![cfg(target_os = "linux")]

use hiway_uring::{setup_diagnostic, Driver};
use std::io;
use std::process::Command;

const CHILD_MODE: &str = "HIWAY_URING_SETUP_DIAGNOSTIC_CHILD";

fn memlock_limits() -> io::Result<(libc::rlim_t, libc::rlim_t)> {
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limits` is initialized and valid for `getrlimit` to write.
    if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &raw mut limits) } == 0 {
        Ok((limits.rlim_cur, limits.rlim_max))
    } else {
        Err(io::Error::last_os_error())
    }
}

fn set_memlock_soft(soft: libc::rlim_t) -> io::Result<()> {
    let (_, hard) = memlock_limits()?;
    let limits = libc::rlimit {
        rlim_cur: soft,
        rlim_max: hard,
    };
    // SAFETY: `limits` is initialized and valid for `setrlimit` to read.
    if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &raw const limits) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn assert_limit_snapshot(text: &str, name: &str, other_name: &str, value: libc::rlim_t) {
    let lower = text.to_ascii_lowercase();
    let name_at = lower
        .find(name)
        .unwrap_or_else(|| panic!("missing {name} limit in {text:?}"));
    let other_at = lower
        .find(other_name)
        .unwrap_or_else(|| panic!("missing {other_name} limit in {text:?}"));
    let component = if name_at < other_at {
        &lower[name_at..other_at]
    } else {
        &lower[name_at..]
    };
    if value == libc::RLIM_INFINITY {
        assert!(
            component.contains("unlimited"),
            "missing unlimited representation for {name} limit in {text:?}"
        );
    } else {
        let count = component.split_once("byte").and_then(|(prefix, _)| {
            let digits: String = prefix
                .trim_end()
                .chars()
                .rev()
                .take_while(char::is_ascii_digit)
                .collect();
            digits
                .chars()
                .rev()
                .collect::<String>()
                .parse::<u128>()
                .ok()
        });
        assert!(
            count == Some(u128::from(value)),
            "expected {value} bytes for {name} limit, found {count:?} in {text:?}"
        );
    }
}

#[test]
fn setup_diagnostic_preserves_non_enomem_errors() {
    let errors = [
        io::Error::from_raw_os_error(libc::EACCES),
        io::Error::new(io::ErrorKind::OutOfMemory, "synthetic out of memory"),
    ];

    for error in errors {
        let original_kind = error.kind();
        let original_raw = error.raw_os_error();
        let original_text = error.to_string();
        let diagnostic = setup_diagnostic(&error).to_string();
        let lower = diagnostic.to_ascii_lowercase();

        assert!(diagnostic.contains(&original_text), "{diagnostic:?}");
        assert!(
            lower.contains("driver"),
            "missing driver setup context: {diagnostic:?}"
        );
        assert!(
            lower.contains("setup") || lower.contains("set up"),
            "missing setup context: {diagnostic:?}"
        );
        assert!(
            !lower.contains("memlock"),
            "misattributed error: {diagnostic:?}"
        );
        assert!(
            !lower.contains("ring allocation"),
            "misattributed error: {diagnostic:?}"
        );
        assert!(
            !lower.contains("memory pressure"),
            "misattributed error: {diagnostic:?}"
        );

        assert_eq!(error.kind(), original_kind);
        assert_eq!(error.raw_os_error(), original_raw);
        assert_eq!(error.to_string(), original_text);
    }
}

#[test]
fn zero_memlock_setup_reports_enomem_and_captured_limits() {
    if std::env::var_os(CHILD_MODE).is_some() {
        zero_memlock_child();
        return;
    }

    let parent_before = memlock_limits().expect("read parent RLIMIT_MEMLOCK");
    let output = Command::new(std::env::current_exe().expect("test executable path"))
        .arg("--exact")
        .arg("zero_memlock_setup_reports_enomem_and_captured_limits")
        .arg("--nocapture")
        .env(CHILD_MODE, "1")
        .output()
        .expect("run isolated low-memlock child");
    let parent_after = memlock_limits().expect("read parent RLIMIT_MEMLOCK after child");

    assert_eq!(parent_after, parent_before, "child changed parent limits");
    assert!(
        output.status.success(),
        "low-memlock child failed ({}):\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn zero_memlock_child() {
    let (_, hard_before) = memlock_limits().expect("read child RLIMIT_MEMLOCK");
    set_memlock_soft(0).expect("lower only child soft RLIMIT_MEMLOCK");
    let (soft_after, hard_after) = memlock_limits().expect("read lowered child limits");
    assert_eq!(soft_after, 0);
    assert_eq!(hard_after, hard_before, "child hard limit changed");
    assert_ne!(hard_after, 0, "hard limit prevents snapshot mutation test");

    let error = match Driver::<(), 1, 64>::new(2) {
        Ok(driver) => {
            drop(driver);
            panic!("ring creation unexpectedly succeeded with soft RLIMIT_MEMLOCK=0");
        }
        Err(error) => error,
    };
    assert_eq!(
        error.raw_os_error(),
        Some(libc::ENOMEM),
        "low-memlock setup returned a non-ENOMEM error: {error:?}"
    );
    assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);

    let original_kind = error.kind();
    let original_raw = error.raw_os_error();
    let original_text = error.to_string();
    let diagnostic = setup_diagnostic(&error);

    set_memlock_soft(1).expect("change child soft limit after diagnostic creation");
    let (soft_now, hard_now) = memlock_limits().expect("read changed child limits");
    assert_eq!(soft_now, 1);
    assert_eq!(hard_now, hard_before);

    let rendered = diagnostic.to_string();
    let lower = rendered.to_ascii_lowercase();
    assert!(rendered.contains(&original_text), "{rendered:?}");
    assert!(
        lower.contains("driver"),
        "missing driver setup context: {rendered:?}"
    );
    assert!(
        lower.contains("setup") || lower.contains("set up"),
        "missing setup context: {rendered:?}"
    );
    assert!(
        lower.contains("ring allocation") || lower.contains("allocate the ring"),
        "missing ring allocation context: {rendered:?}"
    );
    assert!(
        lower.contains("memlock"),
        "missing memlock guidance: {rendered:?}"
    );
    assert!(
        lower.contains("memory pressure"),
        "missing memory-pressure guidance: {rendered:?}"
    );
    assert!(
        ["may", "might", "could", "possible", "possibly", "potential"]
            .iter()
            .any(|qualifier| lower.contains(qualifier)),
        "guidance must describe a possible cause: {rendered:?}"
    );
    assert_limit_snapshot(&rendered, "soft", "hard", 0);
    assert_limit_snapshot(&rendered, "hard", "soft", hard_before);
    assert_eq!(
        rendered,
        diagnostic.to_string(),
        "formatting changed snapshot"
    );

    assert_eq!(error.kind(), original_kind);
    assert_eq!(error.raw_os_error(), original_raw);
    assert_eq!(error.to_string(), original_text);
}
