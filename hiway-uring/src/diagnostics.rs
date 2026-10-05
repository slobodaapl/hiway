use std::io;

/// Formats an error returned by [`Driver::new`](crate::Driver::new) or
/// [`Driver::with_pool`](crate::Driver::with_pool).
///
/// For raw `ENOMEM`, this call captures `RLIMIT_MEMLOCK` for later formatting.
/// The output includes its soft and hard limits with guidance for checking
/// memlock headroom and memory availability. Finite limits are shown in bytes.
///
/// The formatter borrows the error and retains its original display text.
/// The caller keeps access to the error's kind and OS error code.
#[must_use]
pub fn setup_diagnostic(error: &io::Error) -> impl std::fmt::Display + '_ {
    let memlock = if error.raw_os_error() == Some(libc::ENOMEM) {
        read_memlock_limits()
    } else {
        None
    };

    SetupDiagnostic { error, memlock }
}

struct SetupDiagnostic<'a> {
    error: &'a io::Error,
    memlock: Option<(libc::rlim_t, libc::rlim_t)>,
}

impl std::fmt::Display for SetupDiagnostic<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "driver setup failed: {}", self.error)?;
        if self.error.raw_os_error() != Some(libc::ENOMEM) {
            return Ok(());
        }

        match self.memlock {
            Some((soft, hard)) => {
                f.write_str("; ring allocation failed; RLIMIT_MEMLOCK soft=")?;
                write_memlock_limit(f, soft)?;
                f.write_str(", hard=")?;
                write_memlock_limit(f, hard)?;
            }
            None => f.write_str("; ring allocation failed; RLIMIT_MEMLOCK unavailable")?,
        }

        f.write_str(
            "; allocation failure may reflect memlock exhaustion or memory pressure; check process/service memlock limits and memory availability",
        )
    }
}

fn read_memlock_limits() -> Option<(libc::rlim_t, libc::rlim_t)> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: limit points to writable storage for one libc::rlimit.
    if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &raw mut limit) } == 0 {
        Some((limit.rlim_cur, limit.rlim_max))
    } else {
        None
    }
}

fn write_memlock_limit(f: &mut std::fmt::Formatter<'_>, limit: libc::rlim_t) -> std::fmt::Result {
    if limit == libc::RLIM_INFINITY {
        f.write_str("unlimited")
    } else {
        write!(f, "{limit} bytes")
    }
}
