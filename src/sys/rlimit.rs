//! Resource-limit management for Don and spawned children.
//!
//! Don raises a narrow set of soft limits at runner startup so both Don and
//! child services inherit enough headroom for large dev environments.

#[cfg(target_os = "linux")]
type RLimitResource = libc::__rlimit_resource_t;
#[cfg(target_os = "macos")]
type RLimitResource = libc::c_int;

#[derive(Debug, Clone)]
pub(crate) struct ResourceLimitOutcome {
    pub(crate) name: &'static str,
    pub(crate) before_soft: libc::rlim_t,
    pub(crate) after_soft: libc::rlim_t,
    pub(crate) hard: libc::rlim_t,
    pub(crate) error: Option<String>,
}

struct ResourceSpec {
    name: &'static str,
    resource: RLimitResource,
    explicit_system_limit: fn() -> Option<libc::rlim_t>,
}

/// Best-effort raise of Don's process soft limits to the inherited maximum.
///
/// Children inherit these limits from Don, so this mirrors Bazel's startup
/// behavior for resources that matter to dev-process orchestration.
pub(crate) fn raise_soft_resource_limits() -> Vec<ResourceLimitOutcome> {
    let specs = [
        ResourceSpec {
            name: "RLIMIT_NOFILE",
            resource: libc::RLIMIT_NOFILE,
            explicit_system_limit: nofile_system_limit,
        },
        ResourceSpec {
            name: "RLIMIT_NPROC",
            resource: libc::RLIMIT_NPROC,
            explicit_system_limit: nproc_system_limit,
        },
    ];

    specs.iter().map(raise_soft_resource_limit).collect()
}

fn raise_soft_resource_limit(spec: &ResourceSpec) -> ResourceLimitOutcome {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // Safety: `limit` points at valid initialized storage for getrlimit().
    if unsafe { libc::getrlimit(spec.resource, &mut limit) } != 0 {
        return ResourceLimitOutcome {
            name: spec.name,
            before_soft: 0,
            after_soft: 0,
            hard: 0,
            error: Some(format!(
                "getrlimit failed: {}",
                std::io::Error::last_os_error()
            )),
        };
    }

    let before_soft = limit.rlim_cur;
    let target_soft = target_soft_limit(
        limit.rlim_cur,
        limit.rlim_max,
        (spec.explicit_system_limit)(),
    );
    if target_soft == limit.rlim_cur {
        return ResourceLimitOutcome {
            name: spec.name,
            before_soft,
            after_soft: limit.rlim_cur,
            hard: limit.rlim_max,
            error: None,
        };
    }

    limit.rlim_cur = target_soft;
    // Safety: `limit` contains the current hard limit from getrlimit() and a
    // soft limit no greater than that hard limit (or a platform system cap).
    if unsafe { libc::setrlimit(spec.resource, &limit) } != 0 {
        return ResourceLimitOutcome {
            name: spec.name,
            before_soft,
            after_soft: before_soft,
            hard: limit.rlim_max,
            error: Some(format!(
                "setrlimit to {} failed: {}",
                format_rlim(target_soft),
                std::io::Error::last_os_error()
            )),
        };
    }

    ResourceLimitOutcome {
        name: spec.name,
        before_soft,
        after_soft: target_soft,
        hard: limit.rlim_max,
        error: None,
    }
}

fn target_soft_limit(
    current_soft: libc::rlim_t,
    hard: libc::rlim_t,
    explicit_system_limit: Option<libc::rlim_t>,
) -> libc::rlim_t {
    if current_soft == hard {
        return current_soft;
    }

    match explicit_system_limit {
        Some(limit) if hard == libc::RLIM_INFINITY || hard > limit => limit,
        _ => hard,
    }
}

pub(crate) fn format_outcome(outcome: &ResourceLimitOutcome) -> Option<String> {
    if let Some(ref error) = outcome.error {
        return Some(format!(
            "{}: failed to raise soft limit {} -> hard {} ({error})",
            outcome.name,
            format_rlim(outcome.before_soft),
            format_rlim(outcome.hard),
        ));
    }
    if outcome.after_soft == outcome.before_soft {
        return None;
    }
    Some(format!(
        "{}: raised soft limit {} -> {} (hard {})",
        outcome.name,
        format_rlim(outcome.before_soft),
        format_rlim(outcome.after_soft),
        format_rlim(outcome.hard),
    ))
}

fn format_rlim(value: libc::rlim_t) -> String {
    if value == libc::RLIM_INFINITY {
        "unlimited".to_string()
    } else {
        value.to_string()
    }
}

#[cfg(target_os = "macos")]
fn nofile_system_limit() -> Option<libc::rlim_t> {
    sysctl_limit("kern.maxfilesperproc")
}

#[cfg(not(target_os = "macos"))]
fn nofile_system_limit() -> Option<libc::rlim_t> {
    None
}

#[cfg(target_os = "macos")]
fn nproc_system_limit() -> Option<libc::rlim_t> {
    sysctl_limit("kern.maxprocperuid")
}

#[cfg(not(target_os = "macos"))]
fn nproc_system_limit() -> Option<libc::rlim_t> {
    None
}

#[cfg(target_os = "macos")]
fn sysctl_limit(name: &str) -> Option<libc::rlim_t> {
    let c_name = std::ffi::CString::new(name).ok()?;
    let mut value: i32 = 0;
    let mut len = std::mem::size_of::<i32>();
    // Safety: `c_name` is a valid NUL-terminated sysctl name, and `value` /
    // `len` point at writable storage for the result.
    let ret = unsafe {
        libc::sysctlbyname(
            c_name.as_ptr(),
            std::ptr::addr_of_mut!(value).cast(),
            std::ptr::addr_of_mut!(len),
            std::ptr::null_mut(),
            0,
        )
    };
    if ret != 0 || len != std::mem::size_of::<i32>() || value <= 0 {
        return None;
    }
    Some(value as libc::rlim_t)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::target_soft_limit;

    #[test]
    fn target_soft_limit_uses_hard_when_no_explicit_system_limit() {
        assert_eq!(target_soft_limit(256, 1024, None), 1024);
    }

    #[test]
    fn target_soft_limit_uses_explicit_cap_when_hard_exceeds_it() {
        assert_eq!(target_soft_limit(256, 100_000, Some(92_160)), 92_160);
    }

    #[test]
    fn target_soft_limit_keeps_hard_when_below_explicit_cap() {
        assert_eq!(target_soft_limit(256, 4096, Some(92_160)), 4096);
    }

    #[test]
    fn target_soft_limit_is_noop_when_already_at_hard() {
        assert_eq!(target_soft_limit(4096, 4096, Some(92_160)), 4096);
    }
}
