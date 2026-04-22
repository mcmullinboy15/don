//! `don exec` — run an arbitrary command with don's resolved environment.
//!
//! Prepends `<base>/.don/bin` to `PATH` (so services/tasks declared via a
//! `download` preset are available as normal binaries) and then `execvp`s
//! the command, replacing the current process. The user's terminal, signals,
//! and exit code all go to the child directly.

use std::ffi::CString;
use std::path::Path;

/// Errors returned by [`exec_with_don_path`]. These are only returned for
/// pre-exec failures — on success the function never returns.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    /// The command or an argument contained a NUL byte, which is illegal in
    /// argv per POSIX. `execvp` takes C strings so we reject them up front.
    #[error("argument contained a NUL byte")]
    NulArgument,
    /// `execvp` failed — typically "command not found" or permission denied.
    #[error("exec failed: {0}")]
    Exec(nix::errno::Errno),
}

/// Build a new `PATH` by prepending `<base>/.don/bin` to the inherited value.
/// The bin dir is added even if it doesn't exist yet — a user running
/// `don exec` before any download has happened still gets a valid PATH, and
/// downloads that arrive later (no daemon required) appear automatically.
///
/// The base dir is canonicalized when possible so the PATH entry stays
/// absolute — a child that chdirs won't lose track of the bin dir.
pub fn compose_path(base_dir: &Path, current_path: Option<&str>) -> std::ffi::OsString {
    use std::ffi::OsString;

    let absolute_base = std::fs::canonicalize(base_dir).unwrap_or_else(|_| base_dir.to_path_buf());
    let bin_dir = absolute_base.join(".don").join("bin");
    let mut out = OsString::from(bin_dir.as_os_str());
    if let Some(existing) = current_path
        && !existing.is_empty()
    {
        out.push(":");
        out.push(existing);
    }
    out
}

/// Replace the current process with `cmd args...`, using a `PATH` that has
/// `<base>/.don/bin` prepended. Returns only on failure (the successful case
/// is `execvp` which never returns).
pub fn exec_with_don_path(base_dir: &Path, cmd: &str, args: &[String]) -> Result<(), ExecError> {
    let new_path = compose_path(base_dir, std::env::var("PATH").ok().as_deref());
    // SAFETY: single-threaded at this point (we're pre-exec in main).
    // set_var mutates the process env which execvp then inherits.
    unsafe {
        std::env::set_var("PATH", &new_path);
    }

    let c_cmd = CString::new(cmd).map_err(|_| ExecError::NulArgument)?;
    let mut c_args: Vec<CString> = Vec::with_capacity(args.len() + 1);
    c_args.push(c_cmd.clone());
    for arg in args {
        c_args.push(CString::new(arg.as_str()).map_err(|_| ExecError::NulArgument)?);
    }

    // execvp uses the inherited env (including the PATH we just set), so the
    // command is looked up in `<base>/.don/bin` first.
    match nix::unistd::execvp(&c_cmd, &c_args) {
        Ok(never) => match never {},
        Err(errno) => Err(ExecError::Exec(errno)),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn compose_path_prepends_bin_dir() {
        let base = PathBuf::from("/project");
        let composed = compose_path(&base, Some("/usr/bin:/bin"));
        assert_eq!(composed, std::ffi::OsString::from("/project/.don/bin:/usr/bin:/bin"));
    }

    #[test]
    fn compose_path_handles_missing_existing_path() {
        let base = PathBuf::from("/project");
        let composed = compose_path(&base, None);
        assert_eq!(composed, std::ffi::OsString::from("/project/.don/bin"));
    }

    #[test]
    fn compose_path_handles_empty_existing_path() {
        let base = PathBuf::from("/project");
        let composed = compose_path(&base, Some(""));
        assert_eq!(composed, std::ffi::OsString::from("/project/.don/bin"));
    }
}
