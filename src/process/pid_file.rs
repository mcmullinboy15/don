//! Flock-based PID file locking for don's own PID file.
//!
//! Don acquires a PID file at `.don/don.pid` with an `flock` to detect
//! if another don instance is already running. The lock provides a
//! definitive answer without worrying about PID recycling.

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;

/// An flock-held PID file for don's own process.
///
/// The `Flock<File>` is held for the lifetime of this struct.
/// When dropped, the lock is released automatically.
pub struct PidFile {
    _lock: Flock<File>,
    path: PathBuf,
}

/// Errors from PID file operations.
#[derive(Debug, thiserror::Error)]
pub enum PidFileError {
    /// Failed to create parent directories.
    #[error("failed to create pid file directory '{}': {source}", path.display())]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Failed to open or create the PID file.
    #[error("failed to open pid file '{}': {source}", path.display())]
    Open {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Failed to acquire flock — another process holds it.
    #[error("lock held by another process (already running)")]
    AlreadyLocked,
    /// Failed to acquire flock for a non-contention reason.
    #[error("flock failed on '{}': {source}", path.display())]
    Flock {
        path: PathBuf,
        #[source]
        source: Errno,
    },
    /// Failed to write PID to the file.
    #[error("failed to write pid to '{}': {source}", path.display())]
    WritePid {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Blocking task was cancelled.
    #[error("pid file task was cancelled")]
    TaskCancelled,
}

impl PidFile {
    /// Acquire an exclusive, non-blocking flock on the given path and write the PID.
    ///
    /// Runs file I/O on a blocking thread via `tokio::task::spawn_blocking`.
    ///
    /// 1. Creates parent directories if needed.
    /// 2. Opens the file with `O_CREAT | O_CLOEXEC`.
    /// 3. Attempts `flock(LOCK_EX | LOCK_NB)`.
    /// 4. If the lock fails with EWOULDBLOCK, returns `PidFileError::AlreadyLocked`.
    /// 5. Truncates and writes the PID.
    ///
    /// The caller must keep the returned `PidFile` alive for the lifetime of the
    /// don process. Dropping it releases the lock.
    pub async fn acquire(path: PathBuf, pid: i32) -> Result<Self, PidFileError> {
        tokio::task::spawn_blocking(move || Self::acquire_sync(path, pid))
            .await
            .map_err(|_| PidFileError::TaskCancelled)?
    }

    fn acquire_sync(path: PathBuf, pid: i32) -> Result<Self, PidFileError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| PidFileError::CreateDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(nix::libc::O_CLOEXEC)
            .open(&path)
            .map_err(|source| PidFileError::Open {
                path: path.clone(),
                source,
            })?;

        let mut lock = match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => lock,
            Err((_file, Errno::EWOULDBLOCK)) => return Err(PidFileError::AlreadyLocked),
            Err((_file, errno)) => {
                return Err(PidFileError::Flock {
                    path,
                    source: errno,
                });
            }
        };

        // Truncate, write PID, and fsync to ensure durability
        lock.set_len(0).map_err(|source| PidFileError::WritePid {
            path: path.clone(),
            source,
        })?;
        write!(*lock, "{pid}").map_err(|source| PidFileError::WritePid {
            path: path.clone(),
            source,
        })?;
        lock.sync_all().map_err(|source| PidFileError::WritePid {
            path: path.clone(),
            source,
        })?;

        Ok(Self { _lock: lock, path })
    }

    /// The path to the PID file.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Delete the PID file from disk. Idempotent — does not error if already gone.
    pub async fn cleanup(path: PathBuf) -> Result<(), std::io::Error> {
        tokio::task::spawn_blocking(move || match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        })
        .await
        .map_err(std::io::Error::other)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::test_util::TempDir;

    #[tokio::test]
    async fn test_acquire_and_read_pid() {
        let dir = TempDir::new("acquire");
        let path = dir.path().join("test.pid");

        let pf = PidFile::acquire(path.clone(), 12345).await.unwrap();
        assert!(path.exists());

        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "12345");

        drop(pf);
    }

    #[tokio::test]
    async fn test_acquire_twice_fails() {
        let dir = TempDir::new("acquire-twice");
        let path = dir.path().join("test.pid");

        let _pf1 = PidFile::acquire(path.clone(), 12345).await.unwrap();
        let result = PidFile::acquire(path, 99999).await;
        assert!(matches!(result, Err(PidFileError::AlreadyLocked)));
    }

    #[tokio::test]
    async fn test_drop_releases_lock() {
        let dir = TempDir::new("drop-release");
        let path = dir.path().join("test.pid");

        let pf = PidFile::acquire(path.clone(), 12345).await.unwrap();
        drop(pf);

        let pf2 = PidFile::acquire(path, 67890).await.unwrap();
        drop(pf2);
    }

    #[tokio::test]
    async fn test_cleanup_removes_file() {
        let dir = TempDir::new("cleanup");
        let path = dir.path().join("test.pid");

        std::fs::write(&path, "12345").unwrap();
        assert!(path.exists());

        PidFile::cleanup(path.clone()).await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn test_cleanup_idempotent() {
        let dir = TempDir::new("cleanup-idempotent");
        let path = dir.path().join("nope.pid");

        PidFile::cleanup(path).await.unwrap();
    }

    #[tokio::test]
    async fn test_creates_parent_dirs() {
        let dir = TempDir::new("parent-dirs");
        let path = dir.path().join("nested").join("dir").join("test.pid");

        let pf = PidFile::acquire(path.clone(), 42).await.unwrap();
        assert!(path.exists());
        drop(pf);
    }
}
