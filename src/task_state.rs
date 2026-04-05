//! Task state tracking — determines whether a task needs to re-run
//! based on file content hashes.
//!
//! State is stored in `.don/task-state/<task-name>.sha256`.
//! A hash is only written after a task exits successfully (exit code 0).

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Manages task state — tracks file hashes to determine whether a task needs to re-run.
///
/// State is stored in `.don/task-state/<task-name>.sha256`.
/// A hash is only written after a task exits successfully (exit code 0).
#[derive(Clone)]
pub struct TaskState {
    state_dir: PathBuf,
}

impl Default for TaskState {
    fn default() -> Self {
        Self::new(PathBuf::from(".don").join("task-state"))
    }
}

impl TaskState {
    /// Create a new `TaskState` that stores hashes in the given directory.
    pub fn new(state_dir: PathBuf) -> Self {
        Self { state_dir }
    }

    /// Check whether a task needs to run based on its watch patterns.
    ///
    /// Returns `true` if:
    /// - The task has no watch patterns (always runs)
    /// - There is no stored hash (never succeeded before)
    /// - The current file hash differs from the stored hash
    ///
    /// `base_dir` is prepended to glob patterns so they resolve relative to the
    /// task's working directory, not don's cwd. Pass `None` to resolve from cwd.
    ///
    /// Runs filesystem I/O on a blocking thread to avoid stalling the tokio runtime.
    pub async fn needs_run(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        base_dir: Option<&Path>,
    ) -> Result<bool, TaskStateError> {
        let this = self.clone();
        let task_name = task_name.to_string();
        let watch_patterns = watch_patterns.to_vec();
        let base_dir = base_dir.map(Path::to_path_buf);
        tokio::task::spawn_blocking(move || {
            this.needs_run_sync(&task_name, &watch_patterns, base_dir.as_deref())
        })
        .await
        .map_err(|e| TaskStateError::Io(std::io::Error::other(e)))?
    }

    /// Record a successful task run by writing the current file hash.
    /// Only call this after the task exits with code 0.
    ///
    /// `base_dir` must match what was passed to `needs_run`.
    ///
    /// Runs filesystem I/O on a blocking thread to avoid stalling the tokio runtime.
    pub async fn record_success(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        base_dir: Option<&Path>,
    ) -> Result<(), TaskStateError> {
        let this = self.clone();
        let task_name = task_name.to_string();
        let watch_patterns = watch_patterns.to_vec();
        let base_dir = base_dir.map(Path::to_path_buf);
        tokio::task::spawn_blocking(move || {
            this.record_success_sync(&task_name, &watch_patterns, base_dir.as_deref())
        })
        .await
        .map_err(|e| TaskStateError::Io(std::io::Error::other(e)))?
    }

    /// Clear stored state for a task, forcing it to re-run next time.
    ///
    /// Runs filesystem I/O on a blocking thread to avoid stalling the tokio runtime.
    pub async fn clear(&self, task_name: &str) -> Result<(), TaskStateError> {
        let task_name = task_name.to_string();
        let path = self.hash_file_path(&task_name);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(TaskStateError::Io(e)),
        }
    }

    fn needs_run_sync(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        base_dir: Option<&Path>,
    ) -> Result<bool, TaskStateError> {
        if watch_patterns.is_empty() {
            return Ok(true);
        }

        let current_hash = self.compute_hash(watch_patterns, base_dir)?;
        let stored_hash = self.read_stored_hash(task_name)?;

        Ok(stored_hash.as_ref() != Some(&current_hash))
    }

    fn record_success_sync(
        &self,
        task_name: &str,
        watch_patterns: &[String],
        base_dir: Option<&Path>,
    ) -> Result<(), TaskStateError> {
        if watch_patterns.is_empty() {
            return Ok(());
        }

        let hash = self.compute_hash(watch_patterns, base_dir)?;
        let path = self.hash_file_path(task_name);
        std::fs::create_dir_all(&self.state_dir)?;
        std::fs::write(&path, hash.as_bytes())?;
        Ok(())
    }

    /// Compute a combined SHA-256 hash of all files matching the watch patterns.
    ///
    /// The hash includes:
    /// - The sorted list of matched file paths (so adding/removing files triggers a change)
    /// - The contents of each file
    fn compute_hash(
        &self,
        watch_patterns: &[String],
        base_dir: Option<&Path>,
    ) -> Result<String, TaskStateError> {
        let mut paths = Vec::new();
        for pattern in watch_patterns {
            let full_pattern = match base_dir {
                Some(dir) => dir.join(pattern).to_string_lossy().into_owned(),
                None => pattern.clone(),
            };
            for entry in
                glob::glob(&full_pattern).map_err(|e| TaskStateError::Glob(e.to_string()))?
            {
                let path = entry.map_err(|e| TaskStateError::Io(e.into_error()))?;
                if path.is_file() {
                    paths.push(path);
                }
            }
        }
        paths.sort();
        paths.dedup();

        let mut hasher = Sha256::new();

        // Hash the file list itself so adding/removing files is detected
        for path in &paths {
            hasher.update(path.to_string_lossy().as_bytes());
            hasher.update(b"\0");
        }

        // Hash each file's contents
        for path in &paths {
            let contents = std::fs::read(path)?;
            hasher.update(&contents);
            hasher.update(b"\0");
        }

        Ok(format!("{:x}", hasher.finalize()))
    }

    fn hash_file_path(&self, task_name: &str) -> PathBuf {
        self.state_dir.join(format!("{task_name}.sha256"))
    }

    fn read_stored_hash(&self, task_name: &str) -> Result<Option<String>, TaskStateError> {
        let path = self.hash_file_path(task_name);
        match std::fs::read_to_string(&path) {
            Ok(hash) => Ok(Some(hash)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(TaskStateError::Io(e)),
        }
    }
}

/// Errors from task state operations.
/// Errors from task state operations.
#[derive(Debug, thiserror::Error)]
pub enum TaskStateError {
    /// A filesystem operation failed (reading files, writing state, etc.).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// A watch glob pattern was invalid.
    #[error("glob error: {0}")]
    Glob(String),
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::fs;

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir()
                .join("don-test")
                .join(name)
                .join(format!("{}", std::process::id()));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[tokio::test]
    async fn test_task_state() {
        struct TestCase {
            name: &'static str,
            setup: fn(&Path),
            patterns: Vec<String>,
            expect_needs_run_before: bool,
            record_success: bool,
            mutate: Option<fn(&Path)>,
            expect_needs_run_after: bool,
        }

        let cases = vec![
            TestCase {
                name: "no watch patterns always needs run",
                setup: |_| {},
                patterns: vec![],
                expect_needs_run_before: true,
                record_success: true,
                mutate: None,
                expect_needs_run_after: true,
            },
            TestCase {
                name: "first run always needs run",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                expect_needs_run_before: true,
                record_success: true,
                mutate: None,
                expect_needs_run_after: false,
            },
            TestCase {
                name: "unchanged files skip",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                expect_needs_run_before: true,
                record_success: true,
                mutate: None,
                expect_needs_run_after: false,
            },
            TestCase {
                name: "modified file triggers re-run",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                expect_needs_run_before: true,
                record_success: true,
                mutate: Some(|dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a_v2;").unwrap();
                }),
                expect_needs_run_after: true,
            },
            TestCase {
                name: "new file triggers re-run",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                expect_needs_run_before: true,
                record_success: true,
                mutate: Some(|dir| {
                    fs::write(dir.join("b.sql"), "CREATE TABLE b;").unwrap();
                }),
                expect_needs_run_after: true,
            },
            TestCase {
                name: "deleted file triggers re-run",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                    fs::write(dir.join("b.sql"), "CREATE TABLE b;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                expect_needs_run_before: true,
                record_success: true,
                mutate: Some(|dir| {
                    fs::remove_file(dir.join("b.sql")).unwrap();
                }),
                expect_needs_run_after: true,
            },
            TestCase {
                name: "failed task still needs run",
                setup: |dir| {
                    fs::write(dir.join("a.sql"), "CREATE TABLE a;").unwrap();
                },
                patterns: vec!["PLACEHOLDER/*.sql".to_string()],
                expect_needs_run_before: true,
                record_success: false, // simulate failure
                mutate: None,
                expect_needs_run_after: true,
            },
        ];

        for case in &cases {
            let dir = TempDir::new(case.name);
            let state_dir = dir.path().join(".don-state");
            let state = TaskState::new(state_dir);

            (case.setup)(dir.path());

            let patterns: Vec<String> = case
                .patterns
                .iter()
                .map(|p| p.replace("PLACEHOLDER", &dir.path().to_string_lossy()))
                .collect();

            let needs_run = state.needs_run("test-task", &patterns, None).await.unwrap();
            assert_eq!(
                needs_run, case.expect_needs_run_before,
                "case '{}': needs_run before",
                case.name
            );

            if case.record_success {
                state
                    .record_success("test-task", &patterns, None)
                    .await
                    .unwrap();
            }

            if let Some(mutate) = case.mutate {
                mutate(dir.path());
            }

            let needs_run = state.needs_run("test-task", &patterns, None).await.unwrap();
            assert_eq!(
                needs_run, case.expect_needs_run_after,
                "case '{}': needs_run after",
                case.name
            );
        }
    }

    #[tokio::test]
    async fn test_base_dir_resolution() {
        let dir = TempDir::new("base-dir");
        let sub = dir.path().join("subdir");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("a.sql"), "CREATE TABLE a;").unwrap();

        let state = TaskState::new(dir.path().join(".don-state"));
        let patterns = vec!["*.sql".to_string()];

        // Without base_dir: glob resolves from cwd, won't find files in subdir
        let needs_run_no_base = state.needs_run("test", &patterns, None).await.unwrap();
        // With base_dir pointing to subdir: should find the file
        let needs_run_with_base = state
            .needs_run("test", &patterns, Some(&sub))
            .await
            .unwrap();

        // The cwd-relative glob likely finds nothing (no *.sql in cwd), so always needs run
        assert!(needs_run_no_base);
        // The subdir glob finds a.sql, and there's no stored hash, so also needs run
        assert!(needs_run_with_base);

        // Record success with base_dir
        state
            .record_success("test", &patterns, Some(&sub))
            .await
            .unwrap();
        // Now it should skip
        assert!(
            !state
                .needs_run("test", &patterns, Some(&sub))
                .await
                .unwrap()
        );
        // But without base_dir it still needs run (different glob results)
        assert!(state.needs_run("test", &patterns, None).await.unwrap());
    }

    #[tokio::test]
    async fn test_clear() {
        let dir = TempDir::new("clear");
        let state = TaskState::new(dir.path().join(".don-state"));

        fs::write(dir.path().join("a.txt"), "hello").unwrap();
        let patterns = vec![format!("{}/*.txt", dir.path().to_string_lossy())];

        state
            .record_success("my-task", &patterns, None)
            .await
            .unwrap();
        assert!(!state.needs_run("my-task", &patterns, None).await.unwrap());

        state.clear("my-task").await.unwrap();
        assert!(state.needs_run("my-task", &patterns, None).await.unwrap());

        // Clear on non-existent is fine
        state.clear("never-existed").await.unwrap();
    }
}
