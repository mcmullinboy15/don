use crate::globwalk::{glob_pattern_base_dir, matches_glob, matches_ignore};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub(crate) fn working_dir_for(base_dir: &Path, dir: Option<&Path>) -> PathBuf {
    match dir {
        Some(dir) => base_dir.join(dir),
        None => base_dir.to_path_buf(),
    }
}

fn resolve_glob_pattern(base_dir: &Path, pattern: &str) -> String {
    let path = Path::new(pattern);
    if path.is_absolute() {
        path.to_string_lossy().into_owned()
    } else {
        base_dir.join(path).to_string_lossy().into_owned()
    }
}

pub(crate) fn resolve_watch_ignore_patterns(
    item_base_dir: &Path,
    item_ignore_patterns: &[String],
    workspace_base_dir: &Path,
    global_watch_ignore: &[String],
) -> Vec<String> {
    let mut patterns: Vec<String> = item_ignore_patterns
        .iter()
        .map(|pattern| resolve_glob_pattern(item_base_dir, pattern))
        .collect();
    patterns.extend(
        global_watch_ignore
            .iter()
            .map(|pattern| resolve_glob_pattern(workspace_base_dir, pattern)),
    );
    patterns
}

pub(crate) fn any_glob_path_changed_since(
    base_dir: &Path,
    patterns: &[String],
    ignore_patterns: &[String],
    since: SystemTime,
) -> bool {
    let absolute_patterns: Vec<glob::Pattern> = patterns
        .iter()
        .filter_map(|pattern| glob::Pattern::new(&resolve_glob_pattern(base_dir, pattern)).ok())
        .collect();
    let absolute_ignore: Vec<glob::Pattern> = ignore_patterns
        .iter()
        .filter_map(|pattern| glob::Pattern::new(&resolve_glob_pattern(base_dir, pattern)).ok())
        .collect();
    let mut roots: Vec<PathBuf> = patterns
        .iter()
        .map(|pattern| glob_pattern_base_dir(Path::new(&resolve_glob_pattern(base_dir, pattern))))
        .collect();
    roots.sort();
    roots.dedup();

    roots
        .into_iter()
        .any(|root| scan_tree_for_changes(&root, &absolute_patterns, &absolute_ignore, since))
}

fn scan_tree_for_changes(
    path: &Path,
    patterns: &[glob::Pattern],
    ignore_patterns: &[glob::Pattern],
    since: SystemTime,
) -> bool {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return false;
    };
    let path_str = path.to_string_lossy();
    let ignored = ignore_patterns
        .iter()
        .any(|ignore| matches_ignore(ignore, &path_str));
    if ignored {
        return false;
    }
    if patterns
        .iter()
        .any(|pattern| matches_glob(pattern, &path_str))
    {
        let Ok(modified) = metadata.modified() else {
            return false;
        };
        if modified > since {
            return true;
        }
    }

    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return false;
    }

    let Ok(entries) = std::fs::read_dir(path) else {
        return false;
    };
    for entry in entries.flatten() {
        if scan_tree_for_changes(&entry.path(), patterns, ignore_patterns, since) {
            return true;
        }
    }

    false
}
