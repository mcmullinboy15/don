/*!
Shared helpers for the hand-rolled glob walks in `task_state` and
`runner::paths`, which replace `glob::glob` with a symlink-safe manual walk.
*/

use std::path::{Path, PathBuf};

/**
Match `path` against `pattern` component-wise like `glob::glob` — `*`/`?` never
cross `/`. `Pattern::matches` defaults the other way, so the walk must opt in.
*/
pub(crate) fn matches_glob(pattern: &glob::Pattern, path: &str) -> bool {
    pattern.matches_with(
        path,
        glob::MatchOptions {
            require_literal_separator: true,
            ..glob::MatchOptions::default()
        },
    )
}

/**
Whether any component of `pattern` holds a glob metacharacter (`*`, `?`, `[`).
A pattern with none is a literal path that resolves without walking.
*/
pub(crate) fn has_glob_metacharacters(pattern: &Path) -> bool {
    pattern.components().any(|component| {
        let s = component.as_os_str().to_string_lossy();
        s.contains('*') || s.contains('?') || s.contains('[')
    })
}

/**
Longest literal directory prefix of a glob pattern — the root to walk from.
A metacharacter-free pattern is a literal file, so its parent is returned.
*/
pub(crate) fn glob_pattern_base_dir(pattern: &Path) -> PathBuf {
    let mut base = PathBuf::new();
    let mut hit_glob = false;
    for component in pattern.components() {
        let s = component.as_os_str().to_string_lossy();
        if s.contains('*') || s.contains('?') || s.contains('[') {
            hit_glob = true;
            break;
        }
        base.push(component);
    }
    if !hit_glob {
        base = base.parent().map(Path::to_path_buf).unwrap_or_default();
    }
    if base.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        base
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_glob_star_does_not_cross_separator() {
        struct Case {
            pattern: &'static str,
            path: &'static str,
            want: bool,
        }
        let cases = [
            Case {
                pattern: "src/*.ts",
                path: "src/x.ts",
                want: true,
            },
            Case {
                pattern: "src/*.ts",
                path: "src/a/b.ts",
                want: false,
            },
            Case {
                pattern: "a/**/*.ts",
                path: "a/x.ts",
                want: true,
            },
            Case {
                pattern: "a/**/*.ts",
                path: "a/x/y/z.ts",
                want: true,
            },
        ];
        for case in cases {
            let pattern = glob::Pattern::new(case.pattern).unwrap();
            assert_eq!(
                matches_glob(&pattern, case.path),
                case.want,
                "pattern {:?} vs path {:?}",
                case.pattern,
                case.path
            );
        }
    }

    #[test]
    fn test_has_glob_metacharacters() {
        assert!(!has_glob_metacharacters(Path::new("/repo/package.json")));
        assert!(!has_glob_metacharacters(Path::new("yarn.lock")));
        assert!(has_glob_metacharacters(Path::new("/repo/src/*.ts")));
        assert!(has_glob_metacharacters(Path::new("/repo/**/x.ts")));
        assert!(has_glob_metacharacters(Path::new("/repo/[abc].ts")));
        assert!(has_glob_metacharacters(Path::new("/repo/a?.ts")));
    }

    #[test]
    fn test_glob_pattern_base_dir() {
        assert_eq!(
            glob_pattern_base_dir(Path::new("/repo/package.json")),
            PathBuf::from("/repo")
        );
        assert_eq!(
            glob_pattern_base_dir(Path::new("/repo/src/**/*.ts")),
            PathBuf::from("/repo/src")
        );
        assert_eq!(glob_pattern_base_dir(Path::new("*.ts")), PathBuf::from("."));
    }
}
