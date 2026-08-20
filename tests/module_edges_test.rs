//! The module edges this architecture rests on, enforced by grep.
//!
//! Both are stated as rules in `CLAUDE.md` and both were established by
//! deliberate work — `process` was extracted out of `runner`, and the TUI's
//! in-process privileges were removed one import at a time. A rule with no
//! test erodes silently, and these two erode first: the natural thing to
//! reach for when a supervisor needs a fact is the runner type that already
//! has it.

use std::fs;
use std::path::Path;

/// Every `.rs` file under `dir`, recursively.
fn rust_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(rust_files(&path));
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            found.push(path);
        }
    }
    found
}

/// Lines that reference `needle` as code rather than prose. Doc comments and
/// ordinary comments are allowed to name the other side — explaining the
/// boundary is the point.
fn code_references(path: &Path, needle: &str) -> Vec<String> {
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new();
    };
    text.lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            !trimmed.starts_with("//") && line.contains(needle)
        })
        .map(|line| format!("{}: {}", path.display(), line.trim()))
        .collect()
}

#[test]
fn module_edges_hold() {
    struct Case {
        /// Directory whose contents must not reach for `forbidden`.
        dir: &'static str,
        forbidden: &'static str,
        why: &'static str,
    }

    let cases = [
        Case {
            dir: "src/process",
            forbidden: "crate::runner",
            why: "processes report; the runner folds. A supervisor that can name a \
                  runner type can read scheduling state, and the direction of the \
                  dependency is the whole design.",
        },
        Case {
            dir: "src/tui",
            forbidden: "crate::runner",
            why: "the TUI is an ordinary socket client with no in-process \
                  privileges — that is what lets it detach, reattach, and run \
                  several at once.",
        },
    ];

    for case in cases {
        let mut violations = Vec::new();
        for file in rust_files(Path::new(case.dir)) {
            violations.extend(code_references(&file, case.forbidden));
        }
        assert!(
            violations.is_empty(),
            "{} must not reference {} — {}\nfound:\n{}",
            case.dir,
            case.forbidden,
            case.why,
            violations.join("\n")
        );
    }
}
