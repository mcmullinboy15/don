//! Config diffing for live reload.
//!
//! Compares two `Config` values and returns the set of services and tasks
//! that were added, removed, or changed. Used by the runner to apply
//! minimal changes when `don.toml` is modified while the daemon is running.

use super::Config;

/// The difference between two configs.
#[derive(Debug, Default)]
pub struct ConfigDiff {
    pub added_services: Vec<String>,
    pub removed_services: Vec<String>,
    pub changed_services: Vec<String>,
    pub added_tasks: Vec<String>,
    pub removed_tasks: Vec<String>,
    pub changed_tasks: Vec<String>,
}

impl ConfigDiff {
    /// True if no services or tasks were added, removed, or changed.
    pub fn is_empty(&self) -> bool {
        self.added_services.is_empty()
            && self.removed_services.is_empty()
            && self.changed_services.is_empty()
            && self.added_tasks.is_empty()
            && self.removed_tasks.is_empty()
            && self.changed_tasks.is_empty()
    }
}

impl std::fmt::Display for ConfigDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if !self.added_services.is_empty() {
            parts.push(format!("added services: {}", self.added_services.join(", ")));
        }
        if !self.removed_services.is_empty() {
            parts.push(format!("removed services: {}", self.removed_services.join(", ")));
        }
        if !self.changed_services.is_empty() {
            parts.push(format!("changed services: {}", self.changed_services.join(", ")));
        }
        if !self.added_tasks.is_empty() {
            parts.push(format!("added tasks: {}", self.added_tasks.join(", ")));
        }
        if !self.removed_tasks.is_empty() {
            parts.push(format!("removed tasks: {}", self.removed_tasks.join(", ")));
        }
        if !self.changed_tasks.is_empty() {
            parts.push(format!("changed tasks: {}", self.changed_tasks.join(", ")));
        }
        write!(f, "{}", parts.join("; "))
    }
}

/// Compare two configs and return the diff.
pub fn diff_configs(old: &Config, new: &Config) -> ConfigDiff {
    let mut diff = ConfigDiff::default();

    // Services
    for name in old.services.keys() {
        if !new.services.contains_key(name) {
            diff.removed_services.push(name.clone());
        } else if old.services.get(name) != new.services.get(name) {
            diff.changed_services.push(name.clone());
        }
    }
    for name in new.services.keys() {
        if !old.services.contains_key(name) {
            diff.added_services.push(name.clone());
        }
    }

    // Tasks
    for name in old.tasks.keys() {
        if !new.tasks.contains_key(name) {
            diff.removed_tasks.push(name.clone());
        } else if old.tasks.get(name) != new.tasks.get(name) {
            diff.changed_tasks.push(name.clone());
        }
    }
    for name in new.tasks.keys() {
        if !old.tasks.contains_key(name) {
            diff.added_tasks.push(name.clone());
        }
    }

    // Sort for deterministic output.
    diff.added_services.sort();
    diff.removed_services.sort();
    diff.changed_services.sort();
    diff.added_tasks.sort();
    diff.removed_tasks.sort();
    diff.changed_tasks.sort();

    diff
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn parse(toml: &str) -> Config {
        toml.parse().unwrap()
    }

    struct Case {
        name: &'static str,
        old: &'static str,
        new: &'static str,
        added_svc: Vec<&'static str>,
        removed_svc: Vec<&'static str>,
        changed_svc: Vec<&'static str>,
        added_task: Vec<&'static str>,
        removed_task: Vec<&'static str>,
        changed_task: Vec<&'static str>,
    }

    #[test]
    fn diff_table() {
        let cases = vec![
            Case {
                name: "identical configs",
                old: r#"
                    [services.api]
                    run.cmd = "mybin"
                "#,
                new: r#"
                    [services.api]
                    run.cmd = "mybin"
                "#,
                added_svc: vec![],
                removed_svc: vec![],
                changed_svc: vec![],
                added_task: vec![],
                removed_task: vec![],
                changed_task: vec![],
            },
            Case {
                name: "added service",
                old: r#"
                    [services.api]
                    run.cmd = "mybin"
                "#,
                new: r#"
                    [services.api]
                    run.cmd = "mybin"
                    [services.worker]
                    run.cmd = "worker"
                "#,
                added_svc: vec!["worker"],
                removed_svc: vec![],
                changed_svc: vec![],
                added_task: vec![],
                removed_task: vec![],
                changed_task: vec![],
            },
            Case {
                name: "removed service",
                old: r#"
                    [services.api]
                    run.cmd = "mybin"
                    [services.worker]
                    run.cmd = "worker"
                "#,
                new: r#"
                    [services.api]
                    run.cmd = "mybin"
                "#,
                added_svc: vec![],
                removed_svc: vec!["worker"],
                changed_svc: vec![],
                added_task: vec![],
                removed_task: vec![],
                changed_task: vec![],
            },
            Case {
                name: "changed service env",
                old: r#"
                    [services.api]
                    run.cmd = "mybin"
                    env = { PORT = "3000" }
                "#,
                new: r#"
                    [services.api]
                    run.cmd = "mybin"
                    env = { PORT = "4000" }
                "#,
                added_svc: vec![],
                removed_svc: vec![],
                changed_svc: vec!["api"],
                added_task: vec![],
                removed_task: vec![],
                changed_task: vec![],
            },
            Case {
                name: "changed service watch patterns",
                old: r#"
                    [services.api]
                    run.cmd = "mybin"
                    watch = ["src/**/*.rs"]
                "#,
                new: r#"
                    [services.api]
                    run.cmd = "mybin"
                    watch = ["src/**/*.rs", "Cargo.toml"]
                "#,
                added_svc: vec![],
                removed_svc: vec![],
                changed_svc: vec!["api"],
                added_task: vec![],
                removed_task: vec![],
                changed_task: vec![],
            },
            Case {
                name: "added task",
                old: "",
                new: r#"
                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up"]
                "#,
                added_svc: vec![],
                removed_svc: vec![],
                changed_svc: vec![],
                added_task: vec!["migrate"],
                removed_task: vec![],
                changed_task: vec![],
            },
            Case {
                name: "removed task",
                old: r#"
                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up"]
                "#,
                new: "",
                added_svc: vec![],
                removed_svc: vec![],
                changed_svc: vec![],
                added_task: vec![],
                removed_task: vec!["migrate"],
                changed_task: vec![],
            },
            Case {
                name: "changed task",
                old: r#"
                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up"]
                "#,
                new: r#"
                    [tasks.migrate]
                    cmd = "dbmate"
                    args = ["up", "--no-dump-schema"]
                "#,
                added_svc: vec![],
                removed_svc: vec![],
                changed_svc: vec![],
                added_task: vec![],
                removed_task: vec![],
                changed_task: vec!["migrate"],
            },
            Case {
                name: "mixed changes",
                old: r#"
                    [services.api]
                    run.cmd = "api"
                    [services.old]
                    run.cmd = "old"
                    [tasks.migrate]
                    cmd = "dbmate"
                "#,
                new: r#"
                    [services.api]
                    run.cmd = "api-v2"
                    [services.new]
                    run.cmd = "new"
                    [tasks.seed]
                    cmd = "seed"
                "#,
                added_svc: vec!["new"],
                removed_svc: vec!["old"],
                changed_svc: vec!["api"],
                added_task: vec!["seed"],
                removed_task: vec!["migrate"],
                changed_task: vec![],
            },
            Case {
                name: "both empty",
                old: "",
                new: "",
                added_svc: vec![],
                removed_svc: vec![],
                changed_svc: vec![],
                added_task: vec![],
                removed_task: vec![],
                changed_task: vec![],
            },
        ];

        for case in cases {
            let old = parse(case.old);
            let new = parse(case.new);
            let diff = diff_configs(&old, &new);

            assert_eq!(
                diff.added_services, case.added_svc,
                "case '{}': added_services", case.name
            );
            assert_eq!(
                diff.removed_services, case.removed_svc,
                "case '{}': removed_services", case.name
            );
            assert_eq!(
                diff.changed_services, case.changed_svc,
                "case '{}': changed_services", case.name
            );
            assert_eq!(
                diff.added_tasks, case.added_task,
                "case '{}': added_tasks", case.name
            );
            assert_eq!(
                diff.removed_tasks, case.removed_task,
                "case '{}': removed_tasks", case.name
            );
            assert_eq!(
                diff.changed_tasks, case.changed_task,
                "case '{}': changed_tasks", case.name
            );

            let expected_empty = case.added_svc.is_empty()
                && case.removed_svc.is_empty()
                && case.changed_svc.is_empty()
                && case.added_task.is_empty()
                && case.removed_task.is_empty()
                && case.changed_task.is_empty();
            assert_eq!(diff.is_empty(), expected_empty, "case '{}': is_empty", case.name);
        }
    }
}
