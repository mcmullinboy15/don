//! Failure-summary data and presentation shared by the TUI app and renderer.

use std::collections::HashMap;

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};

use crate::client::{ServiceState, TaskState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FailureKind {
    Service,
    Task,
}

impl FailureKind {
    fn label(self) -> &'static str {
        match self {
            Self::Service => "service",
            Self::Task => "task",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FailureState {
    Failed,
    DependencyFailed,
}

/// One root failure or dependency-blocked item shown by the failure summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FailureSummaryItem {
    pub(crate) kind: FailureKind,
    pub(crate) name: String,
    pub(crate) state: FailureState,
    pub(crate) failed_dependencies: Vec<String>,
}

pub(crate) fn has_failures(
    services: &HashMap<String, ServiceState>,
    tasks: &HashMap<String, TaskState>,
) -> bool {
    services
        .values()
        .any(|state| matches!(state, ServiceState::Failed | ServiceState::DependencyFailed))
        || tasks
            .values()
            .any(|state| matches!(state, TaskState::Failed | TaskState::DependencyFailed))
}

pub(crate) fn collect(
    services: &HashMap<String, ServiceState>,
    tasks: &HashMap<String, TaskState>,
    failed_dependencies: &HashMap<String, Vec<String>>,
) -> Vec<FailureSummaryItem> {
    let mut items = Vec::new();
    items.extend(services.iter().filter_map(|(name, state)| {
        let state = match state {
            ServiceState::Failed => FailureState::Failed,
            ServiceState::DependencyFailed => FailureState::DependencyFailed,
            _ => return None,
        };
        Some(summary_item(
            FailureKind::Service,
            name,
            state,
            failed_dependencies,
        ))
    }));
    items.extend(tasks.iter().filter_map(|(name, state)| {
        let state = match state {
            TaskState::Failed => FailureState::Failed,
            TaskState::DependencyFailed => FailureState::DependencyFailed,
            _ => return None,
        };
        Some(summary_item(
            FailureKind::Task,
            name,
            state,
            failed_dependencies,
        ))
    }));
    items.sort_by(|a, b| {
        a.state
            .cmp(&b.state)
            .then_with(|| a.kind.cmp(&b.kind))
            .then_with(|| a.name.cmp(&b.name))
    });
    items
}

fn summary_item(
    kind: FailureKind,
    name: &str,
    state: FailureState,
    failed_dependencies: &HashMap<String, Vec<String>>,
) -> FailureSummaryItem {
    FailureSummaryItem {
        kind,
        name: name.to_string(),
        state,
        failed_dependencies: failed_dependencies.get(name).cloned().unwrap_or_default(),
    }
}

pub(crate) fn text(items: &[FailureSummaryItem]) -> Text<'static> {
    if items.is_empty() {
        return Text::from(Line::from(Span::styled(
            "(no current failures)",
            Style::default().fg(Color::DarkGray),
        )));
    }

    let mut lines = Vec::new();
    let roots: Vec<&FailureSummaryItem> = items
        .iter()
        .filter(|item| item.state == FailureState::Failed)
        .collect();
    let blocked: Vec<&FailureSummaryItem> = items
        .iter()
        .filter(|item| item.state == FailureState::DependencyFailed)
        .collect();

    if !roots.is_empty() {
        lines.push(section_heading("ROOT FAILURES"));
        for item in roots {
            lines.push(item_line(item, Color::Red));
        }
    }

    if !blocked.is_empty() {
        if !lines.is_empty() {
            lines.push(Line::default());
        }
        lines.push(section_heading("BLOCKED BY DEPENDENCIES"));
        for item in blocked {
            lines.push(item_line(item, Color::Rgb(150, 60, 60)));
            let detail = if item.failed_dependencies.is_empty() {
                "root causes unavailable".to_string()
            } else {
                format!("root causes: {}", item.failed_dependencies.join(", "))
            };
            lines.push(Line::from(Span::styled(
                format!("    {detail}"),
                Style::default().fg(Color::Gray),
            )));
        }
    }

    Text::from(lines)
}

fn section_heading(label: &'static str) -> Line<'static> {
    Line::from(Span::styled(
        label,
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ))
}

fn item_line(item: &FailureSummaryItem, color: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("  {:<7} ", item.kind.label()),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            item.name.clone(),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
    ])
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn collect_orders_roots_before_blocked_items() {
        let services = HashMap::from([
            ("api".to_string(), ServiceState::DependencyFailed),
            ("db".to_string(), ServiceState::Failed),
            ("web".to_string(), ServiceState::Ready),
        ]);
        let tasks = HashMap::from([
            ("bootstrap".to_string(), TaskState::Failed),
            ("seed".to_string(), TaskState::DependencyFailed),
        ]);
        let dependencies = HashMap::from([
            ("api".to_string(), vec!["bootstrap".to_string()]),
            (
                "seed".to_string(),
                vec!["bootstrap".to_string(), "db".to_string()],
            ),
        ]);

        let items = collect(&services, &tasks, &dependencies);
        let got: Vec<(FailureKind, &str, FailureState)> = items
            .iter()
            .map(|item| (item.kind, item.name.as_str(), item.state))
            .collect();

        assert_eq!(
            got,
            vec![
                (FailureKind::Service, "db", FailureState::Failed),
                (FailureKind::Task, "bootstrap", FailureState::Failed),
                (FailureKind::Service, "api", FailureState::DependencyFailed),
                (FailureKind::Task, "seed", FailureState::DependencyFailed),
            ]
        );
        assert_eq!(items[3].failed_dependencies, ["bootstrap", "db"]);
    }

    #[test]
    fn text_keeps_every_root_cause_name() {
        let items = vec![FailureSummaryItem {
            kind: FailureKind::Task,
            name: "configure-everything".to_string(),
            state: FailureState::DependencyFailed,
            failed_dependencies: vec![
                "configure-kafka-topics".to_string(),
                "configure-mongo-collections".to_string(),
            ],
        }];

        let rendered = text(&items)
            .lines
            .iter()
            .flat_map(|line| line.spans.iter())
            .map(|span| span.content.as_ref())
            .collect::<Vec<&str>>()
            .join("");

        assert!(rendered.contains("configure-kafka-topics"));
        assert!(rendered.contains("configure-mongo-collections"));
    }
}
