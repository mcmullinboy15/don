//! Where the panes are, and which one has focus.
//!
//! One function computes the rectangles and everything else reads them: the
//! renderer to draw, mouse handling to decide what was clicked, the divider
//! drag to know what it is dragging. Two places computing the same layout is
//! how a click ends up one row off from what it looks like it hit.
//!
//! The status pane is optional and resizable. It is *not* a mode: opening it
//! does not take the log away, which is the whole difference between this and
//! the full-screen tables. Those still exist for when you want the detail;
//! this is for keeping half an eye on things while you read output.

use ratatui::layout::{Constraint, Direction, Layout, Rect};

/// Which side of the screen the status pane occupies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum PaneSide {
    #[default]
    Right,
    Bottom,
}

impl PaneSide {
    pub(crate) fn toggled(self) -> Self {
        match self {
            Self::Right => Self::Bottom,
            Self::Bottom => Self::Right,
        }
    }
}

/// Which pane takes keys that both could claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Focus {
    #[default]
    Logs,
    Status,
}

/// The status pane's state: whether it is open, where, and how big.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StatusPane {
    pub(crate) open: bool,
    pub(crate) side: PaneSide,
    /// Size along the split axis — columns when docked right, rows when
    /// docked bottom. Clamped at layout time rather than at assignment, so a
    /// drag past the edge of a small terminal is remembered and comes back
    /// when the terminal grows.
    pub(crate) extent: u16,
}

impl Default for StatusPane {
    fn default() -> Self {
        Self {
            open: false,
            side: PaneSide::Right,
            extent: 42,
        }
    }
}

/// Smallest useful status pane, and the smallest log pane worth leaving.
const MIN_STATUS: u16 = 16;
const MIN_LOGS: u16 = 20;

/// Everywhere something can be drawn or clicked.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Panes {
    pub(crate) logs: Rect,
    /// `None` when the status pane is closed, or when the terminal is too
    /// small to split without starving the log.
    pub(crate) status: Option<Rect>,
    /// The one-cell line between them, for the resize drag. Empty when there
    /// is no split.
    pub(crate) divider: Option<Rect>,
    pub(crate) bar: Rect,
}

impl Panes {
    /// A layout with nothing in it, for before the first frame.
    pub(crate) fn empty() -> Self {
        Self {
            logs: Rect::new(0, 0, 0, 0),
            status: None,
            divider: None,
            bar: Rect::new(0, 0, 0, 0),
        }
    }

    /// Which pane a screen position falls in, if any.
    pub(crate) fn hit(&self, column: u16, row: u16) -> Option<Focus> {
        let inside = |rect: Rect| {
            column >= rect.x
                && column < rect.x + rect.width
                && row >= rect.y
                && row < rect.y + rect.height
        };
        if self.status.is_some_and(inside) {
            return Some(Focus::Status);
        }
        if inside(self.logs) {
            return Some(Focus::Logs);
        }
        None
    }

    /// Whether a screen position is on the divider, and so starts a resize.
    pub(crate) fn on_divider(&self, column: u16, row: u16) -> bool {
        self.divider.is_some_and(|rect| {
            column >= rect.x
                && column < rect.x + rect.width
                && row >= rect.y
                && row < rect.y + rect.height
        })
    }
}

/// Split `area` into the log pane, the optional status pane, and the bar.
pub(crate) fn layout(area: Rect, bar_height: u16, status: StatusPane) -> Panes {
    if area.height <= bar_height {
        return Panes {
            logs: Rect::new(area.x, area.y, area.width, 0),
            status: None,
            divider: None,
            bar: area,
        };
    }
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(bar_height)])
        .split(area);
    let (body, bar) = (rows[0], rows[1]);

    if !status.open {
        return Panes {
            logs: body,
            status: None,
            divider: None,
            bar,
        };
    }

    match status.side {
        PaneSide::Right => {
            // Too narrow to split: the log keeps the space. Silently refusing
            // beats a two-column status pane and a five-column log.
            if body.width < MIN_LOGS + MIN_STATUS + 1 {
                return Panes {
                    logs: body,
                    status: None,
                    divider: None,
                    bar,
                };
            }
            let width = status.extent.clamp(MIN_STATUS, body.width - MIN_LOGS - 1);
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Min(MIN_LOGS),
                    Constraint::Length(1),
                    Constraint::Length(width),
                ])
                .split(body);
            Panes {
                logs: chunks[0],
                divider: Some(chunks[1]),
                status: Some(chunks[2]),
                bar,
            }
        }
        PaneSide::Bottom => {
            if body.height < 6 + 1 + 3 {
                return Panes {
                    logs: body,
                    status: None,
                    divider: None,
                    bar,
                };
            }
            let height = status.extent.clamp(3, body.height - 6 - 1);
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(6),
                    Constraint::Length(1),
                    Constraint::Length(height),
                ])
                .split(body);
            Panes {
                logs: chunks[0],
                divider: Some(chunks[1]),
                status: Some(chunks[2]),
                bar,
            }
        }
    }
}

/// The extent a divider dragged to `position` implies.
///
/// Returned rather than applied so the caller decides whether the drag is
/// still live; clamping happens in [`layout`], which is the only place that
/// knows the current terminal size.
pub(crate) fn extent_from_drag(area: Rect, side: PaneSide, column: u16, row: u16) -> u16 {
    match side {
        PaneSide::Right => (area.x + area.width)
            .saturating_sub(column)
            .saturating_sub(1),
        PaneSide::Bottom => (area.y + area.height).saturating_sub(row).saturating_sub(1),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    const BAR: u16 = 3;

    /// Layout is the single source of truth for where things are, so the
    /// cases that matter are the ones where a naive split would produce a
    /// pane too small to use.
    #[test]
    fn the_status_pane_never_starves_the_log() {
        struct Case {
            name: &'static str,
            area: Rect,
            status: StatusPane,
            want_status: bool,
            want_log_width: Option<u16>,
        }

        let cases = vec![
            Case {
                name: "closed by default",
                area: Rect::new(0, 0, 120, 40),
                status: StatusPane::default(),
                want_status: false,
                want_log_width: Some(120),
            },
            Case {
                name: "docked right takes its extent, log keeps the rest",
                area: Rect::new(0, 0, 120, 40),
                status: StatusPane {
                    open: true,
                    side: PaneSide::Right,
                    extent: 40,
                },
                want_status: true,
                want_log_width: Some(79),
            },
            Case {
                name: "an oversized extent is clamped, not honoured",
                area: Rect::new(0, 0, 120, 40),
                status: StatusPane {
                    open: true,
                    side: PaneSide::Right,
                    extent: 500,
                },
                want_status: true,
                want_log_width: Some(MIN_LOGS),
            },
            Case {
                name: "too narrow to split at all: the log keeps everything",
                area: Rect::new(0, 0, 30, 40),
                status: StatusPane {
                    open: true,
                    side: PaneSide::Right,
                    extent: 40,
                },
                want_status: false,
                want_log_width: Some(30),
            },
            Case {
                name: "too short to split vertically",
                area: Rect::new(0, 0, 120, 12),
                status: StatusPane {
                    open: true,
                    side: PaneSide::Bottom,
                    extent: 20,
                },
                want_status: false,
                want_log_width: Some(120),
            },
        ];

        for case in cases {
            let panes = layout(case.area, BAR, case.status);
            assert_eq!(
                panes.status.is_some(),
                case.want_status,
                "{}: status pane presence",
                case.name
            );
            if let Some(width) = case.want_log_width {
                assert_eq!(panes.logs.width, width, "{}: log width", case.name);
            }
            assert_eq!(panes.bar.height, BAR, "{}: bar height", case.name);
            // A divider exists exactly when there is a split to divide.
            assert_eq!(
                panes.divider.is_some(),
                case.want_status,
                "{}: divider",
                case.name
            );
        }
    }

    /// A click has to land in the pane it looks like it landed in — the whole
    /// reason layout is computed once and read by everyone.
    #[test]
    fn hit_testing_agrees_with_the_layout() {
        let area = Rect::new(0, 0, 120, 40);
        let panes = layout(
            area,
            BAR,
            StatusPane {
                open: true,
                side: PaneSide::Right,
                extent: 40,
            },
        );
        let status = panes.status.unwrap();

        assert_eq!(panes.hit(0, 0), Some(Focus::Logs));
        assert_eq!(panes.hit(panes.logs.width - 1, 5), Some(Focus::Logs));
        assert_eq!(panes.hit(status.x, 5), Some(Focus::Status));
        assert_eq!(
            panes.hit(status.x + status.width - 1, 5),
            Some(Focus::Status)
        );
        assert!(
            panes.on_divider(panes.logs.width, 5),
            "the gap between them"
        );
        assert_eq!(panes.hit(0, 39), None, "the bar belongs to neither pane");
    }

    /// Dragging the divider left widens the status pane; the arithmetic is
    /// easy to get inverted, and inverted feels broken rather than wrong.
    #[test]
    fn dragging_the_divider_resizes_toward_the_pointer() {
        let area = Rect::new(0, 0, 120, 40);
        assert_eq!(extent_from_drag(area, PaneSide::Right, 80, 0), 39);
        assert_eq!(
            extent_from_drag(area, PaneSide::Right, 60, 0),
            59,
            "dragging further left gives the status pane more room"
        );
        assert_eq!(extent_from_drag(area, PaneSide::Bottom, 0, 30), 9);
    }
}
