//! Where the panes are, and which one has focus.
//!
//! One function computes the rectangles and everything else reads them: the
//! renderer to draw, mouse handling to decide what was clicked, the divider
//! drag to know what it is dragging. Two places computing the same layout is
//! how a click ends up one row off from what it looks like it hit.
//!
//! The side panel is optional and resizable, and holds whichever view was
//! opened — services, tasks, or the log filter. It sits *beside* the log
//! rather than replacing it: acting on a process and watching what it prints
//! are one activity, and a full-screen table forced a choice between them.

use ratatui::layout::{Constraint, Direction, Layout, Rect};

/// Which side of the screen the side panel occupies.
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
    Panel,
}

/// Where the side panel goes and how big it is.
///
/// Deliberately not *whether* it is open — that is the view mode's fact
/// ([`super::app::App::panel_open`]), because what the panel shows and whether
/// it is showing are one decision. Keeping a second `open` flag here meant two
/// places could disagree about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Panel {
    pub(crate) side: PaneSide,
    /// Size along the split axis — columns when docked right, rows when
    /// docked bottom. Clamped at layout time rather than at assignment, so a
    /// drag past the edge of a small terminal is remembered and comes back
    /// when the terminal grows.
    pub(crate) extent: u16,
}

impl Default for Panel {
    fn default() -> Self {
        Self {
            side: PaneSide::Right,
            extent: 48,
        }
    }
}

/// Smallest useful side panel, and the smallest log pane worth leaving.
const MIN_STATUS: u16 = 16;
const MIN_LOGS: u16 = 20;

/// Everywhere something can be drawn or clicked.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Panes {
    pub(crate) logs: Rect,
    /// `None` when the panel is closed, or when the terminal is too
    /// small to split without starving the log.
    pub(crate) status: Option<Rect>,
    /// The grab handle for the resize drag: the panel's border edge
    /// facing the log. Not drawn separately — the pane's own `Block` border is
    /// the visible line, so there is exactly one rule between the panes
    /// instead of a hand-drawn divider stacked beside a border.
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
            return Some(Focus::Panel);
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

/// Split `area` into the log pane, the optional side panel, and the bar.
///
/// `open` comes from the view mode — the panel is on screen exactly when a
/// panel view (services, tasks, filter) is active.
pub(crate) fn layout(area: Rect, bar_height: u16, status: Panel, open: bool) -> Panes {
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

    if !open {
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
            // beats a two-column panel and a five-column log.
            if body.width < MIN_LOGS + MIN_STATUS {
                return Panes {
                    logs: body,
                    status: None,
                    divider: None,
                    bar,
                };
            }
            let width = status.extent.clamp(MIN_STATUS, body.width - MIN_LOGS);
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Min(MIN_LOGS), Constraint::Length(width)])
                .split(body);
            let status_rect = chunks[1];
            Panes {
                logs: chunks[0],
                // The status block's left border column.
                divider: Some(Rect::new(
                    status_rect.x,
                    status_rect.y,
                    1,
                    status_rect.height,
                )),
                status: Some(status_rect),
                bar,
            }
        }
        PaneSide::Bottom => {
            if body.height < 6 + 3 {
                return Panes {
                    logs: body,
                    status: None,
                    divider: None,
                    bar,
                };
            }
            let height = status.extent.clamp(3, body.height - 6);
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(6), Constraint::Length(height)])
                .split(body);
            let status_rect = chunks[1];
            Panes {
                logs: chunks[0],
                // The status block's top border row.
                divider: Some(Rect::new(
                    status_rect.x,
                    status_rect.y,
                    status_rect.width,
                    1,
                )),
                status: Some(status_rect),
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
    // The border being dragged is part of the panel's own extent, so the
    // new extent runs from the pointer to the far screen edge inclusive.
    match side {
        PaneSide::Right => (area.x + area.width).saturating_sub(column),
        PaneSide::Bottom => (area.y + area.height).saturating_sub(row),
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
            status: Panel,
            open: bool,
            want_status: bool,
            want_log_width: Option<u16>,
        }

        let cases = vec![
            Case {
                name: "closed: the log keeps everything",
                area: Rect::new(0, 0, 120, 40),
                status: Panel::default(),
                open: false,
                want_status: false,
                want_log_width: Some(120),
            },
            Case {
                name: "docked right takes its extent, log keeps the rest",
                area: Rect::new(0, 0, 120, 40),
                status: Panel {
                    side: PaneSide::Right,
                    extent: 40,
                },
                open: true,
                want_status: true,
                want_log_width: Some(80),
            },
            Case {
                name: "an oversized extent is clamped, not honoured",
                area: Rect::new(0, 0, 120, 40),
                status: Panel {
                    side: PaneSide::Right,
                    extent: 500,
                },
                open: true,
                want_status: true,
                want_log_width: Some(MIN_LOGS),
            },
            Case {
                name: "too narrow to split at all: the log keeps everything",
                area: Rect::new(0, 0, 30, 40),
                status: Panel {
                    side: PaneSide::Right,
                    extent: 40,
                },
                open: true,
                want_status: false,
                want_log_width: Some(30),
            },
            Case {
                name: "too short to split vertically",
                area: Rect::new(0, 0, 120, 11),
                status: Panel {
                    side: PaneSide::Bottom,
                    extent: 20,
                },
                open: true,
                want_status: false,
                want_log_width: Some(120),
            },
        ];

        for case in cases {
            let panes = layout(case.area, BAR, case.status, case.open);
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
            Panel {
                side: PaneSide::Right,
                extent: 40,
            },
            true,
        );
        let status = panes.status.unwrap();

        assert_eq!(panes.hit(0, 0), Some(Focus::Logs));
        assert_eq!(panes.hit(panes.logs.width - 1, 5), Some(Focus::Logs));
        assert_eq!(panes.hit(status.x, 5), Some(Focus::Panel));
        assert_eq!(
            panes.hit(status.x + status.width - 1, 5),
            Some(Focus::Panel)
        );
        assert!(
            panes.on_divider(panes.logs.width, 5),
            "the status pane's border facing the log"
        );
        assert_eq!(panes.hit(0, 39), None, "the bar belongs to neither pane");
    }

    /// Dragging the divider left widens the status pane; the arithmetic is
    /// easy to get inverted, and inverted feels broken rather than wrong.
    #[test]
    fn dragging_the_divider_resizes_toward_the_pointer() {
        let area = Rect::new(0, 0, 120, 40);
        // The dragged border is part of the status pane, so the extent runs
        // from the pointer to the far edge inclusive.
        assert_eq!(extent_from_drag(area, PaneSide::Right, 80, 0), 40);
        assert_eq!(
            extent_from_drag(area, PaneSide::Right, 60, 0),
            60,
            "dragging further left gives the status pane more room"
        );
        assert_eq!(extent_from_drag(area, PaneSide::Bottom, 0, 30), 10);
    }
}
