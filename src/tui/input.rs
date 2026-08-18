//! Input task — forwards crossterm events to the TUI loop.
//!
//! Thin by design: we don't interpret input here because interpretation
//! depends on the current view mode and on where the panes ended up, both of
//! which live in the main loop. The only filtering done here is dropping
//! events nothing acts on — key releases, and the mouse *moves* that arrive
//! between drags, which would otherwise wake the loop thousands of times for
//! nothing.

use crossterm::event::{Event, EventStream, KeyEventKind, MouseEventKind};
use futures_util::StreamExt;
use tokio::sync::mpsc;

use super::events::AppEvent;

/// Pump crossterm events into the TUI's `AppEvent` channel until stdout is
/// closed or the receiver is dropped.
pub(crate) async fn run(tx: mpsc::Sender<AppEvent>) {
    let mut reader = EventStream::new();
    // Whether the last forwarded motion had shift held — i.e. whether the
    // main loop currently shows a hover highlight that a later motion must
    // clear. This is what keeps `?1003h` affordable: plain mouse movement
    // is dropped here, before the channel, except for the single event that
    // turns an active highlight off.
    let mut hover_live = false;
    while let Some(result) = reader.next().await {
        let Ok(event) = result else { continue };
        let Some(app_event) = translate(event, &mut hover_live) else {
            continue;
        };
        if tx.send(app_event).await.is_err() {
            break;
        }
    }
}

/// Convert a crossterm event to an [`AppEvent`], returning `None` for events
/// the TUI doesn't act on (key releases, unshifted mouse motion, focus,
/// paste).
fn translate(event: Event, hover_live: &mut bool) -> Option<AppEvent> {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Release => None,
        Event::Key(key) => Some(AppEvent::Key(key)),
        Event::Resize(_, _) => Some(AppEvent::Resize),
        // Motion with no button is a flood — most terminals report it for
        // every cell the pointer crosses. Shift-hover is the one consumer:
        // shifted moves go through, and one unshifted move goes through when
        // a highlight is up so it can be taken down. Everything else dies
        // here rather than waking the loop.
        Event::Mouse(mouse) if matches!(mouse.kind, MouseEventKind::Moved) => {
            let shifted = mouse
                .modifiers
                .contains(crossterm::event::KeyModifiers::SHIFT);
            if shifted {
                *hover_live = true;
                Some(AppEvent::Mouse(mouse))
            } else if std::mem::take(hover_live) {
                Some(AppEvent::Mouse(mouse))
            } else {
                None
            }
        }
        Event::Mouse(mouse) => Some(AppEvent::Mouse(mouse)),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyEventState, KeyModifiers};

    fn press(code: KeyCode) -> Event {
        Event::Key(KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        })
    }

    #[test]
    fn translate_key_press_forwards() {
        let got = translate(press(KeyCode::Enter), &mut false);
        assert!(matches!(got, Some(AppEvent::Key(_))));
    }

    #[test]
    fn translate_key_release_dropped() {
        let release = Event::Key(KeyEvent {
            code: KeyCode::Enter,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Release,
            state: KeyEventState::NONE,
        });
        assert!(translate(release, &mut false).is_none());
    }

    #[test]
    fn translate_resize_becomes_resize_variant() {
        assert!(matches!(
            translate(Event::Resize(80, 24), &mut false),
            Some(AppEvent::Resize)
        ));
    }

    /// `?1003h` reports every cell the pointer crosses. The gate is what makes
    /// that affordable: only the moves hover consumes reach the channel — the
    /// shifted ones, plus the single unshifted one that clears a live
    /// highlight.
    #[test]
    fn bare_motion_is_gated_to_what_hover_consumes() {
        use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};

        fn moved(shift: bool) -> Event {
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Moved,
                column: 10,
                row: 5,
                modifiers: if shift {
                    KeyModifiers::SHIFT
                } else {
                    KeyModifiers::NONE
                },
            })
        }

        struct Case {
            name: &'static str,
            events: &'static [bool],
            /// Whether each event is forwarded.
            want: &'static [bool],
        }

        let cases = [
            Case {
                name: "plain motion never wakes the loop",
                events: &[false, false, false],
                want: &[false, false, false],
            },
            Case {
                name: "shifted motion is hover, and always goes through",
                events: &[true, true, true],
                want: &[true, true, true],
            },
            Case {
                name: "one unshifted move clears a live highlight, the rest die",
                events: &[true, false, false, false],
                want: &[true, true, false, false],
            },
        ];

        for case in cases {
            let mut hover_live = false;
            for (shift, want) in case.events.iter().zip(case.want) {
                let got = translate(moved(*shift), &mut hover_live).is_some();
                assert_eq!(got, *want, "{}: shift={}", case.name, shift);
            }
        }

        // A drag is not gated: dragging is how selection works.
        let drag = Event::Mouse(MouseEvent {
            kind: MouseEventKind::Drag(MouseButton::Left),
            column: 1,
            row: 1,
            modifiers: KeyModifiers::NONE,
        });
        assert!(translate(drag, &mut false).is_some());
    }
}
