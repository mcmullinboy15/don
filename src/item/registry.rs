//! The shape shared by every per-item supervisor.
//!
//! Services and tasks both ended up with the same three pieces — a mailbox, a
//! busy flag, and a join handle — differing only in the message they carry.
//! This is that shape, once, generic over the message.
//!
//! The split is by **capability**, and it is the point of the module:
//!
//! - [`ItemRegistry`] is clone-able and send-only. It addresses an item and
//!   has no method that creates or destroys one, so handing it to the file
//!   watcher or the API widens what can ask an item to do something without
//!   widening what can change the set of items.
//! - [`Supervisors`] holds the join handles, so *ending* a supervisor stays
//!   with whoever owns the item set.
//!
//! The registry is a plain `Arc<HashMap<_, _>>` with no lock, and it can be
//! because the item set is fixed at construction (see
//! `setup::build_runtime_maps`). There is no insert and no remove, so there is
//! nothing to synchronise. If items ever became dynamic this needs the
//! [`StateWriter`]/[`StateReader`] treatment rather than a mutex.
//!
//! [`StateWriter`]: super::state_store::StateWriter
//! [`StateReader`]: super::StateReader

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

/// Send-only handle to one item's supervisor.
pub(crate) struct ItemHandle<M> {
    tx: mpsc::UnboundedSender<M>,
    busy: Arc<AtomicBool>,
}

// Derived `Clone` would demand `M: Clone`, which is wrong — the handle clones
// a sender, never a message.
impl<M> Clone for ItemHandle<M> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            busy: Arc::clone(&self.busy),
        }
    }
}

impl<M> ItemHandle<M> {
    /// Queue work. Fails only once the supervisor is gone (shutdown).
    ///
    /// Marks the item busy before sending, not after the supervisor picks the
    /// request up — otherwise a caller could queue work and immediately be
    /// told the item is idle.
    pub(crate) fn request(&self, message: M) -> bool {
        self.busy.store(true, Ordering::Relaxed);
        let sent = self.tx.send(message).is_ok();
        if !sent {
            self.busy.store(false, Ordering::Relaxed);
        }
        sent
    }

    /// Whether work is queued or in progress.
    pub(crate) fn is_busy(&self) -> bool {
        self.busy.load(Ordering::Relaxed)
    }
}

/// Every item's mailbox, addressable by name.
pub(crate) struct ItemRegistry<M> {
    handles: Arc<HashMap<String, ItemHandle<M>>>,
}

impl<M> Clone for ItemRegistry<M> {
    fn clone(&self) -> Self {
        Self {
            handles: Arc::clone(&self.handles),
        }
    }
}

impl<M> ItemRegistry<M> {
    /// The mailbox for `name`, or `None` if it isn't an item of this kind.
    pub(crate) fn get(&self, name: &str) -> Option<&ItemHandle<M>> {
        self.handles.get(name)
    }

    /// Whether `name` has work queued or in progress. `false` for an unknown
    /// name, which is what callers asking "may I start this?" want.
    pub(crate) fn is_busy(&self, name: &str) -> bool {
        self.get(name).is_some_and(ItemHandle::is_busy)
    }

    /// Names with work queued or in progress.
    pub(crate) fn busy_names(&self) -> impl Iterator<Item = &str> {
        self.handles
            .iter()
            .filter(|(_, handle)| handle.is_busy())
            .map(|(name, _)| name.as_str())
    }
}

/// The owner half: the supervisor tasks themselves.
pub(crate) struct Supervisors<M> {
    registry: ItemRegistry<M>,
    joins: Vec<(String, tokio::task::JoinHandle<()>)>,
}

impl<M: Send + 'static> Supervisors<M> {
    /// Start one supervisor per name, with `body` supplying the loop.
    ///
    /// Eager rather than on first use: it makes the registry immutable, which
    /// is what lets it be shared without a lock. An idle supervisor is a
    /// parked task that is never polled until something addresses it.
    pub(crate) fn spawn_all<'a, F, Fut>(
        names: impl Iterator<Item = &'a String>,
        mut body: F,
    ) -> Self
    where
        F: FnMut(String, mpsc::UnboundedReceiver<M>, Arc<AtomicBool>) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut handles = HashMap::new();
        let mut joins = Vec::new();
        for name in names {
            let (tx, rx) = mpsc::unbounded_channel();
            let busy = Arc::new(AtomicBool::new(false));
            let join = tokio::spawn(body(name.clone(), rx, Arc::clone(&busy)));
            handles.insert(name.clone(), ItemHandle { tx, busy });
            joins.push((name.clone(), join));
        }
        Self {
            registry: ItemRegistry {
                handles: Arc::new(handles),
            },
            joins,
        }
    }

    /// The addressing half, for handing to anything that needs to ask an item
    /// to do something.
    pub(crate) fn registry(&self) -> &ItemRegistry<M> {
        &self.registry
    }

    /// Cancel every supervisor, returning the handles to await.
    ///
    /// Deliberately *not* an `async fn` that also waits: shutdown has to fire
    /// every abort before waiting on any of them, or a project with N items
    /// pays the timeout N times over instead of once. Every teardown loop in
    /// `shutdown.rs` has that shape.
    ///
    /// Nothing needs to drop the registry first: the receiver lives inside the
    /// supervisor future, so aborting it drops the receiver, and every
    /// outstanding [`ItemHandle`] — including clones held elsewhere — starts
    /// reporting failure from `request`.
    pub(crate) fn abort_all(&mut self) -> Vec<(String, tokio::task::JoinHandle<()>)> {
        let joins = std::mem::take(&mut self.joins);
        for (_, join) in &joins {
            join.abort();
        }
        joins
    }
}
