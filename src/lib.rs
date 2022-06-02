use futures::future::{self, Either};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt,
    hash::Hash,
    mem,
    sync::Arc,
};
use tokio::sync::{oneshot, OwnedMutexGuard};

#[derive(Clone)]
pub struct BatchMutex<Key: Eq + Hash, Item> {
    queue: Arc<std::sync::Mutex<HashMap<Key, BatchState<Key, Item>>>>,
}

impl<Key: Eq + Hash, Item> Default for BatchMutex<Key, Item> {
    fn default() -> Self {
        Self {
            queue: <_>::default(),
        }
    }
}

impl<Key, Item> BatchMutex<Key, Item>
where
    Key: Eq + Hash + Clone,
{
    pub async fn submit(&self, batch_key: Key, item: Item) -> BatchResult<Key, Item> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let batch_lock = {
            let mut queue = self.queue.lock().unwrap();
            let state = queue.entry(batch_key.clone()).or_default();
            state.items.push(item);
            state.senders.push(tx);
            Arc::clone(&state.lock)
        };

        match future::select(rx, Box::pin(batch_lock.lock_owned())).await {
            Either::Left((result, guard)) => {
                drop(guard);
                match result {
                    Ok(Ok(_)) => BatchResult::Done,
                    Err(_) | Ok(Err(_)) => BatchResult::Failed,
                }
            }
            Either::Right((guard, rx)) => {
                let batch = self
                    .queue
                    .lock()
                    .unwrap()
                    .get_mut(&batch_key)
                    .map(move |state| Batch {
                        guard: Some(guard),
                        items: mem::take(&mut state.items),
                        senders: mem::take(&mut state.senders),
                        cleaner: Cleaner {
                            queue: Arc::clone(&self.queue),
                            key: Some(batch_key),
                        },
                    });

                match batch {
                    Some(batch) => BatchResult::Work(batch),
                    None => match rx.await {
                        Ok(Ok(_)) => BatchResult::Done,
                        Err(_) | Ok(Err(_)) => BatchResult::Failed,
                    },
                }
            }
        }
    }
}

struct BatchState<Key: Eq + Hash, Item> {
    items: Vec<Item>,
    senders: Vec<Sender<Key, Item>>,
    lock: Arc<tokio::sync::Mutex<()>>,
}

type Sender<K, V> = oneshot::Sender<Result<Cleaner<K, V>, Cleaner<K, V>>>;

impl<Key: Eq + Hash, Item> Default for BatchState<Key, Item> {
    fn default() -> Self {
        Self {
            items: <_>::default(),
            senders: <_>::default(),
            lock: <_>::default(),
        }
    }
}

#[must_use]
#[derive(Debug)]
pub enum BatchResult<Key: Eq + Hash + Clone, Item> {
    Done,
    Failed,
    Work(Batch<Key, Item>),
}

pub struct Batch<Key: Eq + Hash + Clone, Item> {
    pub items: Vec<Item>,
    guard: Option<OwnedMutexGuard<()>>,
    senders: Vec<Sender<Key, Item>>,
    cleaner: Cleaner<Key, Item>,
}

impl<Key: Eq + Hash + Clone, Item> fmt::Debug for Batch<Key, Item>
where
    Item: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Batch")
            .field("items", &self.items)
            .finish_non_exhaustive()
    }
}

impl<Key: Eq + Hash + Clone, Item> Batch<Key, Item> {
    pub fn notify_all_ok(&mut self) {
        for tx in self.senders.drain(..) {
            let _ = tx.send(Ok(self.cleaner.clone()));
        }
    }

    fn notify_all_failed(&mut self) {
        for tx in self.senders.drain(..) {
            let _ = tx.send(Err(self.cleaner.clone()));
        }
    }
}

impl<Key: Eq + Hash + Clone, Item> Drop for Batch<Key, Item> {
    fn drop(&mut self) {
        self.notify_all_failed(); // handle senders before dropping guard
        self.guard.take(); // drop before cleaner
    }
}

/// Handle that will try to clean up uncontended leftover batch state on drop.
struct Cleaner<Key: Eq + Hash, Item> {
    queue: Arc<std::sync::Mutex<HashMap<Key, BatchState<Key, Item>>>>,
    key: Option<Key>,
}

impl<Key: Eq + Hash + Clone, Item> Clone for Cleaner<Key, Item> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            key: self.key.clone(),
        }
    }
}

impl<Key: Eq + Hash, Item> Drop for Cleaner<Key, Item> {
    fn drop(&mut self) {
        // try to cleanup leftover states if possible
        if let Some(key) = self.key.take() {
            let mut queue = self.queue.lock().unwrap();
            if let Entry::Occupied(entry) = queue.entry(key) {
                if Arc::strong_count(&entry.get().lock) == 1 {
                    entry.remove();
                }
            }
        }
    }
}
