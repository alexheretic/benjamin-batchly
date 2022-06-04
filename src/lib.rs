//! Low latency batching tool.
//! Bundle lots of single concurrent operations into sequential batches of work.
//!
//! For example many concurrent contending single edatabase update tasks could be
//! bundled into bulk updates.
//!
//! # Example
//! ```
//! use benjamin_batchly::{BatchMutex, BatchResult};
//!
//! # async fn example() -> Result<(), &'static str> {
//! let batcher = BatchMutex::default();
//!
//! # let (batch_key, item) = (1, 2);
//! # async fn db_bulk_insert(_: &[i32]) -> Result<(), &'static str> { Ok(()) }
//! // BatchMutex synchronizes so only one `Work` happens at a time (for a given batch_key).
//! // All concurrent submissions made while an existing `Work` is being processed will
//! // await completion and form the next `Work` batch.
//! match batcher.submit(batch_key, item).await {
//!     BatchResult::Work(mut batch) => {
//!         db_bulk_insert(&batch.items).await?;
//!         batch.notify_all_done();
//!         Ok(())
//!     }
//!     BatchResult::Done(_) => Ok(()),
//!     BatchResult::Failed => Err("failed"),
//! }
//! # }
//! ```
//!
//! # Example: Return values
//! Each item may also received it's own return value inside [`BatchResult::Done`].
//!
//! E.g. a `Result` to pass back why some batch items failed to their submitters.
//! ```
//! use benjamin_batchly::{BatchMutex, BatchResult};
//! use anyhow::anyhow;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // 3rd type is value returned by BatchResult::Done
//! let batcher: BatchMutex<_, _, anyhow::Result<()>> = BatchMutex::default();
//!
//! # let (batch_key, my_item) = (1, 2);
//! # async fn db_bulk_insert(_: &[i32]) -> Vec<(usize, bool)> { <_>::default() }
//! match batcher.submit(batch_key, my_item).await {
//!     BatchResult::Work(mut batch) => {
//!         let results = db_bulk_insert(&batch.items).await;
//!
//!         // iterate over results and notify each item's submitter
//!         for (index, success) in results {
//!             if success {
//!                 batch.notify_done(index, Ok(()));
//!             } else {
//!                 batch.notify_done(index, Err(anyhow!("insert failed")));
//!             }
//!         }
//!
//!         // receive the local `my_item` return value
//!         batch.recv_local_notify_done().unwrap()
//!     }
//!     BatchResult::Done(result) => result,
//!     BatchResult::Failed => Err(anyhow!("batch failed")),
//! }
//! # }
//! ```
use dashmap::{mapref::entry::Entry, DashMap};
use futures_util::future::{self, Either};
use std::{fmt, hash::Hash, mem, sync::Arc};
use tokio::sync::{oneshot, OwnedMutexGuard};

/// Batch HQ. Share and use concurrently to dynamically batch submitted items.
///
/// Cheap to clone (`Arc` guts).
///
/// See [`BatchMutex::submit`] & crate docs.
///
/// * `Key` batch key type.
/// * `Item` single item type to be batched together into a `Vec<Item>`.
/// * `T` value returned by [`BatchResult::Done`], default `()`.
#[derive(Clone)]
pub struct BatchMutex<Key: Eq + Hash, Item, T = ()> {
    queue: Arc<DashMap<Key, BatchState<Key, Item, T>>>,
}

impl<Key: Eq + Hash, Item, T> Default for BatchMutex<Key, Item, T> {
    fn default() -> Self {
        Self {
            queue: <_>::default(),
        }
    }
}

impl<Key, Item, T> fmt::Debug for BatchMutex<Key, Item, T>
where
    Key: Eq + Hash,
    Item: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchMutex").finish_non_exhaustive()
    }
}

impl<Key, Item, T> BatchMutex<Key, Item, T>
where
    Key: Eq + Hash + Clone,
{
    /// Submits an `item` to be processed as a batch, ie with other items
    /// submitted with the same `batch_key`.
    ///
    /// Submissions made while a previous `batch_key` batch is processing will await
    /// that batch finishing. All such waiting items become the next batch as soon
    /// as previous finishes.
    ///
    /// Note since submission adds no artificial latency the very first call will
    /// always result in a batch of a single item.
    ///
    /// # Example
    /// ```
    /// # use benjamin_batchly::{BatchMutex, BatchResult};
    /// # async fn example() -> Result<(), &'static str> {
    /// # let batcher = BatchMutex::default();
    /// # let (batch_key, item) = (1, 2);
    /// # async fn db_bulk_insert(_: &[i32]) -> Result<(), &'static str> { Ok(()) }
    /// // Synchronizes so only one `Work` happens at a time (for a given batch_key).
    /// // All concurrent submissions made while an existing `Work` is being processed will
    /// // await completion and form the next `Work` batch.
    /// match batcher.submit(batch_key, item).await {
    ///     BatchResult::Work(mut batch) => {
    ///         db_bulk_insert(&batch.items).await?;
    ///         batch.notify_all_done();
    ///         Ok(())
    ///     }
    ///     BatchResult::Done(_) => Ok(()),
    ///     BatchResult::Failed => Err("failed"),
    /// }
    /// # }
    /// ```
    pub async fn submit(&self, batch_key: Key, item: Item) -> BatchResult<Key, Item, T> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let batch_lock = {
            let mut state = self.queue.entry(batch_key.clone()).or_default();
            state.items.push(item);
            state.senders.push(tx);
            Arc::clone(&state.lock)
        };

        match future::select(rx, Box::pin(batch_lock.lock_owned())).await {
            Either::Left((result, guard)) => {
                drop(guard);
                match result {
                    Ok(Ok((val, _))) => BatchResult::Done(val),
                    Err(_) | Ok(Err(_)) => BatchResult::Failed,
                }
            }
            Either::Right((guard, rx)) => {
                let batch = {
                    let mut state = self.queue.get_mut(&batch_key).unwrap(); // should always exist in queue at this point
                    Batch {
                        guard: Some(guard),
                        items: mem::take(&mut state.items),
                        senders: state.senders.drain(..).map(Some).collect(),
                        cleaner: Cleaner {
                            queue: Arc::clone(&self.queue),
                            key: Some(batch_key),
                        },
                        local_rx: rx,
                    }
                };
                BatchResult::Work(batch)
            }
        }
    }
}

struct BatchState<Key: Eq + Hash, Item, T> {
    items: Vec<Item>,
    senders: Vec<Sender<Key, Item, T>>,
    lock: Arc<tokio::sync::Mutex<()>>,
}

type Sender<K, V, T> = oneshot::Sender<Result<(T, Cleaner<K, V, T>), Cleaner<K, V, T>>>;
type Receiver<K, V, T> = oneshot::Receiver<Result<(T, Cleaner<K, V, T>), Cleaner<K, V, T>>>;

impl<Key: Eq + Hash, Item, T> Default for BatchState<Key, Item, T> {
    fn default() -> Self {
        Self {
            items: <_>::default(),
            senders: <_>::default(),
            lock: <_>::default(),
        }
    }
}

/// [`BatchMutex::submit`] output.
/// Either a batch of items to process & notify or the result of another submitter
/// handling a batch with the same `batch_key`.
#[must_use]
#[derive(Debug)]
pub enum BatchResult<Key: Eq + Hash + Clone, Item, T> {
    /// A non-empty batch of items including the one submitted locally.
    ///
    /// All [`Batch::items`] should be processed and their submitters notified.
    /// See [`Batch::notify_done`], [`Batch::notify_all_done`].
    Work(Batch<Key, Item, T>),
    /// The submitted item has been processed in a batch by another submitter
    /// which has notified the item as done.
    Done(T),
    /// The item started batch processing by another submitter but was dropped
    /// before notifying done.
    Failed,
}

/// A batch of items to process.
pub struct Batch<Key: Eq + Hash + Clone, Item, T> {
    /// Batch items.
    pub items: Vec<Item>,
    guard: Option<OwnedMutexGuard<()>>,
    senders: Vec<Option<Sender<Key, Item, T>>>,
    cleaner: Cleaner<Key, Item, T>,
    local_rx: Receiver<Key, Item, T>,
}

impl<Key, Item, T> fmt::Debug for Batch<Key, Item, T>
where
    Key: Eq + Hash + Clone,
    Item: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Batch")
            .field("items", &self.items)
            .finish_non_exhaustive()
    }
}

impl<Key: Eq + Hash + Clone, Item, T> Batch<Key, Item, T> {
    /// Notify an individual item as done.
    ///
    /// This means the submitter of that item will receive a [`BatchResult::Done`] with the done value.
    ///
    /// If the item is the local item, the one submitted by the same caller handling the batch,
    /// the result may be received using [`Batch::recv_local_notify_done`].
    pub fn notify_done(&mut self, item_index: usize, val: T) {
        if let Some(tx) = self.senders[item_index].take() {
            let _ = tx.send(Ok((val, self.cleaner.clone())));
        }
    }

    /// Receive the local item's done notification if available.
    ///
    /// The local item is the one submitted by the caller that received [`BatchResult::Work`].
    ///
    /// Since the local item is handled along with multiple other non-local items, this method
    /// can be used to get the local computation result after handling all items and calling
    /// [`Batch::notify_done`] for each (one of which is the local item).
    pub fn recv_local_notify_done(&mut self) -> Option<T> {
        self.local_rx.try_recv().ok().and_then(|v| Some(v.ok()?.0))
    }

    fn notify_all_failed(&mut self) {
        for tx in self.senders.drain(..) {
            let _ = tx.map(|tx| tx.send(Err(self.cleaner.clone())));
        }
    }
}

impl<Key: Eq + Hash + Clone, Item> Batch<Key, Item, ()> {
    /// Notify all items, that have not already been notified, as done.
    ///
    /// This means all other submitters will receive [`BatchResult::Done`].
    ///
    /// Convenience method when using no/`()` item return value.
    pub fn notify_all_done(&mut self) {
        for tx in self.senders.drain(..) {
            let _ = tx.map(|tx| tx.send(Ok(((), self.cleaner.clone()))));
        }
    }
}

impl<Key: Eq + Hash + Clone, Item, T> Drop for Batch<Key, Item, T> {
    fn drop(&mut self) {
        self.notify_all_failed(); // handle senders before dropping guard
        self.guard.take(); // drop before cleaner
    }
}

/// Handle that will try to clean up uncontended leftover batch state on drop.
struct Cleaner<Key: Eq + Hash, Item, T> {
    queue: Arc<DashMap<Key, BatchState<Key, Item, T>>>,
    key: Option<Key>,
}

impl<Key: Eq + Hash + Clone, Item, T> Clone for Cleaner<Key, Item, T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            key: self.key.clone(),
        }
    }
}

impl<Key: Eq + Hash, Item, T> Drop for Cleaner<Key, Item, T> {
    fn drop(&mut self) {
        // try to cleanup leftover states if possible
        if let Some(key) = self.key.take() {
            if let Entry::Occupied(entry) = self.queue.entry(key) {
                if Arc::strong_count(&entry.get().lock) == 1 {
                    entry.remove();
                }
            }
        }
    }
}
