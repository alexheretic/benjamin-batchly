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
use futures_util::future::{self, Either};
use std::{
    collections::{
        hash_map::{Entry, RandomState},
        HashMap,
    },
    fmt,
    hash::{BuildHasher, Hash, Hasher},
    mem,
    sync::{Arc, Mutex},
};
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
pub struct BatchMutex<Key, Item, T = ()> {
    hasher: RandomState,
    queue: Arc<[Shard<Key, Item, T>]>,
}
type Shard<K, I, T> = Mutex<HashMap<K, BatchState<I, T>>>;

impl<Key, Item, T> Clone for BatchMutex<Key, Item, T> {
    fn clone(&self) -> Self {
        Self {
            hasher: self.hasher.clone(),
            queue: self.queue.clone(),
        }
    }
}

impl<Key: Eq + Hash, Item, T> Default for BatchMutex<Key, Item, T> {
    fn default() -> Self {
        let cap =
            (std::thread::available_parallelism().map_or(1, usize::from) * 4).next_power_of_two();
        let mut shards = Vec::with_capacity(cap);
        shards.resize_with(cap, || Mutex::new(HashMap::new()));
        Self {
            hasher: RandomState::new(),
            queue: shards.into_boxed_slice().into(),
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
    Key: Eq + Hash,
{
    fn get_shard(&self, key: &Key) -> &Shard<Key, Item, T> {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        // queue len is always a power of two, so it should evenly divide the hash space and be fair
        let index = (hasher.finish()) as usize % self.queue.len();
        &self.queue[index]
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
            let mut shard = self.get_shard(&batch_key).lock().unwrap();
            let state = shard.entry(batch_key.clone()).or_default();
            state.items.push(item);
            state.senders.push(tx);
            Arc::clone(&state.lock)
        };

        match future::select(rx, Box::pin(batch_lock.lock_owned())).await {
            Either::Left((result, guard)) => {
                drop(guard);
                match result {
                    Ok(Some(val)) => BatchResult::Done(val),
                    Err(_) | Ok(None) => BatchResult::Failed,
                }
            }
            Either::Right((guard, rx)) => {
                let batch = {
                    let mut shard = self.get_shard(&batch_key).lock().unwrap();
                    let state = shard.get_mut(&batch_key).unwrap(); // should always exist in queue at this point
                    Batch {
                        guard: Some(guard),
                        items: mem::take(&mut state.items),
                        senders: state.senders.drain(..).map(Some).collect(),
                        cleaner: Cleaner {
                            queue: self.clone(),
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

struct BatchState<Item, T> {
    items: Vec<Item>,
    senders: Vec<Sender<T>>,
    lock: Arc<tokio::sync::Mutex<()>>,
}

type Sender<T> = oneshot::Sender<Option<T>>;
type Receiver<T> = oneshot::Receiver<Option<T>>;

impl<Item, T> Default for BatchState<Item, T> {
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
    senders: Vec<Option<Sender<T>>>,
    cleaner: Cleaner<Key, Item, T>,
    local_rx: Receiver<T>,
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
    /// Note `item_index` is the original index of the item in [`Batch::items`].
    ///
    /// If the item is the local item, the one submitted by the same caller handling the batch,
    /// the result may be received using [`Batch::recv_local_notify_done`].
    pub fn notify_done(&mut self, item_index: usize, val: T) {
        if let Some(tx) = self.senders.get_mut(item_index).and_then(|i| i.take()) {
            let _ = tx.send(Some(val));
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
        self.local_rx.try_recv().ok().flatten()
    }

    /// Takes items that have been submitted after this [`Batch`] was returned and adds
    /// them to this batch to be processed immediately.
    ///
    /// New items are appended onto [`Batch::items`].
    ///
    /// Returns `true` if any new items were pulled in.
    pub fn pull_waiting_items(&mut self) -> bool {
        if let Some(key) = self.cleaner.key.as_ref() {
            let mut shard = self.cleaner.queue.get_shard(key).lock().unwrap();
            match shard.get_mut(key) {
                Some(next) if !next.items.is_empty() => {
                    self.items.append(&mut next.items);
                    self.senders.extend(next.senders.drain(..).map(Some));
                    true
                }
                _ => false,
            }
        } else {
            false
        }
    }

    fn notify_all_failed(&mut self) {
        for tx in &mut self.senders {
            let _ = tx.take().map(|tx| tx.send(None));
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
        for tx in &mut self.senders {
            let _ = tx.take().map(|tx| tx.send(Some(())));
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
#[derive(Clone)]
struct Cleaner<Key: Eq + Hash, Item, T> {
    queue: BatchMutex<Key, Item, T>,
    key: Option<Key>,
}

impl<Key: Eq + Hash, Item, T> Drop for Cleaner<Key, Item, T> {
    fn drop(&mut self) {
        // try to cleanup leftover states if possible
        if let Some(key) = self.key.take() {
            let mut shard = self.queue.get_shard(&key).lock().unwrap();
            if let Entry::Occupied(entry) = shard.entry(key) {
                if Arc::strong_count(&entry.get().lock) == 1 {
                    entry.remove();
                }
            }
        }
    }
}
