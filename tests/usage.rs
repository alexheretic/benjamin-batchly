use benjamin_batchly::{BatchMutex, BatchResult};
use std::{
    mem,
    time::{Duration, Instant},
};

#[tokio::test]
async fn batch_all_ok() {
    let batcher = BatchMutex::default();

    let batch_key = 123;

    let (a, b, c, d, e) = futures::join!(
        handle_ok(&batcher, batch_key, Item('a')),
        handle_ok(&batcher, batch_key, Item('b')),
        handle_ok(&batcher, batch_key, Item('c')),
        handle_ok(&batcher, batch_key, Item('d')),
        handle_ok(&batcher, batch_key, Item('e')),
    );

    // First task (a) is unblocked and immediately does it's own work
    assert_eq!(a.0, HandleResult::DidWork(vec![Item('a')]));

    // The other tasks await [a] batch completion, each trying to acquire the
    // batch lock. Task b is the first in the lock queue so does the batch.
    assert_eq!(
        b.0,
        HandleResult::DidWork(vec![Item('b'), Item('c'), Item('d'), Item('e')])
    );
    assert!(b.1 >= a.1);

    // since task b handled all remaining tasks they return `Done`
    assert_eq!(c.0, HandleResult::Done);
    assert!(c.1 >= b.1);
    assert_eq!(d.0, HandleResult::Done);
    assert!(d.1 >= b.1);
    assert_eq!(e.0, HandleResult::Done);
    assert!(e.1 >= b.1);
}

#[tokio::test]
async fn batch_cancelled() {
    let batcher = BatchMutex::default();

    let batch_key = 123;

    let (a, b, c, d, e) = futures::join!(
        handle_ok(&batcher, batch_key, Item('a')),
        // cancel b around halfway through
        tokio::time::timeout(
            Duration::from_millis(150),
            handle_ok(&batcher, batch_key, Item('b'))
        ),
        handle_ok(&batcher, batch_key, Item('c')),
        handle_ok(&batcher, batch_key, Item('d')),
        handle_ok(&batcher, batch_key, Item('e')),
    );

    // First task (a) is unblocked and immediately does it's own work
    assert_eq!(a.0, HandleResult::DidWork(vec![Item('a')]));

    // Task b gets the batch but is cancelled before it can process it all
    assert!(b.is_err());

    // All batched tasks fail along with b
    assert_eq!(c.0, HandleResult::Failed);
    assert_eq!(d.0, HandleResult::Failed);
    assert_eq!(e.0, HandleResult::Failed);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Item(char);

/// Submit and handle batch in ~100ms
async fn handle_ok(
    batcher: &BatchMutex<i32, Item>,
    key: i32,
    item: Item,
) -> (HandleResult, Instant) {
    match batcher.submit(key, item).await {
        BatchResult::Done => (HandleResult::Done, Instant::now()),
        BatchResult::Failed => (HandleResult::Failed, Instant::now()),
        BatchResult::Work(mut batch) => {
            let items = mem::take(&mut batch.items);

            // simuluate some io
            tokio::time::sleep(Duration::from_millis(100)).await;
            let finish = Instant::now();

            // notify that each task succeeded
            batch.notify_all_ok();

            (HandleResult::DidWork(items), finish)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HandleResult {
    Done,
    Failed,
    DidWork(Vec<Item>),
}
