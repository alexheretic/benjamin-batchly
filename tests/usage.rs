use anyhow::Context;
use benjamin_batchly::{BatchMutex, BatchResult};
use std::{
    mem,
    time::{Duration, Instant},
};

#[tokio::test]
async fn batch_all_ok() {
    let batcher = BatchMutex::default();

    let batch_key = 123;

    let (a, b, c, d, e) = tokio::join!(
        handle_batch_in_100ms(&batcher, batch_key, Item('a')),
        handle_batch_in_100ms(&batcher, batch_key, Item('b')),
        handle_batch_in_100ms(&batcher, batch_key, Item('c')),
        handle_batch_in_100ms(&batcher, batch_key, Item('d')),
        handle_batch_in_100ms(&batcher, batch_key, Item('e')),
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
    assert_eq!(c.0, HandleResult::Done(()));
    assert!(c.1 >= b.1);
    assert_eq!(d.0, HandleResult::Done(()));
    assert!(d.1 >= b.1);
    assert_eq!(e.0, HandleResult::Done(()));
    assert!(e.1 >= b.1);
}

#[tokio::test]
async fn batch_cancelled() {
    let batcher = BatchMutex::default();

    let batch_key = 123;

    let (a, b, c, d, e) = tokio::join!(
        handle_batch_in_100ms(&batcher, batch_key, Item('a')),
        // cancel b around halfway through
        tokio::time::timeout(
            Duration::from_millis(150),
            handle_batch_in_100ms(&batcher, batch_key, Item('b'))
        ),
        handle_batch_in_100ms(&batcher, batch_key, Item('c')),
        handle_batch_in_100ms(&batcher, batch_key, Item('d')),
        handle_batch_in_100ms(&batcher, batch_key, Item('e')),
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

#[tokio::test]
async fn batch_return_value() {
    let batcher: BatchMutex<_, _, Result<u32, NotEven>> = BatchMutex::default();

    let batch_key = 123;

    let (a, b, c, d, e) = tokio::join!(
        handle_convert_to_u32_if_even(&batcher, batch_key, Item('a')),
        handle_convert_to_u32_if_even(&batcher, batch_key, Item('b')),
        handle_convert_to_u32_if_even(&batcher, batch_key, Item('c')),
        handle_convert_to_u32_if_even(&batcher, batch_key, Item('d')),
        handle_convert_to_u32_if_even(&batcher, batch_key, Item('e')),
    );

    // First task (a) is unblocked and immediately does it's own work
    assert!(matches!(a, Ok(Err(NotEven(97)))), "{a:?}");

    // task b awaits a then processes the batch
    assert!(matches!(b, Ok(Ok(98))), "{b:?}");

    // the remaining tasks are all notified by b
    assert!(matches!(c, Ok(Err(NotEven(99)))), "{c:?}");
    assert!(matches!(d, Ok(Ok(100))), "{d:?}");
    assert!(matches!(e, Ok(Err(NotEven(101)))), "{e:?}");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Item(char);

/// Submit and handle batch in ~100ms.
async fn handle_batch_in_100ms(
    batcher: &BatchMutex<i32, Item>,
    key: i32,
    item: Item,
) -> (HandleResult, Instant) {
    match batcher.submit(key, item).await {
        BatchResult::Done(_) => (HandleResult::Done(()), Instant::now()),
        BatchResult::Failed => (HandleResult::Failed, Instant::now()),
        BatchResult::Work(mut batch) => {
            let items = mem::take(&mut batch.items);
            assert_eq!(items[0], item);

            // simuluate some io
            tokio::time::sleep(Duration::from_millis(100)).await;
            let finish = Instant::now();

            // notify that each task succeeded
            batch.notify_all_done();

            (HandleResult::DidWork(items), finish)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HandleResult<T = ()> {
    Done(T),
    Failed,
    DidWork(Vec<Item>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NotEven(u32);

/// Submit and handle batch in ~100ms.
///
/// Return Ok(u32) for all even converted chars, and an error otherwise.
async fn handle_convert_to_u32_if_even(
    batcher: &BatchMutex<i32, Item, Result<u32, NotEven>>,
    key: i32,
    item: Item,
) -> anyhow::Result<Result<u32, NotEven>> {
    match batcher.submit(key, item).await {
        BatchResult::Done(v) => Ok(v),
        BatchResult::Failed => Err(anyhow::anyhow!("BatchResult::Failed")),
        BatchResult::Work(mut batch) => {
            let items = mem::take(&mut batch.items);

            // simuluate some io
            tokio::time::sleep(Duration::from_millis(100)).await;

            for (idx, item) in items.iter().enumerate() {
                let n = item.0 as u32;
                if n % 2 == 0 {
                    batch.notify_done(idx, Ok(n));
                } else {
                    batch.notify_done(idx, Err(NotEven(n)));
                }
            }

            batch.recv_local_notify_done().context("did not recv local")
        }
    }
}
