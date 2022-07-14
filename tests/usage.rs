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
async fn batch_all_ok_pull_waiting_items() {
    let batcher = BatchMutex::default();

    let batch_key = 123;

    let (a, b, c, d, e) = tokio::join!(
        lock_30ms_pull_waiting_items_and_handle_in_50ms(&batcher, batch_key, Item('a')),
        lock_30ms_pull_waiting_items_and_handle_in_50ms(&batcher, batch_key, Item('b')),
        lock_30ms_pull_waiting_items_and_handle_in_50ms(&batcher, batch_key, Item('c')),
        lock_30ms_pull_waiting_items_and_handle_in_50ms(&batcher, batch_key, Item('d')),
        lock_30ms_pull_waiting_items_and_handle_in_50ms(&batcher, batch_key, Item('e')),
    );

    // First task (a) is unblocked and immediately does it's own work
    // after 30ms it tries to `pull_waiting_items` pulling in the rest f the tasks
    assert_eq!(
        a.0,
        HandleResult::DidWork(vec![Item('a'), Item('b'), Item('c'), Item('d'), Item('e')])
    );

    // since task a handled all tasks the rest return `Done`
    assert_eq!(b.0, HandleResult::Done(()));
    assert!(b.1 >= a.1);
    assert_eq!(c.0, HandleResult::Done(()));
    assert!(c.1 >= b.1);
    assert_eq!(d.0, HandleResult::Done(()));
    assert!(d.1 >= b.1);
    assert_eq!(e.0, HandleResult::Done(()));
    assert!(e.1 >= b.1);
}

#[tokio::test]
async fn batch_different_keys() {
    let batcher = BatchMutex::default();

    let (a, b, c, d, e) = tokio::join!(
        handle_batch_in_100ms(&batcher, 45, Item('a')),
        handle_batch_in_100ms(&batcher, 56, Item('b')),
        handle_batch_in_100ms(&batcher, 67, Item('c')),
        handle_batch_in_100ms(&batcher, 78, Item('d')),
        handle_batch_in_100ms(&batcher, 89, Item('e')),
    );

    // All tasks are unrelated batches of 1
    assert_eq!(a.0, HandleResult::DidWork(vec![Item('a')]));
    assert_eq!(b.0, HandleResult::DidWork(vec![Item('b')]));
    assert_eq!(c.0, HandleResult::DidWork(vec![Item('c')]));
    assert_eq!(d.0, HandleResult::DidWork(vec![Item('d')]));
    assert_eq!(e.0, HandleResult::DidWork(vec![Item('e')]));
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
    assert!(matches!(a, ToU32Result::DidWork(Err(NotEven(97)))), "{a:?}");

    // task b awaits a then processes the batch
    assert!(matches!(b, ToU32Result::DidWork(Ok(98))), "{b:?}");

    // the remaining tasks are all notified by b
    assert!(matches!(c, ToU32Result::Done(Err(NotEven(99)))), "{c:?}");
    assert!(matches!(d, ToU32Result::Done(Ok(100))), "{d:?}");
    assert!(matches!(e, ToU32Result::Done(Err(NotEven(101)))), "{e:?}");
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
            assert_eq!(batch.items[0], item);

            // simuluate some io
            tokio::time::sleep(Duration::from_millis(100)).await;
            let finish = Instant::now();

            // notify that each task succeeded
            batch.notify_all_done();

            (HandleResult::DidWork(mem::take(&mut batch.items)), finish)
        }
    }
}

/// Submit and handle batch in ~100ms.
async fn lock_30ms_pull_waiting_items_and_handle_in_50ms(
    batcher: &BatchMutex<i32, Item>,
    key: i32,
    item: Item,
) -> (HandleResult, Instant) {
    match batcher.submit(key, item).await {
        BatchResult::Done(_) => (HandleResult::Done(()), Instant::now()),
        BatchResult::Failed => (HandleResult::Failed, Instant::now()),
        BatchResult::Work(mut batch) => {
            assert_eq!(batch.items[0], item);

            // simuluate some initial io (like a distributed lock)
            tokio::time::sleep(Duration::from_millis(30)).await;
            batch.pull_waiting_items();

            // simuluate some io
            tokio::time::sleep(Duration::from_millis(50)).await;
            let finish = Instant::now();

            // notify that each task succeeded
            batch.notify_all_done();

            (HandleResult::DidWork(mem::take(&mut batch.items)), finish)
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
) -> ToU32Result<Result<u32, NotEven>> {
    match batcher.submit(key, item).await {
        BatchResult::Done(v) => ToU32Result::Done(v),
        BatchResult::Failed => ToU32Result::Failed,
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

            match batch.recv_local_notify_done() {
                Some(v) => ToU32Result::DidWork(v),
                None => ToU32Result::RecvLocalErr,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ToU32Result<T> {
    Done(T),
    Failed,
    DidWork(T),
    RecvLocalErr,
}
