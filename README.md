# benjamin_batchly [![crates.io](https://img.shields.io/crates/v/benjamin_batchly.svg)](https://crates.io/crates/benjamin_batchly) [![Documentation](https://docs.rs/benjamin_batchly/badge.svg)](https://docs.rs/benjamin_batchly)
Low latency batching tool. Bundle lots of single concurrent operations into sequential batches of work.

```rust
use benjamin_batchly::{BatchMutex, BatchResult};

let batcher = BatchMutex::default();

// BatchMutex synchronizes so only one `Work` happens at a time (for a given batch_key).
// All concurrent submissions made while an existing `Work` is being processed will
// await completion and form the next `Work` batch.
match batcher.submit(batch_key, item).await {
    BatchResult::Work(mut batch) => {
        db_bulk_insert(&batch.items).await?;
        batch.notify_all_done();
        Ok(())
    }
    BatchResult::Done(_) => Ok(()),
    BatchResult::Failed => Err("failed"),
}
```
