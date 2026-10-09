# bounded-work-queue

A simple CPU work queue meant for heavy, long-running and deterministic compute tasks. 

This crate supports fetching cached results and request deduplication.

## Invariants
- If an input has the same key as a currently running task's input, the duplicate task isn't ran and will consume the result of the original one.
- If an input has a key that yields a cached output, no work is done.

## Usage
```sh
cargo add cpu-work-queue
```

## Example
```rs
use std::{collections::HashMap, sync::{Arc, Mutex}};

use bounded_work_queue::{BoundedWorkQueue, CancellationToken, Work, err::Error};

/// Our task: some stupid and weird arithmetic, a pure function of a natural number n. 
#[derive(Default)]
struct Accumulate {
    cache: Mutex<HashMap<u64, u64>>,
}

impl Work for Accumulate {
    type Input = u64;
    type Output = Option<u64>;
    type Key = u64;

    // our work is pure and determinstic given a fixed n, so n is the cache key. 
    fn key(&self, n: &u64) -> u64 {
        *n
    }

    fn run(&self, n: u64, cancel: CancellationToken) -> Option<u64> {
        let mut acc: u64 = 0;
        for _ in 0..n {
            if cancel.is_cancelled() {
                // for some tasks, it also makes sense to return the result mid-search:
                // return Some(acc); 
                return None;
            }
            let product = (1..=n).fold(1u64, |p, i| p.wrapping_mul(i).rotate_left((n % 64) as u32));
            acc = acc.wrapping_add(product);
        }
        Some(acc)
    }

    // check for an existing result in the cache
    async fn cached(&self, n: &u64) -> Option<Option<u64>> {
        self.cache.lock().unwrap().get(n).map(|&v| Some(v))
    }

    // store an output in the cache
    async fn store(&self, n: &u64, output: &Option<u64>, cancelled: bool) {
        if !cancelled && let Some(v) = output {
            self.cache.lock().unwrap().insert(*n, *v);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let queue = Arc::new(
        BoundedWorkQueue::new(Accumulate::default())
            .max_concurrent(8)
            .max_waiting(24)
            // give it 10 seconds to run
            .set_timeout(10_000),
    );

    let first = tokio::spawn({
        let queue = queue.clone();
        async move { queue.submit(20_000).await }
    });
    let second = tokio::spawn({
        let queue = queue.clone();
        async move { queue.submit(20_000).await }
    });
    
    // first and second should return the same result, at the same time.

    let (first, second) = (first.await.unwrap()?, second.await.unwrap()?);
    assert!(Arc::ptr_eq(&first, &second));

    let cached = queue.submit(20_000).await?;
    assert_eq!(cached, first);

    Ok(())
}
```
