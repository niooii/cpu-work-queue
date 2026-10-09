use std::{collections::HashMap, hash::Hash, sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}}, time::Duration};

use tokio::sync::{Semaphore, watch::{Receiver, channel}};

use crate::err::Error;
pub mod err;

#[derive(Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

pub struct BoundedWorkQueue<W> where W: Work {
    work: Arc<W>,
    max_waiting: usize,
    upper_bound_ms: usize,

    in_flight: Arc<Mutex<
        HashMap<W::Key, InFlight<W>>
    >>,

    permits: Arc<Semaphore>,
    waiting_count: Arc<AtomicUsize>
}

struct InFlight<W> where W: Work {
    rx: Receiver<Option<Result<Arc<W::Output>, Error>>>,
    cancel: CancellationToken,
    waiters: usize
}

/// A handle that represents ongoing work.
/// When all Waiters for a specific Key are dropped, the underlying work is cancelled.
struct Waiter<W> where W: Work {
    in_flight: Arc<Mutex<HashMap<W::Key, InFlight<W>>>>,
    key: W::Key,
    cancel: CancellationToken
}

impl<W: Work> Drop for Waiter<W> {
    fn drop(&mut self) {
        let mut map = self.in_flight.lock().unwrap();
        if let Some(job) = map.get_mut(&self.key)
            && Arc::ptr_eq(&job.cancel.0, &self.cancel.0)
        {
            job.waiters -= 1;
            if job.waiters == 0 {
                job.cancel.cancel();
            }
        }
    }
}

pub trait Work: Send + Sync + 'static {
    type Input: Send + 'static;
    type Output: Send + Sync + 'static;
    type Key: Hash + Eq + Clone + Send + Sync + 'static;

    /// A deterministic key derivation function, used for cache lookups and work deduplication.
    fn key(&self, input: &Self::Input) -> Self::Key;

    /// The function that does the actual work. It is the caller's responsibility to implement cancellation correctly.
    fn run(&self, input: Self::Input, cancel: CancellationToken) -> Self::Output;

    /// Retrieve a cached output.
    fn cached(&self, _key: &Self::Key) -> impl Future<Output = Option<Self::Output>> + Send {
        async { None }
    }

    /// Called whenever an output is returned by the executed work, intended to store output in some cache.
    /// It is the caller's choice whether to store cancelled outputs or not. 
    fn store(&self, _key: &Self::Key, _output: &Self::Output, _cancelled: bool) -> impl Future<Output = ()> + Send {
        async {}
    }
}

impl<W: Work> BoundedWorkQueue<W> {
    /// Default limits: 8 concurrent, 24 waiting, 120 seconds upper bound for work
    pub fn new(work: W) -> Self {
        Self {
            work: Arc::new(work),
            max_waiting: 24,
            in_flight: Default::default(),
            waiting_count: Default::default(),
            upper_bound_ms: 120_000,
            permits: Arc::new(Semaphore::new(8))
        }
    }

    /// Set the maximum concurrent jobs to be running at once
    pub fn max_concurrent(self, n: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(n)),
            ..self
        }
    }

    /// Set the maximum number of jobs that can wait in the queue (not being solved)
    pub fn max_waiting(self, n: usize) -> Self {
        Self {
            max_waiting: n,
            ..self
        }
    }

    /// Set the upper bound CPU time of the work before it gets canceled, in ms.
    pub fn set_timeout(self, n: usize) -> Self {
        Self {
            upper_bound_ms: n,
            ..self
        }
    }

    /// Submit some work to be done. 
    pub async fn submit(&self, input: W::Input) -> Result<Arc<W::Output>, Error> {
        let key = self.work.key(&input);

        let (mut rx, cancel) = {
            let mut map = self.in_flight.lock().unwrap();

            if let Some(job) = map.get_mut(&key) && !job.cancel.is_cancelled() {

                job.waiters += 1;
                (job.rx.clone(), job.cancel.clone())

            } else {

                if self.waiting_count.load(Ordering::Relaxed) >= self.max_waiting {
                    return Err(Error::QueueFull);
                }
                self.waiting_count.fetch_add(1, Ordering::Relaxed);

                let (tx, rx) = channel(None);
                let cancel = CancellationToken::default();
                map.insert(key.clone(), InFlight { rx: rx.clone(), cancel: cancel.clone(), waiters: 1 });

                let _work = self.work.clone();
                let _cancel = cancel.clone();
                let _key = key.clone();
                let _map = self.in_flight.clone();
                let _permits = self.permits.clone();
                let _waiting = self.waiting_count.clone();
                let _ub = self.upper_bound_ms;

                tokio::spawn(async move {
                    let out = match _work.cached(&_key).await {
                        Some(hit) => {
                            _waiting.fetch_sub(1, Ordering::Relaxed);
                            Ok(Arc::new(hit))
                        }
                        None => {
                            // waits if max concurrent jobs are running
                            let _p = _permits.acquire_owned().await.expect("will never happen");
                            _waiting.fetch_sub(1, Ordering::Relaxed);

                            if _cancel.is_cancelled() {
                                Err(Error::Cancelled)
                            } else {
                                // do work and fire signal here
                                let mut handle = tokio::task::spawn_blocking({
                                    let _work = _work.clone();
                                    let _cancel = _cancel.clone();
                                    move || _work.run(input, _cancel)
                                });

                                // wait for an upper boudn then attempt to cancel the work
                                let out = tokio::select! {
                                    out = &mut handle => out,
                                    // if there exists a 128 bit platform in the future, i am so so sorry
                                    _ = tokio::time::sleep(Duration::from_millis(_ub as u64)) => {
                                        // attempt to cancel, the cancellation result should be sent over the channel now
                                        _cancel.cancel();
                                        handle.await
                                    }
                                };

                                let out = out.map(Arc::new).map_err(|_| Error::Panicked);
                                if let Ok(out) = &out {
                                    _work.store(&_key, out, _cancel.is_cancelled()).await;
                                }
                                out
                            }
                        }
                    };

                    {
                        let mut map = _map.lock().unwrap();
                        if map.get(&_key).is_some_and(|job| Arc::ptr_eq(&job.cancel.0, &_cancel.0)) {
                            map.remove(&_key);
                        }
                    }
                    let _ = tx.send(Some(out));
                });

                (rx, cancel)
            }

        };

        let _waiter = Waiter { in_flight: self.in_flight.clone(), key, cancel };

        rx.wait_for(Option::is_some).await?.clone().expect("will never happen")
    }
}
