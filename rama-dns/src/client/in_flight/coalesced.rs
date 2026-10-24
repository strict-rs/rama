use super::{InFlight, Outcome, outcome_stream};
use pin_project_lite::pin_project;
use rama_core::{
    error::BoxError,
    futures::{Stream, StreamExt as _, async_stream::stream_fn},
};
use std::{
    hash::Hash,
    mem::take,
    pin::{Pin, pin},
    task::{Context, Poll, ready},
    time::Duration,
};

impl<T> Outcome<T> {
    /// Drain `stream` up to and including its first error.
    pub(crate) fn collect<S>(stream: S) -> Collect<S, T>
    where
        S: Stream<Item = Result<T, BoxError>>,
    {
        Collect {
            stream,
            records: Vec::new(),
        }
    }
}

pin_project! {
    /// The future of [`Outcome::collect`]: it stores its stream once, where
    /// an `async fn` would hold it twice.
    pub(crate) struct Collect<S, T> {
        #[pin]
        stream: S,
        records: Vec<T>,
    }
}

impl<S, T> Future for Collect<S, T>
where
    S: Stream<Item = Result<T, BoxError>>,
{
    type Output = Outcome<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match ready!(this.stream.as_mut().poll_next(cx)) {
                Some(Ok(record)) => this.records.push(record),
                Some(Err(err)) => return Poll::Ready(Outcome::new(take(this.records), Some(err))),
                None => return Poll::Ready(Outcome::new(take(this.records), None)),
            }
        }
    }
}

/// Run a streaming `lookup` once for all concurrent callers of `key`.
pub(crate) fn coalesced_stream<K, T, S>(
    in_flight: InFlight<K, Outcome<T>>,
    key: K,
    timeout: Duration,
    lookup: impl FnOnce() -> S + Send + 'static,
) -> impl Stream<Item = Result<T, BoxError>> + Send
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    S: Stream<Item = Result<T, BoxError>> + Send + 'static,
{
    // one stream, so waiting for the run and replaying it share their state
    stream_fn(async move |mut yielder| {
        let outcome = in_flight
            .run(key, timeout, move || Outcome::collect(lookup()))
            .await;
        let mut items = pin!(outcome_stream(outcome));
        while let Some(item) = items.next().await {
            yielder.yield_item(item).await;
        }
    })
}
