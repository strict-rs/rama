//! Owned, bounded connection attempts with concrete progress and cancellation receipts.

use std::{
    fmt,
    future::Future,
    net::SocketAddr,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use rama_core::futures::{Stream, stream::FuturesUnordered};

/// A failure observed before a connection was selected.
#[derive(Debug)]
pub enum RaceAttemptFailure<R, D> {
    Resolution(R),
    Dial { address: SocketAddr, cause: D },
}

/// How far the operation observed its candidate source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaceCandidates {
    Unfinished,
    Exhausted,
    LimitReached { limit: NonZeroUsize },
}

/// Observations retained when connection establishment ends or is cancelled.
#[derive(Debug)]
pub struct ConnectionRaceReceipt<R, D> {
    pub failures: Vec<RaceAttemptFailure<R, D>>,
    /// Futures dropped without observing their terminal result. This does not claim that no socket was established.
    pub abandoned_dials: Vec<SocketAddr>,
    pub candidates: RaceCandidates,
}

/// The selected connection together with the other observations from its race.
#[derive(Debug)]
pub struct RaceConnected<C, R, D> {
    pub address: SocketAddr,
    pub connection: C,
    pub receipt: ConnectionRaceReceipt<R, D>,
}

/// Establishment ended without a successful connection.
#[derive(Debug)]
pub struct ConnectionRaceFailure<R, D> {
    pub receipt: ConnectionRaceReceipt<R, D>,
}

impl<R: fmt::Debug, D: fmt::Debug> fmt::Display for ConnectionRaceFailure<R, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "connection establishment failed: {:?}", self.receipt)
    }
}

impl<R: std::error::Error + 'static, D: std::error::Error + 'static> std::error::Error
    for ConnectionRaceFailure<R, D>
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.receipt.failures.last().map(|failure| match failure {
            RaceAttemptFailure::Resolution(cause) => cause as &(dyn std::error::Error + 'static),
            RaceAttemptFailure::Dial { cause, .. } => cause,
        })
    }
}

/// Cancellation is distinct from observing a failed dial or cancelling an already completed operation.
#[derive(Debug)]
pub enum RaceCancellation<R, D, Reason> {
    Cancelled {
        reason: Reason,
        receipt: ConnectionRaceReceipt<R, D>,
    },
    AlreadyCompleted {
        reason: Reason,
    },
}

pin_project! {
    struct PendingDial<F> {
        address: SocketAddr,
        #[pin]
        future: F,
    }
}

impl<F, C, D> Future for PendingDial<F>
where
    F: Future<Output = Result<C, D>>,
{
    type Output = (SocketAddr, Result<C, D>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx).map(|result| (*this.address, result))
    }
}

/// A caller-owned connection race. Polling performs work; construction performs none.
///
/// Candidate records, including resolution failures, consume the candidate budget. At most `max_in_flight` dials are polled concurrently. Earlier failures survive a later success. To retain progress on a deadline, poll this value by mutable reference, then pass the deadline's concrete cause to [`Self::cancel`]. Dropping the value cancels its local futures but cannot report a receipt.
pub struct ConnectionRace<S, F, Fut, R, D> {
    candidates: Pin<Box<S>>,
    dial: Box<F>,
    in_flight: FuturesUnordered<PendingDial<Fut>>,
    failures: Vec<RaceAttemptFailure<R, D>>,
    source: RaceCandidates,
    observed_candidates: usize,
    max_candidates: NonZeroUsize,
    max_in_flight: NonZeroUsize,
    completed: bool,
}

// Candidates and dial futures maintain their own pinning through Box and
// FuturesUnordered. This owner never exposes a pinned reference to a failure.
impl<S, F, Fut, R, D> Unpin for ConnectionRace<S, F, Fut, R, D> {}

impl<S, F, Fut, R, D> ConnectionRace<S, F, Fut, R, D> {
    pub fn new(
        candidates: S,
        max_in_flight: NonZeroUsize,
        max_candidates: NonZeroUsize,
        dial: F,
    ) -> Self {
        Self {
            candidates: Box::pin(candidates),
            dial: Box::new(dial),
            in_flight: FuturesUnordered::new(),
            failures: Vec::new(),
            source: RaceCandidates::Unfinished,
            observed_candidates: 0,
            max_candidates,
            max_in_flight,
            completed: false,
        }
    }

    fn take_receipt(&mut self) -> ConnectionRaceReceipt<R, D> {
        let abandoned_dials = Pin::new(&self.in_flight)
            .iter_pin_ref()
            .map(|attempt| attempt.get_ref().address)
            .collect();
        self.in_flight.clear();
        ConnectionRaceReceipt {
            failures: std::mem::take(&mut self.failures),
            abandoned_dials,
            candidates: self.source,
        }
    }

    /// Stop polling candidates and dials, preserving the caller's reason and every observed failure.
    pub fn cancel<Reason>(mut self, reason: Reason) -> RaceCancellation<R, D, Reason> {
        if self.completed {
            return RaceCancellation::AlreadyCompleted { reason };
        }
        RaceCancellation::Cancelled {
            reason,
            receipt: self.take_receipt(),
        }
    }
}

#[cfg(test)]
mod tests;

impl<S, F, Fut, R: fmt::Debug, D: fmt::Debug> fmt::Debug for ConnectionRace<S, F, Fut, R, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionRace")
            .field("failures", &self.failures)
            .field("source", &self.source)
            .field("observed_candidates", &self.observed_candidates)
            .field("max_candidates", &self.max_candidates)
            .field("max_in_flight", &self.max_in_flight)
            .field("completed", &self.completed)
            .finish_non_exhaustive()
    }
}

impl<S, C, F, Fut, R, D> Future for ConnectionRace<S, F, Fut, R, D>
where
    S: Stream<Item = Result<SocketAddr, R>>,
    F: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = Result<C, D>>,
{
    type Output = Result<RaceConnected<C, R, D>, ConnectionRaceFailure<R, D>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.completed {
            return Poll::Pending;
        }
        // A ready source of failures cannot monopolize the runtime.
        for _ in 0..64 {
            match Pin::new(&mut this.in_flight).poll_next(cx) {
                Poll::Ready(Some((address, Ok(connection)))) => {
                    this.completed = true;
                    return Poll::Ready(Ok(RaceConnected {
                        address,
                        connection,
                        receipt: this.take_receipt(),
                    }));
                }
                Poll::Ready(Some((address, Err(cause)))) => {
                    this.failures
                        .push(RaceAttemptFailure::Dial { address, cause });
                    continue;
                }
                Poll::Ready(None) | Poll::Pending => {}
            }
            if this.source != RaceCandidates::Unfinished {
                if this.in_flight.is_empty() {
                    this.completed = true;
                    return Poll::Ready(Err(ConnectionRaceFailure {
                        receipt: this.take_receipt(),
                    }));
                }
                return Poll::Pending;
            }
            if this.in_flight.len() >= this.max_in_flight.get() {
                return Poll::Pending;
            }
            match this.candidates.as_mut().poll_next(cx) {
                Poll::Ready(Some(candidate)) => {
                    // The prior comparison bounds this increment, including at usize::MAX.
                    this.observed_candidates += 1;
                    if this.observed_candidates == this.max_candidates.get() {
                        this.source = RaceCandidates::LimitReached {
                            limit: this.max_candidates,
                        };
                    }
                    match candidate {
                        Ok(address) => this.in_flight.push(PendingDial {
                            address,
                            future: (this.dial)(address),
                        }),
                        Err(cause) => this.failures.push(RaceAttemptFailure::Resolution(cause)),
                    }
                }
                Poll::Ready(None) => this.source = RaceCandidates::Exhausted,
                Poll::Pending => return Poll::Pending,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
