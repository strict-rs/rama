//! Bounded collection whose owner retains partial frames across failure or cancellation.

use super::{Frame, StreamingBody, util::Collected};
use rama_core::bytes::Bytes;
use std::{fmt, pin::Pin, task::Poll};

#[cfg(test)]
mod tests;

/// Why collection stopped. Native body errors retain their concrete type.
#[derive(Debug)]
pub enum CollectionIssue<E> {
    /// The next data frame would exceed the accepted byte budget.
    Limit { limit: usize },
    /// The underlying body failed after the retained prefix.
    Stream(E),
    /// This operation already returned a terminal result.
    Finished,
}

impl<E: fmt::Display> fmt::Display for CollectionIssue<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Limit { limit } => write!(f, "body exceeds {limit} bytes"),
            Self::Stream(cause) => cause.fmt(f),
            Self::Finished => f.write_str("body collection already finished"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CollectionIssue<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Stream(cause) => Some(cause),
            _ => None,
        }
    }
}

/// An owned collection operation. Canceling a borrowed `collect` future leaves
/// the body, accepted prefix and trailers here for the owner to inspect or dispose.
pub struct BodyCollection<B> {
    body: Pin<Box<B>>,
    collected: Collected<Bytes>,
    overflow: Option<Frame<Bytes>>,
    limit: usize,
    received: usize,
    finished: bool,
}

/// A terminal issue together with every retained frame and the unread body.
pub struct CollectionFailure<B: StreamingBody<Data = Bytes>> {
    pub cause: CollectionIssue<B::Error>,
    pub progress: BodyCollection<B>,
}

impl<B: StreamingBody<Data = Bytes, Error: fmt::Debug>> fmt::Debug for CollectionFailure<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectionFailure")
            .field("cause", &self.cause)
            .field("received", &self.progress.received)
            .finish_non_exhaustive()
    }
}

impl<B: StreamingBody<Data = Bytes, Error: fmt::Display>> fmt::Display for CollectionFailure<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.cause.fmt(f)
    }
}

impl<B: StreamingBody<Data = Bytes, Error: std::error::Error + 'static>> std::error::Error
    for CollectionFailure<B>
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

impl<B> fmt::Debug for BodyCollection<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BodyCollection")
            .field("collected", &self.collected)
            .field("overflow", &self.overflow)
            .field("limit", &self.limit)
            .field("received", &self.received)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl<B: StreamingBody<Data = Bytes>> BodyCollection<B> {
    /// Bound accepted data bytes without replacing native failures.
    pub fn new(body: B, limit: usize) -> Self {
        Self {
            body: Box::pin(body),
            collected: Collected::default(),
            overflow: None,
            limit,
            received: 0,
            finished: false,
        }
    }

    /// Observe the accepted data count, including a prefix from an oversized frame.
    pub const fn received(&self) -> usize {
        self.received
    }

    /// Consume this operation, returning retained progress alongside a failure.
    pub async fn finish(mut self) -> Result<Collected<Bytes>, CollectionFailure<B>> {
        match self.collect().await {
            Ok(collected) => Ok(collected),
            Err(cause) => Err(CollectionFailure {
                cause,
                progress: self,
            }),
        }
    }

    /// Finish collection. On failure, progress remains owned by this value.
    pub async fn collect(&mut self) -> Result<Collected<Bytes>, CollectionIssue<B::Error>> {
        std::future::poll_fn(|cx| {
            if self.finished {
                return Poll::Ready(Err(CollectionIssue::Finished));
            }
            for _ in 0..64 {
                match self.body.as_mut().poll_frame(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        self.finished = true;
                        return Poll::Ready(Ok(std::mem::take(&mut self.collected)));
                    }
                    Poll::Ready(Some(Err(cause))) => {
                        self.finished = true;
                        return Poll::Ready(Err(CollectionIssue::Stream(cause)));
                    }
                    Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                        Ok(mut data) => {
                            let remaining = self.limit.saturating_sub(self.received);
                            if data.len() > remaining {
                                let prefix = data.split_to(remaining);
                                self.received = self.limit;
                                self.collected.push_frame(Frame::data(prefix));
                                self.overflow = Some(Frame::data(data));
                                self.finished = true;
                                return Poll::Ready(Err(CollectionIssue::Limit {
                                    limit: self.limit,
                                }));
                            }
                            self.received += data.len();
                            self.collected.push_frame(Frame::data(data));
                        }
                        Err(frame) => self.collected.push_frame(frame),
                    },
                }
            }
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await
    }

    /// Recover accepted frames, an overflowing frame remainder and the unread
    /// body in that order. A stream failure is terminal evidence, not permission
    /// to replay an application operation.
    pub fn into_parts(self) -> (Collected<Bytes>, Option<Frame<Bytes>>, Pin<Box<B>>) {
        (self.collected, self.overflow, self.body)
    }
}
