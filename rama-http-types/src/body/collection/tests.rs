use super::*;
use crate::{
    HeaderMap, HeaderValue,
    body::util::{BodyExt as _, StreamBody},
};
use rama_core::futures::{StreamExt as _, stream};
use std::{
    future::Future as _,
    io,
    sync::Arc,
    task::{Context, Waker},
};

#[derive(Debug)]
struct Observation {
    result: Result<Collected<Bytes>, CollectionIssue<Arc<io::Error>>>,
    prefix: Bytes,
    overflow: Option<Frame<Bytes>>,
    trailers: Option<HeaderMap>,
    unread: Option<Result<Frame<Bytes>, Arc<io::Error>>>,
}

#[tokio::test]
async fn retains_native_failure_and_the_complete_valid_prefix() -> Result<(), Box<Observation>> {
    let native = Arc::new(io::Error::from(io::ErrorKind::ConnectionReset));
    let mut trailers = HeaderMap::new();
    trailers.insert("x-complete", HeaderValue::from_static("yes"));
    let body = StreamBody::new(stream::iter([
        Ok(Frame::data(Bytes::from_static(b"prefix"))),
        Ok(Frame::trailers(trailers.clone())),
        Err(native.clone()),
    ]));
    let mut operation = BodyCollection::new(body, 32);
    let result = operation.collect().await;
    let (mut prefix, overflow, mut body) = operation.into_parts();
    let observed = Observation {
        result,
        trailers: prefix.take_trailers(),
        prefix: prefix.to_bytes(),
        overflow,
        unread: body.frame().await,
    };
    let valid = matches!(&observed.result, Err(CollectionIssue::Stream(cause)) if Arc::ptr_eq(cause, &native))
        && observed.prefix.as_ref() == b"prefix"
        && observed.trailers == Some(trailers)
        && observed.overflow.is_none()
        && observed.unread.is_none();
    valid.then_some(()).ok_or_else(|| Box::new(observed))
}

#[tokio::test]
async fn bounds_buffering_without_losing_overflow_or_unread_frames() -> Result<(), Box<Observation>>
{
    let body = StreamBody::new(stream::iter([
        Ok(Frame::data(Bytes::from_static(b"abcdef"))),
        Ok(Frame::data(Bytes::from_static(b"tail"))),
    ]));
    let mut operation = BodyCollection::new(body, 3);
    let result = operation.collect().await;
    let (mut prefix, overflow, mut body) = operation.into_parts();
    let observed = Observation {
        result,
        trailers: prefix.take_trailers(),
        prefix: prefix.to_bytes(),
        overflow,
        unread: body.frame().await,
    };
    let valid = matches!(observed.result, Err(CollectionIssue::Limit { limit: 3 }))
        && observed.prefix.as_ref() == b"abc"
        && observed
            .overflow
            .as_ref()
            .and_then(Frame::data_ref)
            .is_some_and(|data| data.as_ref() == b"def")
        && matches!(&observed.unread, Some(Ok(frame)) if frame.data_ref().is_some_and(|data| data.as_ref() == b"tail"));
    valid.then_some(()).ok_or_else(|| Box::new(observed))
}

#[test]
fn cancelled_wait_keeps_observed_progress_with_its_owner() -> Result<(), (usize, Bytes)> {
    let body = StreamBody::new(
        stream::iter([Ok::<_, io::Error>(Frame::data(Bytes::from_static(
            b"partial",
        )))])
        .chain(stream::pending()),
    );
    let mut operation = BodyCollection::new(body, 32);
    let pending = {
        let mut waiting = std::pin::pin!(operation.collect());
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    };
    let count = operation.received();
    let (prefix, _, _) = operation.into_parts();
    let observed = (count, prefix.to_bytes());
    (pending && observed.0 == 7 && observed.1.as_ref() == b"partial")
        .then_some(())
        .ok_or(observed)
}
