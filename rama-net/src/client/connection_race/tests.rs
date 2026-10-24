use super::*;
use parking_lot::Mutex;
use rama_core::futures::{future::Either, stream};
use std::{io, sync::Arc, task::Waker};

type Outcome = Poll<
    Result<RaceConnected<u16, io::Error, io::Error>, ConnectionRaceFailure<io::Error, io::Error>>,
>;

fn address(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

#[test]
fn success_keeps_resolution_and_dial_failures() -> Result<(), Outcome> {
    let candidates = stream::iter([
        Err(io::Error::from_raw_os_error(13)),
        Ok(address(1)),
        Ok(address(2)),
    ]);
    let mut operation = ConnectionRace::new(
        candidates,
        NonZeroUsize::MIN.saturating_add(1),
        NonZeroUsize::MIN.saturating_add(3),
        |peer: SocketAddr| {
            std::future::ready(if peer.port() == 1 {
                Err(io::Error::from(io::ErrorKind::ConnectionRefused))
            } else {
                Ok(peer.port())
            })
        },
    );
    let received = Pin::new(&mut operation).poll(&mut Context::from_waker(Waker::noop()));
    let valid = matches!(&received, Poll::Ready(Ok(connection))
        if connection.address == address(2) && connection.connection == 2
            && connection.receipt.abandoned_dials.is_empty()
            && matches!(connection.receipt.failures.as_slice(),
                [RaceAttemptFailure::Resolution(resolution), RaceAttemptFailure::Dial { address: peer, cause }]
                if resolution.raw_os_error() == Some(13) && *peer == address(1)
                    && cause.kind() == io::ErrorKind::ConnectionRefused));
    valid.then_some(()).ok_or(received)
}

#[derive(Debug)]
struct Pending {
    peer: SocketAddr,
    dropped: Arc<Mutex<Vec<SocketAddr>>>,
}

impl Future for Pending {
    type Output = Result<u16, io::Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.dropped.lock().push(self.peer);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum StopReason {
    Requested,
}

#[derive(Debug)]
struct CancellationObservation {
    first_poll: Outcome,
    cancellation: RaceCancellation<io::Error, io::Error, StopReason>,
    dropped: Vec<SocketAddr>,
}

#[test]
fn cancellation_keeps_failed_and_abandoned_attempts() -> Result<(), CancellationObservation> {
    let dropped = Arc::new(Mutex::new(Vec::new()));
    let candidates = stream::iter([
        Ok::<_, io::Error>(address(1)),
        Ok(address(2)),
        Ok(address(3)),
    ]);
    let capture = dropped.clone();
    let mut operation = ConnectionRace::new(
        candidates,
        NonZeroUsize::MIN.saturating_add(1),
        NonZeroUsize::MIN.saturating_add(3),
        move |peer: SocketAddr| {
            if peer.port() == 1 {
                Either::Left(std::future::ready(Err(io::Error::from(
                    io::ErrorKind::ConnectionRefused,
                ))))
            } else {
                Either::Right(Pending {
                    peer,
                    dropped: capture.clone(),
                })
            }
        },
    );
    let first_poll = Pin::new(&mut operation).poll(&mut Context::from_waker(Waker::noop()));
    let cancellation = operation.cancel(StopReason::Requested);
    let mut observed = CancellationObservation {
        first_poll,
        cancellation,
        dropped: dropped.lock().clone(),
    };
    observed.dropped.sort();
    let RaceCancellation::Cancelled { reason, receipt } = &mut observed.cancellation else {
        return Err(observed);
    };
    receipt.abandoned_dials.sort();
    let valid = matches!(observed.first_poll, Poll::Pending)
        && *reason == StopReason::Requested
        && receipt.abandoned_dials == [address(2), address(3)]
        && observed.dropped == [address(2), address(3)]
        && matches!(receipt.failures.as_slice(), [RaceAttemptFailure::Dial { address: peer, cause }]
            if *peer == address(1) && cause.kind() == io::ErrorKind::ConnectionRefused);
    valid.then_some(()).ok_or(observed)
}

#[test]
fn candidate_budget_stops_before_an_unadmitted_dial() -> Result<(), (Outcome, Vec<SocketAddr>)> {
    let attempted = Arc::new(Mutex::new(Vec::new()));
    let capture = attempted.clone();
    let candidates = stream::iter([Ok::<_, io::Error>(address(1)), Ok(address(2))]);
    let mut operation = ConnectionRace::new(
        candidates,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        move |peer| {
            capture.lock().push(peer);
            std::future::ready(Err::<u16, _>(io::Error::from(
                io::ErrorKind::ConnectionRefused,
            )))
        },
    );
    let received = Pin::new(&mut operation).poll(&mut Context::from_waker(Waker::noop()));
    let attempted = attempted.lock().clone();
    let valid = attempted == [address(1)]
        && matches!(&received, Poll::Ready(Err(failure))
        if failure.receipt.candidates == RaceCandidates::LimitReached { limit: NonZeroUsize::MIN }
            && failure.receipt.abandoned_dials.is_empty()
            && matches!(failure.receipt.failures.as_slice(), [RaceAttemptFailure::Dial { address: peer, cause }]
                if *peer == address(1) && cause.kind() == io::ErrorKind::ConnectionRefused));
    valid.then_some(()).ok_or((received, attempted))
}
