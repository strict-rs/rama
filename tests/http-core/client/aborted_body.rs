use std::{fmt, io, net::SocketAddr, sync::Arc, time::Duration};

use futures_channel::{mpsc, oneshot};
use rama::{
    ServiceInput,
    bytes::Bytes,
    futures::SinkExt as _,
    http::{
        Method, Request, Response,
        body::util::StreamBody,
        core::{
            Cause, Error,
            body::{Frame, Incoming},
            client::conn::http1,
        },
    },
};
use tokio::{
    io::AsyncReadExt as _,
    net::{TcpListener, TcpStream},
    time::{Instant, error::Elapsed, timeout_at},
};

const PREFIX: &[u8] = b"POST / HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n";
type Bounded<T> = Result<T, Elapsed>;
type BodyFrame = Result<Frame<Bytes>, io::Error>;

#[derive(Debug)]
struct ProducerReceipt {
    prefix: Bounded<Result<(), mpsc::SendError>>,
    acknowledged: Option<Bounded<Result<(), oneshot::Canceled>>>,
    abort: Option<Bounded<Result<(), mpsc::SendError>>>,
}

async fn produce(
    mut sender: mpsc::Sender<BodyFrame>,
    acknowledged: oneshot::Receiver<()>,
    deadline: Instant,
) -> ProducerReceipt {
    let prefix = timeout_at(
        deadline,
        sender.send(Ok(Frame::data(Bytes::from_static(b"hello")))),
    )
    .await;
    if !matches!(prefix, Ok(Ok(()))) {
        return ProducerReceipt {
            prefix,
            acknowledged: None,
            abort: None,
        };
    }
    let acknowledged = timeout_at(deadline, acknowledged).await;
    if !matches!(acknowledged, Ok(Ok(()))) {
        return ProducerReceipt {
            prefix,
            acknowledged: Some(acknowledged),
            abort: None,
        };
    }
    let abort = timeout_at(
        deadline,
        sender.send(Err(io::Error::from(io::ErrorKind::BrokenPipe))),
    )
    .await;
    ProducerReceipt {
        prefix,
        acknowledged: Some(acknowledged),
        abort: Some(abort),
    }
}

#[derive(Debug)]
struct PeerReceipt {
    prefix: Vec<u8>,
    prefix_read: Bounded<io::Result<usize>>,
    acknowledged: Option<Result<(), ()>>,
    tail: Vec<u8>,
    tail_read: Option<Bounded<io::Result<usize>>>,
}

async fn receive(
    mut peer: TcpStream,
    acknowledged: oneshot::Sender<()>,
    deadline: Instant,
) -> PeerReceipt {
    let mut prefix = Vec::new();
    let prefix_read = loop {
        let read = timeout_at(deadline, peer.read_buf(&mut prefix)).await;
        if !matches!(read, Ok(Ok(n)) if n > 0) || prefix.len() >= PREFIX.len() {
            break read;
        }
    };
    let mut receipt = PeerReceipt {
        prefix,
        prefix_read,
        acknowledged: None,
        tail: Vec::new(),
        tail_read: None,
    };
    if !matches!(receipt.prefix_read, Ok(Ok(n)) if n > 0) || receipt.prefix.len() < PREFIX.len() {
        return receipt;
    }
    receipt.acknowledged = Some(acknowledged.send(()));
    receipt.tail_read = Some(timeout_at(deadline, peer.read_buf(&mut receipt.tail)).await);
    receipt
}

#[derive(Debug)]
struct Observation {
    request: Bounded<Result<Response<Incoming<io::Error>>, Error<io::Error>>>,
    driver: Bounded<Result<(), Error<io::Error>>>,
    producer: ProducerReceipt,
    peer: PeerReceipt,
}

enum Failure {
    Io(io::Error),
    Protocol(Error<io::Error>),
    Observed(Box<Observation>),
}

impl fmt::Debug for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(cause) => f.debug_tuple("TCP fixture setup").field(cause).finish(),
            Self::Protocol(cause) => f.debug_tuple("HTTP fixture setup").field(cause).finish(),
            Self::Observed(received) => f
                .debug_tuple("aborted upload must preserve its prefix and both terminal failures")
                .field(received)
                .finish(),
        }
    }
}

#[tokio::test]
async fn aborted_body_isnt_completed() -> Result<(), Failure> {
    let (sender, receiver) = mpsc::channel::<BodyFrame>(0);
    let mut request = Request::new(StreamBody::new(receiver));
    *request.method_mut() = Method::POST;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .map_err(Failure::Io)?;
    let address = listener.local_addr().map_err(Failure::Io)?;
    let stream = TcpStream::connect(address).await.map_err(Failure::Io)?;
    let (peer, _) = listener.accept().await.map_err(Failure::Io)?;
    let (mut client, connection) = http1::handshake(ServiceInput::new(stream))
        .await
        .map_err(Failure::Protocol)?;
    let (acknowledged, acknowledgement) = oneshot::channel();
    let deadline = Instant::now() + Duration::from_secs(5);

    // Every future stays owned until its bounded terminal result is collected.
    let (request, driver, producer, peer) = tokio::join!(
        timeout_at(deadline, client.send_request(request)),
        timeout_at(deadline, connection),
        produce(sender, acknowledgement, deadline),
        receive(peer, acknowledged, deadline),
    );
    let observed = Observation {
        request,
        driver,
        producer,
        peer,
    };
    let (Ok(Err(request)), Ok(Err(driver))) = (&observed.request, &observed.driver) else {
        return Err(Failure::Observed(Box::new(observed)));
    };
    let shared_cause = matches!((request.cause_ref(), driver.cause_ref()),
        (Some(Cause::Body(request)), Some(Cause::Body(driver)))
        if Arc::ptr_eq(request, driver) && request.kind() == io::ErrorKind::BrokenPipe);
    let prefix_written = matches!(
        &observed.producer,
        ProducerReceipt {
            prefix: Ok(Ok(())),
            acknowledged: Some(Ok(Ok(()))),
            abort: Some(Ok(Ok(())))
        }
    );
    let peer_closed_without_a_completed_body = observed.peer.prefix == PREFIX
        && matches!(observed.peer.prefix_read, Ok(Ok(n)) if n > 0)
        && matches!(observed.peer.acknowledged, Some(Ok(())))
        && observed.peer.tail.is_empty()
        && matches!(observed.peer.tail_read, Some(Ok(Ok(0))));
    if shared_cause && prefix_written && peer_closed_without_a_completed_body {
        Ok(())
    } else {
        Err(Failure::Observed(Box::new(observed)))
    }
}
