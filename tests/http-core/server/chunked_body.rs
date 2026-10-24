use std::{convert::Infallible, fmt, io, net::SocketAddr, sync::Arc, time::Duration};

use parking_lot::Mutex;
use rama::{
    ServiceInput,
    bytes::Bytes,
    http::{
        Method, Request, Response, StatusCode,
        body::{
            BodyCollection, CollectionFailure, CollectionIssue,
            util::{Collected, Empty},
        },
        core::{Cause, Error, body::Incoming, server::conn::http1},
        request::Parts,
    },
    net::uri::Uri,
    service::service_fn,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    time::{Instant, error::Elapsed, timeout_at},
};

const REQUEST: &[u8] = b"\
    POST / HTTP/1.1\r\n\
    Host: example.domain\r\n\
    Transfer-Encoding: chunked\r\n\
    \r\n\
    f0000000000000003\r\n\
    abc\r\n\
    0\r\n\
    \r\n\
    GET /sneaky HTTP/1.1\r\n\
    \r\n";

#[derive(Debug)]
struct RequestObservation {
    head: Parts,
    body: Result<Collected<Bytes>, CollectionFailure<Incoming>>,
}

#[derive(Debug)]
struct Observation {
    requests: Vec<RequestObservation>,
    driver: Result<Result<(), Error>, Elapsed>,
    peer: Result<(), Elapsed>,
    write: Option<io::Result<()>>,
    read: Option<io::Result<usize>>,
    shutdown: Option<io::Result<()>>,
    response: Vec<u8>,
}

enum Failure {
    Setup(io::Error),
    Observed(Box<Observation>),
}

impl fmt::Debug for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Setup(cause) => f.debug_tuple("TCP fixture setup").field(cause).finish(),
            Self::Observed(received) => f
                .debug_tuple("overflowing chunk size must fail both owners without dispatching another request")
                .field(received)
                .finish(),
        }
    }
}

#[tokio::test]
async fn post_with_chunked_overflow() -> Result<(), Failure> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .map_err(Failure::Setup)?;
    let address = listener.local_addr().map_err(Failure::Setup)?;
    let mut peer = TcpStream::connect(address).await.map_err(Failure::Setup)?;
    let (stream, _) = listener.accept().await.map_err(Failure::Setup)?;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let capture = requests.clone();
    let service = service_fn(move |request: Request<Incoming>| {
        let capture = capture.clone();
        async move {
            let (head, body) = request.into_parts();
            let body = BodyCollection::new(body, 1024).finish().await;
            capture.lock().push(RequestObservation { head, body });
            let mut response = Response::new(Empty::<Bytes>::new());
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            Ok::<_, Infallible>(response)
        }
    });
    let driver = http1::Builder::new().serve_connection(ServiceInput::new(stream), service);
    let mut write = None;
    let mut read = None;
    let mut shutdown = None;
    let mut response = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(1);
    let client = async {
        write = Some(peer.write_all(REQUEST).await);
        if matches!(write, Some(Ok(()))) {
            read = Some(peer.read_to_end(&mut response).await);
        }
        shutdown = Some(peer.shutdown().await);
    };
    let (peer, driver) = tokio::join!(timeout_at(deadline, client), timeout_at(deadline, driver));
    let observed = Observation {
        requests: std::mem::take(&mut *requests.lock()),
        driver,
        peer,
        write,
        read,
        shutdown,
        response,
    };
    let ([request], Ok(Err(driver))) = (observed.requests.as_slice(), &observed.driver) else {
        return Err(Failure::Observed(Box::new(observed)));
    };
    let Err(body) = &request.body else {
        return Err(Failure::Observed(Box::new(observed)));
    };
    let shared_native_cause = match (&body.cause, driver.cause_ref()) {
        (CollectionIssue::Stream(body), Some(Cause::Io(driver))) => {
            matches!(body.cause_ref(), Some(Cause::Io(body))
                if Arc::ptr_eq(body, driver) && body.kind() == io::ErrorKind::InvalidData)
        }
        _ => false,
    };
    let rejected_before_body_data = request.head.method == Method::POST
        && request.head.uri == Uri::from_static("/")
        && body.progress.received() == 0;
    let peer_received_rejection_and_eof = matches!(observed.peer, Ok(()))
        && matches!(observed.write, Some(Ok(())))
        && matches!(observed.read, Some(Ok(length)) if length == observed.response.len())
        && matches!(observed.shutdown, Some(Ok(())))
        && observed
            .response
            .starts_with(b"HTTP/1.1 500 Internal Server Error\r\n");
    if shared_native_cause && rejected_before_body_data && peer_received_rejection_and_eof {
        Ok(())
    } else {
        Err(Failure::Observed(Box::new(observed)))
    }
}
