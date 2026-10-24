use super::*;
use parking_lot::Mutex;
use rama_core::{ServiceInput, bytes::Bytes, service::service_fn};
use rama_http_types::{
    StatusCode,
    body::{
        Frame,
        util::{BodyExt as _, Full},
    },
};
use rama_net::uri::Uri;
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::time::{error::Elapsed, timeout};

#[derive(Debug)]
struct BodyObservation {
    status: StatusCode,
    frames: Vec<Result<Frame<Bytes>, Error>>,
}

async fn observe(mut response: Response<Incoming>) -> BodyObservation {
    let status = response.status();
    let mut frames = Vec::new();
    while let Some(frame) = response.body_mut().frame().await {
        let failed = frame.is_err();
        frames.push(frame);
        if failed {
            break;
        }
    }
    BodyObservation { status, frames }
}

#[derive(Debug)]
struct ClientCompletion {
    shutdown: crate::Result<()>,
    termination: SessionTermination<Infallible>,
}

#[derive(Debug)]
struct SessionObservation {
    exchanges: Vec<ExchangeOutcome<BodyObservation, Infallible>>,
    readiness: Vec<crate::Result<()>>,
    client: Result<crate::Result<ClientCompletion>, Elapsed>,
    server: Result<crate::Result<()>, Elapsed>,
    requests: Vec<Uri>,
}

#[tokio::test]
async fn owns_reused_exchanges_and_graceful_completion() -> Result<(), Box<SessionObservation>> {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let capture = requests.clone();
    let service = service_fn(move |request: Request<Incoming>| {
        capture.lock().push(request.uri().clone());
        async { Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok")))) }
    });
    let server = crate::server::conn::http1::Builder::new()
        .serve_connection(ServiceInput::new(server_io), service);
    let mut exchanges = Vec::new();
    let mut readiness = Vec::new();
    let client = async {
        let mut session = super::super::Builder::new()
            .handshake_session(ServiceInput::new(client_io))
            .await?;
        for path in ["/one", "/two"] {
            let mut request = Request::new(Full::new(Bytes::new()));
            *request.uri_mut() = Uri::from_static(path);
            exchanges.push(session.exchange(request, observe).await);
            let ready = session.ready().await;
            let failed = ready.is_err();
            readiness.push(ready);
            if failed {
                break;
            }
        }
        let shutdown = session.shutdown().await;
        Ok(ClientCompletion {
            shutdown,
            termination: session.abort(),
        })
    };
    let (client, server) = tokio::join!(
        timeout(Duration::from_secs(5), client),
        timeout(Duration::from_secs(5), server)
    );
    let observed = SessionObservation {
        exchanges,
        readiness,
        client,
        server,
        requests: requests.lock().clone(),
    };
    let valid = matches!(observed.client, Ok(Ok(ClientCompletion { shutdown: Ok(()), termination: SessionTermination::Completed(Ok(())) })))
        && matches!(observed.readiness.as_slice(), [Ok(()), Ok(())])
        && matches!(observed.server, Ok(Ok(())))
        && observed.requests == [Uri::from_static("/one"), Uri::from_static("/two")]
        && observed.exchanges.len() == 2
        && observed.exchanges.iter().all(|exchange| matches!(&exchange.result, Ok(body)
            if body.status == StatusCode::OK
                && matches!(body.frames.as_slice(), [Ok(frame)] if frame.data_ref().is_some_and(|bytes| bytes.as_ref() == b"ok")))
            && exchange.driver.is_none());
    valid.then_some(()).ok_or_else(|| Box::new(observed))
}

#[derive(Debug)]
struct FailedExchangeObservation {
    phase: Option<ExchangePhase>,
    exchange: Option<ExchangeOutcome<BodyObservation, Infallible>>,
    client: Result<crate::Result<ClientCompletion>, Elapsed>,
    peer: Result<std::io::Result<()>, Elapsed>,
    request: Vec<u8>,
}

#[tokio::test]
async fn failed_request_and_terminal_driver_cause_both_reach_the_owner()
-> Result<(), Box<FailedExchangeObservation>> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let (client_io, mut peer_io) = tokio::io::duplex(4096);
    let mut request = Vec::new();
    let peer = async {
        while !request.ends_with(b"\r\n\r\n") {
            if peer_io.read_buf(&mut request).await? == 0 {
                return Ok(());
            }
        }
        peer_io.write_all(b"invalid response\r\n\r\n").await
    };
    let mut exchange = None;
    let mut phase = None;
    let client = async {
        let mut session = super::super::Builder::new()
            .handshake_session(ServiceInput::new(client_io))
            .await?;
        exchange = Some(
            session
                .exchange(Request::new(Full::new(Bytes::new())), observe)
                .await,
        );
        phase = Some(session.phase());
        // Observe shutdown even when the request already failed.
        let shutdown = session.shutdown().await;
        Ok(ClientCompletion {
            shutdown,
            termination: session.abort(),
        })
    };
    let (client, peer) = tokio::join!(
        timeout(Duration::from_secs(5), client),
        timeout(Duration::from_secs(5), peer)
    );
    let observed = FailedExchangeObservation {
        phase,
        exchange,
        client,
        peer,
        request,
    };
    let valid = observed.phase == Some(ExchangePhase::Submitted)
        && matches!((&observed.exchange, &observed.client),
        (Some(ExchangeOutcome { result: Err(request), .. }), Ok(Ok(ClientCompletion { shutdown: Err(shutdown), termination: SessionTermination::Completed(Err(driver)) })))
        if request.is_parse() && driver.is_parse() && shutdown.is_parse()
            && matches!((request.cause_ref(), driver.cause_ref()), (Some(crate::Cause::Parse(request)), Some(crate::Cause::Parse(driver))) if Arc::ptr_eq(request, driver)))
        && matches!(observed.peer, Ok(Ok(())))
        && observed.request.starts_with(b"GET / HTTP/1.1\r\n");
    valid.then_some(()).ok_or_else(|| Box::new(observed))
}
