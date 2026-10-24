use super::HttpServeResult;
use rama_http_core::server::conn::auto::Builder as AutoBuilder;
use rama_http_core::server::conn::http1::Builder as Http1Builder;
use rama_http_core::server::conn::http2::Builder as Http2Builder;
use rama_net::conn::is_connection_error;
use std::error::Error;

/// A utility trait to allow any of the http-core server builders to be used
/// in the same way to (http) serve a connection.
pub trait HttpCoreConnServer: Send + Sync + private::Sealed + 'static {}

impl HttpCoreConnServer for Http1Builder {}

impl HttpCoreConnServer for Http2Builder {}

impl HttpCoreConnServer for AutoBuilder {}

// This convenience server retains its existing clean-peer-close policy.
// Callers that need every terminal observation own the core connection future.
fn map_auto_http_core_result(
    result: Result<(), rama_http_core::server::conn::auto::AutoConnectionError>,
) -> HttpServeResult {
    use rama_http_core::server::conn::auto::{AutoConnectionError, DetectionError};
    match result {
        Ok(()) => Ok(()),
        Err(AutoConnectionError::Http1(error)) => map_http_core_result(Err(error)),
        Err(AutoConnectionError::Http2(error)) => map_http_core_result(Err(error)),
        Err(AutoConnectionError::Detection(DetectionError::Io(error)))
            if is_connection_error(&error) =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn map_http_core_result<E: Error + Send + Sync + 'static>(
    result: rama_http_core::Result<(), E>,
) -> HttpServeResult {
    use rama_http_core::Cause;
    let error = match result {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };
    // A second failure during required cleanup is never a clean close.
    if !error.preceding().is_empty()
        || matches!(error.cause_ref(), Some(Cause::Finalization { .. }))
    {
        return Err(error.into());
    }
    if error.is_canceled() || error.is_closed() {
        return Ok(());
    }
    match error.cause_ref() {
        Some(Cause::Http2(cause)) if cause.is_go_away() || cause.is_io() => Ok(()),
        Some(Cause::Io(cause)) if is_connection_error(cause) => Ok(()),
        _ => Err(error.into()),
    }
}

mod private {
    use super::{map_auto_http_core_result, map_http_core_result};
    use crate::server::HttpServeResult;

    use rama_core::Service;
    use rama_core::extensions::ExtensionsRef;
    use rama_core::futures::FutureExt;
    use rama_core::graceful::ShutdownGuard;
    use rama_core::io::Io;
    use rama_core::telemetry::tracing;
    use rama_http::service::web::response::IntoResponse;
    use rama_http_core::service::RamaHttpService;
    use rama_http_types::Request;

    use std::convert::Infallible;
    use std::pin::pin;
    use tokio::select;

    pub trait Sealed {
        fn http_core_serve_connection<IO, S, Response, G>(
            &self,
            io: IO,
            service: S,
            guard: Option<ShutdownGuard>,
            graceful: G,
        ) -> impl Future<Output = HttpServeResult> + Send + '_
        where
            IO: Io + ExtensionsRef,
            S: Service<Request, Output = Response, Error = Infallible> + Clone,
            Response: IntoResponse + Send + 'static,
            G: Future<Output = ()> + Send + 'static;
    }

    /// Resolves on executor shutdown or on the connection's own signal.
    async fn shutdown_signal<G>(guard: Option<&ShutdownGuard>, graceful: G)
    where
        G: Future<Output = ()>,
    {
        let guard = async {
            match guard {
                Some(guard) => guard.cancelled().await,
                None => std::future::pending().await,
            }
        };
        select! {
            () = guard => (),
            () = graceful => (),
        }
    }

    macro_rules! serve_until_graceful_shutdown {
        ($conn:ident, $guard:ident, $graceful:ident, $map:ident) => {{
            let mut shutdown = pin!(shutdown_signal($guard.as_ref(), $graceful).fuse());
            select! {
                () = shutdown.as_mut() => {
                    tracing::trace!("signal received: initiate graceful shutdown");
                    $conn.as_mut().graceful_shutdown();
                }
                result = $conn.as_mut() => {
                    tracing::trace!("connection finished");
                    return $map(result);
                }
            }
            let result = $conn.as_mut().await;
            tracing::trace!("connection finished after graceful shutdown");
            $map(result)
        }};
    }

    impl Sealed for super::Http1Builder {
        #[inline]
        async fn http_core_serve_connection<IO, S, Response, G>(
            &self,
            io: IO,
            service: S,
            guard: Option<ShutdownGuard>,
            graceful: G,
        ) -> HttpServeResult
        where
            IO: Io + ExtensionsRef,
            S: Service<Request, Output = Response, Error = Infallible> + Clone,
            Response: IntoResponse + Send + 'static,
            G: Future<Output = ()> + Send + 'static,
        {
            let service = RamaHttpService::new(service);
            let stream = Box::pin(io);
            let mut conn = pin!(self.serve_connection(stream, service).with_upgrades());
            serve_until_graceful_shutdown!(conn, guard, graceful, map_http_core_result)
        }
    }

    impl Sealed for super::Http2Builder {
        #[inline]
        async fn http_core_serve_connection<IO, S, Response, G>(
            &self,
            io: IO,
            service: S,
            guard: Option<ShutdownGuard>,
            graceful: G,
        ) -> HttpServeResult
        where
            IO: Io + ExtensionsRef,
            S: Service<Request, Output = Response, Error = Infallible> + Clone,
            Response: IntoResponse + Send + 'static,
            G: Future<Output = ()> + Send + 'static,
        {
            let service = RamaHttpService::new(service);
            let stream = Box::pin(io);
            let mut conn = pin!(self.serve_connection(stream, service));
            serve_until_graceful_shutdown!(conn, guard, graceful, map_http_core_result)
        }
    }

    impl Sealed for super::AutoBuilder {
        #[inline]
        async fn http_core_serve_connection<IO, S, Response, G>(
            &self,
            io: IO,
            service: S,
            guard: Option<ShutdownGuard>,
            graceful: G,
        ) -> HttpServeResult
        where
            IO: Io + ExtensionsRef,
            S: Service<Request, Output = Response, Error = Infallible> + Clone,
            Response: IntoResponse + Send + 'static,
            G: Future<Output = ()> + Send + 'static,
        {
            let service = RamaHttpService::new(service);
            let stream = Box::pin(io);
            let mut conn = pin!(self.serve_connection_with_upgrades(stream, service));
            serve_until_graceful_shutdown!(conn, guard, graceful, map_auto_http_core_result)
        }
    }
}
