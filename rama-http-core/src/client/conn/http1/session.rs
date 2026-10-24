use super::{Connection, SendRequest};
use crate::{Error, body::Incoming};

#[cfg(test)]
mod tests;
use rama_core::extensions::ExtensionsRef;
use rama_http_types::{Request, Response, body::StreamingBody};
use std::{
    fmt,
    future::Future,
    pin::{Pin, pin},
};
use tokio::io::{AsyncRead, AsyncWrite};

/// A request/consumer result and any independently observed connection-driver result.
#[derive(Debug)]
pub struct ExchangeOutcome<R, E> {
    pub result: Result<R, Error<E>>,
    pub driver: Option<Result<(), Error<E>>>,
}

/// Whether a session observed its driver's terminal result before disposal.
#[derive(Debug)]
pub enum SessionTermination<E> {
    Completed(Result<(), Error<E>>),
    Aborted,
}

/// Observed local exchange progress; submission alone does not prove a remote effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangePhase {
    Idle,
    Admission,
    Submitted,
    Response,
    ConsumerComplete,
    Finalization,
}

/// An HTTP/1 sender and its correlated driver, constructed by [`super::Builder::handshake_session`].
///
/// The session drives I/O while waiting for readiness, a response, or the response consumer. It owns no detached task. Call [`Self::ready`] before returning a completed exchange to reusable storage; call [`Self::shutdown`] for graceful finalization. Deadlines can borrow these futures, then inspect [`Self::terminal`] or consume [`Self::abort`] without losing an already observed terminal result.
pub struct Session<T, B>
where
    T: AsyncRead + AsyncWrite,
    B: StreamingBody<Data: Send + 'static, Error: std::error::Error + Send + Sync + 'static>
        + Send
        + Unpin
        + 'static,
{
    sender: Option<SendRequest<B>>,
    connection: Pin<Box<Connection<T, B>>>,
    terminal: Option<Result<(), Error<B::Error>>>,
    phase: ExchangePhase,
}

impl<T, B> fmt::Debug for Session<T, B>
where
    T: AsyncRead + AsyncWrite,
    B: StreamingBody<Data: Send + 'static, Error: std::error::Error + Send + Sync + 'static>
        + Send
        + Unpin
        + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

impl<T, B> Session<T, B>
where
    T: AsyncRead + AsyncWrite + Unpin + ExtensionsRef,
    B: StreamingBody<Data: Send + 'static, Error: std::error::Error + Send + Sync + 'static>
        + Send
        + Unpin
        + 'static,
{
    pub(super) fn new(sender: SendRequest<B>, connection: Connection<T, B>) -> Self {
        Self {
            sender: Some(sender),
            connection: Box::pin(connection),
            terminal: None,
            phase: ExchangePhase::Idle,
        }
    }

    /// Borrow the actual connection result, if the owner has observed it.
    pub fn terminal(&self) -> Option<&Result<(), Error<B::Error>>> {
        self.terminal.as_ref()
    }

    /// The phase reached by the current exchange or finalization.
    pub fn phase(&self) -> ExchangePhase {
        self.phase
    }

    fn closed(&self) -> Error<B::Error> {
        match &self.terminal {
            Some(Err(cause)) => Error::new_closed().with(cause.clone()),
            Some(Ok(())) | None => Error::new_closed(),
        }
    }

    /// Drive the connection until another request can be admitted or it closes.
    pub async fn ready(&mut self) -> crate::Result<(), B::Error> {
        let Some(sender) = &mut self.sender else {
            return Err(self.closed());
        };
        drive(&mut self.connection, &mut self.terminal, sender.ready()).await
    }

    /// Submit one request and drive its response consumer while retaining driver failures separately.
    pub fn exchange<'a, F, Fut, R>(
        &'a mut self,
        request: Request<B>,
        consume: F,
    ) -> impl Future<Output = ExchangeOutcome<R, B::Error>> + 'a
    where
        F: FnOnce(Response<Incoming<B::Error>>) -> Fut + 'a,
        Fut: Future<Output = R> + 'a,
        R: 'a,
    {
        self.phase = ExchangePhase::Admission;
        async move {
            let Some(sender) = &mut self.sender else {
                return ExchangeOutcome {
                    result: Err(self.closed()),
                    driver: self.terminal.clone(),
                };
            };
            if let Err(error) =
                drive(&mut self.connection, &mut self.terminal, sender.ready()).await
            {
                return ExchangeOutcome {
                    result: Err(error),
                    driver: self.terminal.clone(),
                };
            }
            self.phase = ExchangePhase::Submitted;
            let response = drive(
                &mut self.connection,
                &mut self.terminal,
                sender.send_request(request),
            )
            .await;
            let result = match response {
                Ok(response) => {
                    self.phase = ExchangePhase::Response;
                    let result =
                        drive(&mut self.connection, &mut self.terminal, consume(response)).await;
                    self.phase = ExchangePhase::ConsumerComplete;
                    Ok(result)
                }
                Err(error) => Err(error),
            };
            ExchangeOutcome {
                result,
                driver: self.terminal.clone(),
            }
        }
    }

    /// Close admission and observe the driver's finalization result.
    pub async fn shutdown(&mut self) -> crate::Result<(), B::Error> {
        self.phase = ExchangePhase::Finalization;
        drop(self.sender.take());
        if let Some(result) = &self.terminal {
            return result.clone();
        }
        let result = self.connection.as_mut().await;
        self.terminal = Some(result.clone());
        result
    }

    /// Dispose of the session and report whether a terminal result was observed.
    pub fn abort(self) -> SessionTermination<B::Error> {
        match self.terminal {
            Some(result) => SessionTermination::Completed(result),
            None => SessionTermination::Aborted,
        }
    }
}

async fn drive<T, B, F>(
    connection: &mut Pin<Box<Connection<T, B>>>,
    terminal: &mut Option<Result<(), Error<B::Error>>>,
    future: F,
) -> F::Output
where
    T: AsyncRead + AsyncWrite + Unpin + ExtensionsRef,
    B: StreamingBody<Data: Send + 'static, Error: std::error::Error + Send + Sync + 'static>
        + Send
        + Unpin
        + 'static,
    F: Future,
{
    let mut future = pin!(future);
    loop {
        tokio::select! {
            output = future.as_mut() => return output,
            result = connection.as_mut(), if terminal.is_none() => *terminal = Some(result),
        }
    }
}
