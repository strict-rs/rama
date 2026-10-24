//! Error and Result module.

use std::error::Error as StdError;
use std::{convert::Infallible, fmt, io, sync::Arc};

use crate::h2;
use rama_http_types as http;

/// Result type often returned from methods that can have hyper `Error`s.
pub type Result<T, E = Infallible> = std::result::Result<T, Error<E>>;

/// Represents errors that can occur handling HTTP streams.
///
/// # Formatting
///
/// The `Display` implementation of this type will only print the details of
/// this level of error, even though it may have been caused by another error
/// and contain that error in its source. To print all the relevant
/// information, including the source chain, using something like
/// `std::error::Report`, or equivalent 3rd party types.
///
/// The contents of the formatted error message of this specific `Error` type
/// is unspecified. **You must not depend on it.** The wording and details may
/// change in any version, with the goal of improving error messages.
///
/// # Causes
///
/// Native protocol causes and the caller's body failure type remain available
/// through [`Self::cause_ref`]. Rendering an error does not replace its evidence.
pub struct Error<E = Infallible> {
    inner: Box<ErrorImpl<E>>,
}

struct ErrorImpl<E> {
    kind: Kind,
    cause: Option<Cause<E>>,
    preceding: Vec<Error<E>>,
}

/// The complete cause observed by the HTTP protocol owner.
#[derive(Debug)]
pub enum Cause<E = Infallible> {
    /// Reading, writing or finalizing the underlying byte stream failed.
    Io(Arc<io::Error>),
    /// The HTTP/2 peer or protocol rejected the operation.
    Http2(Arc<h2::Error>),
    /// The HTTP/3 protocol or its transport failed.
    Http3(Arc<crate::h3::Error>),
    /// Required transport finalization failed after an earlier protocol failure.
    Finalization {
        /// The complete failure that caused finalization to begin.
        original: Arc<Error<E>>,
        /// The native finalization failure.
        cause: Arc<io::Error>,
    },
    /// An earlier HTTP failure caused this transition.
    Http(Arc<Error<E>>),
    /// The application-owned outgoing body failed.
    Body(Arc<E>),
    /// The protocol encountered an explicit state or lifecycle failure.
    State(StateFailure),
    /// A body ended before its declared content length.
    IncompleteBody(IncompleteBody),
    /// A native parser failure before protocol classification.
    Parse(Arc<ParserError>),
}

/// Native failures encountered while decoding an HTTP message head.
#[derive(Debug, Clone)]
pub enum ParserError {
    Http(httparse::Error),
    Method(Arc<http::method::InvalidMethod>),
    Status(Arc<http::status::InvalidStatusCode>),
    Uri(rama_net::uri::ParseError),
}

impl ParserError {
    fn classification(&self) -> Parse {
        match self {
            Self::Http(
                httparse::Error::HeaderName
                | httparse::Error::HeaderValue
                | httparse::Error::NewLine
                | httparse::Error::Token,
            ) => Parse::Header(Header::Token),
            Self::Http(httparse::Error::Status) | Self::Status(_) => Parse::Status,
            Self::Http(httparse::Error::TooManyHeaders) => Parse::TooLarge,
            Self::Http(httparse::Error::Version) => Parse::Version,
            Self::Method(_) => Parse::Method,
            Self::Uri(_) => Parse::Uri,
        }
    }
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(cause) => cause.fmt(f),
            Self::Method(cause) => cause.fmt(f),
            Self::Status(cause) => cause.fmt(f),
            Self::Uri(cause) => cause.fmt(f),
        }
    }
}

impl StdError for ParserError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(match self {
            Self::Http(cause) => cause,
            Self::Method(cause) => cause.as_ref(),
            Self::Status(cause) => cause.as_ref(),
            Self::Uri(cause) => cause,
        })
    }
}

/// A protocol state failure without an erased presentation-string cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateFailure {
    ConnectionClosed,
    ConnectionNotReady,
    PeerSettingsUnavailable,
    DispatchDropped { panicking: bool },
    ClientConnectionConsumed,
    ClientUpgradeConsumed,
    ServerConnectionConsumed,
    ServerUpgradeConsumed,
    ClientPingUnavailable,
    ServerPingUnavailable,
    ResponseAlreadyCompleted,
    SendCapacityClosed,
    KeepAliveTimeout,
    Timeout,
}

/// The remaining declared body length when an encoder observes early EOF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncompleteBody {
    pub(crate) remaining: u64,
}

impl IncompleteBody {
    /// Bytes still required by the declared content length.
    pub fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl fmt::Display for IncompleteBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "early end, expected {} more bytes", self.remaining)
    }
}

impl StdError for IncompleteBody {}

impl fmt::Display for StateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ConnectionClosed => "connection closed",
            Self::ConnectionNotReady => "connection was not ready",
            Self::PeerSettingsUnavailable => "connection closed before peer SETTINGS",
            Self::DispatchDropped { panicking: true } => "dispatch dropped during unwinding",
            Self::DispatchDropped { panicking: false } => "runtime dropped the dispatch task",
            Self::ClientConnectionConsumed => "HTTP/1 client connection already consumed",
            Self::ClientUpgradeConsumed => "HTTP/1 client upgrade already consumed",
            Self::ServerConnectionConsumed => "HTTP/1 server connection already consumed",
            Self::ServerUpgradeConsumed => "HTTP/1 server upgrade already consumed",
            Self::ClientPingUnavailable => "HTTP/2 client ping channel unavailable",
            Self::ServerPingUnavailable => "HTTP/2 server ping channel unavailable",
            Self::SendCapacityClosed => "send stream capacity unexpectedly closed",
            Self::ResponseAlreadyCompleted => "response future polled after completion",
            Self::KeepAliveTimeout => "keep-alive timed out",
            Self::Timeout => "operation timed out",
        })
    }
}

impl StdError for StateFailure {}

impl<E> From<io::Error> for Cause<E> {
    fn from(cause: io::Error) -> Self {
        Self::Io(Arc::new(cause))
    }
}

impl<E> From<h2::Error> for Cause<E> {
    fn from(cause: h2::Error) -> Self {
        Self::Http2(Arc::new(cause))
    }
}

impl<E> From<crate::h3::Error> for Cause<E> {
    fn from(cause: crate::h3::Error) -> Self {
        Self::Http3(Arc::new(cause))
    }
}

impl<E> From<Error<E>> for Cause<E> {
    fn from(cause: Error<E>) -> Self {
        Self::Http(Arc::new(cause))
    }
}

impl<E> From<StateFailure> for Cause<E> {
    fn from(cause: StateFailure) -> Self {
        Self::State(cause)
    }
}

impl<E> From<IncompleteBody> for Cause<E> {
    fn from(cause: IncompleteBody) -> Self {
        Self::IncompleteBody(cause)
    }
}

impl<E> From<ParserError> for Cause<E> {
    fn from(cause: ParserError) -> Self {
        Self::Parse(Arc::new(cause))
    }
}

impl<E> Clone for Cause<E> {
    fn clone(&self) -> Self {
        match self {
            Self::Io(cause) => Self::Io(Arc::clone(cause)),
            Self::Http2(cause) => Self::Http2(Arc::clone(cause)),
            Self::Http3(cause) => Self::Http3(Arc::clone(cause)),
            Self::Http(cause) => Self::Http(Arc::clone(cause)),
            Self::Finalization { original, cause } => Self::Finalization {
                original: Arc::clone(original),
                cause: Arc::clone(cause),
            },
            Self::Body(cause) => Self::Body(Arc::clone(cause)),
            Self::State(cause) => Self::State(*cause),
            Self::IncompleteBody(cause) => Self::IncompleteBody(*cause),
            Self::Parse(cause) => Self::Parse(Arc::clone(cause)),
        }
    }
}

impl<E> Clone for Error<E> {
    fn clone(&self) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: self.inner.kind.clone(),
                cause: self.inner.cause.clone(),
                preceding: self.inner.preceding.clone(),
            }),
        }
    }
}

impl Error {
    /// Carry a protocol-only failure into a connection with an application body type.
    pub fn into_body_error<E>(self) -> Error<E> {
        Error {
            inner: Box::new(ErrorImpl {
                kind: self.inner.kind,
                preceding: self
                    .inner
                    .preceding
                    .into_iter()
                    .map(Self::into_body_error)
                    .collect(),
                cause: self.inner.cause.map(|cause| match cause {
                    Cause::Io(cause) => Cause::Io(cause),
                    Cause::Http2(cause) => Cause::Http2(cause),
                    Cause::Http3(cause) => Cause::Http3(cause),
                    Cause::Http(cause) => {
                        Cause::Http(Arc::new(cause.as_ref().clone().into_body_error()))
                    }
                    Cause::Body(cause) => match *cause {},
                    Cause::Finalization { original, cause } => Cause::Finalization {
                        original: Arc::new(original.as_ref().clone().into_body_error()),
                        cause,
                    },
                    Cause::State(cause) => Cause::State(cause),
                    Cause::IncompleteBody(cause) => Cause::IncompleteBody(cause),
                    Cause::Parse(cause) => Cause::Parse(cause),
                }),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Kind {
    Parse(Parse),
    User(User),
    /// A message reached EOF, but is not complete.
    IncompleteMessage,
    /// A connection received a message (or bytes) when not waiting for one.
    UnexpectedMessage,
    /// A pending item was dropped before ever being processed.
    Canceled,
    /// Indicates a channel (client or body sender) is closed.
    ChannelClosed,
    /// An `io::Error` that occurred while trying to read or write to a network stream.
    Io,
    /// User took too long to send headers.
    HeaderTimeout,
    /// Error while reading a body from connection.
    Body,
    /// Error while writing a body to connection.
    BodyWrite,
    /// Error calling `AsyncWrite::shutdown()`.
    Shutdown,

    /// A general error from h2.
    Http2,
    /// The h2 peer refused a request without processing it.
    Http2Refused,
}

#[derive(Debug, Clone)]
pub enum Parse {
    Method,
    Version,
    VersionH2,
    Uri,
    UriTooLong,
    Header(Header),
    TooLarge,
    Status,
    Internal,
    /// Preserves a native failure until the HTTP error constructor classifies it.
    Native(ParserError),
}

#[derive(Debug, Clone)]
pub enum Header {
    Token,
    ContentLengthInvalid,
    TransferEncodingInvalid,
    TransferEncodingUnexpected,
}

#[derive(Debug, Clone)]
pub enum User {
    /// Error calling the user's `Body::poll_data()`.
    Body,
    /// The user aborted writing of the outgoing body.
    BodyWriteAborted,
    /// User tried to send a certain header in an unexpected context.
    ///
    /// For example, sending both `content-length` and `transfer-encoding`.
    UnexpectedHeader,
    /// User tried to send a request target its method cannot carry, such as a CONNECT
    /// without a port.
    InvalidTarget,
    /// User tried to respond with a 1xx (not 101) response code.
    UnsupportedStatusCode,

    /// The dispatch task is gone.
    DispatchGone,
}

// Sentinel type to indicate the error was caused by a timeout.
#[derive(Debug)]
pub(super) struct TimedOut;

impl<E> Error<E> {
    /// Earlier failures observed before this terminal failure.
    pub fn preceding(&self) -> &[Self] {
        &self.inner.preceding
    }

    pub(super) fn preceded_by(mut self, earlier: Self) -> Self {
        self.inner.preceding.push(earlier);
        self
    }
    /// The concrete native or application cause, if the protocol observed one.
    pub fn cause_ref(&self) -> Option<&Cause<E>> {
        self.inner.cause.as_ref()
    }

    /// Returns true if this was an HTTP parse error.
    ///
    /// This can be caused by a malformed HTTP message, an invalid header,
    /// an invalid URI, an invalid HTTP version, or a message head that is
    /// too large. Use the more specific `is_parse_*` methods to determine
    /// the exact cause.
    #[must_use]
    #[inline(always)]
    pub fn is_parse(&self) -> bool {
        matches!(self.inner.kind, Kind::Parse(_))
    }

    /// Returns true if this was an HTTP parse error caused by a message that was too large.
    ///
    /// This is triggered when the message head (request line plus headers for
    /// HTTP/1, or header frame for HTTP/2) exceeds the configured
    /// `max_buf_size`. It also covers the case where the URI alone exceeds the internal
    /// maximum URI length.
    #[must_use]
    #[inline(always)]
    pub fn is_parse_too_large(&self) -> bool {
        matches!(
            self.inner.kind,
            Kind::Parse(Parse::TooLarge | Parse::UriTooLong)
        )
    }

    /// Returns true if this was an HTTP parse error caused by an invalid response status code or
    /// reason phrase.
    #[must_use]
    #[inline(always)]
    pub fn is_parse_status(&self) -> bool {
        matches!(self.inner.kind, Kind::Parse(Parse::Status))
    }

    /// Returns true if this error was caused by user code.
    ///
    /// For example, this can be returned when the user's `Service` returns
    /// an error, the user's `Body` stream yields an error, or the user
    /// sends an unexpected header combination (such as both
    /// `content-length` and `transfer-encoding`).
    #[must_use]
    #[inline(always)]
    pub fn is_user(&self) -> bool {
        matches!(self.inner.kind, Kind::User(_))
    }

    /// Returns true if this was an HTTP parse error caused by HTTP2 preface sent over an HTTP1
    /// connection.
    ///
    /// This can happen when a client sends an HTTP/2 connection preface to a
    /// server that is only expecting HTTP/1.x requests.
    pub fn is_parse_version_h2(&self) -> bool {
        matches!(self.inner.kind, Kind::Parse(Parse::VersionH2))
    }

    /// Returns true if this was about a `Request` that was canceled.
    ///
    /// This typically happens when a pending request is dropped before
    /// it can be dispatched to the connection, for example because the
    /// connection was not ready.
    #[must_use]
    #[inline(always)]
    pub fn is_canceled(&self) -> bool {
        matches!(self.inner.kind, Kind::Canceled)
    }

    /// Returns true if a sender's channel is closed.
    ///
    /// This can occur when the other side of a client or body channel
    /// has been dropped, indicating that the receiver is no longer
    /// interested in the data.
    #[must_use]
    #[inline(always)]
    pub fn is_closed(&self) -> bool {
        matches!(self.inner.kind, Kind::ChannelClosed)
    }

    /// Returns true if the HTTP/2 peer refused this request without processing it,
    /// which makes it safe to retry, even when not idempotent (RFC 9113 section 8.7).
    ///
    /// The peer either reset its stream with `REFUSED_STREAM`, or went away
    /// with a `GOAWAY` that does not cover its stream. Only a request's own
    /// error can be refused, never a connection's.
    #[must_use]
    #[inline(always)]
    pub fn is_refused(&self) -> bool {
        matches!(self.inner.kind, Kind::Http2Refused)
    }

    /// Returns true if the connection closed before a message could complete.
    ///
    /// This means that the supplied IO connection reported EOF (closed) while
    /// hyper's HTTP state indicates more of the message (either request or
    /// response) needed to be transmitted.
    ///
    /// Some cases this could happen (not exhaustive):
    ///
    /// - A request is written on a connection, and the next `read` reports
    ///   EOF (perhaps a server just closed an "idle" connection).
    /// - A message body is only partially receive before the connection
    ///   reports EOF.
    /// - A client writes a request to your server, and then closes the write
    ///   half while waiting for your response. If you need to support this,
    ///   consider enabling [`half_close`].
    ///
    /// [`half_close`]: crate::server::conn::http1::Builder::with_half_close()
    #[inline(always)]
    #[must_use]
    pub fn is_incomplete_message(&self) -> bool {
        matches!(self.inner.kind, Kind::IncompleteMessage)
    }

    /// Returns true if the body write was aborted.
    ///
    /// This occurs when the user's code explicitly aborts writing of the
    /// outgoing body before it completes, for example by dropping the
    /// body sender.
    #[inline(always)]
    #[must_use]
    pub fn is_body_write_aborted(&self) -> bool {
        matches!(self.inner.kind, Kind::User(User::BodyWriteAborted))
    }

    /// Returns true if the error was caused while calling `AsyncWrite::shutdown()`.
    ///
    /// This can happen when the connection is being gracefully shut down
    /// and the underlying IO reports an error during the shutdown sequence.
    #[inline(always)]
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        matches!(self.inner.kind, Kind::Shutdown)
    }

    /// Returns true if the error was caused by a timeout.
    ///
    /// For HTTP/1 and HTTP/2 servers, this includes the header read timeout (see
    /// [`http1::Builder::set_header_read_timeout`](crate::server::conn::http1::Builder::set_header_read_timeout)
    /// and [`http2::Builder::set_header_read_timeout`](crate::server::conn::http2::Builder::set_header_read_timeout)).
    /// It also covers any timeout set via a user-provided timer
    #[inline(always)]
    #[must_use]
    pub fn is_timeout(&self) -> bool {
        let mut current = self;
        for _ in 0..64 {
            if matches!(current.inner.kind, Kind::HeaderTimeout) {
                return true;
            }
            match current.cause_ref() {
                Some(Cause::State(StateFailure::Timeout | StateFailure::KeepAliveTimeout)) => {
                    return true;
                }
                Some(Cause::Io(cause)) => return cause.kind() == io::ErrorKind::TimedOut,
                Some(Cause::Http(cause)) => current = cause,
                Some(Cause::Finalization { original, cause }) => {
                    if cause.kind() == io::ErrorKind::TimedOut {
                        return true;
                    }
                    current = original;
                }
                _ => return false,
            }
        }
        false
    }

    #[inline(always)]
    pub(super) fn new(kind: Kind) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind,
                cause: None,
                preceding: Vec::new(),
            }),
        }
    }

    #[inline(always)]
    pub(super) fn with<C: Into<Cause<E>>>(mut self, cause: C) -> Self {
        self.inner.cause = Some(cause.into());
        self
    }

    #[inline(always)]
    /// Protocol classification; native details remain in [`Self::cause_ref`].
    pub fn kind(&self) -> &Kind {
        &self.inner.kind
    }

    pub(crate) fn find_source<T: StdError + 'static>(&self) -> Option<&T>
    where
        E: StdError + 'static,
    {
        rama_core::error::error_chain(self.source()?).find_map(|cause| cause.downcast_ref::<T>())
    }

    pub(super) fn with_finalization(self, cause: io::Error) -> Self {
        Self::new(Kind::Shutdown).with(Cause::Finalization {
            original: Arc::new(self),
            cause: Arc::new(cause),
        })
    }

    pub(super) fn h2_reason(&self) -> h2::Reason
    where
        E: StdError + 'static,
    {
        match self.cause_ref() {
            Some(Cause::Http2(cause)) => cause.reason().unwrap_or(h2::Reason::INTERNAL_ERROR),
            Some(Cause::Http(cause)) => cause.h2_reason(),
            Some(Cause::Finalization { original, .. }) => original.h2_reason(),
            Some(Cause::Body(_)) => self
                .find_source::<h2::Error>()
                .and_then(h2::Error::reason)
                .unwrap_or(h2::Reason::INTERNAL_ERROR),
            _ => h2::Reason::INTERNAL_ERROR,
        }
    }

    #[inline(always)]
    pub(super) fn new_canceled() -> Self {
        Self::new(Kind::Canceled)
    }

    #[inline(always)]
    pub(super) fn new_parse_internal() -> Self {
        Self::new(Kind::Parse(Parse::Internal))
    }

    #[inline(always)]
    pub(super) fn new_incomplete() -> Self {
        Self::new(Kind::IncompleteMessage)
    }

    #[inline(always)]
    pub(super) fn new_too_large() -> Self {
        Self::new(Kind::Parse(Parse::TooLarge))
    }

    #[inline(always)]
    pub(super) fn new_version_h2() -> Self {
        Self::new(Kind::Parse(Parse::VersionH2))
    }

    #[inline(always)]
    pub(super) fn new_unexpected_message() -> Self {
        Self::new(Kind::UnexpectedMessage)
    }

    #[inline(always)]
    pub(super) fn new_io(cause: std::io::Error) -> Self {
        Self::new(Kind::Io).with(cause)
    }

    #[inline(always)]
    pub(super) fn new_closed() -> Self {
        Self::new(Kind::ChannelClosed)
    }

    #[inline(always)]
    pub(super) fn new_body<C: Into<Cause<E>>>(cause: C) -> Self {
        Self::new(Kind::Body).with(cause)
    }

    #[inline(always)]
    pub(super) fn new_body_write<C: Into<Cause<E>>>(cause: C) -> Self {
        Self::new(Kind::BodyWrite).with(cause)
    }

    #[inline(always)]
    pub(super) fn new_body_write_aborted() -> Self {
        Self::new(Kind::User(User::BodyWriteAborted))
    }

    #[inline(always)]
    fn new_user(user: User) -> Self {
        Self::new(Kind::User(user))
    }

    #[inline(always)]
    pub(super) fn new_user_header() -> Self {
        Self::new_user(User::UnexpectedHeader)
    }

    #[inline(always)]
    pub(super) fn new_user_target() -> Self {
        Self::new_user(User::InvalidTarget)
    }

    #[inline(always)]
    pub(super) fn new_header_timeout() -> Self {
        Self::new(Kind::HeaderTimeout)
    }

    #[inline(always)]
    pub(super) fn new_user_unsupported_status_code() -> Self {
        Self::new_user(User::UnsupportedStatusCode)
    }

    #[inline(always)]
    pub(super) fn new_user_service(cause: Infallible) -> Self {
        match cause {}
    }

    #[inline(always)]
    pub(super) fn new_user_body(cause: E) -> Self {
        Self::new_user(User::Body).with(Cause::Body(Arc::new(cause)))
    }

    #[inline(always)]
    pub(super) fn new_shutdown(cause: std::io::Error) -> Self {
        Self::new(Kind::Shutdown).with(cause)
    }

    #[inline(always)]
    pub(super) fn new_user_dispatch_gone() -> Self {
        Self::new(Kind::User(User::DispatchGone))
    }

    #[inline(always)]
    pub(super) fn new_h2(cause: h2::Error) -> Self {
        match cause.try_into_io() {
            Ok(io_err) => Self::new_io(io_err),
            Err(cause) => Self::new(Kind::Http2).with(cause),
        }
    }

    /// Only for the error of one request: its own stream outcome, or it was never opened.
    pub(super) fn new_h2_request(cause: h2::Error) -> Self {
        // a remote GOAWAY only reaches a request when it does not cover that request
        if cause.is_remote()
            && (cause.is_go_away() || cause.reason() == Some(h2::Reason::REFUSED_STREAM))
        {
            Self::new(Kind::Http2Refused).with(cause)
        } else {
            Self::new_h2(cause)
        }
    }

    fn description(&self) -> &str {
        match self.inner.kind {
            Kind::Parse(Parse::Method) => "invalid HTTP method parsed",
            Kind::Parse(Parse::Version) => "invalid HTTP version parsed",
            Kind::Parse(Parse::VersionH2) => "invalid HTTP version parsed (found HTTP2 preface)",
            Kind::Parse(Parse::Uri) => "invalid URI",
            Kind::Parse(Parse::UriTooLong) => "URI too long",
            Kind::Parse(Parse::Header(Header::Token)) => "invalid HTTP header parsed",
            Kind::Parse(Parse::Header(Header::ContentLengthInvalid)) => {
                "invalid content-length parsed"
            }
            Kind::Parse(Parse::Header(Header::TransferEncodingInvalid)) => {
                "invalid transfer-encoding parsed"
            }
            Kind::Parse(Parse::Header(Header::TransferEncodingUnexpected)) => {
                "unexpected transfer-encoding parsed"
            }
            Kind::Parse(Parse::TooLarge) => "message head is too large",
            Kind::Parse(Parse::Status) => "invalid HTTP status-code parsed",
            Kind::Parse(Parse::Internal) => {
                "internal error inside rama_http_core and/or its dependencies, please report"
            }
            Kind::Parse(Parse::Native(_)) => "native HTTP parser failure",
            Kind::IncompleteMessage => "connection closed before message completed",
            Kind::UnexpectedMessage => "received unexpected message from connection",
            Kind::ChannelClosed => "channel closed",
            Kind::Canceled => "operation was canceled",
            Kind::HeaderTimeout => "read header from client timeout",
            Kind::Body => "error reading a body from connection",
            Kind::BodyWrite => "error writing a body to connection",
            Kind::Shutdown => "error shutting down connection",
            Kind::Http2 => "http2 error",
            Kind::Http2Refused => "http2 request refused by peer",
            Kind::Io => "connection error",
            Kind::User(User::Body) => "error from user's Body stream",
            Kind::User(User::BodyWriteAborted) => "user body write aborted",
            Kind::User(User::UnexpectedHeader) => "user sent unexpected header",
            Kind::User(User::InvalidTarget) => "user sent an invalid request target",
            Kind::User(User::UnsupportedStatusCode) => {
                "response has 1xx status code, not supported by server"
            }
            Kind::User(User::DispatchGone) => "dispatch task is gone",
        }
    }
}

impl<E: fmt::Debug> fmt::Debug for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_tuple("rama_http_core::Error");
        f.field(&self.inner.kind);
        if let Some(cause) = &self.inner.cause {
            f.field(cause);
        }
        if !self.inner.preceding.is_empty() {
            f.field(&self.inner.preceding);
        }
        f.finish()
    }
}

impl<E> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl<E: StdError + 'static> StdError for Error<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.cause_ref().map(|cause| match cause {
            Cause::Io(cause) => cause.as_ref() as &(dyn StdError + 'static),
            Cause::Http2(cause) => cause.as_ref(),
            Cause::Http3(cause) => cause.as_ref(),
            Cause::Http(cause) => cause.as_ref(),
            Cause::Body(cause) => cause.as_ref(),
            Cause::Finalization { original, .. } => original.as_ref(),
            Cause::State(cause) => cause,
            Cause::IncompleteBody(cause) => cause,
            Cause::Parse(cause) => cause.as_ref(),
        })
    }
}

#[doc(hidden)]
impl<E> From<Parse> for Error<E> {
    fn from(err: Parse) -> Self {
        match err {
            Parse::Native(cause) => Self::new(Kind::Parse(cause.classification())).with(cause),
            other => Self::new(Kind::Parse(other)),
        }
    }
}

impl Parse {
    pub(crate) fn content_length_invalid() -> Self {
        Self::Header(Header::ContentLengthInvalid)
    }

    pub(crate) fn transfer_encoding_invalid() -> Self {
        Self::Header(Header::TransferEncodingInvalid)
    }

    pub(crate) fn transfer_encoding_unexpected() -> Self {
        Self::Header(Header::TransferEncodingUnexpected)
    }
}

impl From<httparse::Error> for Parse {
    fn from(err: httparse::Error) -> Self {
        Self::Native(ParserError::Http(err))
    }
}

impl From<http::method::InvalidMethod> for Parse {
    fn from(cause: http::method::InvalidMethod) -> Self {
        Self::Native(ParserError::Method(Arc::new(cause)))
    }
}

impl From<http::status::InvalidStatusCode> for Parse {
    fn from(cause: http::status::InvalidStatusCode) -> Self {
        Self::Native(ParserError::Status(Arc::new(cause)))
    }
}

impl From<rama_net::uri::ParseError> for Parse {
    fn from(cause: rama_net::uri::ParseError) -> Self {
        Self::Native(ParserError::Uri(cause))
    }
}

// ===== impl TimedOut ====

impl fmt::Display for TimedOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("operation timed out")
    }
}

impl StdError for TimedOut {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn parser_failures_preserve_native_codes() -> std::result::Result<(), (httparse::Error, Error)>
    {
        for expected in [
            httparse::Error::HeaderName,
            httparse::Error::HeaderValue,
            httparse::Error::NewLine,
            httparse::Error::Token,
            httparse::Error::Status,
            httparse::Error::TooManyHeaders,
            httparse::Error::Version,
        ] {
            let received = Error::from(Parse::from(expected));
            if !matches!(received.cause_ref(), Some(Cause::Parse(cause))
                if matches!(cause.as_ref(), ParserError::Http(actual) if *actual == expected))
            {
                return Err((expected, received));
            }
        }
        Ok(())
    }

    #[test]
    fn parser_failure_retains_uri_offset_after_body_type_transition()
    -> std::result::Result<(), std::result::Result<rama_net::uri::Uri, Error<BodyFailure>>> {
        let native = match rama_net::uri::Uri::parse(b"/\xff".as_slice()) {
            Ok(uri) => return Err(Ok(uri)),
            Err(cause) => cause,
        };
        let error = Error::from(Parse::from(native.clone())).into_body_error::<BodyFailure>();
        let preserves = matches!(error.kind(), Kind::Parse(Parse::Uri))
            && matches!(error.cause_ref(), Some(Cause::Parse(cause))
                if matches!(cause.as_ref(), ParserError::Uri(actual) if *actual == native));
        preserves.then_some(()).ok_or(Err(error))
    }

    fn satisfies_send_sync<T: Send + Sync + 'static>() {}

    #[test]
    fn error_satisfies_send_sync() {
        satisfies_send_sync::<Error>();
        satisfies_send_sync::<Error<BodyFailure>>();
    }

    #[test]
    fn error_size_of() -> std::result::Result<(), (&'static str, usize, usize)> {
        let expected = mem::size_of::<usize>();
        let received = mem::size_of::<Error>();
        (received == expected).then_some(()).ok_or((
            "expected pointer size, received error handle size",
            expected,
            received,
        ))
    }

    #[derive(Debug)]
    struct Cycle;

    impl fmt::Display for Cycle {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("cycle")
        }
    }

    impl StdError for Cycle {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(self)
        }
    }

    #[test]
    fn source_inspection_terminates_for_cyclic_causes()
    -> std::result::Result<(), (&'static str, Error<Cycle>)> {
        let error = Error::new_user_body(Cycle);
        let valid = matches!(error.cause_ref(), Some(Cause::Body(_)))
            && !error.is_timeout()
            && error.h2_reason() == h2::Reason::INTERNAL_ERROR;
        valid.then_some(()).ok_or((
            "cyclic application source remains concrete and inspection terminates",
            error,
        ))
    }

    #[test]
    fn timeout_source_inspection_respects_depth_limit()
    -> std::result::Result<(), (&'static str, Error, Error)> {
        let mut error: Error = Error::new_canceled().with(StateFailure::Timeout);
        for _ in 0..63 {
            error = Error::new_canceled().with(error);
        }
        let nested = Error::new_canceled().with(error.clone());
        let valid = error.is_timeout()
            && !nested.is_timeout()
            && matches!(nested.cause_ref(), Some(Cause::Http(_)));
        valid.then_some(()).ok_or((
            "bounded classification preserves both complete cause chains",
            error,
            nested,
        ))
    }

    #[test]
    fn h2_reason_unknown() -> std::result::Result<(), (&'static str, Error)> {
        let error: Error = Error::new_closed();
        let valid = error.h2_reason() == h2::Reason::INTERNAL_ERROR && error.cause_ref().is_none();
        valid.then_some(()).ok_or((
            "closed channel must not fabricate an HTTP/2 peer reason",
            error,
        ))
    }

    #[test]
    fn h2_reason_one_level() -> std::result::Result<(), (&'static str, Error<h2::Error>)> {
        let error = Error::new_user_body(h2::Error::from(h2::Reason::ENHANCE_YOUR_CALM));
        let valid = matches!(error.cause_ref(), Some(Cause::Body(cause)) if cause.reason() == Some(h2::Reason::ENHANCE_YOUR_CALM))
            && error.h2_reason() == h2::Reason::ENHANCE_YOUR_CALM;
        valid
            .then_some(())
            .ok_or(("preserve the application's HTTP/2 cause and reason", error))
    }

    #[test]
    fn h2_reason_nested() -> std::result::Result<(), (&'static str, Error<Error>)> {
        let received: Error = Error::new_h2(h2::Error::from(h2::Reason::HTTP_1_1_REQUIRED));
        let error = Error::new_user_body(received);
        let valid = matches!(error.cause_ref(), Some(Cause::Body(cause)) if matches!(cause.cause_ref(), Some(Cause::Http2(_))))
            && error.h2_reason() == h2::Reason::HTTP_1_1_REQUIRED;
        valid
            .then_some(())
            .ok_or(("forwarded native cause retains its peer reason", error))
    }

    #[test]
    fn native_io_cause_is_available_without_downcasting()
    -> std::result::Result<(), (&'static str, Error)> {
        let error: Error = Error::new_io(io::Error::from(io::ErrorKind::ConnectionReset));
        let valid = matches!(error.cause_ref(), Some(Cause::Io(cause)) if cause.kind() == io::ErrorKind::ConnectionReset);
        valid
            .then_some(())
            .ok_or(("retain the native I/O value", error))
    }

    #[test]
    fn finalization_preserves_original_and_shutdown_failures()
    -> std::result::Result<(), (&'static str, Error)> {
        let original: Error = Error::new_io(io::Error::from(io::ErrorKind::ConnectionReset));
        let error = original.with_finalization(io::Error::from(io::ErrorKind::BrokenPipe));
        let valid = matches!(error.cause_ref(), Some(Cause::Finalization { original, cause })
            if cause.kind() == io::ErrorKind::BrokenPipe
                && matches!(original.cause_ref(), Some(Cause::Io(cause)) if cause.kind() == io::ErrorKind::ConnectionReset));
        valid.then_some(()).ok_or((
            "shutdown failure cannot erase the original connection failure",
            error,
        ))
    }

    #[derive(Debug)]
    struct BodyFailure {
        received: Vec<u8>,
    }

    impl fmt::Display for BodyFailure {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "body failed after {} bytes", self.received.len())
        }
    }

    impl StdError for BodyFailure {}

    #[test]
    fn application_body_payload_survives_shared_reporting()
    -> std::result::Result<(), (&'static str, Error<BodyFailure>, Error<BodyFailure>)> {
        let error = Error::new_user_body(BodyFailure {
            received: b"prefix".to_vec(),
        });
        let shared = error.clone();
        let valid = matches!((error.cause_ref(), shared.cause_ref()), (Some(Cause::Body(first)), Some(Cause::Body(second)))
            if Arc::ptr_eq(first, second) && first.received == b"prefix");
        valid.then_some(()).ok_or((
            "independent observers retain the same concrete body failure",
            error,
            shared,
        ))
    }
}
