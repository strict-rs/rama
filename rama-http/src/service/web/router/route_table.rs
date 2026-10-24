use super::{RouterError, route_index::RouteIndex};
use crate::matcher::{PathMatchPolicy, UriParams, path::compile_pattern_with_policy};
use rama_core::service::{BoxService, Service};
use rama_http_types::{Method, Request};
use rama_utils::collections::NonEmptySmallVec;

#[cfg(test)]
mod tests;

/// A request and its path captures, without an extension-map round trip.
#[derive(Debug)]
pub struct MatchedRequest<B> {
    /// The original request, including its concrete body.
    pub request: Request<B>,
    /// Captures in the spelling selected by the route's path policy.
    pub params: UriParams,
}

type Methods<B, O, E> = Vec<(Method, BoxService<MatchedRequest<B>, O, E>)>;

/// Full-path, method-aware routing with concrete body, output and failure types.
///
/// Endpoint state stays with its service. Captures accompany the request as a
/// concrete field, allowing authorization before fallible path decoding. This
/// uses the same specificity index as [`super::Router`]. It does not add body
/// conversion, extractors, implicit HEAD execution, or response projection.
#[must_use]
pub struct RouteTable<B, O, E> {
    routes: RouteIndex<Methods<B, O, E>>,
    policy: PathMatchPolicy,
}

impl<B, O, E> Clone for RouteTable<B, O, E> {
    fn clone(&self) -> Self {
        Self {
            routes: self.routes.clone(),
            policy: self.policy,
        }
    }
}

impl<B, O, E> std::fmt::Debug for RouteTable<B, O, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteTable")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl<B, O, E> RouteTable<B, O, E> {
    /// Select path comparison and capture decoding before registration.
    pub fn new(policy: PathMatchPolicy) -> Self {
        Self {
            routes: RouteIndex::default(),
            policy,
        }
    }

    /// Register a service. The first registered service for a method wins.
    pub fn with_route<S>(mut self, path: &str, method: Method, service: S) -> Self
    where
        S: Service<MatchedRequest<B>, Output = O, Error = E>,
    {
        let pattern = compile_pattern_with_policy(path, self.policy);
        self.routes
            .with_entry(pattern, |methods| methods.push((method, service.boxed())));
        self
    }

    /// Combine registered routes, preserving each compiled path's policy.
    /// New registrations continue to use this table's policy. For identical
    /// patterns and methods, registrations in this table retain precedence.
    pub fn merge(mut self, other: Self) -> Self {
        for (pattern, mut handlers) in other.routes.into_entries() {
            self.routes
                .with_entry(pattern, |methods| methods.append(&mut handlers));
        }
        self
    }
}

impl<B, O, E> Service<Request<B>> for RouteTable<B, O, E>
where
    B: Send + 'static,
    O: Send + 'static,
    E: Send + From<RouterError> + 'static,
{
    type Output = O;
    type Error = E;

    async fn serve(&self, request: Request<B>) -> Result<O, E> {
        let (methods, params) = self
            .routes
            .find(request.uri().path_ref_or_root())
            .ok_or_else(|| E::from(RouterError::NotFound))?;
        if let Some((_, handler)) = methods
            .iter()
            .find(|(method, _)| method == request.method())
        {
            return handler.serve(MatchedRequest { request, params }).await;
        }
        let allowed = NonEmptySmallVec::collect(methods.iter().map(|(method, _)| method.clone()));
        match allowed {
            Some(allowed) => Err(RouterError::MethodNotAllowed(Box::new(allowed)).into()),
            None => Err(RouterError::NotFound.into()),
        }
    }
}
