use super::*;
use rama_core::{extensions::ExtensionsRef as _, service::service_fn};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
enum Failure {
    Routing(RouterError),
    Refused(u32),
}
impl From<RouterError> for Failure {
    fn from(cause: RouterError) -> Self {
        Self::Routing(cause)
    }
}

#[derive(Debug)]
struct Observation {
    results: Vec<Result<MatchedRequest<u32>, Failure>>,
    calls: usize,
}

#[tokio::test]
async fn routes_concrete_requests_and_preserves_captures_and_failures() -> Result<(), Observation> {
    let calls = Arc::new(AtomicUsize::new(0));
    let capture = calls.clone();
    let dynamic = RouteTable::new(PathMatchPolicy::STRICT).with_route(
        "/things/{id}",
        Method::GET,
        service_fn(move |input: MatchedRequest<u32>| {
            capture.fetch_add(1, Ordering::SeqCst);
            async move {
                if *input.request.body() == 9 {
                    Err(Failure::Refused(9))
                } else {
                    Ok(input)
                }
            }
        }),
    );
    let static_route = RouteTable::new(PathMatchPolicy::STRICT).with_route(
        "/things/summary",
        Method::POST,
        service_fn(|input| async move { Ok::<_, Failure>(input) }),
    );
    let router = dynamic.merge(static_route);
    let mut results = Vec::new();
    for (path, value) in [
        ("/things/%FF", 7),
        ("/things/item", 9),
        ("/things/summary", 1),
        ("/Things/item", 3),
    ] {
        let mut request = Request::new(value);
        *request.uri_mut() = rama_net::uri::Uri::from_static(path);
        results.push(router.serve(request).await);
    }
    let observed = Observation {
        results,
        calls: calls.load(Ordering::SeqCst),
    };
    let valid = observed.calls == 2
        && matches!(observed.results.as_slice(),
        [Ok(input), Err(Failure::Refused(9)), Err(Failure::Routing(RouterError::MethodNotAllowed(methods))), Err(Failure::Routing(RouterError::NotFound))]
        if *input.request.body() == 7 && input.params.get("id") == Some("%FF")
            && input.request.extensions().get_ref::<UriParams>().is_none()
            && methods.iter().eq([&Method::POST]));
    valid.then_some(()).ok_or(observed)
}
