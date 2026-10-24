# Rama strict fork

Rama is a reusable networking framework. Keep changes expressed in its public service, transport, protocol and resource-lifecycle contracts.

## Repository scope and contribution context

- Follow [CONTRIBUTING.md](CONTRIBUTING.md), the relevant protocol specifications and the repository's existing command surfaces. Preserve upstream capabilities, feature separation and meaningful behavior tests.
- Keep documentation, examples, fixtures and commit or pull-request descriptions limited to information needed to understand, maintain or verify the repository change. Where contribution guidance is silent, use the minimum sufficient technical context.
- Describe requirements through library behavior and reproducible examples. Use synthetic identities and fixtures. Do not introduce consumer-specific deployment descriptions, unpublished plans, unrelated checkout paths or external workspace instructions.
- Repair a reusable mechanism in the crate that produces it. Follow affected workspace consumers through a contract change; a downstream wrapper cannot recover information already erased by its source.
- Establish current implementation facts from the checked-out source, manifests, features and tests. A target contract or historical assessment is not evidence that an implementation already satisfies it.

## Concrete service and failure contracts

- Retain `Service<Input>` with concrete associated `Output` and `Error` types. Preserve request/response, connection/session and provider/cause relationships through layers, routing, bodies, drivers and finalization.
- Carry known connection, request, peer-identity, trust, deadline and negotiated-protocol facts through concrete or generic fields and typed access. Do not erase known context in `Any`/`TypeId` maps or require downcasts to recover it.
- Preserve native causes with concrete nested enums, associated types or generic parameters. Do not replace a known failure with `BoxError`, `OpaqueError`, `Box<dyn Error>`, `anyhow`, a formatted string or a classification-only value. Correct the earliest erasing constructor.
- Distinguish allocation and dispatch from semantic erasure. `Box<ConcreteFailure>` can preserve a concrete cause, and `BoxService<Input, Output, Error>` retains its semantic types. Prefer static composition when the relevant alternatives are known.
- Preserve the failure stage, attempted targets, native issue, observed transmission progress, valid response prefix and required finalization outcomes where the operation can observe them. A cleanup failure must not erase a completed step or the original failure.
- Use phase-specific values where they prevent meaningful invalid local transitions, such as using an unvalidated configuration or sending through an unprepared session. A local type transition cannot establish a remote effect.
- Decode untrusted input at its boundary. Keep concrete internal outcomes until their owner deliberately produces a public representation. Derive readable messages afterward, and exclude sensitive material without discarding useful failure categories.

## Source ownership

The table assigns required behavior to its producing crates. Inspect the selected call path before concluding that its current implementation needs repair.

| Owner | Required behavior |
|---|---|
| `rama-core`, `rama-error`, `rama-net` | Typed context and service composition, concrete connection causes, stage and attempt facts, and owned task outputs. Reuse the existing executor and Tokio facilities. |
| `rama-dns`, `rama-tcp` | Concrete resolver, candidate, dial and listener outcomes. Preserve candidate ordering, resolver policy, Happy Eyeballs, caching, socket behavior and partial-startup cleanup. |
| `rama-tls`, `rama-tls-rustls`, `rama-crypto` | Typed trust, client identity and protocol configuration; concrete configuration, handshake and ALPN failures. Inspect the selected provider as well as the outer error. |
| `rama-http-types`, `rama-http-core`, `rama-http-backend` | Typed bodies and protocol/sender/connect failures, bounded collection preserving valid prefixes, owned connection drivers and complete terminal outcomes. |
| `rama-http` | Typed endpoint composition and deliberate public-response projection after the complete service outcome has reached its owner. |
| `rama-http-backend` pooling | Reuse pool-identity and response-body lease mechanisms. Preserve identity-mapper failures and reject reuse when security-relevant configuration changes. |
| `rama-ws` | Concrete framing, UTF-8, decompression and transport causes; bounded frames; distinct terminal, close and finalization outcomes. A transport failure must not become an indistinguishable clean end of stream. |

Inspect relevant constructors, helpers, generated code, protocol drivers, provider boundaries and shutdown paths. Reuse compatible generic interfaces; change producers that still erase required evidence. Preserve protocol behavior while converging affected consumers.

## Trust and connection reuse

- Resolve the allowed endpoint, route, trust policy, protocol and resource budgets before their respective effects. Resolution or discovery supplies candidates; it does not establish peer identity.
- Verify the required peer identity before sending application credentials. TLS client-key material needed during a handshake remains scoped to that session. A failed secure connection does not authorize a plaintext downgrade or credential substitution.
- Include the security-relevant connection dimensions in pool identity: endpoint, protocol/ALPN, trust-configuration revision, client identity, authentication scope and proxy route as applicable. Hostname equality alone does not establish safe reuse.
- Keep protocol-specific authentication, retransmission and session responsibilities at their actual owners. Do not introduce redundant wrappers merely to force an independent protocol implementation through a different transport.
- Distinguish connection establishment, request submission, protocol acknowledgement and remote application completion. An ambiguous timeout does not prove that a remote effect failed to occur or authorize replay of an application mutation.

## Task ownership, limits and finalization

- Give every accept loop, connection driver, body producer, receive loop and background task an owner that receives its result and coordinates cancellation, joining and required finalization. Logging a failure or dropping a join handle does not discharge that obligation.
- Name the stage bounded by each deadline: acquisition, queueing, connection, handshake, execution, stream inactivity or finalization. Cancellation retains its reason and observed progress; dropping a future does not prove that no bytes were sent.
- Preserve required cleanup after partial startup or failure. Return completed progress, the original failure, finalization failures and remaining uncertainty together. Make resource custody and disposal responsibility explicit at handoff.
- Bound attempts, tasks, headers, bodies, frames, decompression, queues, outstanding requests, retries, pool waits and shutdown. Return typed capacity failures before starting work that requires unavailable resources.
- Support independently bounded queues, pools and concurrency budgets so a slow peer or long-lived stream cannot exhaust capacity assigned to unrelated work or required finalization. Expose enforceable limits and saturation outcomes; callers choose the isolation domains.
- Distinguish cancellation of a requester's wait from cancellation of the underlying operation. Honor the operation's declared lifetime and completion contract without inventing application-level recovery or retry authority.

## Rust quality and verification

- Keep fork-owned Rust source, macros, generated Rust and tests panic-free. Do not introduce unsafe Rust, panic-based control flow, unchecked unwraps, assertion macros, test-only exemptions or lint suppressions to satisfy a gate. Use fallible test support and assert complete typed outcomes.
- Existing allowances or erased convenience APIs do not establish exceptions to the selected strict contracts. Repair the originating source or generator; an uninspected provider or generated expansion is not a safety certification.
- Use the [justfile](justfile) and [CI definition](.github/workflows/CI.yml) for actual commands. `just fmt` formats the workspace; `just clippy` checks workspace targets/features; `just test` runs workspace nextest and isolated crypto-backend suites; `just test-doc` covers doctests; `just test-no-default-features` covers that configuration; `just qa` composes broader local QA.
- Read recipe prerequisites and effects before invocation. Keep warnings fatal and preserve the relevant feature/provider and platform checks. A broad all-features build does not prove every isolated backend configuration or runtime behavior on another platform.
- Verify complete paths through construction, normal operation, partial progress, failure, cancellation and shutdown. Cover successful trusted requests and rejection of invalid trust, malformed input, incompatible context and exhausted budgets before forbidden effects.
- Verify native-cause preservation, valid response prefixes after terminal failures, completed work alongside failed finalization, observed task completion, security-sensitive pool separation and bounded behavior under slow or disconnected peers.
- Use isolated local peers, synthetic credentials and deterministic fixtures for ordinary tests. Report source inspection, compilation, executed assertions, command exits and platform/runtime coverage accurately.
