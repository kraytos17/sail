// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A session-multiplexing front end for the Spark Connect service.
//!
//! Every client keeps sending its own `session_id`, and every response echoes
//! that same client-supplied id back, keeping clients fully protocol-compliant.
//! On the backend side, all requests are stamped with ONE canonical session
//! id, so the [`SessionManager`] materializes a single driver + worker fleet
//! that every multiplexed client shares.
//!
//! The canonical session is never created explicitly: it comes into existence
//! lazily through the ordinary `get_or_create_session_context(canonical, ...)`
//! path on the first stamped request, and self-heals the same way after a
//! server restart or idle eviction.
//!
//! Delegation happens in-process: requests are decoded once at the edge, the
//! session id field is swapped, and the already-decoded message is handed to
//! the wrapped [`SparkConnectServer`]. No extra network hop, no re-encoding.
//!
//! Per-RPC reverse mapping is stateless: the client id captured before the
//! outbound swap is restored on each inbound response/stream item. The
//! [`ClientRegistry`] only tracks last-seen timestamps for observability and
//! `release_session` bookkeeping; it never routes anything.
//!
//! `server_side_session_id` is deliberately left untouched (= the canonical
//! id): it truthfully identifies the shared backend session and its stability
//! satisfies the client-side idempotency checks.
//!
//! Note: `add_artifacts` cannot delegate through the [`SparkConnectService`]
//! trait because tonic provides no public way to rebuild a `Streaming<T>`
//! request stream. Its handler body is therefore replicated here against the
//! crate-internal `service::handle_add_artifacts` (keep in sync with
//! `server.rs`).

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};
use log::{debug, warn};
use sail_session::session_manager::SessionManager;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use crate::error::SparkError;
use crate::server::SparkConnectServer;
use crate::service;
use crate::service::ExecutePlanResponseStream;
use crate::spark::connect::spark_connect_service_server::SparkConnectService;
use crate::spark::connect::{
    AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse,
    ArtifactStatusesRequest, ArtifactStatusesResponse, CloneSessionRequest, CloneSessionResponse,
    ConfigRequest, ConfigResponse, ExecutePlanRequest, ExecutePlanResponse,
    FetchErrorDetailsRequest, FetchErrorDetailsResponse, GetStatusRequest, GetStatusResponse,
    InterruptRequest, InterruptResponse, ReattachExecuteRequest, ReleaseExecuteRequest,
    ReleaseExecuteResponse, ReleaseSessionRequest, ReleaseSessionResponse,
};

/// How long a client id may stay unseen in the registry before being evicted.
/// This only bounds registry memory; backend session lifecycle is governed by
/// the session manager's own timeout.
const CLIENT_REGISTRY_TTL: Duration = Duration::from_secs(3600);

/// Hard cap on tracked client ids. Crossing it first sweeps stale entries;
/// if everything is still fresh (an id flood), the least-recently-seen entry
/// is evicted so the registry can never exceed this size.
const CLIENT_REGISTRY_MAX_ENTRIES: usize = 1024;

/// How long to wait after the last live client releases its session before
/// tearing down the canonical session. The delay lets a quickly reconnecting
/// client cancel the teardown.
const CANONICAL_SESSION_RELEASE_GRACE: Duration = Duration::from_secs(30);

/// How recently the canonical session must have been used for the teardown to
/// hold off. The session manager's per-session `ActivityTracker` is updated by
/// every protocol that touches the session (Spark Connect and Flight SQL), so
/// this also defers to in-flight work from protocols other than the mux.
const CANONICAL_SESSION_ACTIVITY_HOLD_WINDOW: Duration = Duration::from_secs(60);

#[derive(Debug, Default)]
struct ClientRegistry {
    entries: HashMap<String, Instant>,
    /// Client ids that explicitly released their session. A released client no
    /// longer counts as live; if it reconnects, `touch` clears the mark.
    released: HashSet<String>,
}

impl ClientRegistry {
    fn touch(&mut self, client_id: &str) {
        if self.entries.len() >= CLIENT_REGISTRY_MAX_ENTRIES
            && !self.entries.contains_key(client_id)
        {
            self.sweep(Instant::now());
            if self.entries.len() >= CLIENT_REGISTRY_MAX_ENTRIES
                && let Some((oldest, _)) = self
                    .entries
                    .iter()
                    .min_by_key(|(_, last_seen)| **last_seen)
                    .map(|(id, seen)| (id.clone(), *seen))
            {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(client_id.to_string(), Instant::now());
        self.released.remove(client_id);
    }

    fn sweep(&mut self, now: Instant) {
        self.entries
            .retain(|_, last_seen| now.duration_since(*last_seen) < CLIENT_REGISTRY_TTL);
    }

    /// Marks a client as released: its entry no longer counts as live.
    fn release_client(&mut self, client_id: &str) -> bool {
        let present = self.entries.remove(client_id).is_some();
        self.released.insert(client_id.to_string());
        present
    }

    /// True when at least one tracked client has not explicitly released its
    /// session. Entries whose connection dropped without a release still count
    /// as live (conservative: never tear the canonical session out from under
    /// a client the registry cannot prove is gone).
    fn has_live_clients(&self) -> bool {
        self.entries
            .keys()
            .any(|client_id| !self.released.contains(client_id))
    }
}

type RewrittenExecutePlanStream =
    Pin<Box<dyn Stream<Item = Result<ExecutePlanResponse, Status>> + Send>>;

fn rewrite_execute_stream(
    upstream: ExecutePlanResponseStream,
    client_id: String,
) -> RewrittenExecutePlanStream {
    Box::pin(async_stream::try_stream! {
        let mut upstream = Box::pin(upstream);
        while let Some(item) = upstream.next().await {
            let mut item = item?;
            item.session_id = client_id.clone();
            yield item;
        }
    })
}

/// Registers the original id in the registry and swaps the session id field
/// in place to the canonical id, returning the original client id for the
/// reverse direction.
fn stamp_session_id(registry: &mut ClientRegistry, canonical: &str, field: &mut String) -> String {
    let client_id = std::mem::take(field);
    registry.touch(&client_id);
    *field = canonical.to_string();
    client_id
}

/// Resolves the effective canonical session id, falling back to a fresh UUID.
/// Public so embedders (e.g. the combined server) can resolve the id once and
/// hand the SAME value to both the multiplexer and other protocols.
pub fn resolve_canonical_session_id(canonical_session_id: Option<String>) -> String {
    canonical_session_id
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

#[derive(Debug)]
pub struct MultiplexedSparkConnectServer {
    inner: SparkConnectServer,
    session_manager: SessionManager,
    canonical_session_id: String,
    registry: Arc<Mutex<ClientRegistry>>,
}

impl MultiplexedSparkConnectServer {
    /// Creates a multiplexing front end over the given session manager.
    ///
    /// When `canonical_session_id` is `None` (or blank), a fresh UUID is
    /// generated for the lifetime of this process.
    pub fn new(session_manager: SessionManager, canonical_session_id: Option<String>) -> Self {
        let canonical_session_id = resolve_canonical_session_id(canonical_session_id);
        Self {
            inner: SparkConnectServer::new(session_manager.clone()),
            session_manager,
            canonical_session_id,
            registry: Arc::new(Mutex::new(ClientRegistry::default())),
        }
    }

    pub fn canonical_session_id(&self) -> &str {
        &self.canonical_session_id
    }

    /// Registers the client id, validates the client's observed server-side
    /// session against the canonical one, and swaps the field in place to the
    /// canonical id, returning the original client id for the reverse direction.
    ///
    /// A client that observed a server session different from the canonical one
    /// is stale (e.g. it reconnected after the server restarted and minted a new
    /// canonical session). Forwarding such a request would route it to a session
    /// where its operations do not exist, so it is rejected with a clear error
    /// that lets the client recreate its session.
    fn stamp(
        &self,
        session_id_field: &mut String,
        observed_server_side_session_id: Option<&str>,
    ) -> Result<String, Status> {
        validate_observed_server_side_session_id(
            &self.canonical_session_id,
            observed_server_side_session_id,
        )?;
        match self.registry.lock() {
            // Bookkeeping is best-effort: a poisoned lock must never fail requests.
            Ok(mut registry) => Ok(stamp_session_id(
                &mut registry,
                &self.canonical_session_id,
                session_id_field,
            )),
            Err(_) => Ok(std::mem::replace(
                session_id_field,
                self.canonical_session_id.clone(),
            )),
        }
    }
}

/// Rejects clients whose observed server session diverges from the canonical
/// one. Blank or absent observations (clients that never learned a server-side
/// session) are always accepted.
fn validate_observed_server_side_session_id(
    canonical_session_id: &str,
    observed_server_side_session_id: Option<&str>,
) -> Result<(), Status> {
    if let Some(observed) = observed_server_side_session_id {
        if !observed.is_empty() && observed != canonical_session_id {
            return Err(Status::failed_precondition(
                "session no longer valid; create a new session",
            ));
        }
    }
    Ok(())
}

#[tonic::async_trait]
impl SparkConnectService for MultiplexedSparkConnectServer {
    type ExecutePlanStream = RewrittenExecutePlanStream;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let mut request = request.into_inner();
        debug!("{request:?}");
        let client_id = self.stamp(
            &mut request.session_id,
            request.client_observed_server_side_session_id.as_deref(),
        )?;
        let response = self
            .inner
            .execute_plan(Request::new(request))
            .await?
            .into_inner();
        Ok(Response::new(rewrite_execute_stream(response, client_id)))
    }

    type ReattachExecuteStream = RewrittenExecutePlanStream;

    async fn reattach_execute(
        &self,
        request: Request<ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        let mut request = request.into_inner();
        debug!("{request:?}");
        let client_id = self.stamp(
            &mut request.session_id,
            request.client_observed_server_side_session_id.as_deref(),
        )?;
        let response = self
            .inner
            .reattach_execute(Request::new(request))
            .await?
            .into_inner();
        Ok(Response::new(rewrite_execute_stream(response, client_id)))
    }

    async fn analyze_plan(
        &self,
        request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        let mut request = request.into_inner();
        debug!("{request:?}");
        let client_id = self.stamp(
            &mut request.session_id,
            request.client_observed_server_side_session_id.as_deref(),
        )?;
        let mut response = self
            .inner
            .analyze_plan(Request::new(request))
            .await?
            .into_inner();
        response.session_id = client_id;
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let mut request = request.into_inner();
        debug!("{request:?}");
        // `config` is the session-(re)establishment path: a client whose
        // observed server session diverges from the canonical one is stale, but
        // rejecting the request would kill its session. Instead, proceed and let
        // the response's `server_side_session_id` re-sync the client.
        if let Some(observed) = request.client_observed_server_side_session_id.as_deref() {
            if !observed.is_empty() && observed != self.canonical_session_id {
                warn!(
                    "client observed server session {observed} differs from canonical {}; re-syncing",
                    self.canonical_session_id
                );
            }
        }
        let client_id = self.stamp(&mut request.session_id, None)?;
        let mut response = self.inner.config(Request::new(request)).await?.into_inner();
        response.session_id = client_id;
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn add_artifacts(
        &self,
        request: Request<Streaming<AddArtifactsRequest>>,
    ) -> Result<Response<AddArtifactsResponse>, Status> {
        let mut incoming = request.into_inner();
        let first = match incoming.next().await {
            Some(item) => item?,
            None => {
                return Err(Status::invalid_argument(
                    "at least one artifact request is required",
                ));
            }
        };
        debug!("{first:?}");
        let mut first = first;
        let client_id = self.stamp(
            &mut first.session_id,
            first.client_observed_server_side_session_id.as_deref(),
        )?;
        let user_id = first.user_context.map(|u| u.user_id).unwrap_or_default();
        let ctx = self
            .session_manager
            .get_or_create_session_context(self.canonical_session_id.clone(), user_id)
            .await
            .map_err(SparkError::from)?;
        let payload = first.payload;
        let canonical_session_id = self.canonical_session_id.clone();
        // Subsequent messages must carry the SAME client-visible id as the
        // first one (parity with the consistency check in `server.rs`).
        let stream_client_id = client_id.clone();
        let stream = async_stream::try_stream! {
            if let Some(payload) = payload {
                yield payload;
            }
            while let Some(item) = incoming.next().await {
                let mut item = item?;
                debug!("{item:?}");
                if item.session_id != stream_client_id {
                    Err(Status::invalid_argument("session ID must be consistent"))?;
                }
                item.session_id = canonical_session_id.clone();
                if let Some(payload) = item.payload {
                    yield payload;
                }
            }
        };
        let artifacts = service::handle_add_artifacts(&ctx, stream).await?;
        let response = AddArtifactsResponse {
            session_id: client_id,
            server_side_session_id: self.canonical_session_id.clone(),
            artifacts,
        };
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn artifact_status(
        &self,
        request: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        let mut request = request.into_inner();
        debug!("{request:?}");
        let client_id = self.stamp(
            &mut request.session_id,
            request.client_observed_server_side_session_id.as_deref(),
        )?;
        let mut response = self
            .inner
            .artifact_status(Request::new(request))
            .await?
            .into_inner();
        response.session_id = client_id;
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn interrupt(
        &self,
        request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>, Status> {
        let mut request = request.into_inner();
        debug!("{request:?}");
        let client_id = self.stamp(
            &mut request.session_id,
            request.client_observed_server_side_session_id.as_deref(),
        )?;
        let mut response = self
            .inner
            .interrupt(Request::new(request))
            .await?
            .into_inner();
        response.session_id = client_id;
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn release_execute(
        &self,
        request: Request<ReleaseExecuteRequest>,
    ) -> Result<Response<ReleaseExecuteResponse>, Status> {
        let mut request = request.into_inner();
        debug!("{request:?}");
        let client_id = self.stamp(
            &mut request.session_id,
            request.client_observed_server_side_session_id.as_deref(),
        )?;
        let mut response = self
            .inner
            .release_execute(Request::new(request))
            .await?
            .into_inner();
        response.session_id = client_id;
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    /// Intercepted: releasing one client's logical session must not destroy
    /// the canonical session other clients are still using. The registry
    /// entry goes away; the backend is never called. When the last live
    /// client releases, the canonical session is torn down after a grace
    /// delay so a quickly reconnecting client cancels the teardown.
    async fn release_session(
        &self,
        request: Request<ReleaseSessionRequest>,
    ) -> Result<Response<ReleaseSessionResponse>, Status> {
        let mut request = request.into_inner();
        debug!("{request:?}");
        if request.allow_reconnect {
            Err(SparkError::unsupported("reconnect session"))?;
        }
        // `ReleaseSessionRequest` does not carry `client_observed_server_side_session_id`.
        let client_id = self.stamp(&mut request.session_id, None)?;
        let mut tear_down = false;
        if let Ok(mut registry) = self.registry.lock() {
            registry.release_client(&client_id);
            tear_down = !registry.has_live_clients();
        }
        if tear_down {
            let session_manager = self.session_manager.clone();
            let canonical_session_id = self.canonical_session_id.clone();
            let registry = self.registry.clone();
            tokio::spawn(async move {
                tokio::time::sleep(CANONICAL_SESSION_RELEASE_GRACE).await;
                // Re-check under the lock: a client that reconnected during
                // the grace period cancels the teardown.
                let no_live_clients = match registry.lock() {
                    Ok(guard) => !guard.has_live_clients(),
                    // Bookkeeping is best-effort; never tear down on a poisoned lock.
                    Err(_) => false,
                };
                if !no_live_clients {
                    return;
                }
                // Hold off while ANY protocol has used the shared canonical
                // session recently. The session manager's ActivityTracker is
                // the authority on last activity across all protocols (e.g. a
                // Flight SQL query sharing the fleet).
                match session_manager
                    .session_idle_duration(canonical_session_id.clone())
                    .await
                {
                    // Session gone: nothing to tear down; skip the noisy
                    // "session not found" from delete_session.
                    Ok(None) => return,
                    // Activity within the hold window: still in use.
                    Ok(Some(idle)) if idle < CANONICAL_SESSION_ACTIVITY_HOLD_WINDOW => return,
                    Ok(Some(_)) => {}
                    // Can't determine; never tear down on uncertainty.
                    Err(_) => return,
                }
                if let Err(e) = session_manager.delete_session(canonical_session_id).await {
                    warn!("failed to tear down canonical session: {e}");
                }
            });
        }
        let response = ReleaseSessionResponse {
            session_id: client_id,
            server_side_session_id: self.canonical_session_id.clone(),
        };
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn fetch_error_details(
        &self,
        request: Request<FetchErrorDetailsRequest>,
    ) -> Result<Response<FetchErrorDetailsResponse>, Status> {
        let mut request = request.into_inner();
        debug!("{request:?}");
        let client_id = self.stamp(
            &mut request.session_id,
            request.client_observed_server_side_session_id.as_deref(),
        )?;
        let mut response = self
            .inner
            .fetch_error_details(Request::new(request))
            .await?
            .into_inner();
        response.session_id = client_id;
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn clone_session(
        &self,
        request: Request<CloneSessionRequest>,
    ) -> Result<Response<CloneSessionResponse>, Status> {
        let mut request = request.into_inner();
        debug!("{request:?}");
        let client_id = self.stamp(
            &mut request.session_id,
            request.client_observed_server_side_session_id.as_deref(),
        )?;
        let mut response = self
            .inner
            .clone_session(Request::new(request))
            .await?
            .into_inner();
        response.session_id = client_id;
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn get_status(
        &self,
        request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let mut request = request.into_inner();
        debug!("{request:?}");
        let client_id = self.stamp(
            &mut request.session_id,
            request.client_observed_server_side_session_id.as_deref(),
        )?;
        let mut response = self
            .inner
            .get_status(Request::new(request))
            .await?
            .into_inner();
        response.session_id = client_id;
        debug!("{response:?}");
        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_touch_and_release() {
        let mut registry = ClientRegistry::default();
        assert!(!registry.release_client("a"));
        registry.touch("a");
        assert!(registry.entries.contains_key("a"));
        assert!(registry.release_client("a"));
        assert!(!registry.entries.contains_key("a"));
        assert!(registry.released.contains("a"));
    }

    #[test]
    fn registry_sweep_evicts_only_stale_entries() {
        let now = Instant::now();
        let stale = now - CLIENT_REGISTRY_TTL - Duration::from_secs(1);
        let fresh = now;
        let mut registry = ClientRegistry::default();
        registry.entries.insert("stale".to_string(), stale);
        registry.entries.insert("fresh".to_string(), fresh);
        registry.sweep(now);
        assert!(!registry.entries.contains_key("stale"));
        assert!(registry.entries.contains_key("fresh"));
    }

    #[test]
    fn registry_cap_bounds_memory_under_fresh_id_flood() {
        let mut registry = ClientRegistry::default();
        for i in 0..CLIENT_REGISTRY_MAX_ENTRIES + 10 {
            registry.touch(&format!("client-{i}"));
        }
        assert!(registry.entries.len() <= CLIENT_REGISTRY_MAX_ENTRIES);
        // The oldest entry was evicted; the most recent one survived.
        assert!(!registry.entries.contains_key("client-0"));
        assert!(
            registry
                .entries
                .contains_key(&format!("client-{}", CLIENT_REGISTRY_MAX_ENTRIES + 9))
        );
    }

    #[test]
    fn registry_retouch_at_capacity_does_not_evict() {
        let mut registry = ClientRegistry::default();
        for i in 0..CLIENT_REGISTRY_MAX_ENTRIES {
            registry.touch(&format!("client-{i}"));
        }
        assert_eq!(registry.entries.len(), CLIENT_REGISTRY_MAX_ENTRIES);
        registry.touch("client-5");
        assert_eq!(registry.entries.len(), CLIENT_REGISTRY_MAX_ENTRIES);
        assert!(registry.entries.contains_key("client-5"));
    }

    #[test]
    fn stamp_swaps_field_and_returns_original() {
        let mut registry = ClientRegistry::default();
        let mut field = "client-uuid".to_string();
        let client_id = stamp_session_id(&mut registry, "canonical", &mut field);
        assert_eq!(client_id, "client-uuid");
        assert_eq!(field, "canonical");
        assert!(registry.entries.contains_key("client-uuid"));
    }

    #[test]
    fn resolve_canonical_generates_uuid_when_absent() {
        let id = resolve_canonical_session_id(None);
        assert!(Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn resolve_canonical_rejects_blank_input() {
        for blank in [Some(String::new()), Some("   ".to_string())] {
            let id = resolve_canonical_session_id(blank);
            assert!(Uuid::parse_str(&id).is_ok());
        }
    }

    #[test]
    fn resolve_canonical_honors_explicit_value() {
        let explicit = "11111111-2222-4333-8444-555555555555".to_string();
        let id = resolve_canonical_session_id(Some(explicit.clone()));
        assert_eq!(id, explicit);
    }

    #[test]
    fn observed_session_matching_canonical_is_accepted() {
        let result = validate_observed_server_side_session_id("canonical", Some("canonical"));
        assert!(result.is_ok());
    }

    #[test]
    fn absent_or_blank_observed_session_is_accepted() {
        assert!(validate_observed_server_side_session_id("canonical", None).is_ok());
        assert!(validate_observed_server_side_session_id("canonical", Some("")).is_ok());
    }

    #[test]
    fn divergent_observed_session_is_rejected() {
        let err = validate_observed_server_side_session_id("canonical", Some("stale")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("create a new session"));
    }

    #[test]
    fn release_client_marks_released_and_removes_entry() {
        let mut registry = ClientRegistry::default();
        registry.touch("a");
        assert!(registry.has_live_clients());
        assert!(registry.release_client("a"));
        assert!(!registry.has_live_clients());
        // Releasing an unknown id marks it released but reports no entry.
        assert!(!registry.release_client("b"));
        assert!(!registry.has_live_clients());
    }

    #[test]
    fn released_client_reconnecting_becomes_live_again() {
        let mut registry = ClientRegistry::default();
        registry.touch("a");
        registry.release_client("a");
        assert!(!registry.has_live_clients());
        registry.touch("a");
        assert!(registry.has_live_clients());
        assert!(!registry.released.contains("a"));
    }

    #[test]
    fn live_clients_are_not_detected_after_release() {
        let mut registry = ClientRegistry::default();
        registry.touch("a");
        registry.touch("b");
        registry.release_client("a");
        // "b" is still live.
        assert!(registry.has_live_clients());
        registry.release_client("b");
        assert!(!registry.has_live_clients());
    }
}
