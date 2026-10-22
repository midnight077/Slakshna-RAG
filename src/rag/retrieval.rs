//! Asking peers for chunks, and checking everything they send
//! (`docs/merkle_tree.md`, sections 6.2 and 6.3).
//!
//! Two pipelines share the same checks:
//! - **two_round**: round 1 asks every peer for its centroids nearest to the
//!   query. The asker checks each against the peer's signed root, computes the
//!   similarity itself, and picks the best clusters over all peers. Round 2
//!   asks only the peers owning those clusters for their best chunks.
//! - **flat**: one round; every peer sends its best chunks overall.
//!
//! Every chunk's proof is checked against its cluster's subtree root, which in
//! turn was checked against the peer's signed root. With `rescore`, the asker
//! embeds the chunks itself and ranks them by its own scores, so a peer gains
//! nothing by inflating its numbers.

use super::merkle::{dot, verify_chunk, Hash, SignedRoot};
use super::protocol::{
    self, ProvenChunk, RagRequest, RagResponse, RequestBody, ResponseBody, StaleRoot,
};
use super::registry::FLAG_EQUIVOCATION;
use super::{Chunk, PeerLink, PeerResult, RagService};
use crate::config::SelectBy;
use crate::history::BoxError;
use iroh::EndpointId;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tracing::warn;

/// Values of `Chunk::verification`.
pub const LOCAL: &str = "local";
pub const VERIFIED: &str = "verified";
pub const VERIFIED_NEW_ROOT: &str = "verified_new_root";
pub const FAILED_PROOF: &str = "failed_proof";

/// Flags raised while querying a peer (kept in `PeerResult::flags` and the registry).
pub const FLAG_BAD_CENTROID_PROOF: &str = "bad_centroid_proof";
pub const FLAG_BAD_CHUNK_PROOF: &str = "bad_chunk_proof";
pub const FLAG_SCORE_MISMATCH: &str = "score_mismatch";
pub const FLAG_SCORE_OVERCLAIM: &str = "score_overclaim";

/// What the remote side of one query produced.
#[derive(Debug, Default)]
pub struct RemoteRetrieval {
    pub chunks: Vec<Chunk>,
    pub peers: Vec<PeerResult>,
    /// Round 1 plus cluster selection (two_round only).
    pub select_ms: u64,
    /// Round 2, or the single round of flat mode.
    pub fetch_ms: u64,
    pub rescore_ms: u64,
}

/// One cluster offered in round 1 whose proof checked out.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub peer: EndpointId,
    pub cluster_id: u32,
    /// `dot(query, centroid)`, computed by the asker.
    pub sim: f64,
    /// The peer's claim about its best chunk in this cluster.
    pub claimed_best: Option<f64>,
}

/// Rank round-1 candidates and pick the winners (section 6.2, step 2).
///
/// Keeps at most `max_clusters`, drops those scoring below `min_score`, but
/// always keeps the single best. Returns indices into `candidates`, best first.
pub fn select_clusters(
    candidates: &[Candidate],
    select_by: SelectBy,
    max_clusters: usize,
    min_score: f64,
) -> Vec<usize> {
    let key = |c: &Candidate| match select_by {
        SelectBy::Centroid => c.sim,
        SelectBy::BestChunk => c.claimed_best.unwrap_or(c.sim),
    };
    let mut order: Vec<usize> = (0..candidates.len()).collect();
    order.sort_by(|&a, &b| {
        let (ca, cb) = (&candidates[a], &candidates[b]);
        key(cb)
            .partial_cmp(&key(ca))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| ca.peer.as_bytes().cmp(cb.peer.as_bytes()))
            .then_with(|| ca.cluster_id.cmp(&cb.cluster_id))
    });
    order.truncate(max_clusters);
    order
        .into_iter()
        .enumerate()
        .filter(|(rank, i)| *rank == 0 || key(&candidates[*i]) >= min_score)
        .map(|(_, i)| i)
        .collect()
}

/// The result of checking one peer's chunks.
#[derive(Debug, Default)]
pub struct CheckedChunks {
    pub kept: Vec<Chunk>,
    pub rejected: usize,
    pub bad_proof: bool,
}

/// Check each chunk's proof against the verified subtree root of its cluster
/// (section 6.2, step 4).
///
/// A chunk from a cluster not in `subtree_roots` fails. At most `per_cluster`
/// chunks are taken from each cluster; extras are rejected. Chunks that fail
/// are dropped, or kept marked `failed_proof` if `drop_unverified` is false.
pub fn check_chunks(
    chunks: Vec<ProvenChunk>,
    subtree_roots: &HashMap<u32, Hash>,
    per_cluster: usize,
    new_root: bool,
    drop_unverified: bool,
    node_id: &str,
) -> CheckedChunks {
    let mut checked = CheckedChunks::default();
    let mut taken: HashMap<u32, usize> = HashMap::new();
    for chunk in chunks {
        let count = taken.entry(chunk.cluster_id).or_default();
        if *count >= per_cluster {
            checked.rejected += 1;
            continue;
        }
        *count += 1;

        let proven = subtree_roots.get(&chunk.cluster_id).is_some_and(|root| {
            verify_chunk(&chunk.chunk_id, &chunk.text, &chunk.chunk_proof, root)
        });
        let cluster_id = chunk.cluster_id;
        let mut out = chunk.into_chunk(node_id);
        out.cluster_id = Some(cluster_id);
        if proven {
            let label = if new_root {
                VERIFIED_NEW_ROOT
            } else {
                VERIFIED
            };
            out.verification = Some(label.to_string());
            checked.kept.push(out);
        } else {
            checked.rejected += 1;
            checked.bad_proof = true;
            if !drop_unverified {
                out.verification = Some(FAILED_PROOF.to_string());
                checked.kept.push(out);
            }
        }
    }
    checked
}

/// Replace each chunk's score with `dot(q, its embedding)` (section 6.2,
/// step 5). The peer's number is kept as `claimed_score`. Returns whether any
/// claimed score was off by more than `tolerance`.
pub fn apply_rescore(
    chunks: &mut [Chunk],
    vectors: &[Vec<f32>],
    q: &[f32],
    tolerance: f64,
) -> bool {
    let mut mismatch = false;
    for (chunk, vector) in chunks.iter_mut().zip(vectors) {
        let score = dot(q, vector);
        let claimed = chunk.score;
        chunk.claimed_score = Some(claimed);
        chunk.score = score;
        if (claimed - score).abs() > tolerance {
            chunk.flags.push(FLAG_SCORE_MISMATCH.to_string());
            mismatch = true;
        }
    }
    mismatch
}

/// Clusters whose claimed best chunk score beats the best recomputed score of
/// the verified chunks actually delivered from them by more than `tolerance`.
pub fn overclaimed(claims: &[(u32, f64)], chunks: &[Chunk], tolerance: f64) -> Vec<u32> {
    claims
        .iter()
        .filter(|(cluster, claimed)| {
            let best = chunks
                .iter()
                .filter(|c| {
                    c.cluster_id == Some(*cluster)
                        && c.verification.as_deref() != Some(FAILED_PROOF)
                })
                .map(|c| c.score)
                .fold(f64::NEG_INFINITY, f64::max);
            claimed - best > tolerance
        })
        .map(|(cluster, _)| *cluster)
        .collect()
}

/// A round-1 cluster that passed its proof.
struct Offer {
    cluster_id: u32,
    subtree_root: Hash,
    centroid: Vec<f32>,
    claimed_best: Option<f64>,
}

/// Everything learned about one asked peer during a query.
struct PeerState {
    peer: EndpointId,
    result: PeerResult,
    node_id: String,
    root_version: u64,
    /// The root the peer's answers were checked against was first seen during this query.
    new_root: bool,
    offers: Vec<Offer>,
    /// Clusters won in round 1, best first.
    selected: Vec<u32>,
    /// Round 2 (or the flat round) answered.
    answered: bool,
    chunks: Vec<Chunk>,
}

impl PeerState {
    fn new(peer: EndpointId) -> Self {
        PeerState {
            peer,
            result: PeerResult::new(peer, "ok"),
            node_id: peer.to_string(),
            root_version: 0,
            new_root: false,
            offers: Vec::new(),
            selected: Vec::new(),
            answered: false,
            chunks: Vec::new(),
        }
    }

    fn fail(&mut self, status: &str, error: Option<String>) {
        self.result.status = status.to_string();
        self.result.error = error;
        self.chunks.clear();
    }

    fn finish(mut self) -> (PeerResult, Vec<Chunk>) {
        self.result.chunks = self.chunks.len();
        self.result.selected_clusters = self.selected.clone();
        self.result.rtt_ms =
            self.result.round1_ms.unwrap_or(0) + self.result.round2_ms.unwrap_or(0);
        (self.result, self.chunks)
    }
}

/// How one request to a peer went.
enum Sent {
    Answer(RagResponse),
    /// A `chunks` request named an old root; this is the peer's current one.
    Stale(SignedRoot),
    Failed {
        status: &'static str,
        error: Option<String>,
    },
}

impl RagService {
    /// Raise `flag` on a peer, both in this query's result and in the registry.
    fn raise(&self, state: &mut PeerState, flag: &str) {
        if !state.result.flags.iter().any(|f| f == flag) {
            state.result.flags.push(flag.to_string());
        }
        self.registry().add_flag(state.peer, flag);
    }

    /// The peers to ask, and results for the ones skipped: down for a while
    /// (`skipped_stale`), or caught equivocating (`refused`).
    fn split_targets(&self, link: &PeerLink) -> (Vec<EndpointId>, Vec<PeerResult>) {
        let registry = self.registry();
        let now = Instant::now();
        let stale = Duration::from_secs(self.config.peer_stale_secs);
        let (mut asked, mut skipped) = (Vec::new(), Vec::new());
        for peer in &link.targets {
            if registry
                .get(peer)
                .is_some_and(|entry| entry.has_flag(FLAG_EQUIVOCATION))
            {
                let mut result = PeerResult::new(*peer, "refused");
                result.flags.push(FLAG_EQUIVOCATION.to_string());
                skipped.push(result);
            } else if registry.should_ask(peer, now, stale) {
                asked.push(*peer);
            } else {
                skipped.push(PeerResult::new(*peer, "skipped_stale"));
            }
        }
        (asked, skipped)
    }

    /// Send one request with a timeout and record in the registry whether the peer answered.
    async fn send(
        &self,
        link: &PeerLink,
        peer: EndpointId,
        body: RequestBody,
        timeout_ms: u64,
    ) -> (Sent, u64) {
        let started = Instant::now();
        let request = RagRequest::new(body);
        let outcome = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            protocol::request_peer(
                &link.endpoint,
                peer,
                &request,
                self.config.max_message_bytes,
            ),
        )
        .await;
        let ms = started.elapsed().as_millis() as u64;
        let now = Instant::now();

        let sent = match outcome {
            Ok(Ok(response)) => {
                self.registry().record_ok(peer, now);
                if response.embedding_model != self.config.embedding_model {
                    warn!(
                        "⚠️ Peer {} embeds with {} but this node uses {}; their scores are not comparable",
                        peer.fmt_short(),
                        response.embedding_model,
                        self.config.embedding_model
                    );
                }
                Sent::Answer(response)
            }
            Ok(Err(e)) => match e.downcast::<StaleRoot>() {
                Ok(stale) => {
                    self.registry().record_root(peer, stale.0.clone(), now);
                    Sent::Stale(stale.0)
                }
                Err(e) => {
                    self.registry().record_failure(peer, e.to_string(), now);
                    Sent::Failed {
                        status: "error",
                        error: Some(e.to_string()),
                    }
                }
            },
            Err(_) => {
                self.registry()
                    .record_failure(peer, "request timed out".to_string(), now);
                Sent::Failed {
                    status: "timeout",
                    error: None,
                }
            }
        };
        (sent, ms)
    }

    /// The registry's root for `peer` if it is at `root_version`, with
    /// whether it was first seen after `query_started`.
    fn registered_root(
        &self,
        peer: &EndpointId,
        root_version: u64,
        query_started: Instant,
    ) -> Option<(SignedRoot, bool)> {
        let registry = self.registry();
        let entry = registry.get(peer)?;
        let root = entry.root.clone().filter(|r| r.version == root_version)?;
        let new = entry.first_seen.is_some_and(|t| t >= query_started);
        Some((root, new))
    }

    /// The peer's signed root at `root_version` (section 6.2, step 1). If the
    /// registry is behind, fetch it once with `hello` and look again.
    async fn root_at(
        &self,
        link: &PeerLink,
        peer: EndpointId,
        root_version: u64,
        query_started: Instant,
    ) -> Option<(SignedRoot, bool)> {
        if let Some(found) = self.registered_root(&peer, root_version, query_started) {
            return Some(found);
        }
        self.hello_peer(&link.endpoint, peer).await;
        self.registered_root(&peer, root_version, query_started)
    }

    /// Round 1 with one peer: fetch its nearest centroids and keep those whose
    /// proofs lead to its signed root.
    async fn round1(
        &self,
        link: &PeerLink,
        peer: EndpointId,
        query: &str,
        query_started: Instant,
    ) -> PeerState {
        let mut state = PeerState::new(peer);
        let body = RequestBody::Centroids {
            query: query.to_string(),
            n: self.config.centroids_per_peer,
        };
        let (sent, ms) = self
            .send(link, peer, body, self.config.select_timeout_ms)
            .await;
        state.result.round1_ms = Some(ms);

        let response = match sent {
            Sent::Answer(response) => response,
            Sent::Stale(_) => {
                state.fail(
                    "error",
                    Some("unexpected stale_root in round 1".to_string()),
                );
                return state;
            }
            Sent::Failed { status, error } => {
                state.fail(status, error);
                return state;
            }
        };
        state.node_id = response.node_id;
        let ResponseBody::Centroids {
            root_version,
            centroids,
            ..
        } = response.body
        else {
            state.fail(
                "error",
                Some("peer answered round 1 with another kind".to_string()),
            );
            return state;
        };
        state.root_version = root_version;
        state.result.root_version = Some(root_version);
        state.result.offered_clusters = centroids.len();

        let Some((root, new_root)) = self.root_at(link, peer, root_version, query_started).await
        else {
            state.fail(
                "no_root",
                Some(format!("no signed root at version {}", root_version)),
            );
            return state;
        };
        state.new_root = new_root;
        let Ok(root_hash) = root.root_bytes() else {
            state.fail("no_root", Some("signed root is not a hash".to_string()));
            return state;
        };

        let mut seen = HashSet::new();
        for entry in centroids.into_iter().take(self.config.centroids_per_peer) {
            if !seen.insert(entry.cluster_id) {
                continue; // the same cluster offered twice counts once
            }
            match (
                entry.verify_at(&root_hash, root.dim, root.num_clusters),
                entry.subtree_root(),
            ) {
                (Ok(centroid), Ok(subtree_root)) => state.offers.push(Offer {
                    cluster_id: entry.cluster_id,
                    subtree_root,
                    centroid,
                    claimed_best: entry.best_chunk_score,
                }),
                _ => self.raise(&mut state, FLAG_BAD_CENTROID_PROOF),
            }
        }
        state
    }

    /// Round 2 with one peer: fetch the best chunks of the clusters it won.
    async fn round2(&self, link: &PeerLink, state: &mut PeerState, query: &str) {
        let body = RequestBody::Chunks {
            query: query.to_string(),
            root_version: state.root_version,
            clusters: state.selected.clone(),
            per_cluster: self.config.chunks_per_cluster,
        };
        let (sent, ms) = self
            .send(link, state.peer, body, self.config.peer_timeout_ms)
            .await;
        state.result.round2_ms = Some(ms);

        let response = match sent {
            Sent::Answer(response) => response,
            Sent::Stale(root) => {
                let error = format!(
                    "stale_root: the peer moved to root version {} between rounds",
                    root.version
                );
                state.fail("error", Some(error));
                return;
            }
            Sent::Failed { status, error } => {
                state.fail(status, error);
                return;
            }
        };
        let ResponseBody::Chunks {
            root_version,
            chunks,
            ..
        } = response.body
        else {
            state.fail(
                "error",
                Some("peer answered round 2 with another kind".to_string()),
            );
            return;
        };
        if root_version != state.root_version {
            state.fail(
                "error",
                Some(format!(
                    "root version changed from {} to {}",
                    state.root_version, root_version
                )),
            );
            return;
        }

        // Proofs are checked against the subtree roots verified in round 1.
        let roots: HashMap<u32, Hash> = state
            .offers
            .iter()
            .filter(|o| state.selected.contains(&o.cluster_id))
            .map(|o| (o.cluster_id, o.subtree_root))
            .collect();
        let checked = check_chunks(
            chunks,
            &roots,
            self.config.chunks_per_cluster,
            state.new_root,
            self.config.drop_unverified,
            &state.node_id,
        );
        self.take_checked(state, checked);
    }

    fn take_checked(&self, state: &mut PeerState, checked: CheckedChunks) {
        state.answered = true;
        state.result.rejected_chunks += checked.rejected;
        if checked.bad_proof {
            self.raise(state, FLAG_BAD_CHUNK_PROOF);
        }
        state.chunks = checked.kept;
        if self.config.require_prior_root {
            let before = state.chunks.len();
            state
                .chunks
                .retain(|c| c.verification.as_deref() != Some(VERIFIED_NEW_ROOT));
            state.result.rejected_chunks += before - state.chunks.len();
        }
    }

    /// Flat mode with one peer: its best `top_k` chunks, checked against its root.
    async fn flat_peer(
        &self,
        link: &PeerLink,
        peer: EndpointId,
        query: &str,
        top_k: usize,
        query_started: Instant,
    ) -> PeerState {
        let mut state = PeerState::new(peer);
        let body = RequestBody::Search {
            query: query.to_string(),
            top_k,
        };
        let (sent, ms) = self
            .send(link, peer, body, self.config.peer_timeout_ms)
            .await;
        state.result.round1_ms = Some(ms);

        let response = match sent {
            Sent::Answer(response) => response,
            Sent::Stale(_) => {
                state.fail(
                    "error",
                    Some("unexpected stale_root in a search".to_string()),
                );
                return state;
            }
            Sent::Failed { status, error } => {
                state.fail(status, error);
                return state;
            }
        };
        state.node_id = response.node_id;
        let ResponseBody::Search {
            root_version,
            clusters,
            chunks,
            ..
        } = response.body
        else {
            state.fail(
                "error",
                Some("peer answered a search with another kind".to_string()),
            );
            return state;
        };
        state.root_version = root_version;
        state.result.root_version = Some(root_version);
        state.result.offered_clusters = clusters.len();

        let Some((root, new_root)) = self.root_at(link, peer, root_version, query_started).await
        else {
            state.fail(
                "no_root",
                Some(format!("no signed root at version {}", root_version)),
            );
            return state;
        };
        state.new_root = new_root;
        let Ok(root_hash) = root.root_bytes() else {
            state.fail("no_root", Some("signed root is not a hash".to_string()));
            return state;
        };

        let mut roots = HashMap::new();
        for entry in &clusters {
            match (
                entry.verify_at(&root_hash, root.dim, root.num_clusters),
                entry.subtree_root(),
            ) {
                (Ok(_), Ok(subtree_root)) => {
                    roots.insert(entry.cluster_id, subtree_root);
                }
                _ => self.raise(&mut state, FLAG_BAD_CENTROID_PROOF),
            }
        }
        let extra = chunks.len().saturating_sub(top_k);
        let mut checked = check_chunks(
            chunks.into_iter().take(top_k).collect(),
            &roots,
            usize::MAX,
            new_root,
            self.config.drop_unverified,
            &state.node_id,
        );
        checked.rejected += extra;
        self.take_checked(&mut state, checked);
        state
    }

    /// Re-embed every kept remote chunk in one request and use the asker's
    /// scores (section 6.2, step 5); flag peers whose numbers were off.
    async fn rescore(&self, states: &mut [PeerState], q: &[f32]) -> Result<(), BoxError> {
        let texts: Vec<&str> = states
            .iter()
            .flat_map(|s| s.chunks.iter().map(|c| c.text.as_str()))
            .collect();
        if texts.is_empty() {
            return Ok(());
        }
        let vectors = self.sidecar.embed(&texts).await?;
        if vectors.len() != texts.len() {
            return Err("sidecar returned the wrong number of embeddings".into());
        }

        let tolerance = self.config.score_tolerance;
        let mut next = 0;
        for state in states.iter_mut() {
            let n = state.chunks.len();
            if apply_rescore(&mut state.chunks, &vectors[next..next + n], q, tolerance) {
                self.raise(state, FLAG_SCORE_MISMATCH);
            }
            next += n;

            if state.answered {
                let claims: Vec<(u32, f64)> = state
                    .offers
                    .iter()
                    .filter(|o| state.selected.contains(&o.cluster_id))
                    .filter_map(|o| o.claimed_best.map(|claim| (o.cluster_id, claim)))
                    .collect();
                if !overclaimed(&claims, &state.chunks, tolerance).is_empty() {
                    self.raise(state, FLAG_SCORE_OVERCLAIM);
                }
            }
        }
        Ok(())
    }

    /// The two-round pipeline (section 6.2).
    pub(super) async fn two_round(
        &self,
        query: &str,
        link: &PeerLink,
        query_started: Instant,
    ) -> Result<RemoteRetrieval, BoxError> {
        let (asked, skipped) = self.split_targets(link);
        if asked.is_empty() {
            return Ok(RemoteRetrieval {
                peers: skipped,
                ..Default::default()
            });
        }

        // Round 1 with every peer, and the query embedding, at the same time.
        let select_started = Instant::now();
        let round1 = futures::future::join_all(
            asked
                .iter()
                .map(|peer| self.round1(link, *peer, query, query_started)),
        );
        let query_text = [query];
        let (embedded, mut states) = tokio::join!(self.sidecar.embed(&query_text), round1);
        let q = embedded?
            .pop()
            .ok_or("the sidecar returned no query embedding")?;

        // Score every verified centroid ourselves and pick the winners.
        let mut candidates = Vec::new();
        let mut owner = Vec::new();
        for (index, state) in states.iter().enumerate() {
            for offer in &state.offers {
                if offer.centroid.len() != q.len() {
                    continue; // another embedding size: not comparable
                }
                candidates.push(Candidate {
                    peer: state.peer,
                    cluster_id: offer.cluster_id,
                    sim: dot(&q, &offer.centroid),
                    claimed_best: offer.claimed_best,
                });
                owner.push(index);
            }
        }
        let cfg = &self.config;
        for winner in select_clusters(
            &candidates,
            cfg.select_by,
            cfg.max_clusters,
            cfg.min_centroid_score,
        ) {
            states[owner[winner]]
                .selected
                .push(candidates[winner].cluster_id);
        }
        let select_ms = select_started.elapsed().as_millis() as u64;

        // Round 2, only with peers that won a cluster.
        let fetch_started = Instant::now();
        futures::future::join_all(
            states
                .iter_mut()
                .filter(|s| !s.selected.is_empty())
                .map(|state| self.round2(link, state, query)),
        )
        .await;
        let fetch_ms = fetch_started.elapsed().as_millis() as u64;

        let rescore_started = Instant::now();
        if cfg.rescore {
            self.rescore(&mut states, &q).await?;
        }
        let rescore_ms = rescore_started.elapsed().as_millis() as u64;

        Ok(collect(states, skipped, select_ms, fetch_ms, rescore_ms))
    }

    /// The flat pipeline (section 6.3): one round, the same checks.
    pub(super) async fn flat(
        &self,
        query: &str,
        top_k: usize,
        link: &PeerLink,
        query_started: Instant,
    ) -> Result<RemoteRetrieval, BoxError> {
        let (asked, skipped) = self.split_targets(link);
        if asked.is_empty() {
            return Ok(RemoteRetrieval {
                peers: skipped,
                ..Default::default()
            });
        }

        let fetch_started = Instant::now();
        let searches = futures::future::join_all(
            asked
                .iter()
                .map(|peer| self.flat_peer(link, *peer, query, top_k, query_started)),
        );
        let embed = async {
            if self.config.rescore {
                Some(self.sidecar.embed(&[query]).await)
            } else {
                None
            }
        };
        let (embedded, mut states) = tokio::join!(embed, searches);
        let fetch_ms = fetch_started.elapsed().as_millis() as u64;

        let rescore_started = Instant::now();
        if let Some(embedded) = embedded {
            let q = embedded?
                .pop()
                .ok_or("the sidecar returned no query embedding")?;
            self.rescore(&mut states, &q).await?;
        }
        let rescore_ms = rescore_started.elapsed().as_millis() as u64;

        Ok(collect(states, skipped, 0, fetch_ms, rescore_ms))
    }
}

fn collect(
    states: Vec<PeerState>,
    skipped: Vec<PeerResult>,
    select_ms: u64,
    fetch_ms: u64,
    rescore_ms: u64,
) -> RemoteRetrieval {
    let mut remote = RemoteRetrieval {
        select_ms,
        fetch_ms,
        rescore_ms,
        ..Default::default()
    };
    for state in states {
        let (result, chunks) = state.finish();
        remote.peers.push(result);
        remote.chunks.extend(chunks);
    }
    remote.peers.extend(skipped);
    remote
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rag::merkle::ProofStep;
    use serde_json::Value;

    const FIXTURE: &str = include_str!("../../tests/fixtures/merkle_vectors.json");

    fn peer(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn candidate(seed: u8, cluster_id: u32, sim: f64, claimed_best: Option<f64>) -> Candidate {
        Candidate {
            peer: peer(seed),
            cluster_id,
            sim,
            claimed_best,
        }
    }

    #[test]
    fn selection_ranks_caps_and_thresholds() {
        let candidates = vec![
            candidate(1, 0, 0.30, None),
            candidate(1, 1, 0.70, None),
            candidate(2, 0, 0.50, None),
            candidate(2, 1, 0.10, None),
            candidate(3, 4, 0.60, None),
        ];
        // The best three, best first.
        assert_eq!(
            select_clusters(&candidates, SelectBy::Centroid, 3, 0.0),
            vec![1, 4, 2]
        );
        // The threshold drops weak ones.
        assert_eq!(
            select_clusters(&candidates, SelectBy::Centroid, 5, 0.45),
            vec![1, 4, 2]
        );
        // ...but the single best is always kept.
        assert_eq!(
            select_clusters(&candidates, SelectBy::Centroid, 5, 0.99),
            vec![1]
        );
        assert!(select_clusters(&[], SelectBy::Centroid, 5, 0.2).is_empty());
    }

    #[test]
    fn selection_by_best_chunk_uses_the_claims() {
        let candidates = vec![
            candidate(1, 0, 0.74, Some(0.77)),
            candidate(1, 4, 0.73, Some(0.81)),
        ];
        assert_eq!(
            select_clusters(&candidates, SelectBy::Centroid, 1, 0.2),
            vec![0]
        );
        assert_eq!(
            select_clusters(&candidates, SelectBy::BestChunk, 1, 0.2),
            vec![1]
        );
    }

    /// The fixture's three-cluster corpus as the chunks a peer would send.
    fn fixture_chunks() -> (Vec<ProvenChunk>, HashMap<u32, Hash>) {
        let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
        let case = &fixture["commitments"][0];
        let chunks = case["chunks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| ProvenChunk {
                chunk_id: c["chunk_id"].as_str().unwrap().to_string(),
                source: "fixture".to_string(),
                text: c["text"].as_str().unwrap().to_string(),
                score: 0.5,
                cluster_id: c["cluster"].as_u64().unwrap() as u32,
                chunk_proof: serde_json::from_value::<Vec<ProofStep>>(c["chunk_proof"].clone())
                    .unwrap(),
            })
            .collect();
        let mut roots = HashMap::new();
        for entry in case["clusters"].as_array().unwrap() {
            let mut root = [0u8; 32];
            hex::decode_to_slice(entry["subtree_root"].as_str().unwrap(), &mut root).unwrap();
            roots.insert(entry["cluster_id"].as_u64().unwrap() as u32, root);
        }
        (chunks, roots)
    }

    #[test]
    fn genuine_chunks_verify_and_tampered_ones_are_dropped() {
        let (mut chunks, roots) = fixture_chunks();
        let checked = check_chunks(chunks.clone(), &roots, 10, false, true, "peer");
        assert_eq!(
            (checked.kept.len(), checked.rejected, checked.bad_proof),
            (6, 0, false)
        );
        assert!(checked
            .kept
            .iter()
            .all(|c| c.verification.as_deref() == Some(VERIFIED)));
        assert_eq!(checked.kept[3].cluster_id, Some(2));

        chunks[0].text = chunks[0].text.replace("23", "24");
        let checked = check_chunks(chunks.clone(), &roots, 10, false, true, "peer");
        assert_eq!(
            (checked.kept.len(), checked.rejected, checked.bad_proof),
            (5, 1, true)
        );

        // Kept, but marked, when unverified chunks are not dropped.
        let checked = check_chunks(chunks, &roots, 10, true, false, "peer");
        assert_eq!(checked.kept[0].verification.as_deref(), Some(FAILED_PROOF));
        assert_eq!(
            checked.kept[1].verification.as_deref(),
            Some(VERIFIED_NEW_ROOT)
        );
    }

    #[test]
    fn chunks_from_unrequested_clusters_or_beyond_the_cap_are_rejected() {
        let (chunks, mut roots) = fixture_chunks();
        roots.remove(&2); // cluster 2 was not requested
        let checked = check_chunks(chunks.clone(), &roots, 10, false, true, "peer");
        assert_eq!((checked.kept.len(), checked.rejected), (4, 2));

        // Cluster 0 holds 3 chunks; with a cap of 1 only its first is taken.
        let (_, roots) = fixture_chunks();
        let checked = check_chunks(chunks, &roots, 1, false, true, "peer");
        assert_eq!((checked.kept.len(), checked.rejected), (3, 3));
    }

    fn scored(cluster: u32, score: f64) -> Chunk {
        Chunk {
            chunk_id: format!("c{}", cluster),
            score,
            cluster_id: Some(cluster),
            verification: Some(VERIFIED.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn rescoring_replaces_scores_and_flags_inflated_ones() {
        let q = vec![1.0, 0.0];
        let mut chunks = vec![scored(0, 0.6), scored(1, 0.9)];
        let vectors = vec![vec![0.6, 0.8], vec![0.6, 0.8]];
        assert!(apply_rescore(&mut chunks, &vectors, &q, 0.01));
        assert!((chunks[1].score - 0.6).abs() < 1e-6);
        assert_eq!(chunks[1].claimed_score, Some(0.9));
        assert!(chunks[0].flags.is_empty());
        assert_eq!(chunks[1].flags, vec![FLAG_SCORE_MISMATCH.to_string()]);
    }

    #[test]
    fn overclaims_are_found_per_cluster() {
        let chunks = vec![scored(0, 0.60), scored(1, 0.50)];
        // Cluster 0 delivered what it claimed; cluster 1 did not; cluster 2 delivered nothing.
        let claims = vec![(0, 0.605), (1, 0.80), (2, 0.40)];
        assert_eq!(overclaimed(&claims, &chunks, 0.01), vec![1, 2]);
    }
}
