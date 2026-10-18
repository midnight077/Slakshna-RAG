//! Retrieval-augmented generation on top of the mesh.
//!
//! The node itself does no ML: document search and answer writing live in a
//! local Python sidecar (`rag_engine.py`). This module is the node's side of
//! that — the sidecar client and the query pipeline behind `POST /rag/query`.

pub mod merkle;
pub mod protocol;
pub mod sidecar;

use crate::config::RagConfig;
use crate::history::BoxError;
use merkle::{ Commitment, SignedRoot };
use protocol::{ RagRequest, RequestBody, ResponseBody };
use serde::{ Deserialize, Serialize };
use sha2::{ Digest, Sha256 };
use sidecar::SidecarClient;
use std::collections::HashSet;
use std::time::{ Duration, Instant };
use tokio::sync::RwLock;
use tracing::{ info, warn };

/// One retrieved passage. `node_id` records which node's index it came from;
/// the sidecar does not know it, so the node fills it in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub chunk_id: String,
    pub source: String,
    pub text: String,
    pub score: f64,
    #[serde(default)]
    pub node_id: Option<String>,
}

/// Which indexes a query consults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    /// This node's index plus every reachable peer's.
    #[default]
    All,
    /// Only this node's index.
    Local,
    /// Only peers' indexes.
    Peers,
}

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub query: String,
    #[serde(default)]
    pub top_k: Option<usize>,
    #[serde(default)]
    pub final_k: Option<usize>,
    #[serde(default)]
    pub scope: Scope,
}

/// What a query needs from the mesh to reach other nodes.
pub struct PeerLink {
    pub endpoint: iroh::Endpoint,
    pub targets: Vec<iroh::EndpointId>,
}

/// Outcome of asking one peer for chunks.
#[derive(Debug, Clone, Serialize)]
pub struct PeerResult {
    pub endpoint_id: String,
    /// "ok", "timeout" or "error".
    pub status: String,
    pub chunks: usize,
    pub rtt_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Outcome of fetching one peer's signed root (`hello`).
#[derive(Debug, Clone, Serialize)]
pub struct PeerHello {
    pub endpoint_id: String,
    /// "ok", "timeout" or "error".
    pub status: String,
    pub rtt_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root: Option<SignedRoot>,
    /// Whether `root` is signed by the key behind `endpoint_id`.
    pub signature_valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Timings {
    pub local_retrieve: u64,
    pub peer_retrieve: u64,
    pub generate: u64,
    pub total: u64,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub answer: String,
    pub contexts: Vec<Chunk>,
    pub peers: Vec<PeerResult>,
    pub timings_ms: Timings,
}

/// Pool chunks from several nodes into one ranked list of at most `final_k`.
///
/// Every node embeds with the same model, so cosine scores are comparable
/// across nodes and a plain sort is a fair ranking. Chunks with identical text
/// (the same document held by two nodes) are kept once, at their best score.
pub fn merge_chunks(mut chunks: Vec<Chunk>, final_k: usize) -> Vec<Chunk> {
    chunks.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.chunk_id.cmp(&b.chunk_id))
    });

    let mut seen = HashSet::new();
    chunks.retain(|chunk| seen.insert(Sha256::digest(chunk.text.as_bytes())));
    chunks.truncate(final_k);
    chunks
}

pub struct RagService {
    pub config: RagConfig,
    pub sidecar: SidecarClient,
    node_id: String,
    /// This node's Iroh transport key; its public half is our EndpointId, so
    /// peers can check our root's signature against the id they dial.
    secret_key: iroh::SecretKey,
    /// Our latest signed Merkle root, served to peers in `hello`.
    own_root: RwLock<Option<SignedRoot>>,
}

impl RagService {
    pub fn new(config: RagConfig, sidecar: SidecarClient, node_id: String, secret_key: iroh::SecretKey) -> Self {
        RagService { config, sidecar, node_id, secret_key, own_root: RwLock::new(None) }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub async fn own_root(&self) -> Option<SignedRoot> {
        self.own_root.read().await.clone()
    }

    /// Sign `commitment` and make it the root served to peers.
    pub async fn adopt_commitment(&self, commitment: &Commitment) -> Result<SignedRoot, BoxError> {
        let signed = SignedRoot::sign(&self.secret_key, commitment)?;
        let mut own_root = self.own_root.write().await;
        if own_root.as_ref().map(|r| r.version) != Some(signed.version) {
            info!(
                "🔏 Signed RAG root {:.16}… (version {}, {} chunks in {} clusters)",
                signed.root,
                signed.version,
                signed.num_chunks,
                signed.num_clusters
            );
        }
        *own_root = Some(signed.clone());
        Ok(signed)
    }

    /// Fetch the sidecar's current commitment and sign it. Run at startup and
    /// after every ingest.
    pub async fn refresh_own_root(&self) -> Result<SignedRoot, BoxError> {
        let commitment = self.sidecar.commitment().await?;
        self.adopt_commitment(&commitment).await
    }

    /// Our signed root, signing one first if there is none yet.
    pub async fn current_root(&self) -> Result<SignedRoot, BoxError> {
        match self.own_root().await {
            Some(root) => Ok(root),
            None => self.refresh_own_root().await,
        }
    }

    /// Re-sign if the sidecar answered under a root version we have not signed
    /// (an ingest that bypassed the node), so `hello` matches what we serve.
    pub async fn ensure_root_version(&self, version: u64) {
        if self.own_root().await.map(|r| r.version) != Some(version) {
            if let Err(e) = self.refresh_own_root().await {
                warn!("⚠️ Could not re-sign the RAG root: {}", e);
            }
        }
    }

    /// Fetch one peer's signed root and check its signature against the
    /// peer's EndpointId. Never fails; problems are reported in the result.
    pub async fn hello_peer(&self, endpoint: &iroh::Endpoint, peer: iroh::EndpointId) -> PeerHello {
        let started = Instant::now();
        let request = RagRequest::new(RequestBody::Hello);
        let outcome = tokio::time::timeout(
            Duration::from_millis(self.config.peer_timeout_ms),
            protocol::request_peer(endpoint, peer, &request, self.config.max_message_bytes),
        ).await;

        let mut result = PeerHello {
            endpoint_id: peer.to_string(),
            status: "ok".to_string(),
            rtt_ms: started.elapsed().as_millis() as u64,
            node_id: None,
            root: None,
            signature_valid: false,
            error: None,
        };
        match outcome {
            Ok(Ok(response)) => {
                result.node_id = Some(response.node_id);
                if let ResponseBody::Hello { root } = response.body {
                    result.signature_valid = root.verify(&peer);
                    result.root = Some(root);
                }
            }
            Ok(Err(e)) => {
                result.status = "error".to_string();
                result.error = Some(e.to_string());
            }
            Err(_) => result.status = "timeout".to_string(),
        }
        result
    }

    /// Ask one peer for chunks, bounded by `peer_timeout_ms`. Never fails: a
    /// slow or broken peer yields a `PeerResult` saying so and no chunks.
    async fn ask_peer(
        &self,
        endpoint: &iroh::Endpoint,
        peer: iroh::EndpointId,
        query: &str,
        top_k: usize,
    ) -> (PeerResult, Vec<Chunk>) {
        let started = Instant::now();
        // Until the verifying pipelines exist (Phase M6 of merkle_tree.md), a
        // flat `search` whose proofs are not yet checked.
        let request = RagRequest::new(RequestBody::Search { query: query.to_string(), top_k });
        let outcome = tokio::time::timeout(
            Duration::from_millis(self.config.peer_timeout_ms),
            protocol::request_peer(endpoint, peer, &request, self.config.max_message_bytes),
        ).await;
        let rtt_ms = started.elapsed().as_millis() as u64;

        let (status, error, chunks) = match outcome {
            Ok(Ok(response)) => {
                if response.embedding_model != self.config.embedding_model {
                    warn!(
                        "⚠️ Peer {} embeds with {} but this node uses {}; their scores are not comparable",
                        peer.fmt_short(),
                        response.embedding_model,
                        self.config.embedding_model
                    );
                }
                let chunks = match response.body {
                    ResponseBody::Search { chunks, .. } => chunks
                        .into_iter()
                        .map(|chunk| chunk.into_chunk(&response.node_id))
                        .collect(),
                    _ => Vec::new(),
                };
                ("ok", None, chunks)
            }
            Ok(Err(e)) => ("error", Some(e.to_string()), Vec::new()),
            Err(_) => ("timeout", None, Vec::new()),
        };

        let result = PeerResult {
            endpoint_id: peer.to_string(),
            status: status.to_string(),
            chunks: chunks.len(),
            rtt_ms,
            error,
        };
        (result, chunks)
    }

    /// Chunks from this node's own index, tagged with this node's id.
    pub async fn retrieve_local(&self, query: &str, top_k: usize) -> Result<Vec<Chunk>, BoxError> {
        let mut chunks = self.sidecar.retrieve(query, top_k).await?.chunks;
        for chunk in &mut chunks {
            chunk.node_id = Some(self.node_id.clone());
        }
        Ok(chunks)
    }

    /// Run the full pipeline for one query: retrieve, merge, generate.
    ///
    /// `link` is the way to other nodes; without it only the local index is used.
    pub async fn query(
        &self,
        request: QueryRequest,
        link: Option<PeerLink>,
    ) -> Result<QueryResponse, BoxError> {
        let started = Instant::now();
        let top_k = request.top_k.unwrap_or(self.config.top_k);
        let final_k = request.final_k.unwrap_or(self.config.final_k);

        // The local index and every peer are consulted at the same time, so
        // retrieval takes as long as the slowest one, not the sum.
        let local = async {
            let local_started = Instant::now();
            let chunks = if request.scope != Scope::Peers {
                self.retrieve_local(&request.query, top_k).await?
            } else {
                Vec::new()
            };
            Ok::<_, BoxError>((chunks, local_started.elapsed().as_millis() as u64))
        };

        let remote = async {
            let peers_started = Instant::now();
            let outcomes = match &link {
                Some(link) if request.scope != Scope::Local => {
                    futures::future::join_all(
                        link.targets
                            .iter()
                            .map(|peer| self.ask_peer(&link.endpoint, *peer, &request.query, top_k)),
                    ).await
                }
                _ => Vec::new(),
            };
            (outcomes, peers_started.elapsed().as_millis() as u64)
        };

        let (local, (outcomes, peer_retrieve)) = tokio::join!(local, remote);
        let (mut pooled, local_retrieve) = local?;

        let mut peers = Vec::new();
        for (result, chunks) in outcomes {
            peers.push(result);
            pooled.extend(chunks);
        }

        let contexts = merge_chunks(pooled, final_k);

        // With nothing retrieved the generator would answer from its own
        // memory, which is exactly what RAG is meant to avoid.
        let generate_started = Instant::now();
        let answer = if contexts.is_empty() {
            String::new()
        } else {
            self.sidecar.generate(&request.query, &contexts).await?.answer
        };
        let generate = generate_started.elapsed().as_millis() as u64;

        Ok(QueryResponse {
            answer,
            contexts,
            peers,
            timings_ms: Timings {
                local_retrieve,
                peer_retrieve,
                generate,
                total: started.elapsed().as_millis() as u64,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(id: &str, text: &str, score: f64) -> Chunk {
        Chunk {
            chunk_id: id.to_string(),
            source: "doc".to_string(),
            text: text.to_string(),
            score,
            node_id: None,
        }
    }

    #[test]
    fn merge_sorts_by_score_and_truncates() {
        let merged = merge_chunks(
            vec![chunk("a", "one", 0.2), chunk("b", "two", 0.9), chunk("c", "three", 0.5)],
            2,
        );
        let ids: Vec<_> = merged.iter().map(|c| c.chunk_id.as_str()).collect();
        assert_eq!(ids, vec!["b", "c"]);
    }

    #[test]
    fn merge_keeps_duplicate_text_once_at_best_score() {
        let merged = merge_chunks(
            vec![chunk("n1#0", "same text", 0.4), chunk("n2#0", "same text", 0.7), chunk("x", "other", 0.1)],
            5,
        );
        let ids: Vec<_> = merged.iter().map(|c| c.chunk_id.as_str()).collect();
        assert_eq!(ids, vec!["n2#0", "x"]);
    }

    #[test]
    fn scope_defaults_to_all_and_parses_lowercase() {
        let request: QueryRequest = serde_json::from_str(r#"{"query": "q"}"#).unwrap();
        assert_eq!(request.scope, Scope::All);
        let request: QueryRequest = serde_json::from_str(r#"{"query": "q", "scope": "local"}"#).unwrap();
        assert_eq!(request.scope, Scope::Local);
    }
}
