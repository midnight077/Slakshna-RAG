//! Retrieval-augmented generation on top of the mesh.
//!
//! The node itself does no ML: document search and answer writing live in a
//! local Python sidecar (`rag_engine.py`). This module is the node's side of
//! that — the sidecar client and the query pipeline behind `POST /rag/query`.

pub mod sidecar;

use crate::config::RagConfig;
use crate::history::BoxError;
use serde::{ Deserialize, Serialize };
use sha2::{ Digest, Sha256 };
use sidecar::SidecarClient;
use std::collections::HashSet;
use std::time::Instant;

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

/// Outcome of asking one peer for chunks.
#[derive(Debug, Clone, Serialize)]
pub struct PeerResult {
    pub endpoint_id: String,
    pub status: String,
    pub chunks: usize,
    pub rtt_ms: u64,
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
}

impl RagService {
    pub fn new(config: RagConfig, sidecar: SidecarClient, node_id: String) -> Self {
        RagService { config, sidecar, node_id }
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
    pub async fn query(&self, request: QueryRequest) -> Result<QueryResponse, BoxError> {
        let started = Instant::now();
        let top_k = request.top_k.unwrap_or(self.config.top_k);
        let final_k = request.final_k.unwrap_or(self.config.final_k);

        let mut pooled = Vec::new();

        let local_started = Instant::now();
        if request.scope != Scope::Peers {
            pooled.extend(self.retrieve_local(&request.query, top_k).await?);
        }
        let local_retrieve = local_started.elapsed().as_millis() as u64;

        // Peer retrieval is not wired up yet: every scope currently sees only
        // this node's index.
        let peers = Vec::new();
        let peer_retrieve = 0;

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
