//! Peer-to-peer retrieval: one node asks another for chunks matching a query.
//!
//! This is a second protocol on the node's Iroh endpoint, next to gossip. The
//! asker opens a QUIC connection and one two-way stream, writes a JSON
//! `RagRequest`, and reads a JSON `RagResponse` back. Unlike gossip, only the
//! two nodes involved see the query and the chunks.

use super::{ Chunk, RagService };
use crate::history::BoxError;
use iroh::endpoint::Connection;
use iroh::protocol::{ AcceptError, ProtocolHandler };
use iroh::{ Endpoint, EndpointId };
use serde::{ Deserialize, Serialize };
use std::sync::Arc;
use std::time::Instant;
use tracing::{ info, warn };

/// Protocol identifier. Bump the trailing number on any incompatible change to
/// the request or response, so mismatched builds refuse to connect instead of
/// misreading each other.
pub const RAG_ALPN: &[u8] = b"/slakshna/rag/1";

const WIRE_VERSION: u32 = 1;
/// Upper bound on chunks a peer will return, whatever the asker requests.
const MAX_TOP_K: usize = 50;

#[derive(Debug, Serialize, Deserialize)]
pub struct RagRequest {
    pub v: u32,
    pub query_id: String,
    pub query: String,
    pub top_k: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RagResponse {
    pub v: u32,
    pub query_id: String,
    pub node_id: String,
    pub embedding_model: String,
    pub chunks: Vec<Chunk>,
    pub retrieve_ms: u64,
    pub error: Option<String>,
}

/// The answering side: serves this node's index to peers.
pub struct RagProtocol {
    service: Arc<RagService>,
    allowed_peers: Option<Vec<String>>,
}

impl std::fmt::Debug for RagProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RagProtocol").finish_non_exhaustive()
    }
}

impl RagProtocol {
    pub fn new(service: Arc<RagService>, allowed_peers: Option<Vec<String>>) -> Self {
        RagProtocol { service, allowed_peers }
    }

    async fn answer(&self, request_bytes: &[u8]) -> RagResponse {
        let started = Instant::now();
        let mut response = RagResponse {
            v: WIRE_VERSION,
            query_id: String::new(),
            node_id: self.service.node_id().to_string(),
            embedding_model: self.service.config.embedding_model.clone(),
            chunks: Vec::new(),
            retrieve_ms: 0,
            error: None,
        };

        let request: RagRequest = match serde_json::from_slice(request_bytes) {
            Ok(request) => request,
            Err(e) => {
                response.error = Some(format!("malformed request: {}", e));
                return response;
            }
        };
        response.query_id = request.query_id;
        if request.v != WIRE_VERSION {
            response.error = Some(format!("unsupported request version {}", request.v));
            return response;
        }

        match self.service.retrieve_local(&request.query, request.top_k.min(MAX_TOP_K)).await {
            Ok(chunks) => response.chunks = chunks,
            Err(e) => response.error = Some(format!("retrieval failed: {}", e)),
        }
        response.retrieve_ms = started.elapsed().as_millis() as u64;
        response
    }
}

impl ProtocolHandler for RagProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote = connection.remote_id();

        if let Some(allowed) = &self.allowed_peers {
            if !allowed.is_empty() && !allowed.contains(&remote.to_string()) {
                warn!("🚫 Refused RAG query from unauthorized peer: {}", remote.fmt_short());
                connection.close(1u32.into(), b"not allowed");
                return Ok(());
            }
        }

        let (mut send, mut recv) = connection.accept_bi().await?;
        let request_bytes = recv
            .read_to_end(self.service.config.max_message_bytes)
            .await
            .map_err(AcceptError::from_err)?;

        let response = self.answer(&request_bytes).await;
        info!(
            "📚 RAG query from peer {}: returned {} chunk(s) in {} ms",
            remote.fmt_short(),
            response.chunks.len(),
            response.retrieve_ms
        );

        let response_bytes = serde_json::to_vec(&response).map_err(AcceptError::from_err)?;
        send.write_all(&response_bytes).await.map_err(AcceptError::from_err)?;
        send.finish()?;

        // The asker closes the connection once it has read the response.
        connection.closed().await;
        Ok(())
    }
}

/// The asking side: fetch `top_k` chunks for `query` from one peer.
pub async fn query_peer(
    endpoint: &Endpoint,
    peer: EndpointId,
    query: &str,
    top_k: usize,
    max_message_bytes: usize,
) -> Result<RagResponse, BoxError> {
    let request = RagRequest {
        v: WIRE_VERSION,
        query_id: uuid::Uuid::new_v4().to_string(),
        query: query.to_string(),
        top_k,
    };

    let connection = endpoint.connect(peer, RAG_ALPN).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&serde_json::to_vec(&request)?).await?;
    send.finish()?;
    let response_bytes = recv.read_to_end(max_message_bytes).await?;
    connection.close(0u32.into(), b"done");

    let response: RagResponse = serde_json::from_slice(&response_bytes)?;
    if let Some(error) = response.error {
        return Err(format!("peer reported: {}", error).into());
    }
    if response.query_id != request.query_id {
        return Err("peer answered a different query".into());
    }
    Ok(response)
}
