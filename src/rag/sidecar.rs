//! Client for, and launcher of, the local Python RAG sidecar.

use super::Chunk;
use crate::config::{ NodeConfig, RagConfig };
use crate::history::BoxError;
use serde::{ de::DeserializeOwned, Deserialize, Serialize };
use std::time::Duration;
use tracing::info;

/// How long to wait for a freshly started sidecar to load its embedding model.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(180);
const RETRIEVE_TIMEOUT: Duration = Duration::from_secs(30);
/// Generation runs a language model on CPU and can take most of a minute.
const GENERATE_TIMEOUT: Duration = Duration::from_secs(600);
const INGEST_TIMEOUT: Duration = Duration::from_secs(1800);

#[derive(Debug, Deserialize)]
pub struct RetrieveResponse {
    pub chunks: Vec<Chunk>,
    pub embedding_model: String,
}

#[derive(Debug, Deserialize)]
pub struct GenerateResponse {
    pub answer: String,
}

#[derive(Clone)]
pub struct SidecarClient {
    base_url: String,
    http: reqwest::Client,
}

impl SidecarClient {
    pub fn new(port: u16) -> Self {
        SidecarClient {
            base_url: format!("http://127.0.0.1:{}", port),
            http: reqwest::Client::new(),
        }
    }

    async fn post<B: Serialize, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        timeout: Duration,
    ) -> Result<R, BoxError> {
        let response = self
            .http
            .post(format!("{}{}", self.base_url, path))
            .json(body)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| format!("RAG sidecar unreachable at {}: {}", self.base_url, e))?;

        let status = response.status();
        if !status.is_success() {
            let detail = response.text().await.unwrap_or_default();
            return Err(format!("RAG sidecar {} returned {}: {}", path, status, detail).into());
        }
        Ok(response.json::<R>().await?)
    }

    pub async fn health(&self) -> Result<serde_json::Value, BoxError> {
        let response = self
            .http
            .get(format!("{}/health", self.base_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json().await?)
    }

    pub async fn retrieve(&self, query: &str, top_k: usize) -> Result<RetrieveResponse, BoxError> {
        self.post(
            "/retrieve",
            &serde_json::json!({ "query": query, "top_k": top_k }),
            RETRIEVE_TIMEOUT,
        ).await
    }

    pub async fn generate(&self, query: &str, chunks: &[Chunk]) -> Result<GenerateResponse, BoxError> {
        self.post(
            "/generate",
            &serde_json::json!({ "query": query, "chunks": chunks }),
            GENERATE_TIMEOUT,
        ).await
    }

    /// Forward an ingest request body (`{path}` or `{documents}`) unchanged.
    pub async fn ingest(&self, body: &serde_json::Value) -> Result<serde_json::Value, BoxError> {
        self.post("/ingest", body, INGEST_TIMEOUT).await
    }
}

/// Launch `rag_engine.py serve` for this node and wait until it answers.
///
/// The returned child must be kept alive for as long as the node runs. The
/// sidecar is also told to exit when this process does, so a node that is
/// killed outright does not leave one behind holding the port.
pub async fn spawn_sidecar(
    rag: &RagConfig,
    node: &NodeConfig,
    client: &SidecarClient,
) -> Result<tokio::process::Child, BoxError> {
    if client.health().await.is_ok() {
        return Err(format!(
            "port {} already answers as a RAG sidecar; stop it, or set [rag] spawn_sidecar = false to use it",
            rag.sidecar_port
        ).into());
    }

    info!("📚 Starting RAG sidecar on 127.0.0.1:{} ({})", rag.sidecar_port, rag.python);

    let mut child = tokio::process::Command::new(&rag.python)
        .arg("rag_engine.py")
        .arg("serve")
        .args(["--data-dir", &node.data_dir])
        .args(["--port", &rag.sidecar_port.to_string()])
        .args(["--embedding-model", &rag.embedding_model])
        .args(["--generator", &rag.generator])
        .args(["--generator-model", &rag.generator_model])
        .args(["--generator-url", &rag.generator_url])
        .arg("--exit-with-parent")
        .current_dir(".")
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("Failed to start RAG sidecar with '{}': {}", rag.python, e))?;

    let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
    loop {
        if client.health().await.is_ok() {
            return Ok(child);
        }
        if let Some(status) = child.try_wait()? {
            return Err(format!("RAG sidecar exited during startup ({})", status).into());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("RAG sidecar did not become ready in time".into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
