use serde::{Deserialize, Serialize};
use std::fs;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub federation: FederationConfig,
    #[serde(default)]
    pub training: TrainingConfig,
    #[serde(default)]
    pub compression: CompressionConfig,
    pub node: NodeConfig,
    pub network: NetworkConfig,
    #[serde(default)]
    pub discovery: DiscoveryConfig,
    #[serde(default)]
    pub rag: RagConfig,
    pub logging: LoggingConfig,
}

/// Identifies the federation a node participates in. Every node sharing an `id`
/// derives the same gossip topic and therefore trains together.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationConfig {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingConfig {
    /// Whether this node runs the federated training loop. Set to false on
    /// machines that only take part in the mesh (e.g. CPU-only RAG nodes):
    /// the node still joins the federation but never launches the ML engine.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Length of a federated epoch. Nodes align to this on the wall clock so
    /// that independently started peers land on the same boundary.
    #[serde(default = "default_epoch_duration")]
    pub epoch_duration_secs: u64,
    /// How far into the epoch a node waits for peer updates to arrive before
    /// aggregating with whatever it has.
    #[serde(default = "default_sync_deadline")]
    pub sync_deadline_secs: u64,
    /// Number of participants expected in the federation. Once updates from
    /// this many nodes are on record, the sync barrier is released early.
    #[serde(default = "default_expected_peers")]
    pub expected_peers: usize,
}

fn default_epoch_duration() -> u64 {
    600
}
fn default_sync_deadline() -> u64 {
    300
}
fn default_expected_peers() -> usize {
    6
}

impl Default for TrainingConfig {
    fn default() -> Self {
        TrainingConfig {
            enabled: true,
            epoch_duration_secs: default_epoch_duration(),
            sync_deadline_secs: default_sync_deadline(),
            expected_peers: default_expected_peers(),
        }
    }
}

/// Settings for model-delta transport.  The payload itself remains opaque to
/// the Rust gossip layer; these values are passed to the Python ML engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    #[serde(default = "default_compression_enabled")]
    pub enabled: bool,
    #[serde(default = "default_sparsity")]
    pub sparsity: f64,
    #[serde(default = "default_quantization")]
    pub quantization: String,
    #[serde(default)]
    pub allow_legacy_delta_format: bool,
    #[serde(default = "default_max_payload_bytes")]
    pub max_payload_bytes: usize,
    #[serde(default = "default_max_tensor_elements")]
    pub max_tensor_elements: usize,
}

fn default_compression_enabled() -> bool { true }
fn default_sparsity() -> f64 { 0.1 }
fn default_quantization() -> String { "symmetric_int8".to_string() }
// Base64 expands by roughly 4/3; retain room under gossip's 10 MiB message cap.
fn default_max_payload_bytes() -> usize { 7 * 1024 * 1024 }
fn default_max_tensor_elements() -> usize { 10_000_000 }

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: default_compression_enabled(),
            sparsity: default_sparsity(),
            quantization: default_quantization(),
            allow_legacy_delta_format: false,
            max_payload_bytes: default_max_payload_bytes(),
            max_tensor_elements: default_max_tensor_elements(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: String,
    pub data_dir: String,
    #[serde(default)]
    pub gpu_id: Option<u32>,
    #[serde(default)]
    pub num_gpus: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub host: String,
    pub p2p_port: u16,
    pub ws_port: u16,
    pub api_port: u16,
    /// Optional seed peers, as Iroh EndpointId strings, each optionally suffixed
    /// with `@host:port` to pin a direct address on closed networks.
    ///
    /// Every entry is an equal member of the federation — there is no privileged
    /// node here. The list may be empty: a node that finds no seeds still starts,
    /// advertises itself over mDNS and the mainline DHT, and merges into the swarm
    /// as soon as any peer shows up. Peers learned at runtime are remembered in
    /// `{data_dir}/rocksdb` and redialled automatically on the next start.
    #[serde(default, alias = "boot_nodes")]
    pub peers: Option<Vec<String>>,
    /// Optional list of allowed Iroh EndpointId strings.
    /// If non-empty, only these peers may connect (whitelisting).
    #[serde(default)]
    pub allowed_peers: Option<Vec<String>>,
}

/// Which serverless discovery mechanisms this node participates in. All of them
/// are symmetric: a node both advertises itself and looks others up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// mDNS on the local network. Zero-config discovery of federation members on
    /// the same LAN or the same host, scoped to the federation id.
    #[serde(default = "default_true")]
    pub mdns: bool,
    /// BitTorrent mainline DHT. Resolves an EndpointId to its current address
    /// anywhere on the internet without contacting any server we operate.
    #[serde(default = "default_true")]
    pub dht: bool,
    /// Number 0's public pkarr/DNS address lookup. Fastest resolution path, but
    /// it is third-party infrastructure — turn it off for a fully self-contained
    /// federation and rely on `mdns` + `dht`.
    #[serde(default = "default_true")]
    pub dns: bool,
    /// Use public relay servers as a fallback transport when direct hole punching
    /// fails. Relays only forward encrypted QUIC; they cannot read traffic.
    #[serde(default = "default_true")]
    pub relay: bool,
}

fn default_true() -> bool {
    true
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        DiscoveryConfig { mdns: true, dht: true, dns: true, relay: true }
    }
}

/// Retrieval-augmented generation. Off unless `[rag] enabled = true`.
///
/// The ML work is done by a local Python sidecar (`rag_engine.py`) that the
/// node launches and talks to over HTTP on 127.0.0.1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Local port of this node's sidecar. Must be unique per node on a shared machine.
    #[serde(default = "default_sidecar_port")]
    pub sidecar_port: u16,
    /// Launch the sidecar as a child process. Set false to use one started by hand.
    #[serde(default = "default_true")]
    pub spawn_sidecar: bool,
    /// Python interpreter that has the RAG dependencies installed.
    #[serde(default = "default_rag_python")]
    pub python: String,
    /// Folder of PDF/.txt/.md files to ingest at startup. Files already in the
    /// index are skipped, so this is cheap after the first run.
    #[serde(default)]
    pub corpus_dir: Option<String>,
    /// Must be the same on every node: chunks from different nodes are ranked
    /// against each other by their similarity scores.
    #[serde(default = "default_embedding_model")]
    pub embedding_model: String,
    /// Chunks requested from each node's index.
    #[serde(default = "default_rag_k")]
    pub top_k: usize,
    /// Chunks kept after merging, and passed to the generator.
    #[serde(default = "default_rag_k")]
    pub final_k: usize,
    /// How long to wait for one peer's chunks before answering without them.
    #[serde(default = "default_peer_timeout_ms")]
    pub peer_timeout_ms: u64,
    /// Largest request or response accepted on the peer RAG protocol.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: usize,
    /// "ollama", or "none" to return retrieved chunks without an answer.
    #[serde(default = "default_generator")]
    pub generator: String,
    #[serde(default = "default_generator_model")]
    pub generator_model: String,
    #[serde(default = "default_generator_url")]
    pub generator_url: String,
    /// Timeout for fetching a peer's signed root (`hello`) and, later, for
    /// round 1 of a query. Shorter than `peer_timeout_ms` because round 2 waits on it.
    #[serde(default = "default_select_timeout_ms")]
    pub select_timeout_ms: u64,
    /// How often every peer's signed root is fetched again in the background.
    #[serde(default = "default_root_refresh_secs")]
    pub root_refresh_secs: u64,
    /// A peer not heard from for this long, or whose latest contact failed, is
    /// skipped at query time and left to the background refresh. Peers never
    /// tried yet are always asked.
    #[serde(default = "default_peer_stale_secs")]
    pub peer_stale_secs: u64,
}

fn default_sidecar_port() -> u16 { 8600 }
fn default_rag_python() -> String { ".venv-rag/bin/python".to_string() }
fn default_embedding_model() -> String { "sentence-transformers/all-MiniLM-L6-v2".to_string() }
fn default_rag_k() -> usize { 5 }
fn default_peer_timeout_ms() -> u64 { 5000 }
fn default_max_message_bytes() -> usize { 1024 * 1024 }
fn default_generator() -> String { "ollama".to_string() }
fn default_generator_model() -> String { "qwen2.5:1.5b".to_string() }
fn default_generator_url() -> String { "http://127.0.0.1:11434".to_string() }
fn default_select_timeout_ms() -> u64 { 2000 }
fn default_root_refresh_secs() -> u64 { 60 }
fn default_peer_stale_secs() -> u64 { 180 }

impl Default for RagConfig {
    fn default() -> Self {
        RagConfig {
            enabled: false,
            sidecar_port: default_sidecar_port(),
            spawn_sidecar: true,
            python: default_rag_python(),
            corpus_dir: None,
            embedding_model: default_embedding_model(),
            top_k: default_rag_k(),
            final_k: default_rag_k(),
            peer_timeout_ms: default_peer_timeout_ms(),
            max_message_bytes: default_max_message_bytes(),
            generator: default_generator(),
            generator_model: default_generator_model(),
            generator_url: default_generator_url(),
            select_timeout_ms: default_select_timeout_ms(),
            root_refresh_secs: default_root_refresh_secs(),
            peer_stale_secs: default_peer_stale_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
}

impl Config {
    pub fn load(path: &str) -> Result<Self, BoxError> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        if !config.compression.sparsity.is_finite()
            || config.compression.sparsity <= 0.0
            || config.compression.sparsity > 1.0
        {
            return Err("compression.sparsity must be finite and in (0, 1]".into());
        }
        if config.compression.quantization != "symmetric_int8" {
            return Err("compression.quantization must be symmetric_int8".into());
        }
        if config.compression.max_payload_bytes == 0
            || config.compression.max_payload_bytes > 2000000000
        {
            return Err("compression.max_payload_bytes must be between 1 and 2000000000".into());
        }
        if config.compression.max_tensor_elements == 0 {
            return Err("compression.max_tensor_elements must be positive".into());
        }
        if config.rag.top_k == 0 || config.rag.final_k == 0 {
            return Err("rag.top_k and rag.final_k must be positive".into());
        }
        if config.rag.generator != "ollama" && config.rag.generator != "none" {
            return Err("rag.generator must be \"ollama\" or \"none\"".into());
        }
        if config.rag.select_timeout_ms == 0
            || config.rag.root_refresh_secs == 0
            || config.rag.peer_stale_secs == 0
        {
            return Err(
                "rag.select_timeout_ms, rag.root_refresh_secs and rag.peer_stale_secs must be positive".into()
            );
        }
        Ok(config)
    }

    pub fn save(&self, path: &str) -> Result<(), BoxError> {
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}
