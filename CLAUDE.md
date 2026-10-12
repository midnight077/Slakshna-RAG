# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

SLAKSHNA is a peer-to-peer federated learning (FL) system. Each participant runs one **node**:

- a **Rust binary** (`iiitd`, built from `src/`) that handles networking, identity, storage and an HTTP API;
- a **Python ML engine** (`ml_engine.py`) that the Rust node launches once per epoch to train a LoRA adapter, compress the weight change ("delta"), and merge deltas received from peers.

There is no central server and no bootstrap node. Nodes with the same `[federation] id` find each other and exchange model deltas over an Iroh QUIC gossip mesh.

Related docs:

- `docs/explanation.md` — plain-language walkthrough of the whole project (the user's preferred level of explanation).
- `docs/p2p_rag_plan.md` — the implementation plan for the peer-to-peer RAG pipeline. **Read it before doing any RAG work** and keep its status checklist up to date.
- `docs/AGENTS.md` — existing repository guidelines (style, commits, testing).
- `docs/delta_wire_format.md` — the sparse int8 delta format.
- `walkthrough280726.md` — step-by-step description of one training epoch inside `ml_engine.py`.

## Commands

```bash
git submodule update --init --recursive      # Bhaskera/ is an empty directory until this runs
cd Bhaskera && bash setup.sh && source bhaskera-activate.sh && cd ..
bash setup.sh                                # Python deps, runtime dirs, release build

cargo check                                  # fast validation of Rust changes
cargo build --release                        # produces target/release/iiitd
cargo test --release                         # Rust unit tests (identity.rs, rag/mod.rs); --release reuses the built dependencies
cargo fmt --check

python -m unittest tests/test_delta_transport.py   # delta wire-format tests (needs torch)
python ml_engine.py <my_id> <peer_id...>            # one ML epoch with no networking

# RAG sidecar (Python, separate environment from Bhaskera)
uv venv .venv-rag --python 3.11
uv pip install --python .venv-rag/bin/python --torch-backend cpu -r requirements-rag.txt
.venv-rag/bin/python -m unittest tests/test_rag_store.py
.venv-rag/bin/python rag_engine.py ingest --data-dir ./data-rag-node1 --path corpus/node1
.venv-rag/bin/python rag_engine.py serve --data-dir ./data-rag-node1 --port 8600   # needs Ollama running

./target/release/iiitd --config node1.toml   # run one node
./target/release/iiitd --config examples/rag-2node/node1.toml   # RAG node: no training, starts its own sidecar
curl localhost:8545/rag/query -H 'content-type: application/json' -d '{"query": "..."}'
./start_cluster.sh 2                         # 2, 4 or 6 local nodes from examples/<N>node/, logs in logs/
./stop_cluster.sh
```

Always run from the repository root: the node spawns `python ml_engine.py` with the current directory as its working directory, and the scripts use relative paths.

## Architecture

```
            HTTP client / dashboard
                     │
        ┌────────────▼────────────┐
        │ src/api.rs  (Axum)      │  GET /status /peers /updates /leaderboard /ws
        ├─────────────────────────┤
        │ src/main.rs             │  epoch loop: stage peer deltas → run Python → record → broadcast → wait
        │ src/history.rs          │  in-memory per-peer log of UpdateRecord (ModelUpdate | PeerReview)
        │ src/state.rs            │  RocksDB: keypair, round counter, remembered peers
        │ src/identity.rs         │  Ed25519 keypair, NodeId, Iroh transport seed
        │ src/config.rs           │  TOML config structs and validation
        ├─────────────────────────┤
        │ src/network/mesh.rs     │  Iroh endpoint, discovery (mDNS/DHT/DNS/relay), gossip topic
        │ src/network/mod.rs      │  P2PMessage wire enum, Network trait
        └────────────┬────────────┘
                     │ spawns once per epoch, reads last stdout line as JSON
        ┌────────────▼────────────┐
        │ ml_engine.py            │  train (Bhaskera/Ray) → delta → sparsify+quantize → aggregate → trust
        │ federated_communication/│  sparsification, int8 quantization, DP/clipping, aggregation
        │ Bhaskera/ (submodule)   │  distributed LLM training framework
        └─────────────────────────┘
```

The RAG sidecar (`rag_engine.py`, package `rag/`) is a separate long-running local HTTP service: PDF loading, chunking, embeddings, a FAISS index in `{data_dir}/rag/`, and answer generation through Ollama. When `[rag] enabled = true` the node launches it at startup (`src/rag/sidecar.rs`), and `src/rag/mod.rs` runs the query pipeline behind `POST /rag/query`, `POST /rag/ingest` and `GET /rag/status`. Peers are asked for chunks over a second Iroh protocol, `/slakshna/rag/1` (`src/rag/protocol.rs`): one QUIC stream per query, JSON request and response, sent to every live or remembered peer at once with a per-peer timeout. A peer that is down or runs a build without RAG is reported in the response's `peers` list and skipped.

### One epoch (`src/main.rs`)

1. Sleep until the next wall-clock multiple of `training.epoch_duration_secs`, so independently started nodes align.
2. Write each peer's latest `compressed_delta` to `{data_dir}/network_deltas/{peer_id}_delta.b64`.
3. Run `python ml_engine.py <my_id> <peer_ids...>` with settings passed as environment variables (`IIITD_DATA_DIR`, `SLAKSHNA_*`, `CUDA_VISIBLE_DEVICES`).
4. Parse the **last line of stdout** as `MLEngineOutput` JSON (`weights`, `model_hash`, `validation_score`, `metadata`, `compressed_delta`, optional `round`, `total_epochs`, `is_finished`).
5. Record a `ModelUpdate` plus one `PeerReview` per peer, then gossip them.
6. Wait until `expected_peers` nodes have reported or `sync_deadline_secs` passes.
7. Exit the process when the engine reports `is_finished`.

### Networking (`src/network/mesh.rs`)

- The gossip topic is `sha256(federation.id)`; the mDNS service label is derived from the same id. A mismatched id silently puts nodes in separate swarms.
- Peers are found through mDNS (LAN), the mainline DHT, n0 pkarr/DNS, entries in `network.peers`, and every peer remembered in RocksDB (re-offered every 60 s).
- `network.peers` entries are Iroh EndpointIds, optionally `<EndpointId>@host:port` to pin an address.
- Only `P2PMessage::NewUpdate` is sent or handled. The other variants are unused, and messages that fail to deserialize are dropped without a log line.
- Gossip messages are capped at 10 MiB; `compression.max_payload_bytes` must stay under that after base64 expansion.

## Things that are easy to get wrong

- **Two identities per node.** `NodeId` (`slakshna1…`, bech32 of the public-key hash) replaces `config.node.id` at startup and keys all history and file names. The Iroh `EndpointId` (hex) is what goes in `network.peers` and `allowed_peers`. Both derive from the keypair in `{data_dir}/rocksdb`; deleting `data_dir` changes both.
- **stdout is a protocol.** Python diagnostics must go to `stderr`; any stray `print` after the final JSON breaks the epoch.
- **The node runs `python`, not `python3`.** The virtual environment must be active so `python` resolves.
- **No default config.** The binary falls back to `config.toml`, which is not in the repo. Always pass `--config`.
- **Update history is memory-only.** RocksDB stores the keypair, round number and known peers, not the update log. A restart loses received deltas.
- **Record signatures are placeholders** (`node_signature_<epoch>`), not real Ed25519 signatures. Only the content hash is checked.
- **`allowed_peers` filters on `delivered_from`**, the neighbour that forwarded a gossip message, not its original author.
- **Wire format.** Renaming `RecordKind` or `P2PMessage` variants or fields breaks compatibility; every node in a federation must run the same build.
- **Hardcoded paths.** `node_template.yaml` points at `/mnt/disk1/slakshna/...`; `setup.sh` rewrites that prefix inside `ml_engine.py` with `sed`, which shows up as a diff in a tracked file.
- **`.gitignore` ignores `/docs`, all dotfiles and `Cargo.lock`.** The tracked files under those were force-added; new files in `docs/` need `git add -f`.
- **The training loop runs unless `[training] enabled = false`.** It defaults to on and needs a GPU-class machine plus the Bhaskera environment. The RAG configs in `examples/rag-2node/` turn it off.

## Leftovers from an earlier project

`scripts/benchmark*.py`, `scripts/test_shield_*.py`, `scripts/test_dcp.py`, `deploy.sh`, `Dockerfile` and `docker-compose.yml` reference endpoints and files that no longer exist (transaction nonces, a `config.toml`, an nginx directory). Treat them as stale unless the user says otherwise. `iroh-blobs`, `rayon` and several other dependencies in `Cargo.toml` are not used by `src/`.

## Conventions

- Rust: `cargo fmt`, `snake_case` functions, `CamelCase` types. Python: four spaces, `snake_case`.
- Commits use Conventional Commit prefixes (`feat:`, `docs:`, `refactor:`).
- Runtime output (`data-node*/`, `logs/`, `ml_models/`, `ml_states/`, `hf_cache/`, `config_*.yaml`) is not source and must not be committed.
- Changes inside `Bhaskera/` are committed and pushed in the submodule first (branch `Slakshna`), then the parent repo's pointer is updated.

## Working with the user

The user is new to Rust, Python and networking. Explain changes in plain language, define terms on first use, and prefer the simplest design that works before layering on refinements.
