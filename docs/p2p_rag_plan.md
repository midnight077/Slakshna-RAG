# Peer-to-Peer RAG on SLAKSHNA — Implementation Plan

Written 2026-10-11. Status: Phases 0–4 implemented (see the checklist in section 10). Commands to run everything from scratch are in section 15. Decisions and their reasons are logged in `decisions.md`.

## 1. Goal

A question arrives at one node. That node retrieves relevant text from its own document store, asks every other node for relevant text from theirs, merges the results, and generates one answer.

Version 1 is deliberately naive:

- the raw query text is sent to peers;
- peers send raw text chunks back;
- every node uses the same embedding model;
- no privacy, trust weighting or reranking.

It starts with 2 nodes on 2 CPU machines and must grow to 4, 6, 8 nodes without redesign.

## 2. What the existing code gives us and what it lacks

| Need | Existing piece | Gap |
| :-- | :-- | :-- |
| Nodes find and connect to each other | `src/network/mesh.rs` (Iroh endpoint, discovery, remembered peers) | None. Reused as is. |
| Encrypted, authenticated links | Iroh QUIC, peers identified by EndpointId | None. |
| Ask one peer a question and get an answer back | Only gossip **broadcast** exists (`broadcast_update`) | Need a request/response channel. |
| Accept a query from a user | `src/api.rs` has only `GET` routes | Need `POST /rag/query`. |
| Embedding, vector search, text generation | Nothing. `ml_engine.py` is a one-shot training script | Need a long-running Python service. |
| Run on a CPU machine | The training loop in `src/main.rs` always starts and needs GPUs and Bhaskera | Need a switch to turn training off. |

## 3. Design decisions

### 3.1 Request/response over a dedicated Iroh protocol, not gossip

Gossip floods a message to the whole swarm. Using it for RAG would send every query and every returned chunk to every node, and replies would have to be matched to requests by hand.

Iroh lets one endpoint serve several protocols, each identified by a name called an ALPN. The plan adds a second protocol, `/slakshna/rag/1`, next to gossip. (Since 2026-10-18 it is `/slakshna/rag/2`, with signed roots and proofs; see `merkle_tree.md`, section 5.)

- the asking node opens a direct QUIC connection to each peer and one two-way stream per query;
- it writes the request, the peer writes the response on the same stream;
- only the two nodes involved see the data.

Gossip keeps doing membership and FL updates. No existing wire message changes, so RAG nodes and non-RAG nodes can share a federation.

APIs confirmed in the vendored `iroh-1.0.3` source: `iroh::protocol::ProtocolHandler` (`async fn accept(&self, connection)`), `Router::builder(endpoint).accept(ALPN, handler)`, `Endpoint::connect(addr, alpn)`, `Connection::open_bi()/accept_bi()/remote_id()`, `SendStream::write_all()/finish()`, `RecvStream::read_to_end(limit)`.

### 3.2 Python "sidecar" service for the ML parts

Loading an embedding model takes seconds, so the per-epoch "spawn Python, read last stdout line" pattern is too slow for queries. Instead each node runs one long-lived Python process, the **RAG sidecar**, listening on `127.0.0.1:<sidecar_port>` only. The Rust node talks to it with local HTTP.

The sidecar never talks to the network. All peer traffic goes through Rust and Iroh.

### 3.3 Components for CPU machines

| Part | Choice for v1 | Why |
| :-- | :-- | :-- |
| Embedding model | `sentence-transformers/all-MiniLM-L6-v2` (384 dimensions) | Small, fast on CPU, widely used baseline. |
| Vector store | FAISS `IndexFlatIP` over normalised vectors (exact cosine search). **Since 2026-10-21: FAISS HNSW (`IndexHNSWFlat`, inner product), decision C10.** | Exact results, no tuning, fine up to roughly 10^5–10^6 chunks. |
| Document loading | PDF via `pypdf`, plus `.txt` and `.md` | The test corpus is PDF. |
| Chunk metadata | `chunks.jsonl` next to the index | Readable, easy to debug. |
| Chunking | Fixed windows of about 200 words with 40 words of overlap | Simplest reasonable default. |
| Generator | Pluggable. Default (confirmed by the user): Ollama on localhost with `qwen2.5:1.5b` (already installed on machine 1; `llama3.2:3b` is a stronger option if disk and memory allow). Mode `none` returns merged chunks without an answer. | Only the node that receives the query generates. `none` lets retrieval be tested and evaluated without an LLM. |
| Sidecar server | FastAPI + uvicorn | Small and standard. |

### 3.4 Merging

All nodes share one embedding model, so cosine scores are directly comparable. The asking node pools local and remote chunks, removes duplicates by text hash, sorts by score, and keeps the global top `final_k`. Each response carries the embedding model name; a mismatch is logged as a warning because scores are then not comparable.

### 3.5 Who to ask

`active_peers` in `mesh.rs` lists gossip **neighbours** only. iroh-gossip keeps a small active view (about 5 by default), so from roughly 6 nodes upward a node is not a neighbour of everyone. The fan-out target list is therefore the union of live neighbours and `State::known_peers()` (every peer ever met, stored in RocksDB), minus self. Unreachable peers cost one timeout and are skipped.

## 4. Query flow

```
user ──POST /rag/query──▶ Node 1 (Rust api.rs)
                            │
            ┌───────────────┼──────────────────────────────┐
            ▼ local HTTP    ▼ Iroh stream /slakshna/rag/1   ▼ (same for nodes 3..N)
     Node 1 sidecar      Node 2 (Rust rag handler)
     POST /retrieve         │ local HTTP
                            ▼
                         Node 2 sidecar POST /retrieve
            │               │
            └───── chunks ──┴──▶ Node 1 merges, dedupes, top final_k
                                   │ local HTTP
                                   ▼
                            Node 1 sidecar POST /generate
                                   │
user ◀── answer + sources + timings ┘
```

Local retrieval and all peer requests run concurrently. Total retrieval time is the slowest peer or `peer_timeout_ms`, whichever is smaller.

## 5. Interfaces

### 5.1 Peer wire format (Iroh stream, JSON, one request and one response per stream)

```json
// request
{ "v": 1, "query_id": "uuid", "query": "raw question text", "top_k": 5 }

// response
{ "v": 1, "query_id": "uuid", "node_id": "slakshna1…",
  "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
  "chunks": [ { "chunk_id": "doc12#3", "text": "…", "score": 0.71, "source": "doc12.txt" } ],
  "retrieve_ms": 14, "error": null }
```

The sender writes the JSON and finishes its side of the stream; the receiver reads to end with a size cap (`max_message_bytes`, default 1 MiB).

### 5.2 Sidecar HTTP (localhost only)

| Route | Body | Returns |
| :-- | :-- | :-- |
| `GET /health` | — | `{ready, embedding_model, dim, num_chunks, generator}` |
| `POST /retrieve` | `{query, top_k}` | `{chunks:[{chunk_id,text,score,source}], retrieve_ms}` |
| `POST /generate` | `{query, chunks}` | `{answer, generate_ms, model}` |
| `POST /ingest` | `{path}` or `{documents:[{id,text}]}` | `{added_chunks, num_chunks}` |

### 5.3 Node HTTP API (additions to `src/api.rs`)

| Route | Purpose |
| :-- | :-- |
| `POST /rag/query` `{query, top_k?, final_k?, scope?}` | Full pipeline. `scope` is `"all"` (default), `"local"` or `"peers"`, which gives the baselines for evaluation for free. |
| `POST /rag/ingest` | Forwards to the sidecar. |
| `GET /rag/status` | Sidecar health plus the peer list that a query would fan out to. |

Response of `/rag/query`:

```json
{ "answer": "…",
  "contexts": [ { "node_id": "…", "chunk_id": "…", "score": 0.71, "text": "…", "source": "…" } ],
  "peers": [ { "endpoint_id": "…", "status": "ok|timeout|error", "chunks": 5, "rtt_ms": 38 } ],
  "timings_ms": { "local_retrieve": 12, "peer_retrieve": 41, "generate": 5300, "total": 5360 } }
```

### 5.4 Config (`[rag]` and one new training flag)

```toml
[training]
enabled = false            # NEW, default true. false = do not start the FL epoch loop.

[rag]                      # NEW section, whole section optional, default disabled
enabled = true
sidecar_port = 8600        # unique per node on a shared machine
spawn_sidecar = true       # Rust starts and stops rag_engine.py itself
corpus_dir = "./corpus-node1"
embedding_model = "sentence-transformers/all-MiniLM-L6-v2"
top_k = 5                  # chunks requested from each node
final_k = 5                # chunks kept after merging
peer_timeout_ms = 5000
max_message_bytes = 1048576
generator = "ollama"       # "ollama" | "none"
generator_model = "qwen2.5:1.5b"
generator_url = "http://127.0.0.1:11434"
```

Every field gets a serde default so existing TOML files keep loading unchanged.

## 6. File-by-file changes

### Rust

| File | Change |
| :-- | :-- |
| `Cargo.toml` | Add `reqwest` (json feature) for calling the sidecar. |
| `src/config.rs` | `RagConfig` struct with defaults; `enabled` on `TrainingConfig`; validation (ports, `top_k > 0`). |
| `src/rag/mod.rs` (new) | `RAG_ALPN`, `RagRequest`, `RagResponse`, `Chunk`; `merge_chunks()` with unit tests. |
| `src/rag/sidecar.rs` (new) | `SidecarClient` (`health`, `retrieve`, `generate`, `ingest`); spawn child process, wait for `/health`, kill on shutdown. |
| `src/rag/protocol.rs` (new) | `RagProtocol` implementing `ProtocolHandler`: accept stream, check `allowed_peers` against `connection.remote_id()`, call sidecar, reply. `query_peer()` and `fan_out()` with per-peer timeout. |
| `src/network/mesh.rs` | Register `.accept(RAG_ALPN, rag_protocol)` on the `Router` when RAG is enabled; expose a cloned `Endpoint` and a `rag_targets()` list (neighbours ∪ known peers). Confirm the endpoint's `.alpns(...)` list does not block the new ALPN. |
| `src/api.rs` | The three `/rag/*` routes; add `routing::post`. |
| `src/main.rs` | `mod rag;`; start the sidecar; wrap the epoch loop in `if config.training.enabled`. |

### Python

| File | Purpose |
| :-- | :-- |
| `rag_engine.py` (new, repo root, mirrors `ml_engine.py`) | Entry point: `python rag_engine.py serve --port … --data-dir …` and `python rag_engine.py ingest --path …`. |
| `rag/loaders.py` | Read PDF, `.txt` and `.md` files into plain text. |
| `rag/chunking.py` | Split documents into overlapping chunks. |
| `rag/embedder.py` | Load the model once; `embed(texts) -> normalised vectors`. |
| `rag/store.py` | FAISS index + `chunks.jsonl` under `{data_dir}/rag/`; `add`, `search`, `save`, `load`. |
| `rag/generator.py` | `OllamaGenerator`, `NoneGenerator`; prompt template with numbered contexts. |
| `rag/server.py` | FastAPI app with the four routes. |
| `requirements-rag.txt` | `sentence-transformers`, `faiss-cpu`, `pypdf`, `fastapi`, `uvicorn`, `httpx`, CPU `torch`. Separate from the Bhaskera environment. |
| `tests/test_rag_store.py` | Chunking, add/search round trip, persistence. |

### Configs and scripts

| File | Purpose |
| :-- | :-- |
| `examples/rag-2node/node{1,2}.toml` | Training off, RAG on, distinct ports and data dirs. |
| `scripts/rag_make_configs.py` | Generate N node configs (ports offset by index) for 4/6/8 nodes. |
| `scripts/rag_build_corpus.py` (exists) | Rebuilds the `corpus/` PDFs from Wikipedia. Extend its `NODES` table for more nodes. |
| `scripts/rag_partition_corpus.py` | Split one corpus into N per-node folders. |
| `start_rag_cluster.sh` | Like `start_cluster.sh` for the RAG configs. |
| `scripts/rag_eval.py` | Send a question set to a node, record answers, contexts and timings to CSV. |

## 7. Phases

Each phase ends with something runnable. Do not start a phase until the previous check passes.

### Phase 0 — Make the node run on a CPU machine

- Add `training.enabled` and gate the epoch loop.
- Build on both machines (`cargo build --release`; RocksDB needs `clang` and `cmake`).
- Start one node per machine with training off.
- **Check:** `GET /peers` on each machine lists the other under `connected`. This proves the networking between the two machines before any RAG code exists. If they are not on the same LAN, set `relay = true` or pin `peers = ["<EndpointId>@<ip>:<p2p_port>"]` and open the UDP port.

### Phase 1 — Sidecar alone (Python only)

- Build `rag/` and `rag_engine.py`; ingest `corpus/node1/`.
- **Check:** `curl` to `/retrieve` returns sensible chunks; `/generate` returns an answer; index survives a restart; `tests/test_rag_store.py` passes.

### Phase 2 — Single-node RAG through the Rust node

- `RagConfig`, `SidecarClient`, sidecar spawn, `/rag/query` with local retrieval only.
- **Check:** `POST /rag/query` on one node returns an answer with local contexts.

### Phase 3 — Two nodes, one machine

- `RagProtocol`, `query_peer`, `fan_out`, `merge_chunks`, router registration.
- Node 1 ingests `corpus/node1/`, node 2 ingests `corpus/node2/`. Run the questions in `corpus/questions.jsonl`.
- **Check:** a `node2_only` question asked to node 1 returns node 2's chunks in `contexts` and a correct answer. `scope: "local"` on the same question fails to find them. Killing node 2 produces `status: "timeout"` and a local-only answer rather than an error.

### Phase 4 — Two machines

- Same configs, one node per machine.
- **Check:** Phase 3 checks pass across machines with mDNS only; they also pass with mDNS off and the peer's address pinned in `network.peers`; record round-trip times.

### Phase 5 — 4, 6, 8 nodes

- Config generator, a larger corpus split across N nodes (the current one covers 2), cluster script; several nodes per machine with distinct `p2p_port`, `api_port`, `ws_port`, `sidecar_port`, `data_dir`.
- One Ollama per machine shared by its nodes; one sidecar per node (each holds its own copy of the embedding model, a few hundred MB).
- **Check:** `GET /rag/status` on any node lists N−1 targets; a query reaches all of them; latency grows with the slowest peer, not with N.

### Phase 6 — Evaluation

- Start with `corpus/questions.jsonl` (15 questions: 6 node-1-only, 6 node-2-only, 3 cross-node). It is a smoke test, too small for reported results; a larger question set and corpus are needed for those.
- Compare `scope: "local"`, `scope: "all"`, and a single node holding the whole corpus as the upper bound.
- Metrics: retrieval recall@k against the single-index baseline, answer exact match / F1, latency breakdown, bytes sent per query, all versus number of nodes.

### Later extensions (out of scope for v1)

- Send the query embedding instead of raw text; return summaries or embeddings instead of raw chunks.
- Rerank merged chunks with a cross-encoder.
- Weight or filter peers using the existing trust scores in `src/history.rs`.
- Ask only the most promising peers (routing) instead of everyone.
- Different embedding models per node, which needs score calibration or reranking.
- Use the federated LoRA-tuned model as the generator.
- Streaming answers; caching; signed responses.

## 8. Risks and known limitations of v1

- **Privacy:** peers see raw queries and the asker sees raw chunks. Accepted for v1.
- **Untrusted text:** remote chunks go straight into the prompt, so a peer can inject instructions. Mitigate later with trust scores and prompt framing; set `allowed_peers` during experiments.
- **Stale peers:** `known_peers` never forgets. Dead entries each cost one timeout per query. If this hurts at 8 nodes, filter by last-seen time.
- **Memory:** 4 nodes on one 16 GB machine means 4 embedding models plus one generator model. Use the 1.5B generator if memory is tight.
- **Generation speed:** a 3B model on CPU will take several seconds per answer. This dominates total latency and is independent of the P2P part; report it separately.
- **Port clashes:** every node on a shared machine needs four unique ports.

## 9. Decisions from the user (2026-10-11)

1. **Generator:** a local Ollama model is acceptable.
2. **Corpus:** built by Claude from Wikipedia as PDFs; see `corpus/README.md`.
3. **Network (corrected 2026-10-12):** the machines are **not** on one LAN. Machine 1 (the user's laptop) reaches machine 2 (`192.168.3.143`) only through a FortiClient VPN, so mDNS does not work between them. Node 1 pins node 2 in `network.peers` as `<EndpointId>@192.168.3.143:9001` and starts the connection; UDP from machine 1 to machine 2 is confirmed to work, the reverse direction is untested. Originally assumed: same LAN with `peers = []`. This may change, so pinned addresses (`<EndpointId>@ip:port`) and `relay = true` must keep working and are part of the Phase 4 check.
4. **Training:** RAG nodes are always training-off for now. `training.enabled` stays a plain switch so running both together remains possible later.

## 10. Status checklist

- [x] Test corpus and question set (`corpus/`)
- [x] Phase 0a — training switch (`[training] enabled`), `examples/rag-2node/` configs, two nodes connect on one machine (done 2026-10-11)
- [x] Phase 0b — the two nodes connect across the two machines (done 2026-10-12, over the VPN with a pinned address)
- [x] Phase 1 — sidecar standalone (done 2026-10-12; see section 11)
- [x] Phase 2 — single-node `/rag/query` (done 2026-10-12; see section 12)
- [x] Phase 3 — two nodes, one machine (done 2026-10-12; see section 13)
- [x] Phase 4 — two machines, over the VPN with a pinned address (done 2026-10-12; see section 14). Not tested: node 2 asking node 1, and the relay path.
- [ ] Phase 5 — 4/6/8 nodes
- [ ] Phase 6 — evaluation

## 11. Phase 1 results (2026-10-12, machine 1)

- Environment: `.venv-rag` built from `requirements-rag.txt` (CPU torch). The Rust node will have to launch the sidecar with this environment's `python`, not the Bhaskera one.
- `corpus/node1` ingests to 161 chunks in about 20 s, most of it model loading. The sidecar is ready about 13 s after start.
- Retrieval takes 20–400 ms per query on this machine.
- Generation with `qwen2.5:1.5b` takes 21–32 s per answer with 5 chunks of context. This will dominate end-to-end latency; fewer or shorter chunks would cut it.
- All 6 `node1_only` questions were answered correctly from node 1's index.
- A `node2_only` question (q07) asked of node 1 alone produced a confident wrong answer ("Allahabad") instead of "the context does not contain it". This is the local-only baseline the peer request should fix, and it shows the small model does not reliably refuse.
- Chunk 0 of every PDF starts with the source/licence line added by the corpus builder; it ranks high for many queries because it also contains the article's opening paragraph.

## 12. Phase 2 results and deviations (2026-10-12, machine 1)

- `POST /rag/query`, `POST /rag/ingest`, `GET /rag/status` work on node 1. The three sampled `node1_only` questions were answered correctly through the node, 25–29 s each, almost all of it generation.
- The node launches the sidecar itself, ingests `corpus_dir` at startup (already-indexed files are skipped), and the API comes up about 14 s after start.
- `scope` is accepted but peers are not consulted yet: `all` and `local` both use only the local index, and `peers` returns no contexts.
- When no chunks are retrieved the node skips generation and returns an empty answer. Without this the model answered from memory (and wrongly).
- Added to the plan's `[rag]` fields: `python` (interpreter with the RAG dependencies, default `.venv-rag/bin/python`). `corpus_dir` is optional. `peer_timeout_ms` and `max_message_bytes` are deferred to Phase 3.
- The sidecar gets `--exit-with-parent` and stops by itself if the node dies, including on `kill -9`.
- `reqwest` needed a TLS crypto provider installed at the top of `main()` (`rustls` with `ring`), because Iroh's feature flags apply to our copy of `reqwest` too. Without it the node panics when the first HTTP client is built.
- Machine 2 needs the same setup before Phase 4: `.venv-rag`, Ollama with `qwen2.5:1.5b`, and the `corpus/` folder.

## 13. Phase 3 results (2026-10-12, both nodes on machine 1)

Implemented as planned: `src/rag/protocol.rs` (`RagProtocol` handler, `query_peer`), concurrent fan-out in `RagService::query`, router registration and `rag_link()` in `src/network/mesh.rs`. The sidecar now starts before the network so the mesh can register the handler. `GET /rag/status` lists `peer_targets`.

All 15 questions asked of node 1 with `scope: "all"`:

| Type | Correct | Notes |
| :-- | :-- | :-- |
| `node1_only` | 6 / 6 | Peer chunks did not displace the right local ones. |
| `node2_only` | 6 / 6 | Contexts came from node 2's index. With `scope: "local"`, q07 was answered wrongly ("Allahabad"). |
| `cross_node` | 0 / 3 | See below. |

- Peer round trip on one machine: 18–28 ms. Whole retrieval step: about 55 ms. Generation: 12–18 s.
- The reverse direction works: node 2 answered a `node1_only` question using node 1's chunks.
- Node 2 killed: its entry reports `timeout` after the full 5 s `peer_timeout_ms`, and node 1 answers from local chunks only, without an error.
- Machine 2's node was still on the pre-RAG build. Asking it failed in 55 ms with "peer doesn't support any known protocol" and was reported as `error`; the query carried on. So old and new builds can share a federation.

**Cross-node questions fail, and this is a retrieval limit, not a transport bug.** Each needs one fact from each node, but the question is embedded once and its wording matches one topic much more strongly. For q13 all five merged chunks came from node 1's Chandrayaan documents; node 2's Satish Dhawan chunk scored lower and was cut by `final_k`. A global top-k by score cannot guarantee both halves are present. Options to evaluate later: reserve a share of `final_k` per node, raise `final_k`, rerank, or break the question into sub-questions. A stronger generator is also needed: with the right chunks missing, the 1.5B model guesses instead of refusing.

**Stale peers cost the full timeout.** A peer that is remembered but down delays every query by `peer_timeout_ms` (5 s). Worth fixing before 8 nodes: skip peers not seen recently, or lower the timeout.

## 14. Phase 4 results (2026-10-12, node 1 on machine 1, node 2 on machine 2)

Node 1 was asked the 9 questions that need node 2's documents, with `scope: "all"`. Node 2 is reached through the VPN at its pinned address.

| Type | Correct | Notes |
| :-- | :-- | :-- |
| `node2_only` (q07–q12) | 6 / 6 | Every context chunk came from machine 2's index. |
| `cross_node` (q13–q15) | 0 / 3 | Same retrieval limit as in section 13. |

- Peer round trip across the VPN: 30–74 ms (18–28 ms on one machine). The first query was the slowest, as it also sets up the connection.
- Baseline with `scope: "local"` on three `node2_only` questions: q08 wrong ("2008-2012"), q10 and q12 right or partly right from the model's own memory, not from node 1's documents. So a correct local-only answer does not prove retrieval worked; check `contexts`.
- Every query took an extra 5 s because node 1 still remembers `dd0020…`, the local test copy of node 2 from Phase 3, which is no longer running. Retrieval time was 5001 ms instead of about 60 ms. This is the stale-peer problem from section 13 and is the first thing to fix in Phase 5.
- Cross-node detail: for q13 all five chunks came from node 1; for q15 all five came from node 2; for q14 four came from node 1 and the one from node 2 was about the wrong person. In each case one half of the needed evidence was missing.
- mDNS-only discovery does not apply to this setup (see section 9, item 3). Not yet tested: asking node 2 for a node 1 answer across machines, and the relay path.

## 15. Running it from scratch

These steps take a fresh Linux machine (Ubuntu-like) to a working two-node setup. Do sections 15.1–15.4 on **both** machines, then start the nodes. Run everything from the repository folder.

### 15.1 Install the tools (once per machine)

```bash
sudo apt install -y git build-essential clang cmake pkg-config curl   # compilers; RocksDB needs clang and cmake
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh        # Rust and cargo
curl -LsSf https://astral.sh/uv/install.sh | sh                       # uv, the Python installer
curl -fsSL https://ollama.com/install.sh | sh                         # Ollama, runs the answer-writing model
```

Open a new terminal afterwards so `cargo` and `uv` are found.

### 15.2 Get the code

```bash
git clone git@github.com:midnight077/Slakshna-RAG.git Slakshna
cd Slakshna
git checkout feat/p2p-rag
```

The Bhaskera submodule is not needed for RAG nodes, because training is off.

### 15.3 Build the node program

```bash
cargo build --release          # several minutes the first time; produces target/release/iiitd
cargo test --release           # optional: Rust unit tests
```

### 15.4 Set up the Python helper and the model

```bash
uv venv .venv-rag --python 3.11
uv pip install --python .venv-rag/bin/python --torch-backend cpu -r requirements-rag.txt
.venv-rag/bin/python -m unittest tests/test_rag_store.py      # optional: should print OK

ollama pull qwen2.5:1.5b       # about 1 GB
ollama list                    # should list qwen2.5:1.5b; if Ollama is not running: ollama serve
```

The PDFs in `corpus/` come with the repository. The embedding model (about 90 MB) downloads by itself the first time a node starts, so the first start needs internet access.

### 15.5 Start node 2 (machine 2)

```bash
./target/release/iiitd --config examples/rag-2node/node2.toml
```

Leave it running. Note its EndpointId, which node 1 needs:

```bash
curl localhost:8555/status       # the "endpoint_id" field
```

### 15.6 Point node 1 at node 2, then start node 1 (machine 1)

In `examples/rag-2node/node1.toml`, the `peers` line must hold node 2's EndpointId and address:

```toml
peers = ["<node 2 endpoint_id>@<machine 2 IP>:9001"]
```

It already holds the current values (`313823…@192.168.3.143:9001`). They change only if `data-rag-node2/` is deleted or machine 2's address changes. Connect the VPN, then:

```bash
./target/release/iiitd --config examples/rag-2node/node1.toml
```

### 15.7 Check and ask

```bash
curl localhost:8545/status         # "peers": 1
curl localhost:8545/rag/status     # "peer_targets" lists node 2's EndpointId

curl -s localhost:8545/rag/query -H 'content-type: application/json' \
  -d '{"query": "Where was Abdul Kalam born?"}' | python3 -m json.tool
```

The reply holds `answer`, `contexts` (passages used and which node they came from), `peers` (each peer asked and whether it replied) and `timings_ms`. Add `"scope": "local"` or `"scope": "peers"` to search only this node or only the others. Test questions with expected answers are in `corpus/questions.jsonl`. Node 2 can be asked the same way on port `8555`.

Stop a node with `Ctrl+C` in its terminal, or `pkill -x iiitd`.

### 15.8 Both nodes on one machine

Skip 15.6's VPN step and set node 1's `peers = []`: the two nodes find each other through mDNS on the same machine. Start both commands in two terminals.

### 15.9 Common problems

| Symptom | Cause and fix |
| :-- | :-- |
| `"peers": 0` | Node 2 not running, VPN down, or wrong id/address in `peers`. UDP port 9001 on machine 2 must be reachable. |
| "Address already in use" | A node is already running on that machine: `pkill -x iiitd`. |
| Empty or failed answer | Ollama is not running (`ollama serve`) or the model is missing (`ollama pull qwen2.5:1.5b`). |
| Node exits waiting for the sidecar | `.venv-rag` missing or incomplete; redo 15.4. The first start can take a few minutes while the embedding model downloads. |
| Every question takes 5 s longer | A remembered peer is no longer running (see section 14). |
| New PDFs not used | Put them in the node's `corpus_dir` and restart the node; already-indexed files are skipped. |
