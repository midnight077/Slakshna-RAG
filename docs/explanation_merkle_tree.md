# Verifiable, cluster-routed retrieval explained

This document explains the work done on branches `merkle-tree` and `centroid-directory`: Phases M1–M8 of `docs/merkle_tree.md`. It assumes you know the basic peer-to-peer RAG setup from `docs/p2p_rag_plan.md`: two nodes, each with a Python sidecar that holds a document index, and one node asking the other for text chunks.

It covers:
- what was built and why;
- how a document becomes a signed fingerprint;
- how a question travels between nodes and what is checked at each step;
- how to run and test everything;
- how to read the outputs;
- what the measurements showed, and what is still not protected.

Phase M9 (evaluation of all three retrieval modes on the HPC) is not done yet.

---

## 1. The problem

Before this work, a query in the peer-to-peer RAG system worked like this:

1. Node 1 sends the question to **every** peer it knows.
2. Each peer searches its own index and sends back its best 5 text chunks, each with a similarity **score**.
3. Node 1 pools all chunks, sorts them by score, keeps the best 5 and gives them to the language model to write the answer.

This has two weaknesses:

- **Node 1 must trust whatever comes back.** A peer could send invented text, or claim a score of 0.99 for an irrelevant chunk so that it wins the sort. Node 1 had no way to tell.
- **Every peer gets every question,** and every peer sends chunks, even peers whose documents have nothing to do with the question. A peer that is switched off costs a 5-second timeout on every question.

The work in this branch addresses both:

- Each node **commits** to its whole document collection by publishing one short signed fingerprint. Anything it sends later must come with a proof that it belongs to that fingerprint. Node 1 also recomputes every score itself.
- Each node groups its chunks into **topics**. A question first asks peers for their closest topics; only peers whose topics are good enough are then asked for chunks.
- Peers that are down are noticed in the background and skipped.

---

## 2. Terms used in this document

| Term | Meaning |
| :-- | :-- |
| **Chunk** | A piece of a document, about 200 words. The unit that is searched and sent between nodes. Identified by a `chunk_id` such as `chandrayaan_3.pdf#4` (document name and position). |
| **Embedding** | A list of 384 numbers produced by the model `all-MiniLM-L6-v2` from a piece of text. Texts with similar meaning get similar lists. Every embedding is scaled to length 1. |
| **Cosine similarity / score** | How similar two embeddings are, computed as their **dot product** (multiply the numbers pairwise and add them up). With length-1 vectors this gives a value from −1 to 1; for this model, related texts typically score 0.4–0.8. |
| **K-means clustering** | A method that sorts many vectors into `k` groups (**clusters**) of vectors that are close to each other. Here it sorts each node's chunks into topic groups. |
| **Centroid** | The average embedding of all chunks in one cluster, scaled to length 1. A 384-number summary of one topic. |
| **Hash (SHA-256)** | A function that turns any bytes into a 32-byte fingerprint. Changing even one bit of the input gives a completely different fingerprint, and it is practically impossible to find two inputs with the same fingerprint. Written as 64 hexadecimal characters. |
| **Merkle tree** | A way to fingerprint a list of items with one hash: hash each item, then hash pairs of hashes together, level by level, until one hash is left. |
| **Root** | The single hash at the top of a Merkle tree. It fingerprints every item below it. |
| **Proof** | The list of "neighbour" hashes needed to recompute the root starting from one item. A few hashes long, whatever the size of the collection. |
| **Commitment** | A node's root plus a few numbers describing its index (version, chunk count, cluster count, embedding size, model name). |
| **Signature (Ed25519)** | A 64-byte value that only the holder of a **secret key** can produce for a given message, and that anyone with the matching **public key** can check. |
| **EndpointId** | A node's network address in Iroh. It *is* the node's public key, written as 64 hex characters (e.g. `dd002001c6d7…`). |
| **Signed root** | A commitment plus the node's signature over it. |
| **Version** | A number attached to each root, set to the time in milliseconds when the root was built. It only goes up, so a newer root can be told apart from an older one. |
| **Registry** | Node 1's in-memory notebook about its peers: their latest accepted signed roots, when they last answered, and any rule breaks seen. |
| **Sidecar** | The Python program next to each node that holds the index, makes embeddings and talks to the language model. Only the node on the same machine talks to it. |
| **Directory** | A node's local, verified copy of every peer's centroids, fetched once per root version. Used by the third retrieval mode, `directory` (section 8.3). |
| **ALPN** | The name of a protocol on an Iroh connection. Nodes that don't share an ALPN refuse to connect. The RAG protocol is now `/slakshna/rag/2`. |

---

## 3. The pieces and where they live

```
 Machine of one node
 ┌──────────────────────────────────────────────────────────────────────────┐
 │  Rust node (target/release/iiitd)                                        │
 │   src/api.rs          HTTP API: /rag/query /rag/ingest /rag/status        │
 │                       /rag/debug/hello                                   │
 │   src/rag/mod.rs      RagService: own signed root, query() entry point   │
 │   src/rag/retrieval.rs  two_round and flat pipelines, all checks         │
 │   src/rag/registry.rs   peers' roots, flags, who to ask                  │
 │   src/rag/protocol.rs   wire format v2, answering peers' requests        │
 │   src/rag/merkle.rs     hashing, proof checking, signing                 │
 │   src/rag/sidecar.rs    HTTP client for the Python sidecar               │
 │           │ local HTTP (127.0.0.1)                ▲ QUIC, /slakshna/rag/2 │
 │           ▼                                       │ to and from peers     │
 │  Python sidecar (rag_engine.py serve)                                    │
 │   rag/server.py       HTTP routes                                         │
 │   rag/store.py        index, chunks, clusters, commitment on disk        │
 │   rag/clustering.py   k-means                                            │
 │   rag/merkle.py       hashing, tree building, proofs                     │
 │   rag/embedder.py     text → embedding                                   │
 │   rag/generator.py    answer writing via Ollama                          │
 └──────────────────────────────────────────────────────────────────────────┘
```

The division of work:
- **Python builds.** The sidecar chunks and embeds documents, clusters them, builds the Merkle tree and produces proofs.
- **Rust checks and signs.** The Rust node signs its own root, talks to other nodes, checks every proof it receives and decides what to trust.

The same hash rules are written twice, once in each language. A shared test file makes sure the two produce identical bytes (section 12.3).

### Files added or changed in this branch

| File | What it does |
| :-- | :-- |
| `rag/clustering.py` (new) | `choose_k` picks the number of clusters; `cluster` runs k-means. |
| `rag/merkle.py` (new) | Hash functions, tree building, proofs, the `Commitment` class, the signed-message layout. |
| `rag/store.py` | Keeps clusters and the commitment, saves and checks them, new search methods. |
| `rag/server.py` | New sidecar routes `/commitment`, `/embed`, `/centroids`, `/search_clusters`; proofs on `/retrieve`. |
| `rag_engine.py` | `--n-clusters` option; logs the root at startup. |
| `scripts/rag_merkle_vectors.py` (new) | Writes the shared test vectors file. |
| `tests/fixtures/merkle_vectors.json` (new) | The shared test vectors (inputs and every expected hash). |
| `tests/test_rag_merkle.py`, `tests/test_rag_server.py` (new), `tests/test_rag_store.py` | Python tests. |
| `src/rag/merkle.rs` (new) | Rust hashing, proof checking, signing and signature checking. |
| `src/rag/registry.rs` (new) | The root registry and the rule for which peers to ask. |
| `src/rag/retrieval.rs` (new) | The two-round and flat query pipelines. |
| `src/rag/protocol.rs` | Wire format version 2 and the code that answers peers. |
| `src/rag/mod.rs` | Signed own root, `hello`, new output fields, `query()`. |
| `src/rag/sidecar.rs` | Client methods for the new sidecar routes; passes `--n-clusters`. |
| `src/config.rs` | New `[rag]` settings and their checks. |
| `src/identity.rs` | `Keypair::transport_secret_key()`, the key used for Iroh and for signing roots. |
| `src/main.rs` | Signs the root at startup, starts the background refresh. |
| `src/api.rs` | `own_root` and `registry` in `/rag/status`; re-sign after `/rag/ingest`; `/rag/debug/hello`. |
| `scripts/rag_run_questions.py` (new) | Asks a node all 15 test questions and saves the full responses. |
| `src/rag/directory.rs` (new, M8) | The centroid directory: checking, storing and scoring peers' centroids; fetching directories. |
| `src/rag/adversary.rs` (new, M7) | Simulated attacks (section 15). |

---

## 4. Building the index and its fingerprint (sidecar)

This happens when documents are ingested, usually at node startup (`corpus_dir` in the config), or later through `POST /rag/ingest`.

### 4.1 Steps

1. **Chunk and embed** (unchanged from before). Each new document is split into chunks of about 200 words, and each chunk gets an embedding. The embeddings go into the FAISS index (the search structure) and also into an in-memory array `vectors`. The index is an **HNSW** index: a graph linking each embedding to its nearest neighbours, which search follows instead of comparing the query with every chunk. It is approximate in principle, but with its settings (`M = 32`, `efConstruction = 200`, `efSearch = 128`) it returned exactly the same top 5 as exact search for all 365 test queries on both nodes. Only the plain top-k search (local retrieval, flat mode, `/retrieve`) uses it. Cluster scoring works directly on `vectors`, and the Merkle root does not cover the index file.
2. **Cluster** (`rag/clustering.py`). After all new documents are added, **all** chunks are clustered again, not just the new ones.
   - The number of clusters is `k = round(√number_of_chunks)`, kept between 1 and 64, unless `n_clusters` is set in the config. For node 1's 161 chunks, `k = 13`.
   - FAISS k-means runs with `spherical=True`, so centroids have length 1 and `dot(query, centroid)` is a cosine similarity.
   - Each chunk is assigned to its nearest centroid. Clusters that end up empty are removed, and the rest are renumbered 0, 1, 2, … with no gaps.
3. **Build the Merkle tree** (`rag/merkle.py`, class `Commitment`). Section 5 describes it.
4. **Set the version** to the current time in milliseconds, but always at least one more than the previous version.
5. **Save** everything (section 4.2).

Re-ingesting a document that is already in the index does nothing: no new clustering, no new root, no new version.

### 4.2 Files in `{data_dir}/rag/`

| File | Content |
| :-- | :-- |
| `index.faiss` | The FAISS HNSW search index (all chunk embeddings and the neighbour graph). |
| `chunks.jsonl` | One line per chunk: `chunk_id`, `source`, `text`, and now `cluster`. |
| `centroids.npy` | The centroids, a `[k, 384]` array of 32-bit floats. |
| `meta.json` | Embedding model, dimension, ingested documents, and now `commitment`: `root`, `version`, `num_clusters`, `n_clusters_setting`. |

### 4.3 Loading checks

When the sidecar starts, it rebuilds the Merkle tree from `chunks.jsonl` and `centroids.npy` and compares the root with the one in `meta.json`:

- **They match:** the sidecar starts normally.
- **They differ:** it refuses to start with `index files in … were changed on disk`. Someone edited the files, or a save was cut short. To fix it, delete the `rag/` folder and ingest again.
- **`meta.json` has no `commitment`:** the index was built before this work. It is clustered and committed once, and the sidecar logs `Index in … predates clustering; clustering it now.`
- **`n_clusters` differs from the stored `n_clusters_setting`:** the index is re-clustered, and you get a new root and version.
- **`index.faiss` is in the old exact format (`IndexFlatIP`):** it is rebuilt as HNSW from the same vectors and saved, and the sidecar logs `Index in … uses exact search; rebuilding it as HNSW.` The root and version do not change.

---

## 5. The Merkle tree in detail

The exact bytes are fixed in `docs/merkle_tree.md`, section 2. Python (`rag/merkle.py`) and Rust (`src/rag/merkle.rs`) both follow them.

### 5.1 Three kinds of hash

```
leaf(chunk)       = SHA256( 0x00 ‖ len(id) ‖ id ‖ len(text) ‖ text )
cluster(c)        = SHA256( 0x01 ‖ cluster_id ‖ dim ‖ centroid_bytes ‖ subtree_root )
inner(left,right) = SHA256( 0x02 ‖ left ‖ right )
```

- `‖` means "followed by".
- Lengths, `cluster_id` and `dim` are written as 4-byte big-endian numbers.
- `id` and `text` are the UTF-8 bytes of the chunk id and chunk text, exactly as stored in `chunks.jsonl`.
- `centroid_bytes` are the 384 centroid values as 4-byte little-endian floats (1536 bytes).

**Why the first byte (0x00, 0x01, 0x02):** it marks what kind of thing is being hashed, so a chunk's hash can never be passed off as an inner node's hash or a cluster's hash.

**Why the lengths:** without them, the id `ab` with text `c` and the id `a` with text `bc` would produce the same bytes.

### 5.2 Shape of the tree

There are two levels of tree, built with the same rule:

```
tree_root(list):
    empty list            → 32 zero bytes
    repeat until one is left:
        pair neighbours left to right and replace each pair by inner(left, right);
        if the count is odd, the last one moves up unchanged
```

- **Lower level, one tree per cluster:** the leaves are the chunk hashes of that cluster, in `chunks.jsonl` order. Its root is the cluster's **subtree root**.
- **Upper level:** the leaves are the cluster hashes `cluster(0), cluster(1), …`. Its root is the node's **root**.

The centroid is inside `cluster(c)`, so the root covers the centroids as well as the chunk texts.

### 5.3 Proofs

A proof is a list of steps `{"h": "<hex hash>", "left": true|false}`. To check one, start from the item's hash and, for each step, combine it with the given neighbour hash:

```
h = starting hash
for each step:
    if step.left:  h = inner(step.h, h)     # neighbour is on the left
    else:          h = inner(h, step.h)     # neighbour is on the right
the result must equal the expected root
```

A chunk needs two proofs:
- `chunk_proof`: from `leaf(chunk)` up to its cluster's subtree root;
- `cluster_proof`: from `cluster(c)` up to the node's root.

### 5.4 Worked example (from the test vectors file)

The test file has six chunks in three clusters. Cluster 0 holds three chunks: `chandrayaan_3.pdf#0`, `gaganyaan.pdf#1` and `raman.pdf#5`. Hashes are shortened to 8 characters here.

**Cluster 0's tree:**
```
level 0:  b8500184   9ebfa6b2   e906fbfd       (leaf hashes of the three chunks)
level 1:  ca2f6cfe = inner(b850…, 9ebf…)   e906fbfd (odd one, moves up)
level 2:  d2c6f0e8 = inner(ca2f…, e906…)   → subtree root of cluster 0
```

| Chunk | `chunk_proof` | Folding it |
| :-- | :-- | :-- |
| `chandrayaan_3.pdf#0` | `[(9ebfa6b2, right), (e906fbfd, right)]` | `b850 → inner(b850, 9ebf) = ca2f → inner(ca2f, e906) = d2c6` |
| `raman.pdf#5` | `[(ca2f6cfe, left)]` (one step, because it moved up a level unchanged) | `e906 → inner(ca2f, e906) = d2c6` |

**Upper tree:**
- The cluster hashes are `8ec3c1a8` (cluster 0), `ce783f3d` (cluster 1) and `0fb53eb2` (cluster 2).
- Pairing gives `2da6961b = inner(8ec3…, ce78…)`, with `0fb5…` moving up.
- The root is `71f42f9b = inner(2da6…, 0fb5…)`.
- Cluster 0's `cluster_proof` is `[(ce783f3d, right), (0fb53eb2, right)]`.

If one character of `raman.pdf#5`'s text changes, its leaf hash changes. Folding then no longer gives `d2c6…`, and the chunk is rejected.

### 5.5 What a cluster entry looks like

Whenever a node sends something about a cluster, it sends this **cluster entry**:

```json
{ "cluster_id": 9, "size": 17,
  "centroid": "<1536 bytes as 3072 hex characters>",
  "subtree_root": "<64 hex>",
  "cluster_proof": [ {"h": "<64 hex>", "left": false}, … ],
  "best_chunk_score": 0.697 }
```

- `best_chunk_score` is only present in round-1 answers (section 8.1).
- The receiver checks the entry by computing `cluster(cluster_id, dim, centroid, subtree_root)` and folding `cluster_proof` up to the signed root.

---

## 6. Signed roots (Rust)

### 6.1 What is signed

```
"slakshna-rag-root-v1" ‖ 0x00 ‖ root ‖ version ‖ num_chunks ‖ num_clusters ‖ dim ‖ len(model) ‖ model
```

- `version` and `num_chunks` are 8-byte big-endian numbers.
- `num_clusters`, `dim` and `len(model)` are 4-byte big-endian numbers.

The signed root as JSON (this is node 1's, from `/rag/status`):

```json
{ "root": "802f7d373e43cbbf604a4edd7851fdae4af0c0157d1968b24e37a14d2acc91b3",
  "version": 1792345080027, "num_chunks": 161, "num_clusters": 13, "dim": 384,
  "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
  "signature": "7368462eb0e534bc89322dc0…" }
```

### 6.2 Which key signs it

The node signs with its **Iroh transport key**, the same key its EndpointId comes from (`Keypair::transport_secret_key()` in `src/identity.rs`). So:

- a peer can check the signature using nothing but the EndpointId it already dials;
- the QUIC connection has already proven that the answer comes from the holder of that key;
- anybody else can check the signature too, which matters for evidence (section 7.2).

### 6.3 When a node signs

- **At startup:** after the sidecar is ready and the corpus is ingested, but before the network starts. The first `hello` from any peer already gets a root. Log line: `🔏 Signed RAG root 802f7d373e43cbbf… (version …, 161 chunks in 13 clusters)`.
- **After every `POST /rag/ingest`.**
- **Whenever the sidecar answers a peer under a version the node has not signed yet**, for example after an ingest that went straight to the sidecar.

---

## 7. Talking to peers: protocol version 2

### 7.1 Message kinds

Every request is one JSON object on one QUIC stream, answered by one JSON object. The `kind` field says what it is. `query_id` is a random id; the answer must repeat it.

| kind | Request | Answer |
| :-- | :-- | :-- |
| `hello` | `{"v":2,"kind":"hello","query_id":"…"}` | the peer's signed `root` |
| `centroids` (round 1) | `+ "query": "…", "n": 3` | `root_version`, `centroids`: up to `n` cluster entries with `best_chunk_score` |
| `chunks` (round 2) | `+ "query", "root_version", "clusters": [9,4,2], "per_cluster": 3` | `root_version`, `clusters` (entries), `chunks` (with `cluster_id` and `chunk_proof`) |
| `search` (flat) | `+ "query", "top_k": 5` | `root_version`, `clusters`, `chunks` (like v1, plus proofs) |
| `directory` (M8) | `{"v":2,"kind":"directory","query_id":"…"}` | `root_version`, `clusters`: **every** cluster entry of the current root (about 46 KB for 13 clusters). Answered only by nodes with `[rag] directory = true`; others answer with an error. |

Every answer also has `node_id`, `embedding_model` and `error` (null when all is well).

Rules for the peer that answers:
- **Caps:** at most 10 centroids, 10 clusters, 10 chunks per cluster, and 50 chunks in `search`, whatever is asked.
- **Stale root:** a `chunks` request that names an old `root_version` gets `"error": "stale_root"` plus the peer's current signed root. Cluster numbers can change whenever a node re-clusters, so serving an old number could return the wrong chunks.
- **Unreadable request:** answered with `kind: "error"`.

**Compatibility:** v1 nodes (`/slakshna/rag/1`) and v2 nodes cannot talk. A v1 node shows up as `error: … peer doesn't support any known protocol` and is skipped. This is exactly what happened when machine 2 was still on an old build.

### 7.2 The root registry and the background refresh

Every node runs a background task (`run_root_refresh` in `src/rag/mod.rs`). Five seconds after startup, and then every `root_refresh_secs` (60), it sends `hello` to every peer target at once, each with a timeout of `select_timeout_ms` (2 s). The results go into the registry (`src/rag/registry.rs`).

**Rules for a received root:**

| What arrived | Result | What is kept |
| :-- | :-- | :-- |
| First root from this peer, or a higher version | `accepted` | the new root; `first_seen` = now |
| The same root as before | `unchanged` | — |
| Signature does not match the peer's EndpointId | `bad_signature` flag | the old root |
| A lower version than the stored one | `rollback` flag | the old root |
| Same version, different content | `equivocation` flag | the old root; both signed roots stored as **evidence** |

Two different roots signed under one version are proof that the peer lied: both carry its signature, so anybody can check them. A peer flagged `equivocation` is not asked any more (status `refused`).

**Which peers a query goes to:**
- a peer never tried yet;
- a peer that answered within `peer_stale_secs` (180 s) **and** whose most recent contact did not fail.

All other peers are reported as `skipped_stale` and cost nothing. The background refresh keeps trying them, and the first successful answer brings a peer back. A peer that has just died therefore costs at most one timed-out query. (The "most recent contact did not fail" part was your decision J23.)

**Where the targets come from:** `rag_link()` in `src/network/mesh.rs` lists the live gossip neighbours plus every peer ever remembered in RocksDB. Old test identities therefore stay in the list as `failing` peers (e.g. `994f71…`, `dd0020…` when not running).

The registry lives in memory only. After a restart, every peer's first root is accepted as is again.

---

## 8. One question, step by step

`POST /rag/query` on node 1 calls `RagService::query` (`src/rag/mod.rs`). The mode is `two_round` unless the config or the request says `"retrieval": "flat"`.

### 8.1 two_round (default)

```
node 1                                           peers
  │ (a) local search: own top_k (5) chunks              (runs alongside everything below)
  │ (b) embed the question → q   (own sidecar /embed)
  │ (c) ROUND 1 ──── centroids {query, n=3} ─────────▶ every peer that should be asked
  │ ◀──────── 3 cluster entries + best_chunk_score ───
  │ check each entry against that peer's signed root
  │ score each verified centroid: sim = dot(q, centroid)
  │ SELECT: best max_clusters (5) over all peers; drop sim < 0.2; always keep the best one
  │ ROUND 2 ──── chunks {clusters it won, per_cluster=3} ▶ only peers that won a cluster
  │ ◀──────── chunks + chunk proofs ────────────────
  │ check each chunk's proof against the subtree root verified in round 1
  │ RESCORE: embed all received chunks (own sidecar), score = dot(q, chunk)
  │ MERGE local + remote chunks, sort by score, keep final_k (5)
  │ GENERATE the answer from those 5 chunks (Ollama)
```

**Round 1 checks, per peer** (`round1` in `src/rag/retrieval.rs`):
1. The answer names a `root_version`. Node 1 looks for that version in its registry. If the registry doesn't have it, node 1 sends one `hello` right away and looks again. If it still doesn't match, the peer gets status `no_root` and is ignored for this question.
2. Each cluster entry is checked against that root. If one fails, that centroid is dropped and the peer is flagged `bad_centroid_proof`.
3. Node 1 keeps at most `centroids_per_peer` entries, and counts a cluster offered twice only once.
4. Node 1 computes the similarity itself. It never uses a similarity number from the peer.

**Selection** (`select_clusters`):
- All verified centroids from all peers are ranked together. With `select_by = "best_chunk"`, the ranking uses the peer's claimed best chunk score instead.
- The top `max_clusters` are kept, those below `min_centroid_score` are dropped, and the single best is always kept.
- One peer can win several clusters. Peers that win none get no round 2 at all.

**Round 2 checks, per chunk** (`round2` and `check_chunks`):
- The chunk's cluster must be one that was requested.
- Its proof must lead to the subtree root that was verified in round 1.
- At most `chunks_per_cluster` chunks per cluster are accepted.
- Chunks that fail are dropped (`rejected_chunks` goes up, flag `bad_chunk_proof`). They are kept but marked `failed_proof` only if `drop_unverified = false`.
- If the root was first accepted during this very question, the chunk is marked `verified_new_root` instead of `verified`. With `require_prior_root = true`, such chunks are dropped.

**Rescoring** (`rescore` and `apply_rescore`, if `rescore = true`):
- All received chunk texts are embedded in one call to node 1's own sidecar.
- Each chunk's score becomes `dot(q, embedding)`. The peer's number is kept as `claimed_score`.
- If they differ by more than `score_tolerance` (0.01), the peer and the chunk are flagged `score_mismatch`.
- If a peer claimed a `best_chunk_score` in round 1 that none of its delivered chunks reaches (within the tolerance), the peer is flagged `score_overclaim`.

### 8.2 flat

One round of `search {top_k}` to every peer that should be asked, with the same checks: cluster entries against the root, chunk proofs against the cluster entries, at most `top_k` chunks per peer, then rescoring. It is the old behaviour with verification added, and serves as the baseline for evaluation.

### 8.3 directory (Phase M8)

In `two_round`, round 1 asks every peer, on every question, just to learn which of its clusters are close. With many peers that means many connections, waiting for the slowest one, and every peer seeing every question. But a peer's centroids only change when it ingests documents. So in `directory` mode each node keeps a **directory**: a verified copy of every peer's centroids, fetched once per root version.

**Fetching** (`src/rag/directory.rs`):
- After each background `hello` round (every 60 s), node 1 sends `directory` to every peer whose accepted root version differs from the version of its stored directory (or that has none).
- Each answer is checked against the peer's accepted signed root:
  1. **Completeness:** the entries must be exactly the cluster ids `0 … num_clusters−1`, each once, where `num_clusters` is in the signed root. Otherwise flag `incomplete_directory` (the entries that verify are still kept).
  2. **Each entry, with its position:** `verify_at` checks the cluster proof as before, **and** that its left/right pattern is the one an honest proof for that id has in a tree of `num_clusters` leaves (`expected_sides`). Entries that fail are dropped; flag `bad_centroid_proof`.
- Together, these two checks mean a peer can't leave a whole cluster out of its directory without being noticed. A tree with more leaves than the signed count can't produce proofs of the right shape, and a missing id breaks completeness. The unit tests check this for 1 to 40 clusters.

**A query in directory mode:**
```
node 1                                           peers
  │ (a) local search, as before
  │ (b) embed the question → q
  │ peers with a directory entry at their accepted root version: score every stored centroid locally
  │ peers without one: ROUND 1 as in two_round (fallback), plus a background directory fetch
  │ SELECT: the same rule as two_round (best 5 over all candidates, ≥ 0.2, always keep the best)
  │ ROUND 2 ──── chunks {clusters it won} ──────────▶ only peers that won a cluster
  │ check proofs, rescore, merge, answer — exactly as two_round
```
- Choosing clusters takes a few milliseconds and no network traffic. The scale test scored 13,000 centroids (1000 peers × 13) in 4.7–7.8 ms per query (two runs).
- Only peers that won a cluster see the question at all.
- Directory mode can pick a peer's 4th or 5th best cluster, which `two_round` never sees (each peer offers only its top 3). In the M8 run this made the two modes choose different clusters on 11 of 15 questions; the answers were the same.
- If a peer re-ingested since the last refresh, round 2 gets `stale_root`: that peer gives no chunks for this question, its new root is recorded, and its directory is fetched again in the background for the next question.

**Costs and switches:**
- `[rag] directory = true` turns the mode on **and** makes the node serve its own directory to any allowed peer. A directory is a summary of every topic the node holds (section 14), so it is off by default.
- `directory_fallback = true` (default): peers without a fresh directory entry use round 1; with `false` they are reported as `no_directory` and not asked.
- With `retrieval = "directory"` in the config but `directory = false`, the node refuses to start. A per-query `"retrieval": "directory"` on such a node gets HTTP 400 `directory_disabled`.

### 8.4 What is the same in all modes

- Local chunks are searched as before and are labelled `verification: "local"`.
- Merging sorts all chunks by score, removes duplicate texts and keeps `final_k`.
- If no chunk is left, no answer is generated (the answer is an empty string).

---

## 9. Running it

All commands are run from the repository root.

### 9.1 One-time setup (per machine)

```bash
# Rust build
cargo build --release

# Python environment for the sidecar (already present on both machines)
uv venv .venv-rag --python 3.11
uv pip install --python .venv-rag/bin/python --torch-backend cpu -r requirements-rag.txt

# Answer writing (only needed on the node you ask questions)
ollama pull qwen2.5:1.5b
```

No new Python packages were needed for this work.

### 9.2 Two nodes on one machine

```bash
./target/release/iiitd --config examples/rag-2node/node2.toml     # terminal 1
./target/release/iiitd --config examples/rag-2node/node1.toml     # terminal 2
```

Each node starts its own sidecar, ingests its `corpus_dir`, signs its root and joins the mesh. On one machine they find each other through mDNS. Wait about 10 s after both are up: the first background refresh happens 5 s after startup.

### 9.3 Two machines

The setup is as in Phase 4: node 1 on machine 1 and node 2 on machine 2, over the VPN.
- `examples/rag-2node/node1.toml` already pins machine 2 (`peers = ["313823…@192.168.3.143:9001"]`).
- Both machines must run **the same build**. Push the branch, then on machine 2 run `git pull` and `cargo build --release`, and restart the node.
- Check with `curl localhost:8545/rag/debug/hello` on machine 1 (section 10.4): machine 2 must show `status=ok` and `signature_valid: true`.

### 9.4 Asking questions

```bash
# one question
curl localhost:8545/rag/query -H 'content-type: application/json' \
     -d '{"query": "In which town was A. P. J. Abdul Kalam born and raised?"}'

# the same in flat mode, or only the local index, or only peers
curl localhost:8545/rag/query -H 'content-type: application/json' \
     -d '{"query": "…", "retrieval": "flat"}'          # or "directory" (needs [rag] directory = true)
curl localhost:8545/rag/query -H 'content-type: application/json' \
     -d '{"query": "…", "scope": "local"}'           # or "peers"

# all 15 test questions, full responses saved, a summary per question on screen
python3 scripts/rag_run_questions.py --api http://localhost:8545 \
        --retrieval two_round --out results/two_round.jsonl
python3 scripts/rag_run_questions.py --api http://localhost:8545 \
        --retrieval flat --ids q07,q13 --out results/flat.jsonl
```

Optional request fields: `top_k` (local chunks, and peers' chunks in flat mode), `final_k` (chunks kept after merging), `scope`, `retrieval`.

### 9.5 Adding documents while running

```bash
curl localhost:8545/rag/ingest -H 'content-type: application/json' -d '{"path": "corpus/node1"}'
curl localhost:8545/rag/ingest -H 'content-type: application/json' \
     -d '{"documents": [{"id": "note.txt", "text": "…"}]}'
```

- The answer includes the new `root_version`, and the node logs a new `🔏 Signed RAG root …`.
- Peers pick up the new root at their next refresh (60 s), or straight away if they ask a question and notice the version changed.
- Adding new documents re-clusters the whole index.

### 9.6 Using the sidecar alone (no Rust node)

```bash
.venv-rag/bin/python rag_engine.py ingest --data-dir ./data-rag-node1 --path corpus/node1
.venv-rag/bin/python rag_engine.py serve  --data-dir ./data-rag-node1 --port 8600 --generator none

curl localhost:8600/commitment
curl localhost:8600/centroids -H 'content-type: application/json' -d '{"query": "What does Aditya-L1 study?", "n": 3}'
```

The sidecar routes:

| Route | Body | Returns |
| :-- | :-- | :-- |
| `GET /health` | — | model, chunk count, generator, `commitment` (root, version) |
| `GET /commitment` | — | the unsigned commitment |
| `POST /embed` | `{texts: [...]}` | `{dim, vectors: [hex of float32 values]}` |
| `POST /centroids` | `{query, n}` | `{root_version, centroids: [cluster entry + best_chunk_score]}` |
| `POST /search_clusters` | `{query, root_version, clusters, per_cluster}` | `{root_version, clusters, chunks}`, or HTTP 409 `stale_root` |
| `POST /retrieve` | `{query, top_k, with_proofs?}` | chunks (plus `root_version` and `clusters` with proofs) |
| `POST /generate` | `{query, chunks}` | answer |
| `POST /ingest` | `{path}` or `{documents}` | `{added_chunks, num_chunks, root_version}` |

### 9.7 Stopping

Stop a node with Ctrl+C or `kill <pid>`. Its sidecar stops on its own within about 2 s. If you start a node again immediately and see `port 8600 already answers as a RAG sidecar`, wait two seconds and retry.

---

## 10. Reading the outputs

### 10.1 Startup log (node 1)

```
📚 Starting RAG sidecar on 127.0.0.1:8600 (.venv-rag/bin/python)
📚 Ingested corpus/node1: {"added_chunks":0,"num_chunks":161,"root_version":1792345080027}
📚 RAG sidecar ready: {"commitment":{"root":"802f7d37…","version":1792345080027}, …}
🔏 Signed RAG root 802f7d373e43cbbf… (version 1792345080027, 161 chunks in 13 clusters)
🔑 Iroh EndpointId: 716b71c2de776ec9…
📚 Serving RAG retrieval requests from peers
📌 Pinned direct address for 313823248f: 192.168.3.143:9001
```

- `added_chunks: 0` means every file in `corpus_dir` was already indexed.
- Root and version stay the same across restarts as long as the documents do.

Other lines you may see:

| Log line | Meaning |
| :-- | :-- |
| `📜 Accepted RAG root 5172ade3… (version …) from peer dd0020…` | A peer's first or newer root was accepted into the registry. |
| `🚩 Peer … sent a root flagged Rollback` (or `BadSignature`, `Equivocation`) | A rule break (section 7.2). |
| `📚 RAG centroids request from peer …: returned 3 item(s) in 25 ms` | This node answered a peer (on the answering node). |
| `📚 RAG request from peer … failed: …` | This node could not answer a peer's request. |
| `⚠️ Peer … embeds with … but this node uses …` | Different embedding models: scores are not comparable. |
| `Index in … predates clustering; clustering it now.` | One-time upgrade of an old index (sidecar output). |

### 10.2 `/rag/query` response

A real two_round response from node 1 for q07 ("In which town was A. P. J. Abdul Kalam born and raised?"), shortened:

```json
{
  "retrieval": "two_round",
  "answer": "Rameswaram",
  "contexts": [
    { "chunk_id": "a_p_j_abdul_kalam.pdf#27", "source": "a_p_j_abdul_kalam.pdf",
      "text": "test site in Odisha in Wheeler Island was renamed as Abdul K…",
      "score": 0.6974193225490165, "claimed_score": 0.697419285774231,
      "node_id": "slakshna1acpx4c6y9yymy38zt9z4gyg4mjvj4g90cy8zlh",
      "cluster_id": 9, "verification": "verified" },
    …
  ],
  "peers": [
    { "endpoint_id": "dd002001c6d7…", "status": "ok", "chunks": 9, "rtt_ms": 59,
      "root_version": 1792345080105, "offered_clusters": 3, "selected_clusters": [9, 4, 2],
      "rejected_chunks": 0, "flags": [], "round1_ms": 37, "round2_ms": 22 },
    { "endpoint_id": "313823248f8d…", "status": "skipped_stale", "chunks": 0, "rtt_ms": 0,
      "root_version": null, "offered_clusters": 0, "selected_clusters": [],
      "rejected_chunks": 0, "flags": [], "round1_ms": null, "round2_ms": null }
  ],
  "timings_ms": { "local_retrieve": 38, "peer_retrieve": 530, "select": 39,
                  "fetch": 22, "rescore": 469, "generate": 20556, "total": 21088 }
}
```

**`contexts`:** the chunks given to the language model, best first.

| Field | Meaning |
| :-- | :-- |
| `score` | The score used for ranking. For peers' chunks this is node 1's own recomputed score. |
| `claimed_score` | The peer's own number (only for rescored chunks). Here it agrees to within 4·10⁻⁸. |
| `node_id` | The `slakshna1…` identity of the node whose index the chunk came from. |
| `cluster_id` | The cluster the chunk belongs to on that node (peers' chunks only). |
| `verification` | `local` (own index), `verified` (proof checked against a root known before this question), `verified_new_root` (proof checked against a root first seen during this question), `failed_proof` (kept only if `drop_unverified = false`). |
| `flags` | Appears only when something is wrong, e.g. `["score_mismatch"]`. |

**`peers`:** one entry per peer target.

| Field | Meaning |
| :-- | :-- |
| `status` | `ok`; `timeout`; `error` (see `error`); `no_root` (its answer matched no signed root); `skipped_stale` (not asked: down lately); `refused` (not asked: caught equivocating). |
| `chunks` | Chunks kept from this peer. Here 9 = 3 clusters × 3 chunks. Only 5 contexts survive the merge. |
| `root_version` | The root version the peer answered under. |
| `offered_clusters` | Cluster entries the peer sent (round 1 in two_round, the entries in flat). |
| `selected_clusters` | Clusters of this peer that won round 1, best first. Empty means no round 2 was sent to it (flat mode: always empty). |
| `rejected_chunks` | Chunks dropped: bad proof, not requested, or more than asked for. |
| `flags` | Rule breaks seen in this question (`bad_centroid_proof`, `bad_chunk_proof`, `score_mismatch`, `score_overclaim`, or `equivocation` for refused peers). |
| `round1_ms`, `round2_ms`, `rtt_ms` | Round-trip times; `rtt_ms` is their sum. In flat mode the single round is `round1_ms`; in directory mode a peer chosen from the directory has `round1_ms: null`. |
| `selected_via` | How this peer's clusters were chosen: `"directory"` or `"round1"` (always in two_round; fallback in directory mode). `null` in flat mode and for peers not asked. |
| `bytes_sent`, `bytes_received` | JSON bytes exchanged with this peer during this question, over every request (round 1, round 2, a search, an inline `hello`). Not counted: QUIC/TLS overhead. |

**`timings_ms`:**

| Field | Meaning |
| :-- | :-- |
| `local_retrieve` | Searching the own index. Runs at the same time as the peer work. |
| `select` | Round 1 plus selection (two_round); local scoring plus any fallback round 1 (directory). |
| `fetch` | Round 2, or the single flat round. |
| `rescore` | Embedding the received chunks on this machine. |
| `peer_retrieve` | `select + fetch + rescore`. |
| `generate` | The language model writing the answer. Almost always the largest number. Ollama reuses work for identical prompts, so repeating a question with the same chunks is much faster. |

This example was captured before Phase M8. Since M8 every peer entry also has `selected_via`, `bytes_sent` and `bytes_received`. In directory mode a peer entry looks like this (real output, q07, M8 live check):

```json
{ "endpoint_id": "91c5ad84c0e8…", "status": "ok", "chunks": 13, "rtt_ms": 12,
  "root_version": 1792664772134, "offered_clusters": 13, "selected_clusters": [9, 4, 2, 11, 7],
  "rejected_chunks": 0, "flags": [], "round1_ms": null, "round2_ms": 12,
  "selected_via": "directory", "bytes_sent": 208, "bytes_received": 39442 }
```

`offered_clusters` is every cluster in the stored directory. Peers' statuses can also be `no_directory` (directory mode without fallback). A response may carry a top-level `flags` list, e.g. `select_by_centroid_for_directory`.

### 10.3 `/rag/status`

```json
{ "retrieval": "two_round",
  "own_root": { "root": "802f7d37…", "version": 1792345080027, "num_chunks": 161,
                "num_clusters": 13, "dim": 384, "embedding_model": "…", "signature": "…" },
  "peer_targets": ["dd0020…", "313823…", "994f71…"],
  "registry": [
    { "endpoint_id": "313823…", "status": "failing", "asked_in_queries": false,
      "last_error": "hello timed out", "last_fail_secs_ago": 0, "last_ok_secs_ago": null,
      "root": null, "version": null, "flags": [], "evidence": 0 },
    { "endpoint_id": "dd0020…", "status": "ok", "asked_in_queries": true,
      "root": "5172ade337c58dd7", "version": 1792345080105,
      "first_seen_secs_ago": 1, "last_ok_secs_ago": 1, "flags": [], "evidence": 0, … } ],
  "sidecar": { "commitment": { "root": "802f7d37…", "version": 1792345080027 }, … } }
```

What to look at:

| Field | Meaning |
| :-- | :-- |
| `own_root` | This node's signed root. It must equal `sidecar.commitment`. |
| `registry[].status` | `untried`, `ok` (latest contact succeeded) or `failing` (latest contact failed). |
| `registry[].asked_in_queries` | Whether the next question would go to this peer. |
| `registry[].root`, `version` | The peer's accepted root (first 16 characters) and version. |
| `registry[].flags`, `evidence` | Rule breaks seen from that peer, and how many conflicting signed roots are stored. |
| `directory` | Directory mode: `enabled`, `total_centroids`, `approx_bytes` (memory held), and per peer `root_version`, `clusters` / `signed_clusters`, `complete`, `fetched_secs_ago`, `flags`. Example: `{"enabled": true, "total_centroids": 26, "approx_bytes": 40768, "peers": [{"endpoint_id": "91c5ad…", "root_version": 1792664772134, "clusters": 13, "signed_clusters": 13, "complete": true, "fetched_secs_ago": 18, "flags": []}]}` |

### 10.4 `/rag/debug/hello`

Sends `hello` to every target right now, updates the registry, and reports each answer:

```json
{ "endpoint_id": "dd002001c6d7…", "status": "ok", "rtt_ms": 22,
  "node_id": "slakshna1acpx4…", "root": { … }, "signature_valid": true, "outcome": "accepted" }
```

- `signature_valid: true` means the root was signed by the key behind that EndpointId.
- `outcome` is the registry's verdict: `accepted`, `unchanged`, `bad_signature`, `rollback` or `equivocation`.

Common errors:

| Error | Meaning |
| :-- | :-- |
| `peer doesn't support any known protocol` | The peer runs a build without protocol v2. |
| `No addressing information available` | The peer is remembered but has never been found on the network this time. |
| `status: timeout` | The address is known but nothing answers. |

---

## 11. Configuration (`[rag]` section)

All settings have defaults, so older config files still load. The ones added in this branch:

| Setting | Default | Meaning |
| :-- | :-- | :-- |
| `retrieval` | `"two_round"` | `"two_round"`, `"flat"` or `"directory"`; a query can override it. `"directory"` needs `directory = true`. |
| `directory` | `false` | Keep a verified directory of peers' centroids, and serve this node's own centroids to any allowed peer (section 8.3). |
| `directory_fallback` | `true` | In directory mode, ask peers without a fresh directory entry with round 1. |
| `n_clusters` | `0` | Clusters in this node's index; `0` = round(√chunks), at most 64. A change re-clusters at the next start. |
| `centroids_per_peer` | `3` | Round 1: centroids each peer returns. |
| `max_clusters` | `5` | Round 1: clusters kept over all peers. |
| `min_centroid_score` | `0.2` | Round 1: clusters below this similarity are dropped (the best is always kept). |
| `select_by` | `"centroid"` | Rank clusters by the asker's own similarity (`"centroid"`) or by the peer's claimed best chunk score (`"best_chunk"`). |
| `chunks_per_cluster` | `3` | Round 2: chunks fetched per chosen cluster. |
| `rescore` | `true` | Re-embed peers' chunks and use the asker's own scores. |
| `score_tolerance` | `0.01` | Allowed difference between claimed and recomputed score before flagging. |
| `drop_unverified` | `true` | Drop chunks whose proof fails. |
| `require_prior_root` | `false` | Drop chunks checked against a root first seen during the same question. |
| `select_timeout_ms` | `2000` | Timeout for `hello` and round 1. |
| `root_refresh_secs` | `60` | How often all peers' roots are fetched again. |
| `peer_stale_secs` | `180` | A peer silent for longer is skipped. |
| `adversary` | `"none"` | **Experiments only.** `tamper_text`, `inflate_score`, `fake_centroid`, `equivocate` or `hide_cluster` makes this node attack its peers (section 15). |

The earlier settings still apply: `top_k`, `final_k`, `peer_timeout_ms` (5000, used for round 2 and flat), `max_message_bytes`, `generator` and so on.

One more setting, for experiments only: `adversary` (default `"none"`) makes the node attack its peers on purpose (section 15).

The node refuses to start if a value is out of range, for example `max_clusters = 0`, `min_centroid_score = 1.5`, `n_clusters = 100`, `retrieval = "fast"`, or `retrieval = "directory"` without `directory = true`.

---

## 12. Testing

### 12.1 Python tests (sidecar)

```bash
.venv-rag/bin/python -m unittest tests/test_rag_store.py tests/test_rag_merkle.py tests/test_rag_server.py
```

51 tests, about 2 s, with no model download: a fake embedder counts keywords instead.

| File | Checks |
| :-- | :-- |
| `test_rag_merkle.py` | The code reproduces the fixture exactly. Leaf, inner and cluster hashes and the signed-message bytes are recomputed by hand with `hashlib` and compared. Proofs fold to the root for trees of 1–33 leaves. A changed text or one flipped centroid bit fails. |
| `test_rag_store.py` | The index is HNSW with inner-product scoring, its results match exact search, and an old exact index is rebuilt on load with the same root. Clusters are numbered without gaps and cover every chunk. Every proof folds to the root. The root survives a reload. Hand-edited `chunks.jsonl` or `centroids.npy` stops loading. A new document changes root and version; re-ingesting changes nothing. Old indexes are migrated. Changing `n_clusters` re-clusters. The empty index has the zero root. |
| `test_rag_server.py` | Every sidecar route, in-process: proofs from `/centroids`, `/search_clusters` and `/retrieve` fold to the root; a stale version gets HTTP 409; `/ingest` reports the new version. |

### 12.2 Rust tests (node)

```bash
cargo test --release
rustfmt --edition 2021 --check src/rag/merkle.rs src/rag/registry.rs src/rag/retrieval.rs src/rag/adversary.rs src/rag/directory.rs
cargo test --release directory_scale -- --ignored --nocapture    # the directory scale test (prints timings)
```

50 tests, plus the scale test, which only runs when asked. The `rustfmt` check covers only the new files: older files keep their own brace style (decision I5), so `cargo fmt --check` over the whole tree is expected to fail.

| Module | Checks |
| :-- | :-- |
| `rag::merkle` | The shared fixture (every hash, tree, proof, edge case and the signed-message bytes); a sign/verify round trip; another key, a changed version, a changed model or a malformed signature all fail. |
| `rag::protocol` | Each request serialises to the JSON shape in the plan; large versions round-trip; mismatched ids, kinds and errors are rejected; `stale_root` carries the new root. |
| `rag::registry` | Accept, unchanged, bad signature, rollback, equivocation with evidence; who is asked and who is skipped; the status lines. |
| `rag::adversary` | Each simulated attack, applied to fixture data, is caught by the real checks: tampered chunks fail, inflated scores are flagged, fake centroids fail, a second root under one version is equivocation. |
| `rag::directory` | A directory is stored complete; a hidden cluster is flagged `incomplete_directory` and the rest kept; repeated, extra and forged entries are caught; a directory signed for fewer clusters than built fails; scoring; the status. The scale test: 1000 peers × 13 centroids. |
| `rag::retrieval` | Cluster selection (ranking, cap, threshold, always-keep-best, `best_chunk` mode); fixture chunks verify, tampered ones are dropped; unrequested and over-cap chunks are rejected; rescoring replaces scores and flags inflated ones; overclaims are found. |

### 12.3 The shared test vectors

`tests/fixtures/merkle_vectors.json` lists small inputs and every hash and proof expected from them, including the edge cases: an empty tree, 1, 2, 3 and 5 leaves, one cluster, an empty corpus, and non-ASCII and empty texts. Python and Rust both test against it, which is how the two languages are shown to produce identical bytes.

- To show that the tests would catch a mismatch, the Rust format was deliberately broken twice during M3: once with a different leaf marker byte, once with little-endian lengths. 3 and 4 tests failed respectively.
- Regenerate the file only when the format is changed **on purpose**: `.venv-rag/bin/python scripts/rag_merkle_vectors.py`. A changed fixture means every node must be upgraded together.

### 12.4 Live checks that were done

These are not automated; they were run by hand for each phase and are recorded in section 14 of `docs/merkle_tree.md`:
- the restart keeps the root;
- an independent script folds proofs from the live sidecar;
- signatures are checked with a separate Ed25519 library (Python `cryptography`);
- re-signing after ingest;
- dead-peer skipping and recovery;
- all 15 questions in both modes on one machine and across both machines.

---

## 13. Results so far

| Phase | What was measured |
| :-- | :-- |
| M1 | Node 1's 161 chunks form 13 clusters (sizes 2–34). Ingest takes about 14 s, mostly model loading. The root is identical after restarts, after migrating an old index, and on a third node indexing the same files. |
| M2 | For three node-1 questions the best cluster was the right topic each time. 29 of 29 returned proofs folded to the root with an independent script; every tampered chunk failed. |
| M3 | Python and Rust agree on every byte of the fixture. |
| M4 | Signed roots verify across nodes (3–8 ms per `hello`), and with an independent library. A new root after ingest reached the peer at once. |
| M5 | With a dead remembered peer, peer retrieval fell from 5001 ms to about 30 ms after the first refresh. With rule J23, a peer that just died costs one timed-out question. |
| HNSW switch (decision C10) | Same top 5 as exact search for all 365 test queries on both indexes. All 15 questions in both modes gave the same contexts, in the same order; `node2_only` still 6/6, every remote context `verified`. Roots unchanged. |
| M6 | 15 questions, both modes, one machine and two machines: `node1_only` 6/6, `node2_only` 6/6, `cross_node` 0/3. Every remote context `verified`, no flags. Peer work about 0.4 s (two_round) or 0.24 s (flat), mostly re-embedding; VPN round trips 21–68 ms. |
| M8 | The directory mode: scoring 13,000 centroids takes 4.7–7.8 ms per query (scale test, two runs). Live, all 15 questions in all three modes: `node1_only` 6/6, `node2_only` 6/6 in each, every remote context `verified`. Directory mode needs no round 1 after the first refresh, and selects exactly the predicted clusters. Bytes per query: directory 44 KB, two_round 66 KB, flat 32 KB. All five attacks are caught in directory mode, including `hide_cluster` (`incomplete_directory`). |
| M7 | Each of the four simulated attacks was caught with the predicted flag, in both modes; `FORGED` text never reached a context or an answer; an equivocating peer was refused (section 15). |

Two observations to carry into M9:
- **Selection rarely leaves a peer out with the defaults.** With 2 peers × 3 centroids competing for 5 places, and unrelated topics still scoring above 0.2 with this model, both peers usually win something. A third node did get no round 2 for a question outside its topic. `max_clusters` and `min_centroid_score` need tuning in the evaluation.
- **Cross-node questions still fail.** The five best chunks all come from one side, so half of the needed evidence is missing. This is the same limit as before; verification does not change it.

---

## 14. What this does not protect against

From `docs/merkle_tree.md`, section 11. Keep these in mind for any write-up:

- **Bad content committed in advance.** A peer can ingest misleading documents before signing its root; their proofs pass. The scheme makes peers *commit* to their content, not make it true.
- **Directory mode reveals topic maps.** Any allowed peer can fetch every centroid of a node with `directory = true`, a summary of everything it holds. Use `allowed_peers` if this matters.
- **Hiding chunks.** A proof shows a chunk *is in* the corpus, not that it is the *best* one. A peer can leave out relevant chunks. In two_round it can also hide whole clusters; in directory mode hiding a cluster is caught (`incomplete_directory`), but hiding chunks inside a cluster still isn't. `score_overclaim` catches only the opposite case (claiming more than it delivers).
- **First contact.** The first root seen from a peer is accepted as is, and the registry is lost on restart.
- **A new root per question.** A peer could re-sign for every question. This shows up as `verified_new_root` and a version that keeps climbing; only `require_prior_root = true` blocks it.
- **Privacy.** Peers still see the raw question, centroids reveal a peer's topics, and chunks are sent as plain text (inside the encrypted QUIC connection).

---

## 15. Simulated attackers (Phase M7)

To test the checks against real attacks, a node can be told to misbehave towards its peers with `[rag] adversary` in its config. It answers each request honestly through its sidecar, then corrupts the answer just before sending it (`src/rag/adversary.rs`, called from `protocol.rs`). The asking node is unchanged. **Never set this outside an experiment.** A node with it on logs `😈 [rag] adversary = …: this node will send corrupted answers to peers. EXPERIMENTS ONLY.` at startup.

| `adversary` | What the node does to its answers | What the asking node does |
| :-- | :-- | :-- |
| `tamper_text` | Replaces the middle word of every chunk with `FORGED`. | The chunk's leaf hash changes, so its proof fails. The chunk is dropped; flag `bad_chunk_proof`; `rejected_chunks` counts it. |
| `inflate_score` | Adds 0.3 to every chunk score and every `best_chunk_score`. | Rescoring gives its own score; `claimed_score − score = 0.3`. Flag `score_mismatch`, and in two_round `score_overclaim` (the claimed best chunk is never delivered). The chunks are genuine, so they are kept, ranked by the real scores. |
| `fake_centroid` | Replaces every centroid it sends with the query's embedding, which would have similarity 1 and always win. | The centroid is inside the cluster hash, so the cluster proof fails. Flag `bad_centroid_proof`; the peer wins no cluster and gets no round 2. In flat mode the chunks under the failed entries are rejected too. |
| `hide_cluster` (M8) | Leaves its highest-numbered cluster out of every `directory` answer. | The directory lists 12 of the 13 signed clusters: flag `incomplete_directory`. The 12 that verify are still used. |
| `equivocate` | On every other `hello`, sends a second root (the real one with its first byte flipped) under the same version, properly signed. | The registry sees two different signed roots with one version: flag `equivocation`, both kept as `evidence`, and from then on the peer is `refused` (not asked). |

### How the check was run

The attacks were tested in an isolated test federation, so the misbehaving node could not reach or be remembered by the real nodes:
1. Make copies of `examples/rag-2node/node1.toml` and `node2.toml` with a different `[federation] id`, other ports, `peers = []` and scratch `data_dir`s.
2. In node 2's copy, add `adversary = "tamper_text"` (or another mode) to `[rag]`.
3. Start both, wait about 10 s, and run `python3 scripts/rag_run_questions.py --api http://localhost:<node 1 api port> --retrieval two_round --ids q07,q08,q09,q10,q11,q12 --out …` (and again with `--retrieval flat`).
4. For `equivocate`: ask one question, call `curl localhost:<port>/rag/debug/hello` (the second `hello`), then ask again. The peer is now `refused`.
5. Restart both nodes before the next mode: flags live in memory, and an equivocating peer stays refused until node 1 restarts.

### Results (2026-10-22)

| Mode | Flags on node 2 | Effect | `FORGED` in contexts or answer |
| :-- | :-- | :-- | :-- |
| `tamper_text` | `bad_chunk_proof` | every chunk rejected (7–9 per question in two_round, 5 in flat) | never |
| `inflate_score` | `score_mismatch` (+ `score_overclaim` in two_round) | chunks kept, all re-scored down by exactly 0.3 | never |
| `fake_centroid` | `bad_centroid_proof` (+ `bad_chunk_proof` in flat) | node 2 selected for nothing; no round 2 | never |
| `equivocate` | `equivocation`, 2 roots in `evidence` | first question normal; after the second `hello`, node 2 `refused` | never |

What to read from the outputs:
- `peers[].flags` names the attack.
- `peers[].rejected_chunks` shows how many chunks were thrown away.
- `contexts[].claimed_score` shows inflated claims.
- `/rag/status` → `registry[].flags` and `evidence` keep the record; a refused peer shows `asked_in_queries: false`.

With node 2's chunks rejected, the node-2 questions were answered from node 1's own (unrelated) chunks, and mostly wrongly, e.g. q07 "Allahabad". The checks keep forged text out; they cannot supply the missing evidence. Flags other than `equivocation` are only recorded. A peer caught tampering is still asked next time; acting on flags (trust scores) is a later step.

---

### Results in directory mode (2026-10-22, Phase M8)

| Attack | Node 1's directory of node 2 | What happened | `FORGED` in contexts or answer |
| :-- | :-- | :-- | :-- |
| `fake_centroid` | 0 of 13 entries kept, `bad_centroid_proof` | fell back to round 1, failed there too; no cluster chosen; no round 2 | never |
| `hide_cluster` | 12 of 13 kept, `incomplete_directory` | the 12 remaining clusters were used; chunks verified | never |
| `tamper_text` | complete | every round-2 chunk rejected (`bad_chunk_proof`) | never |
| `inflate_score` | complete | `score_mismatch`, all claims 0.3 too high; own scores used. No `score_overclaim`: directory entries carry no claimed best score | never |
| `equivocate` | complete | first question normal; after the second `hello`, `equivocation` and node 2 `refused` | never |

---

## 16. What comes next

- **M9, evaluation on the HPC** (`docs/merkle_tree.md`, section 16). Compare all three modes (`flat`, `two_round`, `directory`) on the user's dataset and hardware: retrieval quality, answer accuracy, peers contacted, bytes per query (now measured per peer), time spent, and detection rate per attack. Sweep `n_clusters`, `max_clusters`, `min_centroid_score` and `select_by`. An offline simulation covers 1000 nodes.

The design decisions behind everything above are listed, with reasons, in `docs/decisions.md` (sections C, I, J and K).
