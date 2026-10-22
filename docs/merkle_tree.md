# Verifiable, Cluster-Routed Retrieval (Merkle Roots + K-Means) — Implementation Plan

Written 2026-10-18. Status: **Phases M1–M7 done (M7 on 2026-10-22). M8 (centroid directory, a third retrieval mode, section 15) and M9 (evaluation of all three modes on the HPC with the user's dataset, section 16) not started.** M8 and M9 are developed on branch `centroid-directory`. Results per phase are in section 14. Builds on the working P2P RAG pipeline from `docs/p2p_rag_plan.md` (Phases 0–4 done). Read that plan first; this one changes its peer protocol.

## 0. Summary in plain language

Today a query goes to every peer, and every peer sends back its best chunks. The asking node has to trust whatever text and scores come back.

This plan adds two things:

1. **K-means clustering.** Each node sorts its chunks into topic groups (clusters). Each group has a **centroid**, the average embedding of its chunks. A centroid is a 384-number summary of one topic.
2. **A Merkle tree.** Each node fingerprints its whole corpus, chunks *and* centroids, as one 32-byte **root**, and signs it with its key. Peers fetch and remember each other's signed roots when they start up. Any chunk or centroid sent later must come with a short **proof** that it is part of the remembered root.

A query then runs in two rounds:

```
Round 1  asker → every live peer:  "here is the query, send your 3 closest centroids"
         peer  → asker:            centroids + proofs
         asker: checks the proofs, computes the similarities itself, picks the best ≤5 clusters
Round 2  asker → only the peers owning those clusters: "send your best chunks from clusters X, Y"
         peer  → asker:            chunks + proofs
         asker: checks the proofs, re-embeds the chunks to recompute their scores, merges, answers
```

What this gives you:
- A peer cannot make up text or centroids at query time. Anything it sends must match the root it committed to earlier.
- A peer cannot inflate its scores to win the merge, because the asker recomputes every score itself.
- Chunks are only requested from peers that have a matching topic.
- Two different signed roots with the same version number are signed evidence that a peer lied.

What it does **not** give you is listed in section 11. Read that section before making claims in a paper.

**Third mode, `directory` (Phase M8, section 15).** Round 1 asks *every* peer, which doesn't scale to hundreds or thousands of nodes. In directory mode each node keeps a local copy of every peer's centroids, each checked against that peer's signed root. The copy is fetched once per root version, not once per query. Choosing clusters then happens locally, with no network traffic, and only round 2 goes over the network. All three modes (`flat`, `two_round`, `directory`) live in one build, and a query picks one, so they can be compared on identical data (Phase M9, section 16).

## 1. Decisions (D1–D10 approved by the user, 2026-10-18; D11–D16 on 2026-10-22)

| # | Decision | Reason |
| :-- | :-- | :-- |
| D1 | Roots are fetched from peers at startup and refreshed in the background. Centroids are fetched per query (round 1), not published at startup. | The user's design. Each query reveals only a few centroids per peer, not the whole topic map, and they are never stale. |
| D2 | Peers send the **centroid vector**, not a similarity score. The asker computes the similarity itself. | A peer could lie about a score; it can't lie about a vector that has to match its root. |
| D3 | Centroids are **inside** the Merkle tree (two-level tree). | Lets the round-1 centroids be verified, not just the chunks. |
| D4 | Each peer returns its top `centroids_per_peer` (3) centroids. The asker ranks all of them, keeps up to `max_clusters` (5) above `min_centroid_score`, and always keeps at least the best one. One peer can win several clusters. | "Top 5 centroids" is not the same as "5 nodes". With 2 nodes, a fixed 5 would mean everyone. |
| D5 | Round 2 returns the top `chunks_per_cluster` (3) chunks from each selected cluster. | One chunk per node is too little context. Cross-node questions already fail from missing evidence. |
| D6 | Round 1 also returns each centroid's `best_chunk_score`, the peer's claimed best chunk score in that cluster. It is used for flagging, and optionally for selection. | A centroid is an average and can hide one very relevant chunk. The claim can't be verified until round 2, which then checks it. |
| D7 | Roots are signed with the node's Ed25519 key and re-fetched in the background (pull, over the RAG protocol). Not sent over gossip. | Doesn't change the FL gossip wire format. See D8 for the key choice. |
| D8 | The signing key is the node's **Iroh transport key** (`SecretKey::from_bytes(&keypair.transport_seed())`), not the federation keypair. | The asker already knows each peer's EndpointId, and the EndpointId is that public key. QUIC has already proved the connection comes from it. So the signature can be checked with nothing extra, and it gives evidence a third party can check too. |
| D9 | Roots carry a version number that only goes up; a proof names the version it belongs to. | Re-ingesting changes the root. |
| D10 | Stale peers are skipped, using the root registry's last-seen times. Round 1 has its own, shorter timeout. | A down peer currently costs 5 s per query, and with two rounds that delay would come before round 2 can start. |
| D11 | **Centroid directory** is a third retrieval mode (`retrieval = "directory"`), not a separate fork of the code. It is developed on branch `centroid-directory`. (User) | It reuses the roots, proofs, registry, round 2, checks, rescoring and merging. All three modes can be compared in one build, on the same data. |
| D12 | Directory mode reverses D1 **for nodes that enable it**: any allowed peer can fetch a node's full set of centroids. It is off unless `[rag] directory = true`. | At large scale, sending every query to every peer reveals every question to everyone. The directory reveals topic maps instead, and only round-2 peers see the question. Each deployment chooses. |
| D13 | Directories are **pulled** (new request kind `directory`), only when a peer's accepted root version changes, piggybacking on the registry's background refresh. | Centroids only change when a node ingests documents. Gossip push is a later option (section 12). |
| D14 | Directory entries are checked **position-bound**: the proof's shape must match `(cluster_id, num_clusters)`, and a directory must contain exactly the signed `num_clusters` entries, ids `0..k-1`. | This turns "a peer can hide a whole cluster" (section 11) into a detectable `incomplete_directory`, for the cluster level. |
| D15 | Peers without a fresh directory entry fall back to round 1 for that query (`directory_fallback = true`). | New peers, peers that just re-ingested, and peers on older builds still take part, and nothing is silently lost. |
| D16 | Evaluation of all three modes (old M8) becomes **M9**, run on the user's HPC with the user's dataset. (User) | The 2-node test corpus is too small for claims. Scale needs real hardware and a real dataset. |

## 2. Cryptographic format (exact bytes; Python and Rust must match)

All hashes are SHA-256, 32 bytes. In JSON a hash is written as lowercase hex. `u32be`/`u64be` are big-endian unsigned integers. `‖` means concatenation.

### 2.1 Node hashes

```
leaf(chunk)       = H( 0x00 ‖ u32be(len(id)) ‖ id ‖ u32be(len(text)) ‖ text )
                    id, text = UTF-8 bytes of chunk_id and chunk text, exactly as stored in chunks.jsonl
inner(left,right) = H( 0x02 ‖ left ‖ right )
cluster(c)        = H( 0x01 ‖ u32be(cluster_id) ‖ u32be(dim) ‖ centroid_bytes ‖ subtree_root )
                    centroid_bytes = dim float32 values, little-endian (dim*4 bytes)
```

- The **domain bytes** (0x00 leaf, 0x01 cluster, 0x02 inner) stop one kind of node from being passed off as another.
- The **length prefixes** stop two different (id, text) pairs from producing the same bytes.

### 2.2 Tree shape

The same rule builds both levels of the tree:

```
tree_root(nodes):
    if nodes is empty:        return 32 zero bytes
    while len(nodes) > 1:
        next = [inner(nodes[i], nodes[i+1]) for each pair]
        if len(nodes) is odd:  next.append(nodes[-1])        # the last node moves up unchanged
        nodes = next
    return nodes[0]
```

- **Lower level (one tree per cluster):** `subtree_root(c) = tree_root([leaf(ch) for ch in chunks of c, in chunks.jsonl order])`.
- **Upper level:** `root = tree_root([cluster(c) for c in 0..k-1])`.
- Cluster ids run from `0` to `k-1` with no gaps. Empty clusters are dropped and the rest renumbered before hashing (section 3.1).

### 2.3 Proofs

A proof is the list of sibling hashes from a node up to a root, written in JSON as:

```json
[ {"h": "<hex>", "left": true}, {"h": "<hex>", "left": false} ]
```

`left: true` means the sibling is on the left. Folding a proof:

```
h = starting hash
for step in proof:
    h = inner(step.h, h) if step.left else inner(h, step.h)
return h
```

Generating one (index `i` at the bottom level):

```
for each level from the bottom (except the top):
    if i is odd:                    step(sibling = level[i-1], left = true)
    elif i + 1 < len(level):        step(sibling = level[i+1], left = false)
    else:                           no step; this node moves up unchanged
    i = i // 2
```

A full chunk proof has two parts:
- `chunk_proof`: from `leaf(chunk)` up to `subtree_root(c)`;
- `cluster_proof`: from `cluster(c)` up to `root`.

### 2.4 Signed root

Message to sign:

```
"slakshna-rag-root-v1" ‖ 0x00 ‖ root(32) ‖ u64be(version) ‖ u64be(num_chunks)
                        ‖ u32be(num_clusters) ‖ u32be(dim) ‖ u32be(len(model)) ‖ model_utf8
```

JSON form, used on the wire and in `/rag/status`:

```json
{ "root": "<hex>", "version": 1760774400123, "num_chunks": 161, "num_clusters": 13,
  "dim": 384, "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
  "signature": "<128 hex chars>" }
```

- Signed by Rust using `iroh::SecretKey::sign`.
- Verified with `peer_endpoint_id.verify(&msg, &Signature::from_bytes(&sig))`. `EndpointId` is `iroh::PublicKey`; both APIs are confirmed in the vendored `iroh-base-1.0.3/src/key.rs`.
- `version` is **Unix time in milliseconds** when the commitment was built. It only goes up, even if the `rag/` folder is deleted and rebuilt, so peers don't mistake a rebuild for a rollback. If the clock goes backwards, use `max(previous + 1, now_ms)`.

### 2.5 Test vectors (required)

A small generator script, `scripts/rag_merkle_vectors.py`, writes `tests/fixtures/merkle_vectors.json`. The file holds a few chunks, a 4-dimensional centroid set, and every expected hash and proof, including the edge cases: 1 leaf, 2 leaves, 3 leaves (the odd node moving up), 1 cluster, and an empty tree. It also holds one signed-root message as hex (only the message bytes; there is no key in the file).

- The Python tests check that the code reproduces the fixture.
- The Rust tests load it with `include_str!("../../tests/fixtures/merkle_vectors.json")` and check the same values.

This is the only reliable way to catch a byte-order or encoding mismatch between the two languages.

## 3. Python sidecar changes

### 3.1 `rag/clustering.py` (new)

```python
def choose_k(n_chunks, requested):   # requested = config n_clusters; 0 = auto
    # auto: round(sqrt(n)), clamped to [1, 64]; never more than n_chunks
def cluster(vectors, k, seed=1234) -> (centroids float32 [k', dim], assignments int [n]):
    # faiss.Kmeans(dim, k, niter=20, seed=seed, spherical=True); train on vectors
    # assign: kmeans.index.search(vectors, 1)
    # drop empty clusters, renumber 0..k'-1 in order of old id, renormalise centroids to unit length
```

- `spherical=True` keeps the centroids at unit length, so a dot product is a cosine similarity.
- FAISS prints a warning to stderr when there are few points per centroid. That's harmless, and stderr isn't part of any protocol here.
- The results don't need to be identical on other machines. Peers never recompute the clustering; they only check proofs against what was committed.

### 3.2 `rag/merkle.py` (new)

This module contains only hashing, with no FAISS or numpy except for `centroid.astype('<f4').tobytes()`:

- `leaf_hash(chunk_id, text)`, `inner_hash(l, r)`, `cluster_hash(cluster_id, centroid, subtree_root)`;
- `build_levels(nodes) -> list[list[bytes]]`, `tree_root(nodes)`, `make_proof(levels, index)`, `fold_proof(start, proof)`;
- `class Commitment`: built from `(chunks, centroids)`. It holds the per-cluster leaf levels, the cluster levels and the root, and provides `cluster_entry(c)` and `chunk_proof(global_chunk_index)`.

### 3.3 `rag/store.py` changes

New on-disk files and fields in `{data_dir}/rag/`:

| File | Change |
| :-- | :-- |
| `chunks.jsonl` | Every chunk gains `"cluster": <int>`. |
| `centroids.npy` | New. float32 `[k, dim]`. |
| `meta.json` | Gains `"commitment": {"root": hex, "version": int, "num_clusters": int}`. |

Behaviour:
- Keep `self.vectors` (float32 `[n, dim]`) in memory, filled from `index.reconstruct_n(0, ntotal)` on load and appended on add. It is needed for k-means and for per-cluster scoring.
- Split the work: `add_document` only chunks, embeds and appends, and no longer saves. A new `commit()` re-clusters **all** vectors, rebuilds the `Commitment`, sets `version`, and saves everything once. `add_path` calls `commit()` once at the end if anything was added. The server's `/ingest` with `documents` does the same.
  - Why re-cluster everything: ingest happens rarely (mostly at startup) and the corpus is small. Assigning new chunks to old centroids is a later optimisation (section 12).
- Load: rebuild the `Commitment` from `chunks.jsonl` and `centroids.npy`, and **refuse to start** if the rebuilt root differs from `meta.json` ("index files were changed on disk"). An old index without `cluster` fields is migrated once: `commit()` runs on load.
- An empty store has no clusters, the zero root and `version = 0`. Peers treat it as "nothing to offer".
- New methods:
  - `nearest_centroids(query_vec, n)`: top `n` clusters by `centroids @ q`. Each entry has the cluster fields plus `best_chunk_score = max(vectors[cluster] @ q)`.
  - `search_clusters(query_vec, cluster_ids, per_cluster)`: the best `per_cluster` chunks inside each listed cluster, each with `cluster_id` and `chunk_proof`.
  - `search(query, top_k, with_proofs=False)`: unchanged by default. With proofs it adds `cluster_id` and `chunk_proof` and returns the matching cluster entries. Used by `retrieval = "flat"` (section 6.3).
- **Cluster entry** (shared JSON shape):
  ```json
  { "cluster_id": 4, "size": 17, "centroid": "<hex of dim*4 bytes>",
    "subtree_root": "<hex>", "cluster_proof": [ ... ], "best_chunk_score": 0.61 }
  ```
  `best_chunk_score` is present only in nearest-centroid answers. Centroids are hex rather than base64 because the crate has `hex` but no `base64`. 384 × 4 bytes is 3 KB of hex per centroid.

### 3.4 `rag/server.py` new routes

| Route | Body | Returns |
| :-- | :-- | :-- |
| `GET /commitment` | — | `{root, version, num_chunks, num_clusters, dim, embedding_model}` (unsigned; Rust signs it) |
| `POST /embed` | `{texts: [..]}` | `{dim, vectors: ["<hex f32le>", ..]}` |
| `POST /centroids` | `{query, n}` | `{root_version, centroids: [cluster entry + best_chunk_score]}` |
| `POST /search_clusters` | `{query, root_version, clusters: [int], per_cluster}` | `{root_version, clusters: [cluster entry], chunks: [chunk + cluster_id + chunk_proof]}`; HTTP 409 if `root_version` is not current |
| `POST /retrieve` | adds optional `with_proofs: bool` | as today, plus `root_version` and `clusters` when `with_proofs` |

- Add `commitment` (root and version) to `GET /health`.
- `/ingest` responses include the new `root_version`, so Rust knows to re-sign.

### 3.5 Tests: `tests/test_rag_merkle.py` (new), additions to `tests/test_rag_store.py`

- Fixture vectors match (section 2.5).
- Every chunk proof plus its cluster proof folds to the root. Flipping one character of the text, or one bit of a centroid, breaks it.
- Clusters are numbered 0..k-1 with no gaps, every chunk belongs to exactly one, and the sizes add up to `num_chunks`.
- The root survives a save and reload. Editing `chunks.jsonl` by hand makes loading fail.
- Ingesting a new document changes the root and increases the version. Re-ingesting the same document changes nothing.
- `search_clusters` returns only chunks from the requested clusters.

## 4. Rust: `src/rag/merkle.rs` (new)

- `ProofStep { h: String, left: bool }` (serde).
- `leaf_hash`, `inner_hash`, `cluster_hash(cluster_id, dim, centroid_bytes, subtree_root)`, `fold_proof(start, &[ProofStep]) -> Result<[u8;32]>`.
- `ClusterEntry` (serde, matches 3.3), with `fn verify(&self, root: &[u8;32], dim) -> Result<Vec<f32>>`. It checks the centroid length is `dim*4`, folds `cluster_proof` from `cluster_hash(..)` to the root, and returns the decoded centroid.
- `fn verify_chunk(chunk_id, text, chunk_proof, expected_subtree_root) -> bool`.
- `SignedRoot` (serde, matches 2.4), with `fn message_bytes(&self) -> Vec<u8>`, `fn sign(secret: &iroh::SecretKey, commitment) -> SignedRoot`, and `fn verify(&self, signer: &iroh::EndpointId) -> bool`.
- `fn dot(a: &[f32], b: &[f32]) -> f64` (accumulates in f64).
- Unit tests: the fixture file, a sign/verify round trip, and a signature from a different key failing.

Add `mod merkle` to `src/rag/mod.rs` and list `merkle.rs` under the `cargo test` line in `CLAUDE.md`.

## 5. Wire protocol v2 (`src/rag/protocol.rs`)

- ALPN becomes `b"/slakshna/rag/2"` and `WIRE_VERSION = 2`. Nodes on the v1 build fail with "peer doesn't support any known protocol". They are reported as `error` and skipped, as Phase 3 already showed. All RAG nodes must be upgraded together.
- Requests and responses become serde enums tagged by `kind`. One stream still carries one request and one response.

```json
// hello: fetch the peer's signed root
{"v":2, "kind":"hello", "query_id":"…"}
→ {"v":2, "kind":"hello", "query_id":"…", "node_id":"slakshna1…", "embedding_model":"…", "root": SignedRoot, "error":null}

// round 1
{"v":2, "kind":"centroids", "query_id":"…", "query":"…", "n":3}
→ {"v":2, "kind":"centroids", "query_id":"…", "node_id":"…", "embedding_model":"…",
   "root_version":…, "centroids":[ClusterEntry + best_chunk_score], "retrieve_ms":…, "error":null}

// round 2
{"v":2, "kind":"chunks", "query_id":"…", "query":"…", "root_version":…, "clusters":[4,7], "per_cluster":3}
→ {"v":2, "kind":"chunks", …, "root_version":…, "clusters":[ClusterEntry], "chunks":[ProvenChunk], "error":null}

// flat (baseline, one round, same as v1 plus proofs)
{"v":2, "kind":"search", "query_id":"…", "query":"…", "top_k":5}
→ {"v":2, "kind":"search", …, "root_version":…, "clusters":[ClusterEntry], "chunks":[ProvenChunk], "error":null}
```

- `ProvenChunk = { chunk_id, source, text, score, cluster_id, chunk_proof }`. This is a **wire-only** type. After verification it becomes the existing `Chunk` with the new optional fields from section 6.4, so proofs never reach `/generate` or the `/rag/query` output.
- The responder caps: `n ≤ 10`, `clusters.len() ≤ 10`, `per_cluster ≤ 10`, `top_k ≤ 50` (existing `MAX_TOP_K`).
- A `chunks` request with an old `root_version` gets `error: "stale_root"` and the current `root`, so the asker can update its registry and report it.
- `query_peer` becomes `request_peer(endpoint, peer, &RagRequest, max_bytes) -> RagResponse`. It keeps the `query_id` match and error checks.

### 5.1 Own root (`RagService`)

- `RagService::new` takes a `SecretKey`. `main.rs` builds it from `state.get_keypair()` the way `mesh.rs::transport_secret_key` does. Move that one-liner into a shared helper so the two can't drift apart.
- `RagService` holds `own_root: RwLock<Option<SignedRoot>>`. `refresh_own_root()` calls `GET /commitment`, signs the result, and stores it. It runs after the startup ingest in `main.rs` (before the network starts, so the first `hello` already has a root) and after every `/rag/ingest`.

## 6. Asking side

### 6.1 Root registry and stale peers (`src/rag/registry.rs`, new)

```rust
struct PeerRoot {
    root: SignedRoot,           // latest accepted
    first_seen: Instant,        // when this exact root was first accepted
    last_ok: Option<Instant>,   // last successful hello or query
    last_fail: Option<Instant>,
    flags: Vec<String>,         // "equivocation", "rollback", "bad_signature", "score_overclaim", …
    evidence: Vec<SignedRoot>,  // conflicting signed roots kept for equivocation
}
```

- **Background task** (started in `main.rs` after the network): every `root_refresh_secs` (60), plus once 5 s after startup, it sends `hello` to every target from `rag_link()` at the same time, with `select_timeout_ms` as the timeout.
- Rules for accepting a fetched root:
  - Bad signature → `bad_signature`; the old root is kept.
  - Lower version than the stored root → `rollback`; the old root is kept.
  - Same version, different root → `equivocation`. Both signed roots go into `evidence`, and the peer's chunks are refused from then on.
  - Higher version → accepted, `first_seen = now`.
- **Who gets a query:**
  - A target whose `last_ok` is within `peer_stale_secs` (180) **and whose latest contact did not fail**. (The second condition was added on 2026-10-19 by the user's decision J23, after the M5 results showed that a just-died peer otherwise cost a full timeout per query for up to `peer_stale_secs`.)
  - A target the registry has **never** tried yet, so the first query after startup still reaches everyone.
  - Targets that are only failing are left to the background refresh and skipped at query time.
  - This fixes the 5 s stale-peer delay from section 14 of `p2p_rag_plan.md`.
- Pins live in memory only (section 11).

### 6.2 Two-round query (`RagService::query`, `retrieval = "two_round"`)

Run at the same time:
- (a) local retrieval, unchanged (`top_k`, no proofs);
- (b) `POST /embed [query]` on the own sidecar, giving `q`;
- (c) round 1 `centroids` to every selected target, each bounded by `select_timeout_ms`.

Then:

1. **Check round 1, per peer.**
   - The peer must have a registry root, and `root_version` must equal it.
     - Behind or unknown: fetch `hello` once inline, then check again.
     - If it still doesn't match: status `no_root`, skip the peer.
   - For each centroid: `ClusterEntry::verify(root)`.
     - Failure: flag `bad_centroid_proof` and drop that centroid.
   - Score each verified centroid as `sim = dot(q, centroid)`, computed by the asker.
2. **Select.**
   - Rank all verified candidates from all peers by `select_by` (`"centroid"` uses `sim`; `"best_chunk"` uses the claimed `best_chunk_score`).
   - Take the top `max_clusters`, drop those below `min_centroid_score`, and always keep the single best.
   - Group by peer.
3. **Round 2:** one `chunks` request per peer with winning clusters, all at the same time, each bounded by `peer_timeout_ms`.
4. **Check round 2, per chunk.**
   - Its `cluster_id` must be one that was requested.
   - The cluster entry's `subtree_root` must equal the one verified in round 1.
   - `verify_chunk(...)` against that `subtree_root`.
   - On failure: `verification = "failed_proof"`, and the chunk is dropped if `drop_unverified`.
5. **Rescore** (if `rescore`): one `POST /embed` with all verified remote chunk texts; `score = dot(q, v)`.
   - Keep the peer's number as `claimed_score`.
   - If `|claimed − score| > score_tolerance`: flag `score_mismatch` on the peer and the chunk. The chunk is kept with the recomputed score.
   - For each cluster, if the claimed `best_chunk_score` is above the best recomputed score among that cluster's returned chunks by more than `score_tolerance`: flag `score_overclaim`. The peer claimed a better chunk than it delivered.
6. **Freshness.** If the root a chunk was verified against was first seen **during this query** (step 1 fetched it inline), mark `verification = "verified_new_root"` instead of `"verified"`. Drop such chunks when `require_prior_root = true`.
7. Merge local and remote chunks with the existing `merge_chunks(…, final_k)` and generate, as today.

### 6.3 Flat mode (`retrieval = "flat"`)

One round of `kind: "search"`, with the same verification (cluster entries, then chunk proofs, then rescore). It keeps the old one-round behaviour, plus verification, as the **baseline for evaluation**.

### 6.4 Output changes

`Chunk` gains optional fields:
- `cluster_id: Option<u32>`
- `verification: Option<String>`: `"local" | "verified" | "verified_new_root" | "failed_proof" | "unverified"`
- `claimed_score: Option<f64>`

All of them get `#[serde(default, skip_serializing_if = "Option::is_none")]`.

`PeerResult` gains:
- `root_version: Option<u64>`
- `offered_clusters: usize`
- `selected_clusters: Vec<u32>`
- `rejected_chunks: usize`
- `flags: Vec<String>`
- `round1_ms`, `round2_ms`

`status` gains `"skipped_stale"` and `"no_root"`.

`Timings` gains `select` (round 1 plus selection), `fetch` (round 2) and `rescore`, and keeps `peer_retrieve` as their sum.

`GET /rag/status` adds `own_root` (`SignedRoot`) and `registry`: per peer, the version, a short root, first-seen and last-ok times in seconds ago, and flags.

## 7. Config additions (`[rag]`, all with serde defaults so existing TOML still loads)

```toml
retrieval = "two_round"     # "two_round" | "flat"
n_clusters = 0              # 0 = auto, round(sqrt(num_chunks)) clamped to [1, 64]
centroids_per_peer = 3      # round 1: centroids each peer returns
max_clusters = 5            # round 1: clusters kept overall
min_centroid_score = 0.2    # round 1: cosine threshold (the single best cluster is always kept)
select_by = "centroid"      # "centroid" (verifiable) | "best_chunk" (claimed, checked in round 2)
chunks_per_cluster = 3      # round 2
select_timeout_ms = 2000    # timeout for hello and round 1
rescore = true              # re-embed remote chunks and recompute their scores
score_tolerance = 0.01      # allowed |claimed - recomputed| before flagging
drop_unverified = true      # drop chunks whose proof fails
require_prior_root = false  # true: drop chunks under a root first seen during this query
root_refresh_secs = 60
peer_stale_secs = 180
adversary = "none"          # EXPERIMENTS ONLY, see section 9

# Added in Phase M8 (section 15)
# retrieval also accepts "directory"
directory = false           # keep a verified copy of every peer's centroids (needed for retrieval = "directory")
directory_fallback = true   # directory mode: peers without a fresh directory entry get round 1
```

Validation in `config.rs`:
- every count > 0;
- `0 ≤ min_centroid_score ≤ 1`;
- the enum strings are known;
- `n_clusters ≤ 64`;
- warn if `adversary != "none"`.

Pass `n_clusters` to the sidecar as a `rag_engine.py serve --n-clusters` argument (`sidecar.rs::spawn_sidecar`).

## 8. Phases

Each phase ends with a check. Don't start a phase until the previous check passes. Update the checklist in section 13 as you go.

### Phase M1 — Merkle tree and clustering in the store (Python only)
- `rag/clustering.py`, `rag/merkle.py`, store changes (3.3), `scripts/rag_merkle_vectors.py`, `tests/fixtures/merkle_vectors.json`, the tests from 3.5.
- **Check:** `.venv-rag/bin/python -m unittest tests/test_rag_store.py tests/test_rag_merkle.py` passes. Ingesting `corpus/node1` gives about 13 clusters, and the root is unchanged after a sidecar restart.

### Phase M2 — Sidecar routes
- `/commitment`, `/embed`, `/centroids`, `/search_clusters`, `/retrieve?with_proofs`, plus additions to `/health` and `/ingest` (3.4).
- **Check:** with `curl`, a node-1 question returns sensible centroids from `/centroids`. `/search_clusters` on the top cluster returns relevant chunks. A small Python one-off folds a returned proof back to the `/commitment` root.

### Phase M3 — Rust verification library
- `src/rag/merkle.rs` (section 4).
- **Check:** `cargo test --release` passes, including the fixture tests, so Python and Rust agree byte for byte.

### Phase M4 — Wire protocol v2 and own root
- Protocol enums, the responder for all four kinds, `request_peer`, `SecretKey` in `RagService`, `refresh_own_root` (5, 5.1).
- **Check:** two nodes on one machine. Node 1's `/rag/status` shows its `own_root`. A temporary debug call (or unit test) for `hello` from node 1 to node 2 returns a root that verifies against node 2's EndpointId.

### Phase M5 — Root registry and stale peers
- `registry.rs`, the background refresh, target filtering (6.1), the registry in `/rag/status`.
- **Check:** with a remembered-but-dead peer (like `dd0020…` from Phase 4), retrieval time drops from about 5001 ms to the normal ~60 ms after the first refresh. `/rag/status` lists the dead peer as failing.

### Phase M6 — Two-round query and flat mode
- The pipeline in 6.2 and 6.3, output changes (6.4), config (7).
- **Check:**
  - All 15 questions in `corpus/questions.jsonl` run on 2 nodes in both modes.
  - Every remote context is `verified`, and `node2_only` questions still score 6/6.
  - `peers[].selected_clusters` shows that round 2 went only to peers whose clusters won.
  - Cross-node results are recorded, whatever they are; this plan doesn't promise to fix them.
  - Repeat across the two machines (Phase 4 setup).

### Phase M7 — Simulated adversaries
- The `adversary` modes (section 9).
- **Check:** each attack is detected as the table predicts, with the expected flag in `peers[].flags`, and the answer contains no tampered text.

### Phase M8 — Centroid directory mode
- Full design, steps and check in **section 15**.

### Phase M9 — Evaluation of all three modes on the HPC (feeds Phases 5–6 of `p2p_rag_plan.md`)
- Full plan in **section 16**. It replaces the old M8 ("evaluation"), adds `directory` as a third mode, and runs on the user's dataset and HPC.

## 9. Simulated adversaries (`[rag] adversary`, experiments only)

These run in the **responder's** Rust code after its sidecar answers, so Python is unchanged. Never set them outside an experiment.

| Mode | What the peer does | Expected detection on the asker |
| :-- | :-- | :-- |
| `tamper_text` | changes one word in every returned chunk | `failed_proof`; chunk dropped |
| `inflate_score` | adds 0.3 to every chunk `score` and `best_chunk_score` | `score_mismatch`; recomputed scores used; `score_overclaim` |
| `fake_centroid` | replaces each centroid with the query embedding (via its own `/embed`) to always win selection | `bad_centroid_proof`; centroid dropped, so the peer is not selected |
| `equivocate` | signs a different root under the same version on every other `hello` | `equivocation`; both signed roots in `evidence`; peer's chunks refused |
| `hide_cluster` (M8) | leaves its highest-numbered cluster out of every `directory` answer | `incomplete_directory`; the other entries are still used |

In M8, `fake_centroid` also corrupts `directory` answers, following J34.

## 10. Files touched

| File | Change |
| :-- | :-- |
| `rag/clustering.py` | new (3.1) |
| `rag/merkle.py` | new (3.2) |
| `rag/store.py` | clusters, commitment, `commit()`, new search methods (3.3) |
| `rag/server.py` | new routes (3.4) |
| `rag_engine.py` | `--n-clusters`; `cmd_ingest` calls `commit()` |
| `scripts/rag_merkle_vectors.py`, `tests/fixtures/merkle_vectors.json` | new (2.5) |
| `tests/test_rag_merkle.py`, `tests/test_rag_store.py` | new / extended (3.5) |
| `src/rag/merkle.rs` | new (4) |
| `src/rag/registry.rs` | new (6.1) |
| `src/rag/protocol.rs` | v2 wire enums, responder for all kinds, adversary hooks (5, 9) |
| `src/rag/mod.rs` | two-round and flat pipelines, verification, rescore, output fields (6) |
| `src/rag/sidecar.rs` | client methods for the new routes; `--n-clusters` |
| `src/config.rs` | new `[rag]` fields and validation (7) |
| `src/main.rs` | `SecretKey` into `RagService`, `refresh_own_root`, start the registry task |
| `src/network/mesh.rs` | share the transport-key helper |
| `src/api.rs` | `/rag/status` additions; re-sign after `/rag/ingest` |
| `CLAUDE.md`, `docs/p2p_rag_plan.md` | ALPN is now `/slakshna/rag/2`; mention this plan |

New files in `docs/` need `git add -f` (`.gitignore` ignores `/docs`).

## 11. What this does not protect against (say so in any write-up)

- **Poison committed in advance.** A peer can ingest harmful documents before it publishes its root. The proofs then pass. This scheme makes peers *commit*; it doesn't make their content *true*. Trust scores are still needed.
- **Hiding chunks.** A Merkle proof shows that a chunk *is in* the corpus, not that it is the *best* chunk. A peer can leave out relevant chunks or hide a whole cluster in round 1. That can't be detected without revealing much more of the corpus. In directory mode (D14) hiding a *cluster* is detected, but hiding *chunks* within a cluster still is not. `size` in a cluster entry is not committed, so chunk-level completeness would need it in the cluster hash (section 12).
- **Directory mode reveals topic maps (D12).** Any allowed peer can fetch every centroid of a node with `directory = true`. That is a summary of everything the node holds. Use `allowed_peers` if this matters.
- **First contact (trust on first use).** The first root the asker sees from a peer is accepted as is. Pins live in memory only, so after a restart the asker trusts again on first use. Persisting pins in RocksDB is a later step.
- **Root rebuilt per query.** A peer could build and sign a new root for every query. That shows up as `verified_new_root` and a version that keeps climbing. Only `require_prior_root = true` blocks it, at the cost of ignoring peers that just re-ingested.
- **Privacy.** Round 1 still sends the raw query to every live peer. Centroids reveal a peer's topics. Chunks are raw text. This is unchanged from v1.
- **Asking cost.** Rescoring re-embeds up to `max_clusters × chunks_per_cluster` (15) chunks on the asker's CPU, roughly 0.1–0.5 s. That's still small next to 12–30 s of generation.

## 12. Later extensions (out of scope here)

- Assign new chunks to the nearest existing centroid and re-cluster only when clusters drift. This gives an append-only log, where a consistency proof shows the new root extends the old one.
- Keep pinned roots and evidence in RocksDB.
- Feed `flags` into the trust scores in `src/history.rs`.
- Share of `final_k` per node or per cluster, to target the cross-node failures.
- Send the query embedding in round 1 instead of the text.
- Directory mode: announce new root versions over gossip instead of polling every peer each minute. Use an ANN index (HNSW) over the directory if M9 shows brute-force scoring matters (it shouldn't below ~10⁵ centroids). Commit `size` in the cluster hash for chunk-level completeness. A fully distributed index (each node owns a region of embedding space) only beyond ~10⁵ nodes.
- Membership at scale: a node only queries peers it knows (gossip neighbours plus remembered peers). Beyond a few dozen nodes a real membership mechanism is needed; M9 works around it by listing every peer in the config.

## 13. Status checklist

- [x] Phase M1 — clustering and Merkle tree in the store (done 2026-10-18; see section 14)
- [x] Phase M2 — sidecar routes (done 2026-10-18; see section 14)
- [x] Phase M3 — Rust verification library (done 2026-10-18; see section 14)
- [x] Phase M4 — wire protocol v2 and own signed root (done 2026-10-18; see section 14)
- [x] Phase M5 — root registry and stale-peer skipping (done 2026-10-18; see section 14)
- [x] Phase M6 — two-round query and flat mode (done 2026-10-19, on one machine and across both; see section 14)
- [x] Phase M7 — simulated adversaries (done 2026-10-22; see section 14)
- [ ] Phase M8 — centroid directory mode (section 15)
- [ ] Phase M9 — evaluation of all three modes on the HPC (section 16)

## 14. Results

### Phase M1 (2026-10-18, machine 1)

Done: `rag/clustering.py`, `rag/merkle.py`, the store changes from 3.3, `scripts/rag_merkle_vectors.py`, `tests/fixtures/merkle_vectors.json`, `tests/test_rag_merkle.py`, and new tests in `tests/test_rag_store.py`.

Check:
- `.venv-rag/bin/python -m unittest tests/test_rag_store.py tests/test_rag_merkle.py`: 38 tests pass (5 runs in a row, to rule out k-means flakiness).
- `rag_engine.py ingest --path corpus/node1` into an empty index: 161 chunks in **13 clusters**, sizes 15, 10, 17, 7, 15, 12, 14, 2, 11, 13, 4, 34, 7. It took 13.6 s, most of it loading the model. Root `802f7d37…`.
- The sidecar was started, stopped and started again on that index. It logged the same root and version both times, and `/retrieve` still worked.
- Copies of the existing `data-rag-node1` and `data-rag-node2` indexes (built before clustering) are migrated once on load. Node 1 gets the **same root** as the fresh ingest (`802f7d37…`), so clustering is repeatable on this machine. Node 2 has 174 chunks in 13 clusters, root `5172ade3…`. The real `data-rag-node*` folders were not touched; they migrate by themselves the next time a node starts.
- `cargo test --release` still passes (6 tests). `cargo fmt --check` fails, but on Rust code that was already committed before M1 (every file, from its `{ A, B }` brace style). M1 changes no Rust.

Differences from the plan, and additions (logged in `docs/decisions.md`, J2–J8):
- `rag/server.py` changed already in M1. Its `/ingest` with `documents` now calls the new `add_documents`, which adds and then commits once. Without that change, documents ingested this way would never be saved, because `add_document` no longer saves.
- `meta.json`'s `commitment` also stores `n_clusters_setting`. If the configured `n_clusters` differs on the next load, the store re-clusters and the version goes up. This is so the M8 sweep only needs a restart.
- `rag_engine.py` has `--n-clusters` now (both `ingest` and `serve`), and its startup log shows the cluster count, root and version. `sidecar.rs` passes nothing yet; that comes with the config in M6.
- FAISS's "too few points per centroid" warning is turned off with `min_points_per_centroid=1`. That setting only controls the warning, not the result.
- `search()` without proofs returns exactly what it did before; the internal `cluster` field is removed from its results. With proofs it returns `{"chunks", "clusters"}`, the same shape as `search_clusters`.

### Phase M2 (2026-10-18, machine 1)

Done: `/commitment`, `/embed`, `/centroids`, `/search_clusters`, `/retrieve` with `with_proofs`, `commitment` in `/health`, and `root_version` in `/ingest`. New `tests/test_rag_server.py` tests the routes in-process (FastAPI test client, fake embedder).

Check, against the real sidecar on the M1 scratch index (161 chunks, 13 clusters, root `802f7d37…`):
- The three Python test files pass: 46 tests.
- `/centroids` with `n = 3` for three `node1_only` questions. The best cluster, by the asker's own `dot(q, centroid)`, is the right topic every time:

  | Question | Best clusters (asker's similarity) | Documents in the best cluster |
  | :-- | :-- | :-- |
  | q01 Chandrayaan-3 landing date | 1 (0.740), 4 (0.730), 0 (0.665) | Chandrayaan-2 ×7, Chandrayaan-3 ×3 |
  | q03 Mars Orbiter Mission name | 5 (0.720), 4 (0.532), 1 (0.495) | Mars Orbiter Mission ×12 |
  | Aditya-L1, what it studies | 2 (0.395), 10 (0.330), 11 (0.291) | Aditya-L1 ×16, Mars Orbiter ×1 |

- `/search_clusters` on each best cluster returned 3 on-topic chunks (scores 0.47–0.77).
- An independent script (only `hashlib`/`struct`, no `rag/` imports) folded every returned proof to the `/commitment` root: 29 of 29 for 9 centroids, the cluster entries, 9 chunks from `/search_clusters` and 5 from `/retrieve` with proofs. Each chunk with one character added was rejected.
- A stale `root_version` gets HTTP 409, `{"detail": {"error": "stale_root", "root": <current commitment>}}`. `/ingest` of one new document returned a new `root_version` and the root changed. The old version then got 409, and ingesting the same document again changed nothing.
- `cargo test --release` still passes. No Rust changed.

Observed, relevant for M6/M8: for q01 the cluster with the best centroid (1, 0.740) is not the one with the best chunk. Cluster 4 has a lower centroid similarity (0.730) but a better chunk (0.807 against 0.769). This is the case D6 and `select_by = "best_chunk"` were meant for. With `max_clusters = 5`, both clusters would be selected.

Differences from the plan, and additions (logged in `docs/decisions.md`, J9–J11):
- The store's lock is public (`store.lock`). Routes that report a `root_version` hold it while reading the version and the results, so the two always match. The query is embedded before the lock is taken.
- The plan fixed the 409 body only for the peer protocol. On the sidecar it is `{"detail": {"error": "stale_root", "root": <commitment summary>}}`, so Rust can update its own view without a second request.
- An unknown cluster id gets HTTP 400. A negative `n` or `per_cluster` gets 422 (FastAPI's validation error).

### Phase M3 (2026-10-18, machine 1)

Done: `src/rag/merkle.rs`, as in section 4. It has `ProofStep`, `leaf_hash`, `inner_hash`, `cluster_hash`, `fold_proof`, `ClusterEntry::verify`, `verify_chunk`, `SignedRoot` (`sign`, `verify`, `message_bytes`) and `dot`. It is registered in `src/rag/mod.rs`.

Check:
- `cargo test --release`: 15 tests pass, 9 of them new. The fixture tests check every leaf, inner and cluster hash, every tree root (Rust builds the trees again with a test-only builder), every proof, the centroid values, the edge cases (empty tree, 1 leaf, an odd node moving up, 1 cluster, empty corpus) and the signed-root message bytes. So Python and Rust agree byte for byte.
- The tests catch format mistakes. With the leaf marker changed to `0x03`, 3 tests failed; with the length prefixes made little-endian, 4 failed. The file was restored afterwards.
- Signing: a sign/verify round trip works, also after a JSON round trip. Verification fails for another key, a changed `version`, a changed `embedding_model`, a malformed signature and a malformed root.
- `rustfmt --edition 2021 --check src/rag/merkle.rs` is clean. The release build still has the same 9 warnings it had before, none from `merkle.rs`. The Python tests still pass.

Differences from the plan, and additions (logged in `docs/decisions.md`, J12–J13):
- `SignedRoot::message_bytes` returns a `Result`, because `root` is a hex string in JSON and may not be valid. The byte layout is in a free function, `root_message(...)`, which the fixture test calls directly.
- A `Commitment` struct (the unsigned `/commitment` reply) is the input to `SignedRoot::sign`.
- The module has `#![allow(dead_code)]` until the protocol uses it (M4/M6), so the build gains no warnings in the meantime.

### Phase M4 (2026-10-18, both nodes on machine 1)

Done:
- `src/rag/protocol.rs` speaks v2 (`/slakshna/rag/2`). Requests and responses are tagged by `kind`, and the responder serves `hello`, `centroids`, `chunks` and `search` from its sidecar, with the caps from section 5. `request_peer` replaces `query_peer`.
- `RagService` holds the transport key and `own_root`, and gains `refresh_own_root`. `main.rs` signs the root after the startup ingest and before the network starts, and `/rag/ingest` re-signs.
- `Keypair::transport_secret_key()` in `src/identity.rs` is the shared helper, used by both `mesh.rs` and `main.rs`.
- Sidecar client methods for `/commitment`, `/centroids`, `/search_clusters` (with 409 → stale root) and `/retrieve` with proofs.
- `GET /rag/status` shows `own_root`. A new `GET /rag/debug/hello` fetches every peer's root and checks its signature.

Check (examples/rag-2node configs with `generator = "none"`, real `data-rag-node1/2` folders):
- Both nodes signed their root before the network came up: node 1 `802f7d37…` (161 chunks, 13 clusters), node 2 `5172ade3…` (174 chunks, 13 clusters). These are the same roots M1 got from copies of these indexes. On this first start the real indexes were migrated in place.
- Node 1's `/rag/status` shows `own_root`, equal to its sidecar's commitment.
- `hello` from node 1 to node 2 (`dd0020…`) returned node 2's root at its current version with `signature_valid = true`, in 3–4 ms. The reverse direction also worked.
- An independent check with Python's `cryptography` package rebuilt the message bytes from section 2.4. Both signatures verify against their own EndpointId and fail against the other node's key or with `version + 1`.
- Re-signing: node 2 was restarted on a scratch copy of its data folder (same identity; same root after the restart). One document was ingested through `/rag/ingest`, giving version 1792345228347 and root `768db7ea…`. Node 2 logged the new signed root at once, and node 1's next `hello` got it with a valid signature.
- `/rag/query` on node 1 for q07 (a `node2_only` question) still works over v2 `search`: 5 contexts, all from node 2, containing the answer. Peer round trip 30 ms.
- `cargo test --release`: 20 tests pass (5 new wire-format tests). The Python tests pass. The build has no new warnings.

Notes:
- Node 1 still pins machine 2's node (`313823…`), which was not running. Every `hello` and query waited the full 5001 ms for it. This is the stale-peer delay M5 fixes.
- Until M6, `/rag/query` sends a `search` request and uses the returned chunks **without checking their proofs**, as v1 did. M6 adds the verification.

Differences from the plan, and additions (logged in `docs/decisions.md`, J14–J18):
- A failed request is answered with `kind: "error"` and the reason in `error`. `stale_root` is the one exception: it comes back as `kind: "chunks"` with the peer's current `root`. `request_peer` reports it as a typed `StaleRoot` error carrying that root.
- The responder re-signs whenever the sidecar answers under a version it has not signed, for example after an ingest that bypassed the node. A stale-root refusal signs the commitment the sidecar returned with its 409.
- `GET /rag/debug/hello` is kept rather than removed after the check, for the two-machine run in M6. `hello` uses `peer_timeout_ms` until `select_timeout_ms` exists (M6, config).

### Phase M5 (2026-10-18, both nodes on machine 1)

Done:
- `src/rag/registry.rs`: `PeerRoot` and `RootRegistry`, with the accept rules from 6.1 (bad signature, rollback, equivocation with evidence, higher version) and target selection (`should_ask`).
- The background task `run_root_refresh` (5 s after startup, then every `root_refresh_secs`) sends `hello` to every target with `select_timeout_ms`. It is started in `main.rs` after the network.
- `/rag/query` skips peers the registry says are down and reports them as `skipped_stale`. Every query result also updates the registry.
- `/rag/status` has a `registry` list.
- New `[rag]` settings: `select_timeout_ms` (2000), `root_refresh_secs` (60), `peer_stale_secs` (180).

Check (examples/rag-2node configs with `generator = "none"`; node 1 remembers and pins machine 2's node `313823…`, which is not running):

| q07 asked of node 1 | `peer_retrieve` | Peers |
| :-- | --: | :-- |
| Right after startup, before the first refresh | 5001 ms | node 2 ok (410 ms, first connection); `313823…` timeout |
| After the first refresh | 30 ms | node 2 ok; `313823…` `skipped_stale` |
| Again | 23 ms | the same |

`/rag/status` lists `313823…` with `status: "failing"`, `last_error: "hello timed out"` and `asked_in_queries: false`, and node 2 with `status: "ok"` and its accepted root and version.

Recovery, with node 1 on short timers (`root_refresh_secs = 10`, `peer_stale_secs = 20`):

| Time | Node 2 | `peer_retrieve` | Node 2's entry |
| :-- | :-- | --: | :-- |
| 0 s | up | 34 ms | ok |
| 13 s | stopped at 1 s | 5001 ms | timeout (its last answer is still under 20 s old) |
| 33 s | down | 0 ms | `skipped_stale`; answer from local chunks |
| 37 s | restarted | 0 ms | `skipped_stale` (no refresh yet) |
| 48 s | up | 27 ms | ok, after the next refresh |

- `cargo test --release`: 25 tests pass (5 new registry tests). The Python tests pass. The build still has its 9 old warnings and no new ones. `rustfmt --check` is clean on `registry.rs`.

Observed, worth deciding before M8: between a peer dying and `peer_stale_secs` passing, every query still waits out the full timeout. That is up to 3 minutes of 5 s queries with the defaults; in M6 round 1 the wait is `select_timeout_ms` (2 s). This is the rule in 6.1 as written. A stricter rule would also skip a peer whose latest contact failed, and the refresh would bring it back within `root_refresh_secs`.

**Decided 2026-10-19 (user, J23): the stricter rule.** `should_ask` now also requires that the peer's latest contact did not fail. The same recovery run (short timers, node 2 stopped at 2 s, restarted at 24 s):

| Time | Node 2 | `peer_retrieve` | Node 2's entry |
| :-- | :-- | --: | :-- |
| 0 s | up | 35 ms | ok |
| 4 s | just died | 5001 ms | timeout; this failure marks it |
| 9 s | down | 0 ms | `skipped_stale` |
| 17 s | down | 0 ms | `skipped_stale` |
| 35 s | up, after the next refresh | 33 ms | ok |

A just-died peer now costs one timed-out query, not every query for `peer_stale_secs`. The cost is that a peer which misses a single request is skipped until the next successful refresh, at most `root_refresh_secs` later.

Differences from the plan, and additions (logged in `docs/decisions.md`, J19–J22):
- The three settings above come from section 7 and were added in M5 rather than M6, because the registry needs them. The other section-7 settings stay in M6.
- `last_ok` counts any answer, including one with a bad, rolled-back or conflicting root. Reachability decides who is asked; trust is handled by the flags (checked from M6 on).
- `skipped_stale` (from 6.4) is reported already.
- A rollback is flagged without keeping evidence. Equivocation compares the signed fields, not the signature bytes. Each flag is listed once, and evidence is not duplicated.

### Phase M6 (2026-10-19)

Done:
- `src/rag/retrieval.rs` (new) has the two-round pipeline (6.2) and the flat pipeline (6.3), with shared checks. The pure parts are separate functions with unit tests: `select_clusters`, `check_chunks`, `apply_rescore` and `overclaimed`.
- `src/rag/mod.rs` has the output fields from 6.4: `Chunk.cluster_id`, `verification`, `claimed_score` and `flags`; the new `PeerResult` fields; `Timings.select`, `fetch` and `rescore`; and `retrieval` in the response.
- `src/config.rs` has the section-7 settings (except `adversary`, which comes with M7) and their validation.
- `src/rag/sidecar.rs` has `embed()` and passes `--n-clusters`.
- `scripts/rag_run_questions.py` (new) asks a node every question and saves the full responses.
- `cargo test --release`: 31 tests pass (6 new). The Python tests pass. The build has no new warnings, and the blanket `allow(dead_code)` from M3 is gone. `rustfmt --check` is clean on the new files.

Check: all 15 questions asked of node 1 (`examples/rag-2node` configs, Ollama `qwen2.5:1.5b`, node 2 on the same machine), in both modes:

| Type | two_round | flat |
| :-- | :-- | :-- |
| `node1_only` (q01–q06) | 6 / 6 | 6 / 6 |
| `node2_only` (q07–q12) | **6 / 6** | **6 / 6** |
| `cross_node` (q13–q15) | 0 / 3 | 0 / 3 |

- Every remote context was `verified`: 35 in two_round, 36 in flat. No proofs failed and no flags were raised.
- The recomputed scores matched node 2's own to within about 5·10⁻⁸, far inside `score_tolerance = 0.01`.
- In two_round, round 2 went to exactly the peers with `selected_clusters` (checked for every peer of every question).
- Median timings, two_round: `peer_retrieve` 417 ms = select 41 + fetch 25 + rescore 354. Flat: 237 ms = fetch 39 + rescore 197. Rescoring dominates: it embeds 9 chunks (two_round, 3 clusters × 3) or 5 (flat) on the CPU, inside the plan's 0.1–0.5 s estimate.
- Cross-node answers are wrong in both modes, as in Phases 3–4: q13 "Sriharikota"; q14 "Chandrayaan-3, born Satish Dhawan" (two_round) and "Chandrayaan-3, born in Srinagar" (flat); q15 "No.". For q13 all five contexts were local in both modes. The global top-`final_k` still keeps one side's evidence only.
- Generation times are not comparable between the modes: flat ran second, with mostly the same contexts, and Ollama reused its cached prompts.

Selection with a third node (temporary, on machine 1, indexing node 1's documents, generator `none`), two_round:

| Question | node 2 (scientists) won | node 3 (space missions) won |
| :-- | :-- | :-- |
| q01, q03, q05 (space) | 2 clusters | 3 clusters |
| q07 (Kalam) | 3 clusters | **none, so no round 2** |
| q09, q10 (scientists, park) | 3 clusters | 2 clusters |

- Round 2 skips a peer only when its clusters lose. With 2 peers × `centroids_per_peer = 3` = 6 candidates for `max_clusters = 5`, and MiniLM giving unrelated topics cosines above `min_centroid_score = 0.2`, both peers nearly always win something. This is what D4 warned about, and what the M8 sweep of `max_clusters` and `min_centroid_score` is for.
- A fresh index of `corpus/node1` on node 3 had the same root as node 1 (`802f7d37…`), so the commitment is deterministic for the same documents on the same machine.

Two machines (Phase 4 setup): node 1 on machine 1 (`examples/rag-2node/node1.toml`, unchanged); node 2 on machine 2 at `c8b37dc`, reached over the VPN at its pinned address (`313823…@192.168.3.143:9001`). Machine 1's local test nodes were not running, and node 1 skipped them as `failing`.
- Machine 2's first attempt failed with "peer doesn't support any known protocol". It was still on the M1 build, because the branch had not been pushed. After the push, a rebuild and a restart, `hello` returned its root `64ecc151…` (174 chunks, 13 clusters; machine 2's own index, migrated on that start) in 8 ms, with `signature_valid = true`. The independent Python `cryptography` check agreed.
- All 15 questions in both modes. The answers and scores were identical to the one-machine run:

| Type | two_round | flat |
| :-- | :-- | :-- |
| `node1_only` | 6 / 6 | 6 / 6 |
| `node2_only` | **6 / 6** | **6 / 6** |
| `cross_node` | 0 / 3 (the same wrong answers as on one machine) | 0 / 3 |

- Every remote context was `verified` (35 in two_round, 36 in flat), all from machine 2's node (`slakshna19yp80…`). 0 chunks were rejected and no flags were raised. Round 2 went to exactly the peers with `selected_clusters`.
- Round trips over the VPN: round 1 (or the flat round) had a median of 33 ms (range 21–68 ms); round 2 had a median of 23 ms. Median `peer_retrieve`: 417 ms in two_round (select 40, fetch 24, rescore 359) and 240 ms in flat (fetch 37, rescore 202). These are almost the same as on one machine, because local re-embedding dominates, not the network.
- Not tested yet: node 2 on machine 2 asking node 1 (the reverse direction), and the relay path. Both were also left open in Phase 4.

Differences from the plan, and additions (logged in `docs/decisions.md`, J24–J31):
- The pipelines live in a new `src/rag/retrieval.rs` instead of `src/rag/mod.rs`.
- `/rag/query` takes an optional `retrieval` field to override `[rag] retrieval` for one query.
- Caps on what an asker accepts: at most `centroids_per_peer` centroids (a repeated cluster counts once), `chunks_per_cluster` chunks per cluster, and `top_k` chunks in flat mode. Extra chunks count as `rejected_chunks`.
- Round-2 chunks are checked against the subtree roots verified in round 1; the round-2 cluster entries are not needed.
- A peer flagged `equivocation` is not asked at all (`status: "refused"`).
- `Chunk` also has `flags` (e.g. `score_mismatch`). Local chunks are labelled `verification: "local"`. The response says which `retrieval` mode ran.
- `min_centroid_score` applies to the ranking key (the similarity, or the claimed best score with `select_by = "best_chunk"`).
- `score_overclaim` is also raised when a selected cluster delivered no verified chunk at all, but only if round 2 answered.
- `adversary` is left to M7.

### Phase M7 (2026-10-22, machine 1)

Done:
- `[rag] adversary` (`"none"` by default; `tamper_text`, `inflate_score`, `fake_centroid`, `equivocate`) in `src/config.rs`. A node with a mode other than `"none"` logs `😈 [rag] adversary = …: this node will send corrupted answers to peers. EXPERIMENTS ONLY.` at startup.
- `src/rag/adversary.rs` (new) holds the four corruptions as small functions, plus `RagService::misbehave`, which `protocol.rs` calls on every honest answer before sending it. Python and the asking side are unchanged.
- `/rag/status` now shows a peer flagged `equivocation` as `asked_in_queries: false`, matching the queries that refuse it. Before this fix it showed `true`.
- `cargo test --release`: 35 tests pass. 4 new tests feed each corruption, on fixture data, to the M6 checks: tampered chunks fail their proofs, inflated scores are flagged after rescoring, fake centroids fail their proofs, and a second root under one version is recorded as equivocation with both roots as evidence.

Check: an honest node 1 (generator Ollama) and an adversarial node 2, in an isolated test federation (fresh scratch data, own ports, own federation id). Both nodes were restarted for each mode, so no flags carried over. The six `node2_only` questions were asked in two_round and flat mode:

| Mode | node 2's `flags` | What happened to node 2's data | `FORGED` in contexts or answer |
| :-- | :-- | :-- | :-- |
| `tamper_text` | `bad_chunk_proof` | two_round: every chunk rejected (9, or 7 for q10, per question); flat: 5 of 5 rejected. No remote context. | never |
| `inflate_score` | two_round: `score_mismatch`, `score_overclaim`; flat: `score_mismatch` | Chunks kept (they are genuine). Every remote context has `claimed_score − score = 0.300`; ranking used the recomputed scores. | never |
| `fake_centroid` | two_round: `bad_centroid_proof`; flat: `bad_centroid_proof`, `bad_chunk_proof` | two_round: all 3 offered centroids failed, `selected_clusters = []`, no round 2 sent. Flat: entries failed, so all 5 chunks were rejected. | never |
| `equivocate` | `equivocation` | The first q07 was answered normally (5 verified contexts, "Rameswaram"). The second `hello` returned a validly signed root `ae72ade3…` with the same version as the real `5172ade3…`, recorded as `equivocation` with 2 signed roots in `evidence`. The next q07: node 2 `refused`, not asked. | never |

- Every attack was detected with the flag predicted in section 9, and no tampered text reached a context or an answer.
- `score_overclaim` only exists in two_round, because flat mode has no round-1 claims. That is expected.
- With node 2's chunks rejected or refused, the `node2_only` questions were answered from node 1's own chunks, and mostly wrongly: e.g. q07 "Allahabad", q08 "2008-2012". This is the same failure as the `scope: "local"` baseline in Phase 4. The scheme keeps forged text out; it cannot supply the missing evidence.
- Flags other than `equivocation` are recorded but do not stop a peer being asked. Acting on them is the "trust scores" extension in section 12.

Differences from the plan, and additions (logged in `docs/decisions.md`, J32–J36):
- `tamper_text` writes the marker word `FORGED` in place of each chunk's middle word, so "no tampered text in the answer" can be checked by searching.
- `fake_centroid` corrupts the cluster entries of every answer kind, not only round 1, so flat mode is attacked too.
- `equivocate` signs the real root with its first byte flipped, under the same version. "Every other `hello`" counts all `hello`s this node answers, from any peer.
- The `/rag/status` fix described above.
- The live check ran in an isolated test federation with fresh data rather than with the `examples/rag-2node` configs.

## 15. Phase M8 — Centroid directory mode (design)

Planned 2026-10-22. Decisions D11–D15 (section 1) and K1–K9 in `docs/decisions.md`.

### 15.1 Idea in plain language

In `two_round` mode, round 1 sends the question to every peer just to learn which clusters exist and how close they are to the question. With 1000 peers that means:
- 1000 connections per question;
- about 10 MB of centroids coming back;
- waiting for the slowest of 1000 peers;
- all 1000 peers seeing every question.

But a peer's centroids only change when it ingests documents. So each node can keep a **directory**: a local, verified copy of every peer's centroids, mapping each centroid to its owner and cluster:

```
centroid vector → (peer, cluster_id, subtree_root, root_version)
```

A query in `directory` mode then runs like this:

```
asker: embed the query, score it against every centroid in the directory  (local, a few ms)
asker: pick the best ≤ max_clusters clusters (same selection as two_round)
Round 2  asker → only the owners of those clusters (unchanged)
asker: check proofs, rescore, merge, answer (unchanged)
```

Size estimate at 1000 peers with about 13 clusters each:
- 13,000 centroids × 1.5 KB ≈ **20 MB** in memory;
- scoring one query against all of them is about 5 million multiply-adds, a few milliseconds;
- no ANN index is needed at this size (D13 / section 12).

### 15.2 Fetching and checking a directory

**New request kind** (no ALPN change; see K3):

```json
{"v":2, "kind":"directory", "query_id":"…"}
→ {"v":2, "kind":"directory", "query_id":"…", "node_id":"…", "embedding_model":"…",
   "root_version": …, "clusters": [ClusterEntry, …], "error": null}
```

`clusters` lists every cluster of the peer's current commitment, in the 3.3 shape, without `best_chunk_score`.

Size:
- about 3.5 KB per entry, so about 45 KB for 13 clusters and about 225 KB for the maximum of 64, under `max_message_bytes` (1 MiB);
- with a larger embedding model (M9 may use one), it is about `dim × 8` bytes of hex per entry, so check against `max_message_bytes`.

**Sidecar:** new `GET /clusters` returns `{root_version, clusters}`, built from `store.all_clusters()`. It holds the store lock while reading the version and the entries (J9).

**When it is fetched (D13):**
- In `run_root_refresh`, after each `hello` round: for every peer whose accepted root version differs from its directory version (or that has none), send `directory` to all of them at once, bounded by `peer_timeout_ms`.
- During a directory query, a peer that falls back to round 1 (15.3) also gets a background `tokio::spawn` fetch, so the next query can use the directory.
- Only when `[rag] directory = true`.

**Checks on a fetched directory**, against the peer's accepted `SignedRoot` from the registry:
1. `root_version` must equal the registry's accepted version. If the peer is ahead, fetch `hello` once (as `root_at` does) and compare again. If it still differs, discard the answer and record status `no_root`.
2. **Completeness (D14).** The entries' `cluster_id`s must be exactly `0..num_clusters-1`, each once, where `num_clusters` comes from the signed root. Missing, extra or repeated ids raise the flag `incomplete_directory`. Entries that verify are still kept.
3. **Each entry.** `ClusterEntry::verify` (existing) **plus the position check:** the proof's sequence of `left` bits must equal `expected_sides(cluster_id, num_clusters)`:
   ```
   expected_sides(i, n):
       sides = []
       while n > 1:
           if i is odd:        sides.push(true)        # sibling on the left
           elif i + 1 < n:     sides.push(false)       # sibling on the right
           # else: no step, the node moves up unchanged
           i = i // 2;  n = (n + 1) // 2
       return sides
   ```
   A failure raises `bad_centroid_proof`, and the entry is dropped.
4. Store the verified entries as decoded `f32` vectors, with their `subtree_root`s, under the root version.

The position check only rejects dishonest proofs: an honest `make_proof` always produces exactly this shape. For consistency it is used in **all three modes** (`two_round` round 1, `flat` cluster entries, `directory`) through one function, `ClusterEntry::verify_at(root, dim, num_clusters)`. M8 must rerun the M6 checks to show nothing changed for honest peers.

Why the check works:
- A tree's root also fixes how many leaves it has. If a peer signs `num_clusters = n` but built a tree with more leaves, at least one of the `n` proofs (in practice the last one, which walks the right-hand edge) cannot have the expected shape and still fold to the root.
- M8 must confirm this with a unit test, not by argument alone: for every `n` from 1 to 40, build an honest tree with `n + 1` leaves, check its real proofs against a claimed `n`, and assert that at least one fails.

### 15.3 Query flow (`retrieval = "directory"`)

Run at the same time: local retrieval (unchanged) and `POST /embed [query]` on the own sidecar, giving `q`.

1. **Targets.** Same as today: `split_targets` (registry `should_ask`; equivocators `refused`, down peers `skipped_stale`).
2. **Split the targets:**
   - *directory peers*: those with a directory entry whose version equals the registry's accepted version;
   - *fallback peers*: the rest. If `directory_fallback`, they get the existing `round1()`, in parallel with step 3. Otherwise they are reported as `no_directory` and not asked.
3. **Score locally.** One `Candidate` per directory entry, with `sim = dot(q, centroid)` and `claimed_best = None`, plus the candidates from fallback peers' round 1.
   - `select_by = "best_chunk"` has no claims for directory entries, so `select_clusters` uses `sim` for them (its existing `unwrap_or(c.sim)`). Note this in the response's flags once per query (`select_by_centroid_for_directory`), so results aren't misread.
4. **Select** with the existing `select_clusters(...)`.
5. **Round 2** with the existing `round2()`. A `PeerState` for a directory peer is built from its directory entry: `offers` holds every cluster with its `subtree_root` and centroid, and `root_version` is the directory's version. So `check_chunks` and the subtree-root check are unchanged.
6. **A stale root in round 2** (the peer re-ingested after our last refresh) is reported as today (`error`, "stale_root …"). The new root it carries is recorded in the registry, and a directory fetch for that peer is spawned. That query gets no chunks from that peer. There is no inline retry, to keep it simple.
7. Rescore, merge and generate exactly as `two_round`.

Output additions (section 6.4 extended):
- `PeerResult.selected_via: Option<String>`: `"directory"` or `"round1"` (fallback, and always in `two_round`).
- `PeerResult.bytes_sent` and `bytes_received` (u64): request and response bytes, summed over every request to that peer in this query. Counted in `request_peer`, for **all modes**, because M9 needs bytes per query.
- `status` gains `"no_directory"`.
- In directory mode `Timings.select` is the local scoring plus any fallback round 1.
- `GET /rag/status` gains `directory`:
  - `enabled`, `total_centroids`, `approx_bytes`;
  - per peer: `root_version`, `clusters`, `complete`, `fetched_secs_ago`, `flags`.

### 15.4 Adversary additions (section 9)

- `fake_centroid` also corrupts `directory` answers (J34). Expected result: `bad_centroid_proof`, the peer has no usable entries, it falls back to round 1, fails there too, and gets no round 2.
- New `hide_cluster`: drop the entry with the highest `cluster_id` from every `directory` answer. Expected result: `incomplete_directory`, and the remaining entries are used.
- `tamper_text`, `inflate_score` and `equivocate` need no change. They must still be detected in directory mode.

### 15.5 Files touched

| File | Change |
| :-- | :-- |
| `rag/store.py` | `all_clusters()` |
| `rag/server.py` | `GET /clusters` |
| `tests/test_rag_store.py`, `tests/test_rag_server.py` | `all_clusters` returns `num_clusters` entries, ids `0..k-1`, each folding to the root; `/clusters` round trip |
| `src/rag/merkle.rs` | `expected_sides`, `ClusterEntry::verify_at`; tests incl. the `n` vs `n+1` test and "every fixture proof has the expected shape" |
| `src/rag/protocol.rs` | `RequestBody::Directory`, `ResponseBody::Directory`; responder; byte counting in `request_peer` (return the counts alongside the response) |
| `src/rag/sidecar.rs` | `clusters()` client |
| `src/rag/directory.rs` (new) | `CentroidDirectory`: per-peer verified entries; `update(peer, signed_root, response) -> (entries kept, flags)`; `candidates(q, peers)`; `status()`; unit tests; an `#[ignore]` scale test (below) |
| `src/rag/retrieval.rs` | `directory()` pipeline (15.3); `verify_at` in round 1 and flat; `selected_via`, byte fields |
| `src/rag/mod.rs` | `RetrievalMode::Directory`; `PeerResult` fields; directory fetch in `run_root_refresh`; directory in `RagService` |
| `src/rag/adversary.rs` | `hide_cluster`; `fake_centroid` on `directory` |
| `src/config.rs` | `directory`, `directory_fallback`; `Adversary::HideCluster`; validation: `retrieval = "directory"` requires `directory = true` |
| `src/api.rs` | `directory` in `/rag/status`; a per-query `"retrieval": "directory"` with `directory = false` returns HTTP 400 `directory_disabled` |
| `scripts/rag_run_questions.py` | accept `--retrieval directory` |
| `examples/rag-2node/node*.toml` | `directory = true` (retrieval stays `two_round`), so all three modes can be run per query |
| `CLAUDE.md`, `docs/explanation_merkle_tree.md` | describe the third mode |

### 15.6 Steps and check

Steps, in order. Run the unit tests after each:
1. **M8a (Python):** `all_clusters`, `/clusters`, tests.
2. **M8b (Rust, `merkle.rs`):** `expected_sides`, `verify_at`, tests. Switch round 1 and flat to `verify_at`.
3. **M8c (protocol):** the `directory` kind, responder, sidecar client, byte counters, adversary additions.
4. **M8d (`directory.rs`):** the directory, refresh integration, `/rag/status`.
5. **M8e (retrieval):** the `directory` pipeline, fallback, config, API, script.

**Check (all must pass and be recorded in section 14 as "Phase M8"):**
1. Python tests, `cargo test --release`, and `rustfmt --check` on the new files all pass.
2. **Scale test:** `cargo test --release directory_scale -- --ignored --nocapture`.
   - It fills a directory with 1000 peers × 13 random unit centroids (dim 384), checks every entry's proof (built with a test-only tree builder), then times `candidates()` + `select_clusters()` over 100 random queries.
   - Record the time to check all entries, the median and p95 per-query scoring time, and the memory estimate.
   - Expected: scoring under 10 ms per query in release builds.
3. **Live, `examples/rag-2node` + a temporary node 3** (as in M6), generator Ollama:
   - (a) All 15 questions in **all three modes**. `node1_only` and `node2_only` should score as in M6 (6/6 each). Every remote context is `verified`. Two_round and flat match M6, so `verify_at` changed nothing.
   - (b) In directory mode, after the first refresh: every peer has `selected_via = "directory"` and `round1_ms = null`, and round 2 went only to peers with `selected_clusters`.
   - (c) Compare per question the clusters selected by `directory` and by `two_round`. They should agree except when a peer's 4th-or-later cluster ranks in the global top `max_clusters`, which two_round can't see. Report the agreement rate and every difference.
   - (d) Median `select` time: directory vs two_round. Bytes per query in all three modes, from the new byte fields.
   - (e) **Startup:** a query before the first refresh uses the fallback (`selected_via = "round1"`); a query after it uses the directory.
   - (f) **Re-ingest:** ingest one document on node 2 through `/rag/ingest`. A query before node 1's next refresh shows the stale-root case from 15.3 step 6 (or, if the refresh already ran, a fresh directory at the new version). The next query after a refresh uses the new version. `/rag/status` shows the new `root_version` for node 2's directory.
4. **Adversaries** in an isolated test federation (J36), directory mode: `fake_centroid` and `hide_cluster` detected as predicted in 15.4; `tamper_text`, `inflate_score` and `equivocate` detected as in M7; `FORGED` never appears in a context or answer.

## 16. Phase M9 — Evaluation of all three modes on the HPC (plan)

Planned 2026-10-22 (D16). This also covers Phases 5 (4/6/8 nodes) and 6 (evaluation) of `p2p_rag_plan.md`. Start only after M8 is done. The questions in 16.1 must be answered first, because several steps depend on them.

### 16.1 Inputs needed before starting (ask the user and record the answers here)

**Dataset**
- Name, source and licence.
- Format (PDF / text / JSON).
- Number of documents and total size.
- Language.
- Are there **questions** with **gold answers**, and with **gold supporting documents or passages**? Recall needs gold passages or a single-index baseline. Answer accuracy needs gold answers.
- Is there a natural split across "owners" (e.g. per institution or per topic) that should define the nodes?

**HPC**
- Scheduler (SLURM?).
- Number of compute nodes available, and cores, RAM and GPU per node.
- Job time limits.
- Shared filesystem.
- Container support (Apptainer / Singularity).
- Internet access from compute nodes, for Hugging Face models, pip, cargo.
- Can processes on **different compute nodes** reach each other over **UDP** (Iroh/QUIC needs it)? Which port ranges?
- Is there a login node where builds run, and is it the same architecture as the compute nodes?

**Models**
- Embedding model. Keep MiniLM (comparable with M1–M8) or use a stronger one? It must be the same on every node. A larger `dim` affects message sizes (15.2) and the fixture-free parts only.
- Generator for answer quality. Ollama may not be allowed on the HPC. The options are vLLM or `llama.cpp` behind the same Ollama-style API, or `generator = "none"` for retrieval-only metrics.

**Scale**
- Target node counts for the live runs, e.g. 2, 4, 8, 16, 32, 64. The 1000-node point is covered by the offline simulation (16.3).

### 16.2 Preparation

1. **`scripts/rag_prepare_dataset.py`** converts the dataset into:
   - `datasets/<name>/docs/`
   - `datasets/<name>/questions.jsonl`: `{id, question, answers: [...], gold_doc_ids: [...], gold_passages?: [...]}`
   - `datasets/<name>/partitions/<scheme>-N<n>/node{i}/` for each partition scheme and node count. Use symlinks or copies, whichever the shared filesystem prefers.

   Partition schemes, with the seed recorded:
   - **`random`**: documents spread evenly at random (IID);
   - **`topic`**: k-means on document embeddings, so each node holds mostly one topic (non-IID, closest to real federations);
   - **`dirichlet`**: topic mixture per node drawn from Dirichlet(α), with α ∈ {0.1, 1.0}.
2. **Embedding cache.** Embed every chunk **once** (GPU if available) and save `embeddings.npy` plus `chunks.jsonl` per dataset. The simulation (16.3) uses this cache directly.
   - Live nodes ingest normally. Optionally, add a `rag_engine.py ingest --vectors` path that loads cached vectors to save time. That is a tool change, so log it as a decision if done.
3. **Single-index baseline:** one index over the whole corpus, giving the "ideal" top-k for every question.
4. **`scripts/rag_make_configs.py`** (planned in `p2p_rag_plan.md` Phase 5, not yet written). It writes N node configs with:
   - unique ports, `data_dir`, `sidecar_port` and `corpus_dir`;
   - the same `federation.id`;
   - `[discovery]` all off (no mDNS, DHT, DNS or relay);
   - `training.enabled = false`, `directory = true`;
   - `network.peers` listing **every other node** as `<EndpointId>@<host>:<p2p_port>`. This sidesteps gossip's small neighbour view (section 12), so `peer_targets` = N−1.

   Each EndpointId is only known after a node creates its identity. So add `iiitd --config X --print-endpoint-id`: create or load the identity in `data_dir`, print the EndpointId, and exit. This is a small `main.rs` change; log it as a decision. The config script runs it for every node first, then writes the `peers` lists.
5. **HPC job scripts** in `scripts/hpc/`:
   - `build.sh` (cargo build, Python venv);
   - `rag_cluster.sbatch` (place N RAG nodes on M compute nodes, several per compute node, wait until every node's `/rag/status` shows N−1 `peer_targets` with `status: "ok"`, run the question set, collect logs and results, stop everything);
   - one generator server per compute node, if answers are generated.
6. **`scripts/rag_eval.py`** turns the saved JSONL responses (`rag_run_questions.py` output) into per-question rows and aggregate tables (CSV) and plots.

### 16.3 Experiment A — offline routing simulation (1000 nodes, no networking)

`scripts/rag_sim_routing.py` is pure Python. It reuses `rag/clustering.py` so clusters are built exactly as in the store, and it reads the embedding cache. For each partition scheme and N ∈ {10, 100, 1000}, and each question:
- **global**: top `final_k` from the single index (the reference);
- **flat**: each node's top `top_k`, merged to `final_k`;
- **two_round**: each node offers its top `centroids_per_peer` centroids; global `select_clusters` (same rule as Rust); `chunks_per_cluster` from each winner; merged to `final_k`;
- **directory**: every centroid of every node is a candidate; same selection and round 2.

Metrics per question:
- **recall@final_k** against the global top `final_k`;
- **gold recall**: whether any gold passage or document is in the contexts, if gold exists;
- **peers contacted** (round 1 + round 2);
- **estimated bytes**, from the measured message sizes of M8 (bytes per centroid entry, per chunk);
- for directory mode, **directory size per node**.

Sweeps:
- `n_clusters` ∈ {auto, 4, 8, 16, 32, 64};
- `max_clusters` ∈ {1, 3, 5, 10};
- `min_centroid_score` ∈ {0, 0.1, 0.2, 0.3};
- `centroids_per_peer` ∈ {1, 3, 5} (two_round);
- `select_by` (two_round).

This is where "does centroid routing still find the right chunks at 1000 nodes" is answered.

Check: a small run (N = 2, the M6 corpus) reproduces the clusters selected in the live M8 run for the same questions. This shows the simulation matches the real code.

### 16.4 Experiment B — live federation on the HPC

For N in the agreed list, for each partition scheme (at least `random` and `topic`), run every question in `flat`, `two_round` and `directory`, plus `scope: "local"` on one node, and compare with the single-index baseline.

Record per query:
- contexts with origin and verification;
- answer, if a generator is used;
- `timings_ms` (select, fetch, rescore, generate);
- per peer: status, `selected_via`, `round1_ms`, `round2_ms`, bytes, flags.

Also record per run:
- directory memory and fetch bytes;
- `hello` and `directory` refresh traffic per minute;
- CPU time per node, if the scheduler reports it.

Run each configuration at least 3 times, to report the spread of latency.

### 16.5 Experiment C — adversaries at scale

On one N (e.g. 16), in all three modes, make a fraction f ∈ {10%, 30%} of the nodes adversarial: one mode per run, `tamper_text`, `inflate_score`, `fake_centroid`, `hide_cluster`, `equivocate`.

Measure:
- the share of adversarial peers flagged (expected 100% for every mode that is detectable by design);
- `FORGED` in any context or answer (expected 0);
- the loss of recall and answer accuracy compared with f = 0. Rejected honest-looking data is lost evidence; see M7's results.

### 16.6 Outputs

- `results/m9/<experiment>/…` raw JSONL and CSV. Runtime output: keep it out of git, or commit only the small aggregate CSVs. Decide when starting.
- Tables and plots, all against N:
  - recall@k;
  - answer accuracy;
  - median and p95 latency, with breakdown;
  - bytes per query;
  - peers contacted per query;
  - directory size;
  - detection rate.
- A "Phase M9" entry in section 14 with the headline numbers, and section 11's limits restated next to them.
- Tick Phases 5 and 6 in `p2p_rag_plan.md` with a pointer here.

### 16.7 Check

- Every table in 16.6 is filled for the agreed N values.
- The simulation reproduces the live clusters selected at N = 2 (16.3).
- Any claim about 1000 nodes is labelled as coming from the simulation, not from live runs.
