# Verifiable, Cluster-Routed Retrieval (Merkle Roots + K-Means) — Implementation Plan

Written 2026-10-18. Status: **Phases M1–M5 done (2026-10-18); M6–M8 not started.** Results per phase are in section 14. Builds on the working P2P RAG pipeline from `docs/p2p_rag_plan.md` (Phases 0–4 done). Read that plan first; this one changes its peer protocol.

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

## 1. Decisions (approved by the user, 2026-10-18)

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

### Phase M8 — Evaluation (feeds Phase 6 of `p2p_rag_plan.md`)
- Compare `flat` with `two_round` at 2, 4, 6 and 8 nodes:
  - retrieval recall@k against the single-index baseline;
  - answer accuracy;
  - peers contacted in round 2 per query;
  - bytes sent per query (round 1 + round 2);
  - latency split into select, fetch and rescore;
  - extra cost of verification and rescoring;
  - detection rate per adversary.
- Sweep `n_clusters`, `max_clusters`, `min_centroid_score` and `select_by`.

## 9. Simulated adversaries (`[rag] adversary`, experiments only)

These run in the **responder's** Rust code after its sidecar answers, so Python is unchanged. Never set them outside an experiment.

| Mode | What the peer does | Expected detection on the asker |
| :-- | :-- | :-- |
| `tamper_text` | changes one word in every returned chunk | `failed_proof`; chunk dropped |
| `inflate_score` | adds 0.3 to every chunk `score` and `best_chunk_score` | `score_mismatch`; recomputed scores used; `score_overclaim` |
| `fake_centroid` | replaces each centroid with the query embedding (via its own `/embed`) to always win selection | `bad_centroid_proof`; centroid dropped, so the peer is not selected |
| `equivocate` | signs a different root under the same version on every other `hello` | `equivocation`; both signed roots in `evidence`; peer's chunks refused |

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
- **Hiding chunks.** A Merkle proof shows that a chunk *is in* the corpus, not that it is the *best* chunk. A peer can leave out relevant chunks or hide a whole cluster in round 1. That can't be detected without revealing much more of the corpus.
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

## 13. Status checklist

- [x] Phase M1 — clustering and Merkle tree in the store (done 2026-10-18; see section 14)
- [x] Phase M2 — sidecar routes (done 2026-10-18; see section 14)
- [x] Phase M3 — Rust verification library (done 2026-10-18; see section 14)
- [x] Phase M4 — wire protocol v2 and own signed root (done 2026-10-18; see section 14)
- [x] Phase M5 — root registry and stale-peer skipping (done 2026-10-18; see section 14)
- [ ] Phase M6 — two-round query and flat mode
- [ ] Phase M7 — simulated adversaries
- [ ] Phase M8 — evaluation

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
