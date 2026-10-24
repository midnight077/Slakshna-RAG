# Decision log

Every design or working decision made on the peer-to-peer RAG work, with the reason for it. Newest decisions go at the end of the section they belong to. Each entry says who made it:

- **User**: the user chose it;
- **Claude**: Claude chose it while planning or implementing, and the user has not overruled it.

If a decision is later changed, keep the old entry, mark it **Replaced**, and point to the new entry, so the history stays readable.

The full design is in `p2p_rag_plan.md` and, for verifiable cluster-routed retrieval, `merkle_tree.md`; this file only records the choices and why they were made.

## A. Scope and approach

**A1. Build it simple first, then improve it (User, 2026-10-11).**
Version 1 sends the raw question to peers, gets raw text chunks back, and uses one embedding model on every node. No privacy, trust weighting or reranking.
*Why:* a working baseline is needed before refinements can be measured against it.

**A2. Work in phases, each ending with a runnable check (Claude, 2026-10-11).**
Phase 0 networking, 1 Python helper alone, 2 one node, 3 two nodes on one machine, 4 two machines, 5 more nodes, 6 evaluation.
*Why:* when something breaks, only the newest phase can be the cause. Networking was proved first (Phase 0) because it is the part most likely to fail for reasons outside the code.

**A3. Start with 2 nodes and design for 4, 6, 8 from the start (User, 2026-10-11).**
*Why:* the user's research plan. In practice this means no part of the design assumes exactly one peer: the asking node sends to a list of peers, never to "the" peer.

**A4. Accept that peers see raw questions and the asker sees raw chunks (Claude, 2026-10-11).**
*Why:* follows from A1. Privacy-preserving variants (sending embeddings or summaries) are listed as later extensions in the plan.

## B. Overall architecture

**B1. Peers talk over a new, dedicated Iroh protocol `/slakshna/rag/1`, not over gossip (Claude, 2026-10-11).**
*Why:* gossip copies every message to the whole group. For RAG that would send every question and every returned chunk to every node, and replies would have to be matched to questions by hand. A dedicated protocol gives a private question-and-answer channel between exactly two nodes. Iroh already supports several protocols on one connection point, so finding peers, identities and encryption are all reused, and no existing message format changes.

**B2. The search and answer-writing parts run in a separate, always-on Python program, the "sidecar" (Claude, 2026-10-11).**
*Why:* the AI tools (embedding models, FAISS, Ollama clients) are Python libraries. The existing pattern of starting Python once per training round is too slow for questions, because loading the embedding model alone takes several seconds. A long-running helper loads it once.

**B3. The sidecar listens only on the machine itself (`127.0.0.1`) and never talks to other nodes (Claude, 2026-10-11).**
*Why:* all traffic between nodes stays in the Rust program, where identities, encryption and the `allowed_peers` filter already live. The sidecar cannot be reached from outside, so it needs no security of its own.

**B4. Rust talks to the sidecar with local HTTP requests (Claude, 2026-10-11).**
*Why:* the simplest way for two programs in different languages to talk, easy to test with `curl`. Rust uses the `reqwest` library for it.

**B5. Only the node that received the question writes the answer (Claude, 2026-10-11).**
*Why:* peers only search. One answer-writing step per question keeps the cost the same however many nodes there are.

## C. Search and answer components

**C1. Embedding model `sentence-transformers/all-MiniLM-L6-v2` (Claude, 2026-10-11).**
*Why:* small (about 90 MB), fast on a CPU, and a widely used baseline. The machines have no GPUs.

**C2. Vector store: FAISS `IndexFlatIP` over normalised vectors (Claude, 2026-10-11).** **Replaced** by C10.
*Why:* exact search with nothing to tune, fast enough up to hundreds of thousands of chunks. Normalised vectors make the score a cosine similarity, which is comparable between nodes (see D1).

**C3. Chunks of about 200 words with 40 words of overlap (Claude, 2026-10-11).**
*Why:* the simplest reasonable default. The overlap stops a fact from being cut in half at a chunk boundary.

**C4. Read PDF (`pypdf`), `.txt` and `.md` files (Claude, 2026-10-11).**
*Why:* the test corpus is PDF; plain text covers everything else cheaply.

**C5. Store the index as `index.faiss` plus `chunks.jsonl` plus `meta.json` in `{data_dir}/rag/` (Claude, 2026-10-12).**
*Why:* the index survives restarts, and `chunks.jsonl` is readable in a text editor for debugging. `meta.json` records the embedding model, and the sidecar refuses to load an index built with a different one, because the stored vectors would be meaningless.

**C6. Adding the same file twice is skipped (Claude, 2026-10-12).**
*Why:* the node ingests its corpus folder on every start (see F3). Without this, every restart would duplicate every chunk.

**C7. Answer writer: Ollama with `qwen2.5:1.5b` (User accepted Ollama, 2026-10-11; model chosen by Claude).**
*Why:* runs locally on a CPU with no API key, and was already installed on machine 1. `llama3.2:3b` is a stronger option if memory and disk allow. A `none` mode returns the chunks without an answer, so search can be tested without waiting for the model.

**C8. Sidecar web framework: FastAPI with uvicorn (Claude, 2026-10-11).**
*Why:* small and standard for Python HTTP services.

**C9. Separate Python environment `.venv-rag`, made with `uv`, with the CPU-only build of PyTorch (Claude, 2026-10-12).**
*Why:* keeps the RAG libraries apart from the Bhaskera training environment, so neither breaks the other. The CPU build of PyTorch is several GB smaller than the GPU build, which matters on machine 1's nearly full disk.

**C10. Vector store: FAISS HNSW (`IndexHNSWFlat`, inner product) instead of exact search (User, 2026-10-21; settings by Claude).** Replaces C2.
The settings are `M = 32`, `efConstruction = 200` and `efSearch = 128`, as constants in `rag/store.py`. An index saved in the old exact format is rebuilt as HNSW from the same vectors when it is loaded, once, and saved.
*Why:* the user asked for HNSW, which searches a graph instead of comparing the query with every chunk, so it stays fast as corpora grow. The settings are generous for corpora of a few hundred chunks: on both real indexes, HNSW returned the same top 5 as exact search for all 365 test queries, and a two-node run of all 15 questions in both modes gave the same contexts, in the same order, as with exact search. Search time is about 0.1 ms per query either way at this size. The Merkle root does not cover the index file, and cluster scoring (`/centroids`, `/search_clusters`) works on the stored vectors, so roots, proofs and two-round selection are unaffected. The settings are not in the config, because nothing needs to tune them yet; that can change for large corpora in the evaluation.

## D. Combining results from several nodes

**D1. Every node uses the same embedding model, and results are merged by score (User for same model, 2026-10-11; merge rule by Claude).**
The asking node pools its own and its peers' chunks, removes exact duplicates (by a hash of the text), sorts by score and keeps the best `final_k` (default 5).
*Why:* with one model, scores from different nodes mean the same thing and can be sorted together. Each reply names its model, and a mismatch is logged as a warning.

**D2. Ask every known peer at once, each with a 5-second time limit (Claude, 2026-10-11).**
*Why:* asking in parallel means the wait is set by the slowest peer, not by the number of peers. The limit stops a dead peer from holding up the answer forever.

**D3. A peer that fails or times out is reported, not treated as an error (Claude, 2026-10-12).**
Each peer's outcome (`ok`, `timeout` or `error`) appears in the reply's `peers` list, and the answer is built from whatever arrived.
*Why:* in a peer-to-peer system some peers are always down. One missing peer should not cost the user their answer.

**D4. "Every known peer" means live neighbours plus every peer ever remembered (Claude, 2026-10-11).**
*Why:* the gossip system only keeps about 5 direct neighbours. From about 6 nodes up, a node would otherwise never ask some of the others.

**D5. A `scope` option on each question: `all` (default), `local` or `peers` (Claude, 2026-10-11).**
*Why:* gives the comparison baselines needed for evaluation (this node alone, others alone) without separate code.

**D6. Do not ask the model to answer when no chunks were found (Claude, 2026-10-12).**
*Why:* in testing, the model invented a confident answer from memory when given no context. An empty answer is more honest.

**D7. Keep the simple global top-k for now even though questions needing both nodes fail (Claude, 2026-10-12).**
*Why:* the failure was measured (0 of 3 cross-node questions) and is a known limit of A1's design, not a bug. Fixes (a share of `final_k` per node, larger `final_k`, reranking, splitting the question) are to be compared in a later phase, against this baseline.

## E. Peer message format

**E1. JSON, one question and one reply per stream, with a version number `v` (Claude, 2026-10-11).**
*Why:* JSON is readable when debugging. One stream per question needs no request matching. The version number allows the format to change later without confusing old nodes.

**E2. Messages are capped at 1 MiB and a peer may ask for at most 50 chunks (Claude, 2026-10-12).**
*Why:* stops a faulty or hostile peer from making a node use unlimited memory.

**E3. The RAG protocol applies the same `allowed_peers` filter as the rest of the node (Claude, 2026-10-12).**
*Why:* if an experiment restricts who may join, the restriction should cover questions too.

**E4. Nodes with and without RAG can share one group (Claude, 2026-10-12).**
*Why:* nothing in the existing gossip messages changed. A node without RAG answers a question with "does not support this protocol", which is reported as that peer's `error` and skipped. Confirmed in Phase 3.

## F. Node program changes

**F1. A switch `[training] enabled`, on by default; the RAG configs turn it off (User decided RAG nodes are training-off, 2026-10-11; switch design by Claude).**
*Why:* training needs GPUs and Bhaskera, which the CPU machines lack. Defaulting to on keeps every existing config behaving exactly as before. It stays a plain switch so training and RAG can run together later if the user wants.

**F2. All new settings have defaults, and RAG is off unless `[rag] enabled = true` (Claude, 2026-10-11).**
*Why:* existing config files keep loading unchanged.

**F3. The node starts and stops the sidecar itself, and indexes its `corpus_dir` at startup (Claude, 2026-10-12).**
*Why:* one command per node instead of two. The sidecar is started with `--exit-with-parent` and is killed when the node exits, so no stray Python process is left running, even if the node is force-killed. The node waits up to 180 s for the sidecar, because the first start downloads the embedding model.

**F4. The sidecar starts before the networking part (Claude, 2026-10-12).**
*Why:* the network part has to know about the RAG service when it starts, to register the RAG protocol.

**F5. Install the `ring` crypto provider for `rustls` at the very start of `main` (Claude, 2026-10-12).**
*Why:* fixes a crash ("No rustls crypto provider is configured"). Iroh switches on TLS in the shared `reqwest` library but leaves the crypto provider choice to the program, so the program must pick one.

**F6. `reqwest` with default features off and only `json` on (Claude, 2026-10-12).**
*Why:* the sidecar is plain HTTP on the same machine, so no extra TLS stack is needed.

## G. Test data

**G1. Claude builds the test corpus from English Wikipedia, as PDFs (User, 2026-10-11).**
*Why:* the user asked for it. Wikipedia text is free to reuse (CC BY-SA 4.0), and every PDF cites its source and revision. `scripts/rag_build_corpus.py` rebuilds it.

**G2. The two nodes get unrelated topics: Indian space missions for node 1; scientists, a national park and an ancient civilisation for node 2 (Claude, 2026-10-11).**
*Why:* if a question about node 2's topic is answered correctly by node 1, the answer must have come through the peer request (or the model's memory, see G4). Overlapping topics would hide whether the peer request worked.

**G3. 15 test questions: 6 for node 1 only, 6 for node 2 only, 3 needing both (Claude, 2026-10-11).**
*Why:* covers each case the design must handle. It is a smoke test, too small for reported results.

**G4. Judge results by the chunks used, not only by the answer (Claude, 2026-10-12).**
*Why:* in Phase 4 the model answered two node-2 questions correctly without node 2, from its own memory. A right answer alone does not prove the search worked.

**G5. The corpus downloader identifies itself with a generic name, not the user's email (Claude, 2026-10-11).**
*Why:* an early version sent the user's email to Wikipedia in a request header by mistake. Personal details should not go to outside services.

## H. Network setup

**H1. Node 1 lists node 2's id and address in its config (`peers = ["<EndpointId>@192.168.3.143:9001"]`) (Claude, 2026-10-12, after the user explained the network).**
*Why:* the two machines are connected only through a FortiClient VPN, not one local network, so automatic local discovery (mDNS) cannot find node 2. Connections from machine 1 to machine 2 work; the reverse direction has not been tested, so node 1 starts the connection.

**H2. The example configs keep internet-wide discovery and relays off (Claude, 2026-10-11).**
*Why:* not needed while the VPN works, and keeping them off removes outside services from the tests. Pinned addresses and `relay = true` must keep working, because the user said the network may change.

**H3. The two example nodes use different ports (9000/8545/8546/8600 and 9001/8555/8547/8601) (Claude, 2026-10-11).**
*Why:* lets both nodes run on one machine (Phase 3) with the same configs that are used across two machines.

## I. Working practices

**I1. Work on branch `feat/p2p-rag`, with Conventional Commit messages; commit and push only when the user asks (User for when to commit; branch and style by Claude following `docs/AGENTS.md`).**
*Why:* keeps `main` untouched until the work is reviewed.

**I2. Keep Windows-style (CRLF) line endings in source files that already use them (Claude, 2026-10-12).**
*Why:* changing them makes the whole file show as changed in git, hiding the real edits.

**I3. Explain everything in plain language (User, 2026-10-11).**
*Why:* the user is new to Rust, Python and networking. `explanation.md` is written at that level.

**I4. Record every decision in this file (User, 2026-10-18).**
*Why:* so the reasons behind the design can be looked up later.

**I5. Leave the existing Rust code unformatted; run `rustfmt` only on new files (User, 2026-10-18).**
The committed Rust files use a `{ A, B }` brace style that `cargo fmt` would rewrite. They stay as they are. New files (from `src/rag/merkle.rs` on) are checked with `rustfmt --check <file>`, and edits to old files match the code around them. `cargo fmt --check` over the whole tree is no longer a required check.
*Why:* the user chose not to reformat. Reformatting would touch every Rust file and bury the real changes in the diff.

## J. Verifiable, cluster-routed retrieval (Merkle tree and k-means)

**J1. Follow the plan in `merkle_tree.md`, including its decisions D1–D10 and its byte format (User, 2026-10-18).**
*Why:* the user approved that plan. Its decision table is not repeated here. The entries below are choices made while implementing it.

**J2. The store keeps its lock for a whole ingest: all the adds, then one commit (Claude, 2026-10-18).**
`add_documents` (used by `add_path` and by the sidecar's `/ingest`) holds a re-entrant lock while it adds every document and commits. If a document fails partway, whatever was already added is still committed.
*Why:* a query arriving mid-ingest would otherwise see new chunks that have no cluster and no proof yet. Committing on failure keeps memory and disk the same.

**J3. Changing `n_clusters` re-clusters the index on the next start (Claude, 2026-10-18).**
`meta.json` stores the `n_clusters` setting used. A different value at load time triggers a new commit (new root, higher version).
*Why:* the evaluation sweeps `n_clusters`. Without this, a new value would only take effect after the next document was added.

**J4. Switch off FAISS's "too few training points" warning (Claude, 2026-10-18).**
`min_points_per_centroid=1` in `faiss.Kmeans`.
*Why:* with about 160 chunks and 13 clusters the warning would be printed on every ingest, and it is expected here. The setting only controls the warning; the clustering is unchanged.

**J5. On load, a Merkle root that does not match `meta.json` stops the sidecar; files written before clustering are migrated once (Claude, 2026-10-18).**
`meta.json` is written last when saving. An index with no `commitment` entry is clustered and committed on load.
*Why:* a mismatch means the files were edited or a save was cut short. Serving them would sign a root over content nobody committed to. Old indexes have nothing to check against, so they are upgraded instead of rejected.

**J6. The Merkle fixture stores centroids as hex bytes and includes non-ASCII and empty texts (Claude, 2026-10-18).**
The float values are in the file only for reading; the hex is what is hashed. A test checks that the generator script reproduces the committed fixture exactly.
*Why:* a value like 0.6 can round differently when parsed from decimal in another language, and multi-byte UTF-8 is where byte-length bugs show up. The generator check means the fixture can't drift from the code without a test failing.

**J7. The `/retrieve` output stays exactly as before (Claude, 2026-10-18).**
The stored `cluster` field is removed from plain search results. Cluster ids and proofs only appear when they are asked for (`with_proofs`, `search_clusters`).
*Why:* the Rust node and the v1 peer protocol read these results, and nothing in M1 should change what they get.

**J8. Asking for proofs of chunks that are not yet committed is an error (Claude, 2026-10-18).**
*Why:* such chunks are not in the root, so no valid proof exists. Failing loudly is better than returning proofs that would fail on the peer.

**J9. Sidecar routes read the root version and the results under one lock (Claude, 2026-10-18).**
The store's lock is public. `/centroids`, `/search_clusters`, `/retrieve` with proofs and `/ingest` hold it while they read both. The query is embedded before the lock is taken.
*Why:* an ingest landing between the two reads would make a reply claim one root version while carrying proofs for another, and the asker would reject good data.

**J10. A stale `root_version` on `/search_clusters` gets HTTP 409 with the current commitment (Claude, 2026-10-18).**
The body is `{"detail": {"error": "stale_root", "root": {root, version, num_chunks, num_clusters, dim, embedding_model}}}`.
*Why:* cluster numbers change whenever the index is re-clustered, so serving an old cluster id would return the wrong chunks. Including the current commitment lets the Rust responder answer `stale_root` with the new root in one step, as section 5 of the plan requires.

**J11. Route tests run in-process with FastAPI's test client (Claude, 2026-10-18).**
`tests/test_rag_server.py` uses the fake embedder from the store tests.
*Why:* it checks every route, including the proofs, in about a second, with no model download, port or running server. `httpx`, which the test client needs, is already in `requirements-rag.txt`.

**J12. In Rust, roots and hashes stay hex strings in the wire types and are parsed when checked (Claude, 2026-10-18).**
`SignedRoot`, `ClusterEntry` and `ProofStep` hold hex `String`s, exactly like the JSON. Anything that needs bytes parses them and returns an error (or `false` from `verify`) if they are malformed. So `SignedRoot::message_bytes` returns a `Result`, and the byte layout lives in a separate `root_message(...)` function. `SignedRoot::sign` takes a `Commitment` struct, the unsigned reply of the sidecar's `GET /commitment`.
*Why:* data from peers is untrusted, so bad hex must become a rejection, not a crash. Keeping the structs identical to the JSON means serde needs no custom code.

**J13. `src/rag/merkle.rs` allows unused code until the peer protocol uses it (Claude, 2026-10-18).**
*Why:* in M3 only the tests call it. Without the allowance, the build would gain about a dozen "never used" warnings for one or two phases. Remove it once M4/M6 use the module.

**J14. Until Phase M6, `/rag/query` uses the new `search` request without checking proofs (Claude, 2026-10-18).**
*Why:* the protocol changed in M4, but the verifying query pipelines only come in M6. Sending the flat `search` and taking its chunks as v1 did keeps `/rag/query` working in between. B1's design (a dedicated Iroh protocol) is unchanged; only its version is now 2.

**J15. Failed requests are answered with `kind: "error"`; `stale_root` comes back as a `chunks` answer carrying the current root (Claude, 2026-10-18).**
`request_peer` turns a `stale_root` into a typed `StaleRoot` error that holds the peer's signed root.
*Why:* a request that can't be read has no kind to echo, and an empty answer of the right kind would look like "no results". A stale root is different: the asker needs the new root to update its registry, so it gets its own type instead of an error string to parse.

**J16. The responder re-signs whenever its sidecar answers under a version it has not signed (Claude, 2026-10-18).**
On a stale-root refusal it signs the commitment the sidecar returned with its 409, instead of asking again.
*Why:* an ingest can reach the sidecar without going through the node (for example `rag_engine.py ingest` run by hand). `hello` should never hand out a root older than the proofs the node is serving.

**J17. The shared Iroh-key helper is `Keypair::transport_secret_key()` in `src/identity.rs` (Claude, 2026-10-18).**
*Why:* the plan asked for one helper used by both the mesh and the RAG service. Putting it next to `transport_seed()`, on the keypair itself, means neither side can derive the key differently.

**J18. Keep `GET /rag/debug/hello` after the M4 check (Claude, 2026-10-18).**
It fetches every peer target's signed root and reports whether each signature checks out against that peer's EndpointId.
*Why:* the plan called it a temporary debug call, but it is the quickest way to confirm that machine 2 runs the new build and signs correctly before the two-machine run in M6. It can go once the registry in `/rag/status` (M5) shows the same information.

**J19. Add `select_timeout_ms`, `root_refresh_secs` and `peer_stale_secs` in Phase M5 (Claude, 2026-10-18).**
They are from the plan's section 7, which is otherwise done in M6, with the plan's defaults (2000 ms, 60 s, 180 s), serde defaults, and a check that each is positive.
*Why:* the registry and its background refresh, built in M5, need them. Existing configs still load unchanged.

**J20. Any answer from a peer counts as contact for stale-peer skipping, even one with a bad root (Claude, 2026-10-18).**
*Why:* skipping is about not waiting on peers that are down. A peer that answers with a bad signature, a rollback or a conflicting root is clearly up; it is handled by its flags (and from M6 on, its chunks are refused), not by being treated as offline.

**J21. Queries update the registry, and skipped peers are reported as `skipped_stale` from M5 on (Claude, 2026-10-18).**
*Why:* the plan defines `last_ok` as the last successful hello *or query*, so query results must be recorded too. Reporting skipped peers in the `peers` list (a status value from section 6.4) shows why a peer gave no chunks.

**J22. Registry details: rollbacks are flagged without evidence; equivocation compares signed fields; flags and evidence are not duplicated (Claude, 2026-10-18).**
*Why:* the plan asks for evidence only for equivocation, where two signed roots with one version prove the peer lied. Ed25519 signatures are deterministic, so comparing the signed fields is the same as comparing roots, and is robust to how they are encoded. Without the de-duplication, a peer that keeps equivocating would grow the evidence list on every refresh.

**J23. Skip a peer as soon as its latest contact failed (User, 2026-10-19).**
`should_ask` asks a peer only if it has never been tried, or if it answered within `peer_stale_secs` **and** its latest contact (`hello` or query) did not fail. The background refresh keeps asking every peer, and its next answer brings the peer back. This changes the rule in section 6.1 of `merkle_tree.md`, which had only the `peer_stale_secs` condition.
*Why:* in the M5 results, a peer that had just died still cost the full timeout on every query until `peer_stale_secs` (3 minutes by default) passed. Now it costs one. The evaluation (M8) starts and stops nodes often, and those delays would distort its latency numbers. The trade-off: a healthy peer that misses one request is skipped for at most `root_refresh_secs` (60 s).

**J24. The query pipelines go in a new file, `src/rag/retrieval.rs` (Claude, 2026-10-19).**
*Why:* the plan put them in `src/rag/mod.rs`, which already holds the service and its types. A separate file keeps each part readable, and keeps the checking rules (cluster selection, chunk proofs, rescoring, overclaims) as plain functions that are unit-tested without a network.

**J25. A query can choose its retrieval mode (Claude, 2026-10-19).**
`/rag/query` accepts `"retrieval": "two_round"` or `"flat"`, overriding `[rag] retrieval` for that query.
*Why:* the evaluation compares the two modes on the same running nodes. Without the override, every comparison would need a config change and a restart.

**J26. The asker never accepts more than it asked for (Claude, 2026-10-19).**
It takes at most `centroids_per_peer` centroids per peer (a cluster offered twice counts once), at most `chunks_per_cluster` chunks per cluster, and at most `top_k` chunks per peer in flat mode. Extra chunks are counted in `rejected_chunks`.
*Why:* otherwise a peer could win extra places in round 1, or flood the merge with more chunks than any honest peer sends. The plan capped only the responder's side.

**J27. Round-2 chunks are checked against the subtree roots verified in round 1 (Claude, 2026-10-19).**
*Why:* those roots are already proven to belong to the peer's signed root, so checking a chunk against them proves it is in the corpus. The round-2 cluster entries are redundant: if they differed from round 1, the chunk proofs would fail anyway.

**J28. A peer caught equivocating is not asked at all; it is reported as `refused` (Claude, 2026-10-19).**
*Why:* the plan says such a peer's chunks are refused from then on. Not asking at all also saves the round trip, and the new status makes the reason visible. The flag lives in memory, so a restart forgives the peer (plan, section 11).

**J29. Output details: `Chunk.flags`, `verification: "local"`, and `retrieval` in the response (Claude, 2026-10-19).**
*Why:* the plan says a `score_mismatch` is flagged on the chunk as well as on the peer, so a chunk needs a place for flags. Labelling local chunks makes every context's origin explicit, and naming the mode in each response makes saved results self-describing.

**J30. `min_centroid_score` applies to whatever round 1 ranks by (Claude, 2026-10-19).**
It is the asker's similarity by default, or the claimed best chunk score with `select_by = "best_chunk"`.
*Why:* the threshold and the ranking should use the same number. Otherwise a cluster could rank first and still be cut, or the other way round.

**J31. A selected cluster that delivers no verified chunk counts as an overclaim (Claude, 2026-10-19).**
This is only checked if round 2 answered at all; a timeout is not an overclaim.
*Why:* the peer claimed a chunk with that score exists and then did not send one. That is the same promise broken as sending a worse chunk.

**J32. Simulated attacks run on the answering side, after the honest answer (Claude, 2026-10-22).**
They live in `src/rag/adversary.rs`, are switched by `[rag] adversary` (`"none"` by default), and the node logs a warning at startup whenever one is on.
*Why:* this follows the plan (section 9). The node corrupts exactly what a dishonest peer would control, its own reply, while the sidecar, the proofs it computes and the asking side stay real. A detection is then a detection by the production checks, not by test code.

**J33. `tamper_text` writes the marker word `FORGED` into each chunk (Claude, 2026-10-22).**
It replaces the middle word.
*Why:* any one-word change breaks the proof equally. A distinctive marker makes "no tampered text reached the answer" a simple search over every context and answer.

**J34. `fake_centroid` corrupts the cluster entries of every answer kind, not only round 1 (Claude, 2026-10-22).**
*Why:* a real attacker would lie consistently. It also tests flat mode, where corrupted entries make the chunks under them fail as well.

**J35. `equivocate` flips the first byte of the real root and signs it under the same version; "every other hello" counts all peers' hellos together (Claude, 2026-10-22).**
*Why:* it is the simplest second root that is properly signed and clearly different. A single counter needs no per-peer state; with one asking peer in the test, it alternates exactly.

**J36. Live attack tests use an isolated test federation with fresh data (Claude, 2026-10-22).**
The nodes get their own federation id, ports and scratch data folders, and both are restarted for each mode.
*Why:* a misbehaving node must not join, or be remembered by, the real nodes and their RocksDB peer lists. Restarting the honest node between modes clears its in-memory flags, so each mode is judged on its own. The same set-up was used to check the HNSW switch end to end (C10), because a real node was running at the time.

## K. Centroid directory (third retrieval mode) and the HPC evaluation

**K1. The centroid directory is a third retrieval mode in the same code, developed on branch `centroid-directory` (User, 2026-10-22).**
`retrieval = "directory"` sits next to `flat` and `two_round`, rather than living in a fork of the repository.
*Why:* the new mode reuses almost everything: signed roots, proofs, the registry, round 2, checks, rescoring and merging. A fork would need every fix made twice. With one build, a query can pick any mode, so all three are compared on exactly the same nodes and data.

**K2. Phase M8 is the directory mode; evaluation of all three modes moves to M9, run on the user's HPC with the user's dataset (User, 2026-10-22).**
*Why:* the evaluation should cover the new mode too, and the 2-node test corpus is too small for results. The HPC provides scale; the dataset provides questions worth reporting.

**K3. The directory is fetched with a new request kind, `directory`, without changing the ALPN (Claude, 2026-10-22).**
*Why:* adding a kind doesn't change any existing message. A node on an older v2 build answers it with an error. The asker then treats that peer as having no directory and falls back to round 1 for it (K7), so mixed builds still work.

**K4. Directory mode reveals a node's full set of centroids to any allowed peer, and is off unless `[rag] directory = true` (Claude, 2026-10-22).** Reverses D1 for nodes that turn it on.
*Why:* at large scale, `two_round` sends every question to every peer. The directory trades that for revealing topic summaries, and then only the round-2 peers see a question. The privacy cost differs, so each deployment chooses.

**K5. Directories are pulled when a peer's root version changes, as part of the background root refresh (Claude, 2026-10-22).**
*Why:* centroids only change on ingest, so fetching per root version is enough. The registry already tracks root versions, which keeps the change small. Gossip announcements are left for later, if refresh traffic matters in M9.

**K6. The directory is scored by brute force, not an ANN index (Claude, 2026-10-22).**
*Why:* 1000 peers × 13 clusters is about 13,000 vectors, a few milliseconds per query. M8 measures this with a scale test; an index is only worth adding if that number is too high. Measured in M8: median 4.65–7.78 ms per query over two runs, under the 10 ms target.

**K7. Peers without a fresh directory entry fall back to round 1 for that query (`directory_fallback = true`) (Claude, 2026-10-22).**
*Why:* new peers, peers that just re-ingested and peers on older builds still contribute. The response says which peers came from the directory and which from round 1 (`selected_via`).

**K8. Cluster entries are checked position-bound in every mode, and a directory must hold exactly the signed number of clusters (Claude, 2026-10-22).**
The proof's left/right pattern must match `(cluster_id, num_clusters)`.
*Why:* this makes hiding a whole cluster from the directory detectable (`incomplete_directory`). An honest proof always has this shape, so applying the check in all modes costs nothing and keeps one verification path. M8 confirms by test that a tree with more leaves than signed can't pass.

**K9. Each peer's request and response bytes are counted in every mode (Claude, 2026-10-22).**
*Why:* bytes per query is a main metric in M9, and it should be measured, not estimated.

**K10. New simulated attack `hide_cluster`; `fake_centroid` also covers `directory` answers (Claude, 2026-10-22).**
*Why:* `hide_cluster` is the attack K8 is meant to catch. Extending `fake_centroid` follows J34, since attackers lie consistently.

**K11. M9 includes an offline routing simulation for 1000 nodes; live runs stop at the node count the HPC allows (Claude, 2026-10-22).**
*Why:* starting 1000 real nodes is impractical. Whether routing by centroids still finds the right chunks depends only on the vectors, so it can be measured without networking. Claims about 1000 nodes are labelled as simulated.

**K12. Bytes are counted into a counter the caller owns (Claude, 2026-10-22).**
`request_peer` adds the JSON request and response sizes to a `ByteCount` passed in by the caller, as they are written and read. The counts are JSON payload only, not QUIC or TLS overhead.
*Why:* a returned count is lost when a request fails or a timeout cancels it halfway, and those requests also cost bytes. Payload bytes are what the modes differ in; transport overhead is about the same for all of them.

**K13. `fake_centroid` sends the mean direction of the node's own centroids in `directory` answers (Claude, 2026-10-22).**
*Why:* a `directory` request carries no query, so there is nothing to copy as in round 1. The normalised sum of all its centroids is the closest equivalent of "always win": a vector near every topic the node holds. Like any changed centroid, it fails the proof.

**K14. A failed directory fetch doesn't mark the peer as failing (Claude, 2026-10-22).**
*Why:* a peer with `directory = false`, or on an older build, answers `directory` with an error but is up. Under J23 a recorded failure would make queries skip it entirely, instead of asking it through round 1 (K7).

**K15. A stored directory with no verified entries counts as "no directory" (Claude, 2026-10-22).**
Such a peer falls back to round 1, and a directory fetch is spawned on each such query.
*Why:* plan 15.4 expects a `fake_centroid` peer to fall back to round 1 and fail there too. An empty directory can't offer any cluster, so treating it as present would silently drop the peer.

**K16. In directory mode, a peer's directory flags appear in its `PeerResult.flags` (Claude, 2026-10-22).**
*Why:* the flags describe the data this query used (e.g. `incomplete_directory`), so they should be visible in the query's output, not only in `/rag/status`.

**K17. `QueryResponse.flags` holds notes about the whole query (Claude, 2026-10-22).**
The first is `select_by_centroid_for_directory`, set when `select_by = "best_chunk"` met directory entries, which have no claim to rank by. The field is omitted when empty.
*Why:* plan 15.3 asks for this note "in the response's flags", and it belongs to no single peer.

**K18. `RagService::query` takes `self: &Arc<Self>` (Claude, 2026-10-22).**
*Why:* background directory fetches (fallback peers, a stale root in round 2) outlive the query, so they need their own handle on the service. The API already holds the service in an `Arc`, so callers are unchanged.

**K19. The M8 live check used an isolated test federation with fresh data and no pinned peer (Claude, 2026-10-22).**
The three nodes kept the `examples/rag-2node` settings and corpora, but got their own federation id, ports and scratch data folders, and `peers = []`.
*Why:* the re-ingest step (f) adds a document to node 2's index, which would have changed the real `data-rag-node2` permanently. The machine-2 peer pinned in `node1.toml` runs an older build that answers `directory` with an error, which would have muddied "every peer via directory". Same practice as J36.

**K20. A test loads every config under `examples/` (Claude, 2026-10-22).**
*Why:* "existing configs still load" should be checked by a test that keeps running, not by hand once. It covers the 14 example configs, including the training-cluster ones.

## L. M9: comparison with Flower FedRAG on the HPC

**L0. M9 compares Slakshna-RAG with Flower's FedRAG example on MedRAG Textbooks split into 5 shards; questions are MIRAGE MedQA-US + MMLU-Med; everything runs on one CPU server (User, 2026-10-23).**
The user chose the dataset, the 5-node target, the question sets (out of MedQA + MMLU-Med, all four sets, Flower's PubMedQA + BioASQ, or a small subset first) and confirmed the HPC is one 256-thread machine with no GPU and no scheduler. This replaces the open questions of D16 / plan 16.1.
*Why:* Flower's FedRAG is the best-known federated RAG baseline and uses the same MedRAG corpora. The Textbooks corpus comes from the MedQA project, so MedQA and MMLU-Med are the questions it can actually help answer; PubMedQA and BioASQ are about research abstracts.

**L1. Embedder `BAAI/bge-base-en-v1.5` for both systems; no MiniLM rerun (Claude recommended, User chose, 2026-10-23).**
*Why:* clearly better retrieval than MiniLM at a cost CPU can still carry (768 numbers per vector, ~5× slower to embed). One embedder halves indexing and run time. It is used without a query instruction in both systems, so the comparison stays symmetric.

**L2. One shared answering script for every system, using Flower's exact prompt and answer-extraction rule; both systems return contexts only (Claude, 2026-10-23).**
Slakshna nodes run with `generator = "none"`; Flower's server is patched to save its merged documents instead of calling its LLM. `scripts/m9/answer.py` then answers every system's contexts the same way.
*Why:* only retrieval should differ. Separating retrieval from answering also lets identical contexts be answered once (a cache), adds closed-book and global-index references for free, and keeps slow CPU answering out of the latency measurements. Check E0(a) proves the split doesn't change Flower's answers.

**L3. Answering LLM: Ollama `qwen2.5:7b-instruct` on CPU, falling back to `qwen2.5:3b-instruct` if calibration projects more than about a day (Claude, 2026-10-23).**
Temperature 0, seed 0, `num_predict 10`, `num_ctx 8192`.
*Why:* a strong instruction model at multiple-choice medical questions, with no "thinking" mode that would break a 10-token answer limit, and it runs on any Ollama version. On CPU, reading a ~2.5k-token prompt dominates the cost, so the model size is set by measured speed. `num_ctx` must be raised because Ollama's default window silently cuts an 8-snippet prompt.

**L4. Slakshna ingests the MedRAG snippets as given (one snippet = one chunk, id = MedRAG id, text = `content`) instead of re-chunking (Claude, 2026-10-23).**
*Why:* both systems must search exactly the same pieces of text, and Flower uses MedRAG's snippets and their `content` field.

**L5. Candidate budget matched at 40 for every system: `top_k = final_k = 8`, `max_clusters = 4`, `chunks_per_cluster = 8` (Claude, 2026-10-23).**
Flower: 5 × 8. Slakshna flat: 8 local + 4 × 8. Two-round and directory: 8 local + 4 clusters × 8.
*Why:* recall depends on how many candidates are gathered before cutting to 8, so the budget must match for the comparison to be about the method. Other budgets are explored in the optional simulation (E5).

**L6. Flower is run as shipped, except for minimal patches that touch no retrieval logic (Claude, 2026-10-23).**
Patches: the corpus folder from an environment variable; `retriever.yaml` set to bge-base; a copy of the server loop that saves contexts instead of calling the LLM; the run config. Its `IndexIVFFlat` with `nprobe = 1` and its per-query reload of the model and index stay as they are; one optional run (E2b) searches all buckets.
*Why:* a baseline tuned by us would be open to the charge that we weakened or strengthened it. Keeping its real behaviour and showing the index effect separately is the honest option.

**L7. `rag_engine.py ingest --vectors` loads pre-computed embeddings from the M9 cache (Claude, 2026-10-23).**
Clustering, the Merkle tree and the HNSW index are still built by the normal store code; a spot check re-embeds 50 random snippets of every shard file (so 50–900 per node) and requires a difference under 1e-4. Measured on the laptop: ~1e-7.
*Why:* embedding 125.8k snippets with bge-base on CPU takes about an hour each time, and the vectors would be identical. It is a tool change, so it is logged (plan 16.2 of D16 required that).

**L8. New flag `iiitd --print-endpoint-id` (Claude, 2026-10-23).**
*Why:* the config generator must list every other node's EndpointId in `peers` before any node starts, and that id only exists once the node's identity is created. Already planned in D16.

**L9. M9 configs raise `select_timeout_ms` to 5000, `peer_timeout_ms` to 15000 and `max_message_bytes` to 4 MiB (Claude, 2026-10-23).**
*Why:* with 25k chunks per node and 768-number vectors, a 64-cluster directory is ~450 KB of JSON and re-embedding up to 32 peer chunks on 16 CPU threads can take a second or two. The defaults were sized for the 2-node test corpus; spurious timeouts would make results depend on load rather than on the method.

**L10. Flower runs on its Deployment Runtime (a SuperLink plus 5 SuperNode processes), not its simulation engine (Claude, 2026-10-23).**
*Why:* Slakshna's 5 nodes are real processes talking over the network stack; Flower's simulation runs clients inside one Ray process, which would make latency incomparable.

**L11. M9 raw outputs stay out of git; the aggregate CSVs, plots and an environment record are committed under `results/m9/summary/` (Claude, 2026-10-23).**
*Why:* raw files (contexts for 2362 questions × many runs) are large runtime output (CLAUDE.md conventions), while the summary is what the write-up cites and must be reproducible from a known commit.

**L12. Two shard schemes: `book` (whole books, greedy by size) and `random` (seed 1234) (Claude, 2026-10-23).**
*Why:* `book` is how real owners would hold data (each holds whole subjects, non-IID) and is where cluster routing should help; `random` is the IID control. The Dirichlet scheme of D16 is dropped: with 18 books and 5 nodes, `book` already gives strong skew.

**L13. Retrieval quality is recall@8 against an exact search over the whole corpus, matched by text hash (Claude, 2026-10-23).**
*Why:* MIRAGE has gold answers but no gold passages. Flower returns text without ids, so snippets are matched by a hash of their text.

**L14. Accuracy comparisons are paired: McNemar's exact test and a bootstrap 95% interval (Claude, 2026-10-23).**
*Why:* every system answers the same questions, so a paired test is far more sensitive than comparing two percentages, and the interval says which differences are too small to claim.

**L15. Answer accuracy runs on the `book` scheme first; `random` only if the calibration leaves time (Claude, 2026-10-23).**
*Why:* CPU answering is the bottleneck (thousands of questions per context set), and recall@8 from E1 already covers both schemes.

**L16. The untracked `scripts/run_mirage_slakshna.py` is superseded by `scripts/m9/run_slakshna.py` + `scripts/m9/answer.py` (Claude, 2026-10-23).**
*Why:* it answered inline with Flower's LLM, saved no context text (so recall and caching were impossible), and targeted PubMedQA + BioASQ. Its core idea, Flower's own prompt applied to Slakshna's contexts, is kept in L2.

**L17. Laptop tests use `BAAI/bge-small-en-v1.5` as a stand-in for bge-base, on a 2,000-snippet subset over 3 nodes (Claude, 2026-10-23).**
*Why:* the laptop's disk is 98% full (about 2 GB free) and bge-small was already downloaded. It is the same model family (also normalised, 384 numbers), and every M9 script takes the model as an option, so the HPC runs use bge-base unchanged.

**L18. Flower's processes keep all their state in `m9-flwr/`, each with its own `FLWR_HOME`; the SuperLink also gets `FEDRAG_CORPUS_DIR`; runtime dependency installation is off (Claude, 2026-10-23).**
*Why:* a separate home leaves the user's `~/.flwr` alone and stops 5 SuperNodes on one machine from sharing one app folder. The ServerApp checks that the indexes exist before it starts, so it needs the corpus folder too (the first test run failed without it). Dependencies are already in `.venv-flower`; installing at run time would need network access and could change versions mid-experiment.

**L19. The M9 Flower server waits until all N SuperNodes are online (Claude, 2026-10-23).**
Flower's own `node_online_loop` returns as soon as one SuperNode is online.
*Why:* a run started before all clients connect would silently search fewer shards, which would look like poor recall. Waiting changes nothing once all clients are there.

**L20. A Flower run is judged by the lines it wrote, not by `flwr run`'s exit code (Claude, 2026-10-23).**
*Why:* `flwr run --stream` exited 0 although the ServerApp had crashed (seen in testing). `flower_cluster.py run` fails when the run added no output.

**L21. M9 node APIs listen on 127.0.0.1, and `allowed_peers` lists exactly the federation's nodes (Claude, 2026-10-23).**
*Why:* the HPC is a shared machine on a LAN. The peer port always listens on every interface, so `allowed_peers` keeps other machines' nodes out, and the HTTP API, which has no authentication, stays local.

**L22. The LLM calibration loads the model with one untimed request and gives each worker count different questions (Claude, 2026-10-23).**
*Why:* the first test re-sent the same 6 prompts at each worker count, and Ollama answered the repeats from its prompt cache: 15.9 s per question with 1 worker, 0.37 s with 2. That would have badly under-estimated the run time.

**L23. The cluster start/stop helpers are Python scripts (`scripts/m9/slakshna_cluster.py`, `scripts/m9/flower/flower_cluster.py`), not the shell scripts named in the plan (Claude, 2026-10-23).**
*Why:* waiting for readiness means polling JSON (`/rag/status`, `flwr supernode list --format json`) and keeping process ids; that is simpler and more robust in Python, still standard library only.

**L24. The M9 Flower server imports `merge_documents` and `node_online_loop` from Flower's `server_app.py` instead of copying them (Claude, 2026-10-23).**
*Why:* importing guarantees the merge is Flower's code exactly. Only `submit_question` is re-written, to also record which shard each reply came from; it builds the same messages.

**L25. Bytes per query are reported for both systems, with different scopes stated (Claude, 2026-10-23).**
Slakshna: JSON payload of every peer request and response, proofs included (K12). Flower: the text of the returned documents plus 8 bytes per score, as recorded by the M9 server; Flower's message framing is not counted.
*Why:* Flower doesn't expose its wire sizes, and its payload carries no proofs, so a single number would mislead. The write-up shows both with this caveat.

