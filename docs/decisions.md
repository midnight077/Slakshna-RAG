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

**C2. Vector store: FAISS `IndexFlatIP` over normalised vectors (Claude, 2026-10-11).**
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
