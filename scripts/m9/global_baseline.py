"""The reference retrieval: an exact search over the whole corpus in one index.

    .venv-rag/bin/python scripts/m9/global_baseline.py --data datasets/textbooks \
        --out results/m9/raw/global.jsonl

For every question, the top `k` snippets by cosine similarity (exact
`IndexFlatIP` over the cached vectors, query embedded with the same model).
This is what a single central index would return, the target recall@k is
measured against (docs/merkle_tree.md, 16.5). The query is the question text
only, as both federated systems send it.
"""

import argparse
import os
import sys
import time

import faiss
import numpy as np

from m9_common import (DEFAULT_MODEL, ROOT, context_record, load_questions, log, model_slug,
                       read_jsonl, write_jsonl)

sys.path.insert(0, ROOT)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data", required=True)
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--k", type=int, default=8)
    p.add_argument("--out", required=True)
    args = p.parse_args()

    from rag.vector_cache import VectorCache
    from rag.embedder import Embedder

    cache = VectorCache(os.path.join(args.data, "embeddings", model_slug(args.model)))
    corpus = read_jsonl(os.path.join(args.data, "corpus.jsonl"))
    ids = [row["id"] for row in corpus]
    if [ids[i] for i in range(len(ids))] != sorted(cache.row, key=cache.row.get):
        sys.exit("the embedding cache does not match corpus.jsonl; run embed_cache.py again")
    index = faiss.IndexFlatIP(cache.vectors.shape[1])
    index.add(np.ascontiguousarray(cache.vectors, dtype=np.float32))

    embedder = Embedder(args.model)
    questions = load_questions(os.path.join(args.data, "questions.jsonl"))
    rows = []
    for question in questions:
        started = time.time()
        scores, found = index.search(embedder.embed([question["question"]]), args.k)
        contexts = [{"text": corpus[i]["content"], "id": corpus[i]["id"], "node": None, "score": float(s)}
                    for s, i in zip(scores[0], found[0]) if i >= 0]
        rows.append(context_record("global", None, 1, question, contexts, time.time() - started,
                                   {"model": args.model}))
    write_jsonl(args.out, rows)
    log(f"wrote the exact top {args.k} for {len(rows)} questions to {args.out}")


if __name__ == "__main__":
    main()
