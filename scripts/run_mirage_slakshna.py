"""Ask a Slakshna-RAG node the same MIRAGE questions Flower FedRAG used, then answer
them with Flower's own LLMQuerier, so both frameworks share one LLM, prompt and
answer-extraction rule. Only retrieval differs.

Run with the FLOWER FedRAG virtualenv (it has transformers, torch and the
`fedrag` package). The Slakshna node must be running with generator = "none".

    python run_mirage_slakshna.py \
        --fedrag-dir /path/to/fedrag \
        --api http://127.0.0.1:18545 \
        --out results/slakshna_textbooks.jsonl

Defaults mirror the Flower run: pubmedqa|bioasq, first 10 questions each,
k = 8 per node and 8 after merging, SmolLM2-1.7B-Instruct on GPU.
"""

import argparse
import json
import os
import sys
import time
import urllib.request
from collections import defaultdict


def ask(api, body, timeout):
    request = urllib.request.Request(f"{api}/rag/query", data=json.dumps(body).encode(),
                                     headers={"content-type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--fedrag-dir", required=True, help="Flower examples/fedrag folder (has data/mirage.json)")
    p.add_argument("--api", default="http://127.0.0.1:18545")
    p.add_argument("--datasets", default="pubmedqa|bioasq")
    p.add_argument("--num", type=int, default=10, help="questions per dataset (0 = all)")
    p.add_argument("--k", type=int, default=8, help="top_k per node and final_k after merging")
    p.add_argument("--scope", default="all", choices=["all", "local", "peers"])
    p.add_argument("--llm", default="HuggingFaceTB/SmolLM2-1.7B-Instruct")
    p.add_argument("--cpu", action="store_true", help="run the LLM on CPU (default: GPU)")
    p.add_argument("--out", required=True)
    p.add_argument("--timeout", type=float, default=120)
    args = p.parse_args()

    sys.path.insert(0, os.path.abspath(args.fedrag_dir))
    from fedrag.llm_querier import LLMQuerier
    from fedrag.mirage_qa import MirageQA
    from sklearn.metrics import accuracy_score

    mirage_file = os.path.join(args.fedrag_dir, "data", "mirage.json")
    llm = LLMQuerier(args.llm, use_gpu=not args.cpu)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    expected, predicted = defaultdict(list), defaultdict(list)
    unanswered, times = defaultdict(int), defaultdict(list)
    with open(args.out, "w", encoding="utf-8") as out:
        for name in [d.lower() for d in args.datasets.split("|")]:
            dataset = MirageQA(name, mirage_file)  # same sorted order Flower iterates
            n = len(dataset) if args.num == 0 else min(args.num, len(dataset))
            for idx in range(n):
                q = dataset[idx]
                t0 = time.time()
                resp = ask(args.api, {"query": q["question"], "top_k": args.k, "final_k": args.k,
                                      "scope": args.scope}, args.timeout)
                t1 = time.time()
                docs = [c["text"] for c in resp["contexts"]]
                _, pred = llm.answer(q["question"], docs, q["options"], name)
                t2 = time.time()
                if pred is None:
                    unanswered[name] += 1
                else:
                    expected[name].append(q["answer"])
                    predicted[name].append(pred)
                    times[name].append(t2 - t0)
                record = {
                    "dataset": name, "q_index": idx + 1, "question": q["question"], "answer": q["answer"],
                    "predicted": pred, "retrieval_s": round(t1 - t0, 3), "llm_s": round(t2 - t1, 3),
                    "node_timings_ms": resp["timings_ms"], "peers": resp["peers"],
                    "contexts": [{k: c.get(k) for k in ("node_id", "chunk_id", "score")} for c in resp["contexts"]],
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                out.flush()
                nodes = defaultdict(int)
                for c in resp["contexts"]:
                    nodes[(c.get("node_id") or "?")[:12]] += 1
                print(f"{name}_{idx + 1}: pred={pred} gold={q['answer']} "
                      f"retrieval={t1 - t0:.2f}s llm={t2 - t1:.2f}s contexts_by_node={dict(nodes)}", file=sys.stderr)

    print("\nSlakshna-RAG (feat/p2p-rag), same LLM/prompt as Flower FedRAG")
    for name in [d.lower() for d in args.datasets.split("|")]:
        exp, pred = expected[name], predicted[name]
        total = len(exp) + unanswered[name]
        acc = accuracy_score(exp, pred) if exp else 0.0
        correct = sum(e == p for e, p in zip(exp, pred))
        mean_t = sum(times[name]) / len(times[name]) if times[name] else float("nan")
        print(f"QA Dataset: {name}\n  Total Questions: {total}\n  Answered Questions: {len(pred)}\n"
              f"  Accuracy (answered only, Flower's definition): {acc}\n"
              f"  Accuracy (over all questions): {correct / total if total else 0.0}\n"
              f"  Mean Querying Time: {mean_t}")


if __name__ == "__main__":
    main()
