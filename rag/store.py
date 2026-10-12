"""On-disk vector store: a FAISS index plus the chunk text it points at.

Layout under `directory`:
    index.faiss   exact inner-product index over unit vectors (= cosine similarity)
    chunks.jsonl  one JSON object per chunk, in the same order as the index
    meta.json     embedding model, dimension, and the documents already ingested
"""

import hashlib
import json
import os
import threading

import faiss
import numpy as np

from .chunking import chunk_text
from .loaders import list_documents, load_document


class VectorStore:
    def __init__(self, directory, embedder):
        self.directory = directory
        self.embedder = embedder
        self.index = faiss.IndexFlatIP(embedder.dim)
        self.chunks = []
        self.documents = {}  # source name -> sha256 of the document text
        self._lock = threading.Lock()
        os.makedirs(directory, exist_ok=True)
        if os.path.exists(self._path("meta.json")):
            self._load()

    def _path(self, name):
        return os.path.join(self.directory, name)

    def _load(self):
        with open(self._path("meta.json"), "r", encoding="utf-8") as f:
            meta = json.load(f)
        if meta["embedding_model"] != self.embedder.model_name or meta["dim"] != self.embedder.dim:
            raise ValueError(
                f"index in {self.directory} was built with {meta['embedding_model']} "
                f"(dim {meta['dim']}), not {self.embedder.model_name} (dim {self.embedder.dim}); "
                "delete the directory and ingest again to switch models"
            )
        self.documents = meta["documents"]
        self.index = faiss.read_index(self._path("index.faiss"))
        with open(self._path("chunks.jsonl"), "r", encoding="utf-8") as f:
            self.chunks = [json.loads(line) for line in f if line.strip()]
        if self.index.ntotal != len(self.chunks):
            raise ValueError(f"index in {self.directory} is inconsistent: "
                             f"{self.index.ntotal} vectors, {len(self.chunks)} chunks")

    def _save(self):
        faiss.write_index(self.index, self._path("index.faiss"))
        with open(self._path("chunks.jsonl"), "w", encoding="utf-8") as f:
            for chunk in self.chunks:
                f.write(json.dumps(chunk, ensure_ascii=False) + "\n")
        meta = {
            "embedding_model": self.embedder.model_name,
            "dim": self.embedder.dim,
            "documents": self.documents,
        }
        with open(self._path("meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

    @property
    def num_chunks(self):
        return len(self.chunks)

    def add_document(self, source, text):
        """Chunk, embed and store one document. Returns the number of chunks added.

        A document already stored under the same `source` is skipped if its
        text is unchanged and rejected if it differs.
        """
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        with self._lock:
            known = self.documents.get(source)
            if known == digest:
                return 0
            if known is not None:
                raise ValueError(f"document '{source}' is already ingested with different content; "
                                 "delete the index directory and ingest again to replace it")
            pieces = chunk_text(text)
            if not pieces:
                return 0
            vectors = self.embedder.embed(pieces)
            self.index.add(vectors)
            for i, piece in enumerate(pieces):
                self.chunks.append({"chunk_id": f"{source}#{i}", "source": source, "text": piece})
            self.documents[source] = digest
            self._save()
            return len(pieces)

    def add_path(self, path):
        """Ingest a file, or every supported file under a directory."""
        files = list_documents(path)
        if not files:
            raise ValueError(f"no PDF, .txt or .md files found at {path}")
        added = 0
        for file_path in files:
            added += self.add_document(os.path.basename(file_path), load_document(file_path))
        return added

    def search(self, query, top_k):
        """Return up to `top_k` chunks most similar to `query`, best first."""
        with self._lock:
            if not self.chunks or top_k <= 0:
                return []
            vector = self.embedder.embed([query])
            scores, ids = self.index.search(vector, min(top_k, len(self.chunks)))
            results = []
            for score, idx in zip(scores[0], ids[0]):
                if idx < 0:
                    continue
                results.append({**self.chunks[idx], "score": float(score)})
            return results
