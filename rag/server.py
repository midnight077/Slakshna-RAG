"""Local HTTP API of the RAG sidecar. Bound to 127.0.0.1 by rag_engine.py."""

import time
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class RetrieveRequest(BaseModel):
    query: str
    top_k: int = 5


class GenerateRequest(BaseModel):
    query: str
    chunks: List[dict]


class Document(BaseModel):
    id: str
    text: str


class IngestRequest(BaseModel):
    path: Optional[str] = None
    documents: Optional[List[Document]] = None


def create_app(store, generator):
    app = FastAPI(title="SLAKSHNA RAG sidecar")

    @app.get("/health")
    def health():
        return {
            "ready": True,
            "embedding_model": store.embedder.model_name,
            "dim": store.embedder.dim,
            "num_chunks": store.num_chunks,
            "num_documents": len(store.documents),
            "generator": generator.name,
            "generator_model": generator.model,
        }

    @app.post("/retrieve")
    def retrieve(request: RetrieveRequest):
        started = time.perf_counter()
        chunks = store.search(request.query, request.top_k)
        return {
            "chunks": chunks,
            "embedding_model": store.embedder.model_name,
            "retrieve_ms": round((time.perf_counter() - started) * 1000, 1),
        }

    @app.post("/generate")
    def generate(request: GenerateRequest):
        started = time.perf_counter()
        try:
            answer = generator.generate(request.query, request.chunks)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"generator failed: {e}")
        return {
            "answer": answer,
            "model": generator.model,
            "generate_ms": round((time.perf_counter() - started) * 1000, 1),
        }

    @app.post("/ingest")
    def ingest(request: IngestRequest):
        if (request.path is None) == (request.documents is None):
            raise HTTPException(status_code=400, detail="give exactly one of 'path' or 'documents'")
        try:
            if request.path is not None:
                added = store.add_path(request.path)
            else:
                added = sum(store.add_document(doc.id, doc.text) for doc in request.documents)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"added_chunks": added, "num_chunks": store.num_chunks}

    return app
