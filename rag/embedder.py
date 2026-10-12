"""Turn text into unit-length vectors with a sentence-transformers model."""

import numpy as np


class Embedder:
    def __init__(self, model_name):
        from sentence_transformers import SentenceTransformer

        self.model_name = model_name
        self.model = SentenceTransformer(model_name, device="cpu")
        self.dim = self.embed(["dimension probe"]).shape[1]

    def embed(self, texts):
        """Return a float32 array of shape (len(texts), dim) with unit-length rows."""
        vectors = self.model.encode(
            list(texts),
            batch_size=32,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        return np.asarray(vectors, dtype=np.float32)
