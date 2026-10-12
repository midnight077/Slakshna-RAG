"""Unit tests for RAG chunking and the on-disk vector store.

A fake embedder keeps these tests free of model downloads: it counts a few
keywords, so texts sharing a keyword get similar vectors.
"""

import os
import sys
import tempfile
import unittest

import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rag.chunking import chunk_text
from rag.store import VectorStore

KEYWORDS = ["moon", "mars", "rhino", "river"]


class FakeEmbedder:
    model_name = "fake-keyword-embedder"
    dim = len(KEYWORDS) + 1

    def embed(self, texts):
        rows = []
        for text in texts:
            lowered = text.lower()
            row = [float(lowered.count(k)) for k in KEYWORDS] + [0.1]
            rows.append(row)
        vectors = np.asarray(rows, dtype=np.float32)
        return vectors / np.linalg.norm(vectors, axis=1, keepdims=True)


class ChunkingTests(unittest.TestCase):
    def test_windows_overlap_and_cover_all_words(self):
        words = [f"w{i}" for i in range(450)]
        chunks = chunk_text(" ".join(words), chunk_words=200, overlap_words=40)

        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0].split()[-40:], chunks[1].split()[:40])
        self.assertEqual(chunks[-1].split()[-1], "w449")

    def test_short_and_empty_text(self):
        self.assertEqual(chunk_text("just a few words"), ["just a few words"])
        self.assertEqual(chunk_text("   "), [])

    def test_rejects_overlap_not_smaller_than_chunk(self):
        with self.assertRaises(ValueError):
            chunk_text("a b c", chunk_words=5, overlap_words=5)


class VectorStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = VectorStore(self.tmp.name, FakeEmbedder())
        self.store.add_document("moon.txt", "The moon lander touched down near the moon south pole.")
        self.store.add_document("park.txt", "The rhino lives beside the river in the park.")

    def test_search_ranks_matching_document_first(self):
        results = self.store.search("where does the rhino live", top_k=2)

        self.assertEqual(results[0]["source"], "park.txt")
        self.assertEqual(results[0]["chunk_id"], "park.txt#0")
        self.assertGreater(results[0]["score"], results[1]["score"])

    def test_top_k_is_capped_at_index_size(self):
        self.assertEqual(len(self.store.search("moon", top_k=10)), 2)
        self.assertEqual(self.store.search("moon", top_k=0), [])

    def test_index_survives_reload(self):
        reloaded = VectorStore(self.tmp.name, FakeEmbedder())

        self.assertEqual(reloaded.num_chunks, 2)
        self.assertEqual(reloaded.search("moon", top_k=1)[0]["source"], "moon.txt")

    def test_reingesting_same_document_is_a_no_op(self):
        added = self.store.add_document("moon.txt", "The moon lander touched down near the moon south pole.")

        self.assertEqual(added, 0)
        self.assertEqual(self.store.num_chunks, 2)

    def test_changed_document_is_rejected(self):
        with self.assertRaises(ValueError):
            self.store.add_document("moon.txt", "Different text about mars.")

    def test_reload_with_other_model_is_rejected(self):
        other = FakeEmbedder()
        other.model_name = "some-other-model"
        with self.assertRaises(ValueError):
            VectorStore(self.tmp.name, other)


if __name__ == "__main__":
    unittest.main()
