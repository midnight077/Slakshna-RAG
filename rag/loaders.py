"""Read source documents into plain text."""

import os

SUPPORTED_EXTENSIONS = (".pdf", ".txt", ".md")


def load_document(path):
    """Return the text of one PDF, .txt or .md file."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".pdf":
        from pypdf import PdfReader

        reader = PdfReader(path)
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    if ext in (".txt", ".md"):
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    raise ValueError(f"unsupported document type: {path}")


def list_documents(path):
    """Return every supported file at `path` (a file, or a directory searched recursively)."""
    if os.path.isfile(path):
        return [path]
    found = []
    for root, _, files in os.walk(path):
        for name in files:
            if name.lower().endswith(SUPPORTED_EXTENSIONS):
                found.append(os.path.join(root, name))
    return sorted(found)
