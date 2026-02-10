"""Hashing utilities for artifact identity and cache keys."""

import hashlib
import inspect
from pathlib import Path
from typing import Iterable, Any


def hash_file(path: Path, chunk_size: int = 8192) -> str:
    """Compute SHA-256 hash for a file on disk.

    Reads the file in chunks to avoid loading large files entirely into
    memory. Used for content-addressable artifact identity.

    Args:
        path: Path to the file to hash.
        chunk_size: Number of bytes to read per chunk.

    Returns:
        Hex-encoded hash string with ``\"sha256:\"`` prefix.
    """
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha.update(chunk)
    return f"sha256:{sha.hexdigest()}"


def hash_bytes(data: bytes) -> str:
    """Compute SHA-256 hash for an in-memory byte buffer."""
    sha = hashlib.sha256()
    sha.update(data)
    return f"sha256:{sha.hexdigest()}"


def hash_strings(values: Iterable[str]) -> str:
    """Compute SHA-256 hash for a sequence of strings.

    Strings are encoded as UTF-8 and fed to the digest in order. This is
    useful for hashing configuration tuples, identifiers, or small pieces
    of structured state.

    Args:
        values: Iterable of string values to hash.

    Returns:
        Hex-encoded hash string with ``\"sha256:\"`` prefix.
    """
    sha = hashlib.sha256()
    for v in values:
        sha.update(v.encode("utf-8"))
    return f"sha256:{sha.hexdigest()}"


def hash_source(obj: Any) -> str:
    """
    Extract and hash the source code of a function, class, or module.
    Returns a SHA-256 hash string.
    """
    try:
        source = inspect.getsource(obj)
        return hashlib.sha256(source.encode("utf-8")).hexdigest()
    except (OSError, TypeError):
        # Fallback for built-ins or dynamically generated code
        return "source_unavailable"