import hashlib
from pathlib import Path


def hash_file(path: Path, chunk_size: int = 8192) -> str:
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha.update(chunk)
    return f"sha256:{sha.hexdigest()}"


def hash_bytes(data: bytes) -> str:
    sha = hashlib.sha256()
    sha.update(data)
    return f"sha256:{sha.hexdigest()}"


def hash_strings(values: list[str]) -> str:
    sha = hashlib.sha256()
    for v in values:
        sha.update(v.encode("utf-8"))
    return f"sha256:{sha.hexdigest()}"
