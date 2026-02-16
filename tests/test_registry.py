import pytest
from pathlib import Path
from datetime import datetime, timezone
from pipeline.registry_sqlite import SQLiteArtifactRegistry
from pipeline.artifacts import Artifact

@pytest.fixture
def registry(tmp_path):
    """Create a registry backed by a temp file for each test."""
    db_path = tmp_path / "test.db"
    return SQLiteArtifactRegistry(db_path)

def test_register_and_retrieve(registry, tmp_path):
    """Test basic save and load."""
    # Create dummy artifact
    artifact = Artifact(
        id="test_art",
        version="v1",
        content_hash="sha256:123",
        path=tmp_path / "data.csv",
        type="raw",
        format="csv",
        created_at=datetime.now(timezone.utc)
    )
    
    registry.register(artifact)
    
    # Retrieve
    retrieved = registry.get("test_art", "v1")
    assert retrieved is not None
    assert retrieved.content_hash == "sha256:123"

def test_version_increment(registry):
    """Test v1 -> v2 logic."""
    assert registry.next_version("new_art") == "v1"
    
    # Register v1
    a1 = Artifact(
        id="new_art", version="v1", content_hash="h1", 
        path=Path("p1"), type="t", format="f"
    )
    registry.register(a1)
    
    assert registry.next_version("new_art") == "v2"