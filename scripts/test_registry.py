# scripts/test_registry.py
"""
Simple, Windows-safe test for SQLiteArtifactRegistry.
Run: python -m scripts.test_registry
"""

from pathlib import Path
from datetime import datetime, timezone
from tempfile import TemporaryDirectory

from pipeline.artifacts import Artifact
from pipeline.registry_sqlite import SQLiteArtifactRegistry


def test_registry():
    """Test artifact registry in a temporary directory; fully isolated and Windows-safe."""
    with TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_registry.db"

        # Use context manager to ensure connection cleanup
        with SQLiteArtifactRegistry(db_path) as registry:
            # --- Artifact v1 ---
            artifact = Artifact(
                id="test_normalized_bp",
                version="v1",
                content_hash="sha256:abc123def456",
                path=Path(tmpdir) / "bp_hr_normalized.csv",
                type="normalized",
                format="csv",
                created_at=datetime.now(timezone.utc),
                created_by_stage="normalization",
                created_by_run="test-run-001",
                inputs=["raw_bp:v1"],
                metadata={"row_count": 100},
            )
            print("✅ Created artifact v1")

            # --- Register v1 ---
            registry.register(artifact)
            print("✅ Registered artifact v1")

            # --- Retrieve v1 ---
            retrieved = registry.get("test_normalized_bp", "v1")
            assert retrieved is not None
            assert retrieved.content_hash == artifact.content_hash
            print("✅ Retrieved artifact v1")

            # --- Next version ---
            assert registry.next_version("test_normalized_bp") == "v2"
            print("✅ Next version calculated correctly")

            # --- Register v2 ---
            artifact_v2 = Artifact(
                id="test_normalized_bp",
                version="v2",
                content_hash="sha256:xyz789",
                path=Path(tmpdir) / "bp_hr_normalized.csv",
                type="normalized",
                format="csv",
                created_at=datetime.now(timezone.utc),
                created_by_stage="normalization",
                created_by_run="test-run-002",
                inputs=["raw_bp:v1"],
            )
            registry.register(artifact_v2)
            latest = registry.latest("test_normalized_bp")
            assert latest.version == "v2"
            print("✅ Registered artifact v2, latest is v2")

            # --- Round-trip metadata check ---
            row_artifact = registry.get("test_normalized_bp", "v1")
            assert row_artifact.metadata == {"row_count": 100}
            print("✅ Metadata preserved in round-trip")

        # No deletion needed: TemporaryDirectory is auto-cleaned
        print("\n🎯 All tests passed! Registry works correctly.")


if __name__ == "__main__":
    test_registry()
