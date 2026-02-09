import sqlite3
from config.settings import config
from tabulate import tabulate  # pip install tabulate if needed, or just print raw

db_path = config.PROCESSED_DATA_DIR / "registry.db"


def inspect():
    if not db_path.exists():
        print(f"❌ Registry not found at {db_path}")
        return

    print(f"📂 Opening Registry: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("SELECT id, version, type, created_at FROM artifacts ORDER BY created_at DESC").fetchall()

    if not rows:
        print("⚠️ Registry is empty.")
        return

    data = [dict(r) for r in rows]
    print(f"✅ Found {len(data)} artifacts:")
    print(tabulate(data, headers="keys", tablefmt="grid"))
    conn.close()


if __name__ == "__main__":
    inspect()