# Personal Data Platform (PDP)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Architecture](https://img.shields.io/badge/Architecture-DAG-orange)
![Database](https://img.shields.io/badge/Registry-SQLite-green)
![Code Style](https://img.shields.io/badge/Code%20Style-Pydantic-red)

A production-grade, artifact-driven data engineering pipeline designed to ingest, normalize, and aggregate personal health telemetry (Blood Pressure, Heart Rate, Sleep) from heterogeneous sources.

**Core Engineering Signal:** This project demonstrates **Idempotency**, **Atomic I/O**, **Cache-Aware Execution**, and **Metadata Management** without relying on heavy external frameworks like Airflow or Dagster.

## 🏗 Architecture

The pipeline follows a **Push-Model** architecture where nodes return explicit artifact paths, coordinated by a central Orchestrator backed by a SQLite Registry.

```mermaid
graph TD
    subgraph "Orchestration Layer"
        Orchestrator[Pipeline Orchestrator]
        Registry[(SQLite Artifact Registry)]
        State[Pipeline State JSON]
    end

    subgraph "Ingestion Layer"
        GS[Google Sheets Source]
        MB[Mi Band Drive Source]
        Raw{Atomic Write}
    end

    subgraph "Processing Layer"
        Norm[Normalizers]
        Val[Validators]
        Merge[Aggregator]
    end

    Orchestrator -->|Topological Sort| Norm
    Orchestrator -->|Check Hash| Registry
    
    GS --> Raw
    MB --> Raw
    Raw -->|Push Paths| Orchestrator
    
    Norm -->|Push Paths| Orchestrator
    Val -->|Push Paths| Orchestrator
    Merge -->|Push Paths| Orchestrator
    
    Orchestrator -->|Register Artifact| Registry
```
## 🚀 Key Features
### 1. Cache-Aware Execution (Memoization)
The Orchestrator calculates a deterministic Input Hash for every stage, combining:
* **Data Identity:** SHA-256 content hash of input files.
* **Code Identity:** AST-based source code hashing of logic classes (inspect.getsource).
* **Config Identity:** State of global settings and pyproject.toml.
If the hash matches a valid entry in the Registry, the stage is skipped entirely (0ms execution time).
### 2. Atomic I/O (Write-Ahead Log Pattern)
To prevent "Zombie Artifacts" (corrupted files from crashed runs), the pipeline uses an **Atomic Rename Pattern**:
1. Write: Nodes write to filename.csv.<uuid>.tmp.
2. Verify: Node returns success.
3. Commit: Orchestrator performs an OS-level atomic rename (replace()) to .csv and registers the metadata in the same transaction scope.
### 3. Artifact Registry
Metadata is treated as a first-class citizen. A SQLite backend tracks:
* **Lineage:** Which inputs produced which outputs.
* **Versioning:** Automatic v1, v2 incrementing.
* **Audit:** Execution timestamps and row counts.

## 🛠️ Setup & Usage
### Prerequisites
* Python 3.10+
* Google Cloud Service Account (Drive & Sheets API enabled)

### Installation
```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/personal-data-platform.git
cd personal-data-platform

# 2. Install dependencies
pip install -e .

# 3. Configure Secrets
cp .env.example .env
# Edit .env with your Google Credentials path and IDs
```

### Running the Pipeline

The CLI (main.py) supports granular control over execution.
```bash
# Standard Run (Ingest -> Normalize -> Validate -> Merge)
python main.py

# Skip Ingestion (Use existing raw files)
python main.py --skip-ingestion

# Resume from a specific stage (e.g., Validation)
python main.py --start-stage validation

# Clean up all generated artifacts (Dry Run)
python main.py --clean --dry-run
```
## 📂 Project Structure
```text
personal-data-platform/
├── config/             # Pydantic settings & logging config
├── ingestion/          # Source connectors (Strategy Pattern)
├── pipeline/           # Core Engine
│   ├── orchestrator.py # DAG execution & Caching logic
│   ├── registry_sqlite.py # Repository Pattern for Metadata
│   ├── dag.py          # Dependency Graph definition
│   └── nodes.py        # Stage wrappers
├── processing/         # Business Logic (Normalizers/Validators)
├── scripts/            # Utility scripts (Cleanup, Inspection)
└── main.py             # CLI Entrypoint
```

## 🛡️ Design Patterns Used

* Repository Pattern: Decoupling database access from domain logic (registry_sqlite.py).
* Strategy Pattern: Interchangeable ingestion sources (ingestion/interfaces.py).
* Inversion of Control: Nodes "push" results to the Orchestrator rather than the Orchestrator "pulling" from hardcoded paths.
* Guard Clauses: Enforcing "Identity > Status" in execution logic.

## 🔜 Roadmap

- [x] DAG Topological Sort  
- [x] SQLite Artifact Registry  
- [x] Atomic File Operations  

- [ ] Unit Tests for DAG Logic  
- [ ] Visualization Layer (Streamlit/Dash)  
- [ ] Docker Containerization