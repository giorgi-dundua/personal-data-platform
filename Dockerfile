# ==========================================
# Stage 1: Builder (Compiles dependencies)
# ==========================================
FROM python:3.10-slim as builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# ==========================================
# Stage 2: Runtime (The actual image)
# ==========================================
FROM python:3.10-slim

WORKDIR /app

# Create non-root runtime user
RUN useradd -m pipeline_user

# Copy prebuilt virtualenv from builder
COPY --from=builder /opt/venv /opt/venv

# Runtime environment
# VIRTUAL_ENV improves tool compatibility and introspection
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH="/app"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Copy application code
COPY config/ config/
COPY ingestion/ ingestion/
COPY pipeline/ pipeline/
COPY processing/ processing/
COPY scripts/ scripts/
COPY main.py .
COPY dashboard.py .

# Create writable data dirs and secret mount point
# Pre-create .secrets to control ownership for volume mounts
RUN mkdir -p data/raw/google_sheets \
    data/raw/mi_band \
    data/processed/normalized \
    data/processed/validated \
    data/processed/merged \
    .secrets && \
    chown -R pipeline_user:pipeline_user /app

# Switch to non-root user
USER pipeline_user

# Generate Mock Data (Build-Time)
# Set SKIP_VALIDATION=true so settings.py doesn't crash on missing secrets
# Ensures the CSV exists inside the image even though data/ is ignored
RUN SKIP_VALIDATION=true python -m scripts.generate_mock_data

# Default command
CMD ["python", "main.py", "--help"]