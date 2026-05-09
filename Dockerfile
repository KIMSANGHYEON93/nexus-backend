# NEXUS OS Backend — multi-stage production image.
#
# Stage 1 (builder) installs gcc + libpq-dev to compile the asyncpg /
# cryptography native bits, then writes the resulting site-packages to
# /install. Stage 2 (runtime) copies that prefix into a fresh slim base
# WITHOUT any compilers — the final image is significantly smaller and
# carries no build-time CVEs.
#
# Security posture must align with deploy/k8s/api-deployment.yaml:
#     runAsNonRoot: true
#     runAsUser:    10001
#     readOnlyRootFilesystem: true
#
# So this image creates UID 10001 (`appuser`) and runs as that user.
# The k8s manifest's tmpfs /tmp mount provides the writable scratch
# space Python needs for asyncio's DNS cache and httpx temp files.

# ─────────────────────────────────────────────────────────────────────
# Stage 1 — builder: compile + install Python deps
# ─────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Compilers needed for asyncpg's C extension and cryptography's wheel
# fall-back. They only live in this stage; the runtime stage stays slim.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY requirements.txt .
# --prefix lets us copy a clean tree into the runtime stage without
# tracking down individual site-packages locations.
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ─────────────────────────────────────────────────────────────────────
# Stage 2 — runtime: slim base, non-root user, no compilers
# ─────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# libpq5 is the runtime counterpart to libpq-dev — asyncpg's C extension
# dynamically links it. tini gives us proper PID 1 signal handling for
# uvicorn (CTRL-C / SIGTERM propagation under k8s).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq5 \
        tini \
    && rm -rf /var/lib/apt/lists/*

# Non-root user matching the k8s securityContext UID. Use --system so
# the /home/appuser directory isn't created (we don't need it; /tmp
# from the k8s tmpfs mount handles transient writes).
RUN groupadd --system --gid 10001 appuser \
 && useradd  --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin appuser

# Pull the compiled deps tree from the builder stage. /usr/local is
# Python's default prefix on slim, so installed packages slot in
# without changing PYTHONPATH.
COPY --from=builder /install /usr/local

WORKDIR /app

# Copy ONLY the runtime tree. Tests, k8s manifests, .env, and the
# pre-commit config are all excluded by .dockerignore — see that file
# for the full ignore list.
COPY --chown=appuser:appuser src/ /app/src/
COPY --chown=appuser:appuser db/  /app/db/

USER 10001:10001

EXPOSE 8000

# tini reaps zombie processes and forwards SIGTERM so a `kubectl delete
# pod` finishes within terminationGracePeriodSeconds rather than
# waiting for k8s to SIGKILL.
ENTRYPOINT ["/usr/bin/tini", "--"]

# Production cmd. Override with --reload in docker-compose for dev hot
# reload; k8s never overrides this.
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
