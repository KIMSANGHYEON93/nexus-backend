#!/usr/bin/env bash
# Codespace / devcontainer setup. Runs once after the container is built.
#
# Why a separate script (vs. inlining in devcontainer.json):
#   - multi-line conditional logic doesn't fit cleanly in JSON
#   - shellcheck-able and locally re-runnable for debugging
#
# Idempotent: every step is safe to re-run on an existing Codespace.

set -euo pipefail

echo "─── [1/5] Python deps (runtime + dev) ───"
pip install --upgrade pip
pip install -r requirements-dev.txt

echo "─── [2/5] pre-commit hooks ───"
if [ -f .pre-commit-config.yaml ]; then
  pip install pre-commit
  pre-commit install --install-hooks
fi

echo "─── [3/5] .env scaffold ───"
if [ ! -f .env ] && [ -f .env.example ]; then
  cp .env.example .env
  echo "  → .env created from .env.example. Fill in KIS_APP_KEY, KIS_APP_SECRET, KIS_ACCOUNT_NUMBER before running the API."
else
  echo "  → .env already present, skipping."
fi

echo "─── [4/5] Claude Code CLI ───"
if ! command -v claude >/dev/null 2>&1; then
  curl -fsSL https://claude.ai/install.sh | bash || echo "  → Claude CLI install failed (non-fatal). Run manually later."
else
  echo "  → claude already installed: $(claude --version 2>/dev/null || echo unknown)"
fi

echo "─── [5/5] Smoke check ───"
python -c "import fastapi, uvicorn, asyncpg, redis; print('  → core imports OK')"
mypy --version
pytest --version

cat <<'EOF'

✔ Codespace ready.

Next steps:
  1. Fill in .env (KIS_APP_KEY, KIS_APP_SECRET, KIS_ACCOUNT_NUMBER)
  2. docker compose up -d           # start TimescaleDB + Redis
  3. python -m db.migrate && python -m db.seed
  4. uvicorn src.main:app --reload  # API on :8000
  5. claude                         # start Claude Code CLI

EOF
