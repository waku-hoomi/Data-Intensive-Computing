#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p .runtime/logs .runtime/matplotlib
export MPLCONFIGDIR="$ROOT/.runtime/matplotlib"
PY="$ROOT/.venv/bin/python"
STORAGE="hdfs://127.0.0.1:19000/retail_project"
if [[ "${1:-}" == --local ]]; then STORAGE="$ROOT/data/processed"; fi
run() {
  local name="$1"; shift
  echo "Running $name; log: .runtime/logs/$name.log"
  "$@" > ".runtime/logs/$name.log" 2>&1 || { tail -n 60 ".runtime/logs/$name.log"; return 1; }
}
if [[ "$STORAGE" == hdfs:* ]]; then
  run hdfs_start "$PY" scripts/hdfs.py start
  trap '"$PY" scripts/hdfs.py stop > .runtime/logs/hdfs_stop.log 2>&1' EXIT
fi
run ingest "$PY" scripts/project.py ingest
run prepare "$PY" scripts/project.py prepare --storage "$STORAGE"
run discover "$PY" scripts/project.py discover --storage "$STORAGE"
run evaluate "$PY" scripts/project.py evaluate --storage "$STORAGE"
run verify "$PY" scripts/verify_results.py --storage "$STORAGE"
run tests "$PY" -m pytest -q tests --junitxml=results/tests.xml
if [[ "$STORAGE" == hdfs:* ]]; then run hdfs_snapshot "$PY" scripts/hdfs.py snapshot; fi
run report "$PY" scripts/build_report.py
echo 'Complete: reports/Final_Project_Report.pdf and results/'
