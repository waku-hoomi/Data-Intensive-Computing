#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p .runtime data/raw
PYTHON="${RETAIL_PYTHON:-python3}"
if [[ ! -x .venv/bin/python ]]; then "$PYTHON" -m venv .venv; fi
if [[ -f results/environment_lock.txt ]]; then
  .venv/bin/python -m pip install --cache-dir .runtime/pip-cache -r requirements.txt -c results/environment_lock.txt
else
  .venv/bin/python -m pip install --cache-dir .runtime/pip-cache -r requirements.txt
fi
if [[ ! -x .runtime/java_home/bin/java && -z "${JAVA_HOME:-}" ]]; then
  if [[ "$(uname -s)" == Darwin ]]; then platform=mac; else platform=linux; fi
  if [[ "$(uname -m)" == arm64 || "$(uname -m)" == aarch64 ]]; then arch=aarch64; else arch=x64; fi
  curl -fL --retry 2 "https://api.adoptium.net/v3/binary/latest/17/ga/$platform/$arch/jdk/hotspot/normal/eclipse" -o .runtime/java.tar.gz
  mkdir -p .runtime/java-unpacked
  tar -xzf .runtime/java.tar.gz -C .runtime/java-unpacked
  .venv/bin/python - <<'PY'
from pathlib import Path
root=Path.cwd()
java=next((root/'.runtime/java-unpacked').glob('**/bin/java'))
(root/'.runtime/java_home').symlink_to(java.parent.parent)
PY
fi
if [[ ! -d .runtime/hadoop-3.4.2 ]]; then
  curl -fL --retry 2 https://dlcdn.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz -o .runtime/hadoop.tar.gz
  curl -fL --retry 2 https://downloads.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz.sha512 -o .runtime/hadoop.sha512
  .venv/bin/python - <<'PY'
from pathlib import Path
import hashlib,re
p=Path('.runtime')
expected=re.findall(r'\b[0-9a-fA-F]{128}\b',(p/'hadoop.sha512').read_text())[0].lower()
h=hashlib.sha512()
with (p/'hadoop.tar.gz').open('rb') as f:
 for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
assert h.hexdigest()==expected, 'Hadoop download checksum mismatch'
PY
  tar -xzf .runtime/hadoop.tar.gz -C .runtime hadoop-3.4.2/bin hadoop-3.4.2/etc hadoop-3.4.2/libexec hadoop-3.4.2/share/hadoop
fi
if [[ ! -f data/raw/online_retail_ii.zip ]]; then
  curl -fL --retry 2 https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip -o data/raw/online_retail_ii.zip
fi
echo 'Setup complete. Run bash scripts/run_all.sh'
