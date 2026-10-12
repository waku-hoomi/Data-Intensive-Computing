#!/usr/bin/env python3
"""Package committed final-project files only, with verified SHA-256 manifest."""
import argparse
import hashlib
import json
import subprocess
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo = ROOT.parent
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    paths = subprocess.check_output(["git", "ls-files", "final_project"], cwd=repo, text=True).splitlines()
    if not paths:
        raise ValueError("Commit final_project before packaging")
    files = {}
    for name in paths:
        data = (repo / name).read_bytes()
        committed = subprocess.check_output(["git", "show", f"{commit}:{name}"], cwd=repo)
        if data != committed:
            raise ValueError(f"Uncommitted content: {name}")
        files[name] = data
    files["SOURCE_VERSION.txt"] = f"Repository: https://github.com/waku-hoomi/Data-Intensive-Computing\nBranch: codex/retail-final-project\nCommit: {commit}\n".encode()
    files["SUBMISSION_README.txt"] = b"ID2221 Final Project - Group 6\n\nThe two-page report is final_project/reports/Final_Project_Report.pdf.\nComplete code, pinned dependencies, configuration, tests, results and instructions\nare in final_project/. See final_project/README.md.\n\nFrom final_project/: bash scripts/setup.sh, then bash scripts/run_all.sh.\nOriginal data and runtimes are downloaded by setup and are not bundled.\nThe approved proposal is included under final_project/docs/.\n"
    manifest = {name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())}
    files["MANIFEST_SHA256.json"] = (json.dumps(manifest, indent=2) + "\n").encode()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(files.items()):
            archive.writestr(name, data)
    with zipfile.ZipFile(args.output) as archive:
        assert archive.testzip() is None
        for name, expected in manifest.items():
            assert hashlib.sha256(archive.read(name)).hexdigest() == expected
    print(f"Verified {len(files)} packaged files: {args.output}")


if __name__ == "__main__":
    main()
