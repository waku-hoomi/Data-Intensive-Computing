"""Build the deterministic Week 3 update release (PowerShell friendly CLI)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urban_platform.simulation.generate import generate_release  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    default_source = ROOT / "dataset" if (ROOT / "dataset").exists() else ROOT.parent / "Dataset"
    parser.add_argument("--source", type=Path, default=default_source,
                        help="directory with original course files")
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "updates")
    parser.add_argument("--seed", type=int, default=2221)
    args = parser.parse_args()
    manifest = generate_release(args.source, args.output, seed=args.seed)
    print(json.dumps({name: {key: value for key, value in record.items()
                             if key in {"file", "new_records", "duplicate_records",
                                        "modified_records", "sha256"}}
                      for name, record in manifest["datasets"].items()}, indent=2))
    print(f"Manifest: {args.output / 'manifest.json'}")


if __name__ == "__main__":
    main()
