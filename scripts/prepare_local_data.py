"""Copy the six course files from a supplied directory, verifying checksums.

Usage: python scripts/prepare_local_data.py /path/to/course/raw
Existing different files are refused, never silently replaced.
"""
import argparse
import hashlib
import json
import shutil
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    args = parser.parse_args()
    manifest = json.loads((ROOT / "config/input_manifest.json").read_text())
    dest = ROOT / "dataset"
    dest.mkdir(exist_ok=True)
    for entry in manifest:
        source = args.source / entry["file"]
        if digest(source) != entry["sha256"]:
            raise ValueError(f"Course input checksum mismatch: {source}")
        target = dest / source.name
        if target.exists():
            if digest(target) != entry["sha256"]:
                raise ValueError(f"Existing input differs: {target}")
        else:
            shutil.copy2(source, target)
    with zipfile.ZipFile(dest / "air_quality.zip") as archive:
        member = next(n for n in archive.namelist() if n.endswith("hourly_88101_2024.csv"))
        target = dest / "air_quality/hourly_88101_2024.csv"
        target.parent.mkdir(exist_ok=True)
        if not target.exists():
            with archive.open(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output, length=8 * 1024 * 1024)
        if target.stat().st_size != archive.getinfo(member).file_size:
            raise ValueError("Air-quality extraction is incomplete")
    print("Verified six course inputs and extracted air quality.")


if __name__ == "__main__":
    main()
