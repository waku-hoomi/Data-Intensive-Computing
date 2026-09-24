"""Verify course inputs and prepare the project's ignored ``dataset`` directory.

Usage: python scripts/prepare_local_data.py D:\\Homework\\ID2221\\Dataset
The air-quality input may be the original ZIP or its already extracted CSV.
An extracted CSV is recorded with its own measured checksum; it is never
misrepresented as a verified copy of the ZIP.
"""
import argparse
import hashlib
import json
import shutil
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AIR_QUALITY_MEMBER = Path("air_quality/hourly_88101_2024.csv")


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def checked_copy(source: Path, target: Path, expected_sha256: str) -> None:
    if not source.is_file():
        raise FileNotFoundError(source)
    actual = digest(source)
    if actual != expected_sha256:
        raise ValueError(f"Source checksum mismatch: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if digest(target) != actual:
            raise ValueError(f"Existing input differs: {target}")
        return
    temporary = target.with_name(target.name + ".preparing")
    try:
        shutil.copyfile(source, temporary)
        if digest(temporary) != actual:
            raise ValueError(f"Copied input checksum mismatch: {temporary}")
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def prepare_air_quality(source_dir: Path, destination: Path, zip_entry: dict) -> dict:
    archive_path = source_dir / zip_entry["file"]
    source_csv = source_dir / AIR_QUALITY_MEMBER
    target_csv = destination / AIR_QUALITY_MEMBER
    if archive_path.is_file():
        archive_sha = digest(archive_path)
        if archive_sha != zip_entry["sha256"]:
            raise ValueError(f"Course input checksum mismatch: {archive_path}")
        with zipfile.ZipFile(archive_path) as archive:
            members = [name for name in archive.namelist() if name.endswith(AIR_QUALITY_MEMBER.name)]
            if len(members) != 1:
                raise ValueError("Expected one hourly_88101_2024.csv in air-quality ZIP")
            member = members[0]
            target_csv.parent.mkdir(parents=True, exist_ok=True)
            temporary = target_csv.with_name(target_csv.name + ".preparing")
            try:
                with archive.open(member) as input_stream, temporary.open("wb") as output_stream:
                    shutil.copyfileobj(input_stream, output_stream, length=8 * 1024 * 1024)
                if temporary.stat().st_size != archive.getinfo(member).file_size:
                    raise ValueError("Air-quality extraction is incomplete")
                extracted_sha = digest(temporary)
                if target_csv.exists():
                    if digest(target_csv) != extracted_sha:
                        raise ValueError(f"Existing input differs: {target_csv}")
                else:
                    temporary.replace(target_csv)
            finally:
                temporary.unlink(missing_ok=True)
        return {
            "source": str(archive_path.resolve()),
            "source_kind": "verified_zip",
            "source_sha256": archive_sha,
            "archive_manifest_sha256_verified": True,
            "extracted_csv_sha256": extracted_sha,
            "extracted_csv_bytes": target_csv.stat().st_size,
        }
    if not source_csv.is_file():
        raise FileNotFoundError(f"Supply {archive_path} or {source_csv}")
    extracted_sha = digest(source_csv)
    checked_copy(source_csv, target_csv, extracted_sha)
    return {
        "source": str(source_csv.resolve()),
        "source_kind": "already_extracted_csv",
        "source_sha256": extracted_sha,
        "archive_manifest_sha256_verified": False,
        "extracted_csv_sha256": extracted_sha,
        "extracted_csv_bytes": target_csv.stat().st_size,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    args = parser.parse_args()
    source_dir = args.source.resolve()
    if not source_dir.is_dir():
        parser.error(f"Source directory does not exist: {source_dir}")
    manifest = json.loads((ROOT / "config/input_manifest.json").read_text(encoding="utf-8"))
    destination = ROOT / "dataset"
    destination.mkdir(exist_ok=True)
    provenance = {"prepared_at_utc": datetime.now(timezone.utc).isoformat(), "inputs": {}}
    for entry in manifest:
        if entry["file"] == "air_quality.zip":
            provenance["inputs"]["air_quality"] = prepare_air_quality(source_dir, destination, entry)
            continue
        source = source_dir / entry["file"]
        target = destination / source.name
        checked_copy(source, target, entry["sha256"])
        provenance["inputs"][entry["file"]] = {
            "source": str(source.resolve()),
            "sha256": entry["sha256"],
            "bytes": target.stat().st_size,
            "course_manifest_sha256_verified": True,
        }
    record = destination / "source_provenance.json"
    record.write_text(json.dumps(provenance, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"destination": str(destination), "provenance": str(record),
                      "air_quality": provenance["inputs"]["air_quality"]}, indent=2))


if __name__ == "__main__":
    main()
