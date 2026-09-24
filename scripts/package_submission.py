"""Build and verify a Week 3 source/evidence archive without runtime caches."""
import ast
import hashlib
import json
import shutil
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT.parent / "output/Week3_Submission"
ZIP = DEST.parent / "Week3_Submission.zip"
UPDATE_NAMES = ("taxi_trips.parquet", "weather.csv", "air_quality.csv",
                "taxi_zone_lookup.csv", "manifest.json")


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def _files(folder: str, destination: str | None = None):
    base = ROOT / folder
    if base.is_dir():
        for path in sorted(base.rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc":
                yield path, Path(destination or folder) / path.relative_to(base)


def _test_counts(xml_paths: list[Path]) -> tuple[int, int]:
    passed = failed = 0
    for path in xml_paths:
        root = ET.parse(path).getroot()
        for case in root.iter("testcase"):
            tags = {child.tag for child in case}
            failed += bool(tags & {"failure", "error"})
            passed += not bool(tags & {"failure", "error", "skipped"})
    return passed, failed


def main() -> None:
    required = [ROOT / "docs/Lab3_Design_Report.md",
                ROOT / "docs/Lab3_Evaluation_Report.md",
                ROOT / "reports/Lab3_Design_Report.pdf",
                ROOT / "reports/Lab3_Evaluation_Report.pdf"]
    required += [ROOT / "data/updates" / name for name in UPDATE_NAMES]
    missing = [str(path.relative_to(ROOT)) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError("Week 3 submission is incomplete: " + ", ".join(missing))

    pairs = []
    for folder in ("src", "scripts", "sql", "config", "tests", "docs", "reports"):
        pairs.extend(_files(folder))
    pairs.extend(_files("evidence", "evidence/lab2_reference"))
    # Keep final reproducible evidence. Rendered PDF preview PNGs, extracted
    # inspection text, and superseded focused-test XML are local QA scratch
    # files rather than submission evidence.
    for source, relative in _files("artifacts/week3", "evidence/week3"):
        if "pdf_render" in source.parts or "audit_review" in source.parts or source.suffix in {".txt", ".log"}:
            continue
        if source.suffix == ".xml" and source.name != "pytest-all.xml":
            continue
        pairs.append((source, relative))
    pairs.extend((ROOT / "data/updates" / name, Path("updates") / name)
                 for name in UPDATE_NAMES)
    provenance = ROOT / "dataset/source_provenance.json"
    if provenance.is_file():
        pairs.append((provenance, Path("evidence/source_provenance.json")))
    environment = ROOT / "artifacts/environment.json"
    if environment.is_file():
        pairs.append((environment, Path("evidence/week3/environment.json")))
    for name in ("README.md", "requirements.txt", "requirements-lab2.txt", ".gitignore"):
        pairs.append((ROOT / name, Path(name)))

    # Gate on the latest complete regression, not accumulated historical runs.
    xml_paths = [ROOT / "artifacts/week3/pytest-all.xml"]
    tests_passed, tests_failed = _test_counts(xml_paths)
    if tests_failed or tests_passed == 0:
        raise ValueError(f"Cannot package failed test evidence: {tests_failed} failures/errors")
    output_root = (ROOT.parent / "output").resolve()
    destination = DEST.resolve()
    if output_root not in destination.parents:
        raise ValueError(f"Refusing to replace package directory outside {output_root}")
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True, exist_ok=True)
    manifest = {}
    for source, relative in pairs:
        target = DEST / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        source_hash = digest(source)
        if digest(target) != source_hash:
            raise ValueError(f"Package copy failed integrity check: {relative}")
        if target.suffix == ".py":
            ast.parse(target.read_text(encoding="utf-8"))
        manifest[relative.as_posix()] = source_hash
    manifest_path = DEST / "MANIFEST_SHA256.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    with zipfile.ZipFile(ZIP, "w", zipfile.ZIP_DEFLATED) as archive:
        for relative in sorted(manifest):
            archive.write(DEST / relative, "Week3_Submission/" + relative)
        archive.write(manifest_path, "Week3_Submission/MANIFEST_SHA256.json")
    with zipfile.ZipFile(ZIP) as archive:
        if archive.testzip() is not None:
            raise ValueError("Archive CRC validation failed")
        for relative, expected in manifest.items():
            with archive.open("Week3_Submission/" + relative) as stream:
                hasher = hashlib.sha256()
                for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                    hasher.update(block)
                if hasher.hexdigest() != expected:
                    raise ValueError(f"Archive checksum mismatch: {relative}")
    print(json.dumps({"archive": str(ZIP), "bytes": ZIP.stat().st_size,
                      "files": len(manifest) + 1, "tests_passed": tests_passed,
                      "integrity": "CRC and all SHA-256 hashes verified"}, indent=2))


if __name__=="__main__":
    main()
