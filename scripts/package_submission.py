"""Package complete source, reports and measured evidence; exclude data/runtime caches."""
from pathlib import Path
import ast
import hashlib
import json
import shutil
import zipfile
import xml.etree.ElementTree as ET

ROOT=Path(__file__).resolve().parents[1]
DEST=ROOT.parent/"output/Lab2_Submission"
ZIP=DEST.parent/"Lab2_Submission.zip"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    benchmark=json.loads((ROOT/"artifacts/benchmark_all.json").read_text())
    assert len(benchmark)==10 and all(r["results_equal"] for r in benchmark)
    assert all(len(r["records"][side])==3 for r in benchmark for side in ["before","after"])
    tests=ET.parse(ROOT/"artifacts/tests.xml").getroot()
    suites=list(tests.iter("testsuite"))
    assert sum(int(s.get("tests",0)) for s in suites)==9
    assert all(int(s.get("failures",0))==int(s.get("errors",0))==0 for s in suites)
    pairs=[]
    for folder in ["src","scripts","sql","config","tests","reports"]:
        for path in (ROOT/folder).rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts and path.suffix!=".pyc":
                pairs.append((path,path.relative_to(ROOT)))
    for name in ["README.md","requirements.txt","requirements-lab2.txt",".gitignore"]:
        pairs.append((ROOT/name,Path(name)))
    for name in ["Lab2_Design_Report.md","Lab2_Benchmark_Report.md","LAB3_HANDOFF_ZH.md","LAB2_PROGRESS.md"]:
        pairs.append((ROOT/"docs"/name,Path("docs")/name))
    # Retain historical Lab 1 prose under an explicit reference directory.
    for name in ["DATA_CATALOG.md","DESIGN_REPORT.md","ARCHITECTURE.md","BENCHMARK_REPORT.md"]:
        pairs.append((ROOT/"docs"/name,Path("docs/lab1_reference")/name))
    for path in (ROOT/"artifacts").rglob("*"):
        if path.is_file() and path.suffix in {".json",".txt",".xml",".sql"}:
            pairs.append((path,Path("evidence")/path.relative_to(ROOT/"artifacts")))
    pairs.append((ROOT/"data/_metadata/ingestion_log.jsonl",Path("evidence/ingestion_log.jsonl")))
    DEST.mkdir(parents=True,exist_ok=True)
    manifest={}
    for source,relative in pairs:
        target=DEST/relative
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(source,target)
        assert digest(target)==digest(source)
        if target.suffix==".py":
            ast.parse(target.read_text())
        manifest[str(relative)]=digest(target)
    (DEST/"MANIFEST_SHA256.json").write_text(json.dumps(manifest,indent=2,sort_keys=True))
    with zipfile.ZipFile(ZIP,"w",zipfile.ZIP_DEFLATED) as archive:
        for relative in sorted(manifest):
            archive.write(DEST/relative,"Lab2_Submission/"+relative)
        archive.write(DEST/"MANIFEST_SHA256.json","Lab2_Submission/MANIFEST_SHA256.json")
    with zipfile.ZipFile(ZIP) as archive:
        assert archive.testzip() is None
        for relative,expected in manifest.items():
            assert hashlib.sha256(archive.read("Lab2_Submission/"+relative)).hexdigest()==expected
        assert not any("/.git/" in n or "/.venv/" in n or "__MACOSX" in n or ".DS_Store" in n for n in archive.namelist())
    print(json.dumps({"archive":str(ZIP),"bytes":ZIP.stat().st_size,"files":len(manifest)+1,
                      "comparison_pairs":len(benchmark),"timed_executions":60,"tests_passed":9,
                      "integrity":"CRC and all SHA-256 hashes verified"},indent=2))


if __name__=="__main__":
    main()
