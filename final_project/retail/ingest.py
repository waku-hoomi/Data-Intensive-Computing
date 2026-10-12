"""Read Excel without editing it; conversion only, no analytical filtering."""
import datetime as dt
import hashlib
import io
import platform
import zipfile
from pathlib import Path

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq

from .common import ROOT, save_json, sha256

SOURCE_URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
EXPECTED_WORKBOOK_SHA256 = "bcbe73b35f5b7babf197fb0cb983a11f5d9ff929078d4aa53d171b1f2df2e980"
SCHEMA = pa.schema([
    ("invoice", pa.string()), ("stock_code", pa.string()),
    ("description", pa.string()), ("quantity", pa.float64()),
    ("invoice_date", pa.timestamp("us")), ("price", pa.float64()),
    ("customer_id", pa.string()), ("country", pa.string()),
    ("source_sheet", pa.string()), ("source_row", pa.int64())])


def identifier(value):
    if value is None:
        return None
    if isinstance(value, (float, int)) and float(value).is_integer():
        return str(int(value))
    return str(value).strip() or None


def number(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def ingest(source=None):
    source = Path(source or ROOT / "data/raw/online_retail_ii.zip")
    out = ROOT / "data/converted"
    out.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(source) as archive:
        names = [n for n in archive.namelist() if n.endswith(".xlsx")]
        if len(names) != 1:
            raise ValueError("Expected exactly one Excel workbook in official ZIP")
        workbook_bytes = archive.read(names[0])
    if hashlib.sha256(workbook_bytes).hexdigest() != EXPECTED_WORKBOOK_SHA256:
        raise ValueError("Workbook differs from the recorded official source; inspect before starting a new experiment")
    workbook = openpyxl.load_workbook(io.BytesIO(workbook_bytes), read_only=True, data_only=True)
    manifest = {"source_url": SOURCE_URL, "zip_sha256": sha256(source),
                "workbook_sha256": hashlib.sha256(workbook_bytes).hexdigest(),
                "python": platform.python_version(), "sheets": []}
    expected = ["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country"]
    for sheet in workbook:
        rows = sheet.iter_rows(values_only=True)
        if list(next(rows)) != expected:
            raise ValueError(f"Unexpected schema: {sheet.title}")
        target = out / (sheet.title.replace(" ", "_") + ".parquet")
        count, low, high, batch = 0, None, None, []
        with pq.ParquetWriter(target, SCHEMA, compression="snappy") as writer:
            for index, values in enumerate(rows, 2):
                if all(v is None for v in values):
                    continue
                inv, stock, desc, quantity, date, price, customer, country = values
                date = date if isinstance(date, dt.datetime) else None
                row = dict(zip(SCHEMA.names, [identifier(inv), identifier(stock), identifier(desc),
                           number(quantity), date, number(price), identifier(customer),
                           identifier(country), sheet.title, index]))
                batch.append(row)
                count += 1
                if date:
                    low = date if low is None else min(low, date)
                    high = date if high is None else max(high, date)
                if len(batch) >= 25000:
                    writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
                    batch = []
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
        manifest["sheets"].append({"sheet": sheet.title, "rows": count,
                                   "min_date": str(low), "max_date": str(high),
                                   "parquet_sha256": sha256(target)})
        print(f"Converted {sheet.title}: {count:,} rows", flush=True)
    workbook.close()
    save_json(ROOT / "results/source_manifest.json", manifest)
    return manifest
