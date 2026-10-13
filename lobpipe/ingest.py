"""Convert Databento MBO (.dbn.zst) files to Parquet, partitioned by UTC date of ts_recv.

Databento writes a book snapshot at 00:00 UTC each day, so every date partition can be
replayed on its own. row_nr keeps the original file order, which is the order the
book must be rebuilt in.

The dataset is built in <out>.partial and renamed to <out> only after every file has
been written and _manifest.json is in place, so a failed run never leaves a dataset
that looks complete.
"""

import argparse
import json
import os
import shutil
from collections import defaultdict

import databento as db
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DAY_NS = 86_400_000_000_000
CHUNK = 5_000_000

SCHEMA = pa.schema([
    ("row_nr", pa.int64()),
    ("ts_recv", pa.int64()),
    ("ts_event", pa.int64()),
    ("instrument_id", pa.int32()),
    ("order_id", pa.int64()),
    ("price", pa.int64()),
    ("size", pa.int32()),
    ("flags", pa.int16()),
    ("action", pa.string()),
    ("side", pa.string()),
    ("sequence", pa.int64()),
])


def to_table(a, first_row):
    if len(a) and (a["order_id"].max() >= 2**63 or a["size"].max() >= 2**31):
        raise ValueError("value out of range for the parquet schema")
    cols = {
        "row_nr": np.arange(first_row, first_row + len(a), dtype=np.int64),
        "ts_recv": a["ts_recv"].astype(np.int64),
        "ts_event": a["ts_event"].astype(np.int64),
        "instrument_id": a["instrument_id"].astype(np.int32),
        "order_id": a["order_id"].astype(np.int64),
        "price": a["price"].astype(np.int64),
        "size": a["size"].astype(np.int32),
        "flags": a["flags"].astype(np.int16),
        "action": a["action"].astype("U1"),
        "side": a["side"].astype("U1"),
        "sequence": a["sequence"].astype(np.int64),
    }
    return pa.table(cols, schema=SCHEMA)


def day_label(ns):
    return str(np.datetime64(int(ns), "ns").astype("datetime64[D]"))


def _write_files(paths, out_dir):
    manifest = {"files": [], "dates": {}, "symbology": {}}
    dates = defaultdict(lambda: {"rows": 0, "instruments": defaultdict(int), "first": None})
    row = 0
    last_recv = None
    for file_idx, path in enumerate(paths):
        store = db.DBNStore.from_file(path)
        if str(store.schema) != "mbo":
            raise ValueError(f"{path}: expected mbo schema, got {store.schema}")
        for sym, spans in store.metadata.mappings.items():
            manifest["symbology"].setdefault(sym, []).extend(
                {"start": str(s["start_date"]), "end": str(s["end_date"]), "instrument_id": int(s["symbol"])}
                for s in spans
            )
        file_rows = 0
        writer, writer_day = None, None
        for a in store.to_ndarray(count=CHUNK):
            if len(a) == 0:
                continue
            recv = a["ts_recv"].astype(np.int64)
            if np.any(np.diff(recv) < 0) or (last_recv is not None and recv[0] < last_recv):
                raise ValueError(f"{path}: ts_recv is not in order")
            last_recv = recv[-1]
            day_idx = recv // DAY_NS
            cuts = np.flatnonzero(np.diff(day_idx)) + 1
            for lo, hi in zip(np.r_[0, cuts], np.r_[cuts, len(a)]):
                part = a[lo:hi]
                day = day_label(recv[lo])
                if day != writer_day:
                    if writer is not None:
                        writer.close()
                    part_dir = os.path.join(out_dir, f"date={day}")
                    os.makedirs(part_dir, exist_ok=True)
                    writer = pq.ParquetWriter(os.path.join(part_dir, f"part-{file_idx:03d}.parquet"), SCHEMA)
                    writer_day = day
                writer.write_table(to_table(part, row), row_group_size=1_000_000)
                d = dates[day]
                if d["first"] is None:
                    d["first"] = {"row_nr": row, "action": part["action"][0].decode(), "flags": int(part["flags"][0])}
                d["rows"] += len(part)
                iids, counts = np.unique(part["instrument_id"], return_counts=True)
                for iid, c in zip(iids, counts):
                    d["instruments"][str(int(iid))] += int(c)
                row += len(part)
                file_rows += len(part)
        if writer is not None:
            writer.close()
        manifest["files"].append({"path": os.path.abspath(path), "rows": file_rows})
        print(f"{path}: {file_rows:,} rows")
    for day in sorted(dates):
        d = dates[day]
        manifest["dates"][day] = {"rows": d["rows"], "instruments": dict(d["instruments"]), "first": d["first"]}
    manifest["rows"] = row
    return manifest


def ingest(paths, out_dir, overwrite=False):
    out_dir = out_dir.rstrip("/")
    if os.path.exists(out_dir) and not overwrite:
        raise FileExistsError(f"{out_dir} exists (use --overwrite)")
    tmp = out_dir + ".partial"
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    manifest = _write_files(paths, tmp)
    with open(os.path.join(tmp, "_manifest.json"), "w") as f:
        json.dump(manifest, f, indent=1)
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.replace(tmp, out_dir)
    print(f"{manifest['rows']:,} rows in {len(manifest['dates'])} date partitions -> {out_dir}")
    return manifest


def main():
    p = argparse.ArgumentParser(description="Convert Databento MBO files to a date-partitioned Parquet dataset.")
    p.add_argument("--out", required=True, help="output directory for the parquet dataset")
    p.add_argument("--overwrite", action="store_true", help="replace an existing output directory")
    p.add_argument("files", nargs="+", help="Databento MBO files, in time order")
    args = p.parse_args()
    ingest(args.files, args.out, args.overwrite)


if __name__ == "__main__":
    main()
