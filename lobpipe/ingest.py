"""Convert Databento MBO files (.dbn.zst or .dbn) to Parquet with Spark, one folder per
UTC date of ts_recv. Inputs and output can be local paths or s3:// URIs.

1. Every file's header is read in parallel (only the first few KB) and the files are put
   in time order by their start time.
2. Each file is decoded by its own Spark task. row_nr = (file index << 40) | position in
   the file, so sorting by row_nr gives the original order across all files.
3. Spark writes the dataset and its _SUCCESS marker. A summary pass then checks that no
   instrument-day mixes files out of time order and writes _manifest.json. The rebuild
   only reads a dataset that has the manifest.

Each date in Databento's files begins with a book snapshot (at 00:00 UTC, or CME's at the
Sunday reopen), so every instrument-day can be rebuilt on its own. Files split by day
give Spark the most tasks; a compressed file can't be split, so one file is one task.
"""

import argparse
import os
import shutil
import tempfile
from collections import defaultdict
from contextlib import contextmanager
from functools import partial

import databento as db
import databento_dbn as dbn
import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs
from pyspark.sql import functions as F
from pyspark.sql.pandas.types import from_arrow_schema

from lobpipe.fs import for_spark, is_local, join, resolve, write_json

DAY_NS = 86_400_000_000_000
CHUNK = 1_000_000
ROW_BITS = 40  # rows per file < 2^40
FILE_BITS = 23  # files < 2^23, so row_nr fits in a signed 64-bit integer
SUFFIXES = (".dbn", ".dbn.zst")

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
ROWS = SCHEMA.append(pa.field("date", pa.string()))


def day_label(ns):
    return str(np.datetime64(int(ns), "ns").astype("datetime64[D]"))


# ---- pass 1: list and order the files ---------------------------------------------------

def list_inputs(inputs):
    """Every .dbn / .dbn.zst file under the given files or folders, as URIs."""
    files = []
    for uri in inputs:
        fs, path = resolve(uri)
        info = fs.get_file_info(path)
        if info.type == pafs.FileType.Directory:
            found = sorted(i.path for i in fs.get_file_info(pafs.FileSelector(path, recursive=True))
                           if i.is_file and i.path.endswith(SUFFIXES))
        elif info.type == pafs.FileType.File:
            found = [path]
        else:
            raise FileNotFoundError(uri)
        prefix = "" if is_local(uri) else uri.split("://", 1)[0] + "://"
        files += [prefix + p for p in found]
    if len(set(files)) != len(files):
        raise ValueError("the same file is listed more than once")
    return files


def read_header(uri):
    fs, path = resolve(uri)
    decoder = dbn.DBNDecoder()
    with fs.open_input_stream(path) as stream:  # .zst is decompressed on the fly
        records = []
        while not records:
            chunk = stream.read(1 << 16)
            if not chunk:
                raise ValueError(f"{uri}: no DBN header")
            decoder.write(chunk)
            records = decoder.decode()
    m = records[0]
    mappings = {sym: [{"start": str(s["start_date"]), "end": str(s["end_date"]), "instrument_id": int(s["symbol"])}
                      for s in spans] for sym, spans in m.mappings.items()}
    return {"uri": uri, "dataset": m.dataset, "schema": str(m.schema), "start": m.start, "end": m.end,
            "mappings": mappings}


def order_files(headers):
    if len(headers) >= 1 << FILE_BITS:
        raise ValueError(f"at most {(1 << FILE_BITS) - 1} files per dataset")
    for h in headers:
        if h["schema"] != "mbo":
            raise ValueError(f"{h['uri']}: expected mbo schema, got {h['schema']}")
    if len({h["dataset"] for h in headers}) > 1:
        raise ValueError("files come from more than one dataset")
    return sorted(headers, key=lambda h: (h["start"], h["end"] or 0, h["uri"]))


# ---- pass 2: decode, one Spark task per file ---------------------------------------------

@contextmanager
def local_copy(uri):
    """A local path for the file, copying it to a temporary folder first if it is remote."""
    if is_local(uri):
        yield uri
        return
    fs, path = resolve(uri)
    tmp = tempfile.mkdtemp(prefix="lobpipe-")
    try:
        if shutil.disk_usage(tmp).free < fs.get_file_info(path).size:
            raise OSError(f"not enough local disk in {tmp} to copy {uri}")
        local = os.path.join(tmp, os.path.basename(path))
        with fs.open_input_stream(path, compression=None) as src, open(local, "wb") as dst:
            shutil.copyfileobj(src, dst, 16 << 20)
        yield local
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def to_batch(a, file_index, first):
    if a["order_id"].max() >= 2**63 or a["size"].max() >= 2**31 or a["instrument_id"].max() >= 2**31:
        raise ValueError("value out of range for the parquet schema")
    recv = a["ts_recv"].astype(np.int64)
    days, inverse = np.unique(recv // DAY_NS, return_inverse=True)
    cols = {
        "row_nr": (np.int64(file_index) << ROW_BITS) | np.arange(first, first + len(a), dtype=np.int64),
        "ts_recv": recv,
        "ts_event": a["ts_event"].astype(np.int64),
        "instrument_id": a["instrument_id"].astype(np.int32),
        "order_id": a["order_id"].astype(np.int64),
        "price": a["price"].astype(np.int64),
        "size": a["size"].astype(np.int32),
        "flags": a["flags"].astype(np.int16),
        "action": a["action"].astype("U1"),
        "side": a["side"].astype("U1"),
        "sequence": a["sequence"].astype(np.int64),
        "date": np.array([day_label(d * DAY_NS) for d in days])[inverse],
    }
    return pa.RecordBatch.from_pydict(cols, schema=ROWS)


def decode_file(file_index, uri, chunk=CHUNK):
    with local_copy(uri) as path:
        pos, last = 0, None
        for a in db.DBNStore.from_file(path).to_ndarray(count=chunk):
            if len(a) == 0:
                continue
            recv = a["ts_recv"].astype(np.int64)
            if (np.diff(recv) < 0).any() or (last is not None and recv[0] < last):
                raise ValueError(f"{uri}: ts_recv is not in order")
            if pos + len(a) > 1 << ROW_BITS:
                raise ValueError(f"{uri}: more than 2^{ROW_BITS} records")
            last = int(recv[-1])
            yield to_batch(a, file_index, pos)
            pos += len(a)


def decode_partition(batches, files, chunk=CHUNK):
    """mapInArrow body: each input row is the index of one file to decode."""
    for batch in batches:
        for file_index in batch.column("id").to_pylist():
            yield from decode_file(file_index, files[file_index], chunk)


# ---- summary and manifest ---------------------------------------------------------------

def merge_symbology(headers):
    out = defaultdict(list)
    for h in headers:
        for sym, spans in h["mappings"].items():
            for s in spans:
                last = out[sym][-1] if out[sym] else None
                if last and last["instrument_id"] == s["instrument_id"] and last["end"] == s["start"]:
                    last["end"] = s["end"]
                elif s not in out[sym]:
                    out[sym].append(dict(s))
    return dict(out)


def summarize(spark, out, files):
    """Rows per (date, instrument, file); fails if an instrument-day mixes files out of time order."""
    df = spark.read.parquet(for_spark(out))
    parts = (df.groupBy("date", "instrument_id", F.shiftright("row_nr", ROW_BITS).alias("file"))
             .agg(F.count("*").alias("rows"), F.min("ts_recv").alias("first"), F.max("ts_recv").alias("last"))
             .collect())
    by_key = defaultdict(list)
    for p in parts:
        by_key[(p["date"], p["instrument_id"])].append(p)
    for (day, iid), ps in sorted(by_key.items()):
        ps.sort(key=lambda p: p["file"])
        for a, b in zip(ps, ps[1:]):
            if a["last"] >= b["first"]:
                raise ValueError(f"{day} instrument {iid}: {files[a['file']]} and {files[b['file']]} overlap in time")
    dates = defaultdict(lambda: {"rows": 0, "instruments": defaultdict(int)})
    file_rows = defaultdict(int)
    for p in parts:
        dates[p["date"]]["rows"] += p["rows"]
        dates[p["date"]]["instruments"][str(p["instrument_id"])] += p["rows"]
        file_rows[p["file"]] += p["rows"]
    return {day: {"rows": d["rows"], "instruments": dict(sorted(d["instruments"].items()))}
            for day, d in sorted(dates.items())}, file_rows


def ingest(spark, inputs, out, overwrite=False, chunk=CHUNK):
    fs, out_path = resolve(out)
    if fs.get_file_info(out_path).type != pafs.FileType.NotFound and not overwrite:
        raise FileExistsError(f"{out} exists (use --overwrite)")
    files = list_inputs(inputs)
    if not files:
        raise FileNotFoundError(f"no .dbn or .dbn.zst files in {inputs}")
    headers = spark.sparkContext.parallelize(files, min(len(files), 1000)).map(read_header).collect()
    headers = order_files(headers)
    order = [h["uri"] for h in headers]

    n = len(order)
    # rows of a file arrive in time order, so a task only ever writes to a few dates at a
    # time; keeping those files open avoids Spark sorting every row by date before writing
    # (wider tasks, e.g. monthly files, fall back to the sort)
    spark.conf.set("spark.sql.maxConcurrentOutputFileWriters", "4")
    rows = spark.range(0, n, 1, n).mapInArrow(partial(decode_partition, files=order, chunk=chunk), from_arrow_schema(ROWS))
    rows.write.mode("overwrite" if overwrite else "errorifexists").partitionBy("date").parquet(for_spark(out))

    dates, file_rows = summarize(spark, out, order)
    manifest = {
        "files": [{"uri": h["uri"], "start": h["start"], "end": h["end"], "rows": file_rows.get(i, 0)}
                  for i, h in enumerate(headers)],
        "dates": dates,
        "rows": sum(d["rows"] for d in dates.values()),
        "symbology": merge_symbology(headers),
    }
    write_json(join(out, "_manifest.json"), manifest)
    print(f"{manifest['rows']:,} rows from {n} files in {len(dates)} date partitions -> {out}")
    return manifest


def main():
    from lobpipe.spark_job import session

    p = argparse.ArgumentParser(description="Convert Databento MBO files to a date-partitioned Parquet dataset.")
    p.add_argument("--out", required=True, help="output folder or s3:// URI")
    p.add_argument("--overwrite", action="store_true", help="replace an existing output")
    p.add_argument("--master", default="local[4]", help="Spark master, e.g. local[4] or a cluster URL")
    p.add_argument("inputs", nargs="+", help="Databento MBO files or folders of them, local or s3://")
    args = p.parse_args()
    spark = session(name="lobpipe-ingest", master=args.master)
    ingest(spark, args.inputs, args.out, args.overwrite)
    spark.stop()


if __name__ == "__main__":
    main()
