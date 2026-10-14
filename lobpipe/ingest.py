"""Databento MBO files (.dbn or .dbn.zst, local or s3://) -> Parquet, one folder per UTC day.

1. Read every file's header in parallel and put the files in order of their start time.
2. Decode each file in its own Spark task. row_nr = (file number << 40) + position in the
   file, so sorting by row_nr gives the original order across all files.
3. Check that no instrument-day takes rows from files that overlap in time, then write
   _manifest.json with the rows of each day and instrument. The rebuild only reads a
   dataset that has one.
"""

import argparse
import tempfile
from collections import defaultdict
from functools import partial
from os.path import basename, join as path_join

import databento as db
import databento_dbn as dbn
import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs
from pyspark.sql import functions as F
from pyspark.sql.pandas.types import from_arrow_schema

from lobpipe.fs import exists, for_spark, join, resolve, write_json
from lobpipe.spark import session

DAY_NS = 86_400 * 10**9
CHUNK = 1_000_000
ROW_BITS = 40  # room for 2^40 records per file and 2^23 files in a signed 64-bit row_nr

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


def list_files(inputs):
    """Every .dbn / .dbn.zst file in the given files and folders."""
    files = []
    for uri in inputs:
        fs, path = resolve(uri)
        if fs.get_file_info(path).type == pafs.FileType.Directory:
            found = fs.get_file_info(pafs.FileSelector(path, recursive=True))
            paths = sorted(f.path for f in found if f.is_file and f.path.endswith((".dbn", ".dbn.zst")))
        else:
            paths = [path]
        scheme = uri.split("://")[0] + "://" if "://" in uri else ""
        files += [scheme + p for p in paths]
    return files


def read_header(uri):
    """(start time, dataset, schema, uri) from the first few KB of a DBN file."""
    fs, path = resolve(uri)
    decoder, records = dbn.DBNDecoder(), []
    with fs.open_input_stream(path) as f:  # pyarrow decompresses .zst by its extension
        while not records:
            chunk = f.read(1 << 16)
            if not chunk:
                raise ValueError(f"{uri}: no DBN header")
            decoder.write(chunk)
            records = decoder.decode()
    meta = records[0]
    return meta.start, meta.dataset, str(meta.schema), uri


def to_batch(a, first_row):
    """A numpy array of DBN records as an Arrow batch; Arrow rejects values that don't fit."""
    columns = {name: a[name] for name in SCHEMA.names if name in a.dtype.names}
    columns["row_nr"] = np.arange(first_row, first_row + len(a))
    columns["action"] = a["action"].astype("U1")
    columns["side"] = a["side"].astype("U1")
    days, day_of_row = np.unique(a["ts_recv"].astype(np.int64) // DAY_NS, return_inverse=True)
    columns["date"] = days.astype("datetime64[D]").astype(str)[day_of_row]
    return pa.RecordBatch.from_pydict(columns, schema=ROWS)


def decode_file(number, uri, chunk=CHUNK):
    with tempfile.TemporaryDirectory() as tmp:
        path = uri
        if "://" in uri:  # copy a remote file to local disk first
            fs, remote = resolve(uri)
            path = path_join(tmp, basename(remote))
            pafs.copy_files(remote, path, source_filesystem=fs, destination_filesystem=pafs.LocalFileSystem())
        position, last = 0, 0
        for a in db.DBNStore.from_file(path).to_ndarray(count=chunk):
            if len(a) == 0:
                continue
            if a["ts_recv"][0] < last or (np.diff(a["ts_recv"].astype(np.int64)) < 0).any():
                raise ValueError(f"{uri}: records are not in time order")
            yield to_batch(a, (number << ROW_BITS) + position)
            position += len(a)
            last = a["ts_recv"][-1]


def decode_partition(batches, files, chunk=CHUNK):
    """mapInArrow body: each input row is the number of a file to decode."""
    for batch in batches:
        for number in batch.column("id").to_pylist():
            yield from decode_file(number, files[number], chunk)


def summarize(spark, out, files):
    """Rows per day and instrument, and per file. Fails if an instrument-day takes rows
    from two files that overlap in time, since their order would then be ambiguous."""
    parts = (spark.read.parquet(for_spark(out))
             .groupBy("date", "instrument_id", F.shiftright("row_nr", ROW_BITS).alias("file"))
             .agg(F.count("*").alias("rows"), F.min("ts_recv").alias("first"), F.max("ts_recv").alias("last"))
             .orderBy("date", "instrument_id", "file")
             .collect())
    for a, b in zip(parts, parts[1:]):
        if (a.date, a.instrument_id) == (b.date, b.instrument_id) and a.last >= b.first:
            raise ValueError(f"{a.date} instrument {a.instrument_id}: {files[a.file]} and {files[b.file]} overlap")
    dates, file_rows = defaultdict(lambda: defaultdict(int)), defaultdict(int)
    for p in parts:
        dates[p.date][str(p.instrument_id)] += p.rows
        file_rows[p.file] += p.rows
    return {day: {"rows": sum(n.values()), "instruments": dict(n)} for day, n in dates.items()}, file_rows


def ingest(spark, inputs, out, overwrite=False, chunk=CHUNK):
    if exists(out) and not overwrite:
        raise FileExistsError(f"{out} exists (use --overwrite)")
    files = list_files(inputs)
    if not files or len(set(files)) != len(files):
        raise ValueError(f"no DBN files, or a file listed twice, in {inputs}")
    headers = sorted(spark.sparkContext.parallelize(files).map(read_header).collect())
    if any(schema != "mbo" for _, _, schema, _ in headers) or len({h[1] for h in headers}) > 1:
        raise ValueError("every file must be MBO data from one dataset")
    files = [h[3] for h in headers]

    # a file's rows arrive in time order, so each task writes to one date at a time;
    # keeping a few files open saves Spark from sorting every row by date before writing
    spark.conf.set("spark.sql.maxConcurrentOutputFileWriters", "4")
    (spark.range(len(files), numPartitions=len(files))
     .mapInArrow(partial(decode_partition, files=files, chunk=chunk), from_arrow_schema(ROWS))
     .write.mode("overwrite" if overwrite else "errorifexists").partitionBy("date").parquet(for_spark(out)))

    dates, file_rows = summarize(spark, out, files)
    manifest = {"files": [{"uri": f, "rows": file_rows[i]} for i, f in enumerate(files)],
                "dates": dates, "rows": sum(d["rows"] for d in dates.values())}
    write_json(join(out, "_manifest.json"), manifest)
    print(f"{manifest['rows']:,} rows from {len(files)} files in {len(dates)} date partitions -> {out}")
    return manifest


def main():
    p = argparse.ArgumentParser(description="Convert Databento MBO files to a date-partitioned Parquet dataset.")
    p.add_argument("--out", required=True, help="output folder or s3:// URI")
    p.add_argument("--overwrite", action="store_true", help="replace an existing output")
    p.add_argument("--master", default="local[4]", help="Spark master when run with python")
    p.add_argument("inputs", nargs="+", help="Databento MBO files or folders of them, local or s3://")
    args = p.parse_args()
    s3 = any(p.startswith("s3://") for p in [args.out, *args.inputs])
    spark = session(name="lobpipe-ingest", master=args.master, s3=s3)
    ingest(spark, args.inputs, args.out, args.overwrite)
    spark.stop()


if __name__ == "__main__":
    main()
