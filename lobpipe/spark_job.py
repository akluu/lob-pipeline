"""Spark job: MBO Parquet (from lobpipe.ingest) -> LOBSTER rows, one folder per day.

Every instrument-day starts with a book snapshot, so it can be rebuilt on its own. Rows are
hash-partitioned on (instrument_id, date) and sorted by row_nr inside each partition, so
one Python worker sees all rows of an instrument-day in order and streams them through a
Book.

Each instrument-day ends with a status row: "ok" or the reason its book could not be
rebuilt, and the number of input rows it read. A failed day doesn't stop the job; it is
listed in _status.json and left out by export.
"""

import argparse
import sys
from functools import partial

import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from lobpipe.book import COLUMNS, Book, BookError, book_columns
from lobpipe.fs import exists, for_spark, join, read_json, write_json
from lobpipe.spark import session
from lobpipe.transform import et_midnight_ns, feed_rows, output_frame

INPUT_COLUMNS = ["instrument_id", "date", "row_nr", "ts_event", "action", "side", "order_id", "price", "size", "flags"]
OK = "ok"


def output_schema(levels):
    fields = [StructField("instrument_id", IntegerType(), False), StructField("date", StringType(), False)]
    fields += [StructField(c, LongType(), False) for c in COLUMNS + book_columns(levels)]
    fields += [StructField("status", StringType(), True), StructField("input_rows", LongType(), True)]
    return StructType(fields)


class Day:
    """One instrument-day going through its Book. The first error is kept, not raised."""

    def __init__(self, key, levels, verify):
        self.key, self.levels = key, levels
        self.book = Book(levels=levels, verify=verify)
        self.midnight = et_midnight_ns(key[1])
        self.rows_read = 0
        self.error = None

    def _run(self, step):
        if self.error is None:
            try:  # a day the book can't explain is reported, the rest of the run goes on
                return step()
            except BookError as e:
                self.error = str(e)
        return []

    def feed(self, rows):
        self.rows_read += len(rows)
        out = self._run(lambda: self.book.feed(feed_rows(rows, self.midnight)))
        return self._frame(output_frame(out, self.levels)) if out else None

    def status_row(self):
        self._run(self.book.finish)
        zeros = output_frame([[0] * (len(COLUMNS) + 4 * self.levels)], self.levels)
        return self._frame(zeros, self.error or OK, self.rows_read)

    def _frame(self, frame, status=None, rows_read=None):
        frame.insert(0, "date", self.key[1])
        frame.insert(0, "instrument_id", np.int32(self.key[0]))
        frame["status"] = status
        frame["input_rows"] = rows_read
        return frame


def rebuild_partition(batches, levels=10, verify=False):
    """mapInPandas body. Rows arrive sorted by (instrument_id, date, row_nr)."""
    day = None
    for pdf in batches:
        keys = list(zip(pdf["instrument_id"].tolist(), pdf["date"].tolist()))
        starts = [0] + [i for i in range(1, len(keys)) if keys[i] != keys[i - 1]]
        for lo, hi in zip(starts, starts[1:] + [len(keys)]):
            if day is None or day.key != keys[lo]:
                if day is not None:
                    yield day.status_row()
                day = Day(keys[lo], levels, verify)
            out = day.feed(pdf.iloc[lo:hi])
            if out is not None:
                yield out
    if day is not None:
        yield day.status_row()


def build(spark, src, levels=10, dates=None, partitions=64, verify=False):
    df = spark.read.parquet(for_spark(src)).withColumn("date", F.col("date").cast("string"))
    if dates:
        df = df.where(F.col("date").isin(list(dates)))
    df = (df.select(*INPUT_COLUMNS)
          .repartition(partitions, "instrument_id", "date")
          .sortWithinPartitions("instrument_id", "date", "row_nr"))
    return df.mapInPandas(partial(rebuild_partition, levels=levels, verify=verify), output_schema(levels))


def expected_days(src, dates=None):
    """{(instrument_id, date): input rows} from the ingest manifest."""
    if not exists(join(src, "_manifest.json")):
        raise FileNotFoundError(f"{src} has no _manifest.json; run lobpipe.ingest to the end first")
    manifest = read_json(join(src, "_manifest.json"))
    return {(int(iid), day): n for day, d in manifest["dates"].items() for iid, n in d["instruments"].items()
            if not dates or day in dates}


def run(spark, src, out, levels=10, dates=None, partitions=None, verify=False, overwrite=False):
    """Rebuild every instrument-day, write the rows and _status.json; returns the statuses."""
    days = expected_days(src, dates)
    if not days:
        raise ValueError(f"no instrument-days to rebuild in {src}")
    # about 8 tasks per core keeps every core busy while the biggest days finish
    partitions = partitions or min(len(days), 8 * spark.sparkContext.defaultParallelism)
    rows = build(spark, src, levels, dates, partitions, verify)
    rows.write.mode("overwrite" if overwrite else "errorifexists").partitionBy("date").parquet(for_spark(out))

    found = (spark.read.parquet(for_spark(out)).where(F.col("status").isNotNull())
             .select("instrument_id", "date", "status", "input_rows").collect())
    read = {(r.instrument_id, r.date): r.input_rows for r in found}
    if len(read) != len(found) or read != days:
        raise RuntimeError("the status rows do not match the instrument-days and row counts of the input")
    statuses = sorted((r.date, r.instrument_id, r.status) for r in found)
    write_json(join(out, "_status.json"), [{"date": d, "instrument_id": i, "status": s} for d, i, s in statuses])
    return statuses


def read_statuses(lob):
    """{(instrument_id, date): status} from the _status.json of a finished run."""
    if not exists(join(lob, "_status.json")):
        raise FileNotFoundError(f"{lob} has no _status.json; the Spark job did not finish")
    statuses = {}
    for s in read_json(join(lob, "_status.json")):
        key = (s["instrument_id"], s["date"])
        if key in statuses:
            raise ValueError(f"{lob}/_status.json lists instrument {key[0]} on {key[1]} more than once")
        statuses[key] = s["status"]
    return statuses


def ok_days(lob):
    """{(instrument_id, date)} of the instrument-days whose rebuild succeeded."""
    return {key for key, status in read_statuses(lob).items() if status == OK}


def main():
    p = argparse.ArgumentParser(description="Rebuild LOBSTER rows from an MBO Parquet dataset with Spark.")
    p.add_argument("--src", required=True, help="MBO dataset written by lobpipe.ingest (folder or s3://)")
    p.add_argument("--out", required=True, help="output dataset (folder or s3://)")
    p.add_argument("--levels", type=int, default=10)
    p.add_argument("--dates", nargs="*", help="only these UTC dates (YYYY-MM-DD)")
    p.add_argument("--partitions", type=int, help="default: one per instrument-day, at most 8 per core")
    p.add_argument("--verify", action="store_true", help="also check the book against add/cancel/modify depth")
    p.add_argument("--overwrite", action="store_true", help="replace an existing output")
    p.add_argument("--allow-failed-days", action="store_true", help="exit 0 even if some instrument-days failed")
    p.add_argument("--master", default="local[4]", help="Spark master when run with python")
    p.add_argument("--driver-memory", default="4g")
    args = p.parse_args()
    s3 = args.src.startswith("s3://") or args.out.startswith("s3://")
    spark = session(master=args.master, driver_memory=args.driver_memory, s3=s3)
    statuses = run(spark, args.src, args.out, args.levels, args.dates, args.partitions, args.verify, args.overwrite)
    spark.stop()

    failed = [s for s in statuses if s[2] != OK]
    print(f"{len(statuses) - len(failed)} of {len(statuses)} instrument-days rebuilt")
    for day, iid, status in failed[:20]:
        print(f"FAILED {day} instrument {iid}: {status}")
    if len(failed) > 20:
        print(f"... and {len(failed) - 20} more, see {join(args.out, '_status.json')}")
    sys.exit(1 if failed and not args.allow_failed_days else 0)


if __name__ == "__main__":
    main()
