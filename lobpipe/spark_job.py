"""Spark job: MBO Parquet dataset (from lobpipe.ingest) -> LOBSTER rows, partitioned by date.

Each (instrument_id, date) of the input is self-contained because every UTC day starts
with a book snapshot. Rows are hash-partitioned on that key and sorted by row_nr inside
each Spark partition, so one Python worker sees every row of an instrument-day in file
order and can stream them through a Book.

Every instrument-day ends with one status row: "ok", or the reason its book could not
be rebuilt, and the number of input rows it read (checked against the ingest manifest).
A failed day does not stop the job. Data rows have no status; rows of a failed day are
left out by export and the checks, and _status.json lists every instrument-day.
"""

import argparse
import os
import sys
from collections import Counter
from functools import partial

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from lobpipe.book import COLUMNS, Book, BookError, book_columns
from lobpipe.fs import for_spark, join, read_json, write_json
from lobpipe.transform import et_midnight_ns, feed_rows, output_frame

INPUT_COLUMNS = ["instrument_id", "date", "row_nr", "ts_event", "action", "side", "order_id", "price", "size", "flags"]
OK = "ok"
# S3 support: Hadoop's s3a connector (same Hadoop version as PySpark 4.2 ships) and Spark's
# bridge to the S3A committers
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.5.0"
SPARK_CLOUD = "https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.13/4.2.0/spark-hadoop-cloud_2.13-4.2.0.jar"


def output_schema(levels):
    fields = [StructField("instrument_id", IntegerType(), False), StructField("date", StringType(), False)]
    fields += [StructField(c, LongType(), False) for c in COLUMNS + book_columns(levels)]
    fields += [StructField("status", StringType(), True), StructField("input_rows", LongType(), True)]
    return StructType(fields)


def labelled(key, frame, status=None, input_rows=None):
    frame.insert(0, "date", key[1])
    frame.insert(0, "instrument_id", np.int32(key[0]))
    frame["status"] = status
    frame["input_rows"] = input_rows
    return frame


def status_row(key, book, error, input_rows, levels):
    if error is None:
        try:
            book.finish()
        except BookError as e:
            error = str(e)
    cols = COLUMNS + book_columns(levels)
    return labelled(key, pd.DataFrame(np.zeros((1, len(cols)), dtype=np.int64), columns=cols), error or OK, input_rows)


def rebuild_partition(batches, levels=10, verify=False):
    """mapInPandas body. Batches arrive sorted by (instrument_id, date, row_nr)."""
    key = book = error = midnight = None
    seen = 0
    for pdf in batches:
        keys = list(zip(pdf["instrument_id"].tolist(), pdf["date"].tolist()))
        cuts = [0] + [i for i in range(1, len(keys)) if keys[i] != keys[i - 1]] + [len(keys)]
        for lo, hi in zip(cuts[:-1], cuts[1:]):
            if keys[lo] != key:
                if key is not None:
                    yield status_row(key, book, error, seen, levels)
                key, error, seen = keys[lo], None, 0
                book = Book(levels=levels, verify=verify)
                midnight = et_midnight_ns(key[1])
            seen += hi - lo
            if error is not None:
                continue
            try:
                rows = book.feed(feed_rows(pdf.iloc[lo:hi], midnight))
            except BookError as e:
                error = str(e)
                continue
            if rows:
                yield labelled(key, output_frame(rows, levels))
    if key is not None:
        yield status_row(key, book, error, seen, levels)


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
    try:
        manifest = read_json(join(src, "_manifest.json"))
    except FileNotFoundError:
        raise FileNotFoundError(f"{src} has no _manifest.json; run lobpipe.ingest to the end first") from None
    return {(int(iid), day): n for day, d in manifest["dates"].items() for iid, n in d["instruments"].items()
            if not dates or day in dates}


def run(spark, src, out, levels=10, dates=None, partitions=None, verify=False, overwrite=False):
    """Rebuild every instrument-day, write the rows and _status.json; returns the statuses."""
    days = expected_days(src, dates)
    if not days:
        raise ValueError(f"no instrument-days to rebuild in {src}")
    # about 8 tasks per core keeps the cores busy while the biggest days finish
    partitions = partitions or min(len(days), 8 * spark.sparkContext.defaultParallelism)
    rows = build(spark, src, levels, dates, partitions, verify)
    rows.write.mode("overwrite" if overwrite else "errorifexists").partitionBy("date").parquet(for_spark(out))

    found = (spark.read.parquet(for_spark(out)).where(F.col("status").isNotNull())
             .select("instrument_id", "date", "status", "input_rows").collect())
    counts = Counter((int(r["instrument_id"]), str(r["date"])) for r in found)
    read = {(int(r["instrument_id"]), str(r["date"])): r["input_rows"] for r in found}
    if set(counts) != set(days) or max(counts.values()) != 1 or read != days:
        raise RuntimeError("the status rows do not match the instrument-days and row counts of the input")
    statuses = sorted((str(r["date"]), int(r["instrument_id"]), r["status"]) for r in found)
    write_json(join(out, "_status.json"),
               [{"date": day, "instrument_id": iid, "status": status} for day, iid, status in statuses])
    return statuses


def read_statuses(lob):
    """{(instrument_id, date): status} from the _status.json of a finished run."""
    try:
        entries = read_json(join(lob, "_status.json"))
    except FileNotFoundError:
        raise FileNotFoundError(f"{lob} has no _status.json; the Spark job did not finish") from None
    statuses = {}
    for s in entries:
        key = (s["instrument_id"], s["date"])
        if key in statuses:
            raise ValueError(f"{lob}/_status.json lists instrument {key[0]} on {key[1]} more than once")
        statuses[key] = s["status"]
    return statuses


def ok_days(lob):
    """{(instrument_id, date)} of the instrument-days whose rebuild succeeded."""
    return {key for key, status in read_statuses(lob).items() if status == OK}


def has_jar(prefix):
    import pyspark

    return any(f.startswith(prefix) for f in os.listdir(os.path.join(os.path.dirname(pyspark.__file__), "jars")))


def session(name="lobpipe", master="local[4]", batch=50_000, driver_memory="4g", s3=False):
    """A Spark session. Under spark-submit, its master, memory, Python and jar settings are kept.

    S3 goes through Hadoop's s3a connector; credentials come from the usual AWS environment
    variables or instance role, and AWS_ENDPOINT_URL points it at an S3-compatible store.
    """
    builder = SparkSession.builder.appName(name)
    if "PYSPARK_GATEWAY_PORT" not in os.environ:  # started with plain python, not spark-submit
        # started from a running interpreter, PySpark ignores spark.pyspark.python, so point
        # the workers at this interpreter (and its packages) explicitly
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        builder = builder.master(master).config("spark.driver.memory", driver_memory)
        if s3 and not has_jar("hadoop-aws-3.5.0"):
            builder = builder.config("spark.jars.packages", HADOOP_AWS)
        if s3 and not has_jar("spark-hadoop-cloud_2.13-4.2.0"):
            builder = builder.config("spark.jars", SPARK_CLOUD)
    if s3:
        # S3 has no rename, so Spark's usual commit (write to a temporary folder, then rename)
        # would copy every file and could leave a failed attempt's files behind; the S3A
        # "magic" committer uploads files directly and makes them visible only on commit
        builder = (builder.config("spark.hadoop.fs.s3a.committer.name", "magic")
                   .config("spark.sql.sources.commitProtocolClass",
                           "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
                   .config("spark.sql.parquet.output.committer.class",
                           "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"))
    if os.environ.get("AWS_ENDPOINT_URL"):
        builder = (builder.config("spark.hadoop.fs.s3a.endpoint", os.environ["AWS_ENDPOINT_URL"])
                   .config("spark.hadoop.fs.s3a.path.style.access", "true"))
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", region)
    return (builder
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch))
            .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
            .getOrCreate())


def main():
    p = argparse.ArgumentParser(description="Rebuild LOBSTER rows from an MBO Parquet dataset with Spark.")
    p.add_argument("--src", required=True, help="MBO dataset written by lobpipe.ingest (folder or s3://)")
    p.add_argument("--out", required=True, help="output dataset (folder or s3://)")
    p.add_argument("--levels", type=int, default=10)
    p.add_argument("--dates", nargs="*", help="only these UTC dates (YYYY-MM-DD)")
    p.add_argument("--partitions", type=int, help="default: one per instrument-day, at most 8 per core")
    p.add_argument("--verify", action="store_true", help="check the book against A/C/M/R depth after every event")
    p.add_argument("--overwrite", action="store_true", help="replace an existing output")
    p.add_argument("--allow-failed-days", action="store_true", help="exit 0 even if some instrument-days failed")
    p.add_argument("--master", default="local[4]", help="Spark master when run with python (spark-submit sets its own)")
    p.add_argument("--batch", type=int, default=50_000, help="rows per Arrow batch sent to Python")
    p.add_argument("--driver-memory", default="4g")
    args = p.parse_args()
    s3 = any(p.startswith("s3://") for p in (args.src, args.out))
    spark = session(master=args.master, batch=args.batch, driver_memory=args.driver_memory, s3=s3)
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
