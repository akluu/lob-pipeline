"""Spark job: MBO Parquet dataset -> LOBSTER rows (Parquet, partitioned by date).

Each (instrument_id, date) partition of the input is self-contained because every UTC
day starts with a book snapshot. Rows are hash-partitioned on that key and sorted by
row_nr inside each Spark partition, so one Python worker sees every row of an
instrument-day in file order and can stream them through a Book.
"""

import argparse
import sys
from functools import partial

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from lobpipe.book import COLUMNS, Book, book_columns
from lobpipe.transform import et_midnight_ns, feed_rows, output_frame

INPUT_COLUMNS = ["instrument_id", "date", "row_nr", "ts_event", "action", "side", "order_id", "price", "size", "flags"]


def output_schema(levels):
    fields = [StructField("instrument_id", IntegerType(), False), StructField("date", StringType(), False)]
    fields += [StructField(c, LongType(), False) for c in COLUMNS + book_columns(levels)]
    return StructType(fields)


def rebuild_partition(batches, levels=10, verify=False):
    """mapInPandas body. Batches arrive sorted by (instrument_id, date, row_nr)."""
    book, key, midnight = None, None, None
    for pdf in batches:
        keys = list(zip(pdf["instrument_id"].tolist(), pdf["date"].tolist()))
        cuts = [0] + [i for i in range(1, len(keys)) if keys[i] != keys[i - 1]] + [len(keys)]
        for lo, hi in zip(cuts[:-1], cuts[1:]):
            if keys[lo] != key:
                if book is not None:
                    book.finish()
                key = keys[lo]
                book = Book(levels=levels, verify=verify)
                midnight = et_midnight_ns(key[1])
            rows = book.feed(feed_rows(pdf.iloc[lo:hi], midnight))
            if rows:
                out = output_frame(rows, levels)
                out.insert(0, "date", key[1])
                out.insert(0, "instrument_id", np.int32(key[0]))
                yield out
    if book is not None:
        book.finish()


def build(spark, src, levels=10, dates=None, partitions=64, verify=False):
    df = spark.read.parquet(src).withColumn("date", F.col("date").cast("string"))
    if dates:
        df = df.where(F.col("date").isin(list(dates)))
    df = (df.select(*INPUT_COLUMNS)
          .repartition(partitions, "instrument_id", "date")
          .sortWithinPartitions("instrument_id", "date", "row_nr"))
    return df.mapInPandas(partial(rebuild_partition, levels=levels, verify=verify), output_schema(levels))


def session(name="lobpipe", master="local[4]", batch=50_000, driver_memory="4g"):
    return (SparkSession.builder.master(master).appName(name)
            .config("spark.pyspark.python", sys.executable)
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch))
            .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
            .config("spark.driver.memory", driver_memory)
            .getOrCreate())


def main():
    p = argparse.ArgumentParser(description="Rebuild LOBSTER rows from an MBO Parquet dataset with Spark.")
    p.add_argument("--src", required=True, help="MBO parquet dataset written by lobpipe.ingest")
    p.add_argument("--out", required=True, help="output parquet dataset")
    p.add_argument("--levels", type=int, default=10)
    p.add_argument("--dates", nargs="*", help="only these UTC dates (YYYY-MM-DD)")
    p.add_argument("--partitions", type=int, default=64)
    p.add_argument("--verify", action="store_true", help="check the book against A/C/M/R depth after every event")
    p.add_argument("--master", default="local[4]", help="Spark master, e.g. local[4] or a cluster URL")
    p.add_argument("--batch", type=int, default=50_000, help="rows per Arrow batch sent to Python")
    p.add_argument("--driver-memory", default="4g")
    args = p.parse_args()
    spark = session(master=args.master, batch=args.batch, driver_memory=args.driver_memory)
    out = build(spark, args.src, args.levels, args.dates, args.partitions, args.verify)
    out.write.mode("overwrite").partitionBy("date").parquet(args.out)
    spark.stop()


if __name__ == "__main__":
    main()
