"""Check that the Spark output equals a plain single-process rebuild, day by day.

For each date the MBO rows are replayed through one Book per instrument in a normal
Python process. The Spark output for the same date is read back and sorted by
(instrument_id, row_nr, sub). The two must have the same keys in the same order and the
same values in every row (compared through a hash of each row).

usage: python scripts/compare_spark.py --mbo data/mbo --lob data/lob [--dates D ...] [--workers 1]
"""

import argparse
import os
import sys
from multiprocessing import Pool

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lobpipe.book import COLUMNS, Book, book_columns  # noqa: E402
from lobpipe.transform import rebuild_chunks  # noqa: E402

VALUES = COLUMNS + book_columns(10)
INPUT = ["row_nr", "ts_event", "instrument_id", "action", "side", "order_id", "price", "size", "flags"]


def row_hashes(df):
    for column, dtype in df[VALUES].dtypes.items():
        if not pd.api.types.is_integer_dtype(dtype):
            raise ValueError(f"column {column} is {dtype}, expected integers")
    return pd.util.hash_pandas_object(df[VALUES].astype("int64"), index=False).to_numpy()


def key_array(iid, df):
    return np.column_stack([np.full(len(df), iid, dtype=np.int64), df["row_nr"].to_numpy(np.int64),
                            df["sub"].to_numpy(np.int64)])


def stack(keys, hashes):
    if not keys:
        return np.zeros((0, 3), dtype=np.int64), np.zeros(0, dtype=np.uint64)
    return np.concatenate(keys), np.concatenate(hashes)


def direct_rows(mbo, day):
    rows = pq.read_table(os.path.join(mbo, f"date={day}"), columns=INPUT).to_pandas()
    rows = rows.sort_values(["instrument_id", "row_nr"], kind="stable", ignore_index=True)
    keys, hashes = [], []
    for iid, group in rows.groupby("instrument_id", sort=True):
        book = Book(levels=10)
        for frame in rebuild_chunks(book, group, day):
            keys.append(key_array(iid, frame))
            hashes.append(row_hashes(frame))
        book.finish()
    return stack(keys, hashes)


def spark_rows(lob, day):
    folder = os.path.join(lob, f"date={day}")
    keys, hashes = [], []
    if os.path.isdir(folder):
        for name in sorted(f for f in os.listdir(folder) if f.endswith(".parquet")):
            for batch in pq.ParquetFile(os.path.join(folder, name)).iter_batches(batch_size=500_000):
                frame = batch.to_pandas()
                keys.append(np.column_stack([frame["instrument_id"].to_numpy(np.int64), frame["row_nr"].to_numpy(),
                                             frame["sub"].to_numpy()]))
                hashes.append(row_hashes(frame))
    keys, hashes = stack(keys, hashes)
    # Spark writes files in no particular order
    order = np.lexsort((keys[:, 2], keys[:, 1], keys[:, 0]))
    return keys[order], hashes[order]


def compare_day(args):
    mbo, lob, day = args
    direct_keys, direct = direct_rows(mbo, day)
    spark_keys, spark = spark_rows(lob, day)
    duplicates = bool((np.diff(spark_keys, axis=0) == 0).all(axis=1).any())
    same = (not duplicates and len(direct) == len(spark) and bool((direct_keys == spark_keys).all())
            and bool((direct == spark).all()))
    return day, len(direct), len(spark), duplicates, same


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--mbo", required=True, help="MBO parquet written by lobpipe.ingest")
    p.add_argument("--lob", required=True, help="output of lobpipe.spark_job")
    p.add_argument("--dates", nargs="*", help="default: every date in --mbo")
    p.add_argument("--workers", type=int, default=1, help="days compared at once; each holds a whole day in memory")
    args = p.parse_args()
    if not os.path.exists(os.path.join(args.lob, "_SUCCESS")):
        sys.exit(f"{args.lob} has no _SUCCESS marker; the Spark job did not finish")

    def dates_in(root):
        return sorted(d.split("=", 1)[1] for d in os.listdir(root) if d.startswith("date="))

    dates = args.dates or dates_in(args.mbo)
    extra = sorted(set(dates_in(args.lob)) - set(dates_in(args.mbo)))
    jobs = [(args.mbo, args.lob, d) for d in dates]
    if args.workers > 1:
        with Pool(args.workers) as pool:
            results = pool.map(compare_day, jobs, chunksize=1)
    else:
        results = [compare_day(job) for job in jobs]

    bad = 0
    for day, n_direct, n_spark, duplicates, same in results:
        note = "identical" if same else "DUPLICATE KEYS" if duplicates else "DIFFERENT"
        print(f"{day}: direct {n_direct:>11,} rows, spark {n_spark:>11,} rows  {note}")
        bad += not same
    for day in extra:
        print(f"{day}: in the Spark output but not in the MBO input")
    print(f"{len(results) - bad} of {len(results)} dates identical")
    sys.exit(1 if bad or extra or not results else 0)


if __name__ == "__main__":
    main()
