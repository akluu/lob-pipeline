"""Check that the Spark output equals a plain single-process rebuild, day by day.

For each date the MBO rows are replayed through one Book per instrument in a normal
Python process. Every instrument-day must end the same way in both (rebuilt, or failed
with the same error). For the ones rebuilt, the Spark rows read back and sorted by
(instrument_id, row_nr, sub) must have the same keys in the same order and the same
values in every row (compared through a hash of each row). On a large dataset, run it
on a sample with --dates.

usage: python scripts/compare_spark.py --mbo data/mbo --lob data/lob [--dates D ...] [--workers 1]
"""

import argparse
import os
import sys
from collections import defaultdict
from multiprocessing import Pool

import numpy as np
import pandas as pd
import pyarrow.fs as pafs
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lobpipe.book import COLUMNS, book_columns  # noqa: E402
from lobpipe.fs import exists, join, list_dates, resolve  # noqa: E402
from lobpipe.spark_job import read_statuses  # noqa: E402
from lobpipe.transform import OK, Day  # noqa: E402

VALUES = COLUMNS + book_columns(10)
INPUT = ["row_nr", "ts_event", "instrument_id", "action", "side", "order_id", "price", "size", "flags"]


def row_hashes(df):
    for column, dtype in df[VALUES].dtypes.items():
        if not pd.api.types.is_integer_dtype(dtype):
            raise ValueError(f"column {column} is {dtype}, expected integers")
    return pd.util.hash_pandas_object(df[VALUES].astype("int64"), index=False).to_numpy()


def key_array(iids, df):
    return np.column_stack([np.broadcast_to(np.asarray(iids, dtype=np.int64), len(df)),
                            df["row_nr"].to_numpy(np.int64), df["sub"].to_numpy(np.int64)])


def stack(keys, hashes):
    if not keys:
        return np.zeros((0, 3), dtype=np.int64), np.zeros(0, dtype=np.uint64)
    return np.concatenate(keys), np.concatenate(hashes)


def direct_rows(mbo, day):
    """Keys and row hashes of the instrument-days rebuilt, and {instrument_id: status}."""
    fs, path = resolve(join(mbo, f"date={day}"))
    rows = pq.read_table(path, filesystem=fs, columns=INPUT).to_pandas()
    rows = rows.sort_values(["instrument_id", "row_nr"], kind="stable", ignore_index=True)
    keys, hashes, statuses = [], [], {}
    for iid, group in rows.groupby("instrument_id", sort=True):
        replayed, day_keys, day_hashes = Day(int(iid), day), [], []
        for frame in replayed.feed(group):
            day_keys.append(key_array(iid, frame))
            day_hashes.append(row_hashes(frame))
        statuses[int(iid)] = replayed.finish()
        if statuses[int(iid)] == OK:
            keys += day_keys
            hashes += day_hashes
    return *stack(keys, hashes), statuses


def spark_rows(lob, day, instruments, known):
    """Keys and row hashes of the Spark data rows of `instruments`, sorted by key, and the
    number of rows whose instrument has no status at all on that day."""
    fs, path = resolve(join(lob, f"date={day}"))
    keys, hashes, unknown = [], [], 0
    if fs.get_file_info(path).type == pafs.FileType.Directory:
        files = sorted(i.path for i in fs.get_file_info(pafs.FileSelector(path)) if i.path.endswith(".parquet"))
        for name in files:
            for batch in pq.ParquetFile(fs.open_input_file(name)).iter_batches(batch_size=500_000):
                frame = batch.to_pandas()
                unknown += int((~frame["instrument_id"].isin(known)).sum())
                frame = frame[frame["status"].isna() & frame["instrument_id"].isin(instruments)]
                keys.append(key_array(frame["instrument_id"].to_numpy(), frame))
                hashes.append(row_hashes(frame))
    keys, hashes = stack(keys, hashes)
    # Spark writes files in no particular order
    order = np.lexsort((keys[:, 2], keys[:, 1], keys[:, 0]))
    return keys[order], hashes[order], unknown


def compare_day(args):
    mbo, lob, day, spark_status = args
    direct_keys, direct, direct_status = direct_rows(mbo, day)
    rebuilt = [iid for iid, status in direct_status.items() if status == OK]
    spark_keys, spark, unknown = spark_rows(lob, day, rebuilt, list(spark_status))
    duplicates = bool((np.diff(spark_keys, axis=0) == 0).all(axis=1).any())
    same = (direct_status == spark_status and not unknown and not duplicates and len(direct) == len(spark)
            and bool((direct_keys == spark_keys).all()) and bool((direct == spark).all()))
    return day, len(direct), len(spark), len(direct_status) - len(rebuilt), duplicates, same


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--mbo", required=True, help="MBO dataset written by lobpipe.ingest")
    p.add_argument("--lob", required=True, help="output of lobpipe.spark_job")
    p.add_argument("--dates", nargs="*", help="default: every date in --mbo")
    p.add_argument("--workers", type=int, default=1, help="days compared at once; each holds a whole day in memory")
    args = p.parse_args()
    if not exists(join(args.lob, "_SUCCESS")) or not exists(join(args.lob, "_status.json")):
        sys.exit(f"{args.lob} has no _SUCCESS or _status.json; the Spark job did not finish")
    spark_status = defaultdict(dict)
    for (iid, day), status in read_statuses(args.lob).items():
        spark_status[day][iid] = status

    dates = args.dates or list_dates(args.mbo)
    extra = sorted(set(list_dates(args.lob)) - set(list_dates(args.mbo)))
    jobs = [(args.mbo, args.lob, d, spark_status[d]) for d in dates]
    if args.workers > 1:
        with Pool(args.workers) as pool:
            results = pool.map(compare_day, jobs, chunksize=1)
    else:
        results = [compare_day(job) for job in jobs]

    bad = 0
    for day, n_direct, n_spark, failed, duplicates, same in results:
        note = "identical" if same else "DUPLICATE KEYS" if duplicates else "DIFFERENT"
        failed = f", {failed} failed in both" if failed and same else ""
        print(f"{day}: direct {n_direct:>11,} rows, spark {n_spark:>11,} rows  {note}{failed}")
        bad += not same
    for day in extra:
        print(f"{day}: in the Spark output but not in the MBO input")
    print(f"{len(results) - bad} of {len(results)} dates identical")
    sys.exit(1 if bad or extra or not results else 0)


if __name__ == "__main__":
    main()
