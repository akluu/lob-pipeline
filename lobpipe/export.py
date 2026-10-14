"""Write LOBSTER message/orderbook CSV pairs from the Spark output.

Files follow LOBSTER's naming, e.g. ES_2025-10-02_34200000_57600000_message_10.csv.
No header row. Message columns: time (seconds after midnight, New York time, 9 decimals),
event type, order id, size, price (dollars x 10000), direction. Orderbook columns:
ask price 1, ask size 1, bid price 1, bid size 1, ... for each level.

Only a finished Spark output (_SUCCESS and _status.json) is read, which can be local or on
S3, and only instrument-days whose rebuild succeeded are written. A day is sorted and
written in slices, so memory stays near the size of that day's rows in Arrow. The CSV
files go to a local folder: all requested days are written to <out>.partial and published
with a single directory rename, so a reader never sees a half-written file or a message
file without its orderbook.
"""

import argparse
import os
import shutil

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from lobpipe.book import book_columns
from lobpipe.fs import join, resolve
from lobpipe.spark_job import ok_days

MESSAGE_COLUMNS = ["type", "order_id", "size", "price", "direction"]
SLICE = 1_000_000


def seconds_text(time_ns):
    """Seconds after midnight with 9 decimals, e.g. 34200.000000001 (times are never negative)."""
    t = time_ns if isinstance(time_ns, (pa.Array, pa.ChunkedArray)) else pa.array(time_ns, type=pa.int64())
    whole = pc.divide(t, 10**9)
    frac = pc.subtract(t, pc.multiply(whole, 10**9))
    return pc.binary_join_element_wise(pc.cast(whole, pa.string()), pc.utf8_lpad(pc.cast(frac, pa.string()), 9, "0"), ".")


def file_names(ticker, day, levels, window=(34_200_000, 57_600_000)):
    stem = f"{ticker}_{day}_{window[0]}_{window[1]}"
    return f"{stem}_message_{levels}.csv", f"{stem}_orderbook_{levels}.csv"


def export_day(src, day, out_dir, ticker="ES", levels=10, instrument_id=None, ok=None):
    """Write one instrument-day's CSV pair into out_dir (export() stages and publishes)."""
    ok = ok_days(src) if ok is None else ok
    if instrument_id is None:
        passed = sorted(iid for iid, d in ok if d == day)
        if len(passed) != 1:
            raise ValueError(f"{day}: rebuilt instruments {passed}; pass the instrument id to export")
        instrument_id = passed[0]
    elif (instrument_id, day) not in ok:
        raise ValueError(f"{day} instrument {instrument_id} was not rebuilt successfully, see _status.json")
    fs, path = resolve(join(src, f"date={day}"))
    # data rows only: the status row that ends each instrument-day is left out
    rows = (pc.field("instrument_id") == instrument_id) & pc.field("status").is_null()
    table = pq.read_table(path, filesystem=fs, filters=rows, columns=["row_nr", "sub", "time_ns"]
                          + MESSAGE_COLUMNS + book_columns(levels))
    order = pc.sort_indices(table, sort_keys=[("row_nr", "ascending"), ("sub", "ascending")])
    keys = np.column_stack([table["row_nr"].to_numpy(), table["sub"].to_numpy()])[order.to_numpy()]
    if (np.diff(keys, axis=0) == 0).all(axis=1).any():
        raise ValueError(f"{day}: duplicate (row_nr, sub) keys")

    msg_path, book_path = (os.path.join(out_dir, n) for n in file_names(ticker, day, levels))
    opts = pacsv.WriteOptions(include_header=False)
    msg_schema = pa.schema([("time", pa.string())] + [(c, pa.int64()) for c in MESSAGE_COLUMNS])
    book_schema = pa.schema([(c, pa.int64()) for c in book_columns(levels)])
    with pacsv.CSVWriter(msg_path, msg_schema, write_options=opts) as messages, \
            pacsv.CSVWriter(book_path, book_schema, write_options=opts) as book:
        for lo in range(0, table.num_rows, SLICE):
            part = table.take(order[lo:lo + SLICE])
            messages.write_table(pa.table([seconds_text(part["time_ns"])] + [part[c] for c in MESSAGE_COLUMNS],
                                          schema=msg_schema))
            book.write_table(part.select(book_columns(levels)).cast(book_schema))
    return table.num_rows


def export(src, dates, out_dir, ticker="ES", levels=10, overwrite=False, instrument_id=None):
    """Write the CSV pairs for all dates and publish them together as out_dir."""
    out_dir = out_dir.rstrip("/")
    fs, path = resolve(src)
    if fs.get_file_info(join(path, "_SUCCESS")).type == pafs.FileType.NotFound:
        raise FileNotFoundError(f"{src} has no _SUCCESS marker; the Spark job did not finish")
    ok = ok_days(src)
    if os.path.exists(out_dir) and not overwrite:
        raise FileExistsError(f"{out_dir} exists (use --overwrite)")
    tmp = out_dir + ".partial"
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    counts = {day: export_day(src, day, tmp, ticker, levels, instrument_id, ok) for day in dates}
    if os.path.exists(out_dir):
        old = out_dir + ".old"
        shutil.rmtree(old, ignore_errors=True)
        os.replace(out_dir, old)
        os.replace(tmp, out_dir)
        shutil.rmtree(old)
    else:
        os.replace(tmp, out_dir)
    return counts


def main():
    p = argparse.ArgumentParser(description="Write LOBSTER CSV pairs from the Spark output.")
    p.add_argument("--src", required=True, help="dataset written by lobpipe.spark_job (folder or s3://)")
    p.add_argument("--out", required=True, help="local directory for the CSV files")
    p.add_argument("--dates", nargs="+", required=True)
    p.add_argument("--ticker", default="ES")
    p.add_argument("--instrument", type=int, help="instrument id, needed when a day has more than one")
    p.add_argument("--levels", type=int, default=10)
    p.add_argument("--overwrite", action="store_true", help="replace an existing output directory")
    args = p.parse_args()
    counts = export(args.src, args.dates, args.out, args.ticker, args.levels, args.overwrite, args.instrument)
    for day, n in counts.items():
        print(f"{day}: {n:,} rows")


if __name__ == "__main__":
    main()
