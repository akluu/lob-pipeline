"""LOBSTER message/orderbook CSV files for chosen days, from the Spark output (local or S3).

Files are named the LOBSTER way, e.g. ES_2025-10-02_34200000_57600000_message_10.csv, and
have no header. Message columns: time (seconds after midnight New York time, 9 decimals),
event type, order id, size, price (dollars x 10000), direction. Orderbook columns: ask
price 1, ask size 1, bid price 1, bid size 1, ... for each level.

Only instrument-days that rebuilt successfully are written. All files are written to
<out>.partial first and renamed to <out> at the end, so a reader never sees half of them.
"""

import argparse
import os
import shutil

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from lobpipe.book import book_columns
from lobpipe.fs import exists, join, resolve
from lobpipe.spark_job import ok_days

MESSAGE_COLUMNS = ["type", "order_id", "size", "price", "direction"]
SLICE = 1_000_000


def seconds_text(time_ns):
    """Arrow int64 nanoseconds -> text seconds with 9 decimals, e.g. 34200.000000001."""
    whole = pc.divide(time_ns, 10**9)
    frac = pc.subtract(time_ns, pc.multiply(whole, 10**9))
    return pc.binary_join_element_wise(pc.cast(whole, pa.string()), pc.utf8_lpad(pc.cast(frac, pa.string()), 9, "0"), ".")


def file_names(ticker, day, levels, window=(34_200_000, 57_600_000)):
    stem = f"{ticker}_{day}_{window[0]}_{window[1]}"
    return f"{stem}_message_{levels}.csv", f"{stem}_orderbook_{levels}.csv"


def export_day(src, day, out_dir, ticker, levels, instrument_id, ok):
    """Write one instrument-day's CSV pair into out_dir; returns its number of rows."""
    if instrument_id is None:
        rebuilt = sorted(iid for iid, d in ok if d == day)
        if len(rebuilt) != 1:
            raise ValueError(f"{day}: rebuilt instruments {rebuilt}; pass the instrument id to export")
        instrument_id = rebuilt[0]
    if (instrument_id, day) not in ok:
        raise ValueError(f"{day} instrument {instrument_id} was not rebuilt successfully, see _status.json")

    fs, path = resolve(join(src, f"date={day}"))
    data_rows = (pc.field("instrument_id") == instrument_id) & pc.field("status").is_null()
    table = pq.read_table(path, filesystem=fs, filters=data_rows,
                          columns=["row_nr", "sub", "time_ns"] + MESSAGE_COLUMNS + book_columns(levels))
    order = pc.sort_indices(table, sort_keys=[("row_nr", "ascending"), ("sub", "ascending")])
    keys = np.column_stack([table["row_nr"].to_numpy(), table["sub"].to_numpy()])[order.to_numpy()]
    if (np.diff(keys, axis=0) == 0).all(axis=1).any():
        raise ValueError(f"{day}: duplicate (row_nr, sub) keys")

    msg_path, book_path = (os.path.join(out_dir, name) for name in file_names(ticker, day, levels))
    msg_schema = pa.schema([("time", pa.string())] + [(c, pa.int64()) for c in MESSAGE_COLUMNS])
    book_schema = pa.schema([(c, pa.int64()) for c in book_columns(levels)])
    no_header = pacsv.WriteOptions(include_header=False)
    with pacsv.CSVWriter(msg_path, msg_schema, write_options=no_header) as messages, \
            pacsv.CSVWriter(book_path, book_schema, write_options=no_header) as book:
        for lo in range(0, table.num_rows, SLICE):  # sorted in slices to keep memory down
            part = table.take(order[lo:lo + SLICE])
            time = seconds_text(part["time_ns"])
            messages.write_table(pa.table([time] + [part[c] for c in MESSAGE_COLUMNS], schema=msg_schema))
            book.write_table(part.select(book_columns(levels)).cast(book_schema))
    return table.num_rows


def export(src, dates, out_dir, ticker="ES", levels=10, overwrite=False, instrument_id=None):
    """Write the CSV pairs for all dates and publish them together as out_dir."""
    if not exists(join(src, "_SUCCESS")):
        raise FileNotFoundError(f"{src} has no _SUCCESS marker; the Spark job did not finish")
    ok = ok_days(src)
    out_dir = out_dir.rstrip("/")
    if os.path.exists(out_dir) and not overwrite:
        raise FileExistsError(f"{out_dir} exists (use --overwrite)")
    tmp, old = out_dir + ".partial", out_dir + ".old"
    if os.path.exists(tmp):
        shutil.rmtree(tmp)
    os.makedirs(tmp)
    counts = {day: export_day(src, day, tmp, ticker, levels, instrument_id, ok) for day in dates}
    if os.path.exists(out_dir):  # keep the previous export as .old until the new one is in place
        if os.path.exists(old):
            shutil.rmtree(old)
        os.replace(out_dir, old)
    os.replace(tmp, out_dir)
    if os.path.exists(old):
        shutil.rmtree(old)
    return counts


def main():
    p = argparse.ArgumentParser(description="Write LOBSTER CSV pairs from the Spark output.")
    p.add_argument("--src", required=True, help="dataset written by lobpipe.spark_job (folder or s3://)")
    p.add_argument("--out", required=True, help="local folder for the CSV files")
    p.add_argument("--dates", nargs="+", required=True)
    p.add_argument("--ticker", default="ES")
    p.add_argument("--instrument", type=int, help="instrument id, needed when a day has more than one")
    p.add_argument("--levels", type=int, default=10)
    p.add_argument("--overwrite", action="store_true", help="replace an existing output folder")
    args = p.parse_args()
    counts = export(args.src, args.dates, args.out, args.ticker, args.levels, args.overwrite, args.instrument)
    for day, n in counts.items():
        print(f"{day}: {n:,} rows")


if __name__ == "__main__":
    main()
