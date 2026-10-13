"""Write LOBSTER message/orderbook CSV pairs from the Spark output.

Files follow LOBSTER's naming, e.g. ES_2025-10-02_34200000_57600000_message_10.csv.
No header row. Message columns: time (seconds after midnight, New York time, 9 decimals),
event type, order id, size, price (dollars x 10000), direction. Orderbook columns:
ask price 1, ask size 1, bid price 1, bid size 1, ... for each level.

Only a Spark output with a _SUCCESS marker is read. All requested days are written to
<out>.partial and published with a single directory rename, so a reader never sees a
half-written file or a message file without its orderbook.
"""

import argparse
import os
import shutil

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from lobpipe.book import book_columns

MESSAGE_COLUMNS = ["type", "order_id", "size", "price", "direction"]


def seconds_text(time_ns):
    whole = (time_ns // 10**9).astype(str)
    frac = np.char.zfill((time_ns % 10**9).astype(str), 9)
    return np.char.add(np.char.add(whole, "."), frac)


def file_names(ticker, day, levels, window=(34_200_000, 57_600_000)):
    stem = f"{ticker}_{day}_{window[0]}_{window[1]}"
    return f"{stem}_message_{levels}.csv", f"{stem}_orderbook_{levels}.csv"


def export_day(src, day, out_dir, ticker="ES", levels=10, instrument_id=None):
    """Write one day's CSV pair into out_dir (used by export(), which stages and publishes)."""
    table = pq.read_table(os.path.join(src, f"date={day}"))
    df = table.to_pandas()
    if instrument_id is None:
        ids = df["instrument_id"].unique()
        if len(ids) != 1:
            raise ValueError(f"{day} has several instruments {list(ids)}; pass instrument_id")
    else:
        df = df[df["instrument_id"] == instrument_id]
    df = df.sort_values(["row_nr", "sub"], kind="stable")
    if df.duplicated(["row_nr", "sub"]).any():
        raise ValueError(f"{day}: duplicate (row_nr, sub) keys")

    msg_path, book_path = (os.path.join(out_dir, n) for n in file_names(ticker, day, levels))
    opts = pacsv.WriteOptions(include_header=False)
    messages = pa.table({"time": seconds_text(df["time_ns"].to_numpy())}
                        | {c: df[c].to_numpy() for c in MESSAGE_COLUMNS})
    book = pa.table({c: df[c].to_numpy() for c in book_columns(levels)})
    pacsv.write_csv(messages, msg_path, opts)
    pacsv.write_csv(book, book_path, opts)
    return len(df)


def export(src, dates, out_dir, ticker="ES", levels=10, overwrite=False):
    """Write the CSV pairs for all dates and publish them together as out_dir."""
    out_dir = out_dir.rstrip("/")
    if not os.path.exists(os.path.join(src, "_SUCCESS")):
        raise FileNotFoundError(f"{src} has no _SUCCESS marker; the Spark job did not finish")
    if os.path.exists(out_dir) and not overwrite:
        raise FileExistsError(f"{out_dir} exists (use --overwrite)")
    tmp = out_dir + ".partial"
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    counts = {day: export_day(src, day, tmp, ticker, levels) for day in dates}
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
    p.add_argument("--src", required=True, help="parquet dataset written by lobpipe.spark_job")
    p.add_argument("--out", required=True, help="directory for the CSV files")
    p.add_argument("--dates", nargs="+", required=True)
    p.add_argument("--ticker", default="ES")
    p.add_argument("--levels", type=int, default=10)
    p.add_argument("--overwrite", action="store_true", help="replace an existing output directory")
    args = p.parse_args()
    counts = export(args.src, args.dates, args.out, args.ticker, args.levels, args.overwrite)
    for day, n in counts.items():
        print(f"{day}: {n:,} rows")


if __name__ == "__main__":
    main()
