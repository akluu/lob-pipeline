"""Check the rebuilt book against the snapshots in the data.

Every date in the MBO data opens with a book snapshot: a reset followed by one add per
resting order. If the rebuild is right, the book left at the end of day D must equal,
order for order (side, price, size), the snapshot that opens day D+1. This compares every
pair of consecutive calendar dates, for each instrument the ingest manifest lists on both
days. There is no Saturday data, so the Friday -> Sunday gap is never compared.

usage: python scripts/check_snapshots.py --mbo data/mbo [--workers 1]
"""

import argparse
import json
import os
import sys
from datetime import date, timedelta
from multiprocessing import Pool

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lobpipe.book import F_LAST, Book, BookError  # noqa: E402
from lobpipe.transform import rebuild_chunks  # noqa: E402

COLUMNS = ["row_nr", "ts_event", "instrument_id", "action", "side", "order_id", "price", "size", "flags"]


def load(mbo, day, iid):
    """Every row of one instrument on one day, in file order."""
    table = pq.read_table(os.path.join(mbo, f"date={day}"), columns=COLUMNS, filters=[("instrument_id", "=", iid)])
    return table.to_pandas().sort_values("row_nr", kind="stable", ignore_index=True)


def opening_event(mbo, day, iid):
    """Rows of one instrument on one day up to and including its first F_LAST record."""
    folder = os.path.join(mbo, f"date={day}")
    parts = []
    # part files are numbered in input file order, so reading them in name order keeps row order
    for name in sorted(f for f in os.listdir(folder) if f.endswith(".parquet")):
        for batch in pq.ParquetFile(os.path.join(folder, name)).iter_batches(batch_size=100_000, columns=COLUMNS):
            rows = batch.to_pandas()
            rows = rows[rows["instrument_id"] == iid]
            ends = (rows["flags"].to_numpy() & F_LAST).nonzero()[0]
            if len(ends):
                parts.append(rows.iloc[:int(ends[0]) + 1])
                return pd.concat(parts, ignore_index=True)
            parts.append(rows)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=COLUMNS)


def run(book, rows, day):
    """Replay rows through book; return its orders {order_id: [side, price, size]}."""
    if (np.diff(rows["row_nr"].to_numpy()) <= 0).any():
        raise BookError(f"rows of {day} are not in row_nr order")
    for _ in rebuild_chunks(book, rows, day):
        pass
    book.finish()
    if not book.initialized:
        raise BookError(f"no snapshot in the rows of {day}")
    return book.orders


def compare_pair(args):
    mbo, day, nxt, iid = args
    # Book rejects an opening event that is not a reset followed by snapshot adds with
    # unique order ids and a B or A side, and one cut off before its last record
    try:
        expected = run(Book(levels=10), opening_event(mbo, nxt, iid), nxt)
    except BookError as e:
        return day, nxt, iid, None, None, f"BAD SNAPSHOT ({e})"
    try:
        rebuilt = run(Book(levels=10), load(mbo, day, iid), day)
    except BookError as e:
        return day, nxt, iid, None, None, f"REBUILD FAILED ({e})"
    return day, nxt, iid, len(rebuilt), len(expected), "identical" if rebuilt == expected else "DIFFERENT"


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--mbo", required=True, help="MBO parquet written by lobpipe.ingest")
    p.add_argument("--workers", type=int, default=1, help="days compared at once; each holds a whole day in memory")
    args = p.parse_args()
    with open(os.path.join(args.mbo, "_manifest.json")) as f:
        manifest = json.load(f)
    instruments = {day: {int(i) for i in v["instruments"]} for day, v in manifest["dates"].items()}

    pairs, skipped = [], []
    for day in sorted(instruments):
        nxt = str(date.fromisoformat(day) + timedelta(days=1))
        if nxt not in instruments:
            continue
        for iid in sorted(instruments[day] | instruments[nxt]):
            if iid in instruments[day] and iid in instruments[nxt]:
                pairs.append((args.mbo, day, nxt, iid))
            else:
                skipped.append((day, nxt, iid))

    if args.workers > 1:
        with Pool(args.workers) as pool:
            results = pool.map(compare_pair, pairs, chunksize=1)
    else:
        results = [compare_pair(pair) for pair in pairs]

    bad = 0
    for day, nxt, iid, n_rebuilt, n_snap, verdict in results:
        print(f"{day} -> {nxt} instrument {iid}: rebuilt {n_rebuilt or 0:,} orders, snapshot {n_snap or 0:,}  {verdict}")
        bad += verdict != "identical"
    for day, nxt, iid in skipped:
        print(f"{day} -> {nxt} instrument {iid}: only on one of the two days, skipped")
    print(f"{len(results) - bad} of {len(results)} day boundaries identical, {len(skipped)} skipped")
    sys.exit(1 if bad or not results else 0)


if __name__ == "__main__":
    main()
