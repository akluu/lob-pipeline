"""Check the rebuilt book against the snapshots in the data.

Every date in the MBO data opens with a book snapshot: a reset followed by one add per
resting order. If the rebuild is right, the book left at the end of day D must equal,
order for order (side, price, size), the snapshot that opens day D+1. This compares every
pair of consecutive calendar dates, for each instrument the ingest manifest lists on both
days. There is no Saturday data, so the Friday -> Sunday gap is never compared. On a large
dataset, run it on a sample with --dates (each date is compared with the day after it).

usage: python -m scripts.check_snapshots --mbo data/mbo [--dates D ...] [--workers 1]
"""

import argparse
import sys
from datetime import date, timedelta
from multiprocessing import Pool

import pyarrow.parquet as pq

from lobpipe.book import F_LAST
from lobpipe.fs import join, read_json, resolve
from lobpipe.transform import OK, Day

COLUMNS = ["row_nr", "ts_event", "instrument_id", "action", "side", "order_id", "price", "size", "flags"]


def load(mbo, day, iid):
    """Every row of one instrument on one day, in file order (row_nr)."""
    fs, path = resolve(join(mbo, f"date={day}"))
    table = pq.read_table(path, filesystem=fs, columns=COLUMNS, filters=[("instrument_id", "=", iid)])
    rows = table.to_pandas().sort_values("row_nr", ignore_index=True)
    if rows["row_nr"].duplicated().any():
        raise ValueError(f"{day} instrument {iid}: the same row_nr appears twice")
    return rows


def opening_event(mbo, day, iid):
    """Rows of one instrument on one day up to and including its first F_LAST record."""
    rows = load(mbo, day, iid)
    ends = (rows["flags"].to_numpy() & F_LAST).nonzero()[0]
    return rows.iloc[:int(ends[0]) + 1] if len(ends) else rows


def replay(rows, day, iid):
    """The orders left after replaying the rows, and "ok" or what went wrong."""
    replayed = Day(iid, day)
    for _ in replayed.feed(rows):
        pass
    return replayed.book.orders, replayed.finish()


def compare_pair(args):
    mbo, day, nxt, iid = args
    expected, status = replay(opening_event(mbo, nxt, iid), nxt, iid)
    if status != OK:
        return day, nxt, iid, None, None, f"BAD SNAPSHOT ({status})"
    rebuilt, status = replay(load(mbo, day, iid), day, iid)
    if status != OK:
        return day, nxt, iid, None, None, f"REBUILD FAILED ({status})"
    return day, nxt, iid, len(rebuilt), len(expected), "identical" if rebuilt == expected else "DIFFERENT"


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--mbo", required=True, help="MBO parquet written by lobpipe.ingest")
    p.add_argument("--dates", nargs="*", help="default: every date in --mbo")
    p.add_argument("--workers", type=int, default=1, help="days compared at once; each holds a whole day in memory")
    args = p.parse_args()
    manifest = read_json(join(args.mbo, "_manifest.json"))
    instruments = {day: {int(i) for i in v["instruments"]} for day, v in manifest["dates"].items()}

    pairs, skipped = [], []
    for day in sorted(args.dates or instruments):
        if day not in instruments:
            sys.exit(f"{day} is not in {args.mbo}")
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
