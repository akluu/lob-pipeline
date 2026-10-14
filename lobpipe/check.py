"""Check a LOBSTER message/orderbook pair using only the two CSV files.

Every orderbook row must be a valid book (positive prices and sizes, prices improving
toward level 1, empty levels only below occupied ones), and every row must be the row
before it changed exactly as its message says: 1 adds size at the price, 2/3/4 remove
it, 5 changes nothing, and the other side never moves.

Taken as given, since the files can't show them: the first row, a level that appears at
the bottom when one above drops out of view, and messages beyond the visible depth.
"""

import argparse
import sys

import numpy as np
import pyarrow.csv as pacsv

EMPTY_ASK = 9_999_999_999
EMPTY_BID = -9_999_999_999


def read(path, ncols, string_first=False):
    names = [f"c{i}" for i in range(ncols)]
    types = {"c0": "string"} if string_first else {}
    return pacsv.read_csv(path, read_options=pacsv.ReadOptions(column_names=names),
                          convert_options=pacsv.ConvertOptions(column_types=types))


def side_levels(row, side, levels):
    """[(price, size), ...] best first, dummies dropped. side 0 = ask, 1 = bid."""
    out = []
    for i in range(levels):
        price, size = row[4 * i + 2 * side], row[4 * i + 2 * side + 1]
        if price in (EMPTY_ASK, EMPTY_BID):
            break
        out.append((int(price), int(size)))
    return out


def invalid_rows(book, levels):
    """Indices of orderbook rows that are not a valid book, with the reason."""
    found = {}
    for side, empty, better in ((0, EMPTY_ASK, np.less), (1, EMPTY_BID, np.greater)):
        px = book[:, 2 * side::4][:, :levels]
        sz = book[:, 2 * side + 1::4][:, :levels]
        dummy = px == empty
        name = "ask" if side == 0 else "bid"
        checks = [
            ((dummy & (sz != 0)).any(axis=1), f"{name} empty level with a size"),
            ((~dummy & ((px <= 0) | (sz <= 0))).any(axis=1), f"{name} level with a non-positive price or size"),
            ((dummy[:, :-1] & ~dummy[:, 1:]).any(axis=1), f"{name} empty level above an occupied one"),
            ((~dummy[:, 1:] & ~better(px[:, :-1], px[:, 1:])).any(axis=1), f"{name} prices out of order"),
        ]
        for mask, why in checks:
            for i in np.flatnonzero(mask)[:1000]:
                found.setdefault(int(i), why)
    return sorted(found.items())


def expected_levels(prev, price, delta, side, levels):
    """prev levels after adding delta at price (a size can come out negative), and whether
    prev was full; None if a full book's change is beyond its worst level."""
    full = len(prev) == levels
    if full and (price > prev[-1][0] if side == 0 else price < prev[-1][0]):
        return None
    book = dict(prev)
    book[price] = book.get(price, 0) + delta
    if book[price] == 0:
        del book[price]
    return sorted(book.items(), reverse=(side == 1))[:levels], full


def check(message_path, levels=10, max_report=5):
    msg = read(message_path, 6, string_first=True)
    book = read(message_path.replace("message", "orderbook"), 4 * levels).to_pandas().to_numpy()
    kinds = msg.column("c1").to_numpy()
    sizes = msg.column("c3").to_numpy()
    prices = msg.column("c4").to_numpy()
    dirs = msg.column("c5").to_numpy()
    times = np.array(msg.column("c0").to_pylist(), dtype=np.float64)
    problems = []
    exposed_levels = 0
    beyond_depth = 0

    def bad(i, why):
        problems.append((i, why))

    if len(book) != len(kinds):
        bad(-1, f"{len(kinds)} messages but {len(book)} orderbook rows")
    if not np.isin(kinds, [1, 2, 3, 4, 5]).all():
        bad(-1, "unknown event type")
    if not np.isin(dirs, [-1, 1]).all() or (sizes <= 0).any() or (prices <= 0).any():
        bad(-1, "bad direction, size or price")
    if (np.diff(times) < 0).any():
        bad(int(np.flatnonzero(np.diff(times) < 0)[0]) + 1, "time goes backwards")
    for i, why in invalid_rows(book, levels):
        bad(i, why)

    for i in range(1, min(len(book), len(kinds))):
        prev, cur = book[i - 1], book[i]
        kind = kinds[i]
        if kind == 5:
            if not (prev == cur).all():
                bad(i, "type 5 changed the book")
            continue
        side = 1 if dirs[i] == 1 else 0
        other = 1 - side
        if not (prev.reshape(-1, 2)[other::2] == cur.reshape(-1, 2)[other::2]).all():
            bad(i, "opposite side changed")
            continue
        p_lv, c_lv = side_levels(prev, side, levels), side_levels(cur, side, levels)
        delta = sizes[i] if kind == 1 else -sizes[i]
        exp = expected_levels(p_lv, int(prices[i]), int(delta), side, levels)
        if exp is not None and any(size < 0 for _, size in exp[0]):
            bad(i, "size below zero")
            continue
        if exp is None:
            beyond_depth += 1
            if c_lv != p_lv:
                bad(i, "change beyond the visible depth moved the book")
            continue
        exp_lv, was_full = exp
        if c_lv == exp_lv:
            continue
        exposed = (was_full and len(exp_lv) == levels - 1 and len(c_lv) == levels and c_lv[:-1] == exp_lv
                   and (c_lv[-1][0] < exp_lv[-1][0] if side == 1 else c_lv[-1][0] > exp_lv[-1][0]))
        if exposed:
            exposed_levels += 1
        else:
            bad(i, f"book does not match message: expected {exp_lv[:3]}... got {c_lv[:3]}...")

    crossed = int(((book[:, 2] > book[:, 0]) & (book[:, 0] != EMPTY_ASK)).sum())
    print(f"{message_path}: {len(kinds):,} rows, {len(problems)} problems, {crossed} crossed rows; "
          f"taken as given: first row, {exposed_levels:,} levels exposed from beyond depth {levels}, "
          f"{beyond_depth:,} messages beyond the visible depth (rows unchanged)")
    for i, why in problems[:max_report]:
        print(f"  row {i}: {why}")
    return problems


def main():
    p = argparse.ArgumentParser(description="Check a LOBSTER message/orderbook pair for consistency.")
    p.add_argument("message_files", nargs="+")
    p.add_argument("--levels", type=int, default=10)
    args = p.parse_args()
    failed = False
    for path in args.message_files:
        failed |= bool(check(path, args.levels))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
