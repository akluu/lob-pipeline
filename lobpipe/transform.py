"""Run the book over one instrument-day of MBO rows and return LOBSTER rows."""

import gc
from datetime import date, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from lobpipe.book import COLUMNS, Book, book_columns

NY = ZoneInfo("America/New_York")
CHUNK_ROWS = 500_000


def et_midnight_ns(day):
    """UTC nanoseconds of 00:00 New York time on the given date (YYYY-MM-DD)."""
    d = date.fromisoformat(str(day))
    return int(datetime(d.year, d.month, d.day, tzinfo=NY).timestamp()) * 10**9


def feed_rows(df, midnight_ns):
    time_ns = df["ts_event"].to_numpy() - midnight_ns
    cols = [df["row_nr"].tolist(), df["ts_event"].tolist(), time_ns.tolist(), df["action"].tolist(),
            df["side"].tolist(), df["order_id"].tolist(), df["price"].tolist(), df["size"].tolist(),
            df["flags"].tolist()]
    return zip(*cols)


def output_frame(rows, levels):
    cols = COLUMNS + book_columns(levels)
    if not rows:
        return pd.DataFrame(np.zeros((0, len(cols)), dtype=np.int64), columns=cols)
    return pd.DataFrame(np.array(rows, dtype=np.int64), columns=cols)


def rebuild_chunks(book, df, day, chunk_rows=CHUNK_ROWS):
    """Feed df (one instrument, one UTC date, row_nr order) to book; yield output frames."""
    midnight = et_midnight_ns(day)
    enabled = gc.isenabled()
    gc.disable()
    try:
        for lo in range(0, len(df), chunk_rows):
            rows = book.feed(feed_rows(df.iloc[lo:lo + chunk_rows], midnight))
            if rows:
                yield output_frame(rows, book.levels)
    finally:
        if enabled:
            gc.enable()


def rebuild(df, day, levels=10, verify=False):
    book = Book(levels=levels, verify=verify)
    frames = list(rebuild_chunks(book, df, day))
    book.finish()
    out = pd.concat(frames, ignore_index=True) if frames else output_frame([], levels)
    return out, book
