"""Feed MBO rows from a DataFrame through a Book and get LOBSTER rows back as DataFrames."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from lobpipe.book import COLUMNS, book_columns

NY = ZoneInfo("America/New_York")
CHUNK = 500_000


def et_midnight_ns(day):
    """UTC nanoseconds of 00:00 New York time on a date (YYYY-MM-DD)."""
    d = date.fromisoformat(str(day))
    return int(datetime(d.year, d.month, d.day, tzinfo=NY).timestamp()) * 10**9


def feed_rows(df, midnight_ns):
    """The records Book.feed expects, from MBO rows of one day."""
    return zip(df["row_nr"].tolist(), df["ts_event"].tolist(), (df["ts_event"].to_numpy() - midnight_ns).tolist(),
               df["action"].tolist(), df["side"].tolist(), df["order_id"].tolist(), df["price"].tolist(),
               df["size"].tolist(), df["flags"].tolist())


def output_frame(rows, levels):
    cols = COLUMNS + book_columns(levels)
    return pd.DataFrame(np.array(rows, dtype=np.int64).reshape(len(rows), len(cols)), columns=cols)


def rebuild_chunks(book, df, day):
    """Feed one instrument-day (in row_nr order) to the book; yield its output in frames."""
    midnight = et_midnight_ns(day)
    for lo in range(0, len(df), CHUNK):
        rows = book.feed(feed_rows(df.iloc[lo:lo + CHUNK], midnight))
        if rows:
            yield output_frame(rows, book.levels)
