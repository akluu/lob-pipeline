"""Replay one instrument-day of MBO rows through a Book and get LOBSTER rows as DataFrames."""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from lobpipe.book import COLUMNS, Book, BookError, book_columns

NY = ZoneInfo("America/New_York")
CHUNK = 500_000
OK = "ok"


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


class Day:
    """One instrument-day going through its Book. The first error is kept, not raised, so a
    day the book can't explain is reported while the rest of the run goes on."""

    def __init__(self, instrument_id, day, levels=10, verify=False):
        self.instrument_id, self.day, self.levels = instrument_id, day, levels
        self.book = Book(levels=levels, verify=verify)
        self.midnight = et_midnight_ns(day)
        self.rows_read = 0
        self.error = None

    def _run(self, step):
        if self.error is None:
            try:
                return step()
            except BookError as e:
                self.error = str(e)
        return []

    def feed(self, rows):
        """Feed MBO rows (a DataFrame in row_nr order); yields the LOBSTER rows as frames."""
        self.rows_read += len(rows)
        if self.error:
            return
        for lo in range(0, len(rows), CHUNK):
            out = self._run(lambda: self.book.feed(feed_rows(rows.iloc[lo:lo + CHUNK], self.midnight)))
            if out:
                yield self._labelled(output_frame(out, self.levels))

    def finish(self):
        """"ok", or why this instrument-day could not be rebuilt."""
        self._run(self.book.finish)
        if self.error is None and not self.book.initialized:
            self.error = "no book snapshot in the input"
        return self.error or OK

    def status_row(self):
        """The row that ends this instrument-day in the Spark output."""
        status = self.finish()
        return self._labelled(output_frame([[0] * (len(COLUMNS) + 4 * self.levels)], self.levels), status)

    def _labelled(self, frame, status=None):
        frame.insert(0, "date", self.day)
        frame.insert(0, "instrument_id", np.int32(self.instrument_id))
        frame["status"] = status
        frame["input_rows"] = self.rows_read if status else None
        return frame
