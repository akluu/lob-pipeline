import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lobpipe.book import Book
from lobpipe.ingest import SCHEMA
from lobpipe.spark_job import build
from lobpipe.transform import et_midnight_ns, output_frame
from tests.streams import random_stream

DAYS = [("2025-10-02", 7, 1), ("2025-10-03", 7, 2), ("2025-10-03", 9, 3)]  # (date, instrument, seed)


def write_dataset(root):
    expected = []
    row0 = 0
    for day, iid, seed in DAYS:
        s = random_stream(seed, n_events=300)
        midnight = et_midnight_ns(day)
        recs = [(row0 + r[0], midnight + r[1]) + r[3:] for r in s.rows]
        row0 += len(recs)
        df = pd.DataFrame(recs, columns=["row_nr", "ts_event", "action", "side", "order_id", "price", "size", "flags"])
        df["ts_recv"] = df["ts_event"]
        df["instrument_id"] = iid
        df["sequence"] = df["row_nr"]
        table = pa.Table.from_pandas(df[SCHEMA.names], schema=SCHEMA, preserve_index=False)
        part = os.path.join(root, f"date={day}")
        os.makedirs(part, exist_ok=True)
        pq.write_table(table, os.path.join(part, f"part-{iid}.parquet"))

        book = Book(levels=5)
        rows = book.feed((row0 - len(recs) + r[0], midnight + r[1], r[1]) + r[3:] for r in s.rows)
        book.finish()
        out = output_frame(rows, 5)
        out.insert(0, "date", day)
        out.insert(0, "instrument_id", iid)
        expected.append(out)
    return pd.concat(expected, ignore_index=True)


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    root = str(tmp_path_factory.mktemp("mbo"))
    return root, write_dataset(root)


def ordered(df):
    df = df.sort_values(["instrument_id", "date", "row_nr", "sub"]).reset_index(drop=True)
    df["instrument_id"] = df["instrument_id"].astype("int64")
    return df


@pytest.mark.parametrize("batch, partitions", [(1, 1), (7, 3), (100, 2), (100000, 8)])
def test_spark_matches_direct_rebuild(spark, dataset, batch, partitions):
    root, expected = dataset
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch))
    got = build(spark, root, levels=5, partitions=partitions).toPandas()
    assert len(expected) > 500
    pd.testing.assert_frame_equal(ordered(got), ordered(expected), check_dtype=False)


def test_date_filter(spark, dataset):
    root, expected = dataset
    got = build(spark, root, levels=5, dates=["2025-10-03"], partitions=2).toPandas()
    assert set(got["date"]) == {"2025-10-03"}
    want = expected[expected["date"] == "2025-10-03"]
    pd.testing.assert_frame_equal(ordered(got), ordered(want), check_dtype=False)
