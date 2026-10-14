import json
import os
import subprocess
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lobpipe.book import Book
from lobpipe.ingest import SCHEMA
from lobpipe.spark_job import build, run
from lobpipe.transform import OK, et_midnight_ns, output_frame
from tests.streams import random_stream

DAYS = [("2025-10-02", 7, 1), ("2025-10-03", 7, 2), ("2025-10-03", 9, 3)]  # (date, instrument, seed)


def write_dataset(root, levels=5, broken=None):
    """MBO parquet the way ingest writes it, plus the direct rebuild of each instrument-day.

    broken: (date, instrument) whose stream gets a cancel for an order that does not exist.
    """
    expected, manifest = [], {"dates": {}}
    row0 = 0
    for day, iid, seed in DAYS:
        s = random_stream(seed, n_events=300)
        if (day, iid) == broken:
            s.event(("C", "B", 999_999, 100.0, 1))
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
        manifest["dates"].setdefault(day, {"rows": 0, "instruments": {}})["instruments"][str(iid)] = len(recs)

        if (day, iid) != broken:
            book = Book(levels=levels)
            rows = book.feed((row0 - len(recs) + r[0], midnight + r[1], r[1]) + r[3:] for r in s.rows)
            book.finish()
            out = output_frame(rows, levels)
            out.insert(0, "date", day)
            out.insert(0, "instrument_id", iid)
            expected.append(out)
    with open(os.path.join(root, "_manifest.json"), "w") as f:
        json.dump(manifest, f)
    return pd.concat(expected, ignore_index=True)


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    root = str(tmp_path_factory.mktemp("mbo"))
    return root, write_dataset(root)


def ordered(df):
    df = df.sort_values(["instrument_id", "date", "row_nr", "sub"]).reset_index(drop=True)
    df["instrument_id"] = df["instrument_id"].astype("int64")
    return df


def split(got):
    data = got[got["status"].isna()].drop(columns=["status", "input_rows"])
    statuses = {(int(r.instrument_id), r.date): r.status for r in got[got["status"].notna()].itertuples()}
    assert len(statuses) == len(got[got["status"].notna()])
    return data, statuses


@pytest.mark.parametrize("batch, partitions", [(1, 1), (7, 3), (100, 2), (100000, 8)])
def test_spark_matches_direct_rebuild(spark, dataset, batch, partitions):
    root, expected = dataset
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch))
    data, statuses = split(build(spark, root, levels=5, partitions=partitions).toPandas())
    assert len(expected) > 500
    pd.testing.assert_frame_equal(ordered(data), ordered(expected), check_dtype=False)
    assert statuses == {(iid, day): OK for day, iid, _ in DAYS}


def test_date_filter(spark, dataset):
    root, expected = dataset
    data, statuses = split(build(spark, root, levels=5, dates=["2025-10-03"], partitions=2).toPandas())
    assert set(data["date"]) == {"2025-10-03"}
    want = expected[expected["date"] == "2025-10-03"]
    pd.testing.assert_frame_equal(ordered(data), ordered(want), check_dtype=False)
    assert set(statuses) == {(7, "2025-10-03"), (9, "2025-10-03")}


@pytest.mark.parametrize("batch", [1, 50, 100000])
def test_a_failed_day_is_reported_and_the_rest_are_unaffected(spark, tmp_path, batch):
    root = str(tmp_path / "mbo")
    expected = write_dataset(root, broken=("2025-10-03", 7))
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch))
    data, statuses = split(build(spark, root, levels=5, partitions=1).toPandas())
    assert "unknown order" in statuses.pop((7, "2025-10-03"))
    assert statuses == {(7, "2025-10-02"): OK, (9, "2025-10-03"): OK}
    good = data[(data["instrument_id"] != 7) | (data["date"] != "2025-10-03")]
    pd.testing.assert_frame_equal(ordered(good), ordered(expected), check_dtype=False)


def test_run_writes_statuses_and_refuses_to_overwrite(spark, tmp_path):
    root, out = str(tmp_path / "mbo"), str(tmp_path / "lob")
    write_dataset(root, levels=10, broken=("2025-10-02", 7))
    statuses = run(spark, root, out)
    assert [s[:2] for s in statuses] == [("2025-10-02", 7), ("2025-10-03", 7), ("2025-10-03", 9)]
    assert "unknown order" in statuses[0][2] and statuses[1][2] == statuses[2][2] == OK
    with open(os.path.join(out, "_status.json")) as f:
        assert [(s["date"], s["instrument_id"], s["status"]) for s in json.load(f)] == statuses
    with pytest.raises(Exception, match="already exists"):
        run(spark, root, out)
    assert run(spark, root, out, dates=["2025-10-03"], overwrite=True) == statuses[1:]


def test_run_fails_if_input_rows_go_missing(spark, tmp_path):
    root = str(tmp_path / "mbo")
    write_dataset(root, levels=10)
    path = os.path.join(root, "date=2025-10-02", "part-7.parquet")
    table = pq.read_table(path)
    pq.write_table(table.slice(0, table.num_rows - 1), path)  # drop a whole final event
    with pytest.raises(RuntimeError, match="row counts"):
        run(spark, root, str(tmp_path / "lob"))


def test_cli_exit_code_reports_failed_days(tmp_path):
    root = str(tmp_path / "mbo")
    write_dataset(root, levels=10, broken=("2025-10-02", 7))
    cmd = [sys.executable, "-m", "lobpipe.spark_job", "--src", root, "--master", "local[1]", "--driver-memory", "1g"]
    first = subprocess.run(cmd + ["--out", str(tmp_path / "a")], capture_output=True, text=True)
    assert first.returncode == 1 and "FAILED 2025-10-02 instrument 7" in first.stdout, first.stderr[-2000:]
    second = subprocess.run(cmd + ["--out", str(tmp_path / "b"), "--allow-failed-days"], capture_output=True, text=True)
    assert second.returncode == 0 and "2 of 3 instrument-days rebuilt" in second.stdout, second.stderr[-2000:]


def test_run_needs_the_ingest_manifest(spark, tmp_path):
    root = str(tmp_path / "mbo")
    write_dataset(root)
    os.remove(os.path.join(root, "_manifest.json"))
    with pytest.raises(FileNotFoundError, match="_manifest.json"):
        run(spark, root, str(tmp_path / "lob"))


def test_spark_submit_settings_are_kept(tmp_path):
    import pyspark

    script = tmp_path / "job.py"
    script.write_text("import os\n"
                      "from lobpipe.spark import session\n"
                      "s = session(master='local[3]', driver_memory='3g')\n"
                      "print('MASTER', s.sparkContext.master)\n"
                      "print('MEMORY', s.sparkContext.getConf().get('spark.driver.memory', 'unset'))\n"
                      "print('PYTHON', os.environ.get('PYSPARK_PYTHON', 'unset'))\n")
    env = {k: v for k, v in os.environ.items() if k != "PYSPARK_PYTHON"}
    env.update(SPARK_HOME=os.path.dirname(pyspark.__file__), PYSPARK_DRIVER_PYTHON=sys.executable,
               PYTHONPATH=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    submit = os.path.join(os.path.dirname(sys.executable), "spark-submit")
    out = subprocess.run([submit, "--master", "local[1]", str(script)], env=env, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr[-2000:]
    assert "MASTER local[1]" in out.stdout
    assert "MEMORY unset" in out.stdout
    assert "PYTHON unset" in out.stdout
