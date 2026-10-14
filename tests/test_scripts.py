import json
import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lobpipe.book import F_LAST, F_SNAPSHOT, Book
from lobpipe.spark_job import run
from scripts import check_snapshots, compare_spark
from tests.streams import OUT, Stream, random_stream, write_mbo
from tests.test_spark_job import write_dataset

DAY1, DAY2 = "2025-10-02", "2025-10-03"


def closing_book(stream):
    book = Book()
    book.feed(stream.rows)
    book.finish()
    return book.orders


def next_day(orders, change=None):
    """A day that opens with a snapshot of `orders` and then trades a little."""
    snap = [(side, oid, price / 1e9, size) for oid, (side, price, size) in sorted(orders.items())]
    if change == "size":
        side, oid, price, size = snap[0]
        snap[0] = (side, oid, price, size + 1)
    if change == "duplicate":
        snap[1] = (snap[1][0], snap[0][1]) + snap[1][2:]
    s = Stream().snapshot(*snap).event(("A", "B", 9_999, 99.0, 1))
    if change == "missing":
        s.rows = s.rows[len(snap) + 1:]
    if change == "truncated":
        s.rows = s.rows[:len(snap)]
    return s.rows


def run_main(module, argv, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["script"] + argv)
    with pytest.raises(SystemExit) as stop:
        module.main()
    return stop.value.code


def test_snapshots_match_after_a_clean_day(tmp_path, monkeypatch, capsys):
    day1 = random_stream(1, n_events=300)
    write_mbo(str(tmp_path), [(DAY1, 7, day1.rows), (DAY2, 7, next_day(closing_book(day1)))])
    assert run_main(check_snapshots, ["--mbo", str(tmp_path)], monkeypatch) == 0
    assert "1 of 1 day boundaries identical" in capsys.readouterr().out


@pytest.mark.parametrize("change, verdict", [("size", "DIFFERENT"), ("duplicate", "BAD SNAPSHOT"),
                                             ("missing", "BAD SNAPSHOT"), ("truncated", "BAD SNAPSHOT")])
def test_snapshot_problems_fail(tmp_path, monkeypatch, capsys, change, verdict):
    day1 = random_stream(1, n_events=300)
    write_mbo(str(tmp_path), [(DAY1, 7, day1.rows), (DAY2, 7, next_day(closing_book(day1), change))])
    assert run_main(check_snapshots, ["--mbo", str(tmp_path)], monkeypatch) == 1
    assert verdict in capsys.readouterr().out


def test_instrument_change_is_skipped_not_passed(tmp_path, monkeypatch, capsys):
    day1 = random_stream(1, n_events=300)
    write_mbo(str(tmp_path), [(DAY1, 7, day1.rows), (DAY2, 9, next_day(closing_book(day1)))])
    assert run_main(check_snapshots, ["--mbo", str(tmp_path)], monkeypatch) == 1
    assert "0 of 0 day boundaries identical, 2 skipped" in capsys.readouterr().out


def test_opening_event_is_found_by_row_number_not_file_name(tmp_path, monkeypatch, capsys):
    day1 = random_stream(1, n_events=300)
    write_mbo(str(tmp_path), [(DAY1, 7, day1.rows), (DAY2, 7, next_day(closing_book(day1)))])
    # split day 2 so the file with the snapshot sorts last by name
    folder = os.path.join(str(tmp_path), f"date={DAY2}")
    table = pq.read_table(os.path.join(folder, "part-7.parquet"))
    os.remove(os.path.join(folder, "part-7.parquet"))
    pq.write_table(table.slice(0, 5), os.path.join(folder, "part-zzz.parquet"))
    pq.write_table(table.slice(5), os.path.join(folder, "part-aaa.parquet"))
    assert run_main(check_snapshots, ["--mbo", str(tmp_path)], monkeypatch) == 0
    assert "1 of 1 day boundaries identical" in capsys.readouterr().out


def test_repeated_row_numbers_are_rejected(tmp_path, monkeypatch):
    day1 = random_stream(1, n_events=300)
    write_mbo(str(tmp_path), [(DAY1, 7, day1.rows + day1.rows[-2:]), (DAY2, 7, next_day(closing_book(day1)))])
    with pytest.raises(ValueError, match="appears twice"):
        run_main(check_snapshots, ["--mbo", str(tmp_path)], monkeypatch)


def test_missing_day_fails_even_against_an_empty_snapshot(tmp_path, monkeypatch, capsys):
    empty = [(0, OUT, OUT, "R", "N", 0, 2**63 - 1, 0, F_SNAPSHOT | F_LAST)]
    write_mbo(str(tmp_path), [(DAY1, 7, []), (DAY2, 7, empty)])
    assert run_main(check_snapshots, ["--mbo", str(tmp_path)], monkeypatch) == 1
    assert "REBUILD FAILED" in capsys.readouterr().out


@pytest.fixture(scope="module")
def spark_output(spark, tmp_path_factory):
    root = str(tmp_path_factory.mktemp("cmp"))
    mbo, lob = os.path.join(root, "mbo"), os.path.join(root, "lob")
    write_dataset(mbo)
    run(spark, mbo, lob, partitions=3)
    return mbo, lob


def copy_of(spark_output, tmp_path):
    import shutil

    mbo, lob = spark_output
    shutil.copytree(lob, tmp_path / "lob")
    return mbo, str(tmp_path / "lob")


def rewrite_spark_files(lob, day, edit):
    folder = os.path.join(lob, f"date={day}")
    for name in sorted(f for f in os.listdir(folder) if f.endswith(".parquet")):
        table = pq.read_table(os.path.join(folder, name))
        df = edit(table.to_pandas())
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), os.path.join(folder, name))


def test_spark_output_matches(spark_output, monkeypatch, capsys):
    mbo, lob = spark_output
    assert run_main(compare_spark, ["--mbo", mbo, "--lob", lob], monkeypatch) == 0
    assert "2 of 2 dates identical" in capsys.readouterr().out


def first_data_row(df):
    return df.index == df.index[df["status"].isna()][0]


@pytest.mark.parametrize("edit", [
    lambda df: df.assign(instrument_id=df["instrument_id"].where(df["instrument_id"] != 9, 8)),
    lambda df: pd.concat([df, df[first_data_row(df)]], ignore_index=True),
    lambda df: df.assign(bid_size_1=df["bid_size_1"] + first_data_row(df)),
    lambda df: pd.concat([df, df[first_data_row(df)].assign(instrument_id=999)], ignore_index=True),
], ids=["instrument", "duplicate", "value", "unlisted instrument"])
def test_spark_output_problems_fail(spark_output, tmp_path, monkeypatch, edit):
    mbo, lob = copy_of(spark_output, tmp_path)
    rewrite_spark_files(lob, DAY2, edit)
    assert run_main(compare_spark, ["--mbo", mbo, "--lob", lob], monkeypatch) == 1


def test_fractional_value_is_not_rounded_away(spark_output, tmp_path, monkeypatch):
    mbo, lob = copy_of(spark_output, tmp_path)
    rewrite_spark_files(lob, DAY2, lambda df: df.assign(ask_size_1=df["ask_size_1"] + 0.5 * first_data_row(df)))
    with pytest.raises(ValueError, match="ask_size_1"):
        run_main(compare_spark, ["--mbo", mbo, "--lob", lob], monkeypatch)


@pytest.mark.parametrize("marker", ["_SUCCESS", "_status.json"])
def test_unfinished_spark_output_fails(spark_output, tmp_path, monkeypatch, marker):
    mbo, lob = copy_of(spark_output, tmp_path)
    os.remove(os.path.join(lob, marker))
    assert run_main(compare_spark, ["--mbo", mbo, "--lob", lob], monkeypatch) != 0


def test_a_day_that_fails_in_both_is_not_a_difference(spark, tmp_path, monkeypatch, capsys):
    mbo, lob = str(tmp_path / "mbo"), str(tmp_path / "lob")
    write_dataset(mbo, broken=(DAY2, 9))
    run(spark, mbo, lob, partitions=3)
    assert run_main(compare_spark, ["--mbo", mbo, "--lob", lob], monkeypatch) == 0
    assert "1 failed in both" in capsys.readouterr().out
    # a status that disagrees with the direct rebuild is a difference
    with open(os.path.join(lob, "_status.json")) as f:
        statuses = json.load(f)
    for st in statuses:
        if (st["date"], st["instrument_id"]) == (DAY2, 7):
            st["status"] = "row 1: something else"
    with open(os.path.join(lob, "_status.json"), "w") as f:
        json.dump(statuses, f)
    assert run_main(compare_spark, ["--mbo", mbo, "--lob", lob], monkeypatch) == 1
