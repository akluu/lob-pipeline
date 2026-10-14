import json
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lobpipe import export as exp
from lobpipe.book import Book
from lobpipe.check import check
from lobpipe.export import export, file_names, seconds_text
from lobpipe.transform import output_frame
from tests.streams import random_stream

DAYS = ["2025-10-02", "2025-10-03"]
DAY = DAYS[0]


def write_lob(root, statuses=None):
    """Spark-style output for instrument 7 on each day: data rows, a status row, _status.json."""
    statuses = statuses or {day: "ok" for day in DAYS}
    frames = {}
    for seed, day in enumerate(DAYS, start=11):
        s = random_stream(seed, n_events=600)
        book = Book(levels=5)
        rows = book.feed(s.rows)
        book.finish()
        out = output_frame(rows, 5)
        out.insert(0, "instrument_id", np.int32(7))
        end = out.iloc[:1].copy()
        end.loc[:, out.columns[1:]] = 0
        stored = pd.concat([out.assign(status=None, input_rows=None),
                            end.assign(status=statuses[day], input_rows=len(s.rows))], ignore_index=True)
        shuffled = stored.sample(frac=1.0, random_state=0)  # parquet readers give no row order
        os.makedirs(root / f"date={day}")
        pq.write_table(pa.Table.from_pandas(shuffled, preserve_index=False), root / f"date={day}" / "part-0.parquet")
        frames[day] = out
    (root / "_SUCCESS").touch()
    (root / "_status.json").write_text(json.dumps([{"date": d, "instrument_id": 7, "status": st}
                                                   for d, st in statuses.items()]))
    return frames


@pytest.fixture
def exported(tmp_path):
    frames = write_lob(tmp_path / "lob")
    csv_dir = tmp_path / "csv"
    counts = export(str(tmp_path / "lob"), DAYS, str(csv_dir), levels=5)
    msg, ob = file_names("ES", DAY, 5)
    return csv_dir / msg, csv_dir / ob, frames[DAY], counts[DAY]


def test_names_and_time_format():
    assert file_names("ES", DAY, 10) == (f"ES_{DAY}_34200000_57600000_message_10.csv",
                                         f"ES_{DAY}_34200000_57600000_orderbook_10.csv")
    assert seconds_text(pa.array([34_200_000_000_001, 57_599_123_456_789, 5])).to_pylist() == [
        "34200.000000001", "57599.123456789", "0.000000005"]


def test_export_round_trip_and_check(exported):
    msg, ob, out, n = exported
    assert n == len(out) > 500
    lines = msg.read_text().splitlines()
    first = out.sort_values(["row_nr", "sub"]).iloc[0]
    assert lines[0].split(",")[1:] == [str(first[c]) for c in ["type", "order_id", "size", "price", "direction"]]
    assert len(ob.read_text().splitlines()) == n
    assert sorted(os.listdir(msg.parent)) == sorted(n for d in DAYS for n in file_names("ES", d, 5))
    assert check(str(msg), levels=5) == []


def test_output_does_not_depend_on_slice_size(tmp_path, monkeypatch):
    write_lob(tmp_path / "lob")
    export(str(tmp_path / "lob"), [DAY], str(tmp_path / "whole"), levels=5)
    monkeypatch.setattr(exp, "SLICE", 7)
    export(str(tmp_path / "lob"), [DAY], str(tmp_path / "sliced"), levels=5)
    for name in file_names("ES", DAY, 5):
        assert (tmp_path / "sliced" / name).read_bytes() == (tmp_path / "whole" / name).read_bytes()


def test_publishing_is_all_or_nothing(tmp_path, monkeypatch):
    write_lob(tmp_path / "lob")
    src, out = str(tmp_path / "lob"), str(tmp_path / "csv")
    export(src, [DAY], out, levels=5)
    before = sorted(os.listdir(out))
    with pytest.raises(FileExistsError):
        export(src, DAYS, out, levels=5)

    real = exp.export_day

    def dies_on_second_day(src, day, *args, **kwargs):
        if day == DAYS[1]:
            raise RuntimeError("killed")
        return real(src, day, *args, **kwargs)

    monkeypatch.setattr(exp, "export_day", dies_on_second_day)
    with pytest.raises(RuntimeError):
        export(src, DAYS, out, levels=5, overwrite=True)
    assert sorted(os.listdir(out)) == before  # the earlier export is untouched
    with pytest.raises(RuntimeError):
        export(src, DAYS, str(tmp_path / "fresh"), levels=5)
    assert not os.path.exists(tmp_path / "fresh")

    monkeypatch.setattr(exp, "export_day", real)
    export(src, DAYS, out, levels=5, overwrite=True)
    assert len(os.listdir(out)) == 4


def test_refuses_unfinished_spark_output(tmp_path):
    os.makedirs(tmp_path / "lob" / f"date={DAY}")
    with pytest.raises(FileNotFoundError, match="_SUCCESS"):
        export(str(tmp_path / "lob"), [DAY], str(tmp_path / "csv"))


def test_only_days_rebuilt_successfully_are_exported(tmp_path):
    write_lob(tmp_path / "lob", statuses={DAYS[0]: "ok", DAYS[1]: "row 9: cancel for unknown order 5"})
    src = str(tmp_path / "lob")
    with pytest.raises(ValueError, match="rebuilt instruments \\[\\]"):
        export(src, DAYS, str(tmp_path / "csv"), levels=5)
    with pytest.raises(ValueError, match="not rebuilt successfully"):
        export(src, [DAYS[1]], str(tmp_path / "csv"), levels=5, instrument_id=7)
    assert export(src, [DAYS[0]], str(tmp_path / "csv"), levels=5, instrument_id=7)[DAYS[0]] > 500
    listed_twice = json.loads((tmp_path / "lob" / "_status.json").read_text())
    listed_twice.append({"date": DAYS[0], "instrument_id": 7, "status": "row 3: unknown order"})
    (tmp_path / "lob" / "_status.json").write_text(json.dumps(listed_twice))
    with pytest.raises(ValueError, match="more than once"):
        export(src, [DAYS[0]], str(tmp_path / "csv2"), levels=5)
    os.remove(tmp_path / "lob" / "_status.json")
    with pytest.raises(FileNotFoundError, match="_status.json"):
        export(src, [DAYS[0]], str(tmp_path / "csv2"), levels=5)


def visible_row(msg, ob):
    """Index of a mid-file add (type 1) placed at the best price of its side."""
    m = [line.split(",") for line in msg.read_text().splitlines()]
    b = [line.split(",") for line in ob.read_text().splitlines()]
    for i in range(len(m) // 2, len(m)):
        if m[i][1] == "1":
            best = b[i][2] if m[i][5] == "1" else b[i][0]
            if m[i][4] == best:
                return i
    raise AssertionError("no add at the best price")


@pytest.mark.parametrize("col, change", [(3, lambda v: v + 1),      # size
                                         (4, lambda v: v + 2500),   # price, one tick
                                         (5, lambda v: -v)])        # direction
def test_check_catches_a_corrupted_message(exported, col, change):
    msg, ob, _, _ = exported
    i = visible_row(msg, ob)
    lines = msg.read_text().splitlines()
    parts = lines[i].split(",")
    parts[col] = str(change(int(parts[col])))
    lines[i] = ",".join(parts)
    msg.write_text("\n".join(lines) + "\n")
    assert check(str(msg), levels=5)


def edit_book_row(ob, i, edit):
    lines = ob.read_text().splitlines()
    parts = [int(x) for x in lines[i].split(",")]
    edit(parts)
    lines[i] = ",".join(map(str, parts))
    ob.write_text("\n".join(lines) + "\n")


def swap_ask_levels(p):
    p[4], p[8] = p[8], p[4]


def ask_level_2_empty(p):
    p[4], p[5] = 9_999_999_999, 0


def empty_ask_with_size(p):
    p[16], p[17] = 9_999_999_999, 3


@pytest.mark.parametrize("row, edit, why", [
    (None, lambda p: p.__setitem__(19, -5), "non-positive"),        # deepest bid size below zero
    (None, lambda p: p.__setitem__(1, p[1] + 1), ""),               # size off by one
    (None, swap_ask_levels, "out of order"),
    (None, ask_level_2_empty, "empty level above"),
    (None, empty_ask_with_size, "empty level with a size"),
    (0, lambda p: p.__setitem__(3, 0), "non-positive"),             # first row is validated too
])
def test_check_catches_invalid_book_rows(exported, row, edit, why):
    msg, ob, _, _ = exported
    i = len(ob.read_text().splitlines()) // 3 if row is None else row
    edit_book_row(ob, i, edit)
    problems = check(str(msg), levels=5)
    assert any(r == i and why in reason for r, reason in problems), problems[:3]
