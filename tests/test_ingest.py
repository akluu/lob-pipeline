import json
import os

import pandas as pd
import pyarrow.parquet as pq
import pytest

from lobpipe import ingest
from tests.dbn_fixtures import write_dbn

DAY = 86_400_000_000_000
T0 = 1759363200000000000  # 2025-10-02 00:00 UTC


def make_records(t_start, n, oid0, iid=7):
    recs = [{"ts_recv": t_start, "ts_event": t_start - 5, "oid": 0, "px": 2**63 - 1, "sz": 0,
             "act": "R", "side": "N", "flags": 40, "seq": 0, "iid": iid}]
    for i in range(n):
        recs.append({"ts_recv": t_start + 1000 * (i + 1), "ts_event": t_start + 1000 * i, "oid": oid0 + i,
                     "px": 6_750_000_000_000 + 250_000_000 * (i % 7), "sz": 1 + i % 5,
                     "act": "A", "side": "B" if i % 2 else "A", "flags": 128, "seq": 100 + i, "iid": iid})
    return recs


@pytest.fixture
def two_files(tmp_path):
    # the earlier file gets the later name, so order has to come from the headers
    early = make_records(T0 + DAY - 10_000, 20, 6415600000000)  # crosses UTC midnight
    late = make_records(T0 + 2 * DAY, 15, 6415700000000)
    folder = tmp_path / "dbn"
    folder.mkdir()
    write_dbn(folder / "b.dbn", early)
    write_dbn(folder / "a.dbn", late)
    (folder / "notes.txt").write_text("not a dbn file")
    return str(folder), early + late


def read_all(out):
    return pq.read_table(out).to_pandas().sort_values("row_nr").reset_index(drop=True)


def test_round_trip_partitions_and_order(spark, tmp_path, two_files):
    folder, recs = two_files
    out = str(tmp_path / "mbo")
    manifest = ingest.ingest(spark, [folder], out, chunk=3)
    df = read_all(out)

    assert len(df) == len(recs)
    assert df.ts_recv.tolist() == [r["ts_recv"] for r in recs]
    assert df.ts_event.tolist() == [r["ts_event"] for r in recs]
    assert df.order_id.tolist() == [r["oid"] for r in recs]
    assert df.price.tolist() == [r["px"] for r in recs]
    assert df["size"].tolist() == [r["sz"] for r in recs]
    assert df.action.tolist() == [r["act"] for r in recs]
    assert df.side.tolist() == [r["side"] for r in recs]
    assert df["flags"].tolist() == [r["flags"] for r in recs]
    assert df.sequence.tolist() == [r["seq"] for r in recs]
    assert df.instrument_id.tolist() == [7] * len(recs)
    assert df.row_nr.tolist() == [i for i in range(21)] + [(1 << 40) | i for i in range(16)]

    expected_dates = pd.to_datetime(df.ts_recv, unit="ns").dt.strftime("%Y-%m-%d")
    assert (df.date.astype(str) == expected_dates).all()
    assert [f["uri"] for f in manifest["files"]] == [os.path.join(folder, "b.dbn"), os.path.join(folder, "a.dbn")]
    assert [f["rows"] for f in manifest["files"]] == [21, 16]
    assert sorted(manifest["dates"]) == ["2025-10-02", "2025-10-03", "2025-10-04"]
    assert manifest["dates"]["2025-10-04"] == {"rows": 16, "instruments": {"7": 16}}
    assert manifest["rows"] == len(recs)
    with open(os.path.join(out, "_manifest.json")) as f:
        assert json.load(f) == manifest


def test_refuses_to_overwrite(spark, tmp_path, two_files):
    folder, _ = two_files
    out = str(tmp_path / "mbo")
    ingest.ingest(spark, [folder], out)
    with pytest.raises(FileExistsError):
        ingest.ingest(spark, [folder], out)
    manifest = ingest.ingest(spark, [os.path.join(folder, "a.dbn")], out, overwrite=True)
    assert manifest["rows"] == 16
    assert len(read_all(out)) == 16


def test_files_that_overlap_for_one_instrument_are_rejected(spark, tmp_path):
    folder = tmp_path / "dbn"
    folder.mkdir()
    write_dbn(folder / "a.dbn", make_records(T0, 10, 1))
    write_dbn(folder / "b.dbn", make_records(T0 + 5_000, 10, 100))  # same instrument, same hours
    out = str(tmp_path / "mbo")
    with pytest.raises(ValueError, match="overlap"):
        ingest.ingest(spark, [str(folder)], out)
    assert not os.path.exists(os.path.join(out, "_manifest.json"))


def test_files_that_share_a_timestamp_for_one_instrument_are_rejected(spark, tmp_path):
    folder = tmp_path / "dbn"
    folder.mkdir()
    first = make_records(T0, 10, 1)
    second = make_records(first[-1]["ts_recv"], 10, 100)
    second[0]["act"], second[0]["flags"] = "A", 128  # a continuation, not a new snapshot
    write_dbn(folder / "a.dbn", first)
    write_dbn(folder / "b.dbn", second)
    with pytest.raises(ValueError, match="overlap"):
        ingest.ingest(spark, [str(folder)], str(tmp_path / "mbo"))


def test_instrument_ids_beyond_int32_are_rejected(spark, tmp_path):
    write_dbn(tmp_path / "a.dbn", make_records(T0, 3, 1, iid=2**31))
    with pytest.raises(Exception, match="not in range"):
        ingest.ingest(spark, [str(tmp_path / "a.dbn")], str(tmp_path / "mbo"))


def test_other_instruments_may_overlap(spark, tmp_path):
    folder = tmp_path / "dbn"
    folder.mkdir()
    write_dbn(folder / "a.dbn", make_records(T0, 10, 1, iid=7))
    write_dbn(folder / "b.dbn", make_records(T0 + 5_000, 10, 100, iid=9))
    manifest = ingest.ingest(spark, [str(folder)], str(tmp_path / "mbo"))
    assert manifest["dates"]["2025-10-02"]["instruments"] == {"7": 11, "9": 11}


def test_records_out_of_time_order_fail(spark, tmp_path):
    recs = make_records(T0, 10, 1)
    recs[4]["ts_recv"], recs[5]["ts_recv"] = recs[5]["ts_recv"], recs[4]["ts_recv"]
    write_dbn(tmp_path / "a.dbn", recs)
    with pytest.raises(Exception, match="not in time order"):
        ingest.ingest(spark, [str(tmp_path / "a.dbn")], str(tmp_path / "mbo"))


def test_same_file_twice_is_rejected(spark, tmp_path, two_files):
    folder, _ = two_files
    with pytest.raises(ValueError, match="listed twice"):
        ingest.ingest(spark, [folder, os.path.join(folder, "a.dbn")], str(tmp_path / "mbo"))
