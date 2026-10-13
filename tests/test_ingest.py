import json
import os

import pandas as pd
import pyarrow.parquet as pq
import pytest

from lobpipe import ingest
from tests.dbn_fixtures import write_dbn

DAY = 86_400_000_000_000
T0 = 1759363200000000000  # 2025-10-02 00:00 UTC


def make_records(t_start, n, oid0):
    recs = [{"ts_recv": t_start, "ts_event": t_start - 5, "oid": 0, "px": 2**63 - 1, "sz": 0,
             "act": "R", "side": "N", "flags": 40, "seq": 0}]
    for i in range(n):
        recs.append({"ts_recv": t_start + 1000 * (i + 1), "ts_event": t_start + 1000 * i, "oid": oid0 + i,
                     "px": 6_750_000_000_000 + 250_000_000 * (i % 7), "sz": 1 + i % 5,
                     "act": "A", "side": "B" if i % 2 else "A", "flags": 128, "seq": 100 + i})
    return recs


@pytest.fixture
def two_files(tmp_path):
    # file 1 crosses UTC midnight, file 2 is the next day
    f1 = make_records(T0 + DAY - 10_000, 20, 6415600000000)
    f2 = make_records(T0 + 2 * DAY, 15, 6415700000000)
    p1, p2 = tmp_path / "a.dbn", tmp_path / "b.dbn"
    write_dbn(p1, f1)
    write_dbn(p2, f2)
    return [str(p1), str(p2)], f1 + f2


def read_all(out):
    return pq.read_table(out).to_pandas().sort_values("row_nr").reset_index(drop=True)


def test_round_trip_and_partitions(tmp_path, two_files, monkeypatch):
    monkeypatch.setattr(ingest, "CHUNK", 3)
    paths, recs = two_files
    out = str(tmp_path / "mbo")
    manifest = ingest.ingest(paths, out)
    df = read_all(out)

    assert df.row_nr.tolist() == list(range(len(recs)))
    assert df.ts_recv.tolist() == [r["ts_recv"] for r in recs]
    assert df.ts_event.tolist() == [r["ts_event"] for r in recs]
    assert df.order_id.tolist() == [r["oid"] for r in recs]
    assert df.price.tolist() == [r["px"] for r in recs]
    assert df["size"].tolist() == [r["sz"] for r in recs]
    assert df.action.tolist() == [r["act"] for r in recs]
    assert df.side.tolist() == [r["side"] for r in recs]
    assert df["flags"].tolist() == [r["flags"] for r in recs]
    assert df.sequence.tolist() == [r["seq"] for r in recs]

    expected_dates = pd.to_datetime(df.ts_recv, unit="ns").dt.strftime("%Y-%m-%d")
    assert (df.date.astype(str) == expected_dates).all()
    assert sorted(manifest["dates"]) == ["2025-10-02", "2025-10-03", "2025-10-04"]
    assert sum(d["rows"] for d in manifest["dates"].values()) == len(recs) == manifest["rows"]
    assert manifest["dates"]["2025-10-02"]["first"]["action"] == "R"
    with open(os.path.join(out, "_manifest.json")) as f:
        assert json.load(f) == manifest


def test_refuses_to_overwrite(tmp_path, two_files):
    paths, _ = two_files
    out = str(tmp_path / "mbo")
    ingest.ingest(paths, out)
    with pytest.raises(FileExistsError):
        ingest.ingest(paths, out)
    ingest.ingest(paths, out, overwrite=True)
    assert os.path.exists(os.path.join(out, "_manifest.json"))


def test_failed_run_leaves_no_dataset(tmp_path, two_files, monkeypatch):
    paths, _ = two_files
    out = str(tmp_path / "mbo")
    calls = {"n": 0}
    real = ingest.to_table

    def flaky(a, first_row):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("disk full")
        return real(a, first_row)

    monkeypatch.setattr(ingest, "CHUNK", 5)
    monkeypatch.setattr(ingest, "to_table", flaky)
    with pytest.raises(RuntimeError):
        ingest.ingest(paths, out)
    assert not os.path.exists(out)
