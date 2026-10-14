"""The whole pipeline against a local S3 server (moto), compared with the same run on disk.

The first run downloads hadoop-aws and the AWS SDK from Maven Central (several hundred MB)
unless they are already in PySpark's jars folder, as in the Docker image.
"""

import json
import os
import subprocess
import sys

import pytest
import zstandard

from lobpipe.fs import read_json
from lobpipe.transform import et_midnight_ns
from tests.dbn_fixtures import write_dbn
from tests.streams import random_stream

boto3 = pytest.importorskip("boto3")
moto_server = pytest.importorskip("moto.server")

DAYS = ["2025-10-02", "2025-10-03"]
BUCKET = "lobpipe-moto-test"  # a name no one is likely to own, in case a request ever reaches AWS


def write_day(path, day, seed):
    """A compressed DBN file holding one random book stream, like a Databento daily file."""
    midnight = et_midnight_ns(day)
    recs = [{"ts_recv": midnight + r[1] + 10 * r[0], "ts_event": midnight + r[1], "oid": r[5], "px": r[6],
             "sz": r[7], "act": r[3], "side": r[4], "flags": r[8], "seq": r[0]}
            for r in random_stream(seed, n_events=400).rows]
    raw = str(path) + ".raw"
    write_dbn(raw, recs)
    with open(raw, "rb") as src, open(path, "wb") as dst:
        dst.write(zstandard.ZstdCompressor().compress(src.read()))
    os.remove(raw)


@pytest.fixture
def moto():
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    host, port = server.get_host_and_port()
    env = dict(os.environ, AWS_ACCESS_KEY_ID="testing", AWS_SECRET_ACCESS_KEY="testing",
               AWS_DEFAULT_REGION="us-east-1", AWS_ENDPOINT_URL=f"http://{host}:{port}")
    client = boto3.client("s3", endpoint_url=env["AWS_ENDPOINT_URL"], region_name="us-east-1",
                          aws_access_key_id="testing", aws_secret_access_key="testing")
    client.create_bucket(Bucket=BUCKET)
    yield env, client
    server.stop()


def pipeline(env, dbn, mbo, lob, csv):
    for args in (["lobpipe.ingest", "--master", "local[2]", "--out", mbo, dbn],
                 ["lobpipe.spark_job", "--master", "local[2]", "--driver-memory", "1g", "--src", mbo, "--out", lob],
                 ["lobpipe.export", "--src", lob, "--out", csv, "--dates", *DAYS]):
        done = subprocess.run([sys.executable, "-m", *args], env=env, capture_output=True, text=True)
        assert done.returncode == 0, f"{args[0]}:\n{done.stdout[-2000:]}\n{done.stderr[-4000:]}"


def test_pipeline_on_s3_matches_the_run_on_disk(tmp_path, moto, monkeypatch):
    env, client = moto
    local = tmp_path / "dbn"
    local.mkdir()
    for seed, day in enumerate(DAYS, start=1):
        name = f"glbx-mdp3-{day.replace('-', '')}.mbo.dbn.zst"
        write_day(local / name, day, seed)
        client.upload_file(str(local / name), BUCKET, f"dbn/{name}")

    s3 = f"s3://{BUCKET}"
    pipeline(env, f"{s3}/dbn/", f"{s3}/mbo", f"{s3}/lob", str(tmp_path / "csv_s3"))
    pipeline(env, str(local), str(tmp_path / "mbo"), str(tmp_path / "lob"), str(tmp_path / "csv_disk"))

    names = sorted(os.listdir(tmp_path / "csv_disk"))
    assert len(names) == 4 and sorted(os.listdir(tmp_path / "csv_s3")) == names
    for name in names:
        assert (tmp_path / "csv_s3" / name).read_bytes() == (tmp_path / "csv_disk" / name).read_bytes()
    assert os.path.getsize(tmp_path / "csv_disk" / names[0]) > 1000

    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION", "AWS_ENDPOINT_URL"):
        monkeypatch.setenv(k, env[k])
    assert read_json(f"{s3}/mbo/_manifest.json")["dates"] == read_json(str(tmp_path / "mbo" / "_manifest.json"))["dates"]
    assert read_json(f"{s3}/lob/_status.json") == read_json(str(tmp_path / "lob" / "_status.json"))
    # Spark committed both S3 datasets through the S3A committer, which reports itself in _SUCCESS
    for key in ("mbo/_SUCCESS", "lob/_SUCCESS"):
        success = json.loads(client.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        assert success["committer"] == "magic"
