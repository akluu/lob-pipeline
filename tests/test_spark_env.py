import os
import sys

from lobpipe.spark_job import session


def test_spark_map_in_pandas(spark):
    df = spark.range(100).withColumnRenamed("id", "x")

    def double(batches):
        for b in batches:
            yield b.assign(y=b.x * 2)

    out = df.mapInPandas(double, "x long, y long").toPandas().sort_values("x")
    assert out.y.tolist() == [2 * i for i in range(100)]


def test_workers_use_this_interpreter(spark):
    def where(_):
        import sys as worker_sys

        yield worker_sys.prefix

    assert spark.sparkContext.parallelize([0], 1).mapPartitions(where).collect() == [sys.prefix]


def test_session_keeps_an_interpreter_set_by_the_user(spark, monkeypatch):
    monkeypatch.setenv("PYSPARK_PYTHON", "/opt/other/bin/python")
    session(name="lobpipe-tests", master="local[2]", driver_memory="2g")
    assert os.environ["PYSPARK_PYTHON"] == "/opt/other/bin/python"
