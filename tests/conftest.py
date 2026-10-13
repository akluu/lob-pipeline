import os

import pytest


@pytest.fixture(scope="session")
def spark():
    from lobpipe.spark_job import session

    # start the way the job does, with nothing set in the environment
    os.environ.pop("PYSPARK_PYTHON", None)
    spark = session(name="lobpipe-tests", master="local[2]", driver_memory="2g")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    yield spark
    spark.stop()
