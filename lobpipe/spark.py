"""The Spark session used by ingest and spark_job, with S3 support when a path needs it."""

import os
import sys

import pyspark
from pyspark.sql import SparkSession

# Hadoop's S3 connector (same Hadoop version as PySpark 4.2) and Spark's bridge to its committers
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.5.0"
SPARK_CLOUD = "https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.13/4.2.0/spark-hadoop-cloud_2.13-4.2.0.jar"
JARS = os.path.join(os.path.dirname(pyspark.__file__), "jars")


def session(name="lobpipe", master="local[4]", driver_memory="4g", s3=False):
    """Under spark-submit, spark-submit's master, memory, Python and jars are kept."""
    builder = (SparkSession.builder.appName(name)
               .config("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
               .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false"))
    if "PYSPARK_GATEWAY_PORT" not in os.environ:  # started with plain python
        # PySpark started from a running interpreter ignores spark.pyspark.python, so the
        # workers are pointed at this interpreter (and its packages) through the environment
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        builder = builder.master(master).config("spark.driver.memory", driver_memory)
        if s3 and not os.path.exists(os.path.join(JARS, "hadoop-aws-3.5.0.jar")):  # the Docker image has it
            builder = builder.config("spark.jars.packages", HADOOP_AWS)
        if s3 and not os.path.exists(os.path.join(JARS, "spark-hadoop-cloud_2.13-4.2.0.jar")):
            builder = builder.config("spark.jars", SPARK_CLOUD)
    return with_s3(builder).getOrCreate() if s3 else builder.getOrCreate()


def with_s3(builder):
    """Credentials come from the usual AWS variables or the machine's role."""
    # S3 has no rename, so instead of Spark's write-then-rename commit, the S3A "magic"
    # committer uploads files directly and makes them visible when the job commits
    builder = (builder.config("spark.hadoop.fs.s3a.committer.name", "magic")
               .config("spark.sql.sources.commitProtocolClass",
                       "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
               .config("spark.sql.parquet.output.committer.class",
                       "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"))
    if os.environ.get("AWS_ENDPOINT_URL"):  # an S3-compatible store instead of AWS
        builder = (builder.config("spark.hadoop.fs.s3a.endpoint", os.environ["AWS_ENDPOINT_URL"])
                   .config("spark.hadoop.fs.s3a.path.style.access", "true"))
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", region)
    return builder
