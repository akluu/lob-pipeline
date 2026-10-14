FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Spark's S3 connector (same Hadoop version as PySpark's), what it needs at run time, and
# Spark's bridge to the S3A committers
RUN cd "$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && m=https://repo1.maven.org/maven2 \
    && curl -fsSLO $m/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
    && curl -fsSLO $m/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar \
    && curl -fsSLO $m/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar \
    && curl -fsSLO $m/org/wildfly/openssl/wildfly-openssl/2.2.5.Final/wildfly-openssl-2.2.5.Final.jar \
    && curl -fsSLO $m/org/apache/spark/spark-hadoop-cloud_2.13/4.2.0/spark-hadoop-cloud_2.13-4.2.0.jar

COPY lobpipe lobpipe
COPY scripts scripts
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "-m"]
