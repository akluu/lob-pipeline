"""Paths that can be local or on S3.

pyarrow opens local paths and s3:// URIs directly; Spark reads S3 through Hadoop's s3a
connector, so the same URI is spelled s3a:// when it is handed to Spark.
"""

import json
import os

import pyarrow.fs as pafs


def resolve(uri):
    """(pyarrow filesystem, path inside it) for a local path or an s3:// URI."""
    if "://" not in uri:
        uri = os.path.abspath(uri)
    return pafs.FileSystem.from_uri(uri)


def for_spark(uri):
    return "s3a://" + uri[len("s3://"):] if uri.startswith("s3://") else uri


def join(uri, *parts):
    return "/".join([uri.rstrip("/"), *parts])


def read_json(uri):
    fs, path = resolve(uri)
    with fs.open_input_stream(path) as f:
        return json.loads(f.read())


def write_json(uri, value):
    fs, path = resolve(uri)
    with fs.open_output_stream(path) as f:
        f.write(json.dumps(value, indent=1).encode())


def list_dates(uri):
    """Dates of the date=YYYY-MM-DD folders of a dataset."""
    fs, path = resolve(uri)
    return sorted(i.base_name.split("=", 1)[1] for i in fs.get_file_info(pafs.FileSelector(path))
                  if i.type == pafs.FileType.Directory and i.base_name.startswith("date="))


def exists(uri):
    fs, path = resolve(uri)
    return fs.get_file_info(path).type != pafs.FileType.NotFound
