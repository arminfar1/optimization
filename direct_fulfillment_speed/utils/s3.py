import json
import os
import os.path
from multiprocessing.pool import ThreadPool

import boto3
import botocore.exceptions

_pool = ThreadPool(processes=20)


def parse_s3_uri(s3_uri):
    bucket = s3_uri.replace("s3://", "").split("/")[0]
    key = s3_uri.replace("s3://", "").replace(bucket + "/", "", 1)
    return bucket, key


def format_s3_uri(bucket, key):
    return "s3://{}/{}".format(bucket, key)


def delete_s3(s3_path, quiet=False):
    if not quiet:
        print("DELETE {" + s3_path + "}")
        # logger.info('DELETE {}'.format(s3_path))
    bucket, key = parse_s3_uri(s3_path)
    _s3_client().delete_object(Bucket=bucket, Key=key)


def write_s3(s3_path, data):
    bucket, key = parse_s3_uri(s3_path)
    _s3_client().put_object(Bucket=bucket, Key=key, Body=data)


def write_s3_json(s3_path, obj, ensure=False, **kwargs):
    write_s3(s3_path, json.dumps(obj, ensure_ascii=ensure, **kwargs))


def write_s3_csv(s3_path, df, **kwargs):
    # pandas defaults this to "true", but for our purposes "false" is a better default
    if "index" not in kwargs:
        kwargs["index"] = False
    write_s3(s3_path, df.to_csv(**kwargs))


def upload_s3_file(s3_path, data, is_file=False):
    # Parse S3 URI
    bucket, key = parse_s3_uri(s3_path)

    # Initialize S3 client
    s3_client = boto3.client("s3")

    # If the data is a file path, upload the file directly
    if is_file:
        filepath = os.path.expanduser(data)
        s3_client.upload_file(Filename=filepath, Bucket=bucket, Key=key)
    else:
        # If the data is not a file path, assume it's in-memory data
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)


def s3_file_exists(s3_path):
    bucket, key = parse_s3_uri(s3_path)
    try:
        _s3_client().head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError:
        return False


def list_all_s3_files(s3_prefix):
    return list(iter_all_s3_files(s3_prefix))


def iter_all_s3_files(s3_prefix):
    bucket, prefix = parse_s3_uri(s3_prefix)
    token = None
    while True:
        if token:
            resp = _s3_client().list_objects_v2(
                Bucket=bucket, Prefix=prefix, ContinuationToken=token
            )
        else:
            resp = _s3_client().list_objects_v2(Bucket=bucket, Prefix=prefix)

        try:
            for item in resp["Contents"]:
                if not item["Key"].endswith(".json"):  # filter only json files
                    continue
                yield item["Key"]
            token = resp["NextContinuationToken"]
        except KeyError:
            break


def upload_s3_directory(s3_path, local_dir):
    """
    Upload the contents of a local directory to a location in S3
    """
    bucket, prefix = parse_s3_uri(s3_path)
    local_dir = os.path.expanduser(local_dir)

    if prefix[-1] != "/":
        prefix += "/"
    if local_dir[-1] != "/":
        local_dir += "/"

    async_results = []
    for root, _, filenames in os.walk(local_dir):
        for filename in filenames:
            filepath = os.path.join(root, filename)
            filekey = prefix + filepath.replace(local_dir, "")
            object_uri = format_s3_uri(bucket, filekey)
            async_results.append(_pool.apply_async(upload_s3_file, (object_uri, filepath)))
    for result in async_results:
        result.get()


def _s3_client():
    """Extracted into its own function in case there's any cross-cutting config we need to apply"""
    return boto3.client("s3")
