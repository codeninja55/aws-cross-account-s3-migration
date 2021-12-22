import argparse
import datetime
import json
import logging
import os
import sys
import threading
import time
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from math import ceil
from typing import Any, Generator, List

import boto3
import yaml
from boto3 import Session
from botocore.client import BaseClient
from botocore.config import Config as BotoConfig

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__))
)

sts_client = boto3.client('sts')
aws_default_region = 'ap-southeast-2'
boto_config = BotoConfig(
    region_name=aws_default_region,
    signature_version='s3v4',
    retries={
        'max_attempts': 5,
        'mode': 'standard'
    }
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format='%(asctime)s : %(thread)d : %(name)s : %(levelname)s : %(message)s',
    datefmt='%Y:%m:%d %H:%M:%S'
)
root_logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Data class object container the configurations required for script."""
    config_file_path: str = None
    src_role_arn: str = None
    src_bucket_id: str = None
    dest_bucket_id: str = None
    src_cmk_id: str = None
    dest_cmk_id: str = None
    log_level: str = None


def unix_timestamp() -> float: return time.time()


def timestamp() -> datetime: return datetime.datetime.now()


def chunks(lst: List[Any], n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def get_thread_chunks(
        objects_list: List[Any],
        n: int = 8
) -> Generator[Any, Any, None]:
    """Distribute the list into sub-lists of chunks for each thread."""
    size = ceil(len(objects_list) / n)
    final_chunks = chunks(objects_list, size)
    for idx, chunk in enumerate(final_chunks):
        root_logger.info(f"Thread {idx + 1} :: Items {len(chunk)}")
    return chunks(objects_list, size)


def init_arg_parser() -> ArgumentParser:
    parser: ArgumentParser = argparse.ArgumentParser(
        description='Copy S3 data from source account to destination account.'
    )
    parser.add_argument('--config-file-path',
                        type=str,
                        help='The source account IAM role to assume.',
                        dest='config_file_path',
                        default=None)

    parser.add_argument('--source_role_arn',
                        type=str,
                        help='The source account IAM role to assume.',
                        dest='src_role_arn',
                        default=None)
    parser.add_argument('--source_bucket_id',
                        type=str,
                        help='The source account S3 bucket to copy data to.',
                        dest='src_bucket_id',
                        default=None)
    parser.add_argument(
        '--destination_bucket_id',
        type=str,
        help='The destination account S3 bucket to copy data to.',
        dest='dest_bucket_id',
        default=None
    )
    parser.add_argument(
        '--source_cmk_id',
        type=str,
        help='The source account CMK KMS key arn use for decryption.',
        dest='src_cmk_id',
        default=None
    )
    parser.add_argument(
        '--destination_cmk_id',
        type=str,
        help='The destination account CMK KMS key arn use for encryption.',
        dest='dest_cmk_id',
        default=None
    )
    parser.add_argument(
        '--log_level',
        type=str,
        help='The logging level',
        dest='log_level',
        default=None
    )
    return parser


def parse_config_file(file_path: str) -> Config:
    """Parse a YAML file for configurations and return as a Config object."""
    cfg = Config()
    path = os.path.join(__location__, file_path)
    root_logger.info(f"Reading YAML configuration from :: {path}")

    with open(path, "r") as stream:
        config_yaml = yaml.safe_load(stream)
        cfg.config_file_path = path
        if 'source_role_arn' in config_yaml:
            cfg.src_role_arn = config_yaml['source_role_arn']

        if 'source_bucket_id' in config_yaml:
            cfg.src_bucket_id = config_yaml['source_bucket_id']

        if 'destination_bucket_id' in config_yaml:
            cfg.dest_bucket_id = config_yaml['destination_bucket_id']

        if 'source_cmk_id' in config_yaml:
            cfg.src_cmk_id = config_yaml['source_cmk_id']

        if 'destination_cmk_id' in config_yaml:
            cfg.dest_cmk_id = config_yaml['destination_cmk_id']

        if 'log_level' in config_yaml:
            cfg.log_level = config_yaml['log_level']

    return cfg


def get_assume_role_session(role_arn: str) -> Session:
    session_name = f"aws-cross-account-s3-migration_{unix_timestamp()}"
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name
    )
    credentials = assumed_role_object['Credentials']

    return boto3.session.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name=aws_default_region,
    )


def list_bucket_keys(s3, bucket_name: str) -> List[str]:
    src_bucket = s3.Bucket(bucket_name)
    objects = []
    for obj in src_bucket.objects.all():
        objects.append(obj.key)
    return objects


def copy_data(
        s3_client: BaseClient,
        src_bucket_name: str,
        dest_bucket_name: str,
        dest_kms_key_id: str,
        bucket_objects: List[str]
) -> tuple[int, list]:
    logger = logging.getLogger(f"copy_data_{threading.get_ident() }")
    completed = []
    for key in bucket_objects:
        response = s3_client.copy_object(
            Bucket=dest_bucket_name,
            Key=key,
            CopySource={
                'Bucket': src_bucket_name,
                'Key': key
            },
            ACL='bucket-owner-full-control',
            TaggingDirective='COPY',
            MetadataDirective='COPY',
            ServerSideEncryption='aws:kms',
            SSEKMSKeyId=dest_kms_key_id,
            BucketKeyEnabled=True,
        )

        resp_metadata = response.get('ResponseMetadata')
        status_code = resp_metadata.get('HTTPStatusCode', 500)

        output = {
            "StatusCode": status_code,
            "RequestId": resp_metadata.get('RequestId', ''),
            "Date": resp_metadata.get('HTTPHeaders', {}).get('date', ''),
            "RetryAttempts": resp_metadata.get('RetryAttempts', 0),
            "VersionId": response.get('VersionId', ''),
            "ETag": response.get('CopyObjectResult', {}).get('ETag', ''),
        }

        if status_code in range(200, 299):
            logger.info(f"Copy Success {status_code} :: {key} => "
                        f"{json.dumps(output)}")
        else:
            logger.error(f"Copy Error {status_code} :: {key} => {response}")

        completed.append(response)
    return len(completed), completed


def concurrent_copy_object(
        session: Session,
        cfg: Config,
        key_chunks: List[List[Any]],
        threads: int = 8
) -> None:
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []

        for keys_to_copy_list in key_chunks:
            futures.append(
                executor.submit(
                    copy_data,
                    s3_client=session.client('s3', config=boto_config),
                    src_bucket_name=cfg.src_bucket_id,
                    dest_bucket_name=cfg.dest_bucket_id,
                    dest_kms_key_id=cfg.dest_cmk_id,
                    bucket_objects=keys_to_copy_list
                )
            )

        for future in as_completed(futures):
            try:
                completed, responses = future.result()
                root_logger.info(f"Completed :: {completed} objects")
            except Exception as e:
                root_logger.error(f"Copy Error :: {e} :: {sys.exc_info()}")


# And away we go ...
if __name__ == '__main__':
    config = Config()
    args = init_arg_parser().parse_args(namespace=config)

    if args.config_file_path is not None:
        config = parse_config_file(config.config_file_path)

    root_logger.info(f'Configs ::\n{yaml.dump(config)}')

    current_session = get_assume_role_session(config.src_role_arn)
    _s3_resource = current_session.resource('s3', config=boto_config)
    # Test we have access first by checking how many items there are.
    root_logger.info(f"Calculating size of source bucket ::"
                     f" [{config.src_bucket_id}] ...")

    src_bucket_keys = list_bucket_keys(_s3_resource, config.src_bucket_id)
    root_logger.info(f'{len(src_bucket_keys)} objects')

    _threads = 8
    root_logger.info(f"Splitting keys into chunks for number of threads: "
                     f"{_threads} ...")

    _key_chunks = [i for i in get_thread_chunks(src_bucket_keys, _threads)]

    root_logger.info(f"Copying data from [{config.src_bucket_id}] to "
                     f"[{config.dest_bucket_id}] ...")

    concurrent_copy_object(current_session, config, _key_chunks, _threads)

    # Verify we have the same number of objects in the destination bucket
    root_logger.info(f"Calculating size of destination bucket "
                     f"[{config.dest_bucket_id}] ...")
    dest_bucket_keys = list_bucket_keys(_s3_resource, config.dest_bucket_id)
    root_logger.info(f'{len(dest_bucket_keys)} objects')
