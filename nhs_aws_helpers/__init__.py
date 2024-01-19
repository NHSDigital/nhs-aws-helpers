import asyncio
import contextlib
import gzip
import inspect
import io
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, reduce, wraps
from typing import (
    IO,
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
from urllib import parse
from uuid import uuid4

import boto3
import botocore.credentials
import botocore.session
from boto3.dynamodb.types import TypeDeserializer
from boto3.s3.transfer import TransferConfig
from boto3.session import Session
from botocore.config import Config
from botocore.exceptions import ClientError, UnknownServiceError
from botocore.response import StreamingBody
from botocore.retries import quota
from botocore.retries.base import BaseRetryBackoff
from botocore.retries.standard import (
    DEFAULT_MAX_ATTEMPTS,
    RetryContext,
    RetryEventAdapter,
    RetryHandler,
    RetryPolicy,
    RetryQuotaChecker,
    StandardRetryConditions,
)
from mypy_boto3_athena import AthenaClient
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
from mypy_boto3_dynamodb.type_defs import KeysAndAttributesTypeDef
from mypy_boto3_events import EventBridgeClient
from mypy_boto3_firehose import FirehoseClient
from mypy_boto3_kms.client import KMSClient
from mypy_boto3_lambda.client import LambdaClient
from mypy_boto3_logs.client import CloudWatchLogsClient
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.service_resource import Bucket, Object, S3ServiceResource
from mypy_boto3_s3.type_defs import (
    CompletedPartTypeDef,
    DeleteMarkerEntryTypeDef,
    DeleteTypeDef,
    GetObjectOutputTypeDef,
    ObjectVersionTypeDef,
)
from mypy_boto3_secretsmanager.client import SecretsManagerClient
from mypy_boto3_sns.client import SNSClient
from mypy_boto3_sqs import SQSServiceResource
from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_ssm.client import SSMClient
from mypy_boto3_stepfunctions import SFNClient

from nhs_aws_helpers.common import run_in_executor
from nhs_aws_helpers.s3_object_writer import S3ObjectWriter

s3re = re.compile(r"^(s3[an]?)://([^/]+)/(.+)$", re.IGNORECASE)


def s3_build_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def s3_split_path(s3uri: str) -> Tuple[str, str, str]:
    match = s3re.match(s3uri)
    if not match:
        raise ValueError(f"Not a s3 uri: {s3uri}")
    scheme, bucket, key = match.groups()
    return scheme, bucket, key


_default_configs: Dict[str, Config] = {}

_pre_configure: Optional[Callable[[str, str, Optional[Config]], Optional[Config]]] = None


def register_config_default(service: str, config: Config):
    """
        register default config, that will be used or merged for a given aws service
    Args:
        service: single service e.g s3, dynamodb .. or * for all services
        config: boto config
    """
    _default_configs[service] = config


_post_create_client: Optional[Callable[[str, str, object], Any]] = None


def post_create_client(post_create: Callable[[str, str, object], Any]):
    """
        hook to allow configuration of a created service after created
    Args:
        post_create: callable, takes args  aws service, resource/client, created client
    """
    global _post_create_client
    _post_create_client = post_create


def _aws(
    aws_module: str,
    boto_method: Literal["client", "resource"],
    session: Optional[Session],
    config: Optional[Config],
):
    """
    Core function to construct AWS client/resource objects.

    `session` should be:
    - None
    - a session initialized to ptl
    - a session initialized to prod
    see sessions.py
    """
    session_or_module = boto3 if session is None else session

    cls = getattr(session_or_module, boto_method)

    default_config = _default_configs.get(aws_module, _default_configs.get("*"))
    if default_config:
        config = config.merge(default_config) if config else default_config

    aws_endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    aws_region = os.environ.get("AWS_REGION", "eu-west-2")

    kwargs = {"region_name": aws_region, "config": config}
    if aws_endpoint_url is not None:  # For localstack (None = AWS)
        kwargs.update(
            {
                "endpoint_url": aws_endpoint_url,
                "aws_access_key_id": "abc",
                "aws_secret_access_key": "123",
                "verify": False,  # type: ignore[dict-item]
            }
        )
    created = cls(aws_module, **kwargs)

    if _post_create_client:
        _post_create_client(aws_module, boto_method, created)

    return created


def cloudwatchlogs_client(session: Optional[Session] = None, config: Optional[Config] = None) -> CloudWatchLogsClient:
    return _aws("logs", "client", session, config)  # type: ignore[no-any-return]


def athena_client(session: Optional[Session] = None, config: Optional[Config] = None) -> AthenaClient:
    return _aws("athena", "client", session, config)  # type: ignore[no-any-return]


def lambdas(session: Optional[Session] = None, config: Optional[Config] = None) -> LambdaClient:
    return _aws("lambda", "client", session, config)  # type: ignore[no-any-return]


def stepfunctions(session: Optional[Session] = None, config: Optional[Config] = None) -> SFNClient:
    return _aws("stepfunctions", "client", session, config)  # type: ignore[no-any-return]


def s3_resource(session: Optional[Session] = None, config: Optional[Config] = None) -> S3ServiceResource:
    return _aws("s3", "resource", session, config)  # type: ignore[no-any-return]


def s3_client(session: Optional[Session] = None, config: Optional[Config] = None) -> S3Client:
    return _aws("s3", "client", session, config)  # type: ignore[no-any-return]


def register_retry_handler(
    client_or_resource: Union[S3ServiceResource, S3Client],
    on_error: Optional[Callable] = None,
    on_backoff: Optional[Callable[[int, RetryContext], Any]] = None,
):
    if not hasattr(client_or_resource.meta, "events"):
        # it's a resource, so get the client and work with that
        client_or_resource = cast(S3ServiceResource, client_or_resource).meta.client

    client = cast(S3Client, client_or_resource)

    retry_quota = RetryQuotaChecker(quota.RetryQuota())

    max_attempts = client.meta.config.retries.get("total_max_attempts", DEFAULT_MAX_ATTEMPTS)

    service_id = client.meta.service_model.service_id
    service_event_name = service_id.hyphenize()

    handler = RetryHandler(
        retry_policy=RetryPolicy(
            retry_checker=StandardRetryConditions(max_attempts=max_attempts),
            retry_backoff=CustomBackoff(on_backoff=on_backoff),
        ),
        retry_event_adapter=RetryEventAdapter(),
        retry_quota=retry_quota,  # type: ignore[arg-type]
    )

    unique_id = f"retry-config-{service_event_name}"

    # Unregister the retry config that was created by default
    client.meta.events.unregister(
        f"needs-retry.{service_event_name}",
        unique_id=unique_id,
    )

    # Re-register with our own handler
    client.meta.events.register(
        f"needs-retry.{service_event_name}",
        handler.needs_retry,
        unique_id=unique_id,
    )

    def on_response_received(**kwargs):
        if on_error is not None and kwargs.get("exception") or kwargs.get("parsed_response", {}).get("Error"):
            assert on_error
            on_error(**kwargs)

    if on_error:
        client.meta.events.register(f"response-received.{service_event_name}", on_response_received)

    return handler


class CustomBackoff(BaseRetryBackoff):
    _BASE = 2
    _MAX_BACKOFF = 10

    def __init__(
        self,
        max_backoff=_MAX_BACKOFF,
        rand=random.random,
        on_backoff: Optional[Callable[[int, RetryContext], Any]] = None,
    ):
        self._base = self._BASE
        self._max_backoff = max_backoff
        self._random = rand
        self._on_backoff = on_backoff

    def delay_amount(self, context: RetryContext):
        # The context.attempt_number is a 1-based value, but we have to calculate the delay based on aa 0-based value.
        # First delay set to 0 to retry immediately - the remainder are jittered by the random.
        backoff = min(
            0 if context.attempt_number == 1 else (self._random() * (self._base ** (context.attempt_number - 1))),
            self._max_backoff,
        )

        if self._on_backoff:
            self._on_backoff(backoff, context)

        return backoff


def dynamodb(session: Optional[Session] = None, config: Optional[Config] = None) -> DynamoDBServiceResource:
    return _aws("dynamodb", "resource", session, config)  # type: ignore[no-any-return]


def dynamodb_client(session: Optional[Session] = None, config: Optional[Config] = None) -> DynamoDBClient:
    return _aws("dynamodb", "client", session, config)  # type: ignore[no-any-return]


def ddb_table(table_name: str, session: Optional[Session] = None, config: Optional[Config] = None) -> Table:
    return dynamodb(session=session, config=config).Table(table_name)


def sqs_client(session: Optional[Session] = None, config: Optional[Config] = None) -> SQSClient:
    return _aws("sqs", "client", session, config)  # type: ignore[no-any-return]


def sqs_resource(session: Optional[Session] = None, config: Optional[Config] = None) -> SQSServiceResource:
    return _aws("sqs", "resource", session, config)  # type: ignore[no-any-return]


def ssm_client(session: Optional[Session] = None, config: Optional[Config] = None) -> SSMClient:
    return _aws("ssm", "client", session, config)  # type: ignore[no-any-return]


def firehose_client(session: Optional[Session] = None, config: Optional[Config] = None) -> FirehoseClient:
    return _aws("firehose", "client", session, config)  # type: ignore[no-any-return]


def kms_client(session: Optional[Session] = None, config: Optional[Config] = None) -> KMSClient:
    return _aws("kms", "client", session, config)  # type: ignore[no-any-return]


def secrets_client(session: Optional[Session] = None, config: Optional[Config] = None) -> SecretsManagerClient:
    return _aws("secretsmanager", "client", session, config)  # type: ignore[no-any-return]


def secret_value(name: str, session: Optional[Session] = None, config: Optional[Config] = None) -> str:
    return secrets_client(session=session, config=config).get_secret_value(SecretId=name)["SecretString"]


def secret_binary_value(name: str, session: Optional[Session] = None, config: Optional[Config] = None) -> bytes:
    return secrets_client(session=session, config=config).get_secret_value(SecretId=name)["SecretBinary"]


def ssm_parameter(
    name: str, decrypt=False, session: Optional[Session] = None, config: Optional[Config] = None
) -> Union[str, List[str]]:
    ssm = ssm_client(session=session, config=config)
    value = cast(Union[str, List[str]], ssm.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"])
    return value


def sns_client(session: Optional[Session] = None, config: Optional[Config] = None) -> SNSClient:
    return _aws("sns", "client", session, config)  # type: ignore[no-any-return]


def events_client(session: Optional[Session] = None, config: Optional[Config] = None) -> EventBridgeClient:
    return _aws("events", "client", session, config)  # type: ignore[no-any-return]


def s3_bucket(bucket: str, session: Optional[Session] = None, config: Optional[Config] = None) -> Bucket:
    return s3_resource(session=session, config=config).Bucket(bucket)


def s3_object(
    bucket_or_url: str, key: Optional[str] = None, session: Optional[Session] = None, config: Optional[Config] = None
) -> Object:
    if key is not None:
        bucket = bucket_or_url
    else:
        url_parsed = parse.urlparse(bucket_or_url)
        bucket = url_parsed.netloc
        key = url_parsed.path.lstrip("/")

    return s3_resource(session=session, config=config).Object(bucket, key)


def s3_get_tags(
    bucket: str,
    key: str,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
    client: Optional[S3Client] = None,
) -> Dict[str, str]:
    client = client or s3_resource(session=session, config=config).meta.client
    result = client.get_object_tagging(Bucket=bucket, Key=key)
    tags = {pair["Key"]: pair["Value"] for pair in result["TagSet"]}
    return tags


def s3_replace_tags(
    bucket: str,
    key: str,
    tags: Dict[str, str],
    session: Optional[Session] = None,
    config: Optional[Config] = None,
    client: Optional[S3Client] = None,
):
    client = client or s3_resource(session=session, config=config).meta.client

    client.put_object_tagging(
        Bucket=bucket, Key=key, Tagging={"TagSet": [{"Key": k, "Value": v} for k, v in tags.items()]}
    )


def s3_update_tags(
    bucket: str,
    key: str,
    tags: Dict[str, str],
    session: Optional[Session] = None,
    config: Optional[Config] = None,
    client: Optional[S3Client] = None,
) -> Dict[str, str]:
    client = client or s3_resource(session=session, config=config).meta.client

    result = client.get_object_tagging(Bucket=bucket, Key=key)
    existing_tags = {pair["Key"]: pair["Value"] for pair in result["TagSet"]}
    existing_tags.update(tags)

    client.put_object_tagging(
        Bucket=bucket, Key=key, Tagging={"TagSet": [{"Key": k, "Value": v} for k, v in existing_tags.items()]}
    )
    return existing_tags


def s3_get_all_keys(
    bucket: str,
    prefix: str,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
    client: Optional[S3Client] = None,
) -> List[str]:
    client = client or s3_resource(session=session, config=config).meta.client
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    keys = []
    for page in page_iterator:
        keys.extend([content["Key"] for content in page["Contents"]])

    return keys


def s3_delete_keys(
    keys: Iterable[str], bucket: str, session: Optional[Session] = None, config: Optional[Config] = None
):
    buck = s3_bucket(bucket, session=session, config=config)

    all_keys = list(keys)

    if not all_keys:
        return []

    deleted = []

    batch = all_keys[:999]
    remaining = all_keys[999:]
    while batch:
        buck.delete_objects(Delete=cast(DeleteTypeDef, {"Objects": [{"Key": key} for key in batch]}))
        deleted.extend(batch)
        batch = remaining[:999]
        remaining = remaining[999:]

    return deleted


def s3_delete_versioned_keys(
    keys: Iterable[Tuple[str, str]], bucket: str, session: Optional[Session] = None, config: Optional[Config] = None
):
    # delete specific versions, rather than deleting "objects" and adding delete_marker
    buck = s3_bucket(bucket, session=session, config=config)

    all_keys = list(keys)
    deleted = []

    # batch deleting can only handle certain sized batches
    batch = all_keys[:999]

    remaining = all_keys[999:]
    while batch:
        buck.delete_objects(
            Delete=cast(
                DeleteTypeDef, {"Objects": [{"Key": key, "VersionId": version_id} for key, version_id in batch]}
            )
        )
        deleted.extend(batch)
        batch = remaining[:999]
        remaining = remaining[999:]

    return deleted


def s3_delete_all_versions(
    bucket: str,
    prefix: Optional[str] = None,
    predicate=None,
    dry_run: bool = True,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
):
    client = s3_client(session=session, config=config)
    object_response_paginator = client.get_paginator("list_object_versions")

    delete_marker_list = []
    version_list = []

    paginator = (
        object_response_paginator.paginate(Bucket=bucket, Prefix=prefix)
        if prefix
        else object_response_paginator.paginate(Bucket=bucket)
    )

    def filter_items(items: Sequence[Union[DeleteMarkerEntryTypeDef, ObjectVersionTypeDef]]):
        for item in items:
            if predicate and not predicate(item["Key"]):
                continue
            yield {"Key": item["Key"], "VersionId": item["VersionId"]}

    for object_response_itr in paginator:
        delete_marker_list.extend(filter_items(object_response_itr.get("DeleteMarkers") or []))
        version_list.extend(filter_items(object_response_itr.get("Versions") or []))

    if dry_run:
        print("dry run found:")
        print(delete_marker_list)
        print(version_list)
        return

    for i in range(0, len(delete_marker_list), 1000):
        response = client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": delete_marker_list[i : i + 1000], "Quiet": True},  # type: ignore[typeddict-item]
        )
        print(response)

    for i in range(0, len(version_list), 1000):
        response = client.delete_objects(
            Bucket=bucket, Delete={"Objects": version_list[i : i + 1000], "Quiet": True}  # type: ignore[typeddict-item]
        )
        print(response)


def s3_ls(
    uri: str,
    recursive: bool = True,
    predicate: Optional[Callable[[str], bool]] = None,
    versioning: bool = False,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
) -> Generator[Object, None, None]:
    _, bucket, path = s3_split_path(uri)

    yield from s3_list_bucket(
        bucket, path, recursive=recursive, predicate=predicate, versioning=versioning, session=session, config=config
    )


def s3_list_bucket(
    bucket: str,
    prefix: str,
    recursive: bool = True,
    predicate: Optional[Callable[[str], bool]] = None,
    versioning: bool = False,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
) -> Generator[Object, None, None]:
    """list contents of S3 bucket based on filter criteria and versioning flag

    Args:
        bucket (str): bucket name to list contents of
        prefix (str): prefix to filter on
        recursive (bool): whether to recurse or not
        predicate (Callable[[str], bool]): predicate function to filter results on
        versioning (bool): whether to return objects or versions
        session (Session): optional existing session r
        config (Config): optional botocore config

    Returns:
        Generator[object, None, None]: resulting objects or versions
    """
    buck = s3_bucket(bucket, session=session, config=config)
    bc_objects = buck.object_versions if versioning else buck.objects

    for s3_obj in bc_objects.filter(Prefix=prefix, Delimiter="" if recursive else "/"):
        if predicate and not predicate(s3_obj.key):
            continue
        yield s3_obj


def s3_list_delete_markers(
    bucket: str,
    prefix: str,
    recursive: bool = True,
    predicate: Optional[Callable[[str], bool]] = None,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
    client: Optional[S3Client] = None,
) -> Generator[DeleteMarkerEntryTypeDef, None, None]:
    """list contents of S3 bucket based on filter criteria and versioning flag

    Args:
        bucket (str): bucket name to list contents of
        prefix (str): prefix to filter on
        recursive (bool): whether to recurse or not
        predicate (Callable[[str], bool]): predicate function to filter results on
        session (Session): optional existing session r
        config (Config): optional botocore config
        client (S3Client): optional pre created client
    Returns:
        Generator[object, None, None]: resulting objects or versions
    """
    client = client or s3_client(session=session, config=config)
    paginator = client.get_paginator("list_object_versions").paginate(
        Bucket=bucket, Prefix=prefix, Delimiter="" if recursive else "/"
    )
    for page in paginator:
        delete_markers = page.get("DeleteMarkers", [])
        for delete_marker in delete_markers:
            if predicate and not predicate(delete_marker["Key"]):
                continue
            yield delete_marker


def s3_put(bucket: Bucket, key: str, body: Union[bytes, str], encoding: str = "utf-8", **kwargs) -> Object:
    obj = bucket.Object(key)
    encoded = cast(bytes, body)
    if isinstance(body, str):
        encoded = cast(str, body).encode(encoding=encoding)

    obj.put(Body=encoded, **kwargs)
    return obj


def s3_list_prefixes(s3_path: str, session: Optional[Session] = None, config: Optional[Config] = None) -> List[str]:
    _na, bucket, prefix = s3_split_path(s3_path)

    if not prefix.endswith("/"):
        prefix += "/"
    client = s3_client(session=session, config=config)
    result = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")

    return [o["Prefix"].replace(prefix, "").strip("/") for o in result.get("CommonPrefixes", [])]


def s3_list_folders(bucket_name: str, bucket_prefix: str, page_size: int = 100) -> List[str]:
    paginator = s3_client().get_paginator("list_objects")
    folders = []
    iterator = paginator.paginate(
        Bucket=bucket_name, Prefix=bucket_prefix, Delimiter="/", PaginationConfig={"PageSize": page_size}
    )
    for response_data in iterator:
        prefixes = response_data.get("CommonPrefixes", [])
        for prefix in prefixes:
            prefix_name = prefix["Prefix"]
            if prefix_name.endswith("/"):
                folders.append(prefix_name.rstrip("/"))

    return folders


def s3_uri_get_size(s3_uri: str, session: Optional[Session] = None, config: Optional[Config] = None) -> int:
    """
    Get the size in bytes of the file(s) at/under the provided uri
    """
    _, bucket, key = s3_split_path(s3_uri)
    return s3_get_size(bucket, key, session, config)  # type: ignore[no-any-return]


def s3_get_size(
    bucket_name: str, prefix: str, session: Optional[Session] = None, config: Optional[Config] = None
) -> int:
    """
    Get the size in bytes of the file(s) at/under the provided prefix
    """
    size = sum(
        s3_obj.size for s3_obj in s3_bucket(bucket_name, session=session, config=config).objects.filter(Prefix=prefix)
    )
    return size


def s3_upload_multipart_from_copy(
    obj: Object, parts_keys: Sequence[str], executor_type: Type[ThreadPoolExecutor] = ThreadPoolExecutor, **kwargs
):
    multipart_upload = obj.initiate_multipart_upload(**kwargs)

    outstanding_part_keys = set(parts_keys)

    # We expect timeouts and other intermittents (hence multiple attempts) but we don't want to log "failed" actions
    # when that happens. We register ClientErrors as "expected", so they are captured with an "error" status instead.
    # Any non-ClientError exceptions will be logged as "failed"
    def copy_part(part_key: str, _attempt: int):
        index = parts_keys.index(part_key) + 1
        multipart_upload.Part(index).copy_from(  # type: ignore[arg-type]
            CopySource={"Bucket": obj.bucket_name, "Key": part_key}
        )

        return part_key

    with executor_type() as executor:
        attempt = 0
        max_retries = 4

        while outstanding_part_keys:
            attempt += 1
            futures = [executor.submit(copy_part, key, attempt) for key in outstanding_part_keys]
            for future in as_completed(futures):
                with contextlib.suppress(BaseException):
                    # Exception will be logged on copy_part()
                    # This will raise an exception if a future has raised an exception
                    key = future.result()
                    outstanding_part_keys.remove(key)

            if outstanding_part_keys and attempt > max_retries:
                raise OSError(f"Failed to multipart_upload {outstanding_part_keys} after {attempt} attempts")

    return multipart_upload.complete(
        MultipartUpload={
            "Parts": [
                CompletedPartTypeDef(ETag=part.e_tag, PartNumber=part.part_number)  # type: ignore[typeddict-item]
                for part in multipart_upload.parts.all()
            ]
        },
    )


def s3_copy_object(
    source_url: str,
    destination_url: str,
    version_id: Optional[str] = None,
    extra_args: Optional[dict] = None,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
):
    _, src_bucket_name, src_key = s3_split_path(source_url)
    _, dest_bucket_name, dest_key = s3_split_path(destination_url)

    copy_source = {"Bucket": src_bucket_name, "Key": src_key}
    if version_id is not None:
        copy_source["VersionId"] = version_id

    destination_buck = s3_bucket(dest_bucket_name, session=session, config=config)
    destination_buck.copy(copy_source, dest_key, ExtraArgs=extra_args or {})  # type: ignore[arg-type]


def s3_upload_file(
    file_path: str, bucket: str, key: str, session: Optional[Session] = None, config: Optional[Config] = None
):
    """
    Upload a file to a given S3 location
    Args:
        file_path: Path to file to be uploaded
        bucket: Target bucket
        key: Target key
        session: boto3 session
        config: optional botocore config
    """
    client = s3_resource(session=session, config=config).meta.client
    client.upload_file(file_path, bucket, key)


def s3_upload_data(
    bucket: str, key: str, data: str, session: Optional[Session] = None, config: Optional[Config] = None
):
    """
    Upload a file to a given S3 location
    Args:
        bucket: Target bucket
        key: Target key
        data: data to be uploaded
        session: boto3 session
        config: optional botocore config
    """
    s3object = s3_resource(session=session, config=config).Object(bucket, key)
    s3object.put(Body=data.encode())


def s3_download_file(
    file_path: str, bucket: str, key: str, session: Optional[Session] = None, config: Optional[Config] = None
):
    """
    Download a file from a given S3 location
    Args:
        file_path: Path to file to be uploaded
        bucket: Target bucket
        key: Target key
        session: boto3 session
        config: optional botocore config
    """
    client = s3_resource(session=session, config=config).meta.client
    client.download_file(bucket, key, file_path)


def s3_download_fileobj(
    file_obj: Union[IO[Any], StreamingBody],
    bucket: str,
    key: str,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
    transfer_config: Optional[TransferConfig] = None,
):
    """
    Download a file from a given S3 location
    Args:
        file_obj: The file object to stream to
        bucket: Target bucket
        key: Target key
        session: boto3 session
        transfer_config: transfer config to specify threading, chunk size, etc. if desired
        config: optional botocore config
    """
    client = s3_resource(session=session, config=config).meta.client
    if transfer_config:
        client.download_fileobj(bucket, key, file_obj, Config=transfer_config)
    else:
        client.download_fileobj(bucket, key, file_obj)


def s3_create_presigned_url(
    bucket: str, key: str, expiration: int = 3600, session: Optional[Session] = None, config: Optional[Config] = None
):
    """
    Creates a presigned url for a given S3 object
    Args:
        bucket: Target bucket
        key: Target key
        expiration: Time in seconds for the presigned URL to remain valid
        session: boto3 session
        config: optional botocore config

    Returns:
        Presigned URL as string
    """
    # why not s3_client helper ?
    client = s3_resource(session=session, config=config).meta.client
    params = {"Bucket": bucket, "Key": key}
    response = client.generate_presigned_url("get_object", Params=params, ExpiresIn=expiration)
    return response


_RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException", "TransactionConflictException")
_TRANSACTION_CANCELLED = "TransactionCanceledException"
_TRANSACTION_CONFLICT = "TransactionConflict"

FuncT = TypeVar("FuncT", bound=Callable[..., Any])


class DDBRetryBackoff:
    def __init__(self, max_retries: int = 6, backoff: float = 0.01, jitter_ratio: float = 0.1):
        self._max_retries = max_retries
        self._backoff = backoff
        self._jitter_ratio = jitter_ratio
        if jitter_ratio < 0 or jitter_ratio > 0.5:
            raise ValueError(f"jitter ratio should be between 0 and 0.5, actual {jitter_ratio}")

    def _should_retry(self, err: ClientError, retries: int):
        if retries >= self._max_retries:
            return False

        reasons = transaction_cancelled_for_only_reasons(err, _TRANSACTION_CONFLICT)
        if reasons:
            return True

        if err.response["Error"]["Code"] in _RETRY_EXCEPTIONS:
            return True

        return False

    def _sleep_for(self, retries: int):
        backoff = pow(2, retries) * self._backoff
        jitter_ratio = 1 + (random.choice([1, -1]) * self._jitter_ratio)
        return backoff * jitter_ratio

    def __call__(self, func: FuncT) -> FuncT:
        @wraps(func)
        def _wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    result = func(*args, **kwargs)

                    return result

                except ClientError as err:  # noqa: PERF203
                    if not self._should_retry(err, retries):
                        raise

                    retries += 1
                    time.sleep(self._sleep_for(retries))

        @wraps(func)
        async def _async_wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    result = await func(*args, **kwargs)

                    return result

                except ClientError as err:  # noqa: PERF203
                    if not self._should_retry(err, retries):
                        raise

                    retries += 1
                    await asyncio.sleep(self._sleep_for(retries))

        if inspect.iscoroutinefunction(func):
            return cast(FuncT, _async_wrapper)

        return cast(FuncT, _wrapper)


dynamodb_retry_backoff = DDBRetryBackoff


@dynamodb_retry_backoff()
def get_items_batched(
    ddb_table_name: str,
    keys: List[Dict[str, Any]],
    client: Optional[DynamoDBClient] = None,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
    **kwargs,
):
    client = client or cast(DynamoDBClient, dynamodb(session=session, config=config).meta.client)
    response = client.batch_get_item(
        RequestItems={ddb_table_name: cast(KeysAndAttributesTypeDef, {"Keys": keys})}, **kwargs
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    return response


def ddb_get_items(
    ddb_table_name: str,
    keys: List[Dict[str, Any]],
    client: Optional[DynamoDBClient] = None,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
    **kwargs,
) -> List[Dict[str, Any]]:
    client = client or cast(DynamoDBClient, dynamodb(session=session, config=config).meta.client)
    result = []
    remaining = keys
    while remaining:
        batch = remaining[:100]
        remaining = remaining[100:]
        response = get_items_batched(ddb_table_name, batch, client=client, session=session, **kwargs)
        if response.get("Responses") and response["Responses"].get(ddb_table_name):
            result.extend(response["Responses"][ddb_table_name])
        if response.get("UnprocessedKeys") and response["UnprocessedKeys"].get(ddb_table_name):
            remaining = response["UnprocessedKeys"][ddb_table_name]["Keys"] + remaining
    return result


def ddb_query_batch_get_items(
    key_field: str,
    client: Optional[DynamoDBClient] = None,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
    **kwargs,
) -> Generator[dict, None, None]:
    table_name = kwargs["TableName"]
    client = client or cast(DynamoDBClient, dynamodb(session=session, config=config).meta.client)
    paginator = client.get_paginator("query")

    page_iterator = paginator.paginate(**kwargs)

    deserializer = TypeDeserializer()

    for page in page_iterator:
        keys = [{key_field: rec[key_field]} for rec in page["Items"]]
        items = ddb_get_items(table_name, keys)
        for item in items:
            yield {k: deserializer.deserialize(v) for k, v in item.items()}


def ddb_query_paginate(
    client: Optional[DynamoDBClient] = None,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
    **kwargs,
) -> Generator[dict, None, None]:
    client = client or cast(DynamoDBClient, dynamodb(session=session, config=config).meta.client)
    paginator = client.get_paginator("query")

    page_iterator = paginator.paginate(**kwargs)

    for page in page_iterator:
        yield from page["Items"]


def ddb_query_paginated_count(
    client: Optional[DynamoDBClient] = None,
    session: Optional[Session] = None,
    config: Optional[Config] = None,
    **kwargs,
) -> int:
    client = client or cast(DynamoDBClient, dynamodb(session=session, config=config).meta.client)
    paginator = client.get_paginator("query")

    kwargs = {**kwargs, "Select": "COUNT"}

    page_iterator = paginator.paginate(**kwargs)

    return reduce(lambda acc, page: acc + page["Count"], page_iterator, 0)


def s3_gunzip(
    source_url: str, destination_url: str, session: Optional[Session] = None, config: Optional[Config] = None
):
    """
    Unzip a file on S3 to another S3 location. Streams the file and unzips it with local resources,
    and streams the bytes to the new location. Does not use local disk and minimally uses
    local memory.
    """
    _, src_bucket_name, src_key = s3_split_path(source_url)
    _, dest_bucket_name, dest_key = s3_split_path(destination_url)

    s3obj = s3_object(src_bucket_name, src_key, session=session, config=config)
    down_stream = s3obj.get()["Body"]

    s3obj = s3_bucket(dest_bucket_name, session=session, config=config).put_object(Key=dest_key)
    if not s3obj:
        raise Exception(f"Unknown S3 error when putting empty buffer to {destination_url}") from UnknownServiceError

    with gzip.GzipFile(mode="rb", fileobj=down_stream) as zipfile, S3ObjectWriter(s3obj, encoding=None) as new_obj_buff:
        buf = b" "
        while buf:
            buf = zipfile.read(io.DEFAULT_BUFFER_SIZE * 1000)
            if buf:
                new_obj_buff.write(buf)


def assumed_credentials(
    account_id: str,
    role: str,
    role_session_name: Optional[str] = None,
    duration_seconds: int = 1200,
    sts_endpoint_url: Optional[str] = None,
):
    """
    Refreshes the IAM credentials provided to us by STS for the duration of our session. This
    callback is invoked automatically by boto when we are past the lifetime of our
    session (see DurationSeconds in params).

    Returns:
         dict -> A dictionary containing our new set of credentials from STS as well as the
         expiration timestamp for the session.
    """
    region = os.environ.get("AWS_REGION", "eu-west-2")
    endpoint_url = sts_endpoint_url or f"https://sts.{region}.amazonaws.com"

    role_session_name = role_session_name or f"assumed-{uuid4().hex}"

    sts_client = boto3.client("sts", region_name=region, endpoint_url=endpoint_url)

    params = {
        "RoleArn": f"arn:aws:iam::{account_id}:role/{role}",
        "RoleSessionName": role_session_name,
        "DurationSeconds": duration_seconds,
    }

    response = sts_client.assume_role(**params).get("Credentials")

    credentials = {
        "access_key": response.get("AccessKeyId"),
        "secret_key": response.get("SecretAccessKey"),
        "token": response.get("SessionToken"),
        "expiry_time": response.get("Expiration").isoformat(),
    }
    return credentials


def assumed_role_session(
    account_id: str,
    role: str,
    role_session_name: Optional[str] = None,
    duration_seconds: int = 1200,
    refreshable: bool = False,
    sts_endpoint_url: Optional[str] = None,
) -> Session:
    """
    gets assumed role session for given account id and role name
    :return: Boto session for assumed role
    """

    get_credentials = partial(
        assumed_credentials,
        account_id,
        role,
        role_session_name=role_session_name,
        duration_seconds=duration_seconds,
        sts_endpoint_url=sts_endpoint_url,
    )

    credentials = get_credentials()
    if not refreshable:
        boto_credentials = botocore.credentials.Credentials(
            access_key=credentials["access_key"],
            secret_key=credentials["secret_key"],
            token=credentials["token"],
            method="sts-assume-role",
        )
    else:
        boto_credentials = botocore.credentials.RefreshableCredentials.create_from_metadata(
            metadata=credentials,
            refresh_using=get_credentials,
            method="sts-assume-role",
        )

    session = botocore.session.get_session()

    session._credentials = boto_credentials  # type: ignore[attr-defined]

    return Session(botocore_session=session)


async def async_stream_from_s3(
    s3_get_resp: GetObjectOutputTypeDef, bytes_per_chunk: int
) -> AsyncGenerator[bytes, None]:
    body = s3_get_resp["Body"]
    chunk = await run_in_executor(body.read, bytes_per_chunk)
    while chunk:
        yield chunk
        chunk = await run_in_executor(body.read, bytes_per_chunk)
    if body.closed:
        return

    with contextlib.suppress(Exception):
        body.close()


_RE_CANCELLATION_REASONS = re.compile(r"^.+\[(\w+[^\]]*?)\]$")


def transaction_cancellation_reasons(err: ClientError) -> List[str]:
    """
    get cancellation reasons as strings .. e.g.  ['None', 'ConditionalCheckFailed', 'None']
    """

    if err.response["Error"]["Code"] != _TRANSACTION_CANCELLED:
        return []

    match = _RE_CANCELLATION_REASONS.match(err.response["Error"]["Message"])
    if not match:
        return []

    return [reason.strip() for reason in match.group(1).split(",")]


def transaction_cancelled_for_only_reasons(err: ClientError, *match_reason: str) -> List[str]:
    """
    returns all reasons ... if all reasons either match match_reason or 'None'
    """
    reasons = transaction_cancellation_reasons(err)
    match_reasons = {"None", *list(match_reason)}
    return reasons if all(reason in match_reasons for reason in reasons) else []


def cancellation_reasons_if_conditional_check(err: ClientError) -> List[str]:
    return transaction_cancelled_for_only_reasons(err, "ConditionalCheckFailed")
