import json
from typing import Any, Dict, Generator, Tuple

import petname  # type: ignore[import]
import pytest
from mypy_boto3_dynamodb.service_resource import Table
from mypy_boto3_s3.service_resource import Bucket
from mypy_boto3_s3.type_defs import CreateBucketConfigurationTypeDef
from mypy_boto3_sqs.service_resource import Queue

from nhs_aws_helpers import (
    ddb_table,
    dynamodb,
    events_client,
    s3_resource,
    sqs_resource,
)

__all__ = [
    "temp_s3_bucket_session_fixture",
    "temp_s3_bucket_fixture",
    "temp_event_bus_fixture",
    "temp_queue_fixture",
    "temp_fifo_queue_fixture",
    "clone_schema",
    "temp_dynamodb_table",
]


@pytest.fixture(scope="session", name="session_temp_s3_bucket")
def temp_s3_bucket_session_fixture() -> Generator[Bucket, None, None]:
    """
        session temp_s3_bucket ... (use temp_s3_bucket)
    Returns:
        Bucket
    """
    resource = s3_resource()

    bucket_name = f"temp-{petname.generate()}"
    bucket = resource.create_bucket(
        Bucket=bucket_name, CreateBucketConfiguration=CreateBucketConfigurationTypeDef(LocationConstraint="eu-west-2")
    )
    yield bucket

    bucket.objects.all().delete()
    bucket.delete()


@pytest.fixture(name="temp_s3_bucket")
def temp_s3_bucket_fixture(session_temp_s3_bucket: Bucket) -> Bucket:
    """
        yields a temporary s3 bucket for use in unit tests

    Returns:
        Bucket: a temporary empty s3 bucket
    """
    bucket = session_temp_s3_bucket

    bucket.objects.all().delete()

    return bucket


@pytest.fixture(name="temp_event_bus")
def temp_event_bus_fixture() -> Generator[Tuple[Queue, str], None, None]:
    """
        creates a temporary event bus and a Queue to listen to the event bus
    Returns:
        Tuple[Queue, str]: the listening queue and event bus name
    """
    events = events_client()
    sqs = sqs_resource()

    queue_name = f"temp-{petname.Generate(words=2, separator='-')}"
    queue = sqs.create_queue(QueueName=queue_name)

    bus_name = f"temp-{petname.generate()}"
    events.create_event_bus(Name=bus_name)

    rule_name = f"temp-{petname.generate()}"
    events.put_rule(Name=rule_name, EventPattern=json.dumps({"account": ["000000000000"]}), EventBusName=bus_name)

    queue_arn = f"arn:aws:sqs:eu-west-2:000000000000:{queue_name}"
    target_id = f"temp-{petname.generate()}"
    events.put_targets(
        Targets=[{"Id": target_id, "Arn": queue_arn}],
        EventBusName=bus_name,
        Rule=rule_name,
    )

    yield queue, bus_name

    events.remove_targets(Rule=rule_name, EventBusName=bus_name, Ids=[target_id], Force=True)

    events.delete_rule(Name=rule_name, EventBusName=bus_name, Force=True)
    queue.delete()
    events.delete_event_bus(Name=bus_name)


@pytest.fixture(name="temp_queue")
def temp_queue_fixture() -> Generator[Queue, None, None]:
    """
        yields a temporary SQS queue for testing
    Returns:
        Queue
    """
    sqs = sqs_resource()

    queue_name = f"local-{petname.Generate(words=2, separator='-')}"
    queue = sqs.create_queue(QueueName=queue_name, Attributes={"VisibilityTimeout": "2"})

    yield queue

    queue.delete()


@pytest.fixture(name="temp_fifo_queue")
def temp_fifo_queue_fixture() -> Generator[Queue, None, None]:
    """
        yields a temporary Fifo SQS queue for testing
    Returns:
        Queue
    """
    sqs = sqs_resource()

    queue_name = f"local-{petname.Generate(words=2, separator='-')}.fifo"
    queue = sqs.create_queue(QueueName=queue_name, Attributes={"FifoQueue": "true", "VisibilityTimeout": "2"})

    yield queue

    queue.delete()


def clone_schema(table, on_demand_billing_mode: bool = True, provisioned_capacity: int = 250):
    key_schema = table.key_schema

    attributes = table.attribute_definitions

    indexes = table.global_secondary_indexes

    if indexes:
        for index in indexes:
            del index["IndexStatus"]
            del index["IndexSizeBytes"]
            del index["ItemCount"]
            del index["IndexArn"]
            del index["ProvisionedThroughput"]

    clone = {
        "KeySchema": key_schema,
        "AttributeDefinitions": attributes,
    }
    if on_demand_billing_mode:
        provisioned_capacity = 0
    billing: Dict[str, Any] = (
        {
            "BillingMode": "PAY_PER_REQUEST",
        }
        if on_demand_billing_mode
        else {
            "ProvisionedThroughput": {
                "ReadCapacityUnits": provisioned_capacity,
                "WriteCapacityUnits": provisioned_capacity,
            }
        }
    )

    clone.update(billing)

    if indexes:
        if not on_demand_billing_mode:
            for index in indexes:
                index.update(billing)
        clone["GlobalSecondaryIndexes"] = indexes

    return clone


def temp_dynamodb_table(
    source_table_name: str, on_demand_billing_mode: bool = True, provisioned_capacity: int = 500
) -> Generator[Table, None, None]:
    """
    Create a table that copies the schema of <source_table> but uses a random name, can be used throughout
    a test and is deleted at the end.
    """
    ddb = dynamodb()

    table_name = f"pytest-{petname.Generate(words=4, separator='_')}"

    source_table = ddb_table(source_table_name)

    cloned = clone_schema(source_table, on_demand_billing_mode, provisioned_capacity)

    table = ddb.create_table(TableName=table_name, **cloned)

    yield table

    table.delete()
