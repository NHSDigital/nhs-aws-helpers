import json
from typing import Tuple
from uuid import uuid4

from mypy_boto3_s3.service_resource import Bucket
from mypy_boto3_sqs.service_resource import Queue

from nhs_aws_helpers import events_client

from .test_helpers import read_sqs_json_messages


def test_get_temp_queue(temp_queue: Queue):
    assert temp_queue
    assert temp_queue.url

    messages = read_sqs_json_messages(temp_queue)
    assert not messages

    temp_queue.send_message(MessageBody=json.dumps({"my_event": 1234}))

    messages = read_sqs_json_messages(temp_queue)
    assert messages
    assert len(messages) == 1
    assert messages[0]["my_event"] == 1234


def test_get_temp_fifo_queue(temp_fifo_queue: Queue):
    assert temp_fifo_queue
    assert temp_fifo_queue.url

    messages = read_sqs_json_messages(temp_fifo_queue)
    assert not messages

    temp_fifo_queue.send_message(
        MessageGroupId="testing", MessageDeduplicationId=uuid4().hex, MessageBody=json.dumps({"my_event": 1234})
    )

    messages = read_sqs_json_messages(temp_fifo_queue)
    assert messages
    assert len(messages) == 1
    assert messages[0]["my_event"] == 1234


def test_get_temp_event_bus(temp_event_bus: Tuple[Queue, str]):
    events_queue, bus_name = temp_event_bus
    assert events_queue
    assert events_queue.url
    event_bus = events_client()

    messages = read_sqs_json_messages(events_queue)
    assert not messages

    event_bus.put_events(
        Entries=[
            {
                "Source": "my_source",
                "DetailType": "SuperEvent",
                "EventBusName": bus_name,
                "Detail": json.dumps({"my_event": 1234}),
            }
        ]
    )

    messages = read_sqs_json_messages(events_queue)
    assert messages
    assert len(messages) == 1
    assert messages[0]["detail"]["my_event"] == 1234


def test_get_temp_s3_bucket(temp_s3_bucket: Bucket):
    assert temp_s3_bucket
    assert temp_s3_bucket.name

    existing_objects = list(temp_s3_bucket.objects.all())
    assert not existing_objects
    key = uuid4().hex
    value = uuid4().hex.encode()
    new_object = temp_s3_bucket.Object(key)
    new_object.put(Body=value)

    all_objects = list(temp_s3_bucket.objects.all())
    assert len(all_objects) == 1
    got_object = temp_s3_bucket.Object(key)
    res = got_object.get()
    assert res["Body"].read() == value


def test_get_temp_s3_bucket_again_to_check_empty(temp_s3_bucket: Bucket):
    assert temp_s3_bucket
    assert temp_s3_bucket.name

    existing_objects = list(temp_s3_bucket.objects.all())
    assert not existing_objects
