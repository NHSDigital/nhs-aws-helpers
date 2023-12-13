import json
from typing import (
    Callable,
    List,
    Optional,
    cast,
)

from mypy_boto3_sqs.service_resource import Queue


def read_sqs_json_messages(queue: Queue, predicate: Optional[Callable[[dict], bool]] = None, **kwargs) -> List[dict]:
    defaults = {"MaxNumberOfMessages": 10, "WaitTimeSeconds": 0}
    if kwargs:
        defaults.update(kwargs)

    messages = queue.receive_messages(**defaults)  # type: ignore[arg-type]

    if not messages:
        return []

    parsed = cast(List[dict], [json.loads(message.body) for message in messages])

    if not predicate:
        return parsed

    return [msg for msg in parsed if predicate(msg)]
