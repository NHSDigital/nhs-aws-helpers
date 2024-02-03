import asyncio
import dataclasses
import itertools
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
from time import time
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    Generic,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    cast,
    get_args,
    get_origin,
)

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
from mypy_boto3_dynamodb.type_defs import (
    BatchGetItemOutputServiceResourceTypeDef,
    QueryOutputTableTypeDef,
    TransactGetItemTypeDef,
    UpdateItemOutputTableTypeDef,
    WriteRequestTypeDef,
)

from nhs_aws_helpers import dynamodb, dynamodb_retry_backoff
from nhs_aws_helpers.common import (
    is_dataclass_instance,
    optional_origin_type,
    run_in_executor,
)
from nhs_aws_helpers.dynamodb_model_store.base_model import (
    BaseModel,
    model_properties_cache,
)

TBaseModel = TypeVar("TBaseModel", bound=BaseModel)
TBaseModel_co = TypeVar("TBaseModel_co", bound=BaseModel, covariant=True)
TModelKey = TypeVar("TModelKey", bound=Mapping[str, Any])
TPageItem = TypeVar("TPageItem")


@dataclass
class PagedItems(Generic[TPageItem]):
    items: List[TPageItem]
    last_evaluated_key: Optional[Dict[str, Any]]

    def __post_init__(self):
        self.items = self.items or []

    def __iter__(self):
        """this is to allow the dataclass to unpack as a tuple, not iterate the items"""
        yield self.items
        yield self.last_evaluated_key

    def __len__(self):
        return len(self.items)

    def __add__(self, other: "PagedItems") -> "PagedItems":
        items = []
        items.extend(self.items)
        items.extend(other.items)
        return PagedItems(items=items, last_evaluated_key=other.last_evaluated_key)

    def __iadd__(self, other: "PagedItems") -> "PagedItems":
        self.items.extend(other.items)
        self.last_evaluated_key = other.last_evaluated_key
        return self


class BaseModelStore(Generic[TBaseModel, TModelKey]):
    _base_model_type: Type[TBaseModel]
    _model_types: Dict[str, Type[TBaseModel]] = {}  # noqa: RUF012

    def __new__(cls, *args, **kwargs):
        if (
            not hasattr(cls, "_base_model_type")
            or not cls._base_model_type
            or not hasattr(cls._base_model_type, "model_key_fields")
        ):
            raise ValueError(f"{cls.__name__} needs a _base_model_type definition")

        instance = super().__new__(cls)
        return instance

    def __init__(
        self, table_name: str, model_type_index_name: Optional[str] = None, model_type_index_pk: Optional[str] = None
    ):
        self._table_key_fields = self._base_model_type.model_key_fields()
        self._partition_key = self._table_key_fields[0]
        self._table_name = table_name
        self._table: Optional[Table] = None
        self._service: Optional[DynamoDBServiceResource] = None
        self._client: Optional[DynamoDBClient] = None
        self._model_type_index_name = model_type_index_name
        self._model_type_index_pk = model_type_index_pk

    @classmethod
    def register_model_type(cls, model_type: Type[TBaseModel]):
        cls._model_types[model_type.__name__] = model_type

    @property
    def service(self) -> DynamoDBServiceResource:
        if self._service is None:
            self._service = dynamodb()
        return self._service

    @property
    def table(self) -> Table:
        if self._table is None:
            self._table = self.service.Table(self._table_name)
        return self._table

    @property
    def client(self) -> DynamoDBClient:
        return self.service.meta.client

    @property
    def table_key_fields(self) -> List[str]:
        return self._table_key_fields

    @classmethod
    def _deserialise_field(cls, field: dataclasses.Field, value: Any, **kwargs) -> Any:
        return cls.deserialise_value(field.type, value, **kwargs)

    @classmethod
    def deserialise_value(cls, value_type: type, value: Any, **kwargs) -> Any:  # noqa: C901
        value_type = optional_origin_type(value_type)

        if value and value_type in (bytes, bytearray) and hasattr(value, "value"):
            value = value.value
            if value_type == bytearray:
                value = bytearray(value)

        if value_type in (str, bool, bytes, bytearray):
            return value

        if is_dataclass(value_type):
            return cls.deserialise_model(value, value_type, **kwargs)

        if value_type in (int, float):
            return value_type(value)

        if value_type == datetime:
            return datetime.fromisoformat(value)

        if value_type == date:
            return date.fromisoformat(value)

        origin_type = get_origin(value_type)

        if origin_type == list:
            item_type = get_args(value_type)[0]
            return [cls.deserialise_value(item_type, val, **kwargs) for val in value]

        if origin_type == dict:
            val_type = get_args(value_type)[1]
            return {key: cls.deserialise_value(val_type, val, **kwargs) for key, val in value.items()}

        if origin_type == frozenset:
            return frozenset(val for val in value)

        if origin_type == set:
            return set(value)

        return value

    @classmethod
    def _before_deserialise_model(
        cls, model_dict: Dict[str, Any], model_type: Type[TBaseModel], **kwargs
    ) -> Dict[str, Any]:
        return model_dict

    @classmethod
    def deserialise_model(
        cls, model_dict: Dict[str, Any], model_type: Type[TBaseModel_co], **kwargs
    ) -> Optional[TBaseModel_co]:
        if model_dict is None:
            return None

        if not is_dataclass(model_type):
            raise TypeError(f"type {model_type} is not a dataclass")

        model_dict = cls._before_deserialise_model(model_dict, cast(Type[TBaseModel], model_type), **kwargs)

        model_fields = fields(model_type)

        deserialised: Dict[str, Any] = {}
        for field in model_fields:
            value = model_dict.get(field.name)
            if value is None:
                continue

            deserialised[field.name] = cls._deserialise_field(field, value, **kwargs)

        return model_type(**deserialised)  # type: ignore[return-value]

    @classmethod
    def serialise_value(cls, value: Any, **kwargs) -> Optional[Any]:
        if value is None:
            return None

        if is_dataclass_instance(value):
            class_serialised = cls.serialise_model(value, **kwargs)
            return class_serialised

        if isinstance(value, dict):
            dict_serialised = {k: cls.serialise_value(v, **kwargs) for k, v in value.items()}
            return dict_serialised

        if isinstance(value, (list, tuple)):
            if not value:
                return None
            list_serialised = [cls.serialise_value(v, **kwargs) for v in value]
            return list_serialised

        if isinstance(value, (date, datetime)):
            value = value.isoformat()

        if isinstance(value, (set, frozenset)) and not value:
            return None

        return value

    @classmethod
    def _serialise_field(
        cls, model: Any, field_name: str, field_type: type, metadata: Mapping[str, Any], value: Any, **kwargs
    ) -> Any:
        if value is None:
            return None

        if field_type == Optional[str] and value == "":
            return None

        value = cls.serialise_value(value, **kwargs)
        return value

    @classmethod
    def serialise_model(cls, model: Any, **kwargs) -> Optional[Dict[str, Any]]:
        if model is None:
            return None

        if not is_dataclass_instance(model):
            raise TypeError(f"type {type(model)} is not a dataclass")

        model_fields = model_properties_cache(model.__class__)

        result: Dict[str, Any] = {}

        for field_name, field_type, metadata in model_fields:
            value = getattr(model, field_name)

            value = cls._serialise_field(model, field_name, field_type, metadata, value, **kwargs)

            if value is None:
                # don't store None values.
                continue

            result[field_name] = value

        return result

    @dynamodb_retry_backoff()
    async def get_item_with_retry_info(
        self, key: TModelKey, **kwargs
    ) -> Tuple[Optional[Dict[str, Any]], float, str, int]:
        kwargs["Key"] = cast(Dict[str, str], key)

        started = time()
        response = await run_in_executor(self.table.get_item, **kwargs)
        duration = time() - started

        aws_request_id = response["ResponseMetadata"]["RequestId"]
        aws_retries = response["ResponseMetadata"]["RetryAttempts"]

        item: Optional[Dict[str, Any]] = response.get("Item")
        if not item:
            return None, duration, aws_request_id, aws_retries

        return item, duration, aws_request_id, aws_retries

    async def get_item(self, key: TModelKey, **kwargs) -> Optional[Dict[str, Any]]:
        item, _, _, _ = await self.get_item_with_retry_info(key, **kwargs)
        return item

    async def get_model_with_retry_info(
        self, key: TModelKey, model_type: Type[TBaseModel_co], **kwargs
    ) -> Tuple[Optional[TBaseModel_co], float, str, int]:
        item, duration, aws_request_id, aws_retries = await self.get_item_with_retry_info(key, **kwargs)

        if not item:
            return None, duration, aws_request_id, aws_retries

        return (
            cast(Optional[TBaseModel_co], self.deserialise_model(item, model_type, **kwargs)),
            duration,
            aws_request_id,
            aws_retries,
        )

    async def get_model(self, key: TModelKey, model_type: Type[TBaseModel_co], **kwargs) -> Optional[TBaseModel_co]:
        model, _, _, _ = await self.get_model_with_retry_info(key, model_type, **kwargs)

        return model

    async def transact_get_model(
        self, key: TModelKey, model_type: Type[TBaseModel_co], **kwargs
    ) -> Optional[TBaseModel_co]:
        items = await self.transact_get_items(cast(List[TransactGetItemTypeDef], [{"Get": {"Key": key}}]))

        if not items or not items[0]:
            return None

        item = cast(dict, items[0])

        return cast(Optional[TBaseModel_co], self.deserialise_model(item, model_type, **kwargs))

    @dynamodb_retry_backoff()
    async def transact_get_items(self, get_items: Sequence[TransactGetItemTypeDef]) -> List[Optional[dict]]:
        get_items = list(get_items)
        for get_item in get_items:
            get_item["Get"]["TableName"] = self._table_name
        response = await run_in_executor(self.client.transact_get_items, TransactItems=get_items)

        responses = response.get("Responses")
        if not responses:
            return []

        items = [resp.get("Item", None) for resp in responses]
        return items

    def _model_key_tuple(self, key_or_item: Mapping[str, Any]) -> Tuple[str, ...]:
        return tuple(key_or_item[field] for field in self._base_model_type.model_key_fields())

    async def transact_get_models(self, keys: Sequence[TModelKey], **kwargs) -> List[Optional[TBaseModel]]:
        keys = list(keys)
        key_indexes = {self._model_key_tuple(key): ix for ix, key in enumerate(keys)}

        records = await self.transact_get_items([cast(TransactGetItemTypeDef, {"Get": {"Key": key}}) for key in keys])

        models: List[Tuple[Tuple[str, ...], TBaseModel]] = [
            (
                self._model_key_tuple(record),
                cast(TBaseModel, self.deserialise_model(record, self._model_types[record["model_type"]], **kwargs)),
            )
            for record in records
            if record
        ]
        results: List[Optional[TBaseModel]] = [None for _ in range(len(keys))]

        unexpected = []

        for key_tuple, model in models:
            ix = key_indexes.get(key_tuple)
            if ix is None:
                unexpected.append(model)
                continue
            results[ix] = model

        if unexpected:
            raise ValueError(f"unexpected items in batch result: {[model.model_key() for model in unexpected]}")

        return results

    @dynamodb_retry_backoff()
    async def put_item(self, item: Dict[str, Any], **kwargs):
        kwargs["Item"] = item
        await run_in_executor(self.table.put_item, **kwargs)

    async def put_model(self, model: TBaseModel, **kwargs):
        assert model
        item = self.serialise_model(model, **kwargs)
        assert item
        await self.put_item(item, **kwargs)

    @dynamodb_retry_backoff()
    async def put_item_if_not_exists(self, item: Dict[str, Any], **kwargs) -> bool:
        try:
            await self.put_item(item, ConditionExpression=Attr(self._partition_key).not_exists(), **kwargs)
            return True
        except ClientError as e:
            # Ignore the ConditionalCheckFailedException and return false
            if e.response.get("Error", {}).get("Code", "") == "ConditionalCheckFailedException":
                return False
            # other exceptions bubble up
            raise

    async def put_model_if_not_exists(self, model: TBaseModel, **kwargs) -> bool:
        assert model
        item = self.serialise_model(model, **kwargs)
        assert item
        return await self.put_item_if_not_exists(item, **kwargs)

    async def item_exists(self, key: TModelKey, consistent_read: bool = False, **kwargs) -> bool:
        """
        Note: don't call this method as a check before doing a put item, as it will result
        in 2 hits to the DB instead of 1 and won't ensure consistency anyway.

        Instead, just call _put_item with an attribute_not_exists condition.
        """

        item = await self.get_item(
            key, ConsistentRead=consistent_read, ProjectionExpression=self._partition_key, **kwargs
        )
        if not item or self._partition_key not in item:
            return False

        return True

    @dynamodb_retry_backoff()
    async def delete_item(self, key: TModelKey, **kwargs):
        await run_in_executor(self.table.delete_item, Key=cast(Dict[str, Any], key), **kwargs)

    @dynamodb_retry_backoff()
    async def paginate(
        self, paginator_type: Literal["query", "scan", "list_backups", "list_tables"], **kwargs
    ) -> AsyncGenerator[dict, None]:
        paginator = self.client.get_paginator(paginator_type)
        kwargs["TableName"] = self._table_name
        page_iterator = paginator.paginate(**kwargs).__iter__()

        @dynamodb_retry_backoff()
        def _get_next_page():
            try:
                return page_iterator.__next__()
            except StopIteration:
                return None

        page = await run_in_executor(_get_next_page)
        while page:
            last_evaluated_key = page.get("LastEvaluatedKey", None)
            yield page
            if not last_evaluated_key:
                break
            page = await run_in_executor(_get_next_page)

    async def paginate_items(
        self, paginator_type: Literal["query", "scan", "list_backups", "list_tables"], **kwargs
    ) -> AsyncGenerator[PagedItems[dict], None]:
        async for page in self.paginate(paginator_type, **kwargs):
            items = page.get("Items", [])
            last_evaluated_key = page.get("LastEvaluatedKey", None)
            yield PagedItems(items, last_evaluated_key)

    @dynamodb_retry_backoff()
    async def query(self, **kwargs) -> QueryOutputTableTypeDef:
        return await run_in_executor(self.table.query, **kwargs)

    async def query_items(self, **kwargs) -> PagedItems[dict]:
        page = await self.query(**kwargs)
        return PagedItems(page.get("Items", []), cast(Optional[Dict[str, Any]], page.get("LastEvaluatedKey", None)))

    @dynamodb_retry_backoff()
    async def query_count(self, **kwargs) -> int:
        total = 0
        async for page in self.paginate("query", Select="COUNT", **kwargs):
            page_count = page.get("Count", 0)
            total += page_count

        return total

    @dynamodb_retry_backoff()
    async def transact_write_items(self, transact_items: list, **kwargs):
        return await run_in_executor(self.client.transact_write_items, TransactItems=transact_items, **kwargs)

    def _prepare_transaction(self, actions: List[Dict[str, Any]], table_name: str, **kwargs):
        transact_items = []
        for action in actions:
            for operation, item in action.items():
                assert operation in ("Put", "Update", "Delete", "ConditionCheck"), f"unexpected_operation: {operation}"
                serialised = self.serialise_value(item, **kwargs)
                content: Dict[str, Any] = {"TableName": table_name}
                if operation == "Put" and is_dataclass_instance(item):
                    content["Item"] = serialised
                else:
                    assert isinstance(serialised, dict)
                    content.update(serialised)
                transact_items.append({operation: content})
        return transact_items

    async def transact_write(self, actions: List[Dict[str, Any]]):
        transaction = self._prepare_transaction(actions=actions, table_name=self._table_name)
        return await self.transact_write_items(transaction)

    @dynamodb_retry_backoff()
    async def update_item_with_retry_info(
        self, key: TModelKey, **kwargs
    ) -> Tuple[UpdateItemOutputTableTypeDef, float, str, int]:
        kwargs["Key"] = cast(Dict[str, str], key)

        started = time()
        response = await run_in_executor(self.table.update_item, **kwargs)
        duration = time() - started

        aws_request_id = response["ResponseMetadata"]["RequestId"]
        aws_retries = response["ResponseMetadata"]["RetryAttempts"]

        return response, duration, aws_request_id, aws_retries

    async def update_item(self, key: TModelKey, **kwargs) -> UpdateItemOutputTableTypeDef:
        response, _, _, _ = await self.update_item_with_retry_info(key, **kwargs)
        return response

    def batch_writer(self, flush_amount: int = 25) -> "_AsyncBatchWriter":
        return _AsyncBatchWriter(
            store=self,
            flush_amount=flush_amount,
        )

    async def query_all_items(self, max_items: int = 0, **kwargs) -> List[dict]:
        result = []

        async for items, _ in self.paginate_items("query", **kwargs):
            result.extend(items)
            if 0 < max_items <= len(result):
                return result[:max_items]

        return result

    async def get_all_model_keys(self, model_type: Type[TBaseModel_co], max_keys: int = 0) -> List[TModelKey]:
        assert self._model_type_index_name
        assert self._model_type_index_pk
        results = await self.query_all_items(
            IndexName=self._model_type_index_name,
            KeyConditionExpression=Key(self._model_type_index_pk).eq(model_type.__name__),
            max_items=max_keys,
        )
        return [model_type.model_key_from_item(res) for res in results]

    async def get_all_models(
        self, model_type: Type[TBaseModel_co], max_concurrency=10, max_models: int = 0
    ) -> List[TBaseModel_co]:
        model_keys = await self.get_all_model_keys(model_type=model_type, max_keys=max_models)
        return [
            cast(TBaseModel_co, record)
            for record in await self.batch_get_model(model_keys, max_concurrency=max_concurrency)
            if record
        ]

    async def paginate_models(
        self,
        paginator_type: Literal["query", "scan", "list_backups", "list_tables"],
        model_type: Type[TBaseModel_co],
        **kwargs,
    ) -> AsyncGenerator[PagedItems[TBaseModel_co], None]:
        async for items, last_evaluated_key in self.paginate_items(paginator_type, **kwargs):
            yield PagedItems(
                items=[cast(TBaseModel_co, self.deserialise_model(item, model_type, **kwargs)) for item in items],
                last_evaluated_key=last_evaluated_key,
            )

    async def paginate_models_from_index(
        self,
        paginator_type: Literal["query", "scan", "list_backups", "list_tables"],
        model_type: Type[TBaseModel_co],
        index_name: str,
        **kwargs,
    ) -> AsyncGenerator[PagedItems[TBaseModel_co], None]:
        async for items, last_evaluated_key in self.paginate_items(paginator_type, IndexName=index_name, **kwargs):
            model_keys = [model_type.model_key_from_item(item) for item in items]
            models = await self.batch_get_model(model_keys)
            yield PagedItems(
                [cast(TBaseModel_co, model) for model in models],
                last_evaluated_key=last_evaluated_key,
            )

    async def query_models(self, model_type: Type[TBaseModel_co], **kwargs) -> PagedItems[TBaseModel_co]:
        items, last_evaluated_key = await self.query_items(**kwargs)
        return PagedItems(
            items=[cast(TBaseModel_co, self.deserialise_model(item, model_type, **kwargs)) for item in items],
            last_evaluated_key=last_evaluated_key,
        )

    async def query_all_models(self, model_type: Type[TBaseModel_co], **kwargs) -> List[TBaseModel_co]:
        result = []

        async for models, _ in self.paginate_models("query", model_type, **kwargs):
            result.extend(models)
        return result

    def _get_batch_results(
        self, response: BatchGetItemOutputServiceResourceTypeDef
    ) -> Tuple[List[Dict[str, Any]], List[TModelKey]]:
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        results: List[Dict[str, Any]] = response.get("Responses", {}).get(self._table_name) or []
        unprocessed_keys: Dict[str, Any] = response.get("UnprocessedKeys", {})
        unprocessed = cast(List[TModelKey], unprocessed_keys.get(self._table_name, {}).get("Keys") or [])

        return results, unprocessed

    @dynamodb_retry_backoff()
    async def _get_batch(self, keys: List[TModelKey], **kwargs) -> Tuple[List[Dict[str, Any]], List[TModelKey]]:
        if not keys:
            return [], []

        request = kwargs or {}
        request["Keys"] = keys

        response = await run_in_executor(self.service.batch_get_item, RequestItems={self._table_name: request})
        return self._get_batch_results(response)

    async def batch_get_item(self, keys: List[TModelKey], max_concurrency: int = 10, **kwargs) -> List[Dict[str, Any]]:
        def _chunk(it, size):
            it = iter(it)
            return iter(lambda: tuple(itertools.islice(it, size)), ())

        result = []
        remaining = keys
        while remaining:
            batches = _chunk(remaining, 100)  # into batches of max size 100 (max allowed)
            to_retry = []
            async with asyncio.Semaphore(max_concurrency):
                task_results = await asyncio.gather(*[self._get_batch(list(batch), **kwargs) for batch in batches])

            for found, unprocessed in task_results:
                if found:
                    result.extend(found)
                if unprocessed:
                    to_retry.extend(unprocessed)

            remaining = to_retry

        return result

    async def batch_get_item_ordered(
        self, keys: Sequence[TModelKey], max_concurrency: int = 10, **kwargs
    ) -> List[Optional[dict]]:
        keys = list(keys)
        key_indexes: Dict[Tuple[str, ...], int] = {self._model_key_tuple(key): ix for ix, key in enumerate(keys)}

        records = await self.batch_get_item(keys, max_concurrency=max_concurrency, **kwargs)

        results: List[Optional[dict]] = [None for _ in range(len(keys))]

        unexpected = []

        for record in records:
            key_tuple = self._model_key_tuple(record)
            ix = key_indexes.get(key_tuple)
            if ix is None:
                unexpected.append(key_tuple)
                continue
            results[ix] = record

        if unexpected:
            raise ValueError(f"unexpected items in batch result: {list(unexpected)}")

        return results

    async def batch_get_model(
        self, keys: Sequence[TModelKey], max_concurrency: int = 10, **kwargs
    ) -> List[Optional[TBaseModel]]:
        ordered = await self.batch_get_item_ordered(keys, max_concurrency=max_concurrency)

        models: List[Optional[TBaseModel]] = [
            (
                cast(TBaseModel, self.deserialise_model(record, self._model_types[record["model_type"]], **kwargs))
                if record
                else None
            )
            for record in ordered
        ]

        return models


class _AsyncBatchWriter:
    """
    converted from boto3.dynamodb.BatchWriter to be async
    """

    def __init__(self, store: BaseModelStore, flush_amount=25):
        self._store = store
        self._table_name = store.table.table_name
        self._client = store.client
        self._items_buffer: List[WriteRequestTypeDef] = []
        self._flush_amount = flush_amount
        self._overwrite_by_keys = store.table_key_fields

    async def put_item(self, item, **kwargs):
        await self._add_request_and_process({"PutRequest": {"Item": self._store.serialise_value(item, **kwargs)}})

    async def delete_item(self, key, **kwargs):
        await self._add_request_and_process({"DeleteRequest": {"Key": self._store.serialise_value(key, **kwargs)}})

    async def _add_request_and_process(self, request):
        if self._overwrite_by_keys:
            self._remove_dup_keys_request_if_any(request)
        self._items_buffer.append(request)
        await self._flush_if_needed()

    def _remove_dup_keys_request_if_any(self, request):
        pkey_values_new = self._extract_pkey_values(request)
        for item in self._items_buffer:
            if self._extract_pkey_values(item) == pkey_values_new:
                self._items_buffer.remove(item)

    def _extract_pkey_values(self, request):
        if request.get("PutRequest"):
            return [request["PutRequest"]["Item"][key] for key in self._overwrite_by_keys]
        if request.get("DeleteRequest"):
            return [request["DeleteRequest"]["Key"][key] for key in self._overwrite_by_keys]
        return None

    async def _flush_if_needed(self):
        if len(self._items_buffer) >= self._flush_amount:
            await self._flush()

    @dynamodb_retry_backoff()
    async def _flush(self):
        if not self._items_buffer:
            return

        items_to_send = self._items_buffer[: self._flush_amount]
        if not items_to_send:
            return

        self._items_buffer = self._items_buffer[self._flush_amount :]
        response = await run_in_executor(self._client.batch_write_item, RequestItems={self._table_name: items_to_send})
        unprocessed_items = response["UnprocessedItems"]

        if unprocessed_items and unprocessed_items[self._table_name]:
            # Any unprocessed_items are immediately added to the
            # next batch we send.
            self._items_buffer.extend(unprocessed_items[self._table_name])
        else:
            self._items_buffer = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, tb):
        # When we exit, we need to keep flushing whatever is left
        # until there's nothing left in our items buffer.
        if not exc_value:
            while len(self._items_buffer) > 0:
                await self._flush()
        return True
