import pickle
import random
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Generator, List, Mapping, Optional, Type, TypedDict, Union, cast
from uuid import uuid4

import petname  # type: ignore[import]
import pytest as pytest
from boto3.dynamodb.conditions import Key
from mypy_boto3_dynamodb.service_resource import Table

from nhs_aws_helpers import dynamodb
from nhs_aws_helpers.dynamodb_model_store.base_model import (
    BaseModel,
    serialised_property,
)
from nhs_aws_helpers.dynamodb_model_store.base_model_store import (
    BaseModelStore,
    PagedItems,
)


class _MyModelKey(TypedDict):
    my_pk: str
    my_sk: str


@pytest.mark.skipif(sys.version_info < (3, 10), reason="requires python3.10 or higher")
def test_create_py_310_model():
    @dataclass(kw_only=True)  # type: ignore[call-overload]
    class MyBaseModelPy310(BaseModel[_MyModelKey]):
        _model_key_type = _MyModelKey
        base_required_thing: str
        optional_thing: Optional[str] = None

        last_modified: Optional[datetime] = field(default_factory=datetime.utcnow)

        def __post_init__(self):
            self.optional_thing = self.optional_thing or uuid4().hex

    @dataclass
    class SomeDerivedModelPy310(MyBaseModelPy310):
        _index_model_type = False
        required_thing: str  # type: ignore[misc]
        another_optional_thing: Optional[bool] = field(default=None)

        def _create_key(self) -> _MyModelKey:
            return self.key_from(self.required_thing)

        @classmethod
        def key_from(cls, required_thing: str) -> _MyModelKey:
            return _MyModelKey(my_pk="TEST", my_sk=required_thing)

        def __post_init__(self):
            # call this is you have a base __post_init__
            super().__post_init__()
            self.another_optional_thing = True

    model = SomeDerivedModelPy310(base_required_thing="TEST", required_thing="TEST2")
    assert model.last_modified
    assert model.another_optional_thing
    assert model.optional_thing
    assert model.base_required_thing == "TEST"
    assert model.required_thing == "TEST2"
    assert MyBaseModelPy310.model_key_fields() == ["my_pk", "my_sk"]
    assert SomeDerivedModelPy310.model_key_fields() == ["my_pk", "my_sk"]


def test_create_model_store_without_base_model_type():
    class BadModelStore(BaseModelStore[BaseModel, _MyModelKey]):
        pass

    with pytest.raises(ValueError, match=f"{BadModelStore.__name__} needs a _base_model_type definition"):
        BadModelStore("no")


class MyBaseModel(BaseModel[_MyModelKey]):
    """
    pre py 3.10 dataclass inheritance doesn't work very well ... unless al
    but you may still need a base model
    """

    _model_key_type = _MyModelKey

    def get_key(self) -> _MyModelKey:
        raise NotImplementedError("should be implemented in the dervied class")

    @serialised_property
    def my_pk(self) -> str:
        return self.get_key()["my_pk"]

    @serialised_property
    def my_sk(self) -> str:
        return self.get_key()["my_sk"]


@dataclass
class NestedItem:
    event: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class MyDerivedModel(MyBaseModel):
    id: str
    sk_field: Optional[str] = None
    optional_thing: Optional[str] = None

    today: date = field(default_factory=lambda: datetime.utcnow().date())
    last_modified: datetime = field(default_factory=datetime.utcnow)
    some_dict: Optional[dict] = field(default_factory=lambda: {"key": 123})
    chaos: Optional[int] = field(default_factory=lambda: random.randint(0, 10))
    set_type: Optional[set] = field(default_factory=set)
    frozenset_type: Optional[frozenset] = field(default_factory=frozenset)
    none_string: Optional[str] = None
    none_list: Optional[List[str]] = None
    nested_items: List[NestedItem] = field(default_factory=list)
    nested_item: NestedItem = field(default_factory=lambda: NestedItem(event="created"))
    union_date: Union[datetime, None] = field(default_factory=datetime.utcnow)
    bytes_type: Union[bytes, None] = None
    bytearray_type: Union[bytearray, None] = None

    def get_key(self) -> _MyModelKey:
        return _MyModelKey(my_pk=f"AA#{self.id}", my_sk=self.sk_field or "#")

    def __post_init__(self):
        self.sk_field = self.sk_field or "#"
        if (self.chaos or 0) > 4:
            if not self.set_type:
                self.set_type = {self.chaos}
            if not self.frozenset_type:
                self.frozenset_type = frozenset(self.set_type)


@dataclass
class AnotherModel(MyBaseModel):
    id: str
    sk_field: Optional[str] = None
    last_modified: datetime = field(default_factory=datetime.utcnow)

    def get_key(self) -> _MyModelKey:
        return _MyModelKey(my_pk=f"BB#{self.id}", my_sk=self.sk_field or "#")

    def __post_init__(self):
        self.sk_field = self.sk_field or "#"


@dataclass
class UnregisteredModel(MyBaseModel):
    id: str
    sk_field: Optional[str] = None
    last_modified: datetime = field(default_factory=datetime.utcnow)

    def get_key(self) -> _MyModelKey:
        return _MyModelKey(my_pk=f"AA#{self.id}", my_sk=self.sk_field or "#")

    def __post_init__(self):
        self.sk_field = self.sk_field or "#"


class NotSerializable:
    def __init__(self, some_uuid: str):
        self._some_uuid = some_uuid

    @property
    def some_uuid(self) -> str:
        return self._some_uuid


@dataclass
class ASpecialDerivedModel(MyBaseModel):
    id: str
    special_field: NotSerializable
    events: List[NestedItem] = field(default_factory=list)
    last_modified: datetime = field(default_factory=datetime.utcnow)

    def get_key(self) -> _MyModelKey:
        return _MyModelKey(my_pk=f"BB#{self.id}", my_sk="#")

    def __post_init__(self):
        if not self.events:
            self.events.append(NestedItem(event="created"))


def test_create_model():
    model = MyDerivedModel(id="TEST2")
    assert model.last_modified
    assert model.sk_field == "#"
    assert model.id == "TEST2"
    assert MyBaseModel.model_key_fields() == ["my_pk", "my_sk"]
    assert MyDerivedModel.model_key_fields() == ["my_pk", "my_sk"]
    assert MyDerivedModel.model_key_fields() == ["my_pk", "my_sk"]
    assert MyBaseModel.model_key_fields() == ["my_pk", "my_sk"]


class MyModelStore(BaseModelStore[MyBaseModel, _MyModelKey]):
    _base_model_type = MyBaseModel
    _model_types = {cls.__name__: cls for cls in [MyDerivedModel, AnotherModel, ASpecialDerivedModel]}  # noqa: RUF012

    def __init__(self, table_name: str):
        super().__init__(table_name, model_type_index_name="gsi_model_type", model_type_index_pk="model_type")
        self.register_model_type(MyDerivedModel)

    @classmethod
    def _before_deserialise_model(
        cls, model_dict: Dict[str, Any], model_type: Type[MyBaseModel], **kwargs
    ) -> Dict[str, Any]:
        if not model_dict:
            return model_dict

        if model_type != ASpecialDerivedModel or "special_field" not in model_dict:
            return model_dict

        model_dict["special_field"] = pickle.loads(model_dict["special_field"].value)
        return model_dict

    @classmethod
    def _serialise_field(
        cls, model: Any, field_name: str, field_type: type, metadata: Mapping[str, Any], value: Any, **kwargs
    ) -> Any:
        if value is None:
            return None

        if field_type != NotSerializable:
            return super()._serialise_field(model, field_name, field_type, metadata, value, **kwargs)

        return pickle.dumps(value)


@pytest.fixture(scope="session", name="session_temp_ddb_table")
def create_session_temp_ddb_table() -> Generator[Table, None, None]:
    ddb = dynamodb()

    args = {
        "AttributeDefinitions": [
            {"AttributeName": "my_pk", "AttributeType": "S"},
            {"AttributeName": "my_sk", "AttributeType": "S"},
            {"AttributeName": "model_type", "AttributeType": "S"},
        ],
        "KeySchema": [{"AttributeName": "my_pk", "KeyType": "HASH"}, {"AttributeName": "my_sk", "KeyType": "RANGE"}],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "gsi_model_type",
                "KeySchema": [
                    {"AttributeName": "model_type", "KeyType": "HASH"},
                ],
                "Projection": {
                    "ProjectionType": "KEYS_ONLY",
                },
            },
        ],
    }

    table_name = f"pytest-{petname.Generate(words=4, separator='_')}"

    table = ddb.create_table(TableName=table_name, **args)  # type: ignore[arg-type]

    yield table

    table.delete()


@pytest.fixture(name="temp_table")
def create_temp_ddb_table(session_temp_ddb_table: Table) -> Table:
    table = session_temp_ddb_table
    result = table.scan(ProjectionExpression="my_pk, my_sk", ConsistentRead=True)
    with table.batch_writer(["my_pk", "my_sk"]) as writer:
        while True:
            items = result.get("Items", [])
            if not items:
                break
            for item in items:
                writer.delete_item({"my_pk": item["my_pk"], "my_sk": item["my_sk"]})
            if len(writer._items_buffer) > 0:  # type: ignore[attr-defined]
                writer._flush()  # type: ignore[attr-defined]
            if not result.get("LastEvaluatedKey"):
                break

    return table


@pytest.fixture(name="store")
def create_temp_store(temp_table: Table):
    return MyModelStore(temp_table.name)


async def test_store_get_item(store: MyModelStore):
    pk = uuid4().hex
    item = {"my_pk": pk, "my_sk": "#", "field": uuid4().hex}
    store.table.put_item(Item=item)

    result = await store.get_item(_MyModelKey(my_pk=pk, my_sk="#"))
    assert result
    assert result["field"] == item["field"]


async def test_store_put_get_item(store: MyModelStore):
    pk = uuid4().hex
    item = {"my_pk": pk, "my_sk": "#", "field": uuid4().hex}

    await store.put_item(item)

    result = await store.get_item(_MyModelKey(my_pk=pk, my_sk="#"))
    assert result
    assert result["field"] == item["field"]


async def test_store_put_get_model(store: MyModelStore):
    expected = MyDerivedModel(id=uuid4().hex)

    await store.put_model(expected)

    actual = await store.get_model(expected.model_key(), MyDerivedModel)

    assert actual
    assert isinstance(actual, MyDerivedModel)


async def test_store_put_update_get_model(store: MyModelStore):
    expected = MyDerivedModel(id=uuid4().hex)

    await store.put_model(expected)

    actual = await store.get_model(expected.model_key(), MyDerivedModel)

    assert actual
    assert isinstance(actual, MyDerivedModel)

    await store.update_item(
        expected.model_key(), UpdateExpression="SET optional_thing = :val", ExpressionAttributeValues={":val": "BOB"}
    )

    actual = await store.get_model(expected.model_key(), MyDerivedModel)
    assert actual
    assert actual.optional_thing == "BOB"


async def test_store_put_exists_delete_exists(store: MyModelStore):
    expected = MyDerivedModel(id=uuid4().hex)

    await store.put_model(expected)

    model = await store.get_model(expected.model_key(), MyDerivedModel)

    assert model

    exists = await store.item_exists(expected.model_key())
    assert exists

    await store.delete_item(expected.model_key())

    actual = await store.get_item(expected.model_key())

    assert actual is None

    exists = await store.item_exists(expected.model_key())
    assert not exists


async def test_store_put_model_if_not_exists(store: MyModelStore):
    original = MyDerivedModel(id=uuid4().hex, optional_thing=uuid4().hex)

    was_put = await store.put_model_if_not_exists(original)
    assert was_put

    new = MyDerivedModel(id=original.id, optional_thing=uuid4().hex)

    was_put = await store.put_model_if_not_exists(new)
    assert not was_put

    got = await store.get_model(new.model_key(), MyDerivedModel)
    assert got
    assert got.optional_thing == original.optional_thing


async def test_store_put_item_if_not_exists(store: MyModelStore):
    original = {"my_pk": uuid4().hex, "my_sk": "A", "field": uuid4().hex}

    was_put = await store.put_item_if_not_exists(original)
    assert was_put

    new = {"my_pk": original["my_pk"], "my_sk": "A", "field": uuid4().hex}

    was_put = await store.put_item_if_not_exists(new)
    assert not was_put

    got = await store.get_item(_MyModelKey(my_pk=new["my_pk"], my_sk=new["my_sk"]))
    assert got
    assert got["field"] == original["field"]


async def test_inject_custom_serialization(store: MyModelStore):
    put_model = ASpecialDerivedModel(id=uuid4().hex, special_field=NotSerializable(uuid4().hex))

    await store.put_model(put_model)

    got_model = await store.get_model(put_model.model_key(), ASpecialDerivedModel)

    assert got_model
    assert got_model.special_field is not None
    assert got_model.special_field.some_uuid == put_model.special_field.some_uuid
    assert got_model.events
    assert len(got_model.events) == 1
    assert isinstance(got_model.events[0], NestedItem)
    assert got_model.events[0].timestamp == put_model.events[0].timestamp


async def test_query_and_unpack_paged_items(store: MyModelStore):
    partition_key = "PK#1"

    async with store.batch_writer() as writer:
        for i in range(30):
            await writer.put_item({"my_pk": partition_key, "my_sk": f"SK#{i}"})

    items, last_evaluated_key = await store.query_items(KeyConditionExpression=Key("my_pk").eq(partition_key))

    assert len(items) == 30
    assert last_evaluated_key is None


async def test_query_and_unpack_paged_items_with_limit(store: MyModelStore):
    partition_key = "PK#1"

    async with store.batch_writer() as writer:
        for i in range(20):
            await writer.put_item({"my_pk": partition_key, "my_sk": f"SK#{i}"})

    items, last_evaluated_key = await store.query_items(KeyConditionExpression=Key("my_pk").eq(partition_key), Limit=10)

    assert len(items) == 10
    assert last_evaluated_key is not None


async def test_query_paginate_items(store: MyModelStore):
    partition_key = "PK#1"

    async with store.batch_writer() as writer:
        for i in range(20):
            await writer.put_item({"my_pk": partition_key, "my_sk": f"SK#{i}"})

    pages = [
        page
        async for page in store.paginate_items("query", KeyConditionExpression=Key("my_pk").eq(partition_key), Limit=10)
    ]

    assert len(pages) == 3
    assert len(pages[0].items) == 10
    assert len(pages[1].items) == 10
    assert len(pages[2].items) == 0
    assert pages[0].last_evaluated_key is not None
    assert pages[1].last_evaluated_key is not None
    assert pages[2].last_evaluated_key is None


async def test_try_deserialise_non_model(store: MyModelStore):
    with pytest.raises(TypeError):
        store.deserialise_model({"aa": 123}, NotSerializable)  # type: ignore[type-var]


async def test_serialize_deserialize_model(store: MyModelStore):
    model = MyDerivedModel(id=uuid4().hex, nested_items=[NestedItem(event="bob"), NestedItem(event="fin")])
    serialized = store.serialise_model(model)

    assert serialized
    assert serialized["my_pk"] == model.my_pk
    assert serialized["my_sk"] == model.my_sk
    assert serialized["model_type"] == MyDerivedModel.__name__
    assert serialized["id"] == model.id
    assert model.some_dict
    assert serialized["some_dict"] == model.some_dict
    assert serialized["last_modified"] == model.last_modified.isoformat()
    assert serialized["today"] == datetime.utcnow().date().isoformat()
    assert serialized["chaos"] == model.chaos
    if model.set_type:
        assert serialized["set_type"] == model.set_type
    else:
        assert "set_type" not in serialized

    if model.frozenset_type:
        assert serialized["frozenset_type"] == model.frozenset_type
    else:
        assert "frozenset_type" not in serialized

    assert len(serialized["nested_items"]) == 2
    assert isinstance(serialized["nested_items"][0], dict)
    assert isinstance(serialized["nested_item"], dict)
    assert serialized["nested_item"]["event"] == "created"

    assert model.none_string is None
    assert "none_thing" not in serialized
    assert model.none_list is None
    assert "none_list" not in serialized

    deserialized = store.deserialise_model(serialized, MyDerivedModel)
    assert deserialized
    assert deserialized.model_key() == model.model_key()
    assert deserialized.id == model.id
    assert deserialized.chaos == model.chaos
    assert len(deserialized.nested_items) == 2
    assert isinstance(deserialized.nested_items[0], NestedItem)
    assert isinstance(deserialized.nested_item, NestedItem)
    assert deserialized.nested_item.event == "created"
    assert deserialized.last_modified == model.last_modified
    assert deserialized.today == model.today


async def test_transact_get_put_model(store: MyModelStore):
    model = MyDerivedModel(id=uuid4().hex)
    await store.transact_write(actions=[{"Put": model}])

    got = await store.transact_get_model(model.model_key(), MyDerivedModel)

    assert got
    assert got.id == model.id
    assert got.union_date is not None
    assert isinstance(got.union_date, datetime)


async def test_transact_get_models(store: MyModelStore):
    models = [
        MyDerivedModel(id=uuid4().hex),
        ASpecialDerivedModel(id=uuid4().hex, special_field=NotSerializable(uuid4().hex)),
    ]

    await store.transact_write(actions=[{"Put": model} for model in models])

    keys = [
        models[-1].model_key(),
        _MyModelKey(my_pk=uuid4().hex, my_sk=uuid4().hex),
        _MyModelKey(my_pk=uuid4().hex, my_sk=uuid4().hex),
        models[0].model_key(),
    ]

    results = await store.transact_get_models(keys)

    assert len(results) == 4
    assert results[1] is None
    assert results[2] is None
    assert isinstance(results[0], ASpecialDerivedModel)
    assert isinstance(results[-1], MyDerivedModel)


async def test_query_all_items(store: MyModelStore):
    partition_key = "PK:1"

    async with store.batch_writer() as writer:
        for i in range(20):
            await writer.put_item({"my_pk": partition_key, "my_sk": f"SK:{i}"})

    items = await store.query_all_items(KeyConditionExpression=Key("my_pk").eq(partition_key))

    assert len(items) == 20

    items = await store.query_all_items(KeyConditionExpression=Key("my_pk").eq(partition_key), Limit=20)
    assert len(items) == 20


async def test_query_all_models(store: MyModelStore):
    async with store.batch_writer() as writer:
        for i in range(20):
            await writer.put_item(MyDerivedModel(id="1", sk_field=f"SK:{i}"))

    models = await store.query_all_models(MyDerivedModel, KeyConditionExpression=Key("my_pk").eq("AA#1"))

    assert len(models) == 20

    models = await store.query_all_models(MyDerivedModel, KeyConditionExpression=Key("my_pk").eq("AA#1"), Limit=10)
    assert len(models) == 20

    page = await store.query_models(MyDerivedModel, KeyConditionExpression=Key("my_pk").eq("AA#1"))
    assert len(page) == 20


async def test_batch_get_model(store: MyModelStore):
    models: List[MyBaseModel] = [MyDerivedModel(id=uuid4().hex) for _ in range(10)]
    models.extend([AnotherModel(id=uuid4().hex) for _ in range(10)])
    async with store.batch_writer() as writer:
        for model in models:
            await writer.put_item(model)

    got = await store.batch_get_model([model.model_key() for model in models])

    assert len(got) == 20
    assert all(isinstance(model, (MyDerivedModel, AnotherModel)) for model in got)


async def test_batch_get_item(store: MyModelStore):
    models: List[MyBaseModel] = [MyDerivedModel(id=uuid4().hex) for _ in range(10)]
    models.extend([AnotherModel(id=uuid4().hex) for _ in range(10)])
    async with store.batch_writer() as writer:
        for model in models:
            await writer.put_item(model)

    got = await store.batch_get_item([model.model_key() for model in models], ProjectionExpression="last_modified")

    assert len(got) == 20
    assert all(set(item.keys()) == {"last_modified"} for item in got)


async def test_batch_get_item_ordered(store: MyModelStore):
    models: List[MyBaseModel] = [MyDerivedModel(id=uuid4().hex) for _ in range(10)]
    models.extend([AnotherModel(id=uuid4().hex) for _ in range(10)])
    async with store.batch_writer() as writer:
        for model in models:
            await writer.put_item(model)

    got = cast(
        List[dict],
        await store.batch_get_item_ordered(
            [model.model_key() for model in models], ProjectionExpression="my_pk,my_sk,last_modified"
        ),
    )

    assert len(got) == 20
    assert all(set(item.keys()) == {"my_pk", "my_sk", "last_modified"} for item in got)


async def test_unregistered_model(store: MyModelStore):
    models: List[MyBaseModel] = [UnregisteredModel(id=uuid4().hex) for _ in range(10)]
    async with store.batch_writer() as writer:
        for model in models:
            await writer.put_item(model)

    with pytest.raises(KeyError):
        await store.batch_get_model([model.model_key() for model in models])


async def test_query_count(store: MyModelStore):
    partition_key = "PK:1"

    async with store.batch_writer() as writer:
        for i in range(100):
            await writer.put_item({"my_pk": partition_key, "my_sk": f"SK:{i}"})

    count = await store.query_count(KeyConditionExpression=Key("my_pk").eq(partition_key))

    assert count == 100

    count = await store.query_count(KeyConditionExpression=Key("my_pk").eq(partition_key), Limit=7)

    assert count == 100


async def test_get_all_models(store: MyModelStore):
    async with store.batch_writer() as writer:
        for _i in range(100):
            await writer.put_item(AnotherModel(uuid4().hex))

    models = await store.get_all_models(AnotherModel)

    assert len(models) == 100
    assert all(isinstance(model, AnotherModel) for model in models)


async def test_get_all_models_with_a_limit(store: MyModelStore):
    async with store.batch_writer() as writer:
        for _i in range(100):
            await writer.put_item(AnotherModel(uuid4().hex))

    models = await store.get_all_models(AnotherModel, max_models=10)

    assert len(models) == 10
    assert all(isinstance(model, AnotherModel) for model in models)


async def test_paginate_models(store: MyModelStore):
    partition_key = "BB#1"

    async with store.batch_writer() as writer:
        for i in range(100):
            await writer.put_item(AnotherModel(id="1", sk_field=f"SK:{i}"))

    pages = [
        page
        async for page in store.paginate_models(
            "query", AnotherModel, KeyConditionExpression=Key("my_pk").eq(partition_key), Limit=40
        )
    ]

    assert len(pages) == 3


async def test_paginate_models_from_index(store: MyModelStore):
    async with store.batch_writer() as writer:
        for i in range(100):
            await writer.put_item(AnotherModel(id="1", sk_field=f"SK:{i}"))

    pages = [
        page
        async for page in store.paginate_models_from_index(
            paginator_type="query",
            model_type=AnotherModel,
            index_name="gsi_model_type",
            KeyConditionExpression=Key("model_type").eq(AnotherModel.__name__),
            Limit=40,
        )
    ]

    assert len(pages) == 3
    for page in pages:
        assert all(isinstance(model, AnotherModel) for model in page.items)


async def test_paged_items():
    has_size = PagedItems(items=[1, 2, 3], last_evaluated_key={"my_pk": 123})
    assert has_size
    assert len(has_size) == 3

    empty_items: PagedItems = PagedItems(items=[], last_evaluated_key=None)
    assert not empty_items
    assert len(empty_items) == 0

    none_items: PagedItems = PagedItems(items=None, last_evaluated_key=None)  # type: ignore[arg-type]
    assert not none_items
    assert len(none_items) == 0

    has_key: PagedItems = PagedItems(items=[], last_evaluated_key={"my_pk": 456})
    assert not has_key
    assert len(has_key) == 0

    summed: PagedItems = PagedItems(items=[], last_evaluated_key=None)
    for page in [has_size, empty_items, none_items, has_key]:
        summed += page

    assert len(summed.items) == 3
    assert summed.last_evaluated_key == has_key.last_evaluated_key

    new_items = has_size + has_size
    assert len(new_items.items) == 6


async def test_bytes_serialisation(store: MyModelStore):
    bytes_in = b"test"
    model = MyDerivedModel(id=uuid4().hex, bytes_type=bytes_in)
    await store.put_model(model)
    stored = await store.get_model(model.model_key(), MyDerivedModel)
    assert stored
    assert isinstance(stored.bytes_type, bytes)
    assert stored.bytes_type == bytes_in


async def test_bytearray_serialisation(store: MyModelStore):
    bytes_in = bytearray(b"test")
    model = MyDerivedModel(id=uuid4().hex, bytearray_type=bytes_in)
    await store.put_model(model)
    stored = await store.get_model(model.model_key(), MyDerivedModel)
    assert stored
    assert isinstance(stored.bytearray_type, bytearray)
    assert stored.bytearray_type == bytes_in
