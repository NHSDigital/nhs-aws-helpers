from typing import Generator

import petname  # type: ignore[import]
import pytest
from mypy_boto3_dynamodb.service_resource import Table

from nhs_aws_helpers import dynamodb
from nhs_aws_helpers.fixtures import temp_dynamodb_table


@pytest.fixture(scope="session", name="source_ddb_table")
def a_ddb_table() -> Generator[Table, None, None]:
    ddb = dynamodb()

    args = {
        "AttributeDefinitions": [
            {"AttributeName": "my_pk", "AttributeType": "S"},
            {"AttributeName": "my_sk", "AttributeType": "S"},
            {"AttributeName": "model_type", "AttributeType": "S"},
        ],
        "KeySchema": [{"AttributeName": "my_pk", "KeyType": "HASH"}, {"AttributeName": "my_sk", "KeyType": "RANGE"}],
        "ProvisionedThroughput": {"ReadCapacityUnits": 500, "WriteCapacityUnits": 500},
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "string",
                "KeySchema": [
                    {"AttributeName": "model_type", "KeyType": "HASH"},
                ],
                "Projection": {
                    "ProjectionType": "KEYS_ONLY",
                },
                "ProvisionedThroughput": {"ReadCapacityUnits": 500, "WriteCapacityUnits": 500},
            },
        ],
    }

    table_name = f"pytest-{petname.Generate(words=4, separator='_')}"

    table = ddb.create_table(TableName=table_name, **args)  # type: ignore[arg-type]

    yield table

    table.delete()


@pytest.fixture(name="cloned_table_defaults")
def _on_demand_table(source_ddb_table: Table) -> Generator[Table, None, None]:
    yield from temp_dynamodb_table(source_ddb_table.table_name)


@pytest.fixture(name="cloned_table_provisioned")
def _provisioned_table(source_ddb_table: Table) -> Generator[Table, None, None]:
    yield from temp_dynamodb_table(source_ddb_table.table_name, on_demand_billing_mode=False, provisioned_capacity=99)


def test_default_temp_table(cloned_table_defaults: Table):
    assert cloned_table_defaults.billing_mode_summary["BillingMode"] == "PAY_PER_REQUEST"


def test_provisioned_temp_table(cloned_table_provisioned: Table):
    assert cloned_table_provisioned.provisioned_throughput == {"ReadCapacityUnits": 99, "WriteCapacityUnits": 99}
