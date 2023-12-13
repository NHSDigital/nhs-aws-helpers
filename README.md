# NHS AWS Helpers

some useful boto3 utilities

# quick start

### contributing
contributors see [contributing](CONTRIBUTING.md)

### installing
```shell
pip install nhs-aws-helpers
```

## testing

the library comes with a some pytest  fixtures ... [nhs_aws_helpers/fixtures.py](nhs_aws_helpers/fixtures.py)


```python
# conftest.py
# noinspection PyUnresolvedReferences
from nhs_aws_helpers.fixtures import *  # noqa: F403

# mytest.py
import pytest
from mypy_boto3_dynamodb.service_resource import Table

@pytest.fixture(scope="function", name="my_temp_ddb_table")
def my_temp_table_fixture() -> Table:
    yield from temp_dynamodb_table("my-source-table-to-clone")


def my_test(my_temp_ddb_table: Table):
    # do things with table
    print(my_temp_ddb_table.table_name)

```
