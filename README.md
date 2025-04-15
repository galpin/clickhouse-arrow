# Clickhouse Arrow 🏠 🏹

A minimal [ClickHouse](https://clickhouse.com) client that uses the HTTP API and Apache Arrow.

You should probably use [Clickhouse Connect](https://github.com/ClickHouse/clickhouse-connect) instead.

### Installation

```bash
pip install clickhouse_arrow
```

### Examples

```python
import clickhouse_arrow as ch
import pyarrow as pa

# Initialise a client
client = ch.Client("http://localhost:8123", password="password")

# Create a table
client.execute(
    """
    CREATE TABLE test (
        col1 Int64,
        col2 String
    )
    ENGINE = Memory
    """,
)

# Import a table
table = pa.Table.from_pydict(
    {
        "col1": [1, 2, 3],
        "col2": ["a", "b", "d"],
    },
)
client.insert("test", table)

# Read into a table
table = client.read_table("SELECT * FROM test")
print(table)

# Read iterator of batches
batches = client.read_batches("SELECT * FROM test")
for batch in batches:
    print(batch)

# Use query parameters
table = client.read_table(
    """
    SELECT * FROM test
    WHERE col1 = {value:Int64}
    """,
    params={"value": 2},
)
print(table)

# Use query settings
table = client.read_table(
    "SELECT col2 FROM test",
    settings={"output_format_arrow_string_as_string": 1},
)
print(table["col2"])

# Use table schema
table = client.read_table(
    "SELECT * FROM test",
    schema = pa.schema(
        [
            ("col1", pa.int64()),
            ("col2", pa.binary()),
        ]
    ),
)
```
