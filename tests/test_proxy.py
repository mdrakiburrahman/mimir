import pytest
from unittest.mock import MagicMock
from mimir.sql.proxy import MimirProxySession
import pyarrow as pa


@pytest.fixture
def mock_mimir_client(mocker):
    client = MagicMock()
    client.get_schema.return_value = {
        "my_table": {
            "time_dimension": "ts",
            "dimensions": ["my_dim"],
            "metrics": ["my_metric"],
        }
    }
    client.query.return_value = pa.Table.from_pydict(
        {"my_dim": ["a"], "my_metric": [1]}
    )
    return client


@pytest.mark.asyncio
async def test_proxy_schema(mock_mimir_client):
    session = MimirProxySession(mimir_client=mock_mimir_client)
    schema = await session.schema()
    assert "mimir" in schema
    assert "my_table" in schema["mimir"]["mimir"]
    table_schema = schema["mimir"]["mimir"]["my_table"]
    assert "ts" in table_schema
    assert "my_dim" in table_schema
    assert "my_metric" in table_schema


@pytest.mark.asyncio
async def test_proxy_query_mimir(mock_mimir_client):
    session = MimirProxySession(mimir_client=mock_mimir_client)
    sql = "SELECT my_dim, AGG(my_metric) FROM mimir.metrics"
    rows, cols = await session.query(None, sql, {})
    assert len(rows) == 1
    assert len(cols) == 2
    assert "my_dim" in cols
    assert "my_metric" in cols
    mock_mimir_client.query.assert_called_once()


@pytest.mark.asyncio
async def test_proxy_query_duckdb(mock_mimir_client):
    session = MimirProxySession(mimir_client=mock_mimir_client)
    sql = "SELECT 1"
    rows, cols = await session.query(None, sql, {})
    assert len(rows) == 1
    assert len(cols) == 1
    assert rows[0][0] == 1
    assert cols[0] == "1"
    mock_mimir_client.query.assert_not_called()
