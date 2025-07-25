import pytest
from mimir.api.engine import Inquiry
import pyarrow as pa

FIXTURES_PATH = "tests/fixtures"


@pytest.fixture
def mimir_engine_with_duckdb(mimir_engine_factory):
    """Fixture to create a MimirEngine that uses the file-based DuckDB test source."""
    return mimir_engine_factory(
        config_path=f"{FIXTURES_PATH}/configs",
        secrets_path=f"{FIXTURES_PATH}/secrets",
    )


@pytest.mark.integration
def test_inquiry_with_duckdb_source(mimir_engine_with_duckdb, mocker):
    """Test a simple inquiry against the file-based DuckDB source."""
    mocker.patch.object(
        mimir_engine_with_duckdb.config_loader,
        "get_secret",
        return_value={
            "connection_class": "duckdb",
            "path": f"{FIXTURES_PATH}/data/inventory.csv",
        },
    )
    inquiry = Inquiry(
        mimir_engine=mimir_engine_with_duckdb,
        metrics=["stock_level"],
        dimensions=["product_name"],
    )
    result_table = inquiry.dispatch()

    assert result_table.num_rows == 3
    assert "product_name" in result_table.column_names
    assert "stock_level" in result_table.column_names

    # Sort both tables to ensure comparison is correct
    result_table = result_table.sort_by([("product_name", "ascending")])

    expected_data = pa.Table.from_pydict(
        {
            "product_name": ["Keyboard", "Laptop", "Mouse"],
            "stock_level": pa.array([75, 15, 120], type=pa.decimal128(38, 0)),
        }
    ).sort_by([("product_name", "ascending")])

    assert result_table.equals(expected_data)
