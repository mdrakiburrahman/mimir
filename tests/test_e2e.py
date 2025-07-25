import pytest
import time
from mimir.api.client import Client


@pytest.fixture(scope="module")
def client():
    """A fixture that provides a client to the running API server."""
    client = Client(uri="http://localhost:8090")

    # Wait for the API server to be ready
    for _ in range(20):  # 20 attempts, 1 second apart
        try:
            if client.get_schema():
                break
        except Exception:
            time.sleep(1)
    else:
        pytest.fail("API server did not become available in time.")

    yield client


@pytest.mark.e2e
def test_e2e_inquiry(client):
    """A simple E2E test that queries the running API."""
    inquiry_request = {
        "metrics": ["movies_rented", "rentals_revenue"],
        "dimensions": ["dim_rental_category"],
        "global_filter": "dim_rental_category = 'Action'",
    }

    result_table = client.query(**inquiry_request)

    assert result_table.num_rows == 1
    assert "dim_rental_category" in result_table.column_names
    assert "movies_rented" in result_table.column_names
    assert "rentals_revenue" in result_table.column_names

    # Check the values
    result_dict = result_table.to_pydict()
    assert result_dict["dim_rental_category"][0] == "Action"
    # The exact values depend on the sample data, but we can check they are not null
    assert result_dict["movies_rented"][0] is not None
    assert result_dict["rentals_revenue"][0] is not None


@pytest.mark.e2e
def test_e2e_inquiry_no_dimensions(client):
    """Tests an E2E inquiry with no dimensions, expecting a single aggregated row."""
    inquiry_request = {
        "metrics": ["movies_rented", "rentals_revenue"],
    }

    result_table = client.query(**inquiry_request)

    assert result_table.num_rows == 1
    assert "movies_rented" in result_table.column_names
    assert "rentals_revenue" in result_table.column_names
    assert result_table["movies_rented"][0].as_py() is not None
    assert result_table["rentals_revenue"][0].as_py() is not None


@pytest.mark.e2e
def test_e2e_multi_source_inquiry(client):
    """Tests an E2E inquiry that spans both the Postgres and DuckDB sources."""
    inquiry_request = {
        "metrics": ["movies_rented", "stock_level"],
    }

    result_table = client.query(**inquiry_request)

    assert result_table.num_rows == 1
    assert "movies_rented" in result_table.column_names
    assert "stock_level" in result_table.column_names


@pytest.mark.e2e
def test_e2e_multi_source_inquiry_with_granularity(client):
    """Tests a multi-source inquiry with a time granularity."""
    inquiry_request = {
        "metrics": ["movies_rented", "stock_level"],
        "granularity": "DATE",
    }

    result_table = client.query(**inquiry_request)

    assert result_table.num_rows > 1
    assert "ds" in result_table.column_names
    assert "movies_rented" in result_table.column_names
    assert "stock_level" in result_table.column_names
