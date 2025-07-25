import pytest
from mimir.api.engine import Inquiry, AtomicQuery
from mimir.api.exceptions import MimirConfigError
import pyarrow as pa
import pyarrow.compute as pc

FIXTURES_PATH = "tests/fixtures"


@pytest.fixture
def mimir_engine(mimir_engine_factory):
    return mimir_engine_factory(
        config_path=f"{FIXTURES_PATH}/configs",
        secrets_path=f"{FIXTURES_PATH}/secrets",
    )


def test_get_source(mimir_engine):
    source = mimir_engine.get_source("my_source")
    assert source.name == "my_source"
    assert source.time_col == "created_at"


def test_get_metric(mimir_engine):
    metric = mimir_engine.get_metric("my_metric")
    assert metric.name == "my_metric"
    assert metric.source.name == "my_source"


def test_get_dimension(mimir_engine):
    dimension = mimir_engine.get_dimension("my_dimension")
    assert dimension.name == "my_dimension"


def test_get_schema(mimir_engine):
    schema = mimir_engine.get_schema()
    assert "my_source" in schema
    assert "metrics" in schema["my_source"]
    assert "dimensions" in schema["my_source"]
    assert "my_metric" in schema["my_source"]["metrics"]
    assert "my_dimension" in schema["my_source"]["dimensions"]


def test_inquiry(mimir_engine):
    inquiry = Inquiry(
        mimir_engine=mimir_engine,
        metrics=["my_metric"],
        dimensions=["my_dimension"],
    )
    assert len(inquiry.atomic_queries) == 1
    atomic_query = inquiry.atomic_queries[0]
    assert atomic_query.source.name == "my_source"
    assert len(atomic_query.metrics) == 1
    assert atomic_query.metrics[0].name == "my_metric"
    assert len(atomic_query.dimensions) == 1
    assert atomic_query.dimensions[0].name == "my_dimension"


def test_inquiry_dispatch(mimir_engine, mocker):
    # Mock AtomicQuery.execute to return a PyArrow Table
    mock_execute = mocker.patch.object(AtomicQuery, "execute")
    mock_execute.return_value = pa.Table.from_pydict(
        {
            "my_dimension": ["A", "B"],
            "my_metric": [10, 20],
        }
    )

    inquiry = Inquiry(
        mimir_engine=mimir_engine,
        metrics=["my_metric"],
        dimensions=["my_dimension"],
    )

    result_table = inquiry.dispatch()

    # Assert that execute was called for the atomic query
    mock_execute.assert_called_once()

    # Assert the content of the returned table
    assert "my_dimension" in result_table.column_names
    assert "my_metric" in result_table.column_names
    assert result_table.num_rows == 2
    assert pc.all(pc.equal(result_table["my_dimension"], pa.array(["A", "B"]))).as_py()
    assert pc.all(pc.equal(result_table["my_metric"], pa.array([10, 20]))).as_py()


def test_inquiry_no_dimensions(mimir_engine, mocker):
    """Tests an inquiry for a metric with no dimensions, expecting a single aggregated row."""
    mock_execute = mocker.patch.object(AtomicQuery, "execute")
    mock_execute.return_value = pa.Table.from_pydict({"my_metric": [100]})

    inquiry = Inquiry(mimir_engine=mimir_engine, metrics=["my_metric"])
    result_table = inquiry.dispatch()

    assert result_table.num_rows == 1
    assert "my_metric" in result_table.column_names
    assert result_table["my_metric"][0].as_py() == 100


def test_inquiry_empty_result(mimir_engine, mocker):
    """Tests an inquiry that is expected to return an empty result set."""
    mock_execute = mocker.patch.object(AtomicQuery, "execute")
    # The database would return an empty table with the correct schema
    mock_execute.return_value = pa.Table.from_pydict(
        {
            "my_dimension": pa.array([], type=pa.string()),
            "my_metric": pa.array([], type=pa.int64()),
        }
    )

    inquiry = Inquiry(
        mimir_engine=mimir_engine,
        metrics=["my_metric"],
        dimensions=["my_dimension"],
        global_filter="my_dimension = 'NonExistentValue'",
    )
    result_table = inquiry.dispatch()

    assert result_table.num_rows == 0
    assert "my_dimension" in result_table.column_names
    assert "my_metric" in result_table.column_names


def test_inquiry_complex_filter(mimir_engine, mocker):
    """Tests an inquiry with a more complex filter involving AND."""
    mock_execute = mocker.patch.object(AtomicQuery, "execute")
    mock_execute.return_value = pa.Table.from_pydict(
        {
            "my_dimension": ["A"],
            "my_metric": [15],
        }
    )

    inquiry = Inquiry(
        mimir_engine=mimir_engine,
        metrics=["my_metric"],
        dimensions=["my_dimension"],
        global_filter="my_dimension = 'A' AND my_metric > 10",
    )
    inquiry.dispatch()

    # The main purpose of this test is to ensure the filter is compiled correctly
    # into the atomic query's WHERE clause.
    atomic_query = inquiry.atomic_queries[0]
    assert "WHERE" in atomic_query.ast.sql()
    assert "my_dimension = 'A' AND my_metric > 10" in atomic_query.ast.sql()


def test_inquiry_multiple_dimensions(mimir_engine, mocker):
    """Tests an inquiry that groups by more than one dimension."""
    mock_execute = mocker.patch.object(AtomicQuery, "execute")
    mock_execute.return_value = pa.Table.from_pydict(
        {
            "my_dimension": ["A", "A", "B"],
            "my_other_dimension": ["X", "Y", "Y"],
            "my_metric": [10, 20, 30],
        }
    )

    inquiry = Inquiry(
        mimir_engine=mimir_engine,
        metrics=["my_metric"],
        dimensions=["my_dimension", "my_other_dimension"],
    )
    result_table = inquiry.dispatch()

    assert result_table.num_rows == 3
    assert "my_dimension" in result_table.column_names
    assert "my_other_dimension" in result_table.column_names
    # Check that the GROUP BY clause includes both dimensions (by position)
    assert "GROUP BY 1, 2" in inquiry.atomic_queries[0].ast.sql()


def test_inquiry_order_by(mimir_engine, mocker):
    """Tests an inquiry that uses the order_by parameter."""
    mock_execute = mocker.patch.object(AtomicQuery, "execute")
    mock_execute.return_value = pa.Table.from_pydict(
        {
            "my_dimension": ["B", "A"],
            "my_metric": [20, 10],
        }
    )

    inquiry = Inquiry(
        mimir_engine=mimir_engine,
        metrics=["my_metric"],
        dimensions=["my_dimension"],
        order_by="my_metric DESC",
    )
    inquiry.dispatch()

    # Check that the ORDER BY clause is correctly added to the final combined query
    final_sql = inquiry._combine_queries().sql()
    assert "ORDER BY my_metric DESC" in final_sql


def test_inquiry_invalid_metric_source(mimir_engine):
    """Tests that an error is raised for a metric with a non-existent source."""
    with pytest.raises(
        FileNotFoundError,
        match="No file matching for configuration pattern: my_metric_bad_source",
    ):
        mimir_engine.get_metric("my_metric_bad_source")


def test_inquiry_invalid_dimension_for_source(mimir_engine):
    """Tests that an error is raised when a dimension is not available for a source."""
    with pytest.raises(
        MimirConfigError, match="Invalid columns for source 'my_source'"
    ):
        Inquiry(
            mimir_engine=mimir_engine,
            metrics=["my_metric"],
            dimensions=["some_other_dimension"],
        )


def test_inquiry_multi_source(mimir_engine):
    """Tests a valid inquiry that spans multiple data sources."""
    inquiry = Inquiry(
        mimir_engine=mimir_engine,
        metrics=["my_metric", "stock_level"],
        dimensions=["my_dimension"],
    )

    assert len(inquiry.atomic_queries) == 2
    assert {aq.source.name for aq in inquiry.atomic_queries} == {
        "my_source",
        "inventory",
    }

    # Check that the final query joins the two atomic queries
    final_sql = inquiry._combine_queries().sql()
    assert "FULL JOIN" in final_sql
    assert "USING (my_dimension)" in final_sql
