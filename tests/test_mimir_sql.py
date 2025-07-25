import pytest
from mimir.sql.mimir_sql import MimirSql
from mimir.api.exceptions import MimirNotImplementedError


def test_mimir_sql_valid_query():
    sql = "SELECT my_dim, AGG(my_metric) FROM mimir.metrics"
    mimir_sql = MimirSql(sql)
    assert mimir_sql.sql == sql


def test_mimir_sql_multiple_queries():
    sql = "SELECT 1; SELECT 2"
    with pytest.raises(MimirNotImplementedError):
        MimirSql(sql).validate()


def test_mimir_sql_with_cte():
    sql = "WITH my_cte AS (SELECT 1) SELECT * FROM my_cte"
    with pytest.raises(MimirNotImplementedError):
        MimirSql(sql).validate()


def test_mimir_sql_with_subquery():
    sql = "SELECT * FROM (SELECT 1)"
    with pytest.raises(MimirNotImplementedError):
        MimirSql(sql).validate()


def test_parse_inquiry():
    sql = "SELECT my_dim, AGG(my_metric) FROM mimir.metrics WHERE my_dim = 'a' ORDER BY my_dim"
    mimir_sql = MimirSql(sql)
    inquiry = mimir_sql.parse_inquiry()
    assert inquiry["dimensions"] == ["my_dim"]
    assert inquiry["metrics"] == ["my_metric"]
    assert inquiry["global_filter"] == "my_dim = 'a'"
    assert inquiry["order_by"] == "my_dim"
    assert "SELECT my_dim, my_metric" in inquiry["client_sql"]
