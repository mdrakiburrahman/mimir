from enum import Enum

import sqlglot

import typing as t


class GRANULARITY(Enum):
    """An enum for the different granularities that can be used in an inquiry."""

    def __init__(self, alias: str, parse_function: t.Callable):
        self._alias = alias
        self._parse_function = parse_function

    @property
    def alias(self) -> str:
        """The alias of the granularity."""
        return self._alias

    def _get_granularity_expression(self, column_name: str) -> sqlglot.exp.Expression:
        """Returns the SQL expression for the granularity."""
        expr = self._parse_function(column_name)
        assert isinstance(expr, sqlglot.exp.Expression)
        return expr

    TIME = ("ts", lambda col: sqlglot.parse_one(f"{col} as ts"))
    DATE = ("ds", lambda col: sqlglot.parse_one(f"DATE({col}) as ds"))
    MONTH = (
        "year_month",
        lambda col: sqlglot.parse_one(f"DATE_TRUNC('month', {col}) as year_month"),
    )
    YEAR = (
        "year",
        lambda col: sqlglot.parse_one(f"DATE_TRUNC('year', {col}) as year_month"),
    )


class CONFIG_TYPE(Enum):
    """An enum for the different types of configurations."""

    DIMENSION = "dimension"
    METRIC = "metric"
    SOURCE = "source"
