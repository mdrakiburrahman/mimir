from datetime import datetime, timedelta
import functools
import itertools

import sqlglot
import sqlglot.expressions
from pydantic import BaseModel, Field, model_validator, ConfigDict

from mimir.api.connections import Connection
from mimir.api.exceptions import MimirConfigError

import typing as t


@functools.total_ordering
class BaseDefinition(BaseModel):
    """Base Pydantic model for all Mimir definitions.

    This class provides the common attributes and functionality for all
    definitions, such as being hashable and sortable by name.

    Attributes:
        name: The unique name of the definition.
        sql: An optional SQL expression associated with the definition.
        description: An optional description for the definition.
    """

    name: str
    sql: t.Optional[str] = None
    description: t.Optional[str] = None

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    def __eq__(self, other):
        if not isinstance(other, BaseDefinition):
            return NotImplemented
        return self.name == other.name and self.__class__ == other.__class__

    def __lt__(self, other):
        if not isinstance(other, BaseDefinition):
            return NotImplemented
        return self.name < other.name

    def __hash__(self):
        return hash((self.name, self.__class__))


class Dimension(BaseDefinition):
    """Represents a categorical column that can be used for grouping.

    Attributes:
        source_name: The name of the Source this dimension belongs to.
            Defaults to 'local' for dimensions defined directly on a source.
    """

    source_name: t.Optional[str] = Field(default="local")


class Source(BaseDefinition):
    """Represents a data source, typically a table or a view.

    This class defines the connection information and available columns for a
    data source that Mimir can query.

    Attributes:
        time_col: The name of the primary time column for this source.
        connection_name: The name of the connection to use for this source.
        source_dimensions: A list of other dimensions available from this source.
        time_col_alias: An optional alias for the time column.
        local_dimensions: A list of dimensions defined directly in the source's SQL.
        connection: The runtime database connection instance.
    """

    time_col: str
    source_dimensions: t.List[str] = Field(alias="dimensions", default_factory=list)
    time_col_alias: t.Optional[str] = None
    local_dimensions: t.Optional[t.List[str]] = None
    connection: t.Optional[Connection] = None

    @model_validator(mode="after")
    def _initialize_derived_fields(self) -> "Source":
        """Initializes derived fields after the model is validated."""
        if not self.sql:
            raise MimirConfigError("sql field is needed in the source config")

        time_col_alias = self.time_col_alias or self.time_col
        self.time_col_alias = time_col_alias
        self.local_dimensions = [
            col.alias_or_name
            for col in sqlglot.parse_one(self.sql).expressions
            if col.alias_or_name != time_col_alias
        ]

        return self

    def _validate_columns(
        self,
        column_names: list[str],
        metric_names: t.Optional[list[str]] = None,
        granularity_alias: t.Optional[str] = None,
        error_message: t.Optional[str] = None,
    ):
        """Validates that a list of columns are available in this source."""
        error_message = (
            error_message
            or "The following dimensions are missing from the source config: "
        )

        candidates = [
            self.local_dimensions,
            self.source_dimensions,
            metric_names,
            [self.time_col_alias],
        ]
        allowed_cols = list(
            itertools.chain.from_iterable(
                col_list for col_list in candidates if col_list
            )
        )

        if granularity_alias:
            allowed_cols.append(granularity_alias)

        unavailable_cols = [col for col in column_names if col not in allowed_cols]
        if unavailable_cols:
            raise MimirConfigError(
                f"Invalid columns for source '{self.name}'. {error_message} ({', '.join(unavailable_cols)})"
            )

    def validate_dimensions(self, dimensions: list[Dimension | str]):
        """Validates that a list of dimensions are available in this source."""
        self._validate_columns(
            [dim.name if isinstance(dim, Dimension) else dim for dim in dimensions]
        )

    def validate_conditions(
        self,
        where: t.Optional[sqlglot.exp.Where] = None,
        metric_names: t.Optional[list[str]] = None,
    ):
        """Validates that columns in a WHERE clause are available in this source."""
        if not where:
            return
        self._validate_columns(
            [identifier.this for identifier in where.find_all(sqlglot.exp.Identifier)],
            metric_names=metric_names,
        )

    def validate_sort(
        self,
        order_by: t.Optional[sqlglot.exp.Ordered] = None,
        metric_names: t.Optional[list[str]] = None,
        granularity_alias: t.Optional[str] = None,
    ):
        """Validates that columns in an ORDER BY clause are available in this source."""
        if not order_by:
            return
        self._validate_columns(
            [
                identifier.this
                for sorting in order_by
                for identifier in sorting.find_all(sqlglot.exp.Identifier)
            ],
            metric_names=metric_names,
            granularity_alias=granularity_alias,
        )

    def compile_source(
        self,
        dimensions: t.Optional[t.List[Dimension]] = None,
        start_date: t.Optional[datetime] = None,
        end_date: t.Optional[datetime] = None,
    ) -> sqlglot.expressions.Select:
        """Compiles the source SQL, adding dimension columns and date filters.

        Args:
            dimensions: A list of Dimension objects to join into the source query.
            start_date: The start date for a time-based filter.
            end_date: The end date for a time-based filter.

        Returns:
            A sqlglot Select expression representing the compiled source query.
        """
        if not self.sql:
            raise MimirConfigError(
                f"Source '{self.name}' has no SQL expression defined."
            )

        source_ast = sqlglot.parse_one(self.sql)
        assert isinstance(source_ast, sqlglot.exp.Select)
        if dimensions:
            source_ast = source_ast.select(
                *itertools.chain(
                    *[
                        sqlglot.parse_one(dim.sql).expressions
                        for dim in dimensions
                        if dim.source_name != "local" and dim.sql
                    ]
                ),
                append=True,
            )
        if start_date:
            source_ast = source_ast.where(
                f"{self.time_col} >= '{start_date.strftime('%Y-%m-%d')}'"
            )
        if end_date:
            source_ast = source_ast.where(
                f"{self.time_col} < '{(end_date + timedelta(days=1)).strftime('%Y-%m-%d')}'"
            )
        return source_ast


class Metric(BaseDefinition):
    """Represents a numerical or aggregate value that can be queried.

    Attributes:
        source_name: The name of the Source this metric belongs to.
        required_dimensions: A list of dimensions that must be included when
            querying this metric.
        source: The runtime Source object this metric is associated with.
    """

    source: Source
    required_dimensions: t.Optional[t.List[str]] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate(self) -> "Metric":
        """Validates the metric after the model is validated."""
        if not self.sql:
            raise MimirConfigError("sql field is needed in the source config")

        return self
