from __future__ import annotations
import sqlglot
import duckdb
import logging
import functools
from datetime import datetime
import pyarrow as pa
import os
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed

from pydantic import ValidationError

from mimir.api.loaders import BaseConfigLoader
from mimir.api.types import GRANULARITY, CONFIG_TYPE
from mimir.api.definitions import Source, Dimension, Metric
from mimir.api.connections import ConnectionFactory
from mimir.shared import ttl_cache
from mimir.api.exceptions import MimirConfigError, MimirQueryError

import typing as t

logger = logging.getLogger(__name__)


class MimirEngine:
    """The main entry point for interacting with the Mimir semantic layer.

    This engine is responsible for loading configurations, instantiating
    definitions, and providing an interface to query the underlying data sources.

    Args:
        config_loader: An object that conforms to the BaseConfigLoader interface,
            used to fetch raw configuration data.
    """

    def __init__(
        self, config_loader: BaseConfigLoader, validate_connections: bool = True
    ) -> None:
        """Initializes the MimirEngine.

        Args:
            config_loader: The configuration loader to use.
            validate_connections: If False, the engine will not attempt to
                create database connections, allowing for validation of configs
                without secrets.
        """
        self.config_loader = config_loader
        self.validate_connections = validate_connections

    def get_secret(self, secret_name: str) -> t.Optional[t.Dict[str, t.Any]]:
        """Retrieves a secret by name.

        Args:
            secret_name: The name of the secret to retrieve.

        Returns:
            A dictionary containing the secret.
        """
        return self.config_loader.get_secret(secret_name)

    def _init_source(self, conf: t.Dict[str, t.Any]) -> Source:
        """Initializes a Source object from a configuration dictionary."""
        if not conf.get("connection_name"):
            raise MimirConfigError(
                f"The following source config is missing the required parameter 'connection_name': {conf}"
            )

        connection = None
        if self.validate_connections:
            connection_config = self.get_secret(conf["connection_name"])
            if not connection_config:
                raise MimirConfigError(
                    f"Secret '{conf['connection_name']}' not found for source '{conf.get('name', 'unknown')}'"
                )

            if "CONNECTION_HOST" in os.environ:
                connection_config["host"] = os.environ["CONNECTION_HOST"]

            connection = ConnectionFactory(
                connection_name=conf["connection_name"],
                connection_config=connection_config,
            ).create_connection_instance()

        return Source(**conf, connection=connection)

    def _init_dimension(self, conf: t.Dict[str, t.Any]) -> Dimension:
        """Initializes a Dimension object from a configuration dictionary."""

        return Dimension(**conf)

    def _init_metric(self, conf: t.Dict[str, t.Any]) -> Metric:
        """Initializes a Metric object from a configuration dictionary."""
        if not conf.get("source_name"):
            raise MimirConfigError(
                f"The following metric config is missing the required parameter 'source_name': {conf}"
            )

        return Metric(**conf, source=self.get_source(conf["source_name"]))

    @ttl_cache(60)
    def get_source(self, name: str) -> Source:
        """Builds and returns a single Source object by name.

        Args:
            name: The unique name of the source.

        Returns:
            An initialized Source object.

        Raises:
            MimirConfigError: If the source is not found or its configuration is invalid.
        """
        try:
            conf = self.config_loader.get(CONFIG_TYPE.SOURCE, name)
            if not conf:
                raise MimirConfigError(
                    f"Invalid or missing configuration for source '{name}'"
                )
            return self._init_source(conf)
        except (ValidationError, KeyError) as e:
            raise MimirConfigError(
                f"Invalid or missing configuration for source '{name}'"
            ) from e

    @ttl_cache(60)
    def get_dimension(self, name: str) -> Dimension:
        """Builds and returns a single Dimension object by name.

        Args:
            name: The unique name of the dimension.

        Returns:
            An initialized Dimension object.
        """
        return self._init_dimension(
            self.config_loader.get(CONFIG_TYPE.DIMENSION, name)
            or {
                "source": "local",
                "name": name,
            }
        )

    @ttl_cache(60)
    def get_metric(self, name: str) -> Metric:
        """Builds and returns a single Metric object by name.

        Args:
            name: The unique name of the metric.

        Returns:
            An initialized Metric object.
        """
        conf = self.config_loader.get(CONFIG_TYPE.METRIC, name)
        if not conf:
            raise MimirConfigError(
                f"Invalid or missing configuration for metric '{name}'"
            )
        return self._init_metric(conf)

    @ttl_cache(60)
    def get_sources(self) -> t.List[Source]:
        """Builds and returns a list of all available Source objects."""
        return [
            self._init_source(conf)
            for conf in self.config_loader.get_all(CONFIG_TYPE.SOURCE).values()
        ]

    @ttl_cache(60)
    def get_dimensions(self) -> t.List[Dimension]:
        """Builds and returns a list of all available Dimension objects."""
        return [
            self._init_dimension(conf)
            for conf in self.config_loader.get_all(CONFIG_TYPE.DIMENSION).values()
        ]

    @ttl_cache(60)
    def get_metrics(self) -> t.List[Metric]:
        """Builds and returns a list of all available Metric objects."""
        return [
            self._init_metric(conf)
            for conf in self.config_loader.get_all(CONFIG_TYPE.METRIC).values()
        ]

    @ttl_cache(60)
    def get_schema(self) -> t.Dict[str, t.Dict[str, t.Any]]:
        """Returns a schema of all sources and their associated metrics and dimensions."""
        return {
            source.name: {
                "dimensions": source.local_dimensions + source.source_dimensions,
                "metrics": [
                    m.name for m in self.get_metrics() if m.source_name == source.name
                ],
                "time_dimension": source.time_col_alias,
            }
            for source in self.get_sources()
        }


class AtomicQuery:
    """Represents a single, executable query to a data source.

    This class is an internal component of an Inquiry. It builds the SQL for a
    given set of metrics and dimensions against a single source and executes it.

    Args:
        source: The Source object to query.
        mimir_engine: The MimirEngine instance.
        metrics: A list of Metric objects to include in the query.
        dimensions: A list of Dimension objects to group by.
        start_date: The start date for the query.
        end_date: The end date for the query.
        granularity: The time granularity for the query.
        global_filter: A SQL WHERE clause to apply to the query.
    """

    def __init__(
        self,
        source: Source,
        mimir_engine: MimirEngine,
        metrics: t.List[Metric],
        dimensions: t.Optional[list[Dimension]] = None,
        start_date: t.Optional[str] = None,
        end_date: t.Optional[str] = None,
        granularity: t.Optional[GRANULARITY] = None,
        global_filter: t.Optional[sqlglot.exp.Where] = None,
    ):
        self.name = f"tbl_{os.urandom(15).hex()}"
        self.mimir_engine = mimir_engine
        self.source = source
        self.dimensions = dimensions if dimensions else list()
        self.metrics = metrics
        self.required_dimensions = {
            self.mimir_engine.get_dimension(dim)
            for metric in self.metrics
            for dim in metric.required_dimensions or list()
            if dim not in [d.name for d in self.dimensions]
        }
        self.start_date = start_date
        self.end_date = end_date
        self.granularity = granularity
        self.global_filter = global_filter
        self.ast = self._build_sql()

    def __repr__(self):
        return f""" 
        source : {self.source}
        dimensions: {self.dimensions}
        metrics: {self.metrics} 
        filters: {self.global_filter} 
        granularity: {self.granularity}
        """

    def _compile_metrics_request(self) -> sqlglot.expressions.Select:
        """Compiles the SQL for the metrics and dimensions in the query."""
        metrics_expressions = itertools.chain(
            *(
                sqlglot.parse_one(metric.sql).expressions
                for metric in self.metrics
                if metric.sql
            )
        )
        dim_expressions = list(
            filter(
                None,
                itertools.chain(
                    [
                        self.granularity._get_granularity_expression(
                            self.source.time_col_alias  # type: ignore
                        )
                        if self.granularity
                        else None
                    ],
                    [dim.name for dim in self.dimensions],
                ),
            )
        )
        ast = (
            sqlglot.exp.select(*dim_expressions, *metrics_expressions)  # type: ignore
            .from_(self.source.name)
            .group_by(
                *[
                    sqlglot.exp.Literal.number(i)
                    for i in range(1, len(dim_expressions) + 1)
                ]
            )  # type: ignore
        )
        return ast

    def _build_sql(self):
        """Builds the final SQL for the query."""
        start_date_dt = (
            datetime.strptime(self.start_date, "%Y-%m-%d") if self.start_date else None
        )
        end_date_dt = (
            datetime.strptime(self.end_date, "%Y-%m-%d") if self.end_date else None
        )
        compiled_source = self.source.compile_source(
            dimensions=list(
                filter(None, [*self.dimensions, *self.required_dimensions])
            ),
            start_date=start_date_dt,
            end_date=end_date_dt,
        )
        return (
            self._compile_metrics_request()
            .from_(self.source.name)
            .with_(alias=self.source.name, as_=compiled_source)
            .where(self.global_filter)
        )

    def execute(self):
        """Executes the query and returns the results as a pyarrow Table."""
        if not self.source.connection:
            raise MimirQueryError(
                f"Source '{self.source.name}' has no active connection."
            )
        return self.source.connection.query(sql=self.ast.sql())


class Inquiry:
    """Represents a request for data, orchestrating one or more AtomicQueries.

    This is the primary class that users interact with to query Mimir. It splits
    a request across multiple data sources, executes them in parallel, and joins
    the results.

    Args:
        mimir_engine: The MimirEngine instance.
        metrics: A list of metric names to include in the inquiry.
        dimensions: A list of dimension names to group by.
        start_date: The start date for the inquiry (YYYY-MM-DD).
        end_date: The end date for the inquiry (YYYY-MM-DD).
        global_filter: A SQL WHERE clause to apply to the inquiry.
        granularity: The time granularity for the inquiry.
        order_by: The column to order the final results by.
        client_sql: A SQL query to use to combine the results of the atomic queries.
    """

    def __init__(
        self,
        mimir_engine: MimirEngine,
        metrics: list,
        dimensions: t.Optional[t.List[str]] = None,
        start_date: t.Optional[str] = None,
        end_date: t.Optional[str] = None,
        global_filter: t.Optional[str] = None,
        granularity: t.Optional[str] = None,
        order_by: t.Optional[str] = None,
        client_sql: t.Optional[str] = None,
    ):
        where_clause = sqlglot.select().where(global_filter).find(sqlglot.exp.Where)
        order_by_clause = sqlglot.select().order_by(order_by).find(sqlglot.exp.Order)
        self.mimir_engine = mimir_engine
        self.dimensions = [
            mimir_engine.get_dimension(dim) for dim in (dimensions or [])
        ]
        self.metrics = sorted(
            [mimir_engine.get_metric(metric) for metric in metrics],
            key=lambda metric: metric.source.name,
        )
        self.start_date = start_date
        self.end_date = end_date

        self.global_filter = where_clause.this if where_clause else None
        self.granularity = GRANULARITY[granularity] if granularity else None

        self.order_by = order_by_clause.expressions if order_by_clause else None
        self.__client_sql = sqlglot.parse_one(client_sql) if client_sql else None
        self.validate_inquiry()
        self.atomic_queries = self._split_queries()

    def __repr__(self):
        return f""" 
        dimensions: {self.dimensions}
        metrics: {self.metrics} 
        filters: {self.global_filter} 
        granularity: {self.granularity}
        order_by: {self.order_by}
        """

    def validate_inquiry(self):
        """Validates the inquiry to ensure that it is well-formed."""
        sources = {metric.source for metric in self.metrics}
        metric_names = [metric.name for metric in self.metrics]
        granularity_alias = self.granularity.alias if self.granularity else None
        for source in sources:
            source.validate_dimensions(self.dimensions)
            source.validate_conditions(
                where=self.global_filter, metric_names=metric_names
            )
            source.validate_sort(
                order_by=self.order_by,
                metric_names=metric_names,
                granularity_alias=granularity_alias,
            )

    def _split_queries(self) -> t.List[AtomicQuery]:
        """Splits the inquiry into a set of AtomicQueries, one for each source."""
        return [
            AtomicQuery(
                source=source,
                mimir_engine=self.mimir_engine,
                dimensions=self.dimensions,
                metrics=list(metrics),
                start_date=self.start_date,
                end_date=self.end_date,
                granularity=self.granularity,
                global_filter=self.global_filter,
            )
            for source, metrics in itertools.groupby(
                self.metrics, key=lambda metric: metric.source
            )
        ]

    def _combine_queries(self) -> sqlglot.exp.Select:
        """Combines the results of the atomic queries into a single DataFrame."""
        dim_columns = [dim.name for dim in self.dimensions]
        if self.granularity:
            dim_columns.insert(0, self.granularity.alias)
        metric_columns = [metric.name for metric in self.metrics]
        table_names = [query.name for query in self.atomic_queries]
        select_expressions = (
            self.__client_sql.expressions
            if self.__client_sql
            else [*dim_columns, *metric_columns]
        )
        first_table, *other_tables = table_names
        query = sqlglot.select(*select_expressions).from_(first_table)

        if dim_columns:
            query = functools.reduce(
                lambda q, table_name: q.join(
                    table_name, using=dim_columns, join_type="full"
                ),
                other_tables,
                query,
            )
        else:
            query = functools.reduce(
                lambda q, table_name: q.join(table_name, join_type="cross"),
                other_tables,
                query,
            )

        if self.order_by:
            query = query.order_by(*self.order_by)
        return query

    def compile(self) -> str:
        """
        Compiles the full inquiry into a single SQL string without executing it.

        Returns:
            A SQL string for the final federated query.
        """
        # Register dummy tables for compilation
        with duckdb.connect(":memory:") as conn:
            for aq in self.atomic_queries:
                # Create an empty table with the correct schema to allow compilation
                ast_for_schema = aq.ast.limit(0)
                if not aq.source.connection:
                    raise MimirQueryError(
                        f"Source '{aq.source.name}' has no active connection for compilation."
                    )
                dummy_table = aq.source.connection.query(sql=ast_for_schema.sql())
                conn.register(aq.name, dummy_table)

            sql = self._combine_queries().sql(dialect="duckdb")
            return sql

    def dispatch(self) -> pa.Table:
        """Executes the inquiry and returns the results.

        Returns:
            A pyarrow Table containing the final results.
        """
        logger.info("executing queries")
        with duckdb.connect(":memory:") as conn:
            with ThreadPoolExecutor() as executor:
                future_to_query = {
                    executor.submit(atomic_query.execute): atomic_query
                    for atomic_query in self.atomic_queries
                }
                for future in as_completed(future_to_query):
                    atomic_query = future_to_query[future]
                    try:
                        data = future.result()
                        conn.register(atomic_query.name, data)
                        logger.info(f"Registered {atomic_query.name}")
                    except Exception as exc:
                        logger.error(f"{atomic_query} generated an exception: {exc}")
                        raise Exception from exc
            sql = self._combine_queries().sql(dialect="duckdb")
            result = conn.execute(sql)
            return result.fetch_arrow_table()
