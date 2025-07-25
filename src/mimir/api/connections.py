import sqlalchemy
import polars as pl
import pyarrow as pa
import duckdb

import typing as t

from abc import ABC, abstractmethod


class Connection(ABC):
    """Abstract base class for all data connections.

    This class defines the interface for objects that can execute queries.
    """

    @abstractmethod
    def query(self, *args, **kwargs) -> pa.Table:
        """Executes a query and returns the results.

        Returns:
            A pyarrow Table containing the query results.
        """
        raise NotImplementedError()


class DuckDBConnection(Connection):
    """A connection to a DuckDB database.

    Args:
        path: The path to the DuckDB database file.
    """

    def __init__(self, path: str, **kwargs) -> None:
        self.path = path

    def query(self, sql: str) -> pa.Table:
        """Executes a SQL query and returns the results.

        Args:
            sql: The SQL query string to execute.

        Returns:
            A pyarrow Table containing the query results.
        """
        with duckdb.connect(self.path) as con:
            return con.execute(sql).fetch_arrow_table()


class SqlDatabase(Connection):
    """A connection to a SQL database using SQLAlchemy.

    This class manages a connection pool and executes queries against a standard
    SQL database.

    Args:
        flavour: The SQLAlchemy dialect flavour (e.g., 'postgresql').
        host: The database host.
        user: The username for authentication.
        password: The password for authentication.
        port: The port number for the connection.
        schema: The database or schema name to connect to.
    """

    def __init__(
        self,
        flavour: str,
        schema: str,
        host: t.Optional[str] = None,
        user: t.Optional[str] = None,
        password: t.Optional[str] = None,
        port: t.Optional[int] = None,
        pool: t.Optional[sqlalchemy.engine.base.Engine] = None,
        **kwargs,
    ) -> None:
        self.driver = self.get_flavour(flavour)
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.schema = schema
        self.pool = pool or self.create_pool()

    _FLAVOURS = {
        "mysql": "mysql+pymysql",
        "postgresql": "postgresql+psycopg2",
    }

    def get_flavour(self, flavour: str) -> str:
        if not self._FLAVOURS.get(flavour):
            raise NotImplementedError(
                f"The requested SQL flavour {flavour} is not yet implemented"
            )

        return self._FLAVOURS[flavour]

    def create_pool(self) -> sqlalchemy.engine.base.Engine:
        """Initializes a SQLAlchemy connection pool.

        Returns:
            A SQLAlchemy Engine instance.
        """
        return sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL.create(
                drivername=self.driver,
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.schema,
            )
        )

    def create_uri(self) -> sqlalchemy.engine.URL:
        """Creates a SQLAlchemy URL for the connection.

        Returns:
            A SQLAlchemy URL object.
        """
        return sqlalchemy.engine.url.URL.create(
            drivername=self.driver,
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.schema,
        )

    def query(self, sql: str) -> pa.Table:
        """Executes a SQL query and returns the results.

        Args:
            sql: The SQL query string to execute.

        Returns:
            A pyarrow Table containing the query results.
        """
        with self.pool.connect() as conn:
            return pl.read_database(
                query=sqlalchemy.text(sql), connection=conn
            ).to_arrow()


class ConnectionFactory:
    """A factory for creating connection instances based on configuration.

    This class reads connection configuration details and instantiates the
    appropriate Connection subclass.

    Args:
        connection_name: The name of the connection to create, which is used
            to fetch the corresponding secret.
        mimir_engine: The MimirEngine instance, used to fetch secrets.
    """

    def __init__(
        self, connection_name: str, connection_config: t.Dict[str, t.Any]
    ) -> None:
        """Initializes the ConnectionFactory.

        Args:
            connection_name: The name of the connection to create.
            connection_config: The configuration for the connection.
        """
        self.connection_name = connection_name
        self.connection_config = connection_config
        self.connection_class = self.connection_config["connection_class"]

    _TYPES: t.Dict[str, t.Type[Connection]] = {
        "sqldb": SqlDatabase,
        "duckdb": DuckDBConnection,
    }

    def create_connection_instance(self) -> Connection:
        """Creates a connection instance based on the loaded configuration.

        Returns:
            An initialized instance of a Connection subclass (e.g., SqlDatabase).
        """
        return ConnectionFactory._TYPES[self.connection_class](
            **self.connection_config,
        )
