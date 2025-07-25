from mimir.api import Client
from mimir.sql.mimir_sql import MimirSql
import logging
import asyncio
from mysql_mimic.server import MysqlServer
from mysql_mimic.session import Session
import os
import duckdb

import typing as t

logger = logging.getLogger(__name__)


class MimirProxySession(Session):
    """A MySQL proxy session that translates SQL queries into Mimir inquiries."""

    def __init__(self, mimir_client: t.Optional[Client] = None):
        """Initializes the MimirProxySession.

        Args:
            mimir_client: The Mimir client to use.
        """
        super().__init__()
        self.mimir_client = mimir_client or Client(
            os.environ.get("MIMIR_API_URL", "http://localhost:8090")
        )
        self._duck = duckdb.connect()

    async def schema(self):
        """Returns the schema of the Mimir API."""
        return {
            "mimir": {
                "mimir": {
                    tbl_name: {
                        tbl["time_dimension"]: "TIMESTAMP",
                        **{dim: "TEXT" for dim in tbl["dimensions"]},
                        **{metric: "NUMERIC" for metric in tbl["metrics"]},
                    }
                    for tbl_name, tbl in self.mimir_client.get_schema().items()
                }
            }
        }

    async def query(self, expression, sql, attrs):
        """Executes a SQL query.

        Args:
            expression: The SQL expression to execute.
            sql: The SQL query to execute.
            attrs: The attributes of the query.

        Returns:
            A tuple of rows and column names.
        """
        query = MimirSql(sql=sql)
        logger.info(query.table)
        if query.table and query.table.db == "mimir" and query.table.name == "metrics":
            parsed_inquiry_dict = query.parse_inquiry()
            logger.info(f"Proxy sending to client: {parsed_inquiry_dict}")

            t = self.mimir_client.query(**parsed_inquiry_dict)
            column_names = t.column_names
            rows = list(zip(*t.to_pydict().values(), strict=True))
            print(rows[0], column_names)
            return (rows, column_names)

        relation = self._duck.query(sql)
        return relation.fetchall(), relation.columns


async def main():
    """Starts the Mimir SQL proxy."""
    logging.basicConfig(level=logging.DEBUG)
    server = MysqlServer(session_factory=MimirProxySession)
    await server.serve_forever(port=3306)


if __name__ == "__main__":
    asyncio.run(main())
