import sqlglot

import typing as t

from mimir.api.exceptions import MimirNotImplementedError


class MimirSql:
    """A class for parsing and validating Mimir SQL queries."""

    def __init__(self, sql: str) -> None:
        """Initializes the MimirSql object.

        Args:
            sql: The SQL query to parse.
        """
        self.sql = sql
        self.ast = sqlglot.parse_one(sql)
        self.table = self.ast.find(sqlglot.exp.Table)

    def validate(self) -> None:
        """Validates the SQL query."""
        if len(sqlglot.parse(self.sql)) > 1:
            raise MimirNotImplementedError("Multiple queries are not yet supported")

        if self.ast.find(
            sqlglot.exp.CTE, sqlglot.exp.DerivedTable, sqlglot.exp.Subquery
        ):
            raise MimirNotImplementedError(
                "Derived tables, CTEs and subqueries are not yet supported"
            )

    def parse_inquiry(self) -> t.Dict[str, t.Any]:
        """Parses the SQL query and returns a dictionary of inquiry parameters."""
        processed_ast = self.ast.copy()

        dimensions = {
            col.find(sqlglot.exp.Column).find(sqlglot.exp.Identifier).this
            for col in processed_ast.expressions
            if not (
                col.find(sqlglot.exp.Func)
                and col.find(sqlglot.exp.Func).name.upper() == "AGG"
            )
        }

        metrics = {
            col.find(sqlglot.exp.Func).find(sqlglot.exp.Identifier).this
            for col in processed_ast.expressions
            if col.find(sqlglot.exp.Func)
            and col.find(sqlglot.exp.Func).name.upper() == "AGG"
        }

        global_filter = (
            processed_ast.find(sqlglot.exp.Where).this.sql()  # type: ignore
            if processed_ast.find(sqlglot.exp.Where)
            else None
        )
        order_by_expr = processed_ast.find(sqlglot.exp.Ordered)
        order_by = order_by_expr.sql() if order_by_expr else None
        client_sql = sqlglot.exp.Select(expressions=processed_ast.expressions)
        for node in client_sql.find_all(sqlglot.exp.Func):
            if node.name.upper() == "AGG":
                node.replace(node.find(sqlglot.exp.Column))

        return {
            "dimensions": list(dimensions),
            "metrics": list(metrics),
            "global_filter": global_filter,
            "order_by": order_by,
            "client_sql": client_sql.sql(),
        }
