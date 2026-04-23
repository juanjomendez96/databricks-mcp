"""Databricks SDK wrapper for Unity Catalog schema introspection."""

import os
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo, TableInfo


class DatabricksClient:
    """Wraps WorkspaceClient and formats Unity Catalog metadata as Markdown."""

    def __init__(self) -> None:
        host = os.environ.get("DATABRICKS_HOST")
        token = os.environ.get("DATABRICKS_TOKEN")

        if not token:
            raise EnvironmentError("DATABRICKS_TOKEN environment variable is not set.")

        if not host:
            raise EnvironmentError("DATABRICKS_HOST environment variable is not set.")

        self._client = WorkspaceClient(host=host, token=token)

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def get_table_schema(self, catalog: str, schema: str, table: str) -> str:
        """Return Markdown-formatted schema for a Unity Catalog table."""
        full_name = f"{catalog}.{schema}.{table}"

        info: TableInfo = self._client.tables.get(full_name=full_name)

        return self._format_table_schema(info)

    def list_catalogs(self) -> str:
        """Return a Markdown list of catalog names."""
        catalogs = list(self._client.catalogs.list())

        if not catalogs:
            return "_No catalogs found._"

        lines = ["# Catalogs", ""]

        for cat in catalogs:
            lines.append(f"- `{cat.name}`")

        return "\n".join(lines)

    def list_schemas(self, catalog: str) -> str:
        """Return a Markdown list of schema names within a catalog."""
        schemas = list(self._client.schemas.list(catalog_name=catalog))

        if not schemas:
            return f"_No schemas found in catalog `{catalog}`._"

        lines = [f"# Schemas in `{catalog}`", ""]

        for sch in schemas:
            lines.append(f"- `{sch.name}`")

        return "\n".join(lines)

    def list_tables(self, catalog: str, schema: str) -> str:
        """Return a Markdown list of tables with their type."""
        tables = list(
            self._client.tables.list(catalog_name=catalog, schema_name=schema)
        )

        if not tables:
            return f"_No tables found in `{catalog}.{schema}`._"

        lines = [f"# Tables in `{catalog}.{schema}`", ""]

        for tbl in tables:
            table_type = tbl.table_type.value if tbl.table_type else "UNKNOWN"
            lines.append(f"- `{tbl.name}` ({table_type})")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _format_table_schema(self, info: TableInfo) -> str:
        full_name = info.full_name or "unknown"
        table_type = info.table_type.value if info.table_type else "UNKNOWN"
        owner = info.owner or "_unknown_"
        comment = info.comment or "_no description_"

        partitions: Optional[list[str]] = None
        if info.columns:
            partitions = [
                col.name for col in info.columns if col.partition_index is not None
            ]

        lines = [
            f"# Schema: `{full_name}`",
            "",
            f"| Property | Value |",
            f"|----------|-------|",
            f"| Type | {table_type} |",
            f"| Owner | {owner} |",
            f"| Comment | {comment} |",
        ]

        if partitions:
            lines.append(
                f"| Partition columns | {', '.join(f'`{p}`' for p in partitions)} |"
            )

        lines += [
            "",
            "## Columns",
            "",
            "| Column | Type | Nullable | Comment |",
            "|--------|------|----------|---------|",
        ]

        for col in info.columns or []:
            col_name = col.name or ""

            col_type = col.type_text or (col.type_name.value if col.type_name else "")

            nullable = "Yes" if col.nullable else "No"

            col_comment = col.comment or ""

            lines.append(
                f"| `{col_name}` | `{col_type}` | {nullable} | {col_comment} |"
            )

        return "\n".join(lines)
