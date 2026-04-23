"""Unit tests for DatabricksClient with mocked WorkspaceClient."""

import os
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")


def _make_column(name, type_text, nullable=True, comment=None, partition_index=None):
    col = MagicMock()
    col.name = name
    col.type_text = type_text
    col.type_name = None
    col.nullable = nullable
    col.comment = comment
    col.partition_index = partition_index
    return col


def _make_table_info(
    full_name="main.sales.orders",
    table_type_value="MANAGED",
    owner="admin",
    comment="Order table",
    columns=None,
):
    info = MagicMock()
    info.full_name = full_name
    info.table_type = MagicMock()
    info.table_type.value = table_type_value
    info.owner = owner
    info.comment = comment
    info.columns = columns or []
    return info


# ---------------------------------------------------------------------------
# get_table_schema tests
# ---------------------------------------------------------------------------


class TestGetTableSchema:
    def test_basic_schema_formatting(self):
        columns = [
            _make_column("id", "BIGINT", nullable=False, comment="Primary key"),
            _make_column("name", "STRING", nullable=True, comment="Customer name"),
            _make_column("amount", "DOUBLE", nullable=True),
        ]
        table_info = _make_table_info(columns=columns)

        with patch("databricks_mcp.client.WorkspaceClient") as MockWC:
            MockWC.return_value.tables.get.return_value = table_info
            from databricks_mcp.client import DatabricksClient

            client = DatabricksClient()
            result = client.get_table_schema("main", "sales", "orders")

        assert "# Schema: `main.sales.orders`" in result
        assert "MANAGED" in result
        assert "admin" in result
        assert "Order table" in result
        assert "| `id` | `BIGINT` | No | Primary key |" in result
        assert "| `name` | `STRING` | Yes | Customer name |" in result
        assert "| `amount` | `DOUBLE` | Yes |" in result

    def test_partition_columns_shown(self):
        columns = [
            _make_column("event_date", "DATE", partition_index=0),
            _make_column("value", "INT"),
        ]
        table_info = _make_table_info(columns=columns)

        with patch("databricks_mcp.client.WorkspaceClient") as MockWC:
            MockWC.return_value.tables.get.return_value = table_info
            from databricks_mcp.client import DatabricksClient

            client = DatabricksClient()
            result = client.get_table_schema("main", "sales", "events")

        assert "Partition columns" in result
        assert "`event_date`" in result

    def test_empty_columns(self):
        table_info = _make_table_info(columns=[])

        with patch("databricks_mcp.client.WorkspaceClient") as MockWC:
            MockWC.return_value.tables.get.return_value = table_info
            from databricks_mcp.client import DatabricksClient

            client = DatabricksClient()
            result = client.get_table_schema("main", "sales", "empty_table")

        assert "## Columns" in result


# ---------------------------------------------------------------------------
# list_catalogs tests
# ---------------------------------------------------------------------------


class TestListCatalogs:
    def test_lists_catalogs(self):
        cat1 = MagicMock()
        cat1.name = "main"
        cat2 = MagicMock()
        cat2.name = "hive_metastore"

        with patch("databricks_mcp.client.WorkspaceClient") as MockWC:
            MockWC.return_value.catalogs.list.return_value = iter([cat1, cat2])
            from databricks_mcp.client import DatabricksClient

            client = DatabricksClient()
            result = client.list_catalogs()

        assert "# Catalogs" in result
        assert "`main`" in result
        assert "`hive_metastore`" in result

    def test_empty_catalogs(self):
        with patch("databricks_mcp.client.WorkspaceClient") as MockWC:
            MockWC.return_value.catalogs.list.return_value = iter([])
            from databricks_mcp.client import DatabricksClient

            client = DatabricksClient()
            result = client.list_catalogs()

        assert "No catalogs found" in result


# ---------------------------------------------------------------------------
# list_schemas tests
# ---------------------------------------------------------------------------


class TestListSchemas:
    def test_lists_schemas(self):
        sch = MagicMock()
        sch.name = "sales"

        with patch("databricks_mcp.client.WorkspaceClient") as MockWC:
            MockWC.return_value.schemas.list.return_value = iter([sch])
            from databricks_mcp.client import DatabricksClient

            client = DatabricksClient()
            result = client.list_schemas("main")

        assert "# Schemas in `main`" in result
        assert "`sales`" in result

    def test_empty_schemas(self):
        with patch("databricks_mcp.client.WorkspaceClient") as MockWC:
            MockWC.return_value.schemas.list.return_value = iter([])
            from databricks_mcp.client import DatabricksClient

            client = DatabricksClient()
            result = client.list_schemas("main")

        assert "No schemas found" in result


# ---------------------------------------------------------------------------
# list_tables tests
# ---------------------------------------------------------------------------


class TestListTables:
    def test_lists_tables_with_type(self):
        tbl = MagicMock()
        tbl.name = "orders"
        tbl.table_type = MagicMock()
        tbl.table_type.value = "MANAGED"

        with patch("databricks_mcp.client.WorkspaceClient") as MockWC:
            MockWC.return_value.tables.list.return_value = iter([tbl])
            from databricks_mcp.client import DatabricksClient

            client = DatabricksClient()
            result = client.list_tables("main", "sales")

        assert "# Tables in `main.sales`" in result
        assert "`orders` (MANAGED)" in result

    def test_empty_tables(self):
        with patch("databricks_mcp.client.WorkspaceClient") as MockWC:
            MockWC.return_value.tables.list.return_value = iter([])
            from databricks_mcp.client import DatabricksClient

            client = DatabricksClient()
            result = client.list_tables("main", "empty_schema")

        assert "No tables found" in result


# ---------------------------------------------------------------------------
# Auth / env errors
# ---------------------------------------------------------------------------


class TestAuthErrors:
    def test_missing_token_raises(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN")
        with pytest.raises(EnvironmentError, match="DATABRICKS_TOKEN"):
            from databricks_mcp.client import DatabricksClient

            DatabricksClient()

    def test_missing_host_raises(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_HOST")
        with pytest.raises(EnvironmentError, match="DATABRICKS_HOST"):
            from databricks_mcp.client import DatabricksClient

            DatabricksClient()
