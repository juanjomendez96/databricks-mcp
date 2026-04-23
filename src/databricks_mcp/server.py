"""MCP server exposing Databricks Unity Catalog tools."""

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("databricks-mcp")


def _get_client():
    """Lazily import and instantiate DatabricksClient (deferred so env vars can be set)."""
    from databricks_mcp.client import DatabricksClient  # noqa: PLC0415

    return DatabricksClient()


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def get_table_schema(catalog: str, schema: str, table: str) -> str:
    """Get the full column-level schema for a Unity Catalog table.

    Returns a Markdown document with column names, data types, nullability,
    comments, and table metadata (type, owner, partitions).
    """
    try:
        return _get_client().get_table_schema(catalog, schema, table)

    except EnvironmentError as exc:
        return f"**Configuration error:** {exc}"

    except Exception as exc:  # noqa: BLE001
        return _handle_sdk_error(exc, context=f"`{catalog}.{schema}.{table}`")


@mcp.tool()
def list_catalogs() -> str:
    """List all Unity Catalog catalogs accessible with the configured credentials."""
    try:
        return _get_client().list_catalogs()

    except EnvironmentError as exc:
        return f"**Configuration error:** {exc}"

    except Exception as exc:  # noqa: BLE001
        return _handle_sdk_error(exc, context="catalogs")


@mcp.tool()
def list_schemas(catalog: str) -> str:
    """List all schemas within a Unity Catalog catalog."""
    try:
        return _get_client().list_schemas(catalog)

    except EnvironmentError as exc:
        return f"**Configuration error:** {exc}"

    except Exception as exc:  # noqa: BLE001
        return _handle_sdk_error(exc, context=f"catalog `{catalog}`")


@mcp.tool()
def list_tables(catalog: str, schema: str) -> str:
    """List all tables (with their type) within a Unity Catalog schema."""
    try:
        return _get_client().list_tables(catalog, schema)

    except EnvironmentError as exc:
        return f"**Configuration error:** {exc}"

    except Exception as exc:  # noqa: BLE001
        return _handle_sdk_error(exc, context=f"`{catalog}.{schema}`")


# ---------------------------------------------------------------------------
# Error helpers
# ---------------------------------------------------------------------------


def _handle_sdk_error(exc: Exception, context: str) -> str:
    """Translate Databricks SDK exceptions into friendly messages."""
    exc_str = str(exc)
    # Databricks SDK raises generic exceptions; inspect the string for status codes.
    if "UNAUTHENTICATED" in exc_str or "401" in exc_str:
        return (
            f"**Authentication error** while accessing {context}: "
            "your `DATABRICKS_TOKEN` may be expired or invalid."
        )

    if "PERMISSION_DENIED" in exc_str or "403" in exc_str:
        return (
            f"**Permission denied** while accessing {context}: "
            "check that the token has the required Unity Catalog grants (USE CATALOG, USE SCHEMA, SELECT)."
        )

    if "NOT_FOUND" in exc_str or "404" in exc_str:
        return (
            f"**Not found** — {context} does not exist or is not visible to this token. "
            "Use `list_tables` to browse available tables."
        )

    return f"**Unexpected error** while accessing {context}: {exc}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
