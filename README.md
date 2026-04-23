# databricks-mcp

An MCP server that connects to Databricks Unity Catalog and retrieves table schemas. It exposes column-level metadata (names, types, nullability, descriptions) and catalog/schema/table discovery as tools that Claude can call directly.

**Primary use case:** documenting Databricks Asset Bundles by giving Claude accurate, live schema information for tables in your data platform.

---

## Requirements

- Python 3.10+
- [`uv`](https://docs.astral.sh/uv/) package manager
- A Databricks workspace with Unity Catalog enabled
- A personal access token with at minimum `USE CATALOG`, `USE SCHEMA`, and `SELECT` privileges on the objects you want to inspect

---

## Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd databricks-mcp
```

### 2. Install dependencies

```bash
uv sync
```

This creates a `.venv` and installs `mcp[cli]`, `databricks-sdk`, and all dev dependencies.

### 3. Set environment variables

The server requires two environment variables at startup:

| Variable | Description |
|---|---|
| `DATABRICKS_HOST` | Full workspace URL, e.g. `https://adb-1234567890.12.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Personal access token (Settings → Developer → Access tokens in the Databricks UI) |

Export them in your shell or add them to a `.env` file (never commit this file):

```bash
export DATABRICKS_HOST="https://<workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
```

---

## Running the server

### With Claude Code (production use)

Copy `.mcp.json.dist` to `.mcp.json` and replace `[absolute-path-to-repo]` with the absolute path to the cloned repository:

```json
{
  "mcpServers": {
    "databricks-mcp": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/absolute/path/to/databricks-mcp",
        "databricks-mcp"
      ],
      "env": {
        "DATABRICKS_TOKEN": "${DATABRICKS_TOKEN}",
        "DATABRICKS_HOST": "${DATABRICKS_HOST}"
      }
    }
  }
}
```

Claude Code will start and stop the server process automatically — you don't need to manage it manually.

### With MCP Inspector (interactive testing)

```bash
uv run mcp dev src/databricks_mcp/server.py
```

Open the URL printed in the terminal to call tools and inspect their output interactively.

### As a standalone process

```bash
DATABRICKS_HOST="https://..." DATABRICKS_TOKEN="dapi..." uv run databricks-mcp
```

This starts the server in stdio transport mode (the default for MCP). It is intended to be called by an MCP host, not used interactively.

---

## Stopping the server

- **Claude Code**: the server process is managed automatically and stops when the session ends or when you disable the server in MCP settings.
- **MCP Inspector / standalone**: press `Ctrl+C` in the terminal where the server is running.

---

## Available tools

### `get_table_schema`

Returns the full column-level schema for a Unity Catalog table, formatted as Markdown.

**Parameters**

| Name | Type | Description |
|---|---|---|
| `catalog` | string | Unity Catalog catalog name (e.g. `main`) |
| `schema` | string | Schema name within the catalog (e.g. `sales`) |
| `table` | string | Table name (e.g. `orders`) |

**Example output**

```markdown
# Schema: `main.sales.orders`

| Property | Value |
|----------|-------|
| Type | MANAGED |
| Owner | data_team |
| Comment | Transactional orders table |
| Partition columns | `event_date` |

## Columns

| Column | Type | Nullable | Comment |
|--------|------|----------|---------|
| `id` | `BIGINT` | No | Primary key |
| `customer_id` | `BIGINT` | No | FK to customers |
| `amount` | `DOUBLE` | Yes | Order total in EUR |
| `event_date` | `DATE` | No | Partition column |
```

---

### `list_catalogs`

Lists all Unity Catalog catalogs accessible with the configured token.

**Parameters:** none

**Example output**

```markdown
# Catalogs

- `main`
- `hive_metastore`
- `sandbox`
```

---

### `list_schemas`

Lists all schemas within a given catalog.

**Parameters**

| Name | Type | Description |
|---|---|---|
| `catalog` | string | Catalog name to inspect |

**Example output**

```markdown
# Schemas in `main`

- `sales`
- `marketing`
- `finance`
```

---

### `list_tables`

Lists all tables within a given schema, including their type (`MANAGED`, `EXTERNAL`, or `VIEW`).

**Parameters**

| Name | Type | Description |
|---|---|---|
| `catalog` | string | Catalog name |
| `schema` | string | Schema name within the catalog |

**Example output**

```markdown
# Tables in `main.sales`

- `orders` (MANAGED)
- `customers` (MANAGED)
- `product_feed` (EXTERNAL)
- `revenue_summary` (VIEW)
```

---

## Error messages

All tools return a friendly Markdown message instead of raising an exception:

| Situation | Message prefix |
|---|---|
| Missing env var | `**Configuration error:**` |
| Token expired / invalid (401) | `**Authentication error**` |
| Insufficient privileges (403) | `**Permission denied**` |
| Object does not exist (404) | `**Not found**` |
| Any other SDK error | `**Unexpected error**` |

---

## Project structure

```
databricks-mcp/
├── .mcp.json.dist                     # Template for Claude Code server registration
├── pyproject.toml                     # Dependencies and entry point
├── src/
│   └── databricks_mcp/
│       ├── __init__.py
│       ├── server.py                  # FastMCP instance + @mcp.tool() definitions
│       └── client.py                  # DatabricksClient — SDK wrapper + Markdown formatting
└── tests/
    ├── __init__.py
    └── test_client.py                 # Unit tests with mocked WorkspaceClient
```

---

## Running tests

```bash
uv run pytest tests/ -v
```

All tests mock `WorkspaceClient` so no real Databricks connection is required.

---

## Contributing

### Development setup

```bash
uv sync                        # install all deps including dev group
```

### Adding a new tool

1. Add a method to `DatabricksClient` in `src/databricks_mcp/client.py` that wraps the SDK call and returns a Markdown string.
2. Add a `@mcp.tool()` decorated function in `src/databricks_mcp/server.py` that calls the client method and wraps it in the standard `try/except` pattern already used by the other tools.
3. Add unit tests in `tests/test_client.py` using `unittest.mock.patch` to mock `WorkspaceClient`.

### Modifying Markdown output

All formatting lives in `client.py`. The `_format_table_schema` method controls `get_table_schema` output; the other methods (`list_catalogs`, `list_schemas`, `list_tables`) each build their own Markdown lines list directly.

### Adding a dependency

```bash
uv add <package>               # runtime dependency
uv add --dev <package>         # dev-only dependency
```

### Versioning and releases

This project uses [python-semantic-release](https://python-semantic-release.readthedocs.io/) with the [Conventional Commits](https://www.conventionalcommits.org/) specification. Every push to `master` triggers a GitHub Actions workflow that:

1. Inspects commit messages since the last release tag.
2. Bumps `version` in `pyproject.toml` according to the table below.
3. Creates a `vX.Y.Z` git tag and commits the version bump with `chore(release): X.Y.Z [skip ci]`.

**You never manually edit the version** — it is managed entirely by the CI pipeline.

#### Commit message format

```
<type>(<optional scope>): <description>

[optional body]

[optional footer]
```

| Type | Version bump | When to use |
|---|---|---|
| `feat:` | **minor** (`0.1.0` → `0.2.0`) | New tool, new capability, new public behaviour |
| `fix:` | **patch** (`0.1.0` → `0.1.1`) | Bug fix that doesn't change the public API |
| `perf:` | **patch** | Performance improvement with no API change |
| `feat!:` or `BREAKING CHANGE:` in footer | **major** (`0.1.0` → `1.0.0`) | Incompatible API change |
| `refactor:`, `docs:`, `test:`, `chore:`, `ci:`, `build:`, `style:` | none | No release created |

#### Examples

```bash
# patch bump
git commit -m "fix: handle 404 when catalog does not exist"

# minor bump
git commit -m "feat: add execute_query tool"

# major bump
git commit -m "feat!: rename get_table_schema parameters to snake_case"

# no release
git commit -m "docs: update README setup section"
git commit -m "chore: upgrade databricks-sdk"
```

If no commit since the last tag qualifies for a bump, the workflow exits without creating a release.

---

### Code style

- Python 3.10+ — use modern type hints (`list[str]`, `str | None`)
- Keep tool functions in `server.py` thin: they only handle errors and delegate to `client.py`
- All SDK interaction and formatting belongs in `client.py`
- Tests must not make real network calls — always mock `WorkspaceClient`
