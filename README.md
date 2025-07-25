# Mimir

A friendly semantic layer for data analytics.

> **Disclaimer**
>
> Mimir is a project heavily insipred by Airbnb works on metrics stores and it's built to showcase a simple implementation of a semantic layer. It's a playground for demonstrating architectural patterns and exploring what a "metrics-as-code" workflow can feel like.
>
> So while it's built with solid engineering principles in mind, it is **not (and probably would never be) meant for production use**. Feel free to clone it, break it, and learn from it!

## Architecture Overview

Mimir is designed to separate the development and consumption of metrics. The CLI is defined as the (optional) primary tool for the "Control Plane" (managing definitions), while the server components form the "Service Plane" (serving data to clients).

```
+-----------------------+      +----------------+      +-------------------+      +---------------------+
|   Analytics Engineer  |----->|   Mimir CLI    |----->|   Git Repository  |----->|   CI/CD Pipeline    |
| (on their laptop)     |      | (create,       |      | (Metrics as Code) |      | (mimir validate)    |
+-----------------------+      |  validate,     |      +-------------------+      +----------+----------+
                               |  query --dry-run) |                                          |
                               +----------------+                                          | (Sync)
                                                                                           |
                                                                                           v
+-----------------------+      +----------------+      +-------------------+      +---------------------+
|   BI Tool (Tableau)   |----->| SQL Proxy      |----->|   Mimir Server    |<-----|  Config DB / S3     |
+-----------------------+      +----------------+      | (FastAPI)         |      | (Centralized Store) |
                               (MySQL Protocol)        +---------^---------+      +---------------------+
                                                                 |
+-----------------------+                                        | (HTTP API)
| Ad-hoc User           |----->|   Mimir CLI    |-----------------+
| (on their laptop)     |      | (query --host) |
+-----------------------+      +----------------+
```

## Architectural Components

*   **API Server:** A FastAPI-based server that accepts `Inquiry` requests and returns data in the speedy Apache Arrow format.
*   **SQL Proxy:** A clever MySQL-compatible proxy that lets you query Mimir with standard SQL. It translates your SQL into Mimir API requests behind the scenes.
*   **Query Engine:** The brains of the operation, lives either local or inside your Mimir Server. It parses your configs, builds queries, and wrangles data from all your sources.

## Core Features

*   **Configuration-driven:** Define all your metrics and dimensions in simple, human-readable YAML files.
*   **Connect to Multiple Sources:** Mimir can connect to several data sources, including PostgreSQL, MySQL, and DuckDB.
*   **Speak SQL:** Query your metrics using the language you already know and love.

## Quickstart with the Example

This guide will get you up and running with the example application.

### 1. Install Dependencies

First, get all the necessary dependencies installed. The `install` command will use `uv` to set up a virtual environment and grab the dependencies.

```bash
make install
```

### 2. Start the Services

Build and start the Docker containers for the Mimir API, the SQL proxy, and a sample PostgreSQL database.

```bash
make example-up
```

This command will:
- Build the Docker images for the API and proxy.
- Start all services defined in `example/docker-compose.yaml`.
- Spin up a PostgreSQL database with the classic Pagila sample dataset.

### 3. Query Your Data

Once the services are humming, you can query Mimir in two ways:

#### Through the SQL Proxy

Connect to the proxy using any MySQL-compatible client (e.g., `mysql`, DBeaver, DataGrip).

**Connection Details:**
- **Host:** `localhost`
- **Port:** `3306`
- **User/Password:** (anything you want!)

You can then run standard SQL queries. Mimir will figure out the rest.

```sql
SELECT
  dim_rental_category,
  AGG(movies_rented),
  AGG(rentals_revenue)
FROM
  mimir.metrics
WHERE
  dim_rental_category = 'Action'
GROUP BY
  dim_rental_category;
```

#### Through the API

You can also talk to the API directly. The docs are waiting for you at `http://localhost:8090/docs`.

### 4. Stop the Services

When you're done, you can shut everything down gracefully.

```bash
make example-down
```

## Command-Line Interface (CLI)

> **Note:** The CLI is currently in a very experimental stage.

Mimir includes a powerful command-line interface for developing, validating, and querying your semantic layer. You can invoke it using `uv run mimir -- [COMMAND]`.

### Project Scaffolding

**`init`**

Initializes a new Mimir project with the standard directory structure.

```bash
mkdir my-mimir-project
cd my-mimir-project
uv run mimir init
```

**`create`**

Interactively create new metric or dimension definitions.

```bash
# Create a new metric
uv run mimir create metric

# Create a new dimension
uv run mimir create dimension
```

### Introspection and Validation

**`validate`**

Validates all configuration files in a specified directory to ensure they are syntactically correct and semantically valid.

```bash
uv run mimir validate --configs path/to/your/configs
```

**`list`**

Lists all available definitions of a certain type.

```bash
# List all available data sources
uv run mimir list sources

# List all available metrics
uv run mimir list metrics

# List all available dimensions
uv run mimir list dimensions
```

**`describe`**

Shows detailed metadata for a single definition, including its description and underlying SQL.

```bash
uv run mimir describe <definition_name> <metric|dimension|source>
```

### Querying

**`query`**

Runs a query against the Mimir engine. This command can be run in two modes:

1.  **Local Mode:** Runs the query engine directly on your machine, using local config and secret files.
2.  **Remote Mode:** Acts as a client to a running Mimir API server.

```bash
# Run a query locally
uv run mimir query \
  --metric movies_rented \
  --dimension dim_rental_category \
  --filter "dim_rental_category = 'Action'"

# Get the compiled SQL without running the query (for debugging)
uv run mimir query --metric movies_rented --dry-run

# Run a query against a hosted Mimir server
uv run mimir query \
  --host http://mimir.mycompany.com \
  --metric movies_rented
```

## Extensibility

Mimir is built to be extended. If you don't like how something works, you can change it. Here are a couple of ideas to get you started.

### Custom Connections

Want to connect to a data source that isn't supported out-of-the-box?

1.  **Create a new class** that inherits from `mimir.api.connections.Connection`.
2.  **Implement the `query` method.** This method just needs to accept a SQL string and return the results as a `pyarrow.Table`.

Once you've done that, just register your new connection in the `ConnectionFactory` and Mimir will know how to use it.

### Custom Configuration Loaders

Tired of YAML files? Want to load your metric definitions from a database, a Git repo, or an S3 bucket? Go for it.

1.  **Create a new class** that inherits from `mimir.api.loaders.BaseConfigLoader`.
2.  **Implement the required methods** (`get`, `get_all`, `get_secret`).

Pass an instance of your new loader to the `MimirEngine`, and you're off to the races.