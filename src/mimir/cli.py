import typer
from pathlib import Path
import typing as t
import polars as pl
from rich.console import Console
from rich.table import Table

from mimir.api.client import Client
from mimir.api.engine import MimirEngine, Inquiry
from mimir.api.loaders import FileConfigLoader
from mimir.api.exceptions import MimirConfigError, MimirQueryError
from mimir.api.models import InquiryRequest


app = typer.Typer(
    help="Mimir: A semantic layer for data analytics.",
    add_completion=False,
)
list_app = typer.Typer(help="List available definitions.")
app.add_typer(list_app, name="list")

console = Console()


def get_engine(
    configs_path: Path,
    secrets_path: t.Optional[Path],
    validate_connections: bool = True,
) -> MimirEngine:
    """Helper to instantiate the MimirEngine."""
    secret_path_str = (
        str(secrets_path) if secrets_path and validate_connections else None
    )
    return MimirEngine(
        config_loader=FileConfigLoader(
            base_path=str(configs_path),
            secret_base_path=secret_path_str,
        ),
        validate_connections=validate_connections,
    )


@app.command()
def validate(
    configs_path: Path = typer.Option(
        "example/configs",
        "--configs",
        "-c",
        help="Path to the configs directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
    secrets_path: t.Optional[Path] = typer.Option(
        "example/secrets",
        "--secrets",
        "-s",
        help="Path to the secrets directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
    no_secrets: bool = typer.Option(
        False,
        "--no-secrets",
        help="Skip the retrieval of secrets and validation of connections.",
    ),
):
    """
    Validates Mimir configuration files.
    """
    typer.echo(f"Validating configs in: {configs_path}")
    validate_connections = not no_secrets

    if validate_connections and secrets_path:
        typer.echo(f"Using secrets from: {secrets_path}")
    else:
        typer.echo("Skipping secrets and connection validation.")

    try:
        engine = get_engine(configs_path, secrets_path, validate_connections)
        sources = engine.get_sources()
        metrics = engine.get_metrics()
        dimensions = engine.get_dimensions()
        typer.secho("✅ All configurations are valid.", fg=typer.colors.GREEN)
        typer.echo(
            f"Found {len(sources)} sources, {len(metrics)} metrics, and {len(dimensions)} dimensions."
        )
    except MimirConfigError as e:
        typer.secho(f"❌ Invalid configuration found: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.secho(f"An unexpected error occurred: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


@app.command()
def query(
    metrics: t.List[str] = typer.Option(..., "--metric", "-m", help="Metric to query."),
    dimensions: t.List[str] = typer.Option(
        [], "--dimension", "-d", help="Dimension to group by."
    ),
    granularity: t.Optional[str] = typer.Option(
        None, "--granularity", "-g", help="Time granularity (e.g., day, week)."
    ),
    start_date: t.Optional[str] = typer.Option(
        None, "--start-date", help="Start date in YYYY-MM-DD format."
    ),
    end_date: t.Optional[str] = typer.Option(
        None, "--end-date", help="End date in YYYY-MM-DD format."
    ),
    global_filter: t.Optional[str] = typer.Option(
        None, "--filter", "-f", help="SQL WHERE clause to apply."
    ),
    order_by: t.Optional[str] = typer.Option(
        None, "--order-by", "-o", help="Column to order the results by."
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Compile and print the final SQL query without executing it.",
    ),
    host: t.Optional[str] = typer.Option(
        None,
        "--host",
        help="The host of a running Mimir API server to query instead of running locally.",
    ),
    configs_path: Path = typer.Option(
        "example/configs",
        "--configs",
        "-c",
        help="Path to the configs directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
    secrets_path: Path = typer.Option(
        "example/secrets",
        "--secrets",
        "-s",
        help="Path to the secrets directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
):
    """
    Runs a query against the Mimir engine.
    """
    request = InquiryRequest(
        metrics=metrics,
        dimensions=dimensions,
        granularity=granularity,
        start_date=start_date,
        end_date=end_date,
        global_filter=global_filter,
        order_by=order_by,
    ).parse_granularity()

    try:
        if host:
            typer.echo(f"Querying remote Mimir host: {host}")
            client = Client(uri=host)
            result_table = client.query(inquiry=request)
        else:
            engine = get_engine(configs_path, secrets_path)
            typer.echo("Building inquiry...")
            inquiry = Inquiry(mimir_engine=engine, **request.model_dump())

            if dry_run:
                typer.echo("Compiling query...")
                final_sql = inquiry.compile()
                console.print(final_sql)
                return

            typer.echo("Dispatching inquiry...")
            result_table = inquiry.dispatch()

        if result_table.num_rows == 0:
            typer.echo("Query returned no results.")
            return

        df = pl.from_arrow(result_table)
        console.print(df)

    except (MimirConfigError, MimirQueryError) as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.secho(f"An unexpected error occurred: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


@app.command()
def init(
    project_name: str = typer.Argument(
        ".", help="The name of the project directory to create."
    ),
):
    """
    Initializes a new Mimir project with a default directory structure.
    """
    base_path = Path(project_name)
    if base_path.exists() and base_path.is_file():
        typer.secho(
            f"Error: Project path '{base_path}' exists and is a file.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)

    if base_path.exists() and any(base_path.iterdir()):
        typer.secho(
            f"Warning: Project directory '{base_path}' is not empty.",
            fg=typer.colors.YELLOW,
        )

    typer.echo(f"Initializing Mimir project in '{base_path}'...")

    dirs = [
        base_path / "configs" / "metrics",
        base_path / "configs" / "dimensions",
        base_path / "configs" / "sources",
        base_path / "secrets",
    ]

    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        typer.echo(f"  Created {d}/")

    typer.secho("✅ Project initialized successfully.", fg=typer.colors.GREEN)


@app.command()
def describe(
    definition_name: str = typer.Argument(
        ..., help="Name of the definition to describe."
    ),
    definition_type: str = typer.Argument(
        "metric", help="Type of definition (metric, dimension, or source)."
    ),
    configs_path: Path = typer.Option(
        "example/configs",
        "--configs",
        "-c",
        help="Path to the configs directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
):
    """
    Describes a single Mimir definition.
    """
    engine = get_engine(configs_path, None, validate_connections=False)
    try:
        if definition_type == "metric":
            definition = engine.get_metric(definition_name)
        elif definition_type == "dimension":
            definition = engine.get_dimension(definition_name)
        elif definition_type == "source":
            definition = engine.get_source(definition_name)
        else:
            typer.secho(
                f"Invalid definition type: {definition_type}", fg=typer.colors.RED
            )
            raise typer.Exit(code=1)

        table = Table(title=f"{definition_type.capitalize()}: {definition.name}")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="magenta")

        for field, value in definition.model_dump().items():
            if value is not None:
                table.add_row(field, str(value))

        console.print(table)

    except MimirConfigError as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


# ... (existing code) ...


# Add the new list commands
@list_app.command("sources")
def list_sources(
    configs_path: Path = typer.Option(
        "example/configs",
        "--configs",
        "-c",
        help="Path to the configs directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
):
    """Lists all available sources."""
    engine = get_engine(configs_path, None, validate_connections=False)
    sources = engine.get_sources()
    table = Table("Name", "Time Column", "Description")
    for source in sources:
        table.add_row(source.name, source.time_col, source.description)
    console.print(table)


@list_app.command("metrics")
def list_metrics(
    configs_path: Path = typer.Option(
        "example/configs",
        "--configs",
        "-c",
        help="Path to the configs directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
):
    """Lists all available metrics."""
    engine = get_engine(configs_path, None, validate_connections=False)
    metrics = engine.get_metrics()
    table = Table("Name", "Source", "Description")
    for metric in metrics:
        table.add_row(metric.name, metric.source.name, metric.description)
    console.print(table)


@list_app.command("dimensions")
def list_dimensions(
    configs_path: Path = typer.Option(
        "example/configs",
        "--configs",
        "-c",
        help="Path to the configs directory.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
):
    """Lists all available dimensions."""
    engine = get_engine(configs_path, None, validate_connections=False)
    dimensions = engine.get_dimensions()
    table = Table("Name", "Source", "Description")
    for dim in dimensions:
        table.add_row(dim.name, dim.source_name, dim.description)
    console.print(table)


create_app = typer.Typer(help="Create new definitions.")
app.add_typer(create_app, name="create")


@create_app.command("metric")
def create_metric(
    configs_path: Path = typer.Option(
        "configs",
        "--configs",
        "-c",
        help="Path to the configs directory to create the file in.",
        resolve_path=True,
    ),
):
    """Creates a new metric definition interactively."""
    typer.echo("Creating a new metric...")
    name = typer.prompt("Metric Name (e.g., rentals_revenue)")
    source_name = typer.prompt("Source Name (e.g., rentals)")
    sql = typer.prompt("SQL Expression (e.g., SUM(amount))")
    description = typer.prompt("Description", default="")

    metric_data = {
        "name": name,
        "source_name": source_name,
        "sql": f"SELECT {sql} as {name}",
        "description": description,
    }

    # a new import is needed
    import yaml

    file_path = configs_path / "metrics" / f"{name}.yaml"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as f:
        yaml.dump(metric_data, f, sort_keys=False)

    typer.secho(f"✅ Metric '{name}' created at {file_path}", fg=typer.colors.GREEN)


@create_app.command("dimension")
def create_dimension(
    configs_path: Path = typer.Option(
        "configs",
        "--configs",
        "-c",
        help="Path to the configs directory to create the file in.",
        resolve_path=True,
    ),
):
    """Creates a new dimension definition interactively."""
    typer.echo("Creating a new dimension...")
    name = typer.prompt("Dimension Name (e.g., dim_customer_country)")
    source_name = typer.prompt("Source Name (e.g., rentals)")
    sql = typer.prompt("SQL Expression (e.g., country.name)")
    description = typer.prompt("Description", default="")

    dim_data = {
        "name": name,
        "source_name": source_name,
        "sql": f"SELECT {sql} as {name}",
        "description": description,
    }

    import yaml

    file_path = configs_path / "dimensions" / f"{name}.yaml"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as f:
        yaml.dump(dim_data, f, sort_keys=False)

    typer.secho(f"✅ Dimension '{name}' created at {file_path}", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
