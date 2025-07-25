from typer.testing import CliRunner
import pytest
from pathlib import Path
import yaml

from mimir.cli import app

runner = CliRunner()


# Fixture for a temporary valid configs directory
@pytest.fixture
def valid_configs(tmp_path: Path, mocker):
    # Patch the cache so we always get fresh instances
    mocker.patch("mimir.api.engine.ttl_cache", lambda ttl: lambda func: func)

    configs_dir = tmp_path / "configs"
    sources_dir = configs_dir / "sources"
    metrics_dir = configs_dir / "metrics"
    dimensions_dir = configs_dir / "dimensions"

    sources_dir.mkdir(parents=True)
    metrics_dir.mkdir(parents=True)
    dimensions_dir.mkdir(parents=True)

    # Create dummy source
    with open(sources_dir / "sources.yaml", "w") as f:
        yaml.dump(
            {
                "test_source": {
                    "name": "test_source",
                    "time_col": "ts",
                    "sql": "SELECT 1 as id, '2025-01-01'::timestamp as ts",
                    "connection_name": "dummy",
                }
            },
            f,
        )

    # Create dummy metric
    with open(metrics_dir / "test_metric.yaml", "w") as f:
        yaml.dump(
            {
                "name": "test_metric",
                "source_name": "test_source",
                "sql": "SELECT COUNT(*) as test_metric",
            },
            f,
        )

    # Create dummy dimension
    with open(dimensions_dir / "test_dimension.yaml", "w") as f:
        yaml.dump(
            {
                "name": "test_dimension",
                "source_name": "test_source",
                "sql": "SELECT id as test_dimension",
            },
            f,
        )

    return configs_dir


def test_list_sources(valid_configs):
    result = runner.invoke(app, ["list", "sources", "--configs", str(valid_configs)])
    assert result.exit_code == 0
    assert "test_source" in result.stdout


def test_list_metrics(valid_configs):
    result = runner.invoke(app, ["list", "metrics", "--configs", str(valid_configs)])
    assert result.exit_code == 0
    assert "test_metric" in result.stdout


def test_list_dimensions(valid_configs):
    result = runner.invoke(app, ["list", "dimensions", "--configs", str(valid_configs)])
    assert result.exit_code == 0
    assert "test_dimension" in result.stdout


def test_describe_metric(valid_configs):
    result = runner.invoke(
        app, ["describe", "test_metric", "metric", "--configs", str(valid_configs)]
    )
    assert result.exit_code == 0
    assert "Metric: test_metric" in result.stdout
    assert "source_name" in result.stdout
    assert "test_source" in result.stdout


def test_init_command(tmp_path: Path):
    project_dir = tmp_path / "new_mimir_project"
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0
    assert "Project initialized successfully" in result.stdout
    assert (project_dir / "configs" / "metrics").exists()
    assert (project_dir / "configs" / "sources").exists()
    assert (project_dir / "secrets").exists()


def test_create_metric_command(tmp_path: Path):
    configs_dir = tmp_path / "configs"
    result = runner.invoke(
        app,
        ["create", "metric", "--configs", str(configs_dir)],
        input="new_metric\nmy_source\nSUM(value)\nA new metric",
    )
    assert result.exit_code == 0
    metric_file = configs_dir / "metrics" / "new_metric.yaml"
    assert metric_file.exists()
    with open(metric_file) as f:
        content = yaml.safe_load(f)
        assert content["name"] == "new_metric"
        assert content["sql"] == "SELECT SUM(value) as new_metric"


def test_query_host_flag(mocker):
    # Mock the client to avoid real HTTP requests
    mock_client_class = mocker.patch("mimir.cli.Client")
    mock_client_instance = mock_client_class.return_value

    # We don't need a real pyarrow table, just a mock with num_rows
    mock_table = mocker.MagicMock()
    mock_table.num_rows = 0  # So it prints "no results" and we dont have to mock polars
    mock_client_instance.query.return_value = mock_table

    result = runner.invoke(
        app,
        ["query", "--host", "http://fake-mimir-host.com", "--metric", "some_metric"],
    )

    assert result.exit_code == 0
    assert "Querying remote Mimir host" in result.stdout
    mock_client_class.assert_called_with(uri="http://fake-mimir-host.com")
    mock_client_instance.query.assert_called_once()


def test_query_dry_run_fails_without_secrets(valid_configs):
    # This test confirms that dry-run (which needs to compile) fails
    # if it can't instantiate a connection.
    secrets_dir = valid_configs.parent / "secrets"
    secrets_dir.mkdir()
    with open(secrets_dir / "dummy.json", "w") as f:
        f.write('{"host": "localhost"}')

    result = runner.invoke(
        app,
        [
            "query",
            "--metric",
            "test_metric",
            "--configs",
            str(valid_configs),
            "--secrets",
            str(secrets_dir),
            "--dry-run",
        ],
    )
    # This will fail now because it can't connect to a real DB, which is fine.
    # The point is that it passed the config validation stage.
    assert (
        "Error: Invalid or missing configuration for source 'test_source'"
        in result.stdout
    )
