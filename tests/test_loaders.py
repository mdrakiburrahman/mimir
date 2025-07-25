import pytest
from mimir.api.loaders import FileConfigLoader
from mimir.api.types import CONFIG_TYPE

FIXTURES_PATH = "tests/fixtures"


@pytest.fixture
def config_loader():
    return FileConfigLoader(
        base_path=FIXTURES_PATH + "/configs",
        secret_base_path=FIXTURES_PATH + "/secrets",
    )


def test_get_source(config_loader):
    source = config_loader.get(CONFIG_TYPE.SOURCE, "my_source")
    assert source["time_col"] == "created_at"


def test_get_metric(config_loader):
    metric = config_loader.get(CONFIG_TYPE.METRIC, "my_metric")
    assert metric["name"] == "my_metric"


def test_get_dimension(config_loader):
    dimension = config_loader.get(CONFIG_TYPE.DIMENSION, "my_dimension")
    assert dimension["name"] == "my_dimension"


def test_get_secret(config_loader):
    secret = config_loader.get_secret("my_connection")
    assert secret["flavour"] == "postgresql"


def test_get_all_sources(config_loader):
    sources = config_loader.get_all(CONFIG_TYPE.SOURCE)
    assert len(sources) == 2
    assert "my_source" in sources
    assert "inventory" in sources


def test_get_all_metrics(config_loader):
    metrics = config_loader.get_all(CONFIG_TYPE.METRIC)
    assert len(metrics) == 2
    assert "my_metric" in metrics
    assert "stock_level" in metrics


def test_get_all_dimensions(config_loader):
    dimensions = config_loader.get_all(CONFIG_TYPE.DIMENSION)
    assert len(dimensions) == 2
    assert "my_dimension" in dimensions
    assert "my_other_dimension" in dimensions
