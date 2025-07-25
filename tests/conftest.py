import pytest
from mimir.api.engine import MimirEngine
from mimir.api.loaders import FileConfigLoader


@pytest.fixture
def mimir_engine_factory(mocker):
    def _factory(config_path, secrets_path):
        loader = FileConfigLoader(
            base_path=config_path,
            secret_base_path=secrets_path,
        )
        return MimirEngine(config_loader=loader)

    return _factory
