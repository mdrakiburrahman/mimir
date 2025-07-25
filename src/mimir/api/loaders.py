import yaml
import json
import typing as t

from pathlib import Path
from abc import ABC, abstractmethod

from mimir.api.types import CONFIG_TYPE

import logging

logger = logging.getLogger(__name__)


class BaseConfigLoader(ABC):
    """Abstract base class for configuration loaders.

    This class defines the interface for loaders that fetch raw configuration
    data for Mimir definitions (Sources, Dimensions, Metrics) and secrets.
    """

    @abstractmethod
    def get(
        self, config_type: CONFIG_TYPE, config_name: str
    ) -> t.Optional[t.Dict[str, t.Any]]:
        """Fetches a single configuration dictionary by name.

        Args:
            config_type: The type of configuration to fetch.
            config_name: The specific name of the configuration.

        Returns:
            A dictionary containing the configuration, or None if not found.
        """
        raise NotImplementedError

    @abstractmethod
    def get_all(self, config_type: CONFIG_TYPE) -> t.Dict[str, t.Any]:
        """Fetches all configuration dictionaries of a given type.

        Args:
            config_type: The type of configurations to fetch.

        Returns:
            A list of dictionaries, where each dictionary is a configuration.
        """
        raise NotImplementedError

    @abstractmethod
    def get_secret(self, secret_name: str) -> t.Optional[t.Dict[str, t.Any]]:
        """Fetches a single secret dictionary by name.

        Args:
            secret_name: The name of the secret to fetch.

        Returns:
            A dictionary containing the secret.
        """
        raise NotImplementedError


class FileConfigLoader(BaseConfigLoader):
    """Loads Mimir configurations and secrets from the local filesystem.

    This class implements the BaseConfigLoader interface to read YAML-based
    definition files and JSON-based secret files from a specified directory
    structure.

    Args:
        base_path: The root directory for the configuration folders (e.g., 'configs').
        secret_base_path: The root directory for the secrets folder.
        source_folder: The name of the subfolder containing source definitions.
        dimensions_folder: The name of the subfolder containing dimension definitions.
        metrics_folder: The name of the subfolder containing metric definitions.
    """

    def __init__(
        self,
        base_path: str = ".",
        secret_base_path: t.Optional[str] = None,
        source_folder: str = "sources",
        dimensions_folder: str = "dimensions",
        metrics_folder: str = "metrics",
    ):
        """Initializes the FileConfigLoader.

        Args:
            base_path: The base path for the configuration files.
            secret_base_path: The base path for the secret files.
            source_folder: The name of the folder containing the source configurations.
            dimensions_folder: The name of the folder containing the dimension configurations.
            metrics_folder: The name of the folder containing the metric configurations.
        """
        super().__init__()
        self.folders = {
            CONFIG_TYPE.SOURCE: Path(f"{base_path}/{source_folder}"),
            CONFIG_TYPE.DIMENSION: Path(f"{base_path}/{dimensions_folder}"),
            CONFIG_TYPE.METRIC: Path(f"{base_path}/{metrics_folder}"),
        }
        self.secret_folder = Path(secret_base_path) if secret_base_path else None

    def _get_configs_from_fs(
        self, config_type: CONFIG_TYPE, config_pattern: str
    ) -> t.List[t.Dict[str, t.Any]]:
        """Gets a configuration from a file within a specific base path.

        Args:
            config_type: The type of configuration to get (e.g., "Metric").
            config_name: The pattern of the configuration file (without extension), could be exact name or match multiple configs.
            base_path: The directory path where the configuration file is located.

        Returns:
            The loaded YAML configuration as a dictionary.
        """
        try:
            options = [
                path
                for path in self.folders[config_type].glob(f"{config_pattern}.*")
                if path.suffix in {".yaml", ".yml"}
            ]
            if not options:
                raise FileNotFoundError(
                    f"No file matching for configuration pattern: {config_pattern}"
                )

            return [yaml.safe_load(f.read_bytes()) for f in options]

        except FileNotFoundError as e:
            if config_type == CONFIG_TYPE.DIMENSION:
                return []
            else:
                raise FileNotFoundError(e) from e

    @staticmethod
    def _get_secret_from_fs(path: Path) -> t.Dict[str, t.Any]:
        """Gets a secret from a file.

        Args:
            path: The full path to the JSON secret file.

        Returns:
            The loaded JSON secret as a dictionary.
        """
        s = json.loads(path.read_bytes())
        if not isinstance(s, dict):
            raise ValueError("JSON secret is not a dictionary")
        return s

    def _get_sources_configs(self) -> t.Dict[str, t.Dict]:
        """Helper to load all source configurations from their unique dictionary format."""

        return {
            source_name: source_config
            for conf in self._get_configs_from_fs(CONFIG_TYPE.SOURCE, "*")
            for source_name, source_config in conf.items()
        }

    def get(
        self, config_type: CONFIG_TYPE, config_name: str
    ) -> t.Optional[t.Dict[str, t.Any]]:
        """Fetches a single configuration dictionary by name from the filesystem.

        This method handles the special case for sources, which are stored in a
        single dictionary file, versus other definitions which are in individual files.

        Args:
            config_type: The type of configuration to fetch.
            config_name: The specific name of the configuration.

        Returns:
            A dictionary containing the configuration, or None if not found.
        """
        if config_type == CONFIG_TYPE.SOURCE:
            return self._get_sources_configs().get(config_name)

        options = self._get_configs_from_fs(config_type, config_name)

        if not options:
            return None

        conf, *duplicates = options

        if duplicates:
            raise LookupError(f"Multiple config matching for {config_name}: {options}")

        return conf

    def get_secret(self, secret_name: str) -> t.Optional[t.Dict[str, t.Any]]:
        """Fetches a secret dictionary by name from the filesystem.

        Args:
            secret_name: The name of the secret file (without .json extension).

        Returns:
            A dictionary containing the secret, or None if not found or if
            the secret path is not configured.
        """
        if not self.secret_folder:
            return None

        secret_file = self.secret_folder / f"{secret_name}.json"
        if not secret_file.exists():
            return None

        return self._get_secret_from_fs(secret_file)

    def get_all(self, config_type: CONFIG_TYPE) -> t.Dict[str, t.Any]:
        """Fetches all configuration dictionaries of a given type from the filesystem.

        Args:
            config_type: The type of configurations to fetch.

        Returns:
            A dictionary of configs
        """
        if config_type == CONFIG_TYPE.SOURCE:
            return self._get_sources_configs()

        return {
            conf["name"]: conf
            for conf in self._get_configs_from_fs(config_type, "*")
            if conf.get("name")
        }
