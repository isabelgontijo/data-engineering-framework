from src.config import ENVIRONMENTS
from src.notebook_context import get_notebook_context_info


class EnvironmentConfig:
    """
    Detects the current Databricks environment using notebook context,
    and loads corresponding environment-specific configurations.

    Attributes:
        environment (str): Environment name (e.g., 'dev', 'stg', 'prd')
        catalog (str): Unity Catalog name for the environment
        storage_account (str): Azure storage account name
        paths (dict): Dictionary with keys 'bronze', 'silver', 'gold' and ABFSS paths as values
    """

    def __init__(self):
        self.context_info = get_notebook_context_info()
        self.env = self.context_info["env_folder"]

        print(f"[EnvironmentConfig] Detected environment from notebook path: '{self.env}'")

        self.env_config = self._load_environment_config(self.env)
        self.environment = self.env_config["env"]
        self.catalog = self.env_config["catalog"]
        self.storage_account = self.env_config["storage"]
        self.paths = self._build_paths()

    def _load_environment_config(self, env: str) -> dict[str, str]:
        if env not in ENVIRONMENTS:
            raise ValueError(f"Unknown environment folder: '{env}'")
        return ENVIRONMENTS[env]

    def _build_paths(self) -> dict[str, str]:
        base = self.storage_account
        return {
            "bronze": f"abfss://bronze@{base}.dfs.core.windows.net/",
            "silver": f"abfss://silver@{base}.dfs.core.windows.net/",
            "gold": f"abfss://gold@{base}.dfs.core.windows.net/"
        }
