from databricks.sdk.runtime import *
import os
from config.environments import ENVIRONMENTS


class EnvironmentConfig:
    """
    Detects the current environment based on the Git branch (vcs.branch)
    and loads corresponding config settings and ABFSS paths.

    Attributes:
        environment (str): dev, stg, prd
        catalog (str): Unity Catalog name
        storage_account (str): Azure storage account name
        paths (dict): bronze/silver/gold ABFSS URIs

    Example Usage:
        env = EnvironmentConfig()
        print(env.environment)  # 'dev'
        print(env.paths["silver"])  # abfss://silver@<storage>.dfs.core.windows.net/
    """

    def __init__(self):
        self.branch = os.environ.get("vcs.branch", "").lower()

        if not self.branch:
            raise RuntimeError("Git branch not detected. Make sure you're running from a Git folder.")

        self.environment = self._map_branch_to_env(self.branch)
        self.config = ENVIRONMENTS[self.environment]

        self.catalog = self.config["catalog"]
        self.storage_account = self.config["storage"]
        self.paths = self._build_paths()

    def _map_branch_to_env(self, branch: str) -> str:
        """
        Maps Git branch names to environments.
        """
        if "develop" in branch:
            return "dev"
        elif "staging" in branch:
            return "stg"
        elif "main" in branch:
            return "prd"
        else:
            raise ValueError(f"Unrecognized Git branch '{branch}' for environment mapping.")

    def _build_paths(self) -> dict:
        base = self.storage_account
        return {
            "bronze": f"abfss://bronze@{base}.dfs.core.windows.net/",
            "silver": f"abfss://silver@{base}.dfs.core.windows.net/",
            "gold": f"abfss://gold@{base}.dfs.core.windows.net/"
        }
