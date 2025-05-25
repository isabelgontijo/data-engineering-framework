from databricks.sdk.runtime import *
from config.environments import ENVIRONMENTS


class EnvironmentConfig:
    """
    Automatically detects the current Databricks environment based on the cluster's Org ID,
    loads the corresponding configuration, and builds ABFSS paths for data layers.
    
    Attributes:
        environment (str): Environment name (e.g., 'dev', 'stg', 'prd')
        catalog (str): Unity Catalog name for the environment
        storage_account (str): Azure storage account name
        paths (dict): Dictionary with keys 'bronze', 'silver', 'gold' and ABFSS paths as values

    Example:
        >>> from src.config_loader.env_config import EnvironmentConfig
        >>> env = EnvironmentConfig()
        >>> print(env.environment)         # dev / stg / prd
        >>> print(env.catalog)            # Unity Catalog name
        >>> print(env.storage_account)    # Azure storage account
        >>> print(env.paths["silver"])    # abfss://silver@<storage>.dfs.core.windows.net/
    """

    def __init__(self):
        self.org_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
        self.config = self._load_environment_config(self.org_id)

        self.environment = self.config["env"]
        self.catalog = self.config["catalog"]
        self.storage_account = self.config["storage"]

        self.paths = self._build_paths()

    def _load_environment_config(self, org_id: str) -> dict:
        """
        Loads environment-specific settings based on the Org ID.
        """
        if org_id not in ENVIRONMENTS:
            raise ValueError(f"Unknown Org ID: {org_id}")
        return ENVIRONMENTS[org_id]

    def _build_paths(self) -> dict:
        """
        Constructs ABFSS paths for medallion architecture data layers.
        """
        base = self.storage_account
        return {
            "bronze": f"abfss://bronze@{base}.dfs.core.windows.net/",
            "silver": f"abfss://silver@{base}.dfs.core.windows.net/",
            "gold": f"abfss://gold@{base}.dfs.core.windows.net/"
        }
