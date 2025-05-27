from config.settings import ENVIRONMENTS

class EnvironmentConfig:
    """
    Automatically detects the current Databricks environment based on the Git folder (branch)
    and loads the corresponding configuration.

    Attributes:
        environment (str): Environment name (e.g., 'dev', 'stg', 'prd')
        catalog (str): Unity Catalog name for the environment
        storage_account (str): Azure storage account name
        paths (dict): Dictionary with keys 'bronze', 'silver', 'gold' and ABFSS paths as values

    Example Usage:
    from src.config_loader.env_config import EnvironmentConfig

    env = EnvironmentConfig()
    print(env.environment)         # dev / stg / prd
    print(env.catalog)             # Unity Catalog name
    print(env.paths["bronze"])     # abfss://bronze@<storage>.dfs.core.windows.net/
    """

    def __init__(self):
        self.branch = self._extract_git_folder_from_path()
        print(f"[EnvironmentConfig] Detected Git branch: '{self.branch}'")

        self.config = self._load_environment_config(self.branch)

        self.environment = self.config["env"]
        self.catalog = self.config["catalog"]
        self.storage_account = self.config["storage"]

        self.paths = self._build_paths()

    def _load_environment_config(self, branch: str) -> dict:
        """
        Loads environment-specific settings based on the Git folder (branch).
        """
        if branch not in ENVIRONMENTS:
            raise ValueError(f"Unknown Git branch or folder: '{branch}'")
        return ENVIRONMENTS[branch]

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
