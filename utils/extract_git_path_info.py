from typing import Dict
from databricks.sdk import WorkspaceClient

def extract_git_path_info() -> Dict[str, str]:
    """
    Extract useful path information from the current notebook's Git path in Databricks.

    Returns:
        dict: {
            'full_path': str,       # Full notebook path in workspace
            'repo_folder': str,     # Git repo root folder name
            'env_folder': str,      # Environment or branch folder name
            'user_folder': str      # User folder if applicable
        }
    
    Raises:
        ValueError: if the notebook path format is unexpected.
    """
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

    full_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    parts = full_path.strip("/").split("/")

    # Expected pattern:
    # /Workspace/Repos/<repo_folder>/<env_folder>/... or
    # /Repos/<repo_folder>/<env_folder>/...
    # Indexes:   0        1           2           3

    try:
        repo_folder = parts[2]
        env_folder = parts[3]
        user_folder = parts[4] if len(parts) > 4 else ""
    except IndexError as e:
        raise ValueError(f"Unexpected notebook path format: {full_path}") from e

    return {
        "full_path": full_path,
        "repo_folder": repo_folder,
        "env_folder": env_folder,
        "user_folder": user_folder
    }
