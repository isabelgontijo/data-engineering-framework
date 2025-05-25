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
    path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    parts = path.split("/")

    # Exemplo: /Workspace/Repos/your-team/your-repo/dev/notebook-name
    try:
        repo_owner = parts[2]           # 'Repos'
        team_folder = parts[3]          # 'your-team'
        repo_folder = parts[4]          # 'your-repo'
        env_folder = parts[5]           # 'dev', 'stg' ou 'prd'
    except IndexError:
        raise ValueError(f"Unexpected notebook path format: {path}")

    repo_root_path = "/" + "/".join(parts[:6])  # até o env_folder, como base do repo
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").getOrElse("unknown_user")

    return {
        "full_path": path,
        "repo_owner": repo_owner,
        "team_folder": team_folder,
        "repo_folder": repo_folder,
        "env_folder": env_folder,
        "repo_root_path": repo_root_path,
        "user": user
    }
