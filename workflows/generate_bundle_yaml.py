def extract_git_path_info() -> dict:
    """
    Extracts useful information from the current notebook path in Databricks Repos,
    such as the full path, team folder, repository name, environment folder (branch),
    repository root path, and current workspace user.

    The expected notebook path format is:
    /Workspace/Repos/<team_folder>/<repo_folder>/<env_folder>/...

    Example:
    /Workspace/Repos/your-team/your-repo/dev/notebook-name

    Returns:
        dict: A dictionary with the following keys:
            - full_path (str): The full notebook path
            - repo_owner (str): Usually 'Repos'
            - team_folder (str): The team folder inside Repos
            - repo_folder (str): The repository name
            - env_folder (str): The environment folder (e.g., dev, stg, prd)
            - repo_root_path (str): Path up to the repository root including environment
            - user (str): Current workspace user

    Raises:
        ValueError: If the notebook path format is unexpected.
    """
    path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").getOrElse("unknown_user")

    parts = path.split("/")

    try:
        repo_owner = parts[2]      # normalmente 'Repos'
        team_folder = parts[3]     # ex: 'your-team'
        repo_folder = parts[4]     # ex: 'your-repo'
        env_folder = parts[5]      # ex: 'dev', 'stg', 'prd'
    except IndexError:
        raise ValueError(f"Unexpected notebook path format: {path}")

    repo_root_path = "/" + "/".join(parts[:6])

    return {
        "full_path": path,
        "repo_owner": repo_owner,
        "team_folder": team_folder,
        "repo_folder": repo_folder,
        "env_folder": env_folder,
        "repo_root_path": repo_root_path,
        "user": user
    }
