def get_notebook_context_info():
    """
    Extract useful context information from Databricks notebook context,
    including parsed path components like environment folder, team folder, and repo folder.

    Returns:
        dict: Dictionary with selected context info and parsed path info.
    """
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

    notebook_path = ctx.notebookPath().get()
    # Example path: /Repos/dev/my-team/my-repo/...

    path_parts = notebook_path.strip("/").split("/")

    # Validate expected structure
    # Expected: Repos / <env_folder> / <team_folder> / <repo_folder> / ...
    if len(path_parts) < 4 or path_parts[0].lower() != "repos":
        raise ValueError(f"Unexpected notebook path structure: {notebook_path}")

    info = {
        "notebook_path": notebook_path,
        "api_url": ctx.apiUrl().get(),
        "api_token": ctx.apiToken().get(),
        "browser_host_name": ctx.browserHostName().get(),
        "acl_path_of_acl_root": ctx.aclPathOfAclRoot().get(),
        "mlflow_git_relative_path": ctx.mlflowGitRelativePath().get(),
        "notebook_id": ctx.notebookId().get(),
        "cluster_id": ctx.clusterId().get(),
        "user": ctx.user().get(),
        "full_context": ctx.safeAsJson(),

        # Parsed path info:
        "env_folder": path_parts[1],
        "team_folder": path_parts[2],
        "repo_folder": path_parts[3]
    }

    return info
