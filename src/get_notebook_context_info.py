import re

def get_notebook_context_info():
    """
    Extract useful context information from Databricks notebook context,
    including parsed path components like environment folder, team folder, and repo folder.

    Returns:
        dict: Dictionary with selected context info and parsed path info.
    """
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

    notebook_path = ctx.notebookPath().get()
    path_parts = notebook_path.strip("/").split("/")

    browser_host_name = ctx.browserHostName().get()
    workspace_id = re.search(r"adb-(\d+)", browser_host_name).group(1)

    info = {
        "notebook_path": notebook_path,
        "env_folder": path_parts[1],
        "team_folder": path_parts[2],
        "repo_folder": path_parts[3]
        "api_url": ctx.apiUrl().get(),
        "api_token": ctx.apiToken().get(),
        "browser_host_name": browser_host_name,
        "workspace_id": workspace_id,
        "acl_path_of_acl_root": ctx.aclPathOfAclRoot().get(),
        "notebook_id": ctx.notebookId().get(),
        "cluster_id": ctx.clusterId().get(),
        "user": ctx.user().get(),
        "full_context": ctx.safeAsJson(),
    }

    return info
