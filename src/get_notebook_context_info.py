def get_notebook_context_info():
    """
    Extract useful context information from Databricks notebook context.

    Returns:
        dict: Dictionary with selected context info.
    """
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

    info = {
        "notebook_path": ctx.notebookPath().get(),
        "api_url": ctx.apiUrl().get(),
        "api_token": ctx.apiToken().get(),
        "browser_host_name": ctx.browserHostName().get(),
        "acl_path_of_acl_root": ctx.aclPathOfAclRoot().get(),
        "mlflow_git_relative_path": ctx.mlflowGitRelativePath().get(),
        "notebook_id": ctx.notebookId().get(),
        "cluster_id": ctx.clusterId().get(),
        "user": ctx.user().get(),
        "full_context": ctx.safeAsJson()
    }

    return info
