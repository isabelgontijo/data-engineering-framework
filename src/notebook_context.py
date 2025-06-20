from src.custom_exception import CustomException
from databricks.sdk.runtime import *
import json
import re


def notebook_context() -> dict:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        full_context = ctx.safeToJson()
        attributes = json.loads(full_context)["attributes"]
    except Exception as e:
        raise CustomException(f"{e}")

    def get_attr(key: str):
        return attributes.get(key, None)

    path_parts = get_attr("notebook_path").strip("/").split("/")

    return {
        # Diretórios deduzidos
        "env_folder": path_parts[1] if len(path_parts) > 1 else None,
        "team_folder": path_parts[2] if len(path_parts) > 2 else None,
        "repo_folder": path_parts[3] if len(path_parts) > 3 else None,

        # Contexto direto da API
        "mlflow_git_relative_path": get_attr("mlflowGitRelativePath"),
        "non_uc_api_token": get_attr("non_uc_api_token"),
        "dbr_platform_channel": get_attr("dbrPlatformChannel"),
        "enable_delta_live_tables_analysis": get_attr("enableDeltaLiveTablesAnalysis"),
        "mlflow_git_status": get_attr("mlflowGitStatus"),
        "mlflow_git_reference": get_attr("mlflowGitReference"),
        "mlflow_git_url": get_attr("mlflowGitUrl"),
        "notebook_path": get_attr("notebook_path"),
        "notebook_id": get_attr("notebook_id"),
        "org_id": get_attr("orgId"),
        "mlflow_git_commit": get_attr("mlflowGitCommit"),
        "mlflow_git_reference_type": get_attr("mlflowGitReferenceType"),
        "cluster_id": get_attr("clusterId"),
        "workload_class": get_attr("workloadClass"),
        "api_url": get_attr("api_url"),
        "object_type": get_attr("objectType"),
        "acl_path_of_acl_root": get_attr("aclPathOfAclRoot"),
        "workload_id": get_attr("workloadId"),
        "mlflow_git_provider": get_attr("mlflowGitProvider"),
        "api_token": get_attr("api_token"),
        "job_group": get_attr("jobGroup"),
        "user": get_attr("user"),
        "current_run_id": get_attr("currentRunId"),
        "browser_host_name": get_attr("browserHostName"),
    }
