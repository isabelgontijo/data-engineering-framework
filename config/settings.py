# config/settings.py

DATABRICKS_HOST = "https://adb-workspace-id.azuredatabricks.net:443"
WORKSPACE_ID = "workspace_id"

ENVIRONMENTS = {
    "dev": {
        "catalog": "dev",
        "storage": "stodatalakedev",
        "service_principal": "dev-service-principal-uuid"
    },
    "stg": {
        "catalog": "stg",
        "storage": "stodatalakeuat",
        "service_principal": "stg-service-principal-uuid"
    },
    "prd": {
        "catalog": "prd",
        "storage": "stodatalakeprd",
        "service_principal": "prd-service-principal-uuid"
    }
}
