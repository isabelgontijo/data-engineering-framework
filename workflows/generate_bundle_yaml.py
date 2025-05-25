import yaml
from utils.extract_git_path_info import extract_git_path_info
from config.settings import ENVIRONMENTS, DATABRICKS_HOST

def generate_bundle_yaml(output_path="databricks_asset_bundle.yml"):
    """
    Generates a Databricks asset bundle YAML configuration file based on the current
    environment folder extracted from the notebook path.

    It reads environment-specific settings from the global ENVIRONMENTS dictionary
    (which should include keys like 'service_principal' per environment) and
    creates a bundle configuration with the appropriate workspace, mode, and service principal.

    Args:
        output_path (str): Path where the generated YAML file will be saved. Defaults to "databricks_asset_bundle.yml".

    Raises:
        ValueError: If the environment folder extracted from the notebook path is not present in ENVIRONMENTS.

    Side Effects:
        Writes a YAML file to the specified output_path.
        Prints a confirmation message with the environment used.

    Example:
        generate_bundle_yaml()
        # Output: Bundle YAML generated at databricks_asset_bundle.yml for environment 'dev'
    """
    info = extract_git_path_info()
    env = info.get("env_folder")

    if env not in ENVIRONMENTS:
        raise ValueError(f"Unknown environment '{env}' from notebook path")

    env_config = ENVIRONMENTS[env]

    bundle = {
        "bundle": {"name": "databricks_asset_bundle"},
        "include": ["../../workflows/jobs/*.yml"],
        "targets": {
            env: {
                "mode": "production" if env != "dev" else "development",
                "default": True,
                "workspace": {
                    "host": DATABRICKS_HOST,
                    "root_path": f"/Workspace/Repos/{info.get('team_folder')}/{info.get('repo_folder')}/workflows/.bundle/${{bundle.name}}/${{bundle.target}}"
                },
                "run_as": {
                    "service_principal_name": env_config["service_principal"]
                }
            }
        }
    }

    with open(output_path, "w") as f:
        yaml.dump(bundle, f, sort_keys=False)

    print(f"Bundle YAML generated at {output_path} for environment '{env}'")
