from src.formatted_print import formatted_print
from databricks.sdk.runtime import *
from datetime import datetime

#### DEBUG ####

def formatted_print(message: str, debug: bool = False, force_debug_only: bool = False):
    if force_debug_only and not debug:
        return

    if debug:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] {message}")
    else:
        print(message)

def debug_df(df, debug: bool = False, label: str = "DataFrame"):
    if not debug:
        formatted_print(f"[utils/debug_df] Skipping debug for '{label}' (debug=False)", debug)
        return

    formatted_print(f"[utils/debug_df] --- Debug info for '{label}' ---", debug)
    formatted_print(f"[utils/debug_df] Row count: {df.count()}", debug)

    print(f"[utils/debug_df] Schema:")
    df.printSchema()

    print(f"[utils/debug_df] Sample:")
    df.display()

    formatted_print(f"[utils/debug_df] --- End of debug for '{label}' ---", debug)


### WIDGETS UTILS ####

def get_bool_widget(name: str, debug: bool = False) -> bool:
    value = dbutils.widgets.get(name).strip().lower()
    formatted_print(f"[utils/get_bool_widget] Widget '{name}' returned: {value}", debug)
    if value not in ["true", "false"]:
        raise ValueError(f"[{name}] Invalid value: '{value}'. Use 'true' or 'false'.")
    return value == "true"

def get_str_widget(name: str, debug: bool = False) -> str:
    value = dbutils.widgets.get(name).strip()
    formatted_print(f"[utils/get_str_widget] Widget '{name}' returned: '{value}'", debug)
    return value

def get_date_widget(name: str, fmt: str = "%Y-%m-%d", debug: bool = False) -> datetime:
    raw = dbutils.widgets.get(name).strip()
    formatted_print(f"[utils/get_date_widget] Widget '{name}' raw value: '{raw}'", debug)
    try:
        return datetime.strptime(raw, fmt)
    except ValueError:
        raise ValueError(f"[{name}] Invalid date: '{raw}'. Expected format: '{fmt}'.")

def get_list_widget(name: str, sep: str = ",", debug: bool = False) -> list[str]:
    raw = dbutils.widgets.get(name).strip()
    values = [v.strip() for v in raw.split(sep) if v.strip()]
    formatted_print(f"[utils/get_list_widget] Widget '{name}' returned: {values}", debug)
    return values


#### DOCUMENTATION UTILS ####

def apply_table_tags(table: str, tags: dict, debug: bool = False):
    formatted_print(f"[utils/apply_table_tags] Starting tag application on table: {table}", debug)
    for key, value in tags.items():
        try:
            spark.sql(f"ALTER TABLE {table} SET TAGS('{key}' = '{value}')")
            formatted_print(f"[utils/apply_table_tags] Tag applied successfully: {key} = {value}", debug)
        except Exception as e:
            formatted_print(f"[utils/apply_table_tags] Failed to apply tag '{key}': {e}", debug)


#### FIRST WRITE UTILS ####

def register_table_notebook(
        table: str, 
        notebook_path: str, 
        debug: bool = False, 
        log_table: str = 'foundation.observability.notebook_table_map'
    ):
    formatted_print(f"[utils/register_table_notebook] Registering notebook for table: {table}", debug)
    try:
        query = f"""
            INSERT INTO {log_table} 
            (`table`, `notebook_path`) 
            VALUES ('{table}', '{notebook_path}')
        """
        spark.sql(query)
        formatted_print(f"[utils/register_table_notebook] Notebook registered successfully.", debug)
        formatted_print(f"[utils/register_table_notebook] Executed SQL:\n{query}", debug, force_debug_only=True)
    except Exception as e:
        formatted_print(f"[utils/register_table_notebook] Failed to register notebook for {table}: {e}", debug)