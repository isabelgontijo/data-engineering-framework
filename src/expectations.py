from pyspark.sql import Window, DataFrame
from pyspark.sql import functions as F, types as T
from functools import reduce
from typing import List, Dict, Optional, Any

from src.custom_exception import CustomException
from src.notebook_context import notebook_context
from src.formatted_print import formatted_print

class Expectations:
    def __init__(
        self,
        df: DataFrame,
        save_log: bool = True,
        debug: bool = False,
        table_name: Optional[str] = None,
        table_log: str = "foundation.observability.expectations_logs",
        raise_exception: bool = True,
        quarantine: bool = False,
        return_df: bool = False,
    ):
        if not isinstance(save_log, bool):
            raise CustomException("[Expectations/__init__] 'save_log' must be a boolean.")
        if table_name is not None and not isinstance(table_name, str):
            raise CustomException("[Expectations/__init__] 'table_name' must be a string or None.")
        if not isinstance(raise_exception, bool):
            raise CustomException("[Expectations/__init__] 'raise_exception' must be a boolean.")

        self.df = df.withColumn("_metadata", F.array())
        self.save_log = save_log
        self.debug = debug
        self.table_name = table_name
        self.table_log = table_log
        self.raise_exception = raise_exception
        self.quarantine = quarantine
        self.return_df = return_df
        self.env = notebook_context().get("env_folder")
        self.custom_sql_counter = 0

        formatted_print(
            f"""
            [Expectations/__init__] Classe iniciada!
            Parâmetros:
                - save_log: {self.save_log}
                - debug: {self.debug}
                - table_name: {self.table_name}
                - table_log: {self.table_log}
                - raise_exception: {self.raise_exception}
                - quarantine: {self.quarantine}
                - return_df: {self.return_df}
                - env: {self.env}
            """, 
            debug=self.debug, 
            force_debug_only=True
        )

    def apply(self, rules: List[Dict[str, Any]]):
        if not isinstance(rules, list):
            raise CustomException("[apply] 'rules' must be a list.")

        df = self.df
        metadata_list = []
        error_messages = []

        formatted_print(f"[Expectations/apply] Regras recebidas: {'\n'.join([r['quality'] for r in rules])}", debug=self.debug, force_debug_only=True)

        is_not_empty = next((r for r in rules if r.get("quality") == "is_not_empty"), None)
        if is_not_empty:
            criticality = is_not_empty.get("criticality", "error")
            df = self._check_dataframe_is_not_empty(df)
            success = df.select(F.min(F.col("is_not_empty"))).first()[0] == "true"
            metadata = self._build_metadata("is_not_empty", {}, criticality, "0", success)
            metadata_list.append(metadata)

            if not success:
                if self.save_log:
                    self._log(metadata_list)
                msg = "[Expectations/apply] DataFrame está vazio."
                formatted_print(msg, debug=self.debug)
                if criticality == "error":
                    raise CustomException("Empty DataFrame detected.")
                else:
                    return

        for rule in rules:
            quality = rule.get("quality")
            if not quality or quality == "is_not_empty":
                continue

            criticality = rule.get("criticality", "error")
            params = {k: v for k, v in rule.items() if k not in ("quality", "criticality")}

            formatted_print(f"[Expectations/apply] Aplicando regra '{quality}'", debug=self.debug, force_debug_only=True)

            method = getattr(self, f"_check_{quality}", None)
            if method is None:
                raise CustomException(f"Quality rule '{quality}' is not implemented.")

            if quality == "custom_sql":
                df, col_name = method(df, **params)
                rule["col_name"] = col_name
            else:
                df = method(df, **params)

            col_result = rule.get("col_name", quality)
            unexpected_count = df.filter(F.col(col_result) == "false").count()
            success = df.select(F.min(F.col(col_result))).first()[0] == "true"

            metadata = self._build_metadata(quality, params, criticality, unexpected_count, success)
            metadata_list.append(metadata)

            fail_metadata = F.when(
                F.col(col_result) == "false",
                F.struct(
                    F.lit(quality).alias("quality"),
                    F.lit(criticality).alias("criticality"),
                    F.map([F.lit(k) for k in params for _ in (0, 1)][::2], [F.lit(str(v)) for v in params.values()])
                        .alias("parameters")
                )
            )
            df = df.withColumn("_metadata", F.when(
                F.col(col_result) == "false",
                F.array_union(F.col("_metadata"), F.array(fail_metadata))
            ).otherwise(F.col("_metadata")))

        if self.save_log:
            self._log(metadata_list)

        for m in metadata_list:
            if m["success"] == "false" and m["criticality"] == "error":
                msg = f"[Expectations/apply] Falha crítica: {m['quality']} (params={m['parameters']})"
                error_messages.append(msg)
            elif m["success"] == "false" and m["criticality"] == "warn":
                formatted_print(f"[Expectations/apply] Aviso: falha na regra '{m['quality']}'", debug=self.debug)

        if self.return_df:
            if error_messages and self.raise_exception:
                formatted_print("\n".join(error_messages), debug=self.debug)
            return df

        if error_messages and self.raise_exception:
            raise CustomException("\n".join(error_messages))

        if self.quarantine:
            quarantine_df = df.filter(F.size("_metadata") > 0)
            if not quarantine_df.isEmpty():
                self._send_to_quarantine(quarantine_df)

        return None

    def _check_is_unique(self, df: DataFrame, columns: List[str], **kwargs) -> DataFrame:
        if not isinstance(columns, list):
            raise CustomException("[Expectations/_check_is_unique] 'columns' must be a list.")
        for colname in columns:
            if colname not in df.columns:
                raise CustomException(f"Column '{colname}' not found.")
        return df.withColumn("is_unique", (F.count("*").over(Window.partitionBy(*columns)) == 1).cast("string"))

    def _check_is_not_null(self, df: DataFrame, columns: List[str], **kwargs) -> DataFrame:
        if not isinstance(columns, list):
            raise CustomException("[Expectations/_check_is_not_null] 'columns' must be a list.")
        for c in columns:
            if c not in df.columns:
                raise CustomException(f"Column '{c}' not found.")
        condition = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in columns])
        return df.withColumn("is_not_null", (~condition).cast("string"))

    def _check_dataframe_is_not_empty(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.withColumn("is_not_empty", F.lit(not df.isEmpty()).cast("string"))

    def _check_custom_sql(self, df: DataFrame, expression: str, col_name: Optional[str] = None, **kwargs):
        if not expression:
            raise CustomException("[Expectations/_check_custom_sql] 'expression' must be a non-empty string.")
        if not col_name:
            self.custom_sql_counter += 1
            col_name = f"custom_sql_{self.custom_sql_counter}"
        try:
            df = df.withColumn(col_name, F.expr(expression).cast("string"))
            return df, col_name
        except Exception as e:
            raise CustomException(f"Invalid SQL expression: {e}")

    def _send_to_quarantine(self, quarantine_df: DataFrame):
        if not self.table_name:
            raise CustomException("[Expectations/_send_to_quarantine] 'table_name' is required.")
        try:
            table_sanitized = self.table_name.replace(".", "_").replace("-", "_")
            full_table_name = f"foundation.quarantine.{table_sanitized}"

            df_to_write = quarantine_df.withColumn("_quarantined_at", F.current_timestamp())
            df_to_write.write.mode("append").format("delta").saveAsTable(full_table_name)
            formatted_print(f"[Expectations/_send_to_quarantine] 🚨 Linhas enviadas para {full_table_name}", debug=self.debug)
        except Exception as e:
            raise CustomException(f"[Expectations/_send_to_quarantine] Failed to write quarantine: {e}")

    def _build_metadata(self, quality_name: str, parameters: Dict[str, Any], criticality: str, unexpected_values: Any, success: bool) -> Dict[str, Any]:
        return {
            "quality": quality_name,
            "unexpectedValues": str(unexpected_values),
            "success": str(success).lower(),
            "parameters": {str(k): str(v) for k, v in parameters.items()},
            "criticality": criticality,
        }

    def _log(self, metadata_list: list):
        if not isinstance(metadata_list, list):
            raise CustomException("[Expectations/_log] 'metadata_list' must be a list.")
        try:
            metadata_schema = T.StructType([
                T.StructField("quality", T.StringType()),
                T.StructField("unexpectedValues", T.StringType()),
                T.StructField("success", T.StringType()),
                T.StructField("parameters", T.MapType(T.StringType(), T.StringType())),
                T.StructField("criticality", T.StringType())
            ])

            log_schema = T.StructType([
                T.StructField("table", T.StringType()),
                T.StructField("metadata", T.ArrayType(metadata_schema)),
                T.StructField("environment", T.StringType()),
                T.StructField("_createdAt", T.TimestampType())
            ])

            data = [(self.table_name, metadata_list, self.env, None)]
            df_log = spark.createDataFrame(data, schema=log_schema)
            df_log = df_log.withColumn("_createdAt", F.current_timestamp())
            df_log.write.mode("append").format("delta").saveAsTable(self.table_log)

            formatted_print(f"[Expectations/_log] Log salvo em {self.table_log}", debug=self.debug)
        except Exception as e:
            raise CustomException(f"[Expectations/_log] Failed to log quality metadata: {e}")
