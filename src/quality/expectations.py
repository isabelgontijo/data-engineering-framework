from pyspark.sql import Window, DataFrame
from pyspark.sql import functions as F, types as T
from functools import reduce
from typing import List, Dict, Optional, Any
from src.quality.exception import QualityException
from src.notebook_context import get_notebook_context_info


class QualityExpectations:
    def __init__(
        self,
        df: DataFrame,
        save_log: bool = True,
        table_name: Optional[str] = None,
        table_log: str = "foundation.quality.expectations_logs",
        raise_exception: bool = True,
        quarantine: bool = False,
        return_df: bool = False,
    ):
        if not isinstance(save_log, bool):
            raise QualityException("[__init__] 'save_log' must be a boolean.")
        if table_name is not None and not isinstance(table_name, str):
            raise QualityException("[__init__] 'table_name' must be a string or None.")
        if not isinstance(raise_exception, bool):
            raise QualityException("[__init__] 'raise_exception' must be a boolean.")

        self.df = df.withColumn("_metadata", F.array())
        self.save_log = save_log
        self.table_name = table_name
        self.table_log = table_log
        self.raise_exception = raise_exception
        self.quarantine = quarantine
        self.return_df = return_df
        self.custom_sql_counter = 0
        self.env = get_notebook_context_info().get("env_folder")

    def apply(self, rules: List[Dict[str, Any]]):
        if not isinstance(rules, list):
            raise QualityException("[apply] 'rules' must be a list.")

        df = self.df
        metadata_list = []
        error_messages = []

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
                if criticality == "error":
                    raise QualityException("❌ Empty DataFrame detected.")
                else:
                    print("[Expectations] ⚠️ Warning: Empty DataFrame.")
                    return

        for rule in rules:
            quality = rule.get("quality")
            if not quality or quality == "is_not_empty":
                continue

            criticality = rule.get("criticality", "error")
            params = {k: v for k, v in rule.items() if k not in ("quality", "criticality")}

            method = getattr(self, f"_check_{quality}", None)
            if method is None:
                raise QualityException(f"Quality rule '{quality}' is not implemented.")

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

            # enrich metadata on failed rows
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
                msg = f"[Expectations] ❌ Critical failure: {m['quality']} (params={m['parameters']})"
                error_messages.append(msg)
            elif m["success"] == "false" and m["criticality"] == "warn":
                print(f"[Expectations] ⚠️ Warning: failed check {m['quality']}")

        if self.return_df:
            if error_messages and self.raise_exception:
                print("\n".join(error_messages))
            return df

        if error_messages and self.raise_exception:
            raise QualityException("\n".join(error_messages))

        if self.quarantine:
            quarantine_df = df.filter(F.size("_metadata") > 0)
            if not quarantine_df.isEmpty():
                self._send_to_quarantine(quarantine_df)

        return None

    def _check_is_unique(self, df: DataFrame, columns: List[str], **kwargs) -> DataFrame:
        if not isinstance(columns, list):
            raise QualityException("[_check_is_unique] 'columns' must be a list.")
        for colname in columns:
            if colname not in df.columns:
                raise QualityException(f"Column '{colname}' not found.")
        window_spec = Window.partitionBy(*columns)
        return df.withColumn("is_unique", (F.count("*").over(window_spec) == 1).cast("string"))

    def _check_is_not_null(self, df: DataFrame, columns: List[str], **kwargs) -> DataFrame:
        if not isinstance(columns, list):
            raise QualityException("[_check_is_not_null] 'columns' must be a list.")
        for c in columns:
            if c not in df.columns:
                raise QualityException(f"Column '{c}' not found.")
        condition = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in columns])
        return df.withColumn("is_not_null", (~condition).cast("string"))

    def _check_dataframe_is_not_empty(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.withColumn("is_not_empty", F.lit(not df.isEmpty()).cast("string"))

    def _check_custom_sql(self, df: DataFrame, expression: str, col_name: Optional[str] = None, **kwargs):
        if not expression:
            raise QualityException("[_check_custom_sql] 'expression' must be a non-empty string.")
        if not col_name:
            self.custom_sql_counter += 1
            col_name = f"custom_sql_{self.custom_sql_counter}"
        try:
            df = df.withColumn(col_name, F.expr(expression).cast("string"))
            return df, col_name
        except Exception as e:
            raise QualityException(f"Invalid SQL expression: {e}")

    def _send_to_quarantine(self, quarantine_df: DataFrame):
        if not self.table_name:
            raise QualityException("[_send_to_quarantine] 'table_name' is required.")

        try:
            table_sanitized = self.table_name.replace(".", "_").replace("-", "_")
            full_table_name = f"foundation.quarantine.{table_sanitized}"

            df_to_write = quarantine_df.withColumn("_quarantined_at", F.current_timestamp())
            df_to_write.write.mode("append").format("delta").saveAsTable(full_table_name)
            print(f"[_send_to_quarantine] 🚨 Rows sent to quarantine: {full_table_name}")
        except Exception as e:
            raise QualityException(f"[_send_to_quarantine] Failed to write quarantine: {e}")

    def _build_metadata(self, quality_name: str, parameters: Dict[str, Any], criticality: str, unexpected_values: Any, success: bool) -> Dict[str, Any]:
        return {
            "quality": quality_name,
            "unexpected_values": str(unexpected_values),
            "success": str(success).lower(),
            "parameters": {str(k): str(v) for k, v in parameters.items()},
            "criticality": criticality,
        }

    def _log(self, metadata_list: list):
        if not isinstance(metadata_list, list):
            raise QualityException("[_log] 'metadata_list' must be a list.")

        try:
            metadata_schema = T.StructType([
                T.StructField("quality", T.StringType()),
                T.StructField("unexpected_values", T.StringType()),
                T.StructField("success", T.StringType()),
                T.StructField("parameters", T.MapType(T.StringType(), T.StringType())),
                T.StructField("criticality", T.StringType())
            ])

            log_schema = T.StructType([
                T.StructField("table", T.StringType()),
                T.StructField("metadata", T.ArrayType(metadata_schema)),
                T.StructField("environment", T.StringType()),
                T.StructField("_created_at", T.TimestampType())
            ])

            data = [(self.table_name, metadata_list, self.env, None)]
            df_log = spark.createDataFrame(data, schema=log_schema)
            df_log = df_log.withColumn("_created_at", F.current_timestamp())

            df_log.write.mode("append").format("delta").saveAsTable(self.table_log)
            print(f"[_log] ✅ Log saved to: {self.table_log}")

        except Exception as e:
            raise QualityException(f"[_log] Failed to log quality metadata: {e}")
