from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.quality.exception import QualityException

def logger(self, metadata: list):
    """
    Logs data quality validation results to a Delta table for observability.

    Args:
        metadata (str): Metadata list to log. It should be in the structure: [quality, unexpected_values, success, parameters, criticality]
    """
    if not isinstance(metadata_list, list):
            raise QualityException("[QUALITY/logger] ⚠️ 'metadata' should be a list.")

    try:
        metadata_struct_schema = T.StructType([
            T.StructField("quality", T.StringType()),
            T.StructField("unexpected_values", T.StringType()),
            T.StructField("success", T.StringType()),
            T.StructField("parameters", T.MapType(T.StringType(), T.StringType())),
            T.StructField("criticality", T.StringType())
        ])

        log_schema = T.StructType([
            T.StructField("table", T.StringType()),
            T.StructField("metadata", T.ArrayType(metadata_struct_schema)),
            T.StructField("environment", T.StringType()),
            T.StructField("_created_at", T.TimestampType())
        ])

        data = [(self.table_name, metadata, self.env, None)]
        df_log = spark.createDataFrame(data=data, schema=log_schema)
        df_log = df_log.withColumn("_created_at", F.current_timestamp())

        df_log.write.mode("append").format("delta").saveAsTable(self.table_log)
        print(f"[QUALITY/logger] ☑️ Log saved to table: {self.table_log}")

    except Exception as e:
        raise QualityException(f"[QUALITY/logger] ⚠️ Error while saving log: {e}")
