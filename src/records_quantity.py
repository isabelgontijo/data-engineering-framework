from pyspark.sql import functions as F
from src.custom_exception import CustomException
from src.notebook_context import notebook_context
from src.formatted_print import formatted_print


class RecordsQuantity:
    def __init__(
        self,
        table_name: str,
        save_log: bool = True,
        debug: bool = False,
        table_log: str = "foundation.observability.records_quantity"
    ):
        if not isinstance(save_log, bool):
            raise CustomException("[RecordsQuantity/__init__] 'save_log' must be a boolean.")
        if not isinstance(debug, bool):
            raise CustomException("[RecordsQuantity/__init__] 'debug' must be a boolean.")
        if not isinstance(table_name, str):
            raise CustomException("[RecordsQuantity/__init__] 'table_name' must be a string.")

        self.table_name = table_name
        self.save_log = save_log
        self.debug = debug
        self.table_log = table_log
        self.env = notebook_context().get("env_folder")

        formatted_print(
            f"""
            [RecordsQuantity/__init__] Classe iniciada!
            Parâmetros:
                - table_name: {self.table_name}
                - save_log: {self.save_log}
                - table_log: {self.table_log}
                - debug: {self.debug}
                - env: {self.env}
            """, 
            debug=self.debug, 
            force_debug_only=True
        )

    def save(self):
        if not self.save_log:
            formatted_print(f"[RecordsQuantity/save] Logging desativado para a tabela: {self.table_name}", debug=self.debug)
            return

        try:
            history_df = spark.sql(f"DESCRIBE HISTORY {self.table_name} LIMIT 1") \
                .select("timestamp", "operation", "version", "operationMetrics")
        except Exception as e:
            raise CustomException(f"[RecordsQuantity/save] Falha ao ler histórico da tabela {self.table_name}: {e}")

        op = history_df.select("operation").first()["operation"]
        if op not in [
            'WRITE', 'CREATE TABLE AS SELECT', 'REPLACE TABLE AS SELECT',
            'COPY INTO', 'CREATE OR REPLACE TABLE AS SELECT', 'MERGE'
        ]:
            raise CustomException(f"[RecordsQuantity/save] Tipo de operação não suportada: {op}")

        metrics = history_df.select("operationMetrics").first()["operationMetrics"]
        num_output_rows = int(metrics.get("numOutputRows", 0))

        data = {
            "table": self.table_name,
            "environment": self.env,
            "num_output_rows": num_output_rows,
            "_createdAt": None  # será adicionado como current_timestamp
        }

        df_log = spark.createDataFrame([data]) \
            .withColumn("_createdAt", F.current_timestamp())

        try:
            df_log.write.mode("append").format("delta").saveAsTable(self.table_log)
            formatted_print(f"[RecordsQuantity/save] Log salvo em: {self.table_log}", debug=self.debug)
        except Exception as e:
            raise CustomException(f"[RecordsQuantity/save] Falha ao escrever log em {self.table_log}: {e}")
