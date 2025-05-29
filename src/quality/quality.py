from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from functools import reduce
from src.quality.exception import QualityException
from src.quality.logger import logger

class QualityX:

    def __init__(self, 
            df, 
            save_log=True, 
            table_name=None, 
            table_log: str = "metadata.quality.expectations_logs", 
            raise_exception=True, 
            use_quarantine=False, 
            return_df=False
        ):

        if not isinstance(save_log, bool):
            raise QualityException("[QUALITY/__init__] 'save_log' deve ser um booleano.")
        if table_name is not None and not isinstance(table_name, str):
            raise QualityException("[QUALITY/__init__] 'table_name' deve ser uma string ou None.")
        if not isinstance(raise_exception, bool):
            raise QualityException("[QUALITY/__init__] 'raise_exception' deve ser um booleano.")

        self.df = df
        self.save_log = save_log
        self.table_name = table_name
        self.table_log = table_log
        self.raise_exception = raise_exception
        self.use_quarantine = use_quarantine
        self.return_df = return_df
        
        self.custom_sql_counter = 0

        if self.save_log:
            self.logger = QualityLogger(self.table_name, self.table_log)
        else:
            self.logger = None
        
    def apply(self, qualities):

        if not isinstance(qualities, list):
            raise QualityException("[QUALITY/apply] ⚠️ 'qualities' should be a list.")

        df = self.df
        qualities_metadata = []
        error_messages = []

        # Lida com is_not_empty separadamente (antes do loop).  
        # Isso é feito para poder retornar imediatamente se o DataFrame estiver vazio.
        is_not_empty_check = next((q for q in qualities if q.get("quality") == "is_not_empty"), None)
        if is_not_empty_check:
            criticality = is_not_empty_check.get("criticality", "error")  # "error" como padrão
            if criticality not in ("error", "warn"):
                raise QualityException(
                    f"[QUALITY/apply] ⚠️ Criticidade inválida: '{criticality}' para quality 'is_not_empty'. Deve ser 'error' ou 'warn'."
                )

            df = self._check_dataframe_is_not_empty(df)
            try:
                success = df.select(F.min(F.col("is_not_empty"))).first()[0] == "true"
            except Exception as e:
                raise QualityException("[QUALITY/apply_quality] Erro ao verificar se DataFrame está vazio.") from e

            unexpected_values = "0"
            metadata = self._build_metadata(
                "is_not_empty", {}, criticality, unexpected_values, success
            )
            qualities_metadata.append(metadata)

            if not success and criticality == "error":
                if self.save_log and self.logger:
                    self.logger.log_quality(qualities_metadata)  # Salva o log *antes* de lançar a exceção
                raise QualityException("[QUALITY/apply_quality] DataFrame vazio (crítico).")
            elif not success and criticality == "warn":
                print(f"[QUALITY/apply_quality] DataFrame vazio (warn).")
                if self.save_log and self.logger:
                    self.logger.log_quality(qualities_metadata)
                return 

        # Loop principal para outras verificações
        for quality in qualities:
            quality_name = quality.get("quality")
            if not quality_name:
                raise QualityException("[QUALITY/apply_quality] Cada quality deve ter a chave 'quality' definida.")
            if quality_name == "is_not_empty":  # Já tratamos 'is_not_empty' acima
                continue

            criticality = quality.get("criticality", "error")  # "error" como padrão
            if criticality not in ("error", "warn"):
                raise QualityException(
                    f"[QUALITY/apply_quality] Criticidade inválida: '{criticality}' para quality '{quality_name}'. Deve ser 'error' ou 'warn'."
                )

            # Separa os parâmetros da verificação (remove 'quality' e 'criticality')
            parameters = {k: v for k, v in quality.items() if k not in ("quality", "criticality")}

            # Obtém a função de verificação correspondente (e.g., _check_is_unique)
            method_name = f"_check_{quality_name}"
            method = getattr(self, method_name, None)  # Usa getattr para obter o método dinamicamente
            if method is None:
                raise QualityException(f"[QUALITY/apply_quality] Verificação '{quality_name}' não implementada.")

            # Tratamento especial para custom_sql
            if quality_name == "custom_sql":
                result = method(df, **parameters)
                if isinstance(result, tuple):
                    df, col_name_used = result
                    quality["col_name"] = col_name_used  # Armazena o nome da coluna gerada
                else:
                    df = result
                    quality["col_name"] = quality_name
            else:
                df = method(df, **parameters)

            # Calcula os metadados *após* a aplicação da verificação
            check_column = quality.get("col_name", quality_name)
            unexpected_count = df.filter(F.col(check_column) == "false").count()  # Conta as falhas
            success = df.select(F.min(F.col(check_column))).first()[0] == "true"  # Verifica se houve alguma falha

            metadata = self._build_metadata(quality_name, parameters, criticality, unexpected_count, success)
            qualities_metadata.append(metadata)

        # Salva os logs (se habilitado)
        if self.save_log and self.logger:
            self.logger.log_quality(qualities_metadata)  # Passa a lista de metadados para o logger
        
        # Coletar todas as mensagens de erro e aviso
        for metadata in qualities_metadata:
            if metadata["quality"] != "is_not_empty" and metadata["success"] == "false":
                if metadata["criticality"] == "error":
                    error_message = f"[QUALITY/apply_quality] Falha crítica em verificação: {metadata['quality']}"
                    if "parameters" in metadata and metadata["parameters"]:
                        param_str = ", ".join([f"{k}={v}" for k, v in metadata["parameters"].items()])
                        error_message += f" -- parâmetros: {param_str}"
                    error_messages.append(error_message)
                elif metadata["criticality"] == "warn":
                    print(f"[QUALITY/apply_quality] Aviso! Quality ocorreu sem sucesso, há valores inesperados - {metadata['quality']}")
        
        error_summary = "\n".join(error_messages)
        print_error = f"[QUALITY/apply_quality] Ocorreram as seguintes falhas críticas:\n{error_summary}"

        # Retorno do DataFrame se solicitado
        if self.return_df:
            # Trata erros sem interromper o fluxo
            if error_messages:
                if self.raise_exception:
                    # Mistura de flags conflitantes: return_df tem prioridade
                    print(f"AVISO: Conflito de flags - return_df=True prevalece sobre raise_exception=True\n{print_error}")
                else:
                    print(print_error)
            
            return df

        # Caso onde return_df=False
        else:
            if error_messages:
                if self.raise_exception:
                    raise QualityException(print_error)  # Interrompe execução normalmente
                else:
                    print(print_error)
        
        return None

    def _check_is_unique(self, df, columns, **kwargs):

        if not isinstance(columns, list):
            raise QualityException("[QUALITY/_check_is_unique] 'columns' deve ser uma lista.")

        _new_unique_columns = []
        for col_name in columns:
            if not col_name in df.columns:
                raise QualityException(f"[QUALITY/_check_is_unique] A coluna '{col_name}' não existe no DataFrame.")
            col_type = df.schema[col_name].dataType
            # Converte colunas complexas (array, map, struct) para JSON para permitir a comparação
            if isinstance(col_type, (T.ArrayType, T.MapType, T.StructType)):
                _new_unique_columns.append(F.to_json(col_name).alias(col_name))
            else:
                _new_unique_columns.append(col_name)

        # Usa Window function para contar as ocorrências de cada combinação de valores
        window_spec = Window.partitionBy(_new_unique_columns)
        df = df.withColumn("is_unique", (F.count("*").over(window_spec) == 1).cast("string"))
        
        return df
    
    def _check_is_not_null(self, df, columns, **kwargs):

        if not isinstance(columns, list):
            raise QualityException("[QUALITY/_check_is_not_null] 'columns' deve ser uma lista.")

        for col_name in columns:
            if not col_name in df.columns:
                raise QualityException(f"[QUALITY/is_not_null] A coluna '{col_name}' não existe no DataFrame.")

        # Cria uma condição que verifica se *qualquer uma* das colunas é nula
        condition = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in columns])
        df = df.withColumn("is_not_null", (~condition).cast("string"))  # Inverte a condição (NOT NULL)
        return df

    def _check_dataframe_is_not_empty(self, df, **kwargs):

        return df.withColumn("is_not_empty", F.lit(not df.isEmpty()).cast("string"))
    
    def _check_custom_sql(self, df, expression, col_name=None, **kwargs):

        if not isinstance(expression, str):
            raise QualityException("[QUALITY/_check_custom_sql] 'expression' deve ser uma string.")

        if col_name is None:
            self.custom_sql_counter += 1
            col_name = f"custom_sql_{self.custom_sql_counter}"
        
        try:
            # Adiciona a coluna custom_sql com o resultado da expressão SQL
            df = df.withColumn(col_name, F.expr(expression).cast("string"))
            return df, col_name
        except Exception as e:
            raise QualityException(f"[QUALITY/_check_custom_sql] Expressão SQL inválida: {e}")

    def _build_metadata(self, quality_name, parameters, criticality, unexpected_values, success):
        return {
            "quality": quality_name, 
            "unexpected_values": str(unexpected_values),
            "success": str(success).lower(),
            "parameters": {str(k): str(v) for k, v in parameters.items()},
            "criticality": criticality, 
        }

