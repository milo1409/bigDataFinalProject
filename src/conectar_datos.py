from typing import Optional
from pyspark.sql import DataFrame, SparkSession


class ExtraerDatosProcesamiento:
    def __init__(self, spark: SparkSession, utils, config: dict):
        self.spark = spark
        self.Utils = utils
        self.config = config

    def generar_formato_parquet(self,df: DataFrame,mode: str = "overwrite") -> str:

        salida_relativa = self.config.get("data_procesada")
        if salida_relativa is None:
            raise ValueError("La clave 'data_procesada' no existe en el config.")

        salida_absoluta = self.Utils.resolve_path(
            salida_relativa,
            base_path=self.config.get("base_path")
        )

        df.write.mode(mode).parquet(salida_absoluta)

        return salida_absoluta