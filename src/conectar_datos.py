from typing import Optional, List
from functools import reduce
from pyspark.sql import DataFrame, SparkSession


class ExtraerDatosProcesamiento:
    def __init__(self, spark: SparkSession, utils, config: dict):
        self.spark = spark
        self.Utils = utils
        self.config = config

    def generar_formato_parquet(self, df: DataFrame, config: str, mode: str = "overwrite"):
        salida_parquet = salida_parquet['data_procesada']
        salida = self.Utils.resolve_path(salida_parquet, base_path=self.config.get("base_path"))
        ruta_salida_parquet = df.write.mode(mode).parquet(salida)
        return ruta_salida_parquet