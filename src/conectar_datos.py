import os
from typing import Optional
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame


class ExtraerDatosProcesamiento:
    def __init__(self, spark, utils, config: dict):
        self.spark = spark
        self.Utils = utils
        self.config = config

    def generar_formato_parquet(self, df, mode: str = "overwrite") -> str:
   
        if isinstance(df, pd.DataFrame):
            df_spark = self.spark.createDataFrame(df)
        elif isinstance(df, SparkDataFrame):
            df_spark = df
        else:
            raise TypeError("df debe ser un pandas.DataFrame o un pyspark.sql.DataFrame")

        rel_path = self.config.get("data_procesada")
        if rel_path is None:
            raise ValueError("Falta la clave 'data_procesada' en config")

        salida = self.Utils.resolve_path(rel_path, base_path=self.config.get("base_path"))

        df_spark.write.mode(mode).parquet(salida)

        return salida