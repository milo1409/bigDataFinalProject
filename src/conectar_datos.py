import os
from typing import Dict, List, Union
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class ExtraerDatosProcesamiento:
    def __init__(self, spark: SparkSession, utils, config: dict):
        self.spark = spark
        self.Utils = utils
        self.config = config

    def _base_procesada(self) -> str:
        rel_base = self.config.get("data_procesada")
        if rel_base is None:
            raise ValueError("Falta la clave 'data_procesada' en config")
        return self.Utils.resolve_path(rel_base, base_path=self.config.get("base_path"))

    def _write_parquet(
        self,
        df: DataFrame,
        subdir: str,
        mode: str = "overwrite",
    ) -> str:
        salida = os.path.join(self._base_procesada(), subdir)
        df.write.mode(mode).parquet(salida)
        return salida

    def _write_partitioned_parquet(
        self,
        df: DataFrame,
        partition_cols: List[str],
        subdir: str,
        mode: str = "overwrite",
    ) -> str:
        salida = os.path.join(self._base_procesada(), subdir)
        (
            df.write
              .mode(mode)
              .partitionBy(*partition_cols)
              .parquet(salida)
        )
        return salida

    def generar_parquets_dashboard_spark(
        self,
        df: Union[pd.DataFrame, DataFrame],
        mode: str = "overwrite",
        guardar_general: bool = True,
        subdir_general: str = "incidentes",          
        subdir_dashboard: str = "dashboard",         
    ) -> Dict[str, str]:
  
        # 1) Asegurar Spark DF
        if isinstance(df, pd.DataFrame):
            df_s = self.spark.createDataFrame(df)
        elif isinstance(df, DataFrame):
            df_s = df
        else:
            raise TypeError("df debe ser un pandas.DataFrame o un pyspark.sql.DataFrame")

        rutas: Dict[str, str] = {}

        # 2) Guardar parquet general (SIN partición) en subcarpeta dedicada
        if guardar_general:
            rutas["general"] = self._write_parquet(
                df=df_s,
                subdir=subdir_general,
                mode=mode
            )

        # 3) Agregados en Spark
        df_diario = df_s.groupBy("FECHA").agg(F.count(F.lit(1)).alias("TOTAL"))

        df_hm = (
            df_s.groupBy("DIA_SEMANA", "HORA")
                .agg(F.count(F.lit(1)).alias("TOTAL"))
        )

        df_loc = df_s.groupBy("LOCALIDAD").agg(F.count(F.lit(1)).alias("TOTAL"))

        df_tipo = df_s.groupBy("TIPO_INCIDENTE").agg(F.count(F.lit(1)).alias("TOTAL"))

        df_sb = (
            df_s.groupBy("PRIORIDAD_FINAL", "TIPO_INCIDENTE")
                .agg(F.count(F.lit(1)).alias("TOTAL"))
        )

        # 4) Guardar agregados particionados dentro de data_procesada/dashboard/*
        rutas["diario"] = self._write_partitioned_parquet(
            df=df_diario,
            partition_cols=["FECHA"],
            subdir=os.path.join(subdir_dashboard, "agg_diario"),
            mode=mode,
        )

        rutas["heatmap"] = self._write_partitioned_parquet(
            df=df_hm,
            partition_cols=["DIA_SEMANA", "HORA"],
            subdir=os.path.join(subdir_dashboard, "agg_heatmap"),
            mode=mode,
        )

        rutas["localidad"] = self._write_partitioned_parquet(
            df=df_loc,
            partition_cols=["LOCALIDAD"],
            subdir=os.path.join(subdir_dashboard, "agg_localidad"),
            mode=mode,
        )

        rutas["tipo_incidente"] = self._write_partitioned_parquet(
            df=df_tipo,
            partition_cols=["TIPO_INCIDENTE"],
            subdir=os.path.join(subdir_dashboard, "agg_tipo_incidente"),
            mode=mode,
        )

        rutas["sunburst"] = self._write_partitioned_parquet(
            df=df_sb,
            partition_cols=["PRIORIDAD_FINAL", "TIPO_INCIDENTE"],
            subdir=os.path.join(subdir_dashboard, "agg_prioridad_tipo"),
            mode=mode,
        )

        return rutas