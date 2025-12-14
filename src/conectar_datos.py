import os
from typing import Dict, List
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class ExtraerDatosProcesamiento:
    def __init__(self, spark, utils, config: dict):
        self.spark = spark
        self.Utils = utils
        self.config = config

    def _write_partitioned_parquet(
        self,
        df: DataFrame,
        partition_cols: List[str],
        subdir: str,
        mode: str = "overwrite"
    ) -> str:
        rel_base = self.config.get("data_procesada")
        if rel_base is None:
            raise ValueError("Falta la clave 'data_procesada' en config")

        rel_path = os.path.join(rel_base, subdir)
        salida = self.Utils.resolve_path(rel_path, base_path=self.config.get("base_path"))

        (
            df.write
            .mode(mode)
            .partitionBy(*partition_cols)
            .parquet(salida)
        )

        return salida

    def generar_parquets_dashboard_spark(
        self,
        df,                     
        mode: str = "overwrite"
    ) -> Dict[str, str]:

        if isinstance(df, pd.DataFrame):
            df_s = self.spark.createDataFrame(df)
        elif isinstance(df, DataFrame):
            df_s = df
        else:
            raise TypeError("df debe ser un pandas.DataFrame o un pyspark.sql.DataFrame")

        # 1. Tendencia diaria: FECHA → TOTAL
        df_diario = (
            df_s.groupBy("FECHA")
                .agg(F.count(F.lit(1)).alias("TOTAL"))
        )

        # 2. Heatmap: DIA_SEMANA x HORA → TOTAL
        df_hm = (
            df_s.groupBy("DIA_SEMANA", "HORA")
                .agg(F.count(F.lit(1)).alias("TOTAL"))
        )

        # 3. Localidades: LOCALIDAD → TOTAL
        df_loc = (
            df_s.groupBy("LOCALIDAD")
                .agg(F.count(F.lit(1)).alias("TOTAL"))
        )

        # 4. Tipos de incidente: TIPO_INCIDENTE → TOTAL
        df_tipo = (
            df_s.groupBy("TIPO_INCIDENTE")
                .agg(F.count(F.lit(1)).alias("TOTAL"))
        )

        # 5. Sunburst: PRIORIDAD_FINAL x TIPO_INCIDENTE → TOTAL
        df_sb = (
            df_s.groupBy("PRIORIDAD_FINAL", "TIPO_INCIDENTE")
                .agg(F.count(F.lit(1)).alias("TOTAL"))
        )

        # 🔹 3) Escribir en parquet particionado
        rutas: Dict[str, str] = {}

        rutas["diario"] = self._write_partitioned_parquet(
            df=df_diario,
            partition_cols=["FECHA"],
            subdir="agg_diario",
            mode=mode,
        )

        rutas["heatmap"] = self._write_partitioned_parquet(
            df=df_hm,
            partition_cols=["DIA_SEMANA", "HORA"],
            subdir="agg_heatmap",
            mode=mode,
        )

        rutas["localidad"] = self._write_partitioned_parquet(
            df=df_loc,
            partition_cols=["LOCALIDAD"],
            subdir="agg_localidad",
            mode=mode,
        )

        rutas["tipo_incidente"] = self._write_partitioned_parquet(
            df=df_tipo,
            partition_cols=["TIPO_INCIDENTE"],
            subdir="agg_tipo_incidente",
            mode=mode,
        )

        rutas["sunburst"] = self._write_partitioned_parquet(
            df=df_sb,
            partition_cols=["PRIORIDAD_FINAL", "TIPO_INCIDENTE"],
            subdir="agg_prioridad_tipo",
            mode=mode,
        )

        return rutas
