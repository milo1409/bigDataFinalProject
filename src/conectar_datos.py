from typing import Optional, List
from functools import reduce
from pyspark.sql import DataFrame, SparkSession


class ExtraerDatosProcesamiento:
    def __init__(self, spark: SparkSession, utils, config: dict):
        self.spark = spark
        self.Utils = utils
        self.config = config

    def conectar_datos(self):
        pass

    def generar_formato_parquet(self, df: DataFrame, salida_parquet: str, mode: str = "overwrite"):
        salida = self.Utils.resolve_path(salida_parquet, base_path=self.config.get("base_path"))
        df.write.mode(mode).parquet(salida)
        self.Utils.add_message(f"Parquet generado en: {salida}")

    def leer_csv_flexible_spark(
        self,
        ruta_csv: str,
        *,
        sep: str = ";",
        encodings: List[str] = ("utf-8", "latin1", "cp1252"),
        header: bool = True,
        infer_schema: bool = True,
        mode: str = "PERMISSIVE",
        **options
    ) -> Optional[DataFrame]:
        last_err = None
        for enc in encodings:
            try:
                df = (
                    self.spark.read.format("csv")
                    .option("header", str(header).lower())
                    .option("inferSchema", str(infer_schema).lower())
                    .option("sep", sep)
                    .option("encoding", enc)
                    .option("mode", mode)
                    .options(**options)
                    .load(ruta_csv)
                )
                # acción mínima para fallar rápido
                df.limit(1).count()
                return df
            except Exception as e:
                last_err = e

        self.Utils.add_message(f"No se pudo leer {ruta_csv}. Último error: {last_err}")
        return None

    def listar_csvs_desde_config(self, key_folder: str = "data_cruda", recursive: bool = True) -> List[str]:
        base = self.config.get("base_path")
        folder_cfg = self.config.get(key_folder)
        
        if not folder_cfg:
            raise KeyError(f"No existe la llave '{key_folder}' en el JSON o está vacía.")

        folder = self.Utils.resolve_path(folder_cfg, base_path=base)
        rutas = self.Utils.list_files(folder, pattern="*.csv", recursive=recursive)

        if not rutas:
            raise FileNotFoundError(f"No se encontraron CSV en: {folder}")

        self.Utils.add_message(f"CSV encontrados: {len(rutas)} en {folder}")
        return rutas

    def leer_todos_csv_desde_config(
        self,
        key_folder: str = "data_cruda",
        *,
        recursive: bool = True,
        sep: str = ";",
        encodings: List[str] = ("utf-8", "latin1", "cp1252"),
        header: bool = True,
        infer_schema: bool = True,
        allow_missing_cols: bool = True
    ) -> DataFrame:
        rutas = self.listar_csvs_desde_config(key_folder=key_folder, recursive=recursive)

        dfs = []
        for r in rutas:
            df = self.leer_csv_flexible_spark(
                r,
                sep=sep,
                encodings=list(encodings),
                header=header,
                infer_schema=infer_schema,
            )
            if df is not None:
                dfs.append(df)

        if not dfs:
            raise RuntimeError("No se pudo leer ningún CSV (todos fallaron).")

        if len(dfs) == 1:
            return dfs[0]

        return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=allow_missing_cols), dfs)