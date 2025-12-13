

class procesamiento_datos_spark():

    def consolidar_csv_en_carpeta(
        carpeta_datos_crudos: str,
        carpeta_salida: str,
        nombre_salida_base: str = "llamadas_123_consolidado",
        columnas_esperadas=None,
    ):
        patrones = ["*.csv", "*.CSV"]
        archivos = []
        for pat in patrones:
            archivos.extend(glob.glob(os.path.join(carpeta_datos_crudos, pat)))

        if not archivos:
            logger.warning(f"No se encontraron CSV en {carpeta_datos_crudos} para consolidar.")
            return None, None

        logger.info(f"Consolidando {len(archivos)} archivos CSV de {carpeta_datos_crudos}...")

        dfs = []
        for ruta in sorted(archivos):
            logger.info(f"Leyendo {ruta} ...")
            df = leer_csv_flexible(ruta)
            if df is None:
                logger.warning(f"  >> Se omitió {ruta} por error de lectura.")
                continue
            dfs.append(df)

        if not dfs:
            logger.error("No se pudo leer ningún CSV para consolidar.")
            return None, None

        df_consolidado = pd.concat(dfs, ignore_index=True, sort=False)
        df_consolidado = unificar_columnas_duplicadas(df_consolidado)
        df_consolidado = reparar_dataframe_pandas(df_consolidado)
        df_errores = validar_consistencia_localidad_pandas(df_consolidado,catalogo_localidades)
        df_consolidado = corregir_localidad_desde_errores_pandas(df_consolidado, df_errores)
        df_consolidado = df_consolidado.apply(
            lambda col: col.astype(str).str.upper() if col.dtype == "object" else col
        )
        if columnas_esperadas:

            columnas_esperadas = list(columnas_esperadas)

            cols_actuales = list(df_consolidado.columns)

            cols_presentes = [c for c in columnas_esperadas if c in cols_actuales]

            cols_a_eliminar = [c for c in cols_actuales if c not in columnas_esperadas]
            if cols_a_eliminar:
                logger.info(
                    f"Columnas eliminadas por no estar en columnas_esperadas: {cols_a_eliminar}"
                )
                df_consolidado = df_consolidado.drop(columns=cols_a_eliminar)

            df_consolidado = df_consolidado[cols_presentes]

        logger.info(df_consolidado.columns)

        logger.info(
            f"Dataset consolidado: {df_consolidado.shape[0]:,} filas x {df_consolidado.shape[1]} columnas"
        )

        os.makedirs(carpeta_salida, exist_ok=True)

        # --- CSV ---
        ruta_csv_out = os.path.join(carpeta_salida, f"{nombre_salida_base}.csv")
        t0 = time.time()
        df_consolidado.to_csv(ruta_csv_out, index=False, encoding="utf-8", sep=";")
        t_csv = time.time() - t0
        logger.info(f"CSV consolidado guardado en: {ruta_csv_out} (t={t_csv:.2f}s)")

        # --- JSON ---
        ruta_json_out = os.path.join(carpeta_salida, f"{nombre_salida_base}.json")
        t0 = time.time()
        df_consolidado.to_json(
            ruta_json_out,
            orient="records",
            lines=True,
            force_ascii=False
        )
        t_json = time.time() - t0
        logger.info(f"JSON guardado en: {ruta_json_out} (t={t_json:.2f}s)")

        # --- Parquet ---
        ruta_parquet_out = os.path.join(carpeta_salida, f"{nombre_salida_base}.parquet")
        try:
            t0 = time.time()
            df_consolidado.to_parquet(ruta_parquet_out, index=False)
            t_parquet = time.time() - t0
            logger.info(f"Parquet consolidado guardado en: {ruta_parquet_out} (t={t_parquet:.2f}s)")
        except Exception as e:
            logger.error(f"No se pudo guardar Parquet: {e}", exc_info=True)
            t_parquet = None

        # --- Delta Lake ---
        try:
            spark = SparkSession.builder.getOrCreate()
        except Exception as e:
            logger.error(f"No hay SparkSession activo para Delta: {e}", exc_info=True)
            spark = None

        ruta_delta_out = None
        if spark is not None:
            try:
                spark_df = spark.createDataFrame(df_consolidado)
                ruta_delta_out = os.path.join(carpeta_salida, f"{nombre_salida_base}_delta")

                t0 = time.time()
                (spark_df.write
                    .format("delta")
                    .mode("overwrite")
                    .save(ruta_delta_out))
                t_delta = time.time() - t0
                logger.info(f"Delta Lake guardado en carpeta: {ruta_delta_out} (t={t_delta:.2f}s)")
            except Exception as e:
                logger.error(f"No se pudo escribir en formato Delta Lake: {e}", exc_info=True)
                ruta_delta_out = None

        return df_consolidado, {
            "csv": ruta_csv_out,
            "json": ruta_json_out,
            "parquet": ruta_parquet_out,
            "delta": ruta_delta_out,
        }
    



# Funcion recursiva para abrir cada CSV y convertirlo en un dataframe
def leer_csv_flexible(ruta_csv: str) -> pd.DataFrame | None:
  try:
    return pd.read_csv(ruta_csv, sep=";", encoding="utf-8", low_memory=False)
  except UnicodeDecodeError:
    try:
        return pd.read_csv(ruta_csv, sep=";", encoding="latin-1", low_memory=False)
    except Exception as e:
        print(f"No se pudo leer el CSV {ruta_csv}: {e}")
        return None
# Funcion para obtener la ruta y tamaño del directorio consultado
def get_dir_size(path: str) -> int:
    total = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            total += os.path.getsize(fp)
    return total

# Funciones especializadas para el reemplazo de caracteres especiales
def reparar_texto_pandas(s):
    if pd.isna(s):
        return s
    if not isinstance(s, str):
        s = str(s)
    return patron.sub(lambda m: REEMPLAZOS_COMUNES[m.group(0)], s)


def reparar_dataframe_pandas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    cols_texto = df.select_dtypes(include=["object", "string"]).columns

    for col_name in cols_texto:
        df[col_name] = df[col_name].apply(reparar_texto_pandas)

    return df

# Funcion para la unificacion de columnas repetidas
def unificar_columnas_duplicadas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    for canon, variantes in DUPLICATE_GROUPS.items():
        presentes = [c for c in variantes if c in df.columns]
        if not presentes:
            continue
        base = presentes[0]

        if base != canon:
            df[canon] = df[base]
        else:
            canon = base
        for other in presentes[1:]:
            mask = df[canon].isna() & df[other].notna()
            if mask.any():
                df.loc[mask, canon] = df.loc[mask, other]

        drop_cols = [c for c in presentes if c != canon]
        if drop_cols:
            df = df.drop(columns=drop_cols)

    return df

# Funciones para ajustar los valores asociados a las localidades
def validar_consistencia_localidad_pandas(df: pd.DataFrame, catalogo_localidades: Dict[str, str], col_codigo: str = "CODIGO_LOCALIDAD",col_localidad: str = "LOCALIDAD") -> pd.DataFrame:
  df = df.copy()
  def localidad_catalogo(codigo):
      if pd.isna(codigo):
          return np.nan
      return catalogo_localidades.get(codigo) or catalogo_localidades.get(str(codigo))

  df["LOCALIDAD_CATALOGO"] = df[col_codigo].apply(localidad_catalogo)

  mask = (
      df[col_localidad].notna()
      & df["LOCALIDAD_CATALOGO"].notna()
      & (df[col_localidad] != df["LOCALIDAD_CATALOGO"])
  )

  df_inconsistencias = df.loc[mask, [
      "NUMERO_INCIDENTE",
      col_codigo,
      col_localidad,
      "LOCALIDAD_CATALOGO"
  ]].rename(columns={col_localidad: "LOCALIDAD_CSV"})

  print("Inconsistencias con la localidad encontradas (pandas):")
  print(df_inconsistencias.head())

  return df_inconsistencias


def corregir_localidad_desde_errores_pandas(df: pd.DataFrame, df_errores: pd.DataFrame, col_id="NUMERO_INCIDENTE", col_localidad="LOCALIDAD"):
  df = df.copy()
  map_localidad = (
      df_errores[[col_id, "LOCALIDAD_CATALOGO"]]
      .drop_duplicates(subset=[col_id])
      .set_index(col_id)["LOCALIDAD_CATALOGO"]
      .to_dict()
  )

  df[col_localidad] = df.apply(
      lambda row: map_localidad.get(row[col_id], row[col_localidad]),
      axis=1
  )

  print(f"Corrección de localidades aplicada. Registros corregidos: {len(map_localidad)}")

  return df