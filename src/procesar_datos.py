

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