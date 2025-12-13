from typing import Dict, Tuple, List
import os
import pandas as pd
import re

class DataLoader123:

    @staticmethod
    def leer_csv_flexible(ruta_csv: str) -> pd.DataFrame | None:
        try:
            return pd.read_csv(ruta_csv, sep=";", encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            try:
                return pd.read_csv(ruta_csv, sep=";", encoding="latin-1", low_memory=False)
            except Exception as e:
                print(f"No se pudo leer el CSV {ruta_csv}: {e}")
                return None

    @staticmethod
    def reparar_texto_pandas(s):
        REEMPLAZOS_COMUNES = {
            "Ã¡": "á", "Ã©": "é", "Ã­": "í", "Ã³": "ó","ÃÁ": "Á", "Ã‰": "É", "ÃÍ": "Í", "Ã“": "Ó", "Ãš": "Ú",
            "Ã±": "ñ", "Ã‘": "Ñ","Ã¼": "ü", "Ãœ": "Ü","Â¿": "¿", "Â¡": "¡","Â°": "°", "Âª": "ª", "Âº": "º",
            "â€“": "–", "â€”": "—", "â€˜": "‘", "â€™": "’", "â€œ": "“", "â€": "”","â€¢": "•", "â€¦": "…",
            "Ã·": "I","Â§": "§","÷": "I","µ": "A","•": "N","‡": "O","§": "ñ","ê": "E","?": "I","¢": "N","ù":"ñ","¤":"n","¥":"N","à":"O","Ö":"O","":"e","¡":"I"
        }

        if pd.isna(s):
            return s
        if not isinstance(s, str):
            s = str(s)

        patron = re.compile("|".join(map(re.escape, REEMPLAZOS_COMUNES.keys())))
        return patron.sub(lambda m: REEMPLAZOS_COMUNES[m.group(0)], s)

    @staticmethod
    def reparar_dataframe_pandas(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        cols_texto = df.select_dtypes(include=["object", "string"]).columns
        for col_name in cols_texto:
            df[col_name] = df[col_name].apply(DataLoader123.reparar_texto_pandas)
        return df

    @staticmethod
    def unificar_columnas_duplicadas(df: pd.DataFrame, duplicate_groups: Dict) -> pd.DataFrame:
        df = df.copy()
        df = df.replace(r"^\s*$", pd.NA, regex=True)

        for canon, variantes in duplicate_groups.items():
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

    @staticmethod
    def procesar_todos_csv_crudos(
        ruta_cruda: str,
        duplicate_groups: Dict,
        columnas_esperadas: List[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:

        archivos = [
            os.path.join(ruta_cruda, f)
            for f in os.listdir(ruta_cruda)
            if f.lower().endswith(".csv")
        ]

        dfs: List[pd.DataFrame] = []
        inconsistencias: List[pd.DataFrame] = []

        for ruta_csv in archivos:
            df = DataLoader123.leer_csv_flexible(ruta_csv)
            if df is None:
                continue

            df = DataLoader123.reparar_dataframe_pandas(df)
            df = DataLoader123.unificar_columnas_duplicadas(df, duplicate_groups)

            df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

            cols_presentes = [c for c in columnas_esperadas if c in df.columns]
            df = df[cols_presentes]

            dfs.append(df)

        df_total = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        df_inconsistencias_total = pd.concat(inconsistencias, ignore_index=True) if inconsistencias else pd.DataFrame()

        return df_total, df_inconsistencias_total
