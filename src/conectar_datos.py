import pandas as pd

class extraer_datos_procesamiento():

    def conectar_datos():
        pass

    def generar_formato_parquet():
        pass

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