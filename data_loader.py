"""
Librería para leer y procesar archivos de llamadas al 123 de emergencias.

Esta librería proporciona funcionalidades para:
- Cargar archivos CSV individuales o múltiples
- Limpiar y normalizar datos
- Filtrar por diferentes criterios
- Obtener estadísticas básicas

Autor: Big Data Final Project
Fecha: 2025-12-13
"""

import os
import glob
import pandas as pd
from typing import List, Optional, Dict, Union
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')


class DataLoader123:
    """
    Clase principal para cargar y procesar datos de llamadas al 123.
    """
    
    def __init__(self, data_directory: str = "data"):
        """
        Inicializa el cargador de datos.
        
        Args:
            data_directory (str): Ruta al directorio que contiene los archivos CSV.
        """
        self.data_directory = data_directory
        self.data = None
        self.columns = [
            'NUMERO_INCIDENTE',
            'FECHA_INICIO_DESPLAZAMIENTO-MOVIL',
            'CODIGO_LOCALIDAD',
            'LOCALIDAD',
            'EDAD',
            'UNIDAD',
            'GENERO',
            'RED',
            'TIPO_INCIDENTE',
            'PRIORIDAD'
        ]
    
    def get_available_files(self) -> List[str]:
        """
        Obtiene la lista de archivos CSV disponibles en el directorio de datos.
        
        Returns:
            List[str]: Lista de rutas de archivos CSV encontrados.
        """
        pattern = os.path.join(self.data_directory, "*.csv")
        files = glob.glob(pattern)
        return sorted(files)
    
    def load_single_file(self, filepath: str, encoding: str = 'latin-1') -> pd.DataFrame:
        """
        Carga un archivo CSV individual.
        
        Args:
            filepath (str): Ruta al archivo CSV.
            encoding (str): Codificación del archivo (por defecto latin-1).
        
        Returns:
            pd.DataFrame: DataFrame con los datos del archivo.
        """
        try:
            df = pd.read_csv(
                filepath,
                sep=';',
                encoding=encoding,
                low_memory=False
            )
            print(f"✓ Cargado: {os.path.basename(filepath)} ({len(df)} registros)")
            return df
        except Exception as e:
            print(f"✗ Error al cargar {filepath}: {str(e)}")
            return pd.DataFrame()
    
    def load_all_files(self, encoding: str = 'latin-1', limit: Optional[int] = None) -> pd.DataFrame:
        """
        Carga todos los archivos CSV del directorio de datos.
        
        Args:
            encoding (str): Codificación de los archivos (por defecto latin-1).
            limit (int, optional): Número máximo de archivos a cargar.
        
        Returns:
            pd.DataFrame: DataFrame consolidado con todos los datos.
        """
        files = self.get_available_files()
        
        if limit:
            files = files[:limit]
        
        print(f"\n🔄 Cargando {len(files)} archivo(s)...\n")
        
        dataframes = []
        for filepath in files:
            df = self.load_single_file(filepath, encoding)
            if not df.empty:
                # Agregar columna con el nombre del archivo fuente
                df['ARCHIVO_FUENTE'] = os.path.basename(filepath)
                dataframes.append(df)
        
        if dataframes:
            self.data = pd.concat(dataframes, ignore_index=True)
            print(f"\n✅ Total de registros cargados: {len(self.data):,}")
            print(f"📊 Columnas: {', '.join(self.data.columns.tolist())}\n")
            return self.data
        else:
            print("⚠️ No se pudieron cargar datos.")
            return pd.DataFrame()
    
    def load_by_year(self, year: int, encoding: str = 'latin-1') -> pd.DataFrame:
        """
        Carga archivos de un año específico.
        
        Args:
            year (int): Año a cargar (ej: 2021, 2022).
            encoding (str): Codificación de los archivos.
        
        Returns:
            pd.DataFrame: DataFrame con los datos del año especificado.
        """
        pattern = os.path.join(self.data_directory, f"{year}*.csv")
        files = glob.glob(pattern)
        
        print(f"\n🔄 Cargando archivos del año {year}...\n")
        
        dataframes = []
        for filepath in sorted(files):
            df = self.load_single_file(filepath, encoding)
            if not df.empty:
                df['ARCHIVO_FUENTE'] = os.path.basename(filepath)
                dataframes.append(df)
        
        if dataframes:
            self.data = pd.concat(dataframes, ignore_index=True)
            print(f"\n✅ Total de registros del {year}: {len(self.data):,}\n")
            return self.data
        else:
            print(f"⚠️ No se encontraron archivos para el año {year}.")
            return pd.DataFrame()
    
    def load_by_month(self, year: int, month: int, encoding: str = 'latin-1') -> pd.DataFrame:
        """
        Carga archivo de un mes específico.
        
        Args:
            year (int): Año (ej: 2021).
            month (int): Mes (1-12).
            encoding (str): Codificación del archivo.
        
        Returns:
            pd.DataFrame: DataFrame con los datos del mes especificado.
        """
        year_month = f"{year}{month:02d}"
        pattern = os.path.join(self.data_directory, f"{year_month}*.csv")
        files = glob.glob(pattern)
        
        if files:
            filepath = files[0]
            df = self.load_single_file(filepath, encoding)
            if not df.empty:
                df['ARCHIVO_FUENTE'] = os.path.basename(filepath)
                self.data = df
                return df
        
        print(f"⚠️ No se encontró archivo para {year}-{month:02d}.")
        return pd.DataFrame()
    
    def clean_data(self) -> pd.DataFrame:
        """
        Limpia y normaliza los datos cargados.
        
        Returns:
            pd.DataFrame: DataFrame con datos limpios.
        """
        if self.data is None or self.data.empty:
            print("⚠️ No hay datos cargados para limpiar.")
            return pd.DataFrame()
        
        print("\n🧹 Limpiando datos...\n")
        
        # Copia para no modificar el original
        df = self.data.copy()
        
        # Convertir fecha a datetime
        if 'FECHA_INICIO_DESPLAZAMIENTO-MOVIL' in df.columns:
            df['FECHA_INICIO_DESPLAZAMIENTO-MOVIL'] = pd.to_datetime(
                df['FECHA_INICIO_DESPLAZAMIENTO-MOVIL'],
                errors='coerce'
            )
            print("✓ Columna de fecha convertida a datetime")
        
        # Limpiar columna de edad
        if 'EDAD' in df.columns and 'UNIDAD' in df.columns:
            # Combinar EDAD y UNIDAD en una sola columna numérica
            df['EDAD_NUMERICA'] = pd.to_numeric(df['EDAD'], errors='coerce')
            print("✓ Columna de edad convertida a numérica")
        
        # Normalizar valores SIN_DATO a None
        df = df.replace('SIN_DATO', pd.NA)
        print("✓ Valores 'SIN_DATO' normalizados a NA")
        
        # Eliminar espacios en blanco de columnas de texto
        for col in df.select_dtypes(include=['object']):
            if col != 'FECHA_INICIO_DESPLAZAMIENTO-MOVIL':
                df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
        
        print("✓ Espacios en blanco eliminados\n")
        print(f"📊 Datos limpios: {len(df):,} registros\n")
        
        self.data = df
        return df
    
    def get_info(self) -> Dict:
        """
        Obtiene información básica sobre los datos cargados.
        
        Returns:
            Dict: Diccionario con información estadística.
        """
        if self.data is None or self.data.empty:
            return {"error": "No hay datos cargados"}
        
        info = {
            "total_registros": len(self.data),
            "columnas": self.data.columns.tolist(),
            "valores_nulos": self.data.isnull().sum().to_dict(),
            "tipos_datos": self.data.dtypes.to_dict(),
            "memoria_mb": self.data.memory_usage(deep=True).sum() / 1024 / 1024
        }
        
        return info
    
    def filter_by_localidad(self, localidad: str) -> pd.DataFrame:
        """
        Filtra datos por localidad.
        
        Args:
            localidad (str): Nombre de la localidad.
        
        Returns:
            pd.DataFrame: DataFrame filtrado.
        """
        if self.data is None or self.data.empty:
            print("⚠️ No hay datos cargados.")
            return pd.DataFrame()
        
        filtered = self.data[self.data['LOCALIDAD'] == localidad]
        print(f"📍 Registros para {localidad}: {len(filtered):,}")
        return filtered
    
    def filter_by_prioridad(self, prioridad: str) -> pd.DataFrame:
        """
        Filtra datos por nivel de prioridad.
        
        Args:
            prioridad (str): Nivel de prioridad (CRITICA, ALTA, MEDIA, BAJA).
        
        Returns:
            pd.DataFrame: DataFrame filtrado.
        """
        if self.data is None or self.data.empty:
            print("⚠️ No hay datos cargados.")
            return pd.DataFrame()
        
        filtered = self.data[self.data['PRIORIDAD'] == prioridad]
        print(f"🚨 Registros con prioridad {prioridad}: {len(filtered):,}")
        return filtered
    
    def filter_by_tipo_incidente(self, tipo: str) -> pd.DataFrame:
        """
        Filtra datos por tipo de incidente.
        
        Args:
            tipo (str): Tipo de incidente.
        
        Returns:
            pd.DataFrame: DataFrame filtrado.
        """
        if self.data is None or self.data.empty:
            print("⚠️ No hay datos cargados.")
            return pd.DataFrame()
        
        filtered = self.data[self.data['TIPO_INCIDENTE'] == tipo]
        print(f"🆘 Registros de tipo '{tipo}': {len(filtered):,}")
        return filtered
    
    def get_statistics(self) -> Dict:
        """
        Obtiene estadísticas descriptivas de los datos.
        
        Returns:
            Dict: Diccionario con estadísticas.
        """
        if self.data is None or self.data.empty:
            return {"error": "No hay datos cargados"}
        
        stats = {
            "total_incidentes": len(self.data),
            "localidades_unicas": self.data['LOCALIDAD'].nunique() if 'LOCALIDAD' in self.data.columns else 0,
            "tipos_incidente": self.data['TIPO_INCIDENTE'].nunique() if 'TIPO_INCIDENTE' in self.data.columns else 0,
            "rango_fechas": {
                "inicio": str(self.data['FECHA_INICIO_DESPLAZAMIENTO-MOVIL'].min()) if 'FECHA_INICIO_DESPLAZAMIENTO-MOVIL' in self.data.columns else None,
                "fin": str(self.data['FECHA_INICIO_DESPLAZAMIENTO-MOVIL'].max()) if 'FECHA_INICIO_DESPLAZAMIENTO-MOVIL' in self.data.columns else None
            }
        }
        
        # Top 5 localidades con más incidentes
        if 'LOCALIDAD' in self.data.columns:
            stats['top_localidades'] = self.data['LOCALIDAD'].value_counts().head(5).to_dict()
        
        # Top 5 tipos de incidente
        if 'TIPO_INCIDENTE' in self.data.columns:
            stats['top_incidentes'] = self.data['TIPO_INCIDENTE'].value_counts().head(5).to_dict()
        
        # Distribución por género
        if 'GENERO' in self.data.columns:
            stats['distribucion_genero'] = self.data['GENERO'].value_counts().to_dict()
        
        # Distribución por prioridad
        if 'PRIORIDAD' in self.data.columns:
            stats['distribucion_prioridad'] = self.data['PRIORIDAD'].value_counts().to_dict()
        
        return stats
    
    def export_to_csv(self, output_path: str, index: bool = False) -> None:
        """
        Exporta los datos a un archivo CSV.
        
        Args:
            output_path (str): Ruta del archivo de salida.
            index (bool): Si incluir el índice en el CSV.
        """
        if self.data is None or self.data.empty:
            print("⚠️ No hay datos para exportar.")
            return
        
        self.data.to_csv(output_path, index=index, encoding='utf-8-sig', sep=';')
        print(f"✅ Datos exportados a: {output_path}")
    
    def export_to_parquet(self, output_path: str) -> None:
        """
        Exporta los datos a formato Parquet (más eficiente).
        
        Args:
            output_path (str): Ruta del archivo de salida.
        """
        if self.data is None or self.data.empty:
            print("⚠️ No hay datos para exportar.")
            return
        
        self.data.to_parquet(output_path, index=False)
        print(f"✅ Datos exportados a formato Parquet: {output_path}")


# Funciones de utilidad adicionales
def quick_load(data_directory: str = "data", year: Optional[int] = None) -> pd.DataFrame:
    """
    Función rápida para cargar datos sin crear una instancia de la clase.
    
    Args:
        data_directory (str): Directorio de datos.
        year (int, optional): Año específico a cargar.
    
    Returns:
        pd.DataFrame: DataFrame con los datos cargados.
    """
    loader = DataLoader123(data_directory)
    
    if year:
        return loader.load_by_year(year)
    else:
        return loader.load_all_files()


def load_and_clean(data_directory: str = "data", year: Optional[int] = None) -> pd.DataFrame:
    """
    Carga y limpia los datos en un solo paso.
    
    Args:
        data_directory (str): Directorio de datos.
        year (int, optional): Año específico a cargar.
    
    Returns:
        pd.DataFrame: DataFrame con datos limpios.
    """
    loader = DataLoader123(data_directory)
    
    if year:
        loader.load_by_year(year)
    else:
        loader.load_all_files()
    
    return loader.clean_data()


if __name__ == "__main__":
    # Ejemplo de uso
    print("=" * 60)
    print("📚 LIBRERÍA DE CARGA DE DATOS - LLAMADAS 123")
    print("=" * 60)
    
    # Crear instancia del cargador
    loader = DataLoader123("data")
    
    # Mostrar archivos disponibles
    print("\n📁 Archivos disponibles:")
    files = loader.get_available_files()
    for i, f in enumerate(files, 1):
        print(f"  {i}. {os.path.basename(f)}")
    
    # Cargar datos de un año específico
    data = loader.load_by_year(2021)
    
    # Limpiar datos
    clean_data = loader.clean_data()
    
    # Obtener estadísticas
    print("\n📊 ESTADÍSTICAS:")
    print("=" * 60)
    stats = loader.get_statistics()
    for key, value in stats.items():
        print(f"{key}: {value}")
