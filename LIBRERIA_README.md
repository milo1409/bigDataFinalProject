# 📚 Librería Data Loader 123

Librería Python para cargar, procesar y analizar datos de llamadas al servicio de emergencias 123.

## 🎯 Características

- ✅ Carga de archivos CSV individuales o múltiples
- ✅ Filtrado por año, mes o archivo específico
- ✅ Limpieza y normalización automática de datos
- ✅ Filtros por localidad, prioridad y tipo de incidente
- ✅ Estadísticas descriptivas
- ✅ Exportación a CSV y Parquet
- ✅ Manejo eficiente de memoria
- ✅ Soporte para encodings especiales

## 📦 Instalación

### Requisitos

```bash
pip install pandas
```

Para exportar a formato Parquet (opcional):
```bash
pip install pyarrow
```

## 🚀 Uso Rápido

### Ejemplo 1: Cargar todos los archivos

```python
from data_loader import DataLoader123

# Crear instancia
loader = DataLoader123("data")

# Cargar todos los archivos
data = loader.load_all_files()

print(f"Total de registros: {len(data):,}")
```

### Ejemplo 2: Cargar datos de un año específico

```python
from data_loader import DataLoader123

loader = DataLoader123("data")
data_2023 = loader.load_by_year(2023)
```

### Ejemplo 3: Cargar y limpiar datos

```python
from data_loader import load_and_clean

# Carga y limpia en un solo paso
datos_limpios = load_and_clean("data", year=2023)
```

## 📖 Documentación Completa

### Clase `DataLoader123`

#### Constructor

```python
DataLoader123(data_directory: str = "data")
```

**Parámetros:**
- `data_directory`: Ruta al directorio que contiene los archivos CSV

#### Métodos Principales

##### `load_all_files()`
Carga todos los archivos CSV del directorio.

```python
loader = DataLoader123("data")
data = loader.load_all_files()
```

##### `load_by_year(year: int)`
Carga archivos de un año específico.

```python
data_2022 = loader.load_by_year(2022)
```

##### `load_by_month(year: int, month: int)`
Carga archivo de un mes específico.

```python
data_enero = loader.load_by_month(2024, 1)
```

##### `clean_data()`
Limpia y normaliza los datos cargados.

```python
datos_limpios = loader.clean_data()
```

**Tareas de limpieza:**
- Convierte fechas a formato datetime
- Normaliza valores de edad a numéricos
- Reemplaza "SIN_DATO" con valores NA
- Elimina espacios en blanco

##### `get_statistics()`
Obtiene estadísticas descriptivas.

```python
stats = loader.get_statistics()
print(stats['total_incidentes'])
print(stats['top_localidades'])
print(stats['distribucion_prioridad'])
```

**Retorna:**
```python
{
    'total_incidentes': int,
    'localidades_unicas': int,
    'tipos_incidente': int,
    'rango_fechas': {'inicio': str, 'fin': str},
    'top_localidades': dict,
    'top_incidentes': dict,
    'distribucion_genero': dict,
    'distribucion_prioridad': dict
}
```

##### Métodos de Filtrado

```python
# Filtrar por localidad
datos_kennedy = loader.filter_by_localidad("Kennedy")

# Filtrar por prioridad
datos_criticos = loader.filter_by_prioridad("CRITICA")

# Filtrar por tipo de incidente
datos_respiratorio = loader.filter_by_tipo_incidente("Evento Respiratorio")
```

##### Métodos de Exportación

```python
# Exportar a CSV
loader.export_to_csv("salida.csv")

# Exportar a Parquet (más eficiente)
loader.export_to_parquet("salida.parquet")
```

### Funciones de Utilidad

#### `quick_load()`
Carga rápida de datos sin crear instancia.

```python
from data_loader import quick_load

data = quick_load("data", year=2023)
```

#### `load_and_clean()`
Carga y limpia en un solo paso.

```python
from data_loader import load_and_clean

datos_limpios = load_and_clean("data", year=2023)
```

## 📊 Estructura de Datos

Los archivos CSV contienen las siguientes columnas:

| Columna | Descripción |
|---------|-------------|
| `NUMERO_INCIDENTE` | Identificador único del incidente |
| `FECHA_INICIO_DESPLAZAMIENTO-MOVIL` | Fecha y hora del desplazamiento |
| `CODIGO_LOCALIDAD` | Código de la localidad |
| `LOCALIDAD` | Nombre de la localidad |
| `EDAD` | Edad del paciente |
| `UNIDAD` | Unidad de medida de la edad |
| `GENERO` | Género del paciente |
| `RED` | Red de atención (Norte/Sur) |
| `TIPO_INCIDENTE` | Tipo de emergencia |
| `PRIORIDAD` | Nivel de prioridad (CRITICA/ALTA/MEDIA/BAJA) |

## 💡 Ejemplos Avanzados

### Análisis por hora del día

```python
loader = DataLoader123("data")
loader.load_by_year(2023)
datos = loader.clean_data()

# Extraer hora
datos['HORA'] = pd.to_datetime(datos['FECHA_INICIO_DESPLAZAMIENTO-MOVIL']).dt.hour

# Contar incidentes por hora
incidentes_por_hora = datos['HORA'].value_counts().sort_index()
print(incidentes_por_hora)
```

### Comparativa entre años

```python
años = [2021, 2022, 2023, 2024]
comparativa = {}

for año in años:
    loader = DataLoader123("data")
    data = loader.load_by_year(año)
    comparativa[año] = len(data)

print(comparativa)
```

### Filtrado múltiple con pandas

```python
loader = DataLoader123("data")
loader.load_all_files()
datos = loader.clean_data()

# Filtro: Incidentes críticos en Kennedy
criticos_kennedy = datos[
    (datos['LOCALIDAD'] == 'Kennedy') & 
    (datos['PRIORIDAD'] == 'CRITICA')
]

print(f"Incidentes críticos en Kennedy: {len(criticos_kennedy)}")
```

## 🔧 Configuración

### Encoding
Por defecto, los archivos se leen con encoding `latin-1`. Si necesitas otro encoding:

```python
loader = DataLoader123("data")
data = loader.load_all_files(encoding='utf-8')
```

### Límite de archivos
Para cargar solo los primeros N archivos:

```python
data = loader.load_all_files(limit=5)
```

## ⚡ Rendimiento

### Recomendaciones para grandes volúmenes de datos:

1. **Carga por año**: En lugar de cargar todos los archivos a la vez
   ```python
   loader.load_by_year(2023)  # Más eficiente
   ```

2. **Formato Parquet**: Para almacenamiento eficiente
   ```python
   loader.export_to_parquet("datos.parquet")
   # Luego leer con pandas
   datos = pd.read_parquet("datos.parquet")
   ```

3. **Procesamiento por lotes**: Para análisis grandes
   ```python
   for año in [2021, 2022, 2023]:
       loader = DataLoader123("data")
       data = loader.load_by_year(año)
       # Procesar...
       del loader, data  # Liberar memoria
   ```

## 📁 Estructura de Archivos

```
proyecto/
├── data/                           # Carpeta con archivos CSV
│   ├── 202107_llamadas_123_julio2021.csv
│   ├── 202108_llamadas_123_agosto2021.csv
│   └── ...
├── data_loader.py                  # Librería principal
├── ejemplo_uso.py                  # Ejemplos de uso
└── LIBRERIA_README.md             # Esta documentación
```

## 🐛 Solución de Problemas

### Error de encoding
Si encuentras caracteres extraños, prueba diferentes encodings:
```python
loader.load_all_files(encoding='utf-8')
# o
loader.load_all_files(encoding='cp1252')
```

### Memoria insuficiente
Para datasets muy grandes:
```python
# Cargar por partes
loader.load_by_year(2023)  # Solo un año
# O usar chunks con pandas directamente
```

### Archivo no encontrado
Verifica que la ruta sea correcta:
```python
files = loader.get_available_files()
print(f"Archivos encontrados: {len(files)}")
```

## 📝 Notas

- Los archivos CSV usan `;` como separador
- El encoding por defecto es `latin-1` debido a caracteres especiales en español
- Los valores `SIN_DATO` se convierten automáticamente a `NA` durante la limpieza

## 🤝 Contribuciones

Para mejorar esta librería:
1. Agrega nuevos métodos de análisis
2. Optimiza el rendimiento
3. Añade más filtros útiles
4. Documenta casos de uso adicionales

## 📄 Licencia

Este código es parte del proyecto Big Data Final Project.

---

**Autor**: Big Data Team  
**Fecha**: Diciembre 2025  
**Versión**: 1.0
