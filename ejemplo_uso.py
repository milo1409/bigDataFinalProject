"""
Ejemplos de uso de la librería data_loader para procesar datos de llamadas al 123.

Este archivo demuestra las diferentes funcionalidades de la librería.
"""

from data_loader import DataLoader123, quick_load, load_and_clean
import pandas as pd

# ============================================================================
# EJEMPLO 1: Uso básico - Cargar todos los archivos
# ============================================================================
print("\n" + "="*70)
print("EJEMPLO 1: Cargar todos los archivos")
print("="*70)

loader = DataLoader123("data")
data_completa = loader.load_all_files()

# Ver información básica
info = loader.get_info()
print(f"\nTotal de registros: {info['total_registros']:,}")
print(f"Memoria utilizada: {info['memoria_mb']:.2f} MB")


# ============================================================================
# EJEMPLO 2: Cargar datos de un año específico
# ============================================================================
print("\n" + "="*70)
print("EJEMPLO 2: Cargar datos del año 2022")
print("="*70)

loader2 = DataLoader123("data")
data_2022 = loader2.load_by_year(2022)


# ============================================================================
# EJEMPLO 3: Cargar datos de un mes específico
# ============================================================================
print("\n" + "="*70)
print("EJEMPLO 3: Cargar datos de enero 2024")
print("="*70)

loader3 = DataLoader123("data")
data_enero_2024 = loader3.load_by_month(2024, 1)


# ============================================================================
# EJEMPLO 4: Cargar y limpiar datos
# ============================================================================
print("\n" + "="*70)
print("EJEMPLO 4: Cargar y limpiar datos del 2023")
print("="*70)

loader4 = DataLoader123("data")
loader4.load_by_year(2023)
datos_limpios = loader4.clean_data()

# Mostrar primeras filas
print("\n📋 Primeras 5 filas de datos limpios:")
print(datos_limpios.head())


# ============================================================================
# EJEMPLO 5: Obtener estadísticas
# ============================================================================
print("\n" + "="*70)
print("EJEMPLO 5: Estadísticas generales")
print("="*70)

stats = loader4.get_statistics()

print(f"\n📊 Total de incidentes: {stats['total_incidentes']:,}")
print(f"📍 Localidades únicas: {stats['localidades_unicas']}")
print(f"🆘 Tipos de incidente: {stats['tipos_incidente']}")
print(f"📅 Rango de fechas: {stats['rango_fechas']['inicio']} a {stats['rango_fechas']['fin']}")

print("\n🏆 Top 5 Localidades con más incidentes:")
for localidad, cantidad in list(stats['top_localidades'].items())[:5]:
    print(f"  • {localidad}: {cantidad:,}")

print("\n🆘 Top 5 Tipos de incidente:")
for tipo, cantidad in list(stats['top_incidentes'].items())[:5]:
    print(f"  • {tipo}: {cantidad:,}")

print("\n🚨 Distribución por prioridad:")
for prioridad, cantidad in stats['distribucion_prioridad'].items():
    print(f"  • {prioridad}: {cantidad:,}")


# ============================================================================
# EJEMPLO 6: Filtrar datos
# ============================================================================
print("\n" + "="*70)
print("EJEMPLO 6: Filtrar datos por diferentes criterios")
print("="*70)

# Filtrar por localidad
datos_kennedy = loader4.filter_by_localidad("Kennedy")
print(f"\n📍 Incidentes en Kennedy: {len(datos_kennedy):,}")

# Filtrar por prioridad
datos_criticos = loader4.filter_by_prioridad("CRITICA")
print(f"🚨 Incidentes críticos: {len(datos_criticos):,}")

# Filtrar por tipo de incidente
datos_respiratorio = loader4.filter_by_tipo_incidente("Evento Respiratorio")
print(f"🫁 Eventos respiratorios: {len(datos_respiratorio):,}")


# ============================================================================
# EJEMPLO 7: Análisis avanzado con pandas
# ============================================================================
print("\n" + "="*70)
print("EJEMPLO 7: Análisis avanzado")
print("="*70)

if datos_limpios is not None and not datos_limpios.empty:
    # Incidentes por género
    print("\n👥 Distribución por género:")
    genero_dist = datos_limpios['GENERO'].value_counts()
    for gen, cant in genero_dist.items():
        porcentaje = (cant / len(datos_limpios)) * 100
        print(f"  • {gen}: {cant:,} ({porcentaje:.1f}%)")
    
    # Incidentes por red
    print("\n🌐 Distribución por red:")
    red_dist = datos_limpios['RED'].value_counts()
    for red, cant in red_dist.items():
        print(f"  • {red}: {cant:,}")
    
    # Análisis por hora si la fecha está disponible
    if 'FECHA_INICIO_DESPLAZAMIENTO-MOVIL' in datos_limpios.columns:
        datos_limpios['HORA'] = pd.to_datetime(datos_limpios['FECHA_INICIO_DESPLAZAMIENTO-MOVIL']).dt.hour
        print("\n⏰ Top 5 horas con más incidentes:")
        horas_top = datos_limpios['HORA'].value_counts().head(5)
        for hora, cant in horas_top.items():
            print(f"  • {int(hora):02d}:00 - {cant:,} incidentes")


# ============================================================================
# EJEMPLO 8: Exportar datos procesados
# ============================================================================
print("\n" + "="*70)
print("EJEMPLO 8: Exportar datos procesados")
print("="*70)

# Exportar a CSV
# loader4.export_to_csv("datos_procesados_2023.csv")

# Exportar a Parquet (más eficiente para big data)
# loader4.export_to_parquet("datos_procesados_2023.parquet")

print("\n💡 TIP: Descomenta las líneas anteriores para exportar los datos")


# ============================================================================
# EJEMPLO 9: Uso rápido con funciones de utilidad
# ============================================================================
print("\n" + "="*70)
print("EJEMPLO 9: Funciones rápidas de utilidad")
print("="*70)

# Cargar datos de forma rápida
# datos_rapidos = quick_load("data", year=2024)

# Cargar y limpiar en un solo paso
# datos_limpios_rapido = load_and_clean("data", year=2024)

print("\n💡 TIP: Usa quick_load() o load_and_clean() para un acceso más rápido")


# ============================================================================
# EJEMPLO 10: Trabajar con múltiples años
# ============================================================================
print("\n" + "="*70)
print("EJEMPLO 10: Comparativa entre años")
print("="*70)

# Crear un diccionario para almacenar datos por año
datos_por_año = {}

for año in [2021, 2022, 2023, 2024]:
    print(f"\n📅 Procesando año {año}...")
    loader_temp = DataLoader123("data")
    data_temp = loader_temp.load_by_year(año)
    
    if not data_temp.empty:
        datos_por_año[año] = {
            'data': data_temp,
            'total': len(data_temp)
        }

print("\n📊 Resumen por año:")
for año, info in datos_por_año.items():
    print(f"  {año}: {info['total']:,} incidentes")


print("\n" + "="*70)
print("✅ EJEMPLOS COMPLETADOS")
print("="*70)
print("\n💡 Revisa el código para ver más detalles de implementación")
print("📚 Consulta data_loader.py para ver todas las funcionalidades disponibles\n")
