"""
Script de prueba para validar la corrección de coordenadas en properties.
Verifica que las coordenadas lat/lon se preserven correctamente en el GeoJSON.
"""

import sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path

# Agregar path de transformación
sys.path.append(str(Path(__file__).parent))

# Simular datos de prueba
print("="*80)
print("PRUEBA: Corrección de Coordenadas en Properties")
print("="*80)

# Crear datos de prueba con diferentes escenarios
test_data = {
    'upid': ['UNP-001', 'UNP-002', 'UNP-003', 'UNP-004'],
    'nombre_up': ['Test 1', 'Test 2', 'Test 3', 'Test 4'],
    'nombre_centro_gestor': [
        'Secretaría de Bienestar Social',
        'Secretaría de Salud Pública',
        'Secretaría de Educación',
        'Secretaría de Bienestar Social'
    ],
    'geometry': [
        Point(-76.508516, 3.479621),  # Caso 1: Solo geometry
        Point(-76.489005, 3.472794),  # Caso 2: Solo geometry
        None,                          # Caso 3: Sin geometry, con lat/lon
        Point(-76.525165, 3.450414),  # Caso 4: Solo geometry
    ],
    'lat': [None, None, 3.462815, None],  # Solo Test 3 tiene lat
    'lon': [None, None, -76.485576, None]  # Solo Test 3 tiene lon
}

df = pd.DataFrame(test_data)
gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')

print(f"\n📊 Datos de entrada:")
print(f"   Total registros: {len(gdf)}")
print(f"   Con geometry: {gdf['geometry'].notna().sum()}")
print(f"   Con lat/lon: {(gdf['lat'].notna() & gdf['lon'].notna()).sum()}")

# Simular la lógica de corrección
print(f"\n🔧 Aplicando lógica de corrección...")

coords_extraidas = 0
geometrias_creadas = 0

for idx in gdf.index:
    geom = gdf.at[idx, 'geometry']
    lat = gdf.at[idx, 'lat']
    lon = gdf.at[idx, 'lon']
    
    # Caso 1: Hay geometry pero no hay lat/lon -> extraer
    if pd.notna(geom) and hasattr(geom, 'x') and hasattr(geom, 'y'):
        if pd.isna(lat):
            gdf.at[idx, 'lat'] = float(geom.y)
            coords_extraidas += 1
        if pd.isna(lon):
            gdf.at[idx, 'lon'] = float(geom.x)
    
    # Caso 2: Hay lat/lon pero no hay geometry -> crear
    elif (geom is None or pd.isna(geom)) and pd.notna(lat) and pd.notna(lon):
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            gdf.at[idx, 'geometry'] = Point(float(lon), float(lat))
            geometrias_creadas += 1

print(f"   ✅ Coordenadas extraídas desde geometry: {coords_extraidas}")
print(f"   ✅ Geometrías creadas desde lat/lon: {geometrias_creadas}")

# Verificar resultados
print(f"\n📊 Resultados:")
print(f"   Total registros: {len(gdf)}")
print(f"   Con geometry: {gdf['geometry'].notna().sum()}")
print(f"   Con lat/lon: {(gdf['lat'].notna() & gdf['lon'].notna()).sum()}")

# Mostrar detalles por centro gestor
print(f"\n📍 Verificación por centro gestor:")
for centro in gdf['nombre_centro_gestor'].unique():
    centro_data = gdf[gdf['nombre_centro_gestor'] == centro]
    geom_count = centro_data['geometry'].notna().sum()
    coords_count = (centro_data['lat'].notna() & centro_data['lon'].notna()).sum()
    print(f"   {centro}:")
    print(f"      • Registros: {len(centro_data)}")
    print(f"      • Con geometry: {geom_count}")
    print(f"      • Con lat/lon: {coords_count}")
    
    if geom_count != coords_count:
        print(f"      ⚠️ ALERTA: Inconsistencia entre geometry y lat/lon")
    else:
        print(f"      ✅ Consistente")

# Mostrar ejemplos específicos
print(f"\n📋 Ejemplos de registros:")
for idx, row in gdf.iterrows():
    print(f"\n   {row['nombre_up']} ({row['nombre_centro_gestor']}):")
    print(f"      • UPID: {row['upid']}")
    if pd.notna(row['geometry']):
        print(f"      • Geometry: [{row['geometry'].x:.6f}, {row['geometry'].y:.6f}]")
    else:
        print(f"      • Geometry: None")
    
    if pd.notna(row['lat']) and pd.notna(row['lon']):
        print(f"      • Properties: lat={row['lat']:.6f}, lon={row['lon']:.6f}")
    else:
        print(f"      • Properties: lat={row['lat']}, lon={row['lon']}")
    
    # Verificar consistencia
    if pd.notna(row['geometry']) and pd.notna(row['lat']) and pd.notna(row['lon']):
        geom_lat = row['geometry'].y
        geom_lon = row['geometry'].x
        if abs(geom_lat - row['lat']) < 0.000001 and abs(geom_lon - row['lon']) < 0.000001:
            print(f"      ✅ Geometry y properties consistentes")
        else:
            print(f"      ⚠️ Geometry y properties NO consistentes")

print(f"\n{'='*80}")
print(f"PRUEBA COMPLETADA")
print(f"{'='*80}")
