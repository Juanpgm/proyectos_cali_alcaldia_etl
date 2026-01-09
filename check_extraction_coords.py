"""
Script para verificar las coordenadas de Secretaría de Bienestar Social
en el archivo GeoJSON transformado ANTES de cargar a Firebase.
"""

import json
from pathlib import Path

# Ruta al archivo GeoJSON transformado
geojson_path = Path('transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto.geojson')

if not geojson_path.exists():
    print(f"❌ Archivo no encontrado: {geojson_path}")
    exit(1)

# Leer el GeoJSON
with open(geojson_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Filtrar por Secretaría de Bienestar Social
bienestar_features = [
    f for f in data['features'] 
    if f.get('properties', {}).get('nombre_centro_gestor') == 'Secretaría de Bienestar Social'
]

print(f"Total features de Secretaría de Bienestar Social: {len(bienestar_features)}")
print()

# Verificar coordenadas
features_con_geometry_valida = 0
features_con_coords_0 = 0
features_sin_geometry = 0

print("Analizando coordenadas:")
print("-" * 80)

for i, feature in enumerate(bienestar_features[:5], 1):
    props = feature.get('properties', {})
    geom = feature.get('geometry')
    
    upid = props.get('upid', 'N/A')
    nombre = props.get('nombre_up', 'N/A')
    
    print(f"\n{i}. {nombre} (UPID: {upid})")
    print(f"   Dirección: {props.get('direccion', 'N/A')}")
    
    if geom:
        coords = geom.get('coordinates', [])
        print(f"   Geometry type: {geom.get('type')}")
        print(f"   Coordinates: {coords}")
        
        # Verificar si son [0, 0]
        if coords == [0, 0] or (len(coords) >= 2 and coords[0] == 0 and coords[1] == 0):
            print(f"   ⚠️ PROBLEMA: Coordenadas en [0, 0]")
            features_con_coords_0 += 1
        elif len(coords) >= 2 and coords[0] != 0 and coords[1] != 0:
            print(f"   ✅ Coordenadas válidas: lon={coords[0]}, lat={coords[1]}")
            features_con_geometry_valida += 1
        else:
            print(f"   ⚠️ Coordenadas inválidas o vacías")
    else:
        print(f"   ❌ Sin geometry")
        features_sin_geometry += 1

print("\n" + "=" * 80)
print("RESUMEN:")
print(f"  Total features: {len(bienestar_features)}")
print(f"  Con geometry válida: {features_con_geometry_valida}")
print(f"  Con coordenadas [0, 0]: {features_con_coords_0}")
print(f"  Sin geometry: {features_sin_geometry}")

# Estadísticas generales
if features_con_coords_0 > 0:
    print(f"\n⚠️ PROBLEMA DETECTADO: {features_con_coords_0} features tienen coordenadas [0, 0]")
    print("   Esto indica que las coordenadas se pierden ANTES de la carga a Firebase")
    print("   El problema está en la transformación (transformation_app)")
elif features_sin_geometry > 0:
    print(f"\n⚠️ PROBLEMA DETECTADO: {features_sin_geometry} features no tienen geometry")
    print("   Esto indica que las coordenadas no se están creando correctamente")
else:
    print(f"\n✅ TODAS las features tienen coordenadas válidas en el archivo transformado")
