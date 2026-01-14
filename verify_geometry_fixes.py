# -*- coding: utf-8 -*-
"""
Script para verificar las correcciones de geometría implementadas.
Compara el estado actual con los centros gestores problemáticos.
"""

import json
from pathlib import Path
from collections import defaultdict

# Centros gestores que se reportaron con problemas
CENTROS_PROBLEMA = [
    "Secretaría de Bienestar Social",
    "Secretaría de Desarrollo Territorial y Participación Ciudadana",
    "Secretaría de Movilidad",
    "Secretaría de Paz y Cultura Ciudadana",
    "Secretaría de Salud Pública",
    "Secretaría de Seguridad y Justicia"
]

print('='*80)
print('✅ VERIFICACIÓN DE CORRECCIONES DE GEOMETRÍA')
print('='*80)

# Leer el GeoJSON transformado
geojson_path = Path('app_outputs/unidades_proyecto_transformed.geojson')

if not geojson_path.exists():
    print(f'\n❌ No se encontró: {geojson_path}')
    exit(1)

print(f'\n📂 Analizando: {geojson_path}')

with open(geojson_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

features = data.get('features', [])
total = len(features)

print(f'   Total features: {total:,}')

# Estadísticas por centro gestor
stats = defaultdict(lambda: {
    'total': 0,
    'con_geometry': 0,
    'con_lat_lon': 0,
    'sin_geometry_ni_coords': 0
})

for feature in features:
    props = feature.get('properties', {})
    geom = feature.get('geometry')
    
    centro = props.get('nombre_centro_gestor', 'Sin Centro')
    lat = props.get('lat')
    lon = props.get('lon')
    
    stats[centro]['total'] += 1
    
    # Verificar geometry
    has_valid_geom = False
    if geom and geom.get('coordinates'):
        coords = geom['coordinates']
        if isinstance(coords, list) and len(coords) == 2:
            if all(isinstance(c, (int, float)) for c in coords):
                has_valid_geom = True
                stats[centro]['con_geometry'] += 1
    
    # Verificar lat/lon
    has_coords = lat is not None and lon is not None
    if has_coords:
        stats[centro]['con_lat_lon'] += 1
    
    # Sin nada
    if not has_valid_geom and not has_coords:
        stats[centro]['sin_geometry_ni_coords'] += 1

print('\n' + '='*80)
print('📊 ESTADO DE CENTROS GESTORES PROBLEMÁTICOS')
print('='*80)

for centro in CENTROS_PROBLEMA:
    if centro in stats:
        s = stats[centro]
        pct_geom = (s['con_geometry'] / s['total'] * 100) if s['total'] > 0 else 0
        pct_coords = (s['con_lat_lon'] / s['total'] * 100) if s['total'] > 0 else 0
        
        status = '✅' if pct_geom >= 90 else '⚠️' if pct_geom >= 70 else '❌'
        
        print(f'\n{status} {centro}')
        print(f'   Total: {s["total"]:,}')
        print(f'   Con geometry: {s["con_geometry"]:,} ({pct_geom:.1f}%)')
        print(f'   Con lat/lon: {s["con_lat_lon"]:,} ({pct_coords:.1f}%)')
        if s['sin_geometry_ni_coords'] > 0:
            print(f'   ⚠️  Sin geometry ni coords: {s["sin_geometry_ni_coords"]}')
    else:
        print(f'\n❓ {centro}: No encontrado')

# Resumen general
print('\n' + '='*80)
print('📈 RESUMEN GENERAL')
print('='*80)

total_con_geom = sum(s['con_geometry'] for s in stats.values())
total_con_coords = sum(s['con_lat_lon'] for s in stats.values())
total_sin_nada = sum(s['sin_geometry_ni_coords'] for s in stats.values())

print(f'\n📍 Geometrías:')
print(f'   Con geometry válida: {total_con_geom:,} ({total_con_geom/total*100:.1f}%)')
print(f'   Con lat/lon: {total_con_coords:,} ({total_con_coords/total*100:.1f}%)')
print(f'   Sin geometry ni coords: {total_sin_nada} ({total_sin_nada/total*100:.2f}%)')

# Verificar mejora
objetivo_minimo = 90.0
cobertura_actual = (total_con_geom / total * 100) if total > 0 else 0

print(f'\n🎯 Objetivo de cobertura: ≥{objetivo_minimo}%')
print(f'📊 Cobertura actual: {cobertura_actual:.1f}%')

if cobertura_actual >= objetivo_minimo:
    print(f'✅ ¡Objetivo alcanzado! Las correcciones funcionaron correctamente.')
else:
    print(f'⚠️  Aún por debajo del objetivo. Diferencia: {objetivo_minimo - cobertura_actual:.1f}%')

print('\n' + '='*80)
