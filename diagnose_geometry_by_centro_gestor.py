# -*- coding: utf-8 -*-
"""
Script de diagnóstico para verificar problemas de geometría por centro gestor.
Analiza específicamente los centros gestores mencionados.
"""

import json
import pandas as pd
from pathlib import Path
from collections import defaultdict

# Centros gestores a analizar
CENTROS_A_REVISAR = [
    "Secretaría de Bienestar Social",
    "Secretaría de Desarrollo Territorial y Participación Ciudadana",
    "Secretaría de Movilidad",
    "Secretaría de Paz y Cultura Ciudadana",
    "Secretaría de Salud Pública",
    "Secretaría de Seguridad y Justicia"
]

print('='*80)
print('🔍 DIAGNÓSTICO DE GEOMETRÍAS POR CENTRO GESTOR')
print('='*80)

# Verificar archivo GeoJSON transformado
geojson_path = Path('data/transformed/unidades_proyecto_transformed.geojson')

if not geojson_path.exists():
    print(f'\n❌ ERROR: No se encontró el archivo GeoJSON: {geojson_path}')
    print('   Ejecuta el pipeline primero: python pipelines/unidades_proyecto_pipeline.py')
    exit(1)

print(f'\n📂 Leyendo: {geojson_path}')

with open(geojson_path, 'r', encoding='utf-8') as f:
    geojson_data = json.load(f)

features = geojson_data.get('features', [])
total_features = len(features)

print(f'   Total de features: {total_features:,}')

# Analizar por centro gestor
stats_por_centro = defaultdict(lambda: {
    'total': 0,
    'con_geometry_valida': 0,
    'sin_geometry': 0,
    'geometry_null': 0,
    'con_lat_lon': 0,
    'sin_lat_lon': 0,
    'lat_lon_validos': 0,
    'lat_lon_invalidos': 0,
    'ejemplos_sin_geometry': [],
    'ejemplos_lat_lon_invalidos': []
})

print('\n📊 Analizando features...')

for feature in features:
    props = feature.get('properties', {})
    geom = feature.get('geometry')
    
    centro = props.get('nombre_centro_gestor', 'Sin Centro Gestor')
    upid = props.get('upid', 'Sin UPID')
    lat = props.get('lat')
    lon = props.get('lon')
    
    stats = stats_por_centro[centro]
    stats['total'] += 1
    
    # Analizar geometry
    if geom and geom.get('coordinates'):
        coords = geom['coordinates']
        if coords and len(coords) == 2:
            coord_lon = coords[0]
            coord_lat = coords[1]
            
            # Validar que las coordenadas sean números válidos
            try:
                if isinstance(coord_lon, (int, float)) and isinstance(coord_lat, (int, float)):
                    if -77.0 <= coord_lon <= -76.0 and 3.0 <= coord_lat <= 4.0:
                        stats['con_geometry_valida'] += 1
                    else:
                        stats['sin_geometry'] += 1
                        if len(stats['ejemplos_sin_geometry']) < 3:
                            stats['ejemplos_sin_geometry'].append({
                                'upid': upid,
                                'reason': f'Coordenadas fuera de rango: [{coord_lon}, {coord_lat}]',
                                'lat': lat,
                                'lon': lon
                            })
                else:
                    stats['sin_geometry'] += 1
                    if len(stats['ejemplos_sin_geometry']) < 3:
                        stats['ejemplos_sin_geometry'].append({
                            'upid': upid,
                            'reason': f'Coordenadas no numéricas: [{coord_lon}, {coord_lat}]',
                            'lat': lat,
                            'lon': lon
                        })
            except Exception as e:
                stats['sin_geometry'] += 1
                if len(stats['ejemplos_sin_geometry']) < 3:
                    stats['ejemplos_sin_geometry'].append({
                        'upid': upid,
                        'reason': f'Error al validar: {e}',
                        'lat': lat,
                        'lon': lon
                    })
        else:
            stats['sin_geometry'] += 1
            if len(stats['ejemplos_sin_geometry']) < 3:
                stats['ejemplos_sin_geometry'].append({
                    'upid': upid,
                    'reason': 'Coordenadas vacías o mal formateadas',
                    'lat': lat,
                    'lon': lon
                })
    else:
        stats['geometry_null'] += 1
        stats['sin_geometry'] += 1
        if len(stats['ejemplos_sin_geometry']) < 3:
            stats['ejemplos_sin_geometry'].append({
                'upid': upid,
                'reason': 'Geometry es null',
                'lat': lat,
                'lon': lon
            })
    
    # Analizar lat/lon en properties
    if lat is not None and lon is not None:
        stats['con_lat_lon'] += 1
        
        # Validar que lat/lon sean válidos
        try:
            if isinstance(lat, str):
                lat_num = float(lat.replace(',', '.'))
            else:
                lat_num = float(lat)
            
            if isinstance(lon, str):
                lon_num = float(lon.replace(',', '.'))
            else:
                lon_num = float(lon)
            
            if 3.0 <= lat_num <= 4.0 and -77.0 <= lon_num <= -76.0:
                stats['lat_lon_validos'] += 1
            else:
                stats['lat_lon_invalidos'] += 1
                if len(stats['ejemplos_lat_lon_invalidos']) < 3:
                    stats['ejemplos_lat_lon_invalidos'].append({
                        'upid': upid,
                        'lat': lat,
                        'lon': lon,
                        'lat_num': lat_num,
                        'lon_num': lon_num
                    })
        except (ValueError, TypeError) as e:
            stats['lat_lon_invalidos'] += 1
            if len(stats['ejemplos_lat_lon_invalidos']) < 3:
                stats['ejemplos_lat_lon_invalidos'].append({
                    'upid': upid,
                    'lat': lat,
                    'lon': lon,
                    'error': str(e)
                })
    else:
        stats['sin_lat_lon'] += 1

# Mostrar resultados para centros específicos
print('\n' + '='*80)
print('📋 RESULTADOS POR CENTRO GESTOR')
print('='*80)

for centro in CENTROS_A_REVISAR:
    if centro in stats_por_centro:
        stats = stats_por_centro[centro]
        print(f'\n🏢 {centro}')
        print(f'   {"─"*76}')
        print(f'   Total registros: {stats["total"]:,}')
        print(f'   ✅ Con geometry válida: {stats["con_geometry_valida"]:,} ({stats["con_geometry_valida"]/stats["total"]*100:.1f}%)')
        print(f'   ❌ Sin geometry/inválida: {stats["sin_geometry"]:,} ({stats["sin_geometry"]/stats["total"]*100:.1f}%)')
        print(f'   📍 Con lat/lon: {stats["con_lat_lon"]:,}')
        print(f'   ✓ Lat/lon válidos: {stats["lat_lon_validos"]:,}')
        print(f'   ✗ Lat/lon inválidos: {stats["lat_lon_invalidos"]:,}')
        print(f'   ⚠ Sin lat/lon: {stats["sin_lat_lon"]:,}')
        
        # Mostrar ejemplos de problemas
        if stats['ejemplos_sin_geometry']:
            print(f'\n   📌 Ejemplos sin geometry:')
            for ej in stats['ejemplos_sin_geometry']:
                print(f'      • {ej["upid"]}: {ej["reason"]}')
                if ej.get('lat') or ej.get('lon'):
                    print(f'        Tiene lat/lon en props: lat={ej.get("lat")}, lon={ej.get("lon")}')
        
        if stats['ejemplos_lat_lon_invalidos']:
            print(f'\n   📌 Ejemplos lat/lon inválidos:')
            for ej in stats['ejemplos_lat_lon_invalidos']:
                print(f'      • {ej["upid"]}:')
                print(f'        Original: lat={ej.get("lat")}, lon={ej.get("lon")}')
                if 'lat_num' in ej:
                    print(f'        Numérico: lat={ej.get("lat_num")}, lon={ej.get("lon_num")}')
                if 'error' in ej:
                    print(f'        Error: {ej.get("error")}')
    else:
        print(f'\n🏢 {centro}')
        print(f'   ⚠ No se encontraron registros con este nombre')

# Resumen general
print('\n' + '='*80)
print('📊 RESUMEN GENERAL')
print('='*80)

total_centros_problema = 0
total_registros_problema = 0
total_registros_con_latlon_pero_sin_geometry = 0

for centro in CENTROS_A_REVISAR:
    if centro in stats_por_centro:
        stats = stats_por_centro[centro]
        if stats['sin_geometry'] > 0:
            total_centros_problema += 1
            total_registros_problema += stats['sin_geometry']
        
        # Registros con lat/lon válidos pero sin geometry
        con_latlon_sin_geom = stats['lat_lon_validos'] - stats['con_geometry_valida']
        if con_latlon_sin_geom > 0:
            total_registros_con_latlon_pero_sin_geometry += con_latlon_sin_geom

print(f'\n🏢 Centros gestores analizados: {len(CENTROS_A_REVISAR)}')
print(f'⚠ Centros con problemas de geometry: {total_centros_problema}')
print(f'❌ Total registros sin geometry: {total_registros_problema:,}')
print(f'🔧 Registros con lat/lon válidos pero sin geometry: {total_registros_con_latlon_pero_sin_geometry:,}')

if total_registros_con_latlon_pero_sin_geometry > 0:
    print(f'\n💡 PROBLEMA IDENTIFICADO:')
    print(f'   Hay {total_registros_con_latlon_pero_sin_geometry} registros que tienen lat/lon válidos')
    print(f'   pero no tienen geometry. Esto indica que:')
    print(f'   1. La función convert_to_geodataframe() no está creando geometry desde lat/lon')
    print(f'   2. La función export_to_geojson() no está reconstruyendo geometry desde lat/lon')
    print(f'   3. Las coordenadas lat/lon pueden estar en formato incorrecto')

print('\n' + '='*80)
print('✅ DIAGNÓSTICO COMPLETADO')
print('='*80)
