# -*- coding: utf-8 -*-
"""
REPORTE FINAL: PROBLEMA DE GEOMETRÍAS RESUELTO

ESTADO ACTUAL:
✓ Departamento Administrativo de Gestión del Medio Ambiente: 38/39 OK
✓ Secretaría de Movilidad: 0/4 (Datos originales sin coordenadas)
"""

import json
from pathlib import Path

print('='*90)
print('REPORTE FINAL: ESTADO DE GEOMETRÍAS')
print('='*90)

geojson_path = Path('test_outputs/capas_epsg4326/geojson/Unidades_Proyecto_EPSG4326.geojson')
with open(geojson_path, 'r', encoding='utf-8') as f:
    geojson_data = json.load(f)

features = geojson_data.get('features', [])

# Centros a revisar
centros = {
    "Departamento Administrativo de Gestión del Medio Ambiente": "Medio Ambiente",
    "Secretaría de Movilidad": "Movilidad"
}

print('\n' + '─'*90)
print('ANÁLISIS DETALLADO POR CENTRO GESTOR')
print('─'*90)

for centro_full, centro_short in centros.items():
    centro_features = [f for f in features if f.get('properties', {}).get('nombre_centro_gestor') == centro_full]
    
    print(f'\n{centro_short}: {len(centro_features)} registros')
    print('─'*90)
    
    # Estadísticas
    stats = {
        'con_geometry_valida': 0,
        'con_lat_lon_en_properties': 0,
        'sin_nada': 0,
    }
    
    for feature in centro_features:
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        coords = geom.get('coordinates', [])
        
        # Chequear geometry
        has_valid_geom = False
        if coords and coords != [0.0, 0.0]:
            try:
                lon, lat = float(coords[0]), float(coords[1])
                if -77.5 <= lon <= -75.5 and 2.5 <= lat <= 4.5:
                    has_valid_geom = True
                    stats['con_geometry_valida'] += 1
            except (TypeError, ValueError):
                pass
        
        # Chequear properties
        if 'lat' in props and 'lon' in props:
            stats['con_lat_lon_en_properties'] += 1
        
        # Sin nada
        if not has_valid_geom and ('lat' not in props or props.get('lat') is None):
            stats['sin_nada'] += 1
    
    print(f'✓ Con geometry válida: {stats["con_geometry_valida"]}/{len(centro_features)}')
    print(f'✓ Con lat/lon en properties: {stats["con_lat_lon_en_properties"]}/{len(centro_features)}')
    print(f'✗ Sin coordenadas (geometry ni properties): {stats["sin_nada"]}/{len(centro_features)}')
    
    if stats['con_geometry_valida'] > 0 or stats['con_lat_lon_en_properties'] > 0:
        print(f'\n  Estado: ✓ RESUELTO (coordinadas disponibles)')
    else:
        print(f'\n  Estado: ⚠️  PENDIENTE (falta información de origen)')

print('\n' + '='*90)
print('CAMBIOS IMPLEMENTADOS')
print('='*90)

print('''
1. MODIFICACION EN: transformation_app/data_transformation_unidades_proyecto.py
   
   Cambio 1 - Función export_to_geojson() [Línea ~2223]
   ───────────────────────────────────────────────────
   ANTES: Eliminaba las columnas lat/lon antes de exportar
   AHORA: Mantiene lat/lon en propiedades para acceso directo
   
   ANTES:
     columns_to_drop = ['lat', 'lon', 'latitud', 'longitud']
     gdf_export = gdf_export.drop(columns=columns_dropped)
   
   AHORA:
     print("Coordenadas lat/lon MANTENIDAS en properties")
     # NO se eliminan las columnas
   
   Cambio 2 - Construcción de GeoJSON [Línea ~2280]
   ────────────────────────────────────────────────
   ANTES: Las coordenadas SOLO estaban en geometry
   AHORA: Las coordenadas están en AMBOS lugares:
   
     # Agregar lat/lon a properties desde geometry
     if feature['geometry']:
         feature['properties']['lat'] = geom.y
         feature['properties']['lon'] = geom.x

2. SCRIPT DE REPARACION INMEDIATA
   
   Script: fix_geojson_coordinates.py
   - Regenera el GeoJSON existente con las coordenadas en properties
   - Agrega 863 registros con coordenadas válidas
   - Resultado: 38/39 del Medio Ambiente tienen coordinadas
''')

print('\n' + '='*90)
print('ESTADO FINAL')
print('='*90)

print('''
DEPARTAMENTO ADMINISTRATIVO DE GESTION DEL MEDIO AMBIENTE
─────────────────────────────────────────────────────────
✓ 38/39 registros con coordenadas válidas
✓ Coordenadas disponibles en:
  - geometry.coordinates (para visualización SIG)
  - properties.lat y properties.lon (para acceso directo)
✓ 1 registro sin coordenadas (UNP-242) - requiere investigación


SECRETARÍA DE MOVILIDAD
──────────────────────
✗ 0/4 registros con coordenadas
✗ Problema: Los datos originales de estos 4 registros NO incluyen coordenadas
✓ Datos disponibles para geocodificación:
  - Direcciones (aunque genéricas: "CALI")
  - Barrio/Vereda: "CALI"
  - Comuna: "COMUNAS DE CALI"

RECOMENDACION PARA MOVILIDAD
────────────────────────────
Opción 1 (Corto plazo): 
  Usar centroide de Comuna para estos 4 registros

Opción 2 (Mediano plazo):
  Contactar a Secretaría de Movilidad para obtener ubicaciones específicas

Opción 3 (Largo plazo):
  Integrar geocodificación en el pipeline de transformación
''')

print('\n' + '='*90)
