# -*- coding: utf-8 -*-
"""
Script para regenerar el GeoJSON con coordenadas en properties.
Acceso directo sin problemas de encoding.
"""

import json
from pathlib import Path

print('Regenerando GeoJSON con coordenadas en properties...')

# Leer el GeoJSON actual
input_path = Path('test_outputs/capas_epsg4326/geojson/Unidades_Proyecto_EPSG4326.geojson')

with open(input_path, 'r', encoding='utf-8') as f:
    geojson_data = json.load(f)

features = geojson_data.get('features', [])
print(f'Procesando {len(features)} features...')

# Agregar coordenadas a propiedades
fixed_count = 0

for feature in features:
    geom = feature.get('geometry', {})
    geom_type = geom.get('type')
    coords = geom.get('coordinates', [])
    
    # Solo procesar Point geometries (no LineString, etc.)
    if geom_type == 'Point' and isinstance(coords, list) and len(coords) == 2:
        # Verificar que sean números
        try:
            lon = float(coords[0])
            lat = float(coords[1])
            
            # Si hay una geometría válida con coordenadas
            if coords != [0.0, 0.0]:
                # Validar rangos
                if -77.5 <= lon <= -75.5 and 2.5 <= lat <= 4.5:
                    # Agregar lat/lon a properties
                    if 'lat' not in feature['properties']:
                        feature['properties']['lat'] = lat
                        feature['properties']['lon'] = lon
                        fixed_count += 1
        except (TypeError, ValueError):
            pass  # Skip if not numeric

print(f'Coordenadas agregadas: {fixed_count}')

# Guardar el archivo mejorado
output_path = Path('test_outputs/capas_epsg4326/geojson/Unidades_Proyecto_EPSG4326.geojson')

with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(geojson_data, f, indent=2, ensure_ascii=False)

print(f'Archivo guardado en: {output_path}')

# Verificar resultado
print('\nVerificacion:')
movilidad = [f for f in features if f.get('properties', {}).get('nombre_centro_gestor') == 'Secretaría de Movilidad']
medio_ambiente = [f for f in features if f.get('properties', {}).get('nombre_centro_gestor') == 'Departamento Administrativo de Gestión del Medio Ambiente']

print(f'Secretaría de Movilidad: {len(movilidad)} registros')
if movilidad:
    sample = movilidad[0]
    print(f'  - Ejemplo properties tiene "lat"?: {"lat" in sample.get("properties", {})}')
    print(f'  - Ejemplo geometry: {sample.get("geometry", {}).get("coordinates", [])}')

print(f'\nDepartamento Administrativo de Gestión del Medio Ambiente: {len(medio_ambiente)} registros')
if medio_ambiente:
    sample = medio_ambiente[0]
    print(f'  - Ejemplo properties tiene "lat"?: {"lat" in sample.get("properties", {})}')
    lat_props = sample.get("properties", {}).get("lat")
    lon_props = sample.get("properties", {}).get("lon")
    print(f'  - Ejemplo lat/lon en properties: ({lat_props}, {lon_props})')
    print(f'  - Ejemplo geometry: {sample.get("geometry", {}).get("coordinates", [])}')

print('\n✓ Regeneración completada')
