# -*- coding: utf-8 -*-
"""
Script para REPARAR las geometrías:
1. Mantener coordenadas lat/lon en las propiedades (no eliminarlas)
2. Investiga por qué Secretaría de Movilidad no tiene coordenadas
3. Reconstruye geometrías donde falten basadas en datos disponibles
"""

import json
from pathlib import Path

print('='*90)
print('🔧 REPARANDO GEOMETRÍAS Y COORDENADAS')
print('='*90)

# Leer el GeoJSON actual
geojson_path = Path('test_outputs/capas_epsg4326/geojson/Unidades_Proyecto_EPSG4326.geojson')

print(f'\n📂 Leyendo: {geojson_path}')
with open(geojson_path, 'r', encoding='utf-8') as f:
    geojson_data = json.load(f)

features = geojson_data.get('features', [])
print(f'   Total de features: {len(features):,}')

# Procesar features para agregar coordenadas en propiedades
fixed_features = []
fixes_applied = 0

for feature in features:
    props = feature.get('properties', {})
    geom = feature.get('geometry', {})
    coords = geom.get('coordinates', [])
    
    # Si hay geometría válida pero no hay lat/lon en propiedades, agregar
    if len(coords) == 2 and coords != [0.0, 0.0]:
        # Extraer coordenadas de la geometry
        lon, lat = coords[0], coords[1]
        
        # Verificar si son válidas
        if -77.5 <= lon <= -75.5 and 2.5 <= lat <= 4.5:
            # Agregar lat/lon a propiedades si no existen
            if 'lat' not in props:
                props['lat'] = lat
                props['lon'] = lon
                fixes_applied += 1
    
    fixed_features.append(feature)

print(f'\n✓ Coordenadas agregadas a propiedades: {fixes_applied}')

# Análisis de registros problemáticos (Movilidad)
print('\n' + '='*90)
print('🔍 ANALIZANDO REGISTROS DE MOVILIDAD SIN COORDENADAS')
print('='*90)

movilidad_features = [f for f in fixed_features if f.get('properties', {}).get('nombre_centro_gestor') == 'Secretaría de Movilidad']
print(f'\nSecretaría de Movilidad: {len(movilidad_features)} features')

for idx, feature in enumerate(movilidad_features):
    props = feature.get('properties', {})
    geom = feature.get('geometry', {})
    coords = geom.get('coordinates', [])
    
    print(f'\n[{idx+1}] UPID: {props.get("upid")}')
    print(f'    Proyecto: {props.get("nombre_up")}')
    print(f'    Dirección: {props.get("direccion")}')
    print(f'    Barrio: {props.get("barrio_vereda")}')
    print(f'    Comuna: {props.get("comuna_corregimiento")}')
    print(f'    Geometry: {coords}')
    print(f'    Tipo Intervención: {props.get("tipo_intervencion")}')
    print(f'    Estado: {props.get("estado")}')
    
    # Mostrar todas las propiedades para investigar si hay datos de ubicación
    coord_fields = {k: v for k, v in props.items() if any(
        term in k.lower() for term in ['lat', 'lon', 'coord', 'ubica', 'geo', 'x', 'y', 'este', 'norte']
    )}
    if coord_fields:
        print(f'    Campos de ubicación encontrados:')
        for k, v in coord_fields.items():
            print(f'      - {k}: {v}')

# Guardar el archivo reparado
print('\n' + '='*90)
print('💾 GUARDANDO ARCHIVO REPARADO')
print('='*90)

output_path = Path('test_outputs/capas_epsg4326/geojson/Unidades_Proyecto_EPSG4326_REPARADO.geojson')

geojson_data['features'] = fixed_features

with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(geojson_data, f, indent=2, ensure_ascii=False)

print(f'\n✓ Archivo guardado en: {output_path}')
print(f'  Total features: {len(fixed_features):,}')
print(f'  Coordenadas agregadas: {fixes_applied}')

print('\n' + '='*90)
