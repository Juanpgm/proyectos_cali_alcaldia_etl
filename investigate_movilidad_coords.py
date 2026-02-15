# -*- coding: utf-8 -*-
"""
Investigar por qué Secretaría de Movilidad no tiene coordenadas válidas
y buscar datos originales
"""

import json
from pathlib import Path

print('='*90)
print('INVESTIGACION: POR QUE SECRETARIA DE MOVILIDAD NO TIENE COORDENADAS')
print('='*90)

# Leer el GeoJSON
geojson_path = Path('test_outputs/capas_epsg4326/geojson/Unidades_Proyecto_EPSG4326.geojson')

with open(geojson_path, 'r', encoding='utf-8') as f:
    geojson_data = json.load(f)

features = geojson_data.get('features', [])

# Obtener registros de Movilidad
movilidad_features = [f for f in features if f.get('properties', {}).get('nombre_centro_gestor') == 'Secretaría de Movilidad']

print(f'\nSecretaría de Movilidad: {len(movilidad_features)} registros\n')

for idx, feature in enumerate(movilidad_features):
    props = feature.get('properties', {})
    print(f'[{idx+1}] UPID: {props.get("upid")}')
    print(f'    Proyecto: {props.get("nombre_up", "Sin nombre")}')
    print(f'    Dirección: {props.get("direccion", "Sin dirección")}')
    print(f'    Barrio/Vereda: {props.get("barrio_vereda", "Sin barrio")}')
    print(f'    Comuna/Corregimiento: {props.get("comuna_corregimiento", "Sin comuna")}')
    print(f'    Tipo intervención: {props.get("tipo_intervencion", "Sin tipo")}')
    print(f'    Coordinates en properties: lat={props.get("lat")}, lon={props.get("lon")}')
    print(f'    Geometry: {feature.get("geometry", {})}')
    
    # Buscar otros campos que podrían tener información de ubicación
    location_fields = {k: v for k, v in props.items() if any(
        term in k.lower() for term in ['ubica', 'sector', 'zona', 'area', 'region', 'punto']
    )}
    if location_fields:
        print(f'    Otros campos de ubicación:')
        for k, v in location_fields.items():
            print(f'      - {k}: {v}')
    print()

# Buscar en datos de entrada
print('='*90)
print('BUSCANDO EN DATOS DE ORIGEN')
print('='*90)

input_paths = [
    Path('app_inputs/unidades_proyecto_input/unidades_proyecto.json'),
    Path('app_inputs/unidades_proyecto_input/raw_extracted_data.json'),
]

for input_path in input_paths:
    if input_path.exists():
        print(f'\nBuscando en: {input_path}')
        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict) and 'records' in data:
                records = data['records']
            else:
                records = []
            
            movilidad_records = [r for r in records if 'Movilidad' in str(r.get('nombre_centro_gestor', ''))]
            print(f'Encontrados {len(movilidad_records)} registros de Movilidad')
            
            if movilidad_records:
                sample = movilidad_records[0]
                print(f'Estructura de registro de muestra:')
                print(f'  Claves principales: {list(sample.keys())[:10]}')
                
                # Buscar campos de coordenadas
                coord_fields = {k: v for k, v in sample.items() if any(
                    term in k.lower() for term in ['lat', 'lon', 'coord', 'x', 'y', 'este', 'norte']
                )}
                if coord_fields:
                    print(f'  Campos de coordenadas encontrados:')
                    for k, v in coord_fields.items():
                        print(f'    - {k}: {v}')
                else:
                    print(f'  NO HAY CAMPOS DE COORDENADAS EN LOS DATOS DE ORIGEN')
        except Exception as e:
            print(f'Error leyendo {input_path}: {e}')

print('\n' + '='*90)
print('CONCLUSION')
print('='*90)
print('''
La Secretaría de Movilidad NO tiene coordenadas en:
1. El GeoJSON final
2. Las propiedades derivadas de geometry
3. Presumiblemente, tampoco en los datos de origen

Opciones:
A) Los datos originales de Movilidad nunca tuvieron coordenadas
B) Se perdieron durante la transformación
C) Necesitan ser geocodificadas desde las direcciones

Recomendación: Verificar directamente en Google Sheets si estos registros tienen
información de ubicación (dirección, barrio) que permita geocodificación.
''')
