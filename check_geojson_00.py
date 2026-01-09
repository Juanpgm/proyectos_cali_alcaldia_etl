# -*- coding: utf-8 -*-
"""
Verificar coordenadas 0,0 en GeoJSON de test_outputs.
"""

import json
from pathlib import Path

geojson_path = Path('test_outputs/capas_epsg4326/geojson/Unidades_Proyecto_EPSG4326.geojson')

with open(geojson_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

features = data['features']
coords_00 = [f for f in features if f.get('geometry') and f['geometry'].get('coordinates') == [0, 0]]

print(f"Total features: {len(features)}")
print(f"Registros con 0,0: {len(coords_00)}")

if coords_00:
    print("\nPrimeros 10 registros con 0,0:")
    for f in coords_00[:10]:
        props = f['properties']
        nombre = props.get('NOMBRE_UP', props.get('nombre_up', 'Sin nombre'))
        upid = props.get('UPID', props.get('upid', 'N/A'))
        lat = props.get('LAT', props.get('lat', 'N/A'))
        lon = props.get('LON', props.get('lon', 'N/A'))
        print(f"  - {nombre[:50]} | UPID: {upid} | lat: {lat} | lon: {lon}")
