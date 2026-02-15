#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Verificar que los registros de Secretaría de Bienestar Social tengan coordenadas
"""

import json

# Cargar GeoJSON
try:
    with open('app_outputs/unidades_proyecto_transformed.geojson', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print("=" * 80)
    print("ANÁLISIS: Secretaría de Bienestar Social en GeoJSON")
    print("=" * 80)
    
    # Filtrar features de Bienestar Social
    bienestar_features = [
        f for f in data['features'] 
        if f['properties'].get('nombre_centro_gestor') == 'Secretaría de Bienestar Social'
    ]
    
    print(f"\nTotal features Bienestar Social: {len(bienestar_features)}")
    
    # Contar con/sin geometry
    con_geometry = 0
    sin_geometry = 0
    geometry_null = 0
    geometry_zero = 0
    
    for feat in bienestar_features:
        geom = feat.get('geometry')
        if geom is None:
            geometry_null += 1
        elif 'coordinates' in geom:
            coords = geom['coordinates']
            if coords == [0, 0]:
                geometry_zero += 1
            else:
                con_geometry += 1
        else:
            sin_geometry += 1
    
    print(f"\nGeometry válida (no [0,0]): {con_geometry}")
    print(f"Geometry = null: {geometry_null}")
    print(f"Geometry = [0, 0]: {geometry_zero}")
    print(f"Sin geometry field: {sin_geometry}")
    
    # Mostrar algunos ejemplos
    print(f"\n" + "=" * 80)
    print("EJEMPLOS (primeros 5):")
    print("=" * 80)
    
    for i, feat in enumerate(bienestar_features[:5], 1):
        props = feat['properties']
        geom = feat.get('geometry')
        
        print(f"\n{i}. UPID: {props.get('upid')}")
        print(f"   Nombre: {props.get('nombre_up')}")
        print(f"   Geometry: {geom}")
        print(f"   Properties.lat: {props.get('lat')}")
        print(f"   Properties.lon: {props.get('lon')}")
        
        # Verificar intervenciones
        intervenciones = props.get('intervenciones', [])
        if intervenciones:
            interv = intervenciones[0]
            print(f"   Intervención[0].lat: {interv.get('lat')}")
            print(f"   Intervención[0].lon: {interv.get('lon')}")

except FileNotFoundError:
    print("ERROR: No se encuentra el archivo GeoJSON")
    print("El archivo probablemente fue eliminado después de cargarse a S3")
    print("\nEspera a que termine el pipeline y ejecuta este script inmediatamente")
