# -*- coding: utf-8 -*-
"""
REPORTE: Problema de Geometrías Faltantes
- Secretaría de Movilidad: Todas tienen [0.0, 0.0] 
- Departamento Administrativo de Gestión del Medio Ambiente: 38/39 OK, 1 con [0.0, 0.0]

CAUSAS IDENTIFICADAS:
1. Las coordenadas lat/lon NUNCA se incluyen en las propiedades del GeoJSON final
2. El código intencionalmenteeliminó las columnas lat/lon antes de exportar (línea 2226-2230)
3. Para Movilidad, los datos originales NO tenían coordenadas válidas
4. Para Medio Ambiente, sí hay coordenadas en geometry pero faltaría una

SOLUCIÓN PROPUESTA:
1. Modificar el export_to_geojson() para mantener lat/lon en properties
2. Verificar por qué Movilidad no tiene coordenadas en los datos de origen
3. Implementar fallback para geocodificar por dirección/barrio si es necesario
"""

import json
from pathlib import Path
import pandas as pd

print('='*90)
print('📋 REPORTE: ANÁLISIS PROBLEMA GEOMETRÍAS')
print('='*90)

# Leer el GeoJSON
geojson_path = Path('test_outputs/capas_epsg4326/geojson/Unidades_Proyecto_EPSG4326.geojson')
with open(geojson_path, 'r', encoding='utf-8') as f:
    geojson_data = json.load(f)

features = geojson_data.get('features', [])

print('\n1️⃣  PROBLEMA IDENTIFICADO')
print('─' * 90)

movilidad_features = [f for f in features if f.get('properties', {}).get('nombre_centro_gestor') == 'Secretaría de Movilidad']
print(f'\nSecretaría de Movilidad:')
print(f'  - Total: {len(movilidad_features)} features')
print(f'  - Coordinates [0,0]: {sum(1 for f in movilidad_features if f.get("geometry", {}).get("coordinates") == [0.0, 0.0])}')
print(f'  - ❌ Ninguno tiene coordenadas reales')

medio_ambiente_features = [f for f in features if f.get('properties', {}).get('nombre_centro_gestor') == 'Departamento Administrativo de Gestión del Medio Ambiente']
print(f'\nDepartamento Administrativo de Gestión del Medio Ambiente:')
print(f'  - Total: {len(medio_ambiente_features)} features')
valid_coords = sum(1 for f in medio_ambiente_features if f.get("geometry", {}).get("coordinates") != [0.0, 0.0])
print(f'  - Con coordenadas válidas: {valid_coords}')
print(f'  - Con [0,0]: {len(medio_ambiente_features) - valid_coords}')
print(f'  - ✅ Mayoría OK, pero falta 1')

print('\n' + '─' * 90)
print('2️⃣  UBICACIÓN DEL PROBLEMA EN EL CÓDIGO')
print('─' * 90)

print('''
Archivo: transformation_app/data_transformation_unidades_proyecto.py
Función: export_to_geojson() (línea ~2185)
Líneas problemáticas: 2226-2230

Código actual (elimina lat/lon):
──────────────────────────────────
    # CRÍTICO: Eliminar columnas lat/lon del DataFrame
    # Las coordenadas SOLO deben estar en geometry, NO en properties
    columns_to_drop = ['lat', 'lon', 'latitud', 'longitud']
    columns_dropped = [col for col in columns_to_drop if col in gdf_export.columns]
    if columns_dropped:
        gdf_export = gdf_export.drop(columns=columns_dropped)

PROBLEMA: Esto elimina la información de las propiedades que debería estar disponible
para recuperación/validación posterior.
''')

print('\n' + '─' * 90)
print('3️⃣  INFORMACIÓN DISPONIBLE EN PROPIEDADES')
print('─' * 90)

# Chequear qué información está disponible
movilidad_sample = movilidad_features[0]
props = movilidad_sample.get('properties', {})

print('\nPropiedades disponibles de Movilidad (ejemplo):')
location_fields = {k: v for k, v in props.items() if any(
    term in k.lower() for term in ['dir', 'barrio', 'comuna', 'ubica', 'sector']
)}

for key, value in location_fields.items():
    print(f'  - {key}: {value}')

print('\n' + '─' * 90)
print('4️⃣  RECOMENDACIÓN DE SOLUCIÓN')
print('─' * 90)

print('''
OPCIÓN 1 (Recomendada - Corto plazo):
──────────────────────────────────────
Modificar export_to_geojson() para mantener lat/lon en propiedades:

    # CAMBIO: NO eliminar lat/lon, incluirlos en las propiedades
    # Esto permite recuperar/validar las coordenadas después
    for idx, row in gdf_export.iterrows():
        geom = row.get('geometry')
        if pd.notna(geom) and hasattr(geom, 'x'):
            feature['properties']['lat'] = geom.y
            feature['properties']['lon'] = geom.x

Ventajas:
  ✓ Fácil de implementar
  ✓ Preserva datos para recuperación
  ✓ Cumple con estándar GeoJSON
  ✓ Permite validación de fuentes

OPCIÓN 2 (Mediano plazo):
─────────────────────────
Investigar por qué Movilidad no tiene coordenadas en datos de origen y:
  - Geocodificar por dirección/barrio
  - Usar centroides de polígonos administrativos
  - Recuperar de fuentes externas (OSM, Google Maps API)

OPCIÓN 3 (Largo plazo):
──────────────────────
Mejorar el pipeline de extracción para validar coordenadas antes de
la transformación y marcar registros problemáticos.
''')

print('\n' + '='*90)
