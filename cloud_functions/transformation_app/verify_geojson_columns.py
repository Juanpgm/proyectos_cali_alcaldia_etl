import json

# Cargar el GeoJSON
with open('app_outputs/unidades_proyecto_outputs/unidades_proyecto.geojson', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Obtener propiedades del primer feature
props = data['features'][0]['properties']

print('='*60)
print('VERIFICACIÓN DE COLUMNAS EN GEOJSON')
print('='*60)

print('\nColumnas en el GeoJSON (ordenadas alfabéticamente):')
for k in sorted(props.keys()):
    print(f'  - {k}')

print(f'\nTotal: {len(props)} columnas')

print('\n' + '='*60)
print('VERIFICANDO COLUMNAS TEMPORALES (NO DEBERÍAN ESTAR)')
print('='*60)
temporal_cols = {
    'geometry_type': 'geometry_type' in props,
    'geometry_bounds': 'geometry_bounds' in props,
    'processed_timestamp': 'processed_timestamp' in props,
    'longitude': 'longitude' in props,
    'latitude': 'latitude' in props,
    'geometry_json': 'geometry_json' in props,
    'microtio': 'microtio' in props,
    'dataframe': 'dataframe' in props
}

for col, exists in temporal_cols.items():
    status = '❌ PRESENTE (ERROR)' if exists else '✅ NO PRESENTE (OK)'
    print(f'  {col}: {status}')

print('\n' + '='*60)
print('VERIFICANDO COLUMNAS IMPORTANTES')
print('='*60)
important_cols = {
    'bpin': 'bpin' in props,
    'nombre_up': 'nombre_up' in props,
    'upid': 'upid' in props,
    'referencia_proceso': 'referencia_proceso' in props,
    'presupuesto_base': 'presupuesto_base' in props
}

for col, exists in important_cols.items():
    status = '✅ PRESENTE (OK)' if exists else '❌ NO PRESENTE (ERROR)'
    value = props.get(col, 'N/A')
    print(f'  {col}: {status} - Valor: {value}')

# Contar features con BPIN válido
bpin_count = sum(1 for f in data['features'] if f['properties'].get('bpin') is not None)
print(f'\n✅ Features con BPIN válido: {bpin_count}/{len(data["features"])}')

print('\n' + '='*60)
print('✅ VERIFICACIÓN COMPLETADA')
print('='*60)
