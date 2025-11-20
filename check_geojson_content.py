import json

with open('context/unidades_proyecto.geojson', encoding='utf-8') as f:
    data = json.load(f)

print(f"Total features: {len(data['features'])}")

tipos = set([feat['properties'].get('tipo_equipamiento','N/A') for feat in data['features']])
print(f"Tipos: {tipos}")

geometries = set([feat['geometry']['type'] for feat in data['features']])
print(f"Geometries: {geometries}")

estados = set([feat['properties'].get('estado','N/A') for feat in data['features']])
print(f"Estados únicos: {sorted([e for e in estados if e is not None])}")

# Mostrar sample
if data['features']:
    print(f"\nSample feature properties:")
    sample = data['features'][0]['properties']
    for key in ['nombre_up', 'estado', 'tipo_equipamiento', 'avance_obra', 'identificador']:
        print(f"  {key}: {sample.get(key, 'N/A')}")
