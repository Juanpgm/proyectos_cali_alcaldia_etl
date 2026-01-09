import json

# Read transformed GeoJSON
with open('transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto.geojson', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Filter Bienestar Social
bienestar = [f for f in data['features'] if f['properties'].get('nombre_centro_gestor') == 'Secretaría de Bienestar Social']

print(f'Total Bienestar Social: {len(bienestar)}')
print()

# Check first 3
for i, f in enumerate(bienestar[:3]):
    props = f['properties']
    geom = f.get('geometry')
    
    print(f'{i+1}. UPID: {props.get("upid")}')
    print(f'   Nombre: {props.get("nombre_up")}')
    print(f'   Geometry: {geom}')
    print()
