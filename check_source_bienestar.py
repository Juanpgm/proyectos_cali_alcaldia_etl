import json

# Read the source GeoJSON
with open('transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto.geojson', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Find features with 'Secretaría de Bienestar Social'
bienestar = [f for f in data['features'] if f['properties'].get('nombre_centro_gestor') == 'Secretaría de Bienestar Social']

print(f'Total features with Secretaría de Bienestar Social: {len(bienestar)}')
print()

# Check first 5 features
for i, feature in enumerate(bienestar[:5]):
    props = feature['properties']
    geom = feature['geometry']
    
    print(f'Feature {i+1}:')
    print(f'  UPID: {props.get("upid")}')
    print(f'  Nombre UP: {props.get("nombre_up")}')
    print(f'  Lat (property): {props.get("lat")}')
    print(f'  Lon (property): {props.get("lon")}')
    print(f'  Has lat column: {"lat" in props}')
    print(f'  Has lon column: {"lon" in props}')
    
    if geom and geom.get('coordinates'):
        coords = geom['coordinates']
        print(f'  Geometry coordinates: {coords}')
    else:
        print(f'  Geometry: None')
    
    print()
