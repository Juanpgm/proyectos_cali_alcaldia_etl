import json

# Read the GeoJSON file
with open('a:/programing_workspace/proyectos_cali_alcaldia_etl/test_outputs/capas_epsg4326/geojson/Unidades_Proyecto_EPSG4326.geojson', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Find features with 'Secretaría de Bienestar Social'
bienestar_features = [f for f in data['features'] if f['properties'].get('nombre_centro_gestor') == 'Secretaría de Bienestar Social']

print(f'Total features with Secretaría de Bienestar Social: {len(bienestar_features)}')
print()

# Check first 5 features
for i, feature in enumerate(bienestar_features[:5]):
    props = feature['properties']
    geom = feature['geometry']
    
    print(f'Feature {i+1}:')
    print(f'  UPID: {props.get("upid")}')
    print(f'  Nombre UP: {props.get("nombre_up")}')
    print(f'  Lat (property): {props.get("lat")}')
    print(f'  Lon (property): {props.get("lon")}')
    
    if geom and geom.get('coordinates'):
        coords = geom['coordinates']
        print(f'  Geometry coordinates: {coords}')
        if coords[0] == 0 and coords[1] == 0:
            print(f'  ⚠️ WARNING: Coordinates are 0,0 but properties show lat={props.get("lat")}, lon={props.get("lon")}')
    else:
        print(f'  Geometry: None')
        print(f'  ⚠️ WARNING: No geometry but properties show lat={props.get("lat")}, lon={props.get("lon")}')
    print()
