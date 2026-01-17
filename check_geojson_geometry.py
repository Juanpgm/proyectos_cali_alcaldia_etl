# Analizar geometrías en GeoJSON transformado
import json

geojson_path = 'cloud_functions/transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto.geojson'
with open(geojson_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

total = len(data['features'])
con_geometry = sum(1 for f in data['features'] if f.get('geometry') and f['geometry'].get('coordinates'))

print(f"Features con geometry: {con_geometry}/{total} ({con_geometry*100/total:.1f}%)")

# Analizar propiedades lat/lon
con_lat_lon = sum(1 for f in data['features'] if f['properties'].get('lat') is not None and f['properties'].get('lon') is not None)
print(f"Features con lat/lon en properties: {con_lat_lon}/{total} ({con_lat_lon*100/total:.1f}%)")

# Contar los que tienen lat/lon pero NO tienen geometry
lat_lon_sin_geom = sum(1 for f in data['features'] if (f['properties'].get('lat') is not None and f['properties'].get('lon') is not None) and not (f.get('geometry') and f['geometry'].get('coordinates')))
print(f"Features con lat/lon pero SIN geometry: {lat_lon_sin_geom}")

print("\nEjemplos de features con lat/lon pero sin geometry:")
count = 0
for f in data['features']:
    if count >= 5:
        break
    if (f['properties'].get('lat') is not None and f['properties'].get('lon') is not None) and not (f.get('geometry') and f['geometry'].get('coordinates')):
        print(f"\n  UPID: {f['properties'].get('upid')}")
        print(f"  lat: {f['properties'].get('lat')}")
        print(f"  lon: {f['properties'].get('lon')}")
        print(f"  geometry: {f.get('geometry')}")
        count += 1
