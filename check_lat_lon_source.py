import json

# Verificar UNP-11 en el archivo de descarga
data = json.load(open('context/unidades_proyecto_descarga.geojson', 'r', encoding='utf-8'))
features = [f for f in data['features'] if f['properties'].get('upid') == 'UNP-11']

if features:
    f = features[0]
    props = f['properties']
    print("UNP-11 en archivo de descarga:")
    print(f"  lat: {props.get('lat', 'NO-KEY')}")
    print(f"  lon: {props.get('lon', 'NO-KEY')}")
    print(f"  latitud: {props.get('latitud', 'NO-KEY')}")
    print(f"  longitud: {props.get('longitud', 'NO-KEY')}")
    print(f"  geometry: {f.get('geometry')}")
    print(f"\n  nombre_up: {props.get('nombre_up')}")
else:
    print("UNP-11 NO encontrado en archivo de descarga")

# Verificar varios UPIDs problemáticos
print("\n" + "="*60)
print("Verificando otros UPIDs problemáticos:")
print("="*60)

problem_upids = ['UNP-11', 'UNP-12', 'UNP-21', 'UNP-22', 'UNP-24', 'UNP-25']
for upid in problem_upids:
    features = [f for f in data['features'] if f['properties'].get('upid') == upid]
    if features:
        f = features[0]
        props = f['properties']
        geom = f.get('geometry')
        has_geom = geom is not None and geom != {}
        lat = props.get('lat') or props.get('latitud')
        lon = props.get('lon') or props.get('longitud')
        print(f"\n{upid}:")
        print(f"  lat/lon: {lat}, {lon}")
        print(f"  has geometry: {has_geom}")
        if has_geom:
            print(f"  geometry type: {geom.get('type')}")
