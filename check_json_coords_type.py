import json

# Leer JSON
with open('cloud_functions/transformation_app/app_inputs/unidades_proyecto_input/unidades_proyecto.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Verificar tipos de lat/lon
lat_as_string = sum(1 for r in data if isinstance(r.get('lat'), str))
lat_as_number = sum(1 for r in data if isinstance(r.get('lat'), (int, float)))
lat_as_none = sum(1 for r in data if r.get('lat') is None)

print(f"Total registros: {len(data)}")
print(f"lat como string: {lat_as_string}")
print(f"lat como número: {lat_as_number}")
print(f"lat como None: {lat_as_none}")

# Buscar strings con comas
coords_str_coma = [r for r in data if isinstance(r.get('lat'), str) and ',' in r.get('lat', '')]
print(f"\nlat como string CON coma: {len(coords_str_coma)}")

if coords_str_coma:
    print(f"\nEjemplos:")
    for i, r in enumerate(coords_str_coma[:5]):
        print(f"  {i+1}. lat: {r['lat']}, lon: {r['lon']}, centro: {r.get('nombre_centro_gestor', 'N/A')}")
