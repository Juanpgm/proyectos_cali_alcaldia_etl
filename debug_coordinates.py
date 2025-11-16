import json

# Límites de Cali
CALI_LAT_MIN = 3.0
CALI_LAT_MAX = 4.0
CALI_LON_MIN = -77.0
CALI_LON_MAX = -76.0

# Leer el GeoJSON de entrada
with open('context/unidades_proyecto_descarga.geojson', 'r', encoding='utf-8') as f:
    data = json.load(f)

print('=== ANÁLISIS DE CALIDAD DE COORDENADAS ===')
print(f'Total features: {len(data["features"])}')

# Contadores de problemas
zeros = 0  # [0, 0]
positive_lon = 0  # Longitud positiva (debería ser negativa)
outside_cali = 0  # Fuera de los límites de Cali
valid = 0  # Coordenadas válidas
total_with_coords = 0

for feature in data['features']:
    geom = feature.get('geometry')
    if geom and geom.get('type') == 'Point':
        coords = geom.get('coordinates')
        if isinstance(coords, list) and len(coords) >= 2:
            total_with_coords += 1
            lon, lat = coords[0], coords[1]
            
            # Verificar problemas
            if lon == 0 and lat == 0:
                zeros += 1
            elif lon > 0:  # Longitud debería ser negativa para Cali
                positive_lon += 1
            elif not (CALI_LAT_MIN <= lat <= CALI_LAT_MAX and CALI_LON_MIN <= lon <= CALI_LON_MAX):
                outside_cali += 1
            else:
                valid += 1

print(f'\n=== RESULTADOS ===')
print(f'Total con coordenadas: {total_with_coords}')
print(f'  ✅ Válidas (dentro de Cali): {valid} ({valid/total_with_coords*100:.1f}%)')
print(f'  ❌ Coordenadas [0, 0]: {zeros} ({zeros/total_with_coords*100:.1f}%)')
print(f'  ❌ Longitud positiva (error de signo): {positive_lon} ({positive_lon/total_with_coords*100:.1f}%)')
print(f'  ❌ Fuera de límites de Cali: {outside_cali} ({outside_cali/total_with_coords*100:.1f}%)')
print(f'\n💡 Total con errores: {zeros + positive_lon + outside_cali} ({(zeros + positive_lon + outside_cali)/total_with_coords*100:.1f}%)')

# Mostrar algunos ejemplos de longitudes positivas
print(f'\n=== EJEMPLOS DE LONGITUDES POSITIVAS (primeros 5) ===')
count = 0
for i, feature in enumerate(data['features']):
    geom = feature.get('geometry')
    if geom and geom.get('type') == 'Point':
        coords = geom.get('coordinates')
        if isinstance(coords, list) and len(coords) >= 2:
            lon, lat = coords[0], coords[1]
            if lon > 0 and lat > 0:
                print(f'Feature {i}: [{lon}, {lat}] -> debería ser [{-lon}, {lat}]')
                count += 1
                if count >= 5:
                    break
