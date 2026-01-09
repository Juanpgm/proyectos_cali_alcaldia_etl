"""
Debug: Verificar si las coordenadas de Bienestar Social se leyeron correctamente
"""
import json
import pandas as pd

print("=" * 80)
print("VERIFICANDO EXTRACCIÓN DE COORDENADAS - BIENESTAR SOCIAL")
print("=" * 80)
print()

# Leer el archivo JSON extraído
json_path = 'cloud_functions/transformation_app/app_inputs/unidades_proyecto_input/unidades_proyecto.json'
with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Filtrar registros de Bienestar Social
bienestar = [r for r in data if r.get('nombre_centro_gestor') == 'Secretaría de Bienestar Social']

print(f"✅ Encontrados {len(bienestar)} registros de Bienestar Social\n")

if bienestar:
    print("📍 COORDENADAS EXTRAÍDAS:")
    print("-" * 80)
    
    coords_validas = 0
    coords_nulas = 0
    
    for i, record in enumerate(bienestar[:5], 1):  # Mostrar primeros 5
        nombre = record.get('nombre_up', 'N/A')
        lat = record.get('lat')
        lon = record.get('lon')
        
        print(f"{i}. {nombre}")
        print(f"   lat = {lat} (tipo: {type(lat).__name__})")
        print(f"   lon = {lon} (tipo: {type(lon).__name__})")
        
        if lat and lon and isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            coords_validas += 1
            print(f"   ✅ Coordenadas válidas")
        else:
            coords_nulas += 1
            print(f"   ❌ Coordenadas inválidas o nulas")
        print()
    
    if len(bienestar) > 5:
        print(f"... y {len(bienestar) - 5} registros más\n")
    
    # Contar totales
    for record in bienestar:
        lat = record.get('lat')
        lon = record.get('lon')
        if lat and lon and isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            coords_validas += 1
        else:
            coords_nulas += 1
    
    print("=" * 80)
    print("RESUMEN:")
    print("=" * 80)
    print(f"Total registros: {len(bienestar)}")
    print(f"✅ Con coordenadas válidas: {coords_validas}")
    print(f"❌ Sin coordenadas: {coords_nulas}")
