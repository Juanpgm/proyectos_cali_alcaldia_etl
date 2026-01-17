# -*- coding: utf-8 -*-
"""
Reparar coordenadas en JSON extraído - convertir strings con comas a floats
"""
import json
import pandas as pd

def fix_coordinates_in_json():
    """Repara coordenadas en el JSON extraído"""
    
    # Leer JSON
    json_path = 'cloud_functions/transformation_app/app_inputs/unidades_proyecto_input/unidades_proyecto.json'
    print(f"Leyendo {json_path}...")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"Total registros: {len(data)}")
    
    # Función de conversión
    def convert_coord(value):
        """Convierte coordenada a float, manejando comas"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            value = value.strip().replace(',', '.')
            if value == '' or value.lower() in ['nan', 'none', 'null']:
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        return None
    
    # Reparar coordenadas
    fixed_count = 0
    for record in data:
        lat_orig = record.get('lat')
        lon_orig = record.get('lon')
        
        lat_fixed = convert_coord(lat_orig)
        lon_fixed = convert_coord(lon_orig)
        
        if lat_fixed != lat_orig or lon_fixed != lon_orig:
            record['lat'] = lat_fixed
            record['lon'] = lon_fixed
            fixed_count += 1
    
    print(f"Registros reparados: {fixed_count}")
    
    # Guardar JSON reparado
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"\n[OK] JSON reparado guardado: {json_path}")
    
    # Verificar reparación
    lat_as_string = sum(1 for r in data if isinstance(r.get('lat'), str))
    lat_as_number = sum(1 for r in data if isinstance(r.get('lat'), (int, float)))
    lat_as_none = sum(1 for r in data if r.get('lat') is None)
    
    print(f"\nVerificación:")
    print(f"  lat como string: {lat_as_string}")
    print(f"  lat como número: {lat_as_number}")
    print(f"  lat como None: {lat_as_none}")


if __name__ == '__main__':
    fix_coordinates_in_json()
