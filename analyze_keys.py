#!/usr/bin/env python3
"""
Script para identificar claves problemáticas en los datos
"""

import sys
import json
import os
sys.path.append('.')

def analyze_problematic_keys():
    print('=== ANÁLISIS DE CLAVES PROBLEMÁTICAS ===')
    
    # Cargar archivo real
    geojson_path = r'a:\programing_workspace\proyectos_cali_alcaldia_etl\transformation_app\app_outputs\unidades_proyecto_outputs\unidades_proyecto.geojson'
    
    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Analizar las primeras 5 features
    for i in range(min(5, len(data['features']))):
        feature = data['features'][i]
        properties = feature.get('properties', {})
        
        print(f'\n📋 Feature #{i+1}:')
        print(f'  Total properties: {len(properties)}')
        
        # Buscar claves problemáticas
        problematic_keys = []
        for key, value in properties.items():
            # Verificar si la clave es problemática
            if not isinstance(key, str):
                problematic_keys.append(f'Key is not string: {type(key)} -> {key}')
            elif not key or key.isspace():
                problematic_keys.append(f'Key is empty or whitespace: "{key}"')
            elif key == '':
                problematic_keys.append('Key is empty string')
        
        if problematic_keys:
            print(f'  ❌ Problematic keys found:')
            for prob in problematic_keys:
                print(f'    - {prob}')
        else:
            print(f'  ✅ No problematic keys found')
        
        # Mostrar todas las claves para inspección
        print(f'  📄 All keys:')
        for j, key in enumerate(properties.keys()):
            if j < 10:  # Solo mostrar primeras 10
                print(f'    {j+1}. "{key}" ({type(key).__name__}) = {str(properties[key])[:50]}...')
        
        if len(properties) > 10:
            print(f'    ... and {len(properties) - 10} more keys')

if __name__ == "__main__":
    analyze_problematic_keys()