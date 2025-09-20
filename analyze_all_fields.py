import json
import sys

# Cargar datos PAA
with open('transformation_app/app_outputs/paa_dacp/paa_dacp.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Analizar todos los campos string por tamaño
field_analysis = {}

for record in data:
    for field, value in record.items():
        if isinstance(value, str) and value is not None:
            length = len(value)
            if field not in field_analysis:
                field_analysis[field] = {'max_length': 0, 'values_over_10': [], 'values_over_20': [], 'values_over_50': [], 'values_over_100': []}
            
            if length > field_analysis[field]['max_length']:
                field_analysis[field]['max_length'] = length
            
            if length > 10:
                field_analysis[field]['values_over_10'].append((value[:50] + '...' if len(value) > 50 else value, length))
            if length > 20:
                field_analysis[field]['values_over_20'].append((value[:50] + '...' if len(value) > 50 else value, length))
            if length > 50:
                field_analysis[field]['values_over_50'].append((value[:50] + '...' if len(value) > 50 else value, length))
            if length > 100:
                field_analysis[field]['values_over_100'].append((value[:50] + '...' if len(value) > 50 else value, length))

print('=== ANÁLISIS COMPLETO DE CAMPOS STRING ===')
for field, analysis in sorted(field_analysis.items()):
    max_len = analysis['max_length']
    print(f'\n{field}: max_length={max_len}')
    
    if max_len > 10:
        print(f'  > VARCHAR(10): {len(analysis["values_over_10"])} valores exceden')
        if analysis['values_over_10']:
            # Mostrar algunos ejemplos
            for val, length in analysis['values_over_10'][:3]:
                print(f'    - "{val}" (len={length})')
    
    if max_len > 20:
        print(f'  > VARCHAR(20): {len(analysis["values_over_20"])} valores exceden')
    
    if max_len > 50:
        print(f'  > VARCHAR(50): {len(analysis["values_over_50"])} valores exceden')
    
    if max_len > 100:
        print(f'  > VARCHAR(100): {len(analysis["values_over_100"])} valores exceden')
        if analysis['values_over_100']:
            for val, length in analysis['values_over_100'][:2]:
                print(f'    - "{val}" (len={length})')