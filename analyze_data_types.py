#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze data types in the JSON files
"""

import json
from collections import defaultdict

def analyze_json_file(file_path, sample_size=200):
    """Analyze data types in a JSON file"""
    print(f"\n=== Analyzing {file_path} ===")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='latin-1') as f:
            data = json.load(f)
    
    if not data:
        print("No data found")
        return
    
    print(f"Total records: {len(data)}")
    
    # Analyze data types for first N records
    type_analysis = defaultdict(lambda: defaultdict(int))
    
    sample_data = data[:sample_size]
    print(f"Analyzing first {len(sample_data)} records...")
    
    for record in sample_data:
        for key, value in record.items():
            type_name = type(value).__name__
            type_analysis[key][type_name] += 1
    
    # Print analysis
    for field, types in type_analysis.items():
        print(f"\n{field}:")
        for type_name, count in types.items():
            print(f"  {type_name}: {count}")
            
            # Show some examples for specific fields
            if field in ['programa_presupuestal', 'bpin', 'cod_sector', 'cod_producto']:
                examples = [r.get(field) for r in sample_data if r.get(field) is not None][:5]
                print(f"    Examples: {examples}")
        
        # Highlight mixed types
        if len(types) > 1:
            print(f"  ⚠️  MIXED TYPES!")
            for type_name in types:
                examples = [r.get(field) for r in sample_data if r.get(field) is not None and type(r.get(field)).__name__ == type_name][:3]
                print(f"    {type_name} examples: {examples}")

if __name__ == "__main__":
    analyze_json_file("transformation_app/app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json")