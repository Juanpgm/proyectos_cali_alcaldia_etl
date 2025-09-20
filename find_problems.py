#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Find specific problematic records
"""

import json

def find_problematic_records():
    print("=== Finding problematic records ===")
    
    try:
        with open("transformation_app/app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json", 'r', encoding='utf-8') as f:
            data = json.load(f)
    except UnicodeDecodeError:
        with open("transformation_app/app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json", 'r', encoding='latin-1') as f:
            data = json.load(f)
    
    print(f"Total records: {len(data)}")
    
    # Find records with string programa_presupuestal
    string_programa = []
    string_bpin = []
    
    for i, record in enumerate(data):
        programa = record.get('programa_presupuestal')
        bpin = record.get('bpin')
        
        if isinstance(programa, str):
            string_programa.append((i, programa))
        
        if isinstance(bpin, str):
            string_bpin.append((i, bpin))
    
    print(f"\nRecords with string programa_presupuestal: {len(string_programa)}")
    for i, (idx, value) in enumerate(string_programa[:10]):
        print(f"  {idx}: {value}")
    
    print(f"\nRecords with string bpin: {len(string_bpin)}")
    for i, (idx, value) in enumerate(string_bpin[:10]):
        print(f"  {idx}: {value}")
    
    # Check the batch ranges that failed
    print("\n=== Checking failed batch ranges ===")
    
    failed_ranges = [(600, 700), (700, 800), (800, 900), (900, 1000), (1000, 1100), (1100, 1200), (1200, 1254)]
    
    for start, end in failed_ranges:
        range_records = data[start:end]
        has_string_programa = any(isinstance(r.get('programa_presupuestal'), str) for r in range_records)
        has_string_bpin = any(isinstance(r.get('bpin'), str) for r in range_records)
        
        print(f"Range {start}-{end}: string programa={has_string_programa}, string bpin={has_string_bpin}")
        
        if has_string_programa:
            examples = [(i+start, r.get('programa_presupuestal')) for i, r in enumerate(range_records) 
                       if isinstance(r.get('programa_presupuestal'), str)][:3]
            print(f"  String programa examples: {examples}")

if __name__ == "__main__":
    find_problematic_records()