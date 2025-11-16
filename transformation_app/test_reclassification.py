# -*- coding: utf-8 -*-
"""
Script para probar la reclasificación de valores de geocoding

Prueba la función reclassify_geocoding_values con el archivo actual
"""

import os
import sys
import pandas as pd
import re
import json
from pathlib import Path
from difflib import SequenceMatcher

def reclassify_geocoding_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reclassify values using reference data from basemaps GeoJSON files.
    """
    if 'barrio_vereda_val_s3' not in df.columns or 'comuna_corregimiento_val_s3' not in df.columns:
        print("⚠️  Warning: Required columns not found, skipping reclassification")
        return df
    
    df = df.copy()
    
    print(f"\n🔄 Loading reference data from basemaps...")
    
    # Load reference data
    try:
        basemaps_dir = Path(__file__).parent.parent / 'basemaps'
        
        # Load barrios/veredas
        barrios_path = basemaps_dir / 'barrios_veredas.geojson'
        with open(barrios_path, 'r', encoding='utf-8') as f:
            barrios_data = json.load(f)
        
        barrios_set = set()
        for feature in barrios_data['features']:
            barrio = feature['properties'].get('barrio_vereda', '')
            if barrio:
                barrios_set.add(barrio.strip())
        
        print(f"   ✓ Loaded {len(barrios_set)} barrios/veredas from reference")
        
        # Load comunas/corregimientos
        comunas_path = basemaps_dir / 'comunas_corregimientos.geojson'
        with open(comunas_path, 'r', encoding='utf-8') as f:
            comunas_data = json.load(f)
        
        comunas_set = set()
        for feature in comunas_data['features']:
            comuna = feature['properties'].get('comuna_corregimiento', '')
            if comuna:
                comunas_set.add(comuna.strip())
        
        print(f"   ✓ Loaded {len(comunas_set)} comunas/corregimientos from reference")
        
    except Exception as e:
        print(f"   ⚠️  Error loading reference data: {e}")
        barrios_set = set()
        comunas_set = set()
    
    # Helper function for fuzzy matching
    def normalize_string(s):
        if not isinstance(s, str):
            return ""
        s = s.upper().strip()
        s = re.sub(r'\s+', ' ', s)
        return s
    
    def find_best_match(value, reference_set, threshold=0.8):
        if not value or not reference_set:
            return None
        
        normalized_value = normalize_string(value)
        
        # Exact match first
        for ref in reference_set:
            if normalize_string(ref) == normalized_value:
                return ref
        
        # Fuzzy match
        best_match = None
        best_ratio = 0
        
        for ref in reference_set:
            ratio = SequenceMatcher(None, normalized_value, normalize_string(ref)).ratio()
            if ratio > best_ratio and ratio >= threshold:
                best_ratio = ratio
                best_match = ref
        
        return best_match
    
    print(f"\n🔄 Analyzing values for reclassification...")
    
    # Show current state before reclassification
    print(f"\n📊 BEFORE reclassification:")
    barrio_vals = df[df['barrio_vereda_val_s3'] != 'ERROR']['barrio_vereda_val_s3'].value_counts().head(10)
    comuna_vals = df[df['comuna_corregimiento_val_s3'] != 'ERROR']['comuna_corregimiento_val_s3'].value_counts().head(10)
    
    print(f"\n   Top 10 barrio_vereda_val_s3 values:")
    for val, count in barrio_vals.items():
        print(f"      {val}: {count}")
    
    print(f"\n   Top 10 comuna_corregimiento_val_s3 values:")
    for val, count in comuna_vals.items():
        print(f"      {val}: {count}")
    
    # Track changes
    changes = []
    reclassified_count = 0
    
    # Iterate through records
    for idx, row in df.iterrows():
        barrio_val = row.get('barrio_vereda_val_s3', 'ERROR')
        comuna_val = row.get('comuna_corregimiento_val_s3', 'ERROR')
        
        # Skip if both are ERROR
        if barrio_val == 'ERROR' and comuna_val == 'ERROR':
            continue
        
        # Skip 'Cali' - it's ambiguous
        if barrio_val == 'Cali' and comuna_val == 'Cali':
            continue
        
        needs_swap = False
        
        # Check if barrio_vereda_val_s3 is actually a comuna/corregimiento
        if barrio_val != 'ERROR' and isinstance(barrio_val, str):
            if find_best_match(barrio_val, comunas_set, threshold=0.85):
                needs_swap = True
        
        # Check if comuna_corregimiento_val_s3 is actually a barrio/vereda
        if comuna_val != 'ERROR' and isinstance(comuna_val, str) and not needs_swap:
            if find_best_match(comuna_val, barrios_set, threshold=0.85):
                needs_swap = True
        
        # Perform swap if needed
        if needs_swap:
            df.at[idx, 'comuna_corregimiento_val_s3'] = barrio_val
            df.at[idx, 'barrio_vereda_val_s3'] = comuna_val
            reclassified_count += 1
            
            changes.append({
                'upid': row.get('upid'),
                'old_barrio': barrio_val,
                'old_comuna': comuna_val,
                'new_barrio': comuna_val,
                'new_comuna': barrio_val
            })
    
    print(f"\n✓ Reclassified {reclassified_count} records")
    
    # Show changes
    if changes:
        print(f"\n📋 Sample of changes (first 10):")
        for i, change in enumerate(changes[:10], 1):
            print(f"\n   {i}. UPID: {change['upid']}")
            print(f"      OLD - Barrio: {change['old_barrio']}, Comuna: {change['old_comuna']}")
            print(f"      NEW - Barrio: {change['new_barrio']}, Comuna: {change['new_comuna']}")
    
    # Show distribution after reclassification
    print(f"\n📊 AFTER reclassification:")
    
    barrio_non_error = (df['barrio_vereda_val_s3'] != 'ERROR').sum()
    comuna_non_error = (df['comuna_corregimiento_val_s3'] != 'ERROR').sum()
    
    print(f"   barrio_vereda_val_s3: {barrio_non_error} valid values")
    print(f"   comuna_corregimiento_val_s3: {comuna_non_error} valid values")
    
    barrio_vals_after = df[df['barrio_vereda_val_s3'] != 'ERROR']['barrio_vereda_val_s3'].value_counts().head(10)
    comuna_vals_after = df[df['comuna_corregimiento_val_s3'] != 'ERROR']['comuna_corregimiento_val_s3'].value_counts().head(10)
    
    print(f"\n   Top 10 barrio_vereda_val_s3 values:")
    for val, count in barrio_vals_after.items():
        print(f"      {val}: {count}")
    
    print(f"\n   Top 10 comuna_corregimiento_val_s3 values:")
    for val, count in comuna_vals_after.items():
        print(f"      {val}: {count}")
    
    return df


if __name__ == '__main__':
    print("="*80)
    print("TESTING GEOCODING RECLASSIFICATION")
    print("="*80)
    
    # Load data
    gdf_path = Path(__file__).parent / 'app_outputs' / 'unidades_proyecto_outputs' / 'gdf_geolocalizar.xlsx'
    
    print(f"\n📂 Loading: {gdf_path}")
    df = pd.read_excel(gdf_path)
    print(f"✓ Loaded {len(df)} records")
    
    # Test reclassification
    df_reclassified = reclassify_geocoding_values(df)
    
    # Save result
    output_path = Path(__file__).parent / 'app_outputs' / 'unidades_proyecto_outputs' / 'gdf_geolocalizar_reclassified.xlsx'
    df_reclassified.to_excel(output_path, index=False, engine='xlsxwriter')
    print(f"\n✓ Saved reclassified data to: {output_path}")
