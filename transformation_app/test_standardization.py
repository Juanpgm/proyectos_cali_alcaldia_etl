"""
Test script for geocoding value standardization.
Tests the standardize_geocoding_values() function using basemaps reference data.
"""

import sys
import os
from pathlib import Path
import pandas as pd
import json
from difflib import SequenceMatcher
import re

# Add parent directory to path
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

print("="*80)
print("TESTING GEOCODING VALUE STANDARDIZATION")
print("="*80)

# Load data
data_path = current_dir / "app_outputs" / "unidades_proyecto_outputs" / "gdf_geolocalizar.xlsx"
print(f"\n📂 Loading: {data_path}")
df = pd.read_excel(data_path)
print(f"✓ Loaded {len(df)} records")

# Load reference data
print(f"\n🔄 Loading reference data from basemaps...")
basemaps_dir = project_root / 'basemaps'

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

# Helper functions
def normalize_string(s):
    """Normalize string for comparison."""
    if not isinstance(s, str):
        return ""
    s = s.upper().strip()
    s = re.sub(r'\s+', ' ', s)
    return s

def find_best_match(value, reference_set, threshold=0.85):
    """Find best match in reference set using fuzzy matching."""
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

# Analyze values BEFORE standardization
print(f"\n{'='*80}")
print("BEFORE STANDARDIZATION")
print("="*80)

geocoded = df[df['barrio_vereda_val_s3'] != 'ERROR'].copy()
print(f"\nTotal geocoded records: {len(geocoded)}")

print(f"\nTop 15 barrio_vereda_val_s3 values:")
barrio_dist = geocoded['barrio_vereda_val_s3'].value_counts()
for val, count in barrio_dist.head(15).items():
    print(f"   {val}: {count}")

print(f"\nTop 15 comuna_corregimiento_val_s3 values:")
comuna_dist = geocoded['comuna_corregimiento_val_s3'].value_counts()
for val, count in comuna_dist.head(15).items():
    print(f"   {val}: {count}")

# Test standardization
print(f"\n{'='*80}")
print("TESTING STANDARDIZATION")
print("="*80)

df_test = df.copy()
barrio_changes = []
comuna_changes = []

for idx, row in df_test.iterrows():
    barrio_val = row.get('barrio_vereda_val_s3', 'ERROR')
    comuna_val = row.get('comuna_corregimiento_val_s3', 'ERROR')
    
    # Test barrio standardization
    if barrio_val != 'ERROR' and isinstance(barrio_val, str):
        standardized_barrio = find_best_match(barrio_val, barrios_set, threshold=0.85)
        if standardized_barrio and standardized_barrio != barrio_val:
            df_test.at[idx, 'barrio_vereda_val_s3'] = standardized_barrio
            barrio_changes.append({
                'upid': row.get('upid'),
                'old': barrio_val,
                'new': standardized_barrio,
                'similarity': SequenceMatcher(None, normalize_string(barrio_val), normalize_string(standardized_barrio)).ratio()
            })
    
    # Test comuna standardization
    if comuna_val != 'ERROR' and isinstance(comuna_val, str):
        standardized_comuna = find_best_match(comuna_val, comunas_set, threshold=0.85)
        if standardized_comuna and standardized_comuna != comuna_val:
            df_test.at[idx, 'comuna_corregimiento_val_s3'] = standardized_comuna
            comuna_changes.append({
                'upid': row.get('upid'),
                'old': comuna_val,
                'new': standardized_comuna,
                'similarity': SequenceMatcher(None, normalize_string(comuna_val), normalize_string(standardized_comuna)).ratio()
            })

print(f"\n✓ Found {len(barrio_changes)} barrio values to standardize")
print(f"✓ Found {len(comuna_changes)} comuna values to standardize")
print(f"✓ Total: {len(barrio_changes) + len(comuna_changes)} values to standardize")

# Show barrio changes
if barrio_changes:
    print(f"\n📋 Barrio standardization changes (first 15):")
    for i, change in enumerate(barrio_changes[:15], 1):
        print(f"   {i}. UPID: {change['upid']} (similarity: {change['similarity']:.2%})")
        print(f"      OLD: {change['old']}")
        print(f"      NEW: {change['new']}")

# Show comuna changes
if comuna_changes:
    print(f"\n📋 Comuna standardization changes (first 15):")
    for i, change in enumerate(comuna_changes[:15], 1):
        print(f"   {i}. UPID: {change['upid']} (similarity: {change['similarity']:.2%})")
        print(f"      OLD: {change['old']}")
        print(f"      NEW: {change['new']}")

# Analyze values AFTER standardization
print(f"\n{'='*80}")
print("AFTER STANDARDIZATION")
print("="*80)

geocoded_after = df_test[df_test['barrio_vereda_val_s3'] != 'ERROR'].copy()
print(f"\nTotal geocoded records: {len(geocoded_after)}")

print(f"\nTop 15 barrio_vereda_val_s3 values:")
barrio_dist_after = geocoded_after['barrio_vereda_val_s3'].value_counts()
for val, count in barrio_dist_after.head(15).items():
    print(f"   {val}: {count}")

print(f"\nTop 15 comuna_corregimiento_val_s3 values:")
comuna_dist_after = geocoded_after['comuna_corregimiento_val_s3'].value_counts()
for val, count in comuna_dist_after.head(15).items():
    print(f"   {val}: {count}")

# Save test results
output_path = current_dir / "app_outputs" / "unidades_proyecto_outputs" / "gdf_geolocalizar_standardized_test.xlsx"
df_test.to_excel(output_path, index=False)
print(f"\n✓ Saved test results to: {output_path}")

print(f"\n{'='*80}")
print("✅ STANDARDIZATION TEST COMPLETED")
print("="*80)
