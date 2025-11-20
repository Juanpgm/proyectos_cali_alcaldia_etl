"""
Test script for distance validation between geometry and geometry_val_s2.
Tests the Haversine distance calculation.
"""

import sys
import os
from pathlib import Path
import pandas as pd
import json
from math import radians, cos, sin, asin, sqrt

# Add parent directory to path
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

print("="*80)
print("TESTING DISTANCE VALIDATION")
print("="*80)

# Load data
data_path = current_dir / "app_outputs" / "unidades_proyecto_outputs" / "gdf_geolocalizar.xlsx"
print(f"\n📂 Loading: {data_path}")
df = pd.read_excel(data_path)
print(f"✓ Loaded {len(df)} records")

# Haversine distance function
def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees) in meters.
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    
    # Radius of earth in meters
    r = 6371000
    
    return c * r

def calculate_distance_between_geometries(row):
    """Calculate distance between geometry and geometry_val_s2."""
    geom1 = row.get('geometry')
    geom2 = row.get('geometry_val_s2')
    
    # If either is ERROR or empty, return None
    if not geom1 or geom1 == 'ERROR' or not geom2 or geom2 == 'ERROR':
        return None
    
    try:
        # Parse both geometries
        if isinstance(geom1, str):
            geom1_obj = json.loads(geom1)
        else:
            geom1_obj = geom1
        
        if isinstance(geom2, str):
            geom2_obj = json.loads(geom2)
        else:
            geom2_obj = geom2
        
        # Extract coordinates [lon, lat]
        coords1 = geom1_obj.get('coordinates', [])
        coords2 = geom2_obj.get('coordinates', [])
        
        if len(coords1) < 2 or len(coords2) < 2:
            return None
        
        lon1, lat1 = coords1[0], coords1[1]
        lon2, lat2 = coords2[0], coords2[1]
        
        # Calculate distance using Haversine
        distance_meters = haversine_distance(lat1, lon1, lat2, lon2)
        
        return round(distance_meters, 2)
        
    except (json.JSONDecodeError, ValueError, TypeError, KeyError):
        return None

# Calculate distances
print(f"\n{'='*80}")
print("CALCULATING DISTANCES")
print("="*80)

print(f"\n🔄 Calculating distances for all records...")
df['validacion_distancias'] = df.apply(calculate_distance_between_geometries, axis=1)

# Statistics
valid_distances = df['validacion_distancias'].notna()
distance_count = valid_distances.sum()

print(f"\n📊 Results:")
print(f"   Total records: {len(df)}")
print(f"   Valid comparisons: {distance_count}")
print(f"   No comparison available: {len(df) - distance_count}")

if distance_count > 0:
    distances = df[valid_distances]['validacion_distancias']
    
    print(f"\n📏 Distance statistics:")
    print(f"   Min distance: {distances.min():.2f} meters")
    print(f"   Max distance: {distances.max():.2f} meters")
    print(f"   Mean distance: {distances.mean():.2f} meters")
    print(f"   Median distance: {distances.median():.2f} meters")
    print(f"   Std deviation: {distances.std():.2f} meters")
    
    # Distance ranges
    under_100m = (distances < 100).sum()
    range_100_500m = ((distances >= 100) & (distances < 500)).sum()
    range_500_1000m = ((distances >= 500) & (distances < 1000)).sum()
    over_1000m = (distances >= 1000).sum()
    
    print(f"\n   Distance ranges:")
    print(f"   - < 100m: {under_100m} ({under_100m/distance_count*100:.1f}%)")
    print(f"   - 100-500m: {range_100_500m} ({range_100_500m/distance_count*100:.1f}%)")
    print(f"   - 500-1000m: {range_500_1000m} ({range_500_1000m/distance_count*100:.1f}%)")
    print(f"   - > 1000m: {over_1000m} ({over_1000m/distance_count*100:.1f}%)")
    
    # Show sample of records with distances
    print(f"\n📋 Sample records with calculated distances (first 10):")
    sample = df[valid_distances][['upid', 'nombre_up', 'validacion_distancias']].head(10)
    for idx, row in sample.iterrows():
        print(f"   {row['upid']}: {row['validacion_distancias']:.2f}m - {row['nombre_up']}")
    
    # Show records with largest distances
    print(f"\n⚠️  Records with largest distances (top 10):")
    largest = df[valid_distances].nlargest(10, 'validacion_distancias')[['upid', 'nombre_up', 'validacion_distancias', 'direccion']]
    for idx, row in largest.iterrows():
        print(f"   {row['upid']}: {row['validacion_distancias']:.2f}m")
        print(f"      {row['nombre_up']}")
        print(f"      {row.get('direccion', 'N/A')}")
    
    # Show records with smallest distances (best matches)
    print(f"\n✅ Records with smallest distances (top 10 - best matches):")
    smallest = df[valid_distances].nsmallest(10, 'validacion_distancias')[['upid', 'nombre_up', 'validacion_distancias', 'direccion']]
    for idx, row in smallest.iterrows():
        print(f"   {row['upid']}: {row['validacion_distancias']:.2f}m")
        print(f"      {row['nombre_up']}")
        print(f"      {row.get('direccion', 'N/A')}")

# Save test results
output_path = current_dir / "app_outputs" / "unidades_proyecto_outputs" / "gdf_geolocalizar_with_distances.xlsx"
df.to_excel(output_path, index=False)
print(f"\n✓ Saved results with distances to: {output_path}")

print(f"\n{'='*80}")
print("✅ DISTANCE VALIDATION TEST COMPLETED")
print("="*80)
