"""
Test script for forward geocoding (address → coordinates).
Tests the creation of direccion_api column and forward geocoding functionality.
"""

import sys
import os
from pathlib import Path
import pandas as pd
import json
from dotenv import load_dotenv

# Load environment variables from .env.local or .env.prod
current_dir = Path(__file__).parent
project_root = current_dir.parent
env_local = project_root / '.env.local'
env_prod = project_root / '.env.prod'

if env_local.exists():
    load_dotenv(env_local)
    print(f"✓ Variables de entorno cargadas desde: .env.local")
elif env_prod.exists():
    load_dotenv(env_prod)
    print(f"✓ Variables de entorno cargadas desde: .env.prod")
else:
    print(f"⚠️  Warning: No .env file found")

# Add parent directory to path
sys.path.insert(0, str(project_root))

from utils.google_maps_geocoder import GoogleMapsGeocoder

print("="*80)
print("TESTING FORWARD GEOCODING (Address → Coordinates)")
print("="*80)

# Load data
data_path = current_dir / "app_outputs" / "unidades_proyecto_outputs" / "gdf_geolocalizar.xlsx"
print(f"\n📂 Loading: {data_path}")
df = pd.read_excel(data_path)
print(f"✓ Loaded {len(df)} records")

# Create direccion_api column
print(f"\n{'='*80}")
print("CREATING 'direccion_api' COLUMN")
print("="*80)

def create_full_address(row):
    """Combine location components into full address for geocoding."""
    parts = ["Colombia", "Valle del Cauca", "Cali"]
    
    # Add comuna/corregimiento if available
    comuna = row.get('comuna_corregimiento_val_s3', '')
    if comuna and comuna != 'ERROR' and comuna != 'Cali':
        parts.append(comuna)
    
    # Add barrio/vereda if available
    barrio = row.get('barrio_vereda_val_s3', '')
    if barrio and barrio != 'ERROR' and barrio != 'Cali':
        parts.append(barrio)
    
    # Add direccion if available
    direccion = row.get('direccion', '')
    if direccion and isinstance(direccion, str) and direccion.strip():
        parts.append(direccion.strip())
    
    return ", ".join(parts)

df['direccion_api'] = df.apply(create_full_address, axis=1)

print(f"✓ Created 'direccion_api' column")

# Show statistics
valid_addresses = df[
    (df['direccion_api'].notna()) &
    (df['direccion_api'] != '') &
    (df['direccion_api'] != 'ERROR')
]

print(f"\n📊 Statistics:")
print(f"   Total records: {len(df)}")
print(f"   Valid addresses: {len(valid_addresses)}")
print(f"   Invalid/Empty: {len(df) - len(valid_addresses)}")

# Show sample addresses
print(f"\n📋 Sample addresses (first 10 with complete information):")
sample = valid_addresses[valid_addresses['direccion_api'].str.len() > 50].head(10)
for idx, row in sample.iterrows():
    print(f"\n   UPID: {row.get('upid')}")
    print(f"   Address: {row.get('direccion_api')}")

# Test forward geocoding with 5 records
print(f"\n{'='*80}")
print("TESTING FORWARD GEOCODING (5 records)")
print("="*80)

# Initialize geocoder
geocoder = GoogleMapsGeocoder(use_adc=True)

# Test with first 5 valid addresses
test_df = valid_addresses.head(5).copy()
test_df['geometry_val_s2'] = "ERROR"

success_count = 0
error_count = 0

print(f"\n🔄 Testing {len(test_df)} addresses...")

for idx, row in test_df.iterrows():
    address = row.get('direccion_api')
    
    print(f"\n   Testing: {address}")
    
    coords = geocoder.get_coordinates_from_address(address)
    
    if coords:
        lat, lon = coords
        print(f"   ✓ Found: ({lat}, {lon})")
        
        # Create GeoJSON Point
        geometry_geojson = {
            "type": "Point",
            "coordinates": [lon, lat]
        }
        
        test_df.at[idx, 'geometry_val_s2'] = json.dumps(geometry_geojson)
        success_count += 1
    else:
        print(f"   ✗ Not found")
        error_count += 1

print(f"\n{'='*80}")
print("TEST RESULTS")
print("="*80)
print(f"   Successful: {success_count}/{len(test_df)}")
print(f"   Failed: {error_count}/{len(test_df)}")

# Show results
print(f"\n📋 Results:")
for idx, row in test_df.iterrows():
    print(f"\n   UPID: {row.get('upid')}")
    print(f"   Address: {row.get('direccion_api')}")
    
    geometry = row.get('geometry_val_s2')
    if geometry and geometry != 'ERROR':
        geom_obj = json.loads(geometry)
        coords = geom_obj.get('coordinates', [])
        # GeoJSON format is [lon, lat], display as [lat, lon] for readability
        if len(coords) >= 2:
            print(f"   GeoJSON (stored): {coords} [lon, lat]")
            print(f"   Display format: [{coords[1]}, {coords[0]}] [lat, lon]")
        else:
            print(f"   Coordinates: {coords}")
    else:
        print(f"   Coordinates: ERROR")

print(f"\n{'='*80}")
print("✅ FORWARD GEOCODING TEST COMPLETED")
print("="*80)
print(f"\nTo run on all records, integrate into pipeline:")
print(f"   python data_transformation_unidades_proyecto.py")
