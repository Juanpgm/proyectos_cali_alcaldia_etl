# -*- coding: utf-8 -*-
"""
Script para probar la extracción mejorada de comuna con exploración de componentes

Prueba casos donde se obtiene "Cali" para ver si podemos encontrar Comuna
"""

import os
import sys
import json
from pathlib import Path

# Cargar variables de entorno
try:
    from dotenv import load_dotenv
    project_root = Path(__file__).parent.parent
    env_local = project_root / '.env.local'
    
    if env_local.exists():
        load_dotenv(env_local)
        print(f"✓ Variables de entorno cargadas\n")
except ImportError:
    pass

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import googlemaps

def test_improved_extraction():
    """Probar la extracción mejorada con casos reales."""
    
    print("="*80)
    print("TESTING IMPROVED COMUNA EXTRACTION")
    print("="*80)
    
    # Load gdf_geolocalizar
    gdf_path = Path(__file__).parent / 'app_outputs' / 'unidades_proyecto_outputs' / 'gdf_geolocalizar.xlsx'
    
    print(f"\n📂 Loading: {gdf_path}")
    df = pd.read_excel(gdf_path)
    
    # Filter records where comuna_corregimiento_val_s3 == "Cali"
    cali_records = df[df['comuna_corregimiento_val_s3'] == 'Cali'].copy()
    
    print(f"✓ Found {len(cali_records)} records with comuna='Cali'")
    
    # Initialize Google Maps client
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    if not api_key:
        print("\n❌ GOOGLE_MAPS_API_KEY not configured")
        return
    
    gmaps = googlemaps.Client(key=api_key)
    
    # Test with first 5 records
    test_records = cali_records.head(5)
    
    print(f"\n🔍 Testing improved extraction with {len(test_records)} records...")
    print(f"{'='*80}\n")
    
    for idx, row in test_records.iterrows():
        upid = row['upid']
        nombre = row['nombre_up']
        geometry_str = row['geometry']
        
        # Parse geometry
        try:
            geometry = json.loads(geometry_str)
            coords = geometry['coordinates']
            lon, lat = coords[0], coords[1]
        except:
            print(f"⚠️  {upid}: Could not parse geometry")
            continue
        
        print(f"📍 {upid}: {nombre}")
        print(f"   Coordinates: ({lat:.6f}, {lon:.6f})")
        print(f"   Current: barrio={row['barrio_vereda_val_s3']}, comuna={row['comuna_corregimiento_val_s3']}")
        
        # Get API results
        try:
            results = gmaps.reverse_geocode((lat, lon))
            
            if not results:
                print(f"   ❌ No results from API\n")
                continue
            
            # Show all components
            print(f"\n   📋 Available address components:")
            all_components = []
            
            for result in results:
                for component in result.get('address_components', []):
                    name = component.get('long_name')
                    types = ', '.join(component.get('types', []))
                    
                    # Highlight if contains COMUNA
                    marker = " ⭐" if 'COMUNA' in name.upper() or 'CORREGIMIENTO' in name.upper() else ""
                    
                    entry = f"      {name} ({types}){marker}"
                    if entry not in all_components:
                        all_components.append(entry)
            
            for comp in all_components[:15]:  # Show first 15
                print(comp)
            
            # Apply improved extraction logic
            first_found = None
            comuna_found = None
            
            for result in results:
                address_components = result.get('address_components', [])
                
                for component in address_components:
                    long_name = component.get('long_name', '')
                    types = component.get('types', [])
                    
                    # Check neighborhood
                    if 'neighborhood' in types and not first_found:
                        first_found = long_name
                    
                    # Check for COMUNA/CORREGIMIENTO in name
                    if long_name and isinstance(long_name, str):
                        name_upper = long_name.upper()
                        if ('COMUNA' in name_upper or 'CORREGIMIENTO' in name_upper) and not comuna_found:
                            comuna_found = long_name
            
            print(f"\n   🎯 Extraction results:")
            print(f"      neighborhood type: {first_found}")
            print(f"      COMUNA in name: {comuna_found}")
            
            if first_found == 'Cali' and comuna_found:
                print(f"   ✅ IMPROVED: Would use '{comuna_found}' instead of 'Cali'")
            elif first_found != 'Cali':
                print(f"   ✓ OK: '{first_found}' is not Cali")
            else:
                print(f"   ⚠️  No improvement: Still 'Cali'")
            
            print(f"\n{'-'*80}\n")
            
        except Exception as e:
            print(f"   ❌ Error: {e}\n")
    
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Total records with comuna='Cali': {len(cali_records)}")
    print(f"Sample tested: {len(test_records)}")
    print(f"\nThe improved extraction logic will:")
    print(f"  1. First try 'neighborhood' type")
    print(f"  2. If result is 'Cali', search ALL components for 'COMUNA' or 'CORREGIMIENTO'")
    print(f"  3. Fallback to other administrative levels")

if __name__ == '__main__':
    test_improved_extraction()
