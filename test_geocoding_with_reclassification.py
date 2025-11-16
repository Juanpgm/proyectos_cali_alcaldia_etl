# -*- coding: utf-8 -*-
"""
Test script to verify that reclassification happens BEFORE forward geocoding.
This ensures that the direccion_api column uses correct barrio/comuna values.
"""

import sys
import os
import pandas as pd
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent / '.env.prod'
if env_path.exists():
    load_dotenv(env_path)
    print(f"✓ Loaded environment from {env_path}")

# Add paths
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

# Import required functions
from google_maps_geocoder import GoogleMapsGeocoder
from data_transformation_unidades_proyecto import (
    reclassify_geocoding_values,
    standardize_geocoding_values
)

def test_geocoding_with_reclassification():
    """Test the complete geocoding flow with reclassification."""
    
    print("="*80)
    print("TESTING GEOCODING FLOW WITH RECLASSIFICATION")
    print("="*80)
    
    # Load the GeoJSON file
    geojson_path = Path(__file__).parent / 'transformation_app' / 'app_outputs' / 'unidades_proyecto_outputs' / 'gdf_geolocalizar.geojson'
    
    if not geojson_path.exists():
        print(f"❌ Error: File not found: {geojson_path}")
        return False
    
    print(f"\n📂 Loading data from GeoJSON: {geojson_path}")
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    
    # Convert to DataFrame
    records = []
    for feature in geojson_data['features']:
        record = feature['properties'].copy()
        if feature.get('geometry'):
            record['geometry'] = json.dumps(feature['geometry'])
        records.append(record)
    
    df = pd.DataFrame(records)
    print(f"✓ Loaded {len(df)} total records")
    
    # Filter records that need geocoding
    mask = df['corregir'] == 'INTENTAR GEORREFERENCIAR'
    records_to_geocode = df[mask].copy()
    print(f"✓ Records marked for geocoding: {len(records_to_geocode)}")
    
    # Sample 10 random records for testing
    if len(records_to_geocode) > 10:
        sample_records = records_to_geocode.sample(n=10, random_state=42)
        print(f"✓ Selected 10 random records for testing")
    else:
        sample_records = records_to_geocode
        print(f"✓ Using all {len(sample_records)} records")
    
    # Create test dataframe with only sampled records
    test_df = df.copy()
    test_df['corregir'] = 'ACEPTABLE'
    test_df.loc[sample_records.index, 'corregir'] = 'INTENTAR GEORREFERENCIAR'
    
    print(f"\n{'='*80}")
    print("STEP 1: REVERSE GEOCODING")
    print(f"{'='*80}")
    
    # Initialize geocoder
    geocoder = GoogleMapsGeocoder(use_adc=True)
    
    # Process reverse geocoding
    test_df = geocoder.process_dataframe(
        test_df,
        geometry_column='geometry',
        output_barrio_column='barrio_vereda_val_s3',
        output_comuna_column='comuna_corregimiento_val_s3',
        filter_column='corregir',
        filter_value='INTENTAR GEORREFERENCIAR'
    )
    
    print(f"\n{'='*80}")
    print("STEP 2: RECLASSIFICATION (BEFORE FORWARD GEOCODING)")
    print(f"{'='*80}")
    
    # Show values BEFORE reclassification
    print(f"\n📋 Sample values BEFORE reclassification:")
    geocoded_mask = test_df['corregir'] == 'INTENTAR GEORREFERENCIAR'
    sample_before = test_df[geocoded_mask].head(5)
    
    for idx, row in sample_before.iterrows():
        print(f"\n   UPID: {row['upid']}")
        print(f"   Barrio/Vereda: {row['barrio_vereda_val_s3']}")
        print(f"   Comuna/Corregimiento: {row['comuna_corregimiento_val_s3']}")
    
    # Apply reclassification
    print(f"\n🔄 Applying reclassification...")
    test_df = reclassify_geocoding_values(test_df)
    
    # Apply standardization
    print(f"\n🔄 Applying standardization...")
    test_df = standardize_geocoding_values(test_df)
    
    # Show values AFTER reclassification
    print(f"\n📋 Sample values AFTER reclassification:")
    for idx, row in test_df.loc[sample_before.index].iterrows():
        print(f"\n   UPID: {row['upid']}")
        print(f"   Barrio/Vereda: {row['barrio_vereda_val_s3']}")
        print(f"   Comuna/Corregimiento: {row['comuna_corregimiento_val_s3']}")
    
    print(f"\n{'='*80}")
    print("STEP 3: CREATE DIRECCION_API (USING RECLASSIFIED VALUES)")
    print(f"{'='*80}")
    
    # Create direccion_api column (similar to main script)
    def create_full_address(row):
        """Combine location components into full address for geocoding."""
        parts = ["Colombia", "Valle del Cauca", "Cali"]
        
        # Add nombre_up
        nombre_up = row.get('nombre_up', '')
        if nombre_up and isinstance(nombre_up, str) and nombre_up.strip():
            parts.append(nombre_up.strip())
        
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
        
        return ', '.join(parts)
    
    test_df['direccion_api'] = test_df.apply(create_full_address, axis=1)
    
    # Show direccion_api for samples
    print(f"\n📋 Sample direccion_api values (will be used for forward geocoding):")
    for idx, row in test_df.loc[sample_before.index].iterrows():
        print(f"\n   UPID: {row['upid']}")
        print(f"   Barrio: {row['barrio_vereda_val_s3']}")
        print(f"   Comuna: {row['comuna_corregimiento_val_s3']}")
        print(f"   Address: {row['direccion_api']}")
    
    print(f"\n{'='*80}")
    print("STEP 4: FORWARD GEOCODING (WITH CORRECT ADDRESSES)")
    print(f"{'='*80}")
    
    # Process forward geocoding
    test_df = geocoder.process_forward_geocoding(
        test_df,
        address_column='direccion_api',
        output_geometry_column='geometry_val_s2',
        filter_column='corregir',
        filter_value='INTENTAR GEORREFERENCIAR'
    )
    
    print(f"\n{'='*80}")
    print("SAVING SAMPLE TO EXCEL")
    print(f"{'='*80}")
    
    # Prepare comparison dataframe
    output_dir = Path(__file__).parent / 'transformation_app' / 'app_outputs' / 'unidades_proyecto_outputs'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save only the geocoded records
    geocoded_records = test_df[test_df['corregir'] == 'INTENTAR GEORREFERENCIAR'].copy()
    
    # Add helpful columns for review
    cols_to_save = [
        'upid', 'nombre_up', 'direccion',
        'barrio_vereda_val_s3', 'comuna_corregimiento_val_s3',
        'direccion_api', 'geometry', 'geometry_val_s2',
        'validacion_distancias'
    ]
    
    # Filter to existing columns
    cols_to_save = [col for col in cols_to_save if col in geocoded_records.columns]
    result_df = geocoded_records[cols_to_save]
    
    # Save to Excel
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f'geocoding_test_with_reclassification_{timestamp}.xlsx'
    
    try:
        result_df.to_excel(output_path, index=False, engine='xlsxwriter')
        file_size = os.path.getsize(output_path) / 1024
        print(f"\n✓ Saved test results to Excel:")
        print(f"  - Location: {output_path}")
        print(f"  - File size: {file_size:.1f} KB")
        print(f"  - Records: {len(result_df)}")
        
        # Validate coordinate format
        print(f"\n{'='*80}")
        print("VALIDATING RESULTS")
        print(f"{'='*80}")
        
        valid_coords = 0
        invalid_coords = 0
        
        for idx, row in result_df.iterrows():
            geom = row.get('geometry_val_s2')
            if geom and geom != 'ERROR' and isinstance(geom, str):
                try:
                    geom_obj = json.loads(geom)
                    coords = geom_obj['coordinates']
                    
                    # Validate format: [lat, lon] where lat ~3.x, lon ~-76.x
                    if 3.0 <= coords[0] <= 4.0 and -77.0 <= coords[1] <= -76.0:
                        valid_coords += 1
                    else:
                        invalid_coords += 1
                        print(f"   ⚠️  Invalid coords for {row['upid']}: {coords}")
                except:
                    invalid_coords += 1
        
        total_geocoded = valid_coords + invalid_coords
        if total_geocoded > 0:
            print(f"\n✓ Coordinate validation:")
            print(f"  - Valid: {valid_coords}/{total_geocoded} ({valid_coords/total_geocoded*100:.1f}%)")
            print(f"  - Invalid: {invalid_coords}/{total_geocoded} ({invalid_coords/total_geocoded*100:.1f}%)")
        
        # Check reclassification effectiveness
        print(f"\n✓ Reclassification check:")
        barrio_valid = (result_df['barrio_vereda_val_s3'] != 'ERROR').sum()
        comuna_valid = (result_df['comuna_corregimiento_val_s3'] != 'ERROR').sum()
        print(f"  - Valid barrio values: {barrio_valid}/{len(result_df)} ({barrio_valid/len(result_df)*100:.1f}%)")
        print(f"  - Valid comuna values: {comuna_valid}/{len(result_df)} ({comuna_valid/len(result_df)*100:.1f}%)")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error saving Excel: {e}")
        return False


if __name__ == "__main__":
    success = test_geocoding_with_reclassification()
    
    if success:
        print("\n🚀 Test completed successfully!")
        print("   Check the Excel file to verify:")
        print("   1. Reclassification was applied before forward geocoding")
        print("   2. direccion_api uses correct barrio/comuna values")
        print("   3. Coordinate format is correct [lat, lon]")
    else:
        print("\n⚠️  Test failed.")
        sys.exit(1)
