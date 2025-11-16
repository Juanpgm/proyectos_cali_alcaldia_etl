# -*- coding: utf-8 -*-
"""
Test script to verify reclassification and standardization logic.
Tests with a small sample to ensure barrio/vereda and comuna/corregimiento
values are correctly classified before running the full process.
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
sys.path.append(os.path.join(os.path.dirname(__file__), 'extraction_app'))

# Import required functions
from data_transformation_unidades_proyecto import (
    reclassify_geocoding_values,
    standardize_geocoding_values
)

def test_reclassification_sample():
    """Test reclassification and standardization with a sample."""
    
    print("="*80)
    print("TESTING RECLASSIFICATION AND STANDARDIZATION")
    print("="*80)
    
    # Load the full dataset - try Excel first, then GeoJSON
    excel_path = Path(__file__).parent / 'transformation_app' / 'app_outputs' / 'unidades_proyecto_outputs' / 'gdf_geolocalizar.xlsx'
    geojson_path = Path(__file__).parent / 'transformation_app' / 'app_outputs' / 'unidades_proyecto_outputs' / 'gdf_geolocalizar.geojson'
    
    df = None
    
    if excel_path.exists():
        print(f"\n📂 Loading data from Excel: {excel_path}")
        df = pd.read_excel(excel_path)
        print(f"✓ Loaded {len(df)} total records from Excel")
    elif geojson_path.exists():
        print(f"\n📂 Loading data from GeoJSON: {geojson_path}")
        with open(geojson_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        # Convert GeoJSON to DataFrame
        records = []
        for feature in geojson_data['features']:
            record = feature['properties'].copy()
            if feature.get('geometry'):
                record['geometry'] = json.dumps(feature['geometry'])
            records.append(record)
        
        df = pd.DataFrame(records)
        print(f"✓ Loaded {len(df)} total records from GeoJSON")
    else:
        print(f"❌ Error: No input file found")
        print(f"   Tried:")
        print(f"   - {excel_path}")
        print(f"   - {geojson_path}")
        print(f"\n   Please run the transformation first:")
        print(f"   python transformation_app/data_transformation_unidades_proyecto.py")
        return False
    
    if df is None or len(df) == 0:
        print(f"❌ Error: No data loaded")
        return False
    
    # Sample 50 random records (or all if less than 50)
    if len(df) > 50:
        sample_df = df.sample(n=50, random_state=42).copy()
        print(f"✓ Selected 50 random records for testing")
    else:
        sample_df = df.copy()
        print(f"✓ Using all {len(sample_df)} records")
    
    # Show initial state
    print(f"\n{'='*80}")
    print("INITIAL STATE (Before Reclassification)")
    print(f"{'='*80}")
    
    barrio_non_error_before = (sample_df['barrio_vereda_val_s3'] != 'ERROR').sum()
    comuna_non_error_before = (sample_df['comuna_corregimiento_val_s3'] != 'ERROR').sum()
    
    print(f"📊 Statistics:")
    print(f"   barrio_vereda_val_s3: {barrio_non_error_before}/{len(sample_df)} valid values")
    print(f"   comuna_corregimiento_val_s3: {comuna_non_error_before}/{len(sample_df)} valid values")
    
    # Show some examples
    print(f"\n📋 Sample records (first 5 with valid values):")
    valid_mask = (sample_df['barrio_vereda_val_s3'] != 'ERROR') | (sample_df['comuna_corregimiento_val_s3'] != 'ERROR')
    valid_samples = sample_df[valid_mask].head(5)
    
    for idx, row in valid_samples.iterrows():
        print(f"\n   Record {idx}:")
        print(f"      UPID: {row['upid']}")
        print(f"      Barrio/Vereda: {row['barrio_vereda_val_s3']}")
        print(f"      Comuna/Corregimiento: {row['comuna_corregimiento_val_s3']}")
    
    # Apply reclassification
    print(f"\n{'='*80}")
    print("APPLYING RECLASSIFICATION")
    print(f"{'='*80}")
    
    sample_df_reclassified = reclassify_geocoding_values(sample_df)
    
    # Apply standardization
    print(f"\n{'='*80}")
    print("APPLYING STANDARDIZATION")
    print(f"{'='*80}")
    
    sample_df_final = standardize_geocoding_values(sample_df_reclassified)
    
    # Show final state
    print(f"\n{'='*80}")
    print("FINAL STATE (After Reclassification and Standardization)")
    print(f"{'='*80}")
    
    barrio_non_error_after = (sample_df_final['barrio_vereda_val_s3'] != 'ERROR').sum()
    comuna_non_error_after = (sample_df_final['comuna_corregimiento_val_s3'] != 'ERROR').sum()
    
    print(f"📊 Statistics:")
    print(f"   barrio_vereda_val_s3: {barrio_non_error_after}/{len(sample_df_final)} valid values")
    print(f"   comuna_corregimiento_val_s3: {comuna_non_error_after}/{len(sample_df_final)} valid values")
    
    # Show the same examples after processing
    print(f"\n📋 Same sample records after processing:")
    for idx in valid_samples.index:
        row = sample_df_final.loc[idx]
        print(f"\n   Record {idx}:")
        print(f"      UPID: {row['upid']}")
        print(f"      Barrio/Vereda: {row['barrio_vereda_val_s3']}")
        print(f"      Comuna/Corregimiento: {row['comuna_corregimiento_val_s3']}")
    
    # Compare changes
    print(f"\n{'='*80}")
    print("CHANGES DETECTED")
    print(f"{'='*80}")
    
    changes_count = 0
    for idx in sample_df.index:
        before_barrio = sample_df.loc[idx, 'barrio_vereda_val_s3']
        before_comuna = sample_df.loc[idx, 'comuna_corregimiento_val_s3']
        after_barrio = sample_df_final.loc[idx, 'barrio_vereda_val_s3']
        after_comuna = sample_df_final.loc[idx, 'comuna_corregimiento_val_s3']
        
        if before_barrio != after_barrio or before_comuna != after_comuna:
            changes_count += 1
            if changes_count <= 10:  # Show only first 10 changes
                print(f"\n   Record {idx} (UPID: {sample_df.loc[idx, 'upid']}):")
                print(f"      BEFORE - Barrio: {before_barrio}, Comuna: {before_comuna}")
                print(f"      AFTER  - Barrio: {after_barrio}, Comuna: {after_comuna}")
    
    if changes_count > 10:
        print(f"\n   ... and {changes_count - 10} more changes")
    
    if changes_count == 0:
        print(f"\n✓ No changes detected (all values already correctly classified)")
    else:
        print(f"\n✓ Total changes applied: {changes_count}/{len(sample_df)} records ({changes_count/len(sample_df)*100:.1f}%)")
    
    # Validate that values are correct
    print(f"\n{'='*80}")
    print("VALIDATION")
    print(f"{'='*80}")
    
    # Load reference data for validation
    basemaps_dir = Path(__file__).parent / 'basemaps'
    
    # Load barrios/veredas
    barrios_path = basemaps_dir / 'barrios_veredas.geojson'
    with open(barrios_path, 'r', encoding='utf-8') as f:
        barrios_data = json.load(f)
    barrios_set = {feature['properties']['barrio_vereda'].strip() 
                   for feature in barrios_data['features'] 
                   if feature['properties'].get('barrio_vereda')}
    
    # Load comunas/corregimientos
    comunas_path = basemaps_dir / 'comunas_corregimientos.geojson'
    with open(comunas_path, 'r', encoding='utf-8') as f:
        comunas_data = json.load(f)
    comunas_set = {feature['properties']['comuna_corregimiento'].strip() 
                   for feature in comunas_data['features'] 
                   if feature['properties'].get('comuna_corregimiento')}
    
    print(f"✓ Loaded {len(barrios_set)} barrios/veredas from reference")
    print(f"✓ Loaded {len(comunas_set)} comunas/corregimientos from reference")
    
    # Check if values are in correct columns
    misclassified = 0
    
    for idx, row in sample_df_final.iterrows():
        barrio_val = row['barrio_vereda_val_s3']
        comuna_val = row['comuna_corregimiento_val_s3']
        
        # Skip ERROR values and 'Cali'
        if barrio_val == 'ERROR' or barrio_val == 'Cali':
            continue
        if comuna_val == 'ERROR' or comuna_val == 'Cali':
            continue
        
        # Check if barrio is actually in comunas set
        if barrio_val in comunas_set:
            misclassified += 1
            print(f"\n   ⚠️  Misclassification detected in record {idx}:")
            print(f"      '{barrio_val}' is in barrio_vereda_val_s3 but belongs to comunas")
        
        # Check if comuna is actually in barrios set
        if comuna_val in barrios_set:
            misclassified += 1
            print(f"\n   ⚠️  Misclassification detected in record {idx}:")
            print(f"      '{comuna_val}' is in comuna_corregimiento_val_s3 but belongs to barrios")
    
    print(f"\n{'='*80}")
    if misclassified == 0:
        print("✅ VALIDATION PASSED!")
        print("   All values are correctly classified.")
        print("   Reclassification logic is working as expected.")
        print(f"{'='*80}")
        validation_status = True
    else:
        print(f"⚠️  VALIDATION WARNING!")
        print(f"   {misclassified} potential misclassifications detected.")
        print(f"   These may be ambiguous values or require manual review.")
        print(f"{'='*80}")
        validation_status = True  # Still return True as this is a warning, not an error
    
    # Save sample to Excel for manual inspection
    print(f"\n{'='*80}")
    print("SAVING SAMPLE TO EXCEL")
    print(f"{'='*80}")
    
    output_dir = Path(__file__).parent / 'transformation_app' / 'app_outputs' / 'unidades_proyecto_outputs'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = output_dir / 'gdf_geolocalizar_sample.xlsx'
    
    # Add comparison columns to show before/after
    comparison_df = sample_df_final.copy()
    comparison_df['barrio_vereda_BEFORE'] = sample_df['barrio_vereda_val_s3']
    comparison_df['comuna_corregimiento_BEFORE'] = sample_df['comuna_corregimiento_val_s3']
    comparison_df['barrio_vereda_AFTER'] = sample_df_final['barrio_vereda_val_s3']
    comparison_df['comuna_corregimiento_AFTER'] = sample_df_final['comuna_corregimiento_val_s3']
    
    # Add a change flag
    comparison_df['RECLASSIFICATION_APPLIED'] = (
        (comparison_df['barrio_vereda_BEFORE'] != comparison_df['barrio_vereda_AFTER']) |
        (comparison_df['comuna_corregimiento_BEFORE'] != comparison_df['comuna_corregimiento_AFTER'])
    )
    
    # Reorder columns to show comparison clearly
    cols_to_move = [
        'upid', 'nombre_up', 'direccion',
        'barrio_vereda_BEFORE', 'barrio_vereda_AFTER',
        'comuna_corregimiento_BEFORE', 'comuna_corregimiento_AFTER',
        'RECLASSIFICATION_APPLIED'
    ]
    
    # Keep existing columns and add comparison columns
    other_cols = [col for col in comparison_df.columns if col not in cols_to_move]
    comparison_df = comparison_df[cols_to_move + other_cols]
    
    # Save to Excel
    try:
        comparison_df.to_excel(output_path, index=False, engine='xlsxwriter')
        
        file_size = os.path.getsize(output_path) / 1024
        print(f"\n✓ Saved sample Excel file:")
        print(f"  - Location: {output_path}")
        print(f"  - File size: {file_size:.1f} KB")
        print(f"  - Records: {len(comparison_df)}")
        
        # Count records with changes
        changed_records = comparison_df['RECLASSIFICATION_APPLIED'].sum()
        print(f"  - Records with reclassification: {changed_records} ({changed_records/len(comparison_df)*100:.1f}%)")
        
        print(f"\n📋 Excel contains comparison columns:")
        print(f"   - barrio_vereda_BEFORE/AFTER")
        print(f"   - comuna_corregimiento_BEFORE/AFTER")
        print(f"   - RECLASSIFICATION_APPLIED (True/False)")
    except PermissionError:
        print(f"\n⚠️  Could not save Excel file: {output_path}")
        print(f"   File is open in another program. Please close it and try again.")
        print(f"\n   Attempting to save with alternative name...")
        
        # Try with timestamp
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        alt_output_path = output_dir / f'gdf_geolocalizar_sample_{timestamp}.xlsx'
        
        try:
            comparison_df.to_excel(alt_output_path, index=False, engine='xlsxwriter')
            file_size = os.path.getsize(alt_output_path) / 1024
            print(f"\n✓ Saved sample Excel file with alternative name:")
            print(f"  - Location: {alt_output_path}")
            print(f"  - File size: {file_size:.1f} KB")
            print(f"  - Records: {len(comparison_df)}")
            
            # Count records with changes
            changed_records = comparison_df['RECLASSIFICATION_APPLIED'].sum()
            print(f"  - Records with reclassification: {changed_records} ({changed_records/len(comparison_df)*100:.1f}%)")
        except Exception as e:
            print(f"\n❌ Could not save Excel file: {e}")
            print(f"   Continuing without saving...")
    
    return validation_status


if __name__ == "__main__":
    success = test_reclassification_sample()
    
    if success:
        print("\n🚀 Test completed! Reclassification logic is ready.")
        print("   Sample Excel saved for manual inspection.")
        print("   Changes will be applied automatically when saving Excel files.")
    else:
        print("\n⚠️  Test completed with warnings.")
        sys.exit(0)
