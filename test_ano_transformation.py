# -*- coding: utf-8 -*-
"""
Test script for ano column standardization in unidades_proyecto transformation.
"""

import sys
import os
import pandas as pd

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

from data_transformation_unidades_proyecto import unidades_proyecto_transformer

def test_ano_transformation():
    """Test the ano column standardization with sample data."""
    
    print("="*80)
    print("TESTING ANO COLUMN STANDARDIZATION")
    print("="*80)
    
    # Create sample test data with various ano values
    test_data = [
        {'upid': 'UNP-1', 'nombre_up': 'Proyecto Test 1', 'ano': 2024, 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},
        {'upid': 'UNP-2', 'nombre_up': 'Proyecto Test 2', 'ano': 2025, 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},
        {'upid': 'UNP-3', 'nombre_up': 'Proyecto Test 3', 'ano': 2030, 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},
        {'upid': 'UNP-4', 'nombre_up': 'Proyecto Test 4', 'ano': None, 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},  # Null
        {'upid': 'UNP-5', 'nombre_up': 'Proyecto Test 5', 'ano': '', 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},  # Empty
        {'upid': 'UNP-6', 'nombre_up': 'Proyecto Test 6', 'ano': 2020, 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},  # Invalid (too old)
        {'upid': 'UNP-7', 'nombre_up': 'Proyecto Test 7', 'ano': 2035, 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},  # Invalid (too new)
        {'upid': 'UNP-8', 'nombre_up': 'Proyecto Test 8', 'ano': '2027', 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},  # String valid
        {'upid': 'UNP-9', 'nombre_up': 'Proyecto Test 9', 'ano': '2024.0', 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},  # String with decimal
        {'upid': 'UNP-10', 'nombre_up': 'Proyecto Test 10', 'ano': 2026.5, 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},  # Float
        {'upid': 'UNP-11', 'nombre_up': 'Proyecto Test 11', 'ano': 'invalid', 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},  # Invalid text
        {'upid': 'UNP-12', 'nombre_up': 'Proyecto Test 12', 'ano': 2028, 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},  # Valid
        {'upid': 'UNP-13', 'nombre_up': 'Proyecto Test 13', 'ano': 0, 'geom': '{"type": "Point", "coordinates": [-76.5, 3.4]}'},  # Zero (invalid)
    ]
    
    print(f"\n📊 Created test dataset with {len(test_data)} records")
    print("="*80)
    
    # Show original data
    print("\n📋 ORIGINAL ANO VALUES:")
    print("-"*80)
    df_original = pd.DataFrame(test_data)
    for idx, row in df_original.iterrows():
        print(f"  Row {idx+1:2d}: ano = {repr(row['ano']):15s} (type: {type(row['ano']).__name__})")
    
    # Run transformation
    print("\n" + "="*80)
    print("🔄 RUNNING TRANSFORMATION...")
    print("="*80)
    
    try:
        result_df = unidades_proyecto_transformer(data=test_data, use_drive_extraction=False)
        
        if result_df is not None:
            print("\n" + "="*80)
            print("✅ TRANSFORMATION COMPLETED SUCCESSFULLY")
            print("="*80)
            
            # Show transformed ano values
            print("\n📋 TRANSFORMED ANO VALUES:")
            print("-"*80)
            for idx, row in result_df.iterrows():
                upid = row.get('upid', 'N/A')
                ano = row.get('ano', 'N/A')
                ano_type = type(ano).__name__
                print(f"  {upid}: ano = {str(ano):15s} (type: {ano_type})")
            
            # Count results
            print("\n" + "="*80)
            print("📊 ANO STANDARDIZATION RESULTS:")
            print("="*80)
            
            ano_counts = result_df['ano'].value_counts()
            print("\nDistribution:")
            for ano_value, count in ano_counts.items():
                percentage = (count / len(result_df)) * 100
                print(f"  {str(ano_value):15s}: {count:3d} records ({percentage:5.1f}%)")
            
            # Calculate statistics
            total_records = len(result_df)
            revisar_count = (result_df['ano'] == 'REVISAR').sum()
            valid_count = total_records - revisar_count
            
            print(f"\n{'='*80}")
            print("📈 SUMMARY STATISTICS:")
            print("="*80)
            print(f"  Total records:           {total_records}")
            print(f"  Valid years (2024-2030): {valid_count} ({valid_count/total_records*100:.1f}%)")
            print(f"  Require review:          {revisar_count} ({revisar_count/total_records*100:.1f}%)")
            
            # List records that need review
            if revisar_count > 0:
                print(f"\n⚠️  RECORDS REQUIRING REVIEW:")
                print("-"*80)
                revisar_records = result_df[result_df['ano'] == 'REVISAR']
                for idx, row in revisar_records.iterrows():
                    upid = row.get('upid', 'N/A')
                    nombre = row.get('nombre_up', 'N/A')
                    # Get original value from test_data
                    original_ano = test_data[idx]['ano']
                    print(f"  {upid}: '{nombre}' - Original: {repr(original_ano)}")
            
            print("\n" + "="*80)
            print("✅ TEST COMPLETED")
            print("="*80)
            
        else:
            print("\n❌ ERROR: Transformation returned None")
            
    except Exception as e:
        print(f"\n❌ ERROR during transformation: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_ano_transformation()
