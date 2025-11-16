"""
Test script for barrio_vereda spatial intersection with tipo_equipamiento exclusion
"""
import sys
import os
import pandas as pd

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

from data_transformation_unidades_proyecto import perform_spatial_intersection

def test_barrio_vereda_exclusion():
    """Test that Infraestructura vial is excluded from barrio_vereda intersection"""
    print("\n=== Testing barrio_vereda exclusion for Infraestructura vial ===")
    
    # Create sample test data with different tipo_equipamiento values
    test_data = pd.DataFrame({
        'upid': ['TEST001', 'TEST002', 'TEST003', 'TEST004', 'TEST005'],
        'nombre_proyecto': ['Proyecto A', 'Proyecto B', 'Proyecto C', 'Proyecto D', 'Proyecto E'],
        'tipo_equipamiento': [
            'Infraestructura vial',  # Should be excluded
            'INFRAESTRUCTURA VIAL',  # Should be excluded (case insensitive)
            'Espacio público',       # Should NOT be excluded
            'Equipamiento social',   # Should NOT be excluded
            None                     # Should NOT be excluded (null value)
        ],
        'geometry': [
            '{"type": "Point", "coordinates": [-76.5320, 3.4516]}',
            '{"type": "Point", "coordinates": [-76.5224, 3.3700]}',
            '{"type": "Point", "coordinates": [-76.5000, 3.4800]}',
            '{"type": "Point", "coordinates": [-76.5100, 3.4500]}',
            '{"type": "Point", "coordinates": [-76.5200, 3.4600]}'
        ]
    })
    
    print(f"  Sample data: {len(test_data)} records")
    print(f"  - Infraestructura vial: 2 records (should be excluded)")
    print(f"  - Other tipos: 3 records (should be processed)")
    
    # Perform spatial intersection with exclusion condition
    result_df = perform_spatial_intersection(
        test_data,
        'basemaps/barrios_veredas.geojson',
        'barrio_vereda',
        'barrio_vereda_val',
        exclude_condition=lambda row: str(row.get('tipo_equipamiento', '')).strip().lower() == 'infraestructura vial'
    )
    
    print(f"\n  Results by tipo_equipamiento:")
    for idx, row in result_df.iterrows():
        upid = row['upid']
        tipo = row['tipo_equipamiento']
        barrio_val = row['barrio_vereda_val']
        print(f"    {upid}: tipo='{tipo}' → barrio_vereda_val='{barrio_val}'")
    
    # Verify column was created
    assert 'barrio_vereda_val' in result_df.columns, "Column barrio_vereda_val not created"
    print(f"\n  ✓ Column 'barrio_vereda_val' created successfully")
    
    # Check that Infraestructura vial records kept REVISAR (excluded from intersection)
    infraestructura_vial = result_df[
        result_df['tipo_equipamiento'].str.strip().str.lower() == 'infraestructura vial'
    ]
    all_revisar = (infraestructura_vial['barrio_vereda_val'] == 'REVISAR').all()
    print(f"  ✓ All 'Infraestructura vial' records have REVISAR: {all_revisar}")
    assert all_revisar, "Infraestructura vial records should remain as REVISAR"
    
    # Check that other records were processed (could have valid barrio or REVISAR)
    other_records = result_df[
        (result_df['tipo_equipamiento'].isna()) | 
        (result_df['tipo_equipamiento'].str.strip().str.lower() != 'infraestructura vial')
    ]
    all_have_values = other_records['barrio_vereda_val'].notna().all()
    print(f"  ✓ All non-excluded records have barrio_vereda_val: {all_have_values}")
    
    # Show value distribution
    print(f"\n  Value distribution:")
    value_counts = result_df['barrio_vereda_val'].value_counts()
    for value, count in value_counts.items():
        print(f"    - {value}: {count}")
    
    # Verify all records processed
    assert len(result_df) == len(test_data), "Record count mismatch"
    print(f"\n  ✓ All {len(result_df)} records processed successfully")
    
    return True


if __name__ == "__main__":
    print("Testing Barrio/Vereda Exclusion Functionality")
    print("=" * 70)
    
    try:
        success = test_barrio_vereda_exclusion()
        
        print("\n" + "=" * 70)
        if success:
            print("✓ All tests passed!")
        else:
            print("✗ Some tests failed!")
        
    except Exception as e:
        print(f"\n✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()
