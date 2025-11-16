"""
Integration test for full transformation with barrio_vereda exclusion
"""
import sys
import os
import pandas as pd

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

from data_transformation_unidades_proyecto import _process_unidades_proyecto_dataframe

def test_full_pipeline_with_exclusion():
    """Test full transformation pipeline with barrio_vereda exclusion"""
    print("\n=== Testing full pipeline with barrio_vereda exclusion ===")
    
    # Create sample test data that mimics real data
    test_data = pd.DataFrame({
        'ano': ['2024', '2024', '2024', '2024', '2024'],
        'nombre_proyecto': ['Vía Principal Norte', 'Parque El Recreo', 'Corredor Vial Sur', 'Centro Deportivo', 'Espacio Público Central'],
        'tipo_equipamiento': [
            'Infraestructura vial',  # Should be excluded from barrio
            'Espacio público',       # Should get barrio
            'INFRAESTRUCTURA VIAL',  # Should be excluded (case insensitive)
            'Equipamiento deportivo',# Should get barrio
            'Espacio público'        # Should get barrio
        ],
        'tipo_intervencion': ['Construcción', 'Mejoramiento', 'Construcción', 'Construcción', 'Mejoramiento'],
        'estado': ['En ejecución', 'Terminado', 'En ejecución', 'Terminado', 'En ejecución'],
        'comuna_corregimiento': ['COMUNA 6', 'COMUNA 17', 'COMUNA 19', 'COMUNA 2', 'COMUNA 10'],
        'geometry': [
            '{"type": "Point", "coordinates": [-76.5320, 3.4516]}',
            '{"type": "Point", "coordinates": [-76.5224, 3.3700]}',
            '{"type": "Point", "coordinates": [-76.5000, 3.4800]}',
            '{"type": "Point", "coordinates": [-76.5100, 3.4500]}',
            '{"type": "Point", "coordinates": [-76.5200, 3.4600]}'
        ]
    })
    
    print(f"  Input data: {len(test_data)} records")
    print(f"    - Infraestructura vial: 2 records")
    print(f"    - Other tipos: 3 records")
    
    # Process through full pipeline
    result_df = _process_unidades_proyecto_dataframe(test_data)
    
    print(f"\n  Output data: {len(result_df)} records")
    print(f"  Columns: {len(result_df.columns)}")
    
    # Check that required columns exist
    required_cols = ['upid', 'ano', 'tipo_intervencion', 'estado', 
                     'comuna_corregimiento_val', 'barrio_vereda_val']
    missing_cols = [col for col in required_cols if col not in result_df.columns]
    if missing_cols:
        print(f"\n  Available columns: {list(result_df.columns)}")
        assert False, f"Missing columns: {missing_cols}"
    print(f"\n  ✓ All required columns present")
    
    # Check comuna_corregimiento_val (all should be populated)
    comuna_counts = result_df['comuna_corregimiento_val'].value_counts()
    print(f"\n  comuna_corregimiento_val distribution:")
    for value, count in comuna_counts.items():
        print(f"    - {value}: {count}")
    
    # Check barrio_vereda_val
    barrio_counts = result_df['barrio_vereda_val'].value_counts()
    print(f"\n  barrio_vereda_val distribution:")
    for value, count in barrio_counts.items():
        print(f"    - {value}: {count}")
    
    # Verify Infraestructura vial records have REVISAR for barrio_vereda_val
    infraestructura_vial = result_df[
        result_df['tipo_equipamiento'].str.strip().str.lower() == 'infraestructura vial'
    ]
    print(f"\n  Infraestructura vial records: {len(infraestructura_vial)}")
    for idx, row in infraestructura_vial.iterrows():
        print(f"    - {row['nombre_proyecto']}: barrio_vereda_val='{row['barrio_vereda_val']}'")
    
    all_revisar = (infraestructura_vial['barrio_vereda_val'] == 'REVISAR').all()
    assert all_revisar, "Infraestructura vial should have REVISAR for barrio_vereda_val"
    print(f"  ✓ All Infraestructura vial records correctly excluded")
    
    # Verify other records were processed for barrio
    other_records = result_df[
        result_df['tipo_equipamiento'].str.strip().str.lower() != 'infraestructura vial'
    ]
    print(f"\n  Other records: {len(other_records)}")
    for idx, row in other_records.iterrows():
        print(f"    - {row['nombre_proyecto']}: barrio_vereda_val='{row['barrio_vereda_val']}'")
    
    all_have_values = other_records['barrio_vereda_val'].notna().all()
    assert all_have_values, "All non-excluded records should have barrio_vereda_val"
    print(f"  ✓ All non-excluded records have barrio_vereda_val")
    
    print(f"\n  ✓ Full pipeline test passed!")
    return True


if __name__ == "__main__":
    print("Testing Full Pipeline with Barrio/Vereda Exclusion")
    print("=" * 70)
    
    try:
        success = test_full_pipeline_with_exclusion()
        
        print("\n" + "=" * 70)
        if success:
            print("✓ Integration test passed!")
        else:
            print("✗ Integration test failed!")
        
    except Exception as e:
        print(f"\n✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()
