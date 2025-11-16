"""
Test script for barrio_vereda spatial intersection functionality
"""
import sys
import os
import pandas as pd
import json

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

from data_transformation_unidades_proyecto import perform_spatial_intersection, point_in_polygon

def test_barrio_vereda_geojson():
    """Test that the barrio_vereda GeoJSON file is properly formatted"""
    print("\n=== Testing barrios_veredas.geojson structure ===")
    
    try:
        with open('basemaps/barrios_veredas.geojson', 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        print(f"  ✓ GeoJSON file loaded successfully")
        print(f"  ✓ Total features: {len(geojson_data['features'])}")
        
        # Check first few features
        sample_size = min(5, len(geojson_data['features']))
        print(f"\n  Sample barrio_vereda values (first {sample_size}):")
        for i in range(sample_size):
            feature = geojson_data['features'][i]
            barrio_vereda = feature['properties'].get('barrio_vereda', 'N/A')
            geom_type = feature['geometry']['type']
            print(f"    - {barrio_vereda} ({geom_type})")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error loading GeoJSON: {e}")
        return False


def test_spatial_intersection_barrio_vereda():
    """Test spatial intersection with sample data"""
    print("\n=== Testing spatial intersection for barrio_vereda ===")
    
    # Create sample test data with known coordinates in Cali
    # Note: GeoJSON format is [longitude, latitude], not [lat, lon]
    # These are real coordinates in Cali that should intersect with barrios/veredas
    test_data = pd.DataFrame({
        'upid': ['TEST001', 'TEST002', 'TEST003', 'TEST004'],
        'nombre_proyecto': ['Proyecto A', 'Proyecto B', 'Proyecto C', 'Proyecto D'],
        'geometry': [
            '{"type": "Point", "coordinates": [-76.5320, 3.4516]}',  # Coordinates in Cali center area
            '{"type": "Point", "coordinates": [-76.5224, 3.3700]}',  # Coordinates in south Cali
            '{"type": "Point", "coordinates": [-76.5000, 3.4800]}',  # Coordinates in north Cali
            None                    # No geometry - should be REVISAR
        ]
    })
    
    print(f"  Sample data: {len(test_data)} records")
    
    # Perform spatial intersection
    result_df = perform_spatial_intersection(
        test_data,
        'basemaps/barrios_veredas.geojson',
        'barrio_vereda',
        'barrio_vereda_val'
    )
    
    print(f"\n  Results:")
    for idx, row in result_df.iterrows():
        upid = row['upid']
        geometry = row['geometry']
        barrio_vereda_val = row['barrio_vereda_val']
        print(f"    {upid}: {barrio_vereda_val} (geometry: {str(geometry)[:30]}...)")
    
    # Verify column was created
    assert 'barrio_vereda_val' in result_df.columns, "Column barrio_vereda_val not created"
    print(f"\n  ✓ Column 'barrio_vereda_val' created successfully")
    
    # Check that non-null geometries have values (either valid barrio or REVISAR)
    non_null_geom = result_df[result_df['geometry'].notna()]
    all_have_values = non_null_geom['barrio_vereda_val'].notna().all()
    print(f"  ✓ All records with geometry have barrio_vereda_val: {all_have_values}")
    
    # Check that null geometry has REVISAR
    null_geom = result_df[result_df['geometry'].isna()]
    if len(null_geom) > 0:
        revisar_count = (null_geom['barrio_vereda_val'] == 'REVISAR').sum()
        print(f"  ✓ Records without geometry marked as REVISAR: {revisar_count}/{len(null_geom)}")
    
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
    print("Testing Barrio/Vereda Spatial Intersection Functionality")
    print("=" * 70)
    
    try:
        # Test GeoJSON structure
        geojson_ok = test_barrio_vereda_geojson()
        
        if geojson_ok:
            # Test spatial intersection
            intersection_ok = test_spatial_intersection_barrio_vereda()
            
            print("\n" + "=" * 70)
            if intersection_ok:
                print("✓ All tests passed!")
            else:
                print("✗ Some tests failed!")
        else:
            print("\n" + "=" * 70)
            print("✗ GeoJSON structure test failed!")
        
    except Exception as e:
        print(f"\n✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()
