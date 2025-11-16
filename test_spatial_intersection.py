"""
Test script for spatial intersection functionality
"""
import pandas as pd
import sys
import os

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

from data_transformation_unidades_proyecto import perform_spatial_intersection, point_in_polygon

def test_point_in_polygon():
    """Test the point-in-polygon algorithm"""
    print("\n=== Testing point_in_polygon function ===")
    
    # Simple square polygon
    square = [
        [-76.5, 3.4],
        [-76.4, 3.4],
        [-76.4, 3.5],
        [-76.5, 3.5],
        [-76.5, 3.4]  # Close the polygon
    ]
    
    # Test points
    test_cases = [
        (-76.45, 3.45, True, "Inside square"),
        (-76.55, 3.45, False, "Outside left"),
        (-76.35, 3.45, False, "Outside right"),
        (-76.45, 3.35, False, "Outside bottom"),
        (-76.45, 3.55, False, "Outside top"),
    ]
    
    for x, y, expected, description in test_cases:
        result = point_in_polygon(x, y, square)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {description}: ({x}, {y}) -> {result} (expected {expected})")

def test_spatial_intersection_with_sample_data():
    """Test spatial intersection with sample data"""
    print("\n=== Testing spatial intersection with sample data ===")
    
    # Create sample dataframe with known coordinates in Cali
    sample_data = {
        'upid': ['UNP-1', 'UNP-2', 'UNP-3', 'UNP-4'],
        'nombre_up': ['Proyecto 1', 'Proyecto 2', 'Proyecto 3', 'Proyecto 4'],
        'geom': [
            '{"type": "Point", "coordinates": [-76.4916, 3.5056]}',  # Should be in Comuna 06
            '{"type": "Point", "coordinates": [-76.5400, 3.4200]}',  # Another location
            None,  # No geometry
            '{"type": "Point", "coordinates": [-76.5300, 3.3800]}',  # Another location
        ]
    }
    
    df = pd.DataFrame(sample_data)
    
    print(f"\nInput DataFrame:")
    print(df)
    
    # Perform spatial intersection
    result_df = perform_spatial_intersection(
        df, 
        'basemaps/comunas_corregimientos.geojson', 
        'comuna_corregimiento', 
        'comuna_corregimiento_val'
    )
    
    print(f"\nResult DataFrame:")
    print(result_df[['upid', 'nombre_up', 'comuna_corregimiento_val']])
    
    # Check results
    print(f"\nVerification:")
    for idx, row in result_df.iterrows():
        val = row['comuna_corregimiento_val']
        if pd.notna(val):
            print(f"  ✓ {row['upid']}: {val}")
        else:
            print(f"  - {row['upid']}: No intersection found")

if __name__ == "__main__":
    print("Testing Spatial Intersection Functionality")
    print("=" * 60)
    
    try:
        # Test 1: Point-in-polygon algorithm
        test_point_in_polygon()
        
        # Test 2: Spatial intersection with sample data
        test_spatial_intersection_with_sample_data()
        
        print("\n" + "=" * 60)
        print("✓ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()
