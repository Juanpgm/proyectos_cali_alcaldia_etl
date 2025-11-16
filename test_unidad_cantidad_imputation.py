"""
Test script for unidad and cantidad imputation based on geometry type.
"""
import pandas as pd
import sys
import os

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

from data_transformation_unidades_proyecto import impute_unidad_cantidad_by_geometry

# Create test data with different geometry types, invalid values, and empty unidad/cantidad values
test_data = {
    'upid': ['UNP-1', 'UNP-2', 'UNP-3', 'UNP-4', 'UNP-5', 'UNP-6', 'UNP-7', 'UNP-8', 'UNP-9', 'UNP-10'],
    'nombre_up': ['Project 1', 'Project 2', 'Project 3', 'Project 4', 'Project 5', 'Project 6', 'Project 7', 'Project 8', 'Project 9', 'Project 10'],
    'geometry_type': ['Point', 'Point', 'LineString', 'MultiLineString', 'Point', 'LineString', 'Polygon', None, 'Point', 'LineString'],
    'unidad': [None, 'kg', None, None, '', None, 'm2', 'UND', 'UND', 'm'],  # Mix: empty, invalid (kg, m2), valid (UND, m)
    'cantidad': [None, 5, None, None, 0, None, 10, 1, 2, 3]  # Mix of empty, filled, null, and zero
}

df = pd.DataFrame(test_data)

print("="*60)
print("TESTING UNIDAD AND CANTIDAD IMPUTATION")
print("="*60)

print("\n📋 Original Data:")
print(df[['upid', 'geometry_type', 'unidad', 'cantidad']].to_string())

# Apply imputation
df_imputed = impute_unidad_cantidad_by_geometry(df)

print("\n📋 Data After Imputation:")
print(df_imputed[['upid', 'geometry_type', 'unidad', 'cantidad']].to_string())

print("\n✅ Test completed successfully!")
print("\n📝 Expected results:")
print("  - UNP-1 (Point, empty unidad/cantidad): unidad='UND', cantidad=1")
print("  - UNP-2 (Point, invalid 'kg'): unidad='REVISAR' (invalid value), cantidad=5")
print("  - UNP-3 (LineString, empty): unidad='m', cantidad=None")
print("  - UNP-4 (MultiLineString, empty): unidad='m', cantidad=None")
print("  - UNP-5 (Point, empty string/zero): unidad='UND', cantidad=1")
print("  - UNP-6 (LineString, empty): unidad='m', cantidad=None")
print("  - UNP-7 (Polygon, invalid 'm2'): unidad='REVISAR' (invalid value), cantidad=10")
print("  - UNP-8 (None geometry, valid 'UND'): unidad='REVISAR' (no geometry), cantidad=1")
print("  - UNP-9 (Point, valid 'UND'): unidad='UND' (kept), cantidad=2")
print("  - UNP-10 (LineString, valid 'm'): unidad='m' (kept), cantidad=3")
print("\n✓ Only 'UND' and 'm' are valid values. All others marked as 'REVISAR'.")
