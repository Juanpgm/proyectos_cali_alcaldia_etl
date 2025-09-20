#!/usr/bin/env python3
"""
Test script for UnidadProyecto model
Tests model creation from GeoJSON data
"""

import sys
import os
import json
from datetime import datetime

# Add the database_management directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'database_management'))

try:
    from core.models import UnidadProyecto, Base
    print("✅ Model imported successfully")
except ImportError as e:
    print(f"❌ Error importing model: {e}")
    sys.exit(1)

def test_model_creation():
    """Test creating UnidadProyecto instances from GeoJSON data."""
    
    print("\n=== TESTING UNIDADPROYECTO MODEL ===")
    
    # Load GeoJSON file
    geojson_path = 'transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto_equipamientos.geojson'
    
    if not os.path.exists(geojson_path):
        print(f"❌ GeoJSON file not found: {geojson_path}")
        return
    
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    
    features = geojson_data.get('features', [])
    print(f"📊 Total features in GeoJSON: {len(features)}")
    
    if not features:
        print("❌ No features found in GeoJSON")
        return
    
    # Test with first few features
    test_count = min(5, len(features))
    successful_creations = 0
    
    print(f"\n🧪 Testing model creation with first {test_count} features:")
    
    for i in range(test_count):
        feature = features[i]
        
        try:
            # Create model instance from GeoJSON feature
            proyecto = UnidadProyecto.from_geojson_feature(feature)
            
            print(f"\n  Feature {i+1}:")
            print(f"    ✅ Created: {proyecto}")
            print(f"    📍 Key: {proyecto.key}")
            print(f"    📍 BPIN: {proyecto.bpin}")
            print(f"    📍 Identifier: {proyecto.identificador}")
            print(f"    📍 Coordinates: ({proyecto.latitude}, {proyecto.longitude})")
            print(f"    📍 Has coordinates: {proyecto.has_coordinates}")
            print(f"    📍 Funding source: {proyecto.fuente_financiacion}")
            print(f"    📍 Project class: {proyecto.clase_obra}")
            print(f"    📍 Location: {proyecto.comuna_corregimiento}")
            
            # Test to_dict method
            proyecto_dict = proyecto.to_dict()
            print(f"    📊 Dictionary keys: {len(proyecto_dict)} fields")
            
            # Test geometry bounds parsing
            if proyecto.geometry_bounds:
                bounds_dict = proyecto.geometry_bounds_dict
                print(f"    🗺️  Geometry bounds: {bounds_dict}")
            
            successful_creations += 1
            
        except Exception as e:
            print(f"    ❌ Error creating model for feature {i+1}: {e}")
    
    print(f"\n📈 RESULTS:")
    print(f"  ✅ Successful model creations: {successful_creations}/{test_count}")
    print(f"  📊 Success rate: {successful_creations/test_count*100:.1f}%")
    
    if successful_creations == test_count:
        print(f"  🎉 ALL TESTS PASSED! Model is ready for database operations.")
    else:
        print(f"  ⚠️  Some tests failed. Review the errors above.")
    
    # Test model schema information
    print(f"\n📋 MODEL SCHEMA INFORMATION:")
    print(f"  Table name: {UnidadProyecto.__tablename__}")
    print(f"  Model columns: {len(UnidadProyecto.__table__.columns)} fields")
    
    print(f"\n📝 Available columns:")
    for i, column in enumerate(UnidadProyecto.__table__.columns, 1):
        nullable = "NULL" if column.nullable else "NOT NULL"
        print(f"    {i:2d}. {column.name}: {column.type} ({nullable})")

if __name__ == "__main__":
    test_model_creation()