"""
Test para verificar que las validaciones espaciales funcionan con el fallback a barrio_vereda_val
"""
import pandas as pd
import json
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Create test data
test_data = {
    'upid': ['TEST-1', 'TEST-2', 'TEST-3'],
    'geometry': [
        '{"type": "Point", "coordinates": [3.4513596, -76.5430387]}',  # COMUNA 02
        '{"type": "Point", "coordinates": [3.4537155, -76.5499934]}',  # COMUNA 01
        '{"type": "Point", "coordinates": [3.4408964, -76.5315187]}'   # COMUNA 03
    ],
    'barrio_vereda_val': ['Normandia', 'Terron Colorado', 'San Pedro'],  # From spatial intersection
    'barrio_vereda_val_s3': [None, None, None],  # Empty from reverse geocoding (fallback scenario)
    'corregir': ['INTENTAR GEORREFERENCIAR', 'INTENTAR GEORREFERENCIAR', 'ACEPTABLE']
}

gdf_test = pd.DataFrame(test_data)

print("="*80)
print("TEST: VALIDACIONES ESPACIALES CON FALLBACK")
print("="*80)

print(f"\n📋 Datos de prueba:")
print(f"   - Total registros: {len(gdf_test)}")
print(f"   - Con geometría válida: {gdf_test['geometry'].notna().sum()}")
print(f"   - Con barrio_vereda_val: {gdf_test['barrio_vereda_val'].notna().sum()}")
print(f"   - Con barrio_vereda_val_s3: {gdf_test['barrio_vereda_val_s3'].notna().sum()}")

# Load barrios GeoJSON
try:
    import geopandas as gpd
    from shapely.geometry import Point
    
    basemaps_dir = Path(__file__).parent.parent / 'basemaps'
    barrios_path = basemaps_dir / 'barrios_veredas.geojson'
    
    barrios_gdf = gpd.read_file(barrios_path)
    print(f"\n✓ Loaded {len(barrios_gdf)} barrios/veredas with geometries")
    
    # Helper function
    def validate_point_in_barrio(lon, lat, expected_barrio, barrios_gdf):
        """Validate if a point (lon, lat) intersects with the expected barrio."""
        try:
            point = Point(lon, lat)
            print(f"       · Point created: {point}")
            
            # Find barrio that contains this point
            containing_barrios = barrios_gdf[barrios_gdf.geometry.contains(point)]
            print(f"       · Containing barrios found: {len(containing_barrios)}")
            
            if len(containing_barrios) == 0:
                return "FUERA DE RANGO"
            
            actual_barrio = containing_barrios.iloc[0]['barrio_vereda']
            print(f"       · Actual barrio: {actual_barrio}")
            print(f"       · Expected barrio: {expected_barrio}")
            
            if actual_barrio == expected_barrio:
                return "EN EL RANGO"
            else:
                return "FUERA DE RANGO"
                
        except Exception as e:
            print(f"       · Error in validation: {e}")
            return None
    
    # Validation function with fallback
    def validate_geometry_in_barrio(row):
        """Validate if geometry point is in the correct barrio."""
        geom = row.get('geometry')
        # Try barrio_vereda_val_s3 first, fallback to barrio_vereda_val
        expected_barrio = row.get('barrio_vereda_val_s3')
        if not expected_barrio or expected_barrio == 'ERROR':
            expected_barrio = row.get('barrio_vereda_val', 'ERROR')
        
        print(f"\n   Testing {row['upid']}:")
        print(f"     - barrio_vereda_val_s3: {row.get('barrio_vereda_val_s3')}")
        print(f"     - barrio_vereda_val: {row.get('barrio_vereda_val')}")
        print(f"     - expected_barrio (after fallback): {expected_barrio}")
        print(f"     - corregir: {row.get('corregir')}")
        
        if not geom or geom == 'ERROR' or expected_barrio == 'ERROR' or expected_barrio == 'REVISAR':
            print(f"     → Skipped (invalid data)")
            return None
        
        try:
            if isinstance(geom, str):
                geom_obj = json.loads(geom)
            elif isinstance(geom, dict):
                geom_obj = geom
            else:
                print(f"     → Skipped (invalid geometry type)")
                return None
            
            coords = geom_obj.get('coordinates', [])
            if len(coords) < 2:
                print(f"     → Skipped (invalid coordinates)")
                return None
            
            lat, lon = coords[0], coords[1]
            print(f"     - Coordinates: lat={lat}, lon={lon}")
            result = validate_point_in_barrio(lon, lat, expected_barrio, barrios_gdf)
            print(f"     → Result: {result}")
            return result
            
        except Exception as e:
            print(f"     → Error: {e}")
            return None
    
    print(f"\n🔄 Aplicando validaciones...")
    
    # Initialize column
    gdf_test['geometry_distancias'] = None
    
    # Apply only to INTENTAR GEORREFERENCIAR
    mask_georeferenciar = gdf_test['corregir'] == 'INTENTAR GEORREFERENCIAR'
    print(f"\n   Registros a validar: {mask_georeferenciar.sum()}")
    
    gdf_test.loc[mask_georeferenciar, 'geometry_distancias'] = gdf_test[mask_georeferenciar].apply(
        validate_geometry_in_barrio, axis=1
    )
    
    print(f"\n" + "="*80)
    print("RESULTADOS")
    print("="*80)
    
    print(f"\n📊 Estadísticas:")
    validations = gdf_test['geometry_distancias'].notna()
    validation_count = validations.sum()
    print(f"   - Total validaciones: {validation_count}")
    
    if validation_count > 0:
        en_rango = (gdf_test['geometry_distancias'] == 'EN EL RANGO').sum()
        fuera_rango = (gdf_test['geometry_distancias'] == 'FUERA DE RANGO').sum()
        print(f"   - EN EL RANGO: {en_rango}")
        print(f"   - FUERA DE RANGO: {fuera_rango}")
        
        print(f"\n📋 Detalle:")
        for _, row in gdf_test.iterrows():
            result = row['geometry_distancias']
            if pd.notna(result):
                print(f"   {row['upid']}: {row['barrio_vereda_val']} → {result}")
    else:
        print(f"   ⚠️  No se realizaron validaciones")
    
    print(f"\n✅ TEST COMPLETADO\n")
    
except ImportError as e:
    print(f"❌ Error: {e}")
    print(f"   Instalar: pip install geopandas shapely")
except Exception as e:
    print(f"❌ Error inesperado: {e}")
    import traceback
    traceback.print_exc()
