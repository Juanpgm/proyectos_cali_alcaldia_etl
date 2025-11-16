# -*- coding: utf-8 -*-
"""
Test script to verify geocoding coordinate format with a small sample.
Tests with 20 random records before running full geocoding.
"""

import sys
import os

# Set UTF-8 encoding for stdout/stderr (fixes PowerShell encoding issues)
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())
import pandas as pd
import geopandas as gpd
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent / '.env.prod'
if env_path.exists():
    load_dotenv(env_path)
    print(f"[OK] Loaded environment from {env_path}")

# Add paths
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

# Import transformation functions (includes extraction from Google Drive)
from data_transformation_unidades_proyecto import unidades_proyecto_transformer
from google_maps_geocoder import GoogleMapsGeocoder
from data_transformation_unidades_proyecto import (
    reclassify_geocoding_values,
    standardize_geocoding_values
)

# Import for spatial validation
import geopandas as gpd
from shapely.geometry import Point
from shapely import wkt

def test_geocoding_sample():
    """Test geocoding with 20 random records from REAL extracted data."""
    
    print("="*80)
    print("TESTING GEOCODING WITH REAL DATA FROM EXTRACTION")
    print("="*80)
    
    # STEP 0: Load REAL data from gdf_geolocalizar.geojson
    # This file contains data that was ALREADY extracted from Google Drive and transformed
    print(f"\n{'='*80}")
    print("STEP 0: LOADING REAL DATA FROM gdf_geolocalizar.geojson")
    print(f"{'='*80}")
    print("📄 This file contains REAL data extracted from Google Drive")
    print("   (Already downloaded and transformed by the pipeline)")
    
    try:
        # Load gdf_geolocalizar.geojson which has real data from Google Drive
        geojson_path = Path(__file__).parent / 'transformation_app' / 'app_outputs' / 'unidades_proyecto_outputs' / 'gdf_geolocalizar.geojson'
        
        if not geojson_path.exists():
            print(f"\n❌ ERROR: File not found: {geojson_path}")
            print("   Run the full transformation pipeline first to generate this file:")
            print("   python transformation_app/data_transformation_unidades_proyecto.py")
            return False
        
        # Read GeoJSON file (it contains REAL data from Google Drive)
        with open(geojson_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        # Convert GeoJSON features to DataFrame
        features = geojson_data['features']
        records = []
        for feature in features:
            props = feature['properties'].copy()
            # Keep geometry as JSON string (as expected by geocoder)
            props['geometry'] = json.dumps(feature['geometry'])
            records.append(props)
        
        df_extracted = pd.DataFrame(records)
        
        print(f"\n✓ Loaded {len(df_extracted)} REAL records from gdf_geolocalizar.geojson")
        print(f"✓ Data source: Google Drive (already extracted and transformed)")
        print(f"✓ Columns available: {list(df_extracted.columns[:10])}...")
        
    except Exception as e:
        print(f"\n❌ ERROR loading data: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Select random sample
    sample_size = 20
    if len(df_extracted) > sample_size:
        gdf = df_extracted.sample(n=sample_size, random_state=42).copy()
        print(f"✓ Selected {sample_size} random records for testing")
    else:
        gdf = df_extracted.copy()
        print(f"✓ Using all {len(gdf)} records")
    
    print(f"\n📊 Sample Summary:")
    print(f"   Records to test: {len(gdf)}")
    if 'direccion' in gdf.columns:
        valid_dirs = gdf['direccion'].dropna().head(3).tolist()
        print(f"   Sample addresses: {valid_dirs}")
    
    # Check required columns for geocoding (geometry should already exist from transformation)
    required_cols = ['direccion', 'barrio_vereda', 'comuna_corregimiento', 'geometry']
    missing_cols = [col for col in required_cols if col not in gdf.columns]
    
    if missing_cols:
        print(f"\n❌ ERROR: Missing required columns: {missing_cols}")
        print(f"   Available columns: {list(gdf.columns)}")
        return False
    
    # Verify geometry column has valid data
    valid_geoms = gdf[gdf['geometry'].notna() & (gdf['geometry'] != 'ERROR')].shape[0]
    print(f"\n✓ Geometry column found: {valid_geoms}/{len(gdf)} valid geometries")
    
    if valid_geoms == 0:
        print(f"❌ ERROR: No valid geometries found in data")
        return False
    
    # Initialize validation columns (as in real pipeline)
    print(f"\n✓ Initializing validation columns for geocoding...")
    if 'barrio_vereda_val_s3' not in gdf.columns:
        gdf['barrio_vereda_val_s3'] = 'ERROR'
    if 'comuna_corregimiento_val_s3' not in gdf.columns:
        gdf['comuna_corregimiento_val_s3'] = 'ERROR'
    
    # Create direccion_api column (as in real pipeline)
    gdf['direccion_api'] = gdf.apply(
        lambda row: f"{row.get('direccion', '')}, {row.get('barrio_vereda', '')}, {row.get('comuna_corregimiento', '')}, Cali, Valle del Cauca, Colombia".strip(),
        axis=1
    )
    print(f"   ✓ Created 'direccion_api' column for geocoding")
    
    # Mark all records for geocoding (simulating 'INTENTAR GEORREFERENCIAR')
    gdf['corregir'] = 'INTENTAR GEORREFERENCIAR'
    
    # Use the sample as test data
    test_gdf = gdf.copy()
    
    print(f"\n{'='*80}")
    print("STEP 1: REVERSE GEOCODING TEST (REAL DATA)")
    print(f"{'='*80}")
    
    # Initialize geocoder
    geocoder = GoogleMapsGeocoder(use_adc=True)
    
    # Process reverse geocoding
    test_gdf = geocoder.process_dataframe(
        test_gdf,
        geometry_column='geometry',
        output_barrio_column='barrio_vereda_val_s3',
        output_comuna_column='comuna_corregimiento_val_s3',
        filter_column='corregir',
        filter_value='INTENTAR GEORREFERENCIAR'
    )
    
    print(f"\n{'='*80}")
    print("STEP 2: FORWARD GEOCODING TEST (REAL DATA)")
    print(f"{'='*80}")
    
    # Determine address column
    address_col = 'direccion_api' if 'direccion_api' in test_gdf.columns else 'direccion'
    print(f"   Using address column: {address_col}")
    
    # Process forward geocoding
    test_gdf = geocoder.process_forward_geocoding(
        test_gdf,
        address_column=address_col,
        output_geometry_column='geometry_val_s2',
        filter_column='corregir',
        filter_value='INTENTAR GEORREFERENCIAR'
    )
    
    print(f"\n{'='*80}")
    print("STEP 3: RECLASSIFICATION BEFORE FORWARD GEOCODING (PIPELINE REAL)")
    print(f"{'='*80}")
    
    # Convert to DataFrame for reclassification (if it's a GeoDataFrame)
    if hasattr(test_gdf, 'to_crs'):
        print("   Converting GeoDataFrame to DataFrame for processing...")
        test_df = pd.DataFrame(test_gdf)
    else:
        test_df = test_gdf
    
    # Show stats before reclassification
    if 'barrio_vereda_val_s3' in test_df.columns:
        barrio_non_error_before = (test_df['barrio_vereda_val_s3'] != 'ERROR').sum()
        print(f"   Barrio/Vereda values before: {barrio_non_error_before}/{len(test_df)} non-ERROR")
    
    if 'comuna_corregimiento_val_s3' in test_df.columns:
        comuna_non_error_before = (test_df['comuna_corregimiento_val_s3'] != 'ERROR').sum()
        print(f"   Comuna/Corregimiento values before: {comuna_non_error_before}/{len(test_df)} non-ERROR")
    
    # Apply reclassification (swaps values if they're in wrong columns)
    print(f"\n   🔄 Applying reclassification logic...")
    test_df = reclassify_geocoding_values(test_df)
    
    print(f"\n{'='*80}")
    print("STEP 4: SPATIAL VALIDATION WITH GEOJSON (PIPELINE REAL)")
    print(f"{'='*80}")
    print("🔄 Validating with point-in-polygon intersection...")
    print("   Using official GeoJSON geometries (quality standards)")
    
    # Load basemaps
    basemaps_dir = Path(__file__).parent / 'basemaps'
    barrios_path = basemaps_dir / 'barrios_veredas.geojson'
    comunas_path = basemaps_dir / 'comunas_corregimientos.geojson'
    
    barrios_gdf = gpd.read_file(barrios_path)
    comunas_gdf = gpd.read_file(comunas_path)
    
    print(f"   ✓ Loaded {len(barrios_gdf)} barrios/veredas geometries")
    print(f"   ✓ Loaded {len(comunas_gdf)} comunas/corregimientos geometries")
    
    # Perform spatial validation (like the real pipeline)
    spatial_matches = 0
    for idx, row in test_df.iterrows():
        geom_str = row.get('geometry')
        if not geom_str or geom_str == 'ERROR':
            continue
        
        try:
            # Parse geometry
            geom_obj = json.loads(geom_str) if isinstance(geom_str, str) else geom_str
            coords = geom_obj['coordinates']
            lat, lon = coords[0], coords[1]  # Custom format: [lat, lon]
            
            # Create point (Shapely uses lon, lat)
            point = Point(lon, lat)
            
            # Find barrio match - use EXACT value from GeoJSON
            barrio_match = None
            for _, barrio_row in barrios_gdf.iterrows():
                if barrio_row.geometry.contains(point):
                    barrio_match = barrio_row['barrio_vereda']
                    break
            
            # Find comuna match - use EXACT value from GeoJSON
            comuna_match = None
            for _, comuna_row in comunas_gdf.iterrows():
                if comuna_row.geometry.contains(point):
                    comuna_match = comuna_row['comuna_corregimiento']
                    break
            
            # OVERWRITE with GeoJSON values (quality standards)
            if barrio_match:
                test_df.at[idx, 'barrio_vereda_val_s3'] = barrio_match
                spatial_matches += 1
            
            if comuna_match:
                test_df.at[idx, 'comuna_corregimiento_val_s3'] = comuna_match
                spatial_matches += 1
                
        except Exception as e:
            continue
    
    print(f"   ✓ Spatial validation completed: {spatial_matches} matches found")
    
    # Apply standardization AFTER spatial validation (like real pipeline)
    print(f"\n   🔄 Applying standardization logic...")
    test_df = standardize_geocoding_values(test_df)
    
    # Show stats after all processing
    print(f"\n   ✅ Complete validation pipeline executed!")
    
    if 'barrio_vereda_val_s3' in test_df.columns:
        barrio_non_error_after = (test_df['barrio_vereda_val_s3'] != 'ERROR').sum()
        print(f"   Barrio/Vereda values after: {barrio_non_error_after}/{len(test_df)} non-ERROR")
    
    if 'comuna_corregimiento_val_s3' in test_df.columns:
        comuna_non_error_after = (test_df['comuna_corregimiento_val_s3'] != 'ERROR').sum()
        print(f"   Comuna/Corregimiento values after: {comuna_non_error_after}/{len(test_df)} non-ERROR")
    
    # Update test_gdf with processed data
    test_gdf = test_df
    
    print(f"\n{'='*80}")
    print("VALIDATING COORDINATE FORMAT (REAL DATA)")
    print(f"{'='*80}")
    
    # Check a few samples
    if 'geometry_val_s2' in test_gdf.columns:
        geocoded_mask = (test_gdf['geometry_val_s2'] != 'ERROR') & (test_gdf['geometry_val_s2'].notna())
        geocoded_samples = test_gdf[geocoded_mask].head(5)
    else:
        print("   ⚠️  No geocoded results found (geometry_val_s2 column missing)")
        geocoded_samples = test_gdf.head(5)
    
    print(f"\n✓ Checking coordinate format for {len(geocoded_samples)} REAL samples:")
    
    all_correct = True
    
    for idx, row in geocoded_samples.iterrows():
        geom_original = row['geometry']
        
        # Get identifier
        upid = row.get('upid', row.get('identificador', row.get('referencia_proceso', f'Row-{idx}')))
        print(f"\n📍 Record ID: {upid}")
        
        # Parse original geometry
        if hasattr(geom_original, 'geom_type'):
            # Shapely geometry object - usando formato Point(lat, lon) personalizado
            if geom_original.geom_type == 'Point':
                # En este proyecto: Point.x = latitud, Point.y = longitud (formato no estándar)
                orig_lat = geom_original.x
                orig_lon = geom_original.y
                print(f"   Original:  Point({orig_lat:.6f}, {orig_lon:.6f})")
                print(f"   Format:    Point(lat={orig_lat:.6f}, lon={orig_lon:.6f})")
                
                # Validate: lat ~3.x and lon ~-76.x
                if 3.0 <= orig_lat <= 4.0 and -77.0 <= orig_lon <= -76.0:
                    print(f"   ✅ CORRECT: Point(lat, lon) format - lat={orig_lat:.6f}, lon={orig_lon:.6f}")
                else:
                    print(f"   ❌ WRONG: Coordinates out of Cali range")
                    print(f"      Expected: lat between 3.0-4.0, lon between -77.0 to -76.0")
                    all_correct = False
        elif isinstance(geom_original, str):
            try:
                geom_orig_obj = json.loads(geom_original)
                if geom_orig_obj is None or 'coordinates' not in geom_orig_obj:
                    print(f"   ⚠️  Invalid geometry (no coordinates)")
                    continue
                orig_coords = geom_orig_obj['coordinates']
                print(f"   Original:  {geom_original[:100]}...")
                print(f"   Format:    [coord1={orig_coords[0]:.6f}, coord2={orig_coords[1]:.6f}]")
            except (json.JSONDecodeError, KeyError, TypeError, IndexError) as e:
                print(f"   ⚠️  Could not parse original geometry: {e}")
                continue
            
            # Validate: determine if [lat, lon] or [lon, lat]
            if 3.0 <= orig_coords[0] <= 4.0 and -77.0 <= orig_coords[1] <= -76.0:
                print(f"   ✅ CORRECT: [lat, lon] format")
            elif 3.0 <= orig_coords[1] <= 4.0 and -77.0 <= orig_coords[0] <= -76.0:
                print(f"   ⚠️  REVERSED: [lon, lat] format (should be [lat, lon])")
                all_correct = False
            else:
                print(f"   ❌ WRONG: Coordinates out of range")
                all_correct = False
        
        # Parse geocoded geometry if exists
        if 'geometry_val_s2' in row and row['geometry_val_s2'] and row['geometry_val_s2'] != 'ERROR':
            geom_geocoded = row['geometry_val_s2']
            
            if isinstance(geom_geocoded, str):
                try:
                    geom_geo_obj = json.loads(geom_geocoded)
                    geo_coords = geom_geo_obj['coordinates']
                    print(f"   Geocoded:  {geom_geocoded[:100]}...")
                    print(f"   Format:    [coord1={geo_coords[0]:.6f}, coord2={geo_coords[1]:.6f}]")
                    
                    # Validate: determine if [lat, lon] or [lon, lat]
                    if 3.0 <= geo_coords[0] <= 4.0 and -77.0 <= geo_coords[1] <= -76.0:
                        print(f"   ✅ CORRECT: [lat, lon] format")
                    elif 3.0 <= geo_coords[1] <= 4.0 and -77.0 <= geo_coords[0] <= -76.0:
                        print(f"   ⚠️  REVERSED: [lon, lat] format (should be [lat, lon])")
                        all_correct = False
                    else:
                        print(f"   ❌ WRONG: Coordinates out of range")
                        all_correct = False
                except json.JSONDecodeError:
                    print(f"   ⚠️  Could not parse geocoded geometry")
    
    print(f"\n{'='*80}")
    if all_correct:
        print("✅ ALL COORDINATE FORMATS ARE CORRECT!")
        print("   Format: [latitude, longitude]")
        print("   Ready to process all records.")
        print(f"{'='*80}")
        
        # Generate Excel file with results
        print(f"\n{'='*80}")
        print("GENERATING EXCEL FILE WITH RESULTS")
        print(f"{'='*80}")
        
        output_dir = Path(__file__).parent / 'test_outputs'
        output_dir.mkdir(exist_ok=True)
        
        output_file = output_dir / 'geocoding_test_results.xlsx'
        
        # Select only geocoded records for output
        geocoded_mask = (test_gdf['corregir'] == 'INTENTAR GEORREFERENCIAR') if 'corregir' in test_gdf.columns else test_gdf.index.isin(test_gdf.index[:20])
        output_df = test_gdf[geocoded_mask].copy()
        
        # Select relevant columns for comparison (siguiendo el proceso real del pipeline)
        columns_to_export = [
            'upid',
            'nombre_up',
            'nombre_up_detalle',
            'direccion',
            'direccion_api',
            'barrio_vereda',
            'comuna_corregimiento',
            'barrio_vereda_val',
            'barrio_vereda_val_s2',
            'barrio_vereda_val_s3',
            'comunas_corregimientos_val',
            'comunas_corregimientos_val_s2',
            'comuna_corregimiento_val_s3',
            'geometry',
            'geometry_val_s2',
            'corregir'
        ]
        
        # Filter only existing columns
        columns_to_export = [col for col in columns_to_export if col in output_df.columns]
        
        # Create export dataframe
        export_df = output_df[columns_to_export].copy()
        
        # Convert geometry columns to readable format
        if 'geometry' in export_df.columns:
            export_df['Geometria_Original'] = export_df['geometry'].apply(
                lambda g: f"Point({g.x:.6f}, {g.y:.6f})" if hasattr(g, 'x') else str(g)[:100]
            )
            export_df = export_df.drop('geometry', axis=1)
        
        # Add readable geometry from forward geocoding
        if 'geometry_val_s2' in export_df.columns:
            export_df['Geometria_Forward_Geocoding'] = export_df['geometry_val_s2'].apply(
                lambda g: str(g)[:100] if g and g != 'ERROR' else g
            )
            export_df = export_df.drop('geometry_val_s2', axis=1)
        
        # Rename columns for clarity (nombres descriptivos del proceso real)
        column_mapping = {
            'upid': 'ID_Proyecto',
            'nombre_up': 'Nombre_UP',
            'nombre_up_detalle': 'Detalle_UP',
            'direccion': 'Direccion_Origen',
            'direccion_api': 'Direccion_Construccion_API',
            'barrio_vereda': 'Barrio_Vereda_Dato_Origen',
            'comuna_corregimiento': 'Comuna_Corregimiento_Dato_Origen',
            'barrio_vereda_val': 'Barrio_Vereda_Val_S1_Interseccion',
            'barrio_vereda_val_s2': 'Barrio_Vereda_Val_S2_Distancia',
            'barrio_vereda_val_s3': 'Barrio_Vereda_Val_S3_API_Final',
            'comunas_corregimientos_val': 'Comuna_Corregimiento_Val_S1_Interseccion',
            'comunas_corregimientos_val_s2': 'Comuna_Corregimiento_Val_S2_Distancia',
            'comuna_corregimiento_val_s3': 'Comuna_Corregimiento_Val_S3_API_Final',
            'corregir': 'Estado_Necesita_Georreferenciar'
        }
        
        export_df = export_df.rename(columns={k: v for k, v in column_mapping.items() if k in export_df.columns})
        
        # Save to Excel
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            export_df.to_excel(writer, sheet_name='Resultados Geocoding', index=False)
            
            # Auto-adjust column widths
            worksheet = writer.sheets['Resultados Geocoding']
            for idx, col in enumerate(export_df.columns, 1):
                max_length = max(
                    export_df[col].astype(str).map(len).max(),
                    len(str(col))
                )
                adjusted_width = min(max_length + 2, 60)
                column_letter = worksheet.cell(1, idx).column_letter
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"\n✅ Excel file generated successfully!")
        print(f"   Location: {output_file}")
        print(f"   Records: {len(export_df)}")
        print(f"   Columns: {len(export_df.columns)}")
        print(f"\n📋 Columns included:")
        for col in export_df.columns:
            print(f"   • {col}")
        
        return True
    else:
        print("❌ COORDINATE FORMAT ERRORS DETECTED!")
        print("   Expected: [latitude, longitude]")
        print("   Please fix the geocoding code before processing all records.")
        print(f"{'='*80}")
        return False


if __name__ == "__main__":
    success = test_geocoding_sample()
    
    if success:
        print("\n🚀 Test passed! Run full geocoding with:")
        print("   python data_transformation_unidades_proyecto.py --geocode")
    else:
        print("\n⚠️  Test failed! Fix coordinate format issues first.")
        sys.exit(1)
