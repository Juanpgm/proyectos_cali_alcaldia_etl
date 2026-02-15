import geopandas as gpd
import pandas as pd
import json
from database.config import get_firestore_client
from shapely.geometry import Point, LineString, Polygon, MultiLineString, shape
import os
from datetime import datetime

def initialize_firebase():
    """Initialize Firebase connection using centralized config"""
    try:
        client = get_firestore_client()
        print("✓ Firebase initialized using centralized config")
        return client
    except Exception as e:
        print(f"❌ Error initializing Firebase: {e}")
        raise

def load_territorios_geojson(geojson_path):
    """Load the territorios buffer polygons from GeoJSON file"""
    print(f"\nLoading territorios from: {geojson_path}")
    gdf_territorios = gpd.read_file(geojson_path)
    
    # Ensure CRS is WGS84
    if gdf_territorios.crs is None:
        gdf_territorios.set_crs("EPSG:4326", inplace=True)
    elif gdf_territorios.crs != "EPSG:4326":
        gdf_territorios = gdf_territorios.to_crs("EPSG:4326")
    
    print(f"✓ Territorios loaded: {len(gdf_territorios)} polygon(s)")
    print(f"  Geometry type: {gdf_territorios.geometry.geom_type.unique()}")
    
    # Show territorios info
    if 'Territorio' in gdf_territorios.columns:
        print(f"\n  Territorios encontrados:")
        for idx, row in gdf_territorios.iterrows():
            territorio = row.get('Territorio', f'Territorio {idx}')
            comuna = row.get('Comuna', 'N/A')
            direccion = row.get('dirección', 'N/A')
            print(f"    - {territorio} (Comuna {comuna}): {direccion}")
    
    return gdf_territorios

def download_unidades_proyecto_from_firebase(db):
    """Download all unidades_proyecto from Firebase"""
    print("\nDownloading unidades_proyecto from Firebase...")
    
    collection_ref = db.collection('unidades_proyecto')
    docs = collection_ref.stream()
    
    data_list = []
    geometries = []
    
    for doc in docs:
        doc_data = doc.to_dict()
        doc_data['doc_id'] = doc.id
        
        # Extract geometry if exists
        geometry = None
        if doc_data.get('has_geometry') and 'geometry' in doc_data:
            geom_data = doc_data['geometry']
            try:
                if isinstance(geom_data, dict):
                    geometry = shape(geom_data)
                elif isinstance(geom_data, str):
                    geom_dict = json.loads(geom_data)
                    geometry = shape(geom_dict)
            except Exception as e:
                print(f"  Warning: Could not parse geometry for doc {doc.id}: {e}")
        
        geometries.append(geometry)
        
        # Remove geometry from dict to avoid duplication
        if 'geometry' in doc_data:
            del doc_data['geometry']
        
        data_list.append(doc_data)
    
    print(f"✓ Downloaded {len(data_list)} documents from Firebase")
    
    # Create GeoDataFrame
    df = pd.DataFrame(data_list)
    gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:4326")
    
    # Filter only features with valid geometry
    gdf_with_geom = gdf[gdf.geometry.notna()].copy()
    print(f"✓ Features with valid geometry: {len(gdf_with_geom)}")
    
    if len(gdf_with_geom) > 0:
        geom_types = gdf_with_geom.geometry.geom_type.value_counts()
        print(f"  Geometry types distribution:")
        for geom_type, count in geom_types.items():
            print(f"    - {geom_type}: {count}")
    
    return gdf_with_geom

def perform_spatial_intersection(gdf_unidades, gdf_territorios):
    """Perform spatial intersection to find unidades within territorios polygons"""
    print("\nPerforming spatial intersection...")
    
    # Perform spatial join to get which territorio each unidad belongs to
    gdf_intersected = gpd.sjoin(
        gdf_unidades, 
        gdf_territorios, 
        how='inner', 
        predicate='intersects'
    )
    
    print(f"✓ Found {len(gdf_intersected)} unidades_proyecto within territorios")
    
    if len(gdf_intersected) > 0:
        # Show distribution by territorio
        if 'Territorio' in gdf_intersected.columns:
            print(f"\n  Distribution by Territorio:")
            territorio_counts = gdf_intersected['Territorio'].value_counts()
            for territorio, count in territorio_counts.items():
                print(f"    - {territorio}: {count} unidades")
        
        geom_types = gdf_intersected.geometry.geom_type.value_counts()
        print(f"\n  Geometry types in result:")
        for geom_type, count in geom_types.items():
            print(f"    - {geom_type}: {count}")
    
    return gdf_intersected

def export_to_excel(gdf, output_path):
    """Export GeoDataFrame to Excel with geometry coordinates"""
    print(f"\nExporting to Excel: {output_path}")
    
    # Create a copy for export
    df_export = gdf.copy()
    
    # Extract coordinates as string representation
    def geometry_to_coords(geom):
        if geom is None:
            return None
        try:
            if geom.geom_type == 'Point':
                return f"POINT({geom.x}, {geom.y})"
            elif geom.geom_type == 'LineString':
                coords = ', '.join([f"({x}, {y})" for x, y in geom.coords])
                return f"LINESTRING({coords})"
            elif geom.geom_type == 'MultiLineString':
                lines = []
                for line in geom.geoms:
                    coords = ', '.join([f"({x}, {y})" for x, y in line.coords])
                    lines.append(f"({coords})")
                return f"MULTILINESTRING({', '.join(lines)})"
            elif geom.geom_type == 'Polygon':
                coords = ', '.join([f"({x}, {y})" for x, y in geom.exterior.coords])
                return f"POLYGON(({coords}))"
            else:
                return str(geom)
        except Exception as e:
            return f"Error: {str(e)}"
    
    # Add coordinate representation
    df_export['geometry_coords'] = df_export.geometry.apply(geometry_to_coords)
    
    # Get GeoJSON representation
    df_export['geometry_geojson'] = df_export.geometry.apply(
        lambda geom: json.dumps(geom.__geo_interface__) if geom is not None else None
    )
    
    # Drop the geometry column for Excel export
    df_export = df_export.drop(columns=['geometry'])
    
    # Reorder columns to put identifiers and territorio info first
    priority_cols = [
        'doc_id', 'identificador', 'nombre_up', 'nombre_up_detalle', 
        'direccion', 'estado', 'Territorio', 'Comuna', 'dirección', 
        'geometry_type', 'geometry_coords'
    ]
    other_cols = [col for col in df_export.columns if col not in priority_cols]
    
    # Filter to only existing columns
    existing_priority_cols = [col for col in priority_cols if col in df_export.columns]
    final_cols = existing_priority_cols + other_cols
    
    df_export = df_export[final_cols]
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    
    # Export to Excel
    df_export.to_excel(output_path, index=False, engine='openpyxl')
    
    print(f"✓ Excel file created successfully!")
    print(f"  Total rows: {len(df_export)}")
    print(f"  Total columns: {len(df_export.columns)}")

def main():
    """Main execution function"""
    print("="*70)
    print("SPATIAL INTERSECTION: Unidades Proyecto x Territorios Microterritorios")
    print("="*70)
    
    # Paths
    territorios_path = r"basemaps\microtios\Territorios_buffer_150m.geojson"
    output_excel = r"app_outputs\unidades_proyecto_territorios_microterritorios.xlsx"
    
    try:
        # 1. Initialize Firebase
        db = initialize_firebase()
        
        # 2. Load territorios polygons
        gdf_territorios = load_territorios_geojson(territorios_path)
        
        # 3. Download unidades_proyecto from Firebase
        gdf_unidades = download_unidades_proyecto_from_firebase(db)
        
        if len(gdf_unidades) == 0:
            print("\n⚠ No unidades_proyecto with geometry found in Firebase")
            return
        
        # 4. Perform spatial intersection
        gdf_result = perform_spatial_intersection(gdf_unidades, gdf_territorios)
        
        if len(gdf_result) == 0:
            print("\n⚠ No unidades_proyecto found within territorios polygons")
            return
        
        # 5. Export to Excel
        export_to_excel(gdf_result, output_excel)
        
        # 6. Summary
        print("\n" + "="*70)
        print("SUMMARY")
        print("="*70)
        print(f"Total unidades_proyecto in Firebase: {len(gdf_unidades)}")
        print(f"Total territorios polygons: {len(gdf_territorios)}")
        print(f"Unidades within Territorios: {len(gdf_result)}")
        print(f"Percentage: {len(gdf_result)/len(gdf_unidades)*100:.2f}%")
        print(f"\nOutput file: {output_excel}")
        print("="*70)
        
        # Optional: Also save as GeoJSON
        output_geojson = r"app_outputs\unidades_proyecto_territorios_microterritorios.geojson"
        gdf_result.to_file(output_geojson, driver='GeoJSON')
        print(f"✓ GeoJSON also saved: {output_geojson}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
