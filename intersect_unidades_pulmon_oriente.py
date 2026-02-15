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

def load_polygon_geojson(geojson_path):
    """Load the polygon from GeoJSON file"""
    print(f"\nLoading polygon from: {geojson_path}")
    gdf_polygon = gpd.read_file(geojson_path)
    
    # Ensure CRS is WGS84
    if gdf_polygon.crs is None:
        gdf_polygon.set_crs("EPSG:4326", inplace=True)
    elif gdf_polygon.crs != "EPSG:4326":
        gdf_polygon = gdf_polygon.to_crs("EPSG:4326")
    
    print(f"✓ Polygon loaded: {len(gdf_polygon)} feature(s)")
    print(f"  Geometry type: {gdf_polygon.geometry.geom_type.unique()}")
    
    return gdf_polygon

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

def perform_spatial_intersection(gdf_unidades, gdf_polygon):
    """Perform spatial intersection to find unidades within polygon"""
    print("\nPerforming spatial intersection...")
    
    # Get the polygon geometry (assuming single polygon)
    polygon = gdf_polygon.geometry.iloc[0]
    
    # Perform spatial intersection
    # Using 'within' for features completely within the polygon
    # Or 'intersects' for features that touch or overlap
    gdf_intersected = gdf_unidades[gdf_unidades.geometry.intersects(polygon)].copy()
    
    print(f"✓ Found {len(gdf_intersected)} unidades_proyecto within/intersecting the polygon")
    
    if len(gdf_intersected) > 0:
        geom_types = gdf_intersected.geometry.geom_type.value_counts()
        print(f"  Geometry types in result:")
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
    
    # Reorder columns to put identifiers first
    priority_cols = ['doc_id', 'identificador', 'nombre_up', 'nombre_up_detalle', 
                     'direccion', 'estado', 'geometry_type', 'geometry_coords']
    other_cols = [col for col in df_export.columns if col not in priority_cols]
    
    # Filter to only existing columns
    existing_priority_cols = [col for col in priority_cols if col in df_export.columns]
    final_cols = existing_priority_cols + other_cols
    
    df_export = df_export[final_cols]
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Export to Excel
    df_export.to_excel(output_path, index=False, engine='openpyxl')
    
    print(f"✓ Excel file created successfully!")
    print(f"  Total rows: {len(df_export)}")
    print(f"  Total columns: {len(df_export.columns)}")

def main():
    """Main execution function"""
    print("="*70)
    print("SPATIAL INTERSECTION: Unidades Proyecto x Pulmón de Oriente")
    print("="*70)
    
    # Paths
    polygon_path = r"basemaps\pulmon_oriente\PoligonoPropuestoPulmonDeOriente.geojson"
    output_excel = r"app_outputs\unidades_proyecto_pulmon_oriente.xlsx"
    
    try:
        # 1. Initialize Firebase
        db = initialize_firebase()
        
        # 2. Load polygon
        gdf_polygon = load_polygon_geojson(polygon_path)
        
        # 3. Download unidades_proyecto from Firebase
        gdf_unidades = download_unidades_proyecto_from_firebase(db)
        
        if len(gdf_unidades) == 0:
            print("\n⚠ No unidades_proyecto with geometry found in Firebase")
            return
        
        # 4. Perform spatial intersection
        gdf_result = perform_spatial_intersection(gdf_unidades, gdf_polygon)
        
        if len(gdf_result) == 0:
            print("\n⚠ No unidades_proyecto found within the polygon")
            return
        
        # 5. Export to Excel
        export_to_excel(gdf_result, output_excel)
        
        # 6. Summary
        print("\n" + "="*70)
        print("SUMMARY")
        print("="*70)
        print(f"Total unidades_proyecto in Firebase: {len(gdf_unidades)}")
        print(f"Unidades within Pulmón de Oriente: {len(gdf_result)}")
        print(f"Percentage: {len(gdf_result)/len(gdf_unidades)*100:.2f}%")
        print(f"\nOutput file: {output_excel}")
        print("="*70)
        
        # Optional: Also save as GeoJSON
        output_geojson = r"app_outputs\unidades_proyecto_pulmon_oriente.geojson"
        gdf_result.to_file(output_geojson, driver='GeoJSON')
        print(f"✓ GeoJSON also saved: {output_geojson}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
