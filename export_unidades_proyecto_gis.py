import geopandas as gpd
import pandas as pd
import json
import firebase_admin
from firebase_admin import credentials, firestore
from shapely.geometry import Point, LineString, Polygon, MultiLineString, shape
import os
from pathlib import Path
import zipfile
import fiona

# Enable KML driver
fiona.drvsupport.supported_drivers['KML'] = 'rw'

def initialize_firebase():
    """Initialize Firebase connection"""
    try:
        firebase_admin.get_app()
        print("✓ Firebase already initialized")
    except ValueError:
        cred = credentials.Certificate('target-credentials.json')
        firebase_admin.initialize_app(cred)
        print("✓ Firebase initialized successfully")
    
    return firestore.client()

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

def clean_column_names(gdf):
    """Clean column names to be compatible with Shapefile format"""
    # Shapefile has 10 character limit for column names
    # Create mapping for shorter names
    name_mapping = {}
    for col in gdf.columns:
        if col != 'geometry':
            # Truncate to 10 chars and make unique
            short_name = col[:10]
            counter = 1
            original_short = short_name
            while short_name in name_mapping.values():
                short_name = f"{original_short[:8]}{counter}"
                counter += 1
            name_mapping[col] = short_name
    
    return name_mapping

def export_to_shapefile(gdf, output_dir):
    """Export GeoDataFrame to Shapefile format"""
    print(f"\nExporting to Shapefile...")
    
    # Create output directory
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Shapefile path
    shp_path = output_dir / "unidades_proyecto.shp"
    
    # Create a copy for export
    gdf_export = gdf.copy()
    
    # Clean column names for shapefile compatibility
    name_mapping = clean_column_names(gdf_export)
    
    # Save original column names to a separate file
    mapping_file = output_dir / "column_names_mapping.txt"
    with open(mapping_file, 'w', encoding='utf-8') as f:
        f.write("Shapefile Column Name -> Original Column Name\n")
        f.write("="*60 + "\n")
        for original, short in name_mapping.items():
            f.write(f"{short:12} -> {original}\n")
    
    print(f"  Column name mapping saved to: {mapping_file}")
    
    # Rename columns
    gdf_export.rename(columns=name_mapping, inplace=True)
    
    # Convert complex data types to strings for Shapefile compatibility
    for col in gdf_export.columns:
        if col != 'geometry':
            if gdf_export[col].dtype == 'object':
                gdf_export[col] = gdf_export[col].astype(str)
            elif pd.api.types.is_datetime64_any_dtype(gdf_export[col]):
                gdf_export[col] = gdf_export[col].astype(str)
    
    # Export to Shapefile
    print(f"  Creating shapefile: {shp_path}")
    gdf_export.to_file(shp_path, driver='ESRI Shapefile', encoding='utf-8')
    
    print(f"✓ Shapefile created successfully!")
    print(f"  Location: {shp_path}")
    
    # List all created files
    shp_files = list(output_dir.glob("unidades_proyecto.*"))
    print(f"  Files created:")
    for f in shp_files:
        print(f"    - {f.name} ({os.path.getsize(f) / 1024:.2f} KB)")
    
    return str(shp_path)

def export_to_kmz(gdf, output_dir):
    """Export GeoDataFrame to KMZ format"""
    print(f"\nExporting to KMZ...")
    
    # Create output directory
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Paths
    kml_path = output_dir / "unidades_proyecto.kml"
    kmz_path = output_dir / "unidades_proyecto.kmz"
    
    # Create a copy for export
    gdf_export = gdf.copy()
    
    # Create Name field for better display
    if 'nombre_up' in gdf_export.columns:
        gdf_export['Name'] = gdf_export['nombre_up']
    elif 'identificador' in gdf_export.columns:
        gdf_export['Name'] = gdf_export['identificador']
    else:
        gdf_export['Name'] = [f"Feature {i+1}" for i in range(len(gdf_export))]
    
    # Create Description with key information
    def create_description(row):
        desc_parts = []
        
        # Key fields
        key_fields = [
            ('identificador', 'Identificador'),
            ('nombre_up', 'Nombre'),
            ('nombre_up_detalle', 'Detalle'),
            ('direccion', 'Dirección'),
            ('estado', 'Estado'),
            ('tipo_intervencion', 'Tipo Intervención'),
            ('comuna_corregimiento', 'Comuna'),
            ('barrio_vereda', 'Barrio'),
            ('presupuesto_base', 'Presupuesto'),
            ('avance_obra', 'Avance %'),
            ('ano', 'Año')
        ]
        
        for field, label in key_fields:
            if field in row and row[field] and str(row[field]) != 'None':
                value = row[field]
                if field == 'presupuesto_base':
                    try:
                        value = f"${float(value):,.0f}"
                    except:
                        pass
                desc_parts.append(f"<b>{label}:</b> {value}")
        
        return "<br>".join(desc_parts) if desc_parts else "Sin información"
    
    gdf_export['Description'] = gdf_export.apply(create_description, axis=1)
    
    # Reorder columns
    cols = ['Name', 'Description'] + [col for col in gdf_export.columns 
                                       if col not in ['Name', 'Description', 'geometry']] + ['geometry']
    gdf_export = gdf_export[[col for col in cols if col in gdf_export.columns]]
    
    # Export to KML
    print(f"  Creating KML: {kml_path}")
    gdf_export.to_file(kml_path, driver='KML')
    
    # Create KMZ (compressed KML)
    print(f"  Creating KMZ: {kmz_path}")
    with zipfile.ZipFile(kmz_path, 'w', zipfile.ZIP_DEFLATED) as kmz:
        kmz.write(kml_path, arcname='doc.kml')
    
    # Clean up temporary KML
    if kml_path.exists():
        os.remove(kml_path)
    
    print(f"✓ KMZ created successfully!")
    print(f"  Location: {kmz_path}")
    print(f"  Size: {os.path.getsize(kmz_path) / 1024:.2f} KB")
    
    return str(kmz_path)

def main():
    """Main execution function"""
    print("="*70)
    print("EXPORT: Unidades Proyecto from Firebase to Shapefile and KMZ")
    print("="*70)
    
    # Output directory
    output_dir = "app_outputs/gis_exports"
    
    try:
        # 1. Initialize Firebase
        db = initialize_firebase()
        
        # 2. Download unidades_proyecto from Firebase
        gdf = download_unidades_proyecto_from_firebase(db)
        
        if len(gdf) == 0:
            print("\n⚠ No unidades_proyecto with geometry found in Firebase")
            return
        
        # 3. Export to Shapefile
        shp_file = export_to_shapefile(gdf, output_dir)
        
        # 4. Export to KMZ
        kmz_file = export_to_kmz(gdf, output_dir)
        
        # 5. Summary
        print("\n" + "="*70)
        print("EXPORT COMPLETE")
        print("="*70)
        print(f"Total features exported: {len(gdf)}")
        print(f"\nOutput directory: {output_dir}")
        print(f"\nFiles for QGIS:")
        print(f"  • Shapefile: {shp_file}")
        print(f"  • KMZ: {kmz_file}")
        print("\nHow to use in QGIS:")
        print("  1. Open QGIS")
        print("  2. Go to Layer → Add Layer → Add Vector Layer")
        print("  3. Browse to the .shp or .kmz file")
        print("  4. Click 'Add'")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
