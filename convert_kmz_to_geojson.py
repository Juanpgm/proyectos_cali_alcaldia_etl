import geopandas as gpd
import fiona
import os
from pathlib import Path

# Enable KML driver
fiona.drvsupport.supported_drivers['KML'] = 'rw'
fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'

def convert_kmz_to_geojson(kmz_path, output_path=None):
    """
    Convert KMZ file to GeoJSON format
    
    Parameters:
    -----------
    kmz_path : str
        Path to the input KMZ file
    output_path : str, optional
        Path for the output GeoJSON file. If None, uses same name as input with .geojson extension
    
    Returns:
    --------
    str : Path to the created GeoJSON file
    """
    
    # Validate input file exists
    if not os.path.exists(kmz_path):
        raise FileNotFoundError(f"KMZ file not found: {kmz_path}")
    
    print(f"Reading KMZ file: {kmz_path}")
    
    # Read the KMZ file using geopandas
    # KMZ is a zipped KML, geopandas can handle it with the right path format
    gdf = gpd.read_file(f"zip://{kmz_path}")
    
    print(f"Successfully loaded {len(gdf)} features")
    print(f"Geometry types: {gdf.geometry.geom_type.unique()}")
    print(f"CRS: {gdf.crs}")
    print(f"\nColumns: {gdf.columns.tolist()}")
    
    # If no output path specified, create one based on input filename
    if output_path is None:
        input_path = Path(kmz_path)
        output_path = input_path.parent / f"{input_path.stem}.geojson"
    
    # Ensure output directory exists
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Convert to WGS84 if not already (standard for GeoJSON)
    if gdf.crs and gdf.crs != "EPSG:4326":
        print(f"Converting from {gdf.crs} to EPSG:4326 (WGS84)")
        gdf = gdf.to_crs("EPSG:4326")
    
    # Save as GeoJSON
    print(f"\nSaving GeoJSON to: {output_path}")
    gdf.to_file(output_path, driver='GeoJSON')
    
    print(f"✓ Conversion completed successfully!")
    print(f"✓ Output file: {output_path}")
    
    # Display basic info about the data
    print(f"\n{'='*60}")
    print("Data Preview:")
    print(f"{'='*60}")
    print(gdf.head())
    
    return str(output_path)


if __name__ == "__main__":
    # Input KMZ file path
    kmz_file = r"basemaps\pulmon_oriente\PoligonoPropuestoPulmonDeOriente.kmz"
    
    # Output GeoJSON file path (optional - will auto-generate if not specified)
    output_file = r"basemaps\pulmon_oriente\PoligonoPropuestoPulmonDeOriente.geojson"
    
    try:
        # Convert KMZ to GeoJSON
        result_path = convert_kmz_to_geojson(kmz_file, output_file)
        
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        import traceback
        traceback.print_exc()
