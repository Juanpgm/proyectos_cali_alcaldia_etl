import geopandas as gpd
import os
from pathlib import Path

def convert_shapefile_to_geojson(shp_path, output_path=None):
    """
    Convert Shapefile to GeoJSON format
    
    Parameters:
    -----------
    shp_path : str
        Path to the input Shapefile (.shp)
    output_path : str, optional
        Path for the output GeoJSON file. If None, uses same name as input with .geojson extension
    
    Returns:
    --------
    str : Path to the created GeoJSON file
    """
    
    # Validate input file exists
    if not os.path.exists(shp_path):
        raise FileNotFoundError(f"Shapefile not found: {shp_path}")
    
    print(f"Reading Shapefile: {shp_path}")
    
    # Read the Shapefile using geopandas
    gdf = gpd.read_file(shp_path)
    
    print(f"✓ Successfully loaded {len(gdf)} features")
    print(f"  Geometry types: {gdf.geometry.geom_type.unique()}")
    print(f"  CRS: {gdf.crs}")
    print(f"  Columns: {gdf.columns.tolist()}")
    
    # If no output path specified, create one based on input filename
    if output_path is None:
        input_path = Path(shp_path)
        output_path = input_path.parent / f"{input_path.stem}.geojson"
    
    # Ensure output directory exists
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Convert to WGS84 if not already (standard for GeoJSON)
    if gdf.crs and gdf.crs != "EPSG:4326":
        print(f"Converting from {gdf.crs} to EPSG:4326 (WGS84)")
        gdf = gdf.to_crs("EPSG:4326")
    elif gdf.crs is None:
        print("⚠ Warning: No CRS defined. Assuming EPSG:4326")
        gdf.set_crs("EPSG:4326", inplace=True)
    
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
    
    # Display geometry bounds
    bounds = gdf.total_bounds
    print(f"\n{'='*60}")
    print("Spatial Extent:")
    print(f"{'='*60}")
    print(f"  Min X (West):  {bounds[0]:.6f}")
    print(f"  Min Y (South): {bounds[1]:.6f}")
    print(f"  Max X (East):  {bounds[2]:.6f}")
    print(f"  Max Y (North): {bounds[3]:.6f}")
    
    return str(output_path)


if __name__ == "__main__":
    # Input Shapefile path
    shp_file = r"basemaps\microtios\Territorios_buffer_150m.shp"
    
    # Output GeoJSON file path (optional - will auto-generate if not specified)
    output_file = r"basemaps\microtios\Territorios_buffer_150m.geojson"
    
    try:
        # Convert Shapefile to GeoJSON
        result_path = convert_shapefile_to_geojson(shp_file, output_file)
        
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        import traceback
        traceback.print_exc()
