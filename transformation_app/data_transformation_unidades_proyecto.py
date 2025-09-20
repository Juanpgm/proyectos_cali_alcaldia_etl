# -*- coding: utf-8 -*-
"""
Data transformation module for project units (unidades de proyecto) with geospatial data processing.
Optimized for FastAPI compatibility, scalability, and map visualization.
"""

import os
import pandas as pd
import json
import numpy as np
from typing import Optional, Dict, List, Any, Tuple
from datetime import datetime
from functools import reduce, partial
from functools import reduce, partial
from operator import methodcaller


# Functional programming utilities
def compose(*functions):
    """Compose multiple functions into a single function."""
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(value, *functions):
    """Apply a sequence of functions to a value (pipe operator)."""
    return reduce(lambda acc, func: func(acc), functions, value)


def curry(func):
    """Convert a function to a curried version."""
    def curried(*args, **kwargs):
        if len(args) + len(kwargs) >= func.__code__.co_argcount:
            return func(*args, **kwargs)
        return lambda *more_args, **more_kwargs: curried(*(args + more_args), **dict(kwargs, **more_kwargs))
    return curried


# Functional data cleaning utilities
def clean_numeric_column(df: pd.DataFrame, column_name: str, default_value: float = 0.0) -> pd.DataFrame:
    """Clean a numeric column using functional approach."""
    if column_name in df.columns:
        df = df.copy()
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').fillna(default_value)
    return df


def clean_monetary_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Clean a monetary column using functional approach."""
    if column_name in df.columns:
        df = df.copy()
        df[column_name] = df[column_name].apply(
            lambda x: clean_monetary_value(x) if pd.notna(x) else 0.00
        )
        # Ensure it's numeric before rounding
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').fillna(0.0)
    return df


def apply_functional_cleaning(df: pd.DataFrame, monetary_cols: List[str], numeric_cols: List[str]) -> pd.DataFrame:
    """Apply data cleaning using functional composition."""
    result_df = df.copy()
    
    # Apply monetary cleaning
    for col in monetary_cols:
        result_df = clean_monetary_column(result_df, col)
    
    # Apply numeric cleaning
    for col in numeric_cols:
        result_df = clean_numeric_column(result_df, col)
    
    return result_df


def normalize_column_names(columns):
    """Normalize column names by converting to lowercase and removing special characters."""
    return [col.lower().strip().replace(' ', '_').replace('.', '_').replace('(', '').replace(')', '') for col in columns]


def clean_monetary_value(value):
    """Clean monetary values by removing currency symbols and thousands separators."""
    if pd.isna(value) or value is None:
        return None
    
    # Convert to string first
    str_value = str(value).strip()
    
    # Handle special cases
    if str_value == '-' or str_value == '' or str_value.lower() == 'nan' or str_value.lower() == 'null':
        return None
    
    # Remove currency symbols and spaces
    cleaned = str_value.replace('$', '').replace(' ', '').strip()
    
    # Handle the case where cleaned string is just "-" after cleaning
    if cleaned == '-' or cleaned == '':
        return 0.00
    
    try:
        # Handle different decimal formats
        # Case 1: European format with dots as thousands separators (155.521.600)
        if '.' in cleaned and ',' not in cleaned:
            # Check if it's likely thousands separators (multiple dots or number after last dot > 2 digits)
            parts = cleaned.split('.')
            if len(parts) > 2 or (len(parts) == 2 and len(parts[1]) > 2):
                # Treat as thousands separators, remove all dots
                cleaned = cleaned.replace('.', '')
        
        # Case 2: Mixed format with commas and dots ($224,436,000.00)
        elif ',' in cleaned and '.' in cleaned:
            # Assume comma is thousands separator, dot is decimal
            cleaned = cleaned.replace(',', '')
        
        # Case 3: Only commas (could be thousands or decimal)
        elif ',' in cleaned and '.' not in cleaned:
            # Check position of comma
            comma_pos = cleaned.rfind(',')
            if len(cleaned) - comma_pos - 1 <= 2:
                # Last comma has 1-2 digits after, likely decimal
                cleaned = cleaned.replace(',', '.')
            else:
                # Remove commas (thousands separators)
                cleaned = cleaned.replace(',', '')
        
        # Convert to float
        result = float(cleaned)
        return round(result, 2)  # Always return with 2 decimal places
        
    except (ValueError, TypeError) as e:
        print(f"    Error cleaning '{str_value}': {e} - Setting to 0.00")
        return 0.00


def parse_geojson_geometry(geom_str: str) -> Optional[Dict[str, Any]]:
    """
    Parse GeoJSON geometry string and validate format.
    
    Args:
        geom_str: String containing GeoJSON geometry
        
    Returns:
        Dict containing parsed geometry or None if invalid
    """
    if pd.isna(geom_str) or not geom_str:
        return None
    
    try:
        geom_obj = json.loads(geom_str)
        
        # Validate required fields
        if 'type' not in geom_obj or 'coordinates' not in geom_obj:
            return None
        
        # Validate geometry type
        valid_types = ['Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon']
        if geom_obj['type'] not in valid_types:
            return None
        
        return geom_obj
    
    except (json.JSONDecodeError, TypeError):
        return None


def extract_coordinates_info(geom_obj: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[Dict]]:
    """
    Extract coordinate information from geometry object.
    
    Args:
        geom_obj: Parsed GeoJSON geometry object
        
    Returns:
        Tuple of (longitude, latitude, coordinate_bounds) for the geometry centroid/representative point
    """
    if not geom_obj or 'coordinates' not in geom_obj:
        return None, None, None
    
    geom_type = geom_obj['type']
    coords = geom_obj['coordinates']
    
    try:
        if geom_type == 'Point':
            # For Point: coordinates are [lon, lat] or [lat, lon, elevation]
            lon, lat = coords[0], coords[1]
            bounds = {'min_lon': lon, 'max_lon': lon, 'min_lat': lat, 'max_lat': lat}
            return lon, lat, bounds
        
        elif geom_type == 'LineString':
            # For LineString: coordinates are [[lon1, lat1], [lon2, lat2], ...]
            if coords:
                lons = [coord[0] for coord in coords]
                lats = [coord[1] for coord in coords]
                
                # Calculate centroid
                center_lon = sum(lons) / len(lons)
                center_lat = sum(lats) / len(lats)
                
                # Calculate bounds
                bounds = {
                    'min_lon': min(lons),
                    'max_lon': max(lons),
                    'min_lat': min(lats),
                    'max_lat': max(lats)
                }
                
                return center_lon, center_lat, bounds
        
        elif geom_type == 'Polygon':
            # For Polygon: coordinates are [[[lon1, lat1], [lon2, lat2], ...]]
            if coords and coords[0]:
                exterior_ring = coords[0]
                lons = [coord[0] for coord in exterior_ring]
                lats = [coord[1] for coord in exterior_ring]
                
                # Calculate centroid
                center_lon = sum(lons) / len(lons)
                center_lat = sum(lats) / len(lats)
                
                # Calculate bounds
                bounds = {
                    'min_lon': min(lons),
                    'max_lon': max(lons),
                    'min_lat': min(lats),
                    'max_lat': max(lats)
                }
                
                return center_lon, center_lat, bounds
        
        # For other complex geometries, return None for now
        return None, None, None
    
    except (IndexError, TypeError, ValueError):
        return None, None, None


def create_feature_collection(dataframe: pd.DataFrame) -> Dict[str, Any]:
    """
    Create a GeoJSON FeatureCollection from a dataframe with geometry data.
    Preserves ALL original columns from the source data.
    INCLUDES ALL RECORDS, using lat/lon to create geometry when geom is invalid.
    
    Args:
        dataframe: DataFrame containing geometry and properties
        
    Returns:
        Dict containing GeoJSON FeatureCollection
    """
    features = []
    
    for idx, row in dataframe.iterrows():
        try:
            # Try to parse existing geometry first
            geom_obj = None
            if 'geom' in row and pd.notna(row['geom']):
                geom_obj = parse_geojson_geometry(row['geom'])
            
            # If no valid geometry found, try to create from lat/lon
            if not geom_obj:
                lat_val = None
                lon_val = None
                
                # Try to get latitude
                for lat_col in ['lat', 'latitude']:
                    if lat_col in row and pd.notna(row[lat_col]):
                        try:
                            # Handle European decimal format (comma as decimal separator)
                            lat_str = str(row[lat_col]).replace(',', '.')
                            lat_val = float(lat_str)
                            break
                        except (ValueError, TypeError):
                            continue
                
                # Try to get longitude  
                for lon_col in ['lon', 'longitude']:
                    if lon_col in row and pd.notna(row[lon_col]):
                        try:
                            # Handle European decimal format (comma as decimal separator)
                            lon_str = str(row[lon_col]).replace(',', '.')
                            lon_val = float(lon_str)
                            break
                        except (ValueError, TypeError):
                            continue
                
                # Create Point geometry if we have both lat and lon
                if lat_val is not None and lon_val is not None:
                    # Validate coordinate ranges
                    if -90 <= lat_val <= 90 and -180 <= lon_val <= 180:
                        geom_obj = {
                            "type": "Point",
                            "coordinates": [lon_val, lat_val]  # GeoJSON format: [longitude, latitude]
                        }
                        print(f"  Created Point geometry from lat/lon for row {idx}: [{lon_val}, {lat_val}]")
                    else:
                        print(f"  Invalid lat/lon coordinates for row {idx}: lat={lat_val}, lon={lon_val}")
            
            # Create properties - Include ALL columns except geometry columns
            properties = {}
            geometry_columns = ['geom', 'geometry', 'geometria']  # Only exclude actual geometry columns
            
            for col, value in row.items():
                if col not in geometry_columns:
                    # Handle different data types for JSON serialization
                    if pd.isna(value):
                        properties[col] = None
                    elif isinstance(value, (np.int64, np.int32, pd.Int64Dtype)):
                        properties[col] = int(value)
                    elif isinstance(value, (np.float64, np.float32)):
                        # Round coordinates to 6 decimal places, other floats to 2
                        if col in ['longitude', 'latitude', 'lat', 'lon']:
                            properties[col] = round(float(value), 6)
                        else:
                            properties[col] = round(float(value), 2)
                    elif isinstance(value, float):
                        # Handle regular Python floats
                        if col in ['longitude', 'latitude', 'lat', 'lon']:
                            properties[col] = round(value, 6)
                        else:
                            properties[col] = round(value, 2)
                    elif isinstance(value, bool):
                        properties[col] = bool(value)
                    else:
                        # Convert to string for other types
                        properties[col] = str(value) if value is not None else None
            
            # Create feature (geometry can be null for records without any coordinate info)
            feature = {
                "type": "Feature", 
                "geometry": geom_obj,  # Can be null if no coordinates available
                "properties": properties
            }
            
            features.append(feature)
            
        except Exception as e:
            print(f"Warning: Could not create feature for row {idx}: {e}")
            continue
    
    # Create FeatureCollection
    feature_collection = {
        "type": "FeatureCollection",
        "features": features
    }
    
    return feature_collection


def unidades_proyecto_transformer(data_directory: str = "app_inputs/unidades_proyecto_input") -> Optional[pd.DataFrame]:
    """
    Transform project units data for infrastructure equipment.
    Creates GeoJSON output optimized for map visualization.
    
    Args:
        data_directory (str): Path to the directory containing Excel files
        
    Returns:
        DataFrame with equipamientos data or None if failed
    """
    
    # Get the absolute path to the data directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    directory_path = os.path.join(current_dir, data_directory)
    
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"Directory not found: {directory_path}")
    
    print(f"Loading data from: {directory_path}")
    
    # Define file paths - only equipamientos
    equipamientos_path = os.path.join(directory_path, "obras_equipamientos.xlsx")
    
    # Check if file exists
    if not os.path.exists(equipamientos_path):
        raise FileNotFoundError(f"File not found: {equipamientos_path}")
    
    # Load Excel file
    print("Loading obras_equipamientos.xlsx...")
    df_equipamientos = pd.read_excel(equipamientos_path)
    print(f"Loaded {len(df_equipamientos)} rows from obras_equipamientos.xlsx")
    
    # ================================
    # PROCESS EQUIPAMIENTOS DATA - PRESERVE ALL ORIGINAL COLUMNS
    # ================================
    print("\n" + "="*60)
    print("PROCESSING EQUIPAMIENTOS DATA")
    print("="*60)
    
    # Work directly with the original DataFrame to preserve all columns
    print("Preserving all original columns and adding standardized processing...")
    
    # Create a copy to work with
    unidad_proyecto_infraestructura_equipamientos = df_equipamientos.copy()
    
    # Add computed columns without removing originals
    print("Adding computed coordinate and geometry information...")
    
    # Initialize new columns for computed values
    unidad_proyecto_infraestructura_equipamientos['longitude'] = None
    unidad_proyecto_infraestructura_equipamientos['latitude'] = None
    unidad_proyecto_infraestructura_equipamientos['geometry_bounds'] = None
    unidad_proyecto_infraestructura_equipamientos['geometry_type'] = None
    unidad_proyecto_infraestructura_equipamientos['processed_timestamp'] = datetime.now().isoformat()
    
    # Clean and standardize data types while preserving original columns
    print("Cleaning and standardizing data types...")
    
    # Clean text columns
    text_columns = ['nickname_detalle', 'direccion', 'descripcion_intervencion', 'identificador', 'nickname']
    for col in text_columns:
        if col in unidad_proyecto_infraestructura_equipamientos.columns:
            unidad_proyecto_infraestructura_equipamientos[col] = unidad_proyecto_infraestructura_equipamientos[col].apply(
                lambda x: None if pd.isna(x) else str(x).strip()
            )
    
    # Clean numeric columns (monetary and percentages)
    numeric_columns = ['presupuesto_base', 'ppto_base', 'avance_obra', 'avance_fisico_obra', 'usuarios']
    for col in numeric_columns:
        if col in unidad_proyecto_infraestructura_equipamientos.columns:
            unidad_proyecto_infraestructura_equipamientos[col] = pd.to_numeric(
                unidad_proyecto_infraestructura_equipamientos[col], errors='coerce'
            ).fillna(0.0)
    
    # Clean BPIN if present
    if 'bpin' in unidad_proyecto_infraestructura_equipamientos.columns:
        unidad_proyecto_infraestructura_equipamientos['bpin'] = unidad_proyecto_infraestructura_equipamientos['bpin'].apply(
            lambda x: int(float(x)) if pd.notna(x) and str(x).replace('.', '').isdigit() else None
        )
    
    # Clean boolean columns
    boolean_columns = ['centros_gravedad']
    for col in boolean_columns:
        if col in unidad_proyecto_infraestructura_equipamientos.columns:
            unidad_proyecto_infraestructura_equipamientos[col] = unidad_proyecto_infraestructura_equipamientos[col].astype(bool)
    
    # ================================
    # GEOSPATIAL PROCESSING - PRESERVE ALL COLUMNS
    # ================================
    print("\n" + "="*60)
    print("PROCESSING GEOSPATIAL DATA")
    print("="*60)
    
    # Process geometries for equipamientos
    print(f"\nProcessing geometries for equipamientos...")
    
    valid_geoms = 0
    invalid_geoms = 0
    
    # Process each row and extract geospatial information
    for idx, row in unidad_proyecto_infraestructura_equipamientos.iterrows():
        # Try different geometry column names
        geom_value = None
        for geom_col in ['geom', 'geometry', 'geometria']:
            if geom_col in row and pd.notna(row[geom_col]):
                geom_value = row[geom_col]
                break
        
        if geom_value:
            geom_obj = parse_geojson_geometry(geom_value)
            
            if geom_obj:
                valid_geoms += 1
                
                # Extract coordinates and bounds
                lon, lat, bounds = extract_coordinates_info(geom_obj)
                unidad_proyecto_infraestructura_equipamientos.at[idx, 'longitude'] = round(lon, 6) if lon is not None else None
                unidad_proyecto_infraestructura_equipamientos.at[idx, 'latitude'] = round(lat, 6) if lat is not None else None
                unidad_proyecto_infraestructura_equipamientos.at[idx, 'geometry_bounds'] = json.dumps(bounds) if bounds else None
                unidad_proyecto_infraestructura_equipamientos.at[idx, 'geometry_type'] = geom_obj['type']
            else:
                invalid_geoms += 1
        else:
            invalid_geoms += 1
    
    print(f"  Valid geometries: {valid_geoms}")
    print(f"  Invalid geometries: {invalid_geoms}")
    if valid_geoms + invalid_geoms > 0:
        print(f"  Success rate: {valid_geoms/(valid_geoms+invalid_geoms)*100:.1f}%")
    else:
        print(f"  Success rate: N/A (no data)")
    
    # ================================
    # DATA SUMMARY - ALL COLUMNS PRESERVED
    # ================================
    print("\n" + "="*60)
    print("DATA TRANSFORMATION SUMMARY")
    print("="*60)
    
    print(f"\nUnidad Proyecto Infraestructura Equipamientos:")
    print(f"  Rows: {len(unidad_proyecto_infraestructura_equipamientos)}")
    print(f"  Total columns (ALL PRESERVED): {len(unidad_proyecto_infraestructura_equipamientos.columns)}")
    print(f"  Original columns preserved: {len([col for col in df_equipamientos.columns if col in unidad_proyecto_infraestructura_equipamientos.columns])}")
    print(f"  New computed columns added: {len([col for col in unidad_proyecto_infraestructura_equipamientos.columns if col not in df_equipamientos.columns])}")
    print(f"  Valid geometries: {unidad_proyecto_infraestructura_equipamientos['geometry_type'].notna().sum()}")
    
    # Show column list for verification
    print(f"\nAll columns included:")
    for i, col in enumerate(sorted(unidad_proyecto_infraestructura_equipamientos.columns)):
        print(f"  {i+1:2d}. {col}")
    
    return unidad_proyecto_infraestructura_equipamientos


def save_equipamientos_geojson(df_equipamientos: pd.DataFrame, output_directory: str = "app_outputs/unidades_proyecto_outputs"):
    """
    Save the processed equipamientos data as GeoJSON file.
    
    Args:
        df_equipamientos: Processed equipamientos dataframe
        output_directory: Directory to save output files
    """
    
    # Create output directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, output_directory)
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\n" + "="*60)
    print("SAVING GEOJSON DATA")
    print("="*60)
    print(f"Output directory: {output_dir}")
    
    try:
        # Create GeoJSON FeatureCollection
        feature_collection = create_feature_collection(df_equipamientos)
        
        # Save GeoJSON file
        geojson_filepath = os.path.join(output_dir, "unidades_proyecto_equipamientos.geojson")
        with open(geojson_filepath, 'w', encoding='utf-8') as f:
            json.dump(feature_collection, f, indent=2, ensure_ascii=False)
        
        file_size = os.path.getsize(geojson_filepath) / 1024  # Size in KB
        feature_count = len(feature_collection['features'])
        
        print(f"✓ Successfully saved: unidades_proyecto_equipamientos.geojson")
        print(f"  - Features: {feature_count}")
        print(f"  - File size: {file_size:.1f} KB")
        print(f"  - Location: {geojson_filepath}")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to save GeoJSON file: {e}")
        return False
    """
    Save the processed data in multiple formats optimized for different use cases.
    
    Args:
        df_equipamientos: Processed equipamientos dataframe
        df_vial: Processed vial dataframe  
        output_directory: Directory to save output files
    """
    
    # Create output directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, output_directory)
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\n" + "="*60)
    print("SAVING GEOSPATIAL DATA")
    print("="*60)
    print(f"Output directory: {output_dir}")

def main():
    """Main function for testing the transformer."""
    try:
        print("="*80)
        print("UNIDADES DE PROYECTO DATA TRANSFORMATION")
        print("="*80)
        
        # Transform the data
        df_equipamientos = unidades_proyecto_transformer()
        
        if df_equipamientos is not None:
            print("\n" + "="*60)
            print("TRANSFORMATION COMPLETED SUCCESSFULLY")
            print("="*60)
            
            # Display sample data
            print(f"\nEquipamientos sample data:")
            print(df_equipamientos.head(3))
            print(f"\nEquipamientos columns: {list(df_equipamientos.columns)}")
            
            # Save the processed data as GeoJSON
            success = save_equipamientos_geojson(df_equipamientos)
            
            if success:
                print("\n" + "="*60)
                print("GEOJSON FILE SAVED SUCCESSFULLY")
                print("="*60)
            
            return df_equipamientos
        
        else:
            print("Error: Data transformation failed")
            return None
        
    except Exception as e:
        print(f"Error in transformation: {e}")
        return None


if __name__ == "__main__":
    df_equipamientos = main()
