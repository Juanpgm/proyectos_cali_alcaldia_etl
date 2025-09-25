# -*- coding: utf-8 -*-
"""
Data transformation module for project units (unidades de proyecto) with geospatial data processing.
Implements functional programming patterns for clean, scalable, and reusable transformations.
"""

import os
import sys
import pandas as pd
import json
import numpy as np
from typing import Optional, Dict, List, Any, Tuple, Union, Callable
from datetime import datetime
from functools import reduce, partial, wraps
from pathlib import Path

# Add utils to path for temp file manager
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from temp_file_manager import process_in_memory, TempFileManager


# Functional programming utilities
def compose(*functions: Callable) -> Callable:
    """Compose multiple functions into a single function."""
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(value: Any, *functions: Callable) -> Any:
    """Apply a sequence of functions to a value (pipe operator)."""
    return reduce(lambda acc, func: func(acc), functions, value)


def curry(func: Callable) -> Callable:
    """Convert a function to a curried version for partial application."""
    @wraps(func)
    def curried(*args, **kwargs):
        if len(args) + len(kwargs) >= func.__code__.co_argcount:
            return func(*args, **kwargs)
        return lambda *more_args, **more_kwargs: curried(*(args + more_args), **dict(kwargs, **more_kwargs))
    return curried


def safe_transform(func: Callable, fallback_value: Any = None) -> Callable:
    """Safely execute transformation functions with error handling."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Warning in {func.__name__}: {e}")
            return fallback_value
    return wrapper


# Functional data cleaning utilities
def clean_numeric_column(df: pd.DataFrame, column_name: str, default_value: float = 0.0) -> pd.DataFrame:
    """Clean a numeric column using functional approach."""
    if column_name in df.columns:
        df = df.copy()
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').fillna(default_value)
    return df


def clean_monetary_column(df: pd.DataFrame, column_name: str, as_integer: bool = False) -> pd.DataFrame:
    """Clean a monetary column using functional approach."""
    if column_name in df.columns:
        df = df.copy()
        df[column_name] = df[column_name].apply(
            lambda x: clean_monetary_value(x) if pd.notna(x) else 0.00
        )
        # Ensure it's numeric before processing
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').fillna(0.0)
        
        # Convert to integer if requested (removes decimals)
        if as_integer:
            df[column_name] = df[column_name].astype(int)
    return df


def apply_functional_cleaning(df: pd.DataFrame, monetary_cols: List[str], numeric_cols: List[str], integer_monetary_cols: List[str] = None) -> pd.DataFrame:
    """Apply data cleaning using functional composition."""
    result_df = df.copy()
    
    if integer_monetary_cols is None:
        integer_monetary_cols = []
    
    # Apply monetary cleaning (as integers)
    for col in integer_monetary_cols:
        result_df = clean_monetary_column(result_df, col, as_integer=True)
    
    # Apply monetary cleaning (with decimals)
    for col in monetary_cols:
        if col not in integer_monetary_cols:
            result_df = clean_monetary_column(result_df, col, as_integer=False)
    
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


# Pure functions for data processing
def generate_upid_for_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate unique upid (Unidades de Proyecto ID) with format UNP-# for records without upid.
    Preserves existing upid values and ensures no duplicates.
    
    Args:
        df: DataFrame with potential upid column
        
    Returns:
        DataFrame with upid column populated
    """
    result_df = df.copy()
    
    # Ensure upid column exists
    if 'upid' not in result_df.columns:
        result_df['upid'] = None
    
    # Find existing upid values to determine next consecutive number
    existing_upids = set()
    max_consecutive = 0
    
    for upid in result_df['upid'].dropna():
        if isinstance(upid, str) and upid.startswith('UNP-'):
            existing_upids.add(upid)
            # Extract number from UNP-# format
            try:
                number_part = upid.replace('UNP-', '')
                if number_part.isdigit():
                    max_consecutive = max(max_consecutive, int(number_part))
            except (ValueError, AttributeError):
                continue
    
    # Generate upid for records without one
    new_upids_count = 0
    next_consecutive = max_consecutive + 1
    
    for idx in result_df.index:
        current_upid = result_df.at[idx, 'upid']
        
        # Only assign upid if it's null, empty, or NaN
        if pd.isna(current_upid) or current_upid is None or str(current_upid).strip() == '':
            new_upid = f"UNP-{next_consecutive}"
            
            # Ensure uniqueness (very unlikely but safety check)
            while new_upid in existing_upids:
                next_consecutive += 1
                new_upid = f"UNP-{next_consecutive}"
            
            result_df.at[idx, 'upid'] = new_upid
            existing_upids.add(new_upid)
            next_consecutive += 1
            new_upids_count += 1
    
    print(f"✓ UPID Generation:")
    print(f"  - Existing upids preserved: {len(existing_upids) - new_upids_count}")
    print(f"  - New upids generated: {new_upids_count}")
    print(f"  - Total upids: {len(existing_upids)}")
    print(f"  - Next available number: UNP-{next_consecutive}")
    
    return result_df


def add_computed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add computed columns for coordinates and metadata."""
    result_df = df.copy()
    
    # Add computed columns without modifying original data
    new_columns = {
        'longitude': None,
        'latitude': None,
        'geometry_bounds': None,
        'geometry_type': None,
        'processed_timestamp': datetime.now().isoformat()
    }
    
    for col, default_value in new_columns.items():
        result_df[col] = default_value
    
    print(f"✓ Added computed columns: {list(new_columns.keys())}")
    return result_df


def clean_text_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Clean a single text column using functional approach."""
    if column in df.columns:
        result_df = df.copy()
        result_df[column] = result_df[column].apply(
            lambda x: None if pd.isna(x) else str(x).strip()
        )
        return result_df
    return df


def clean_numeric_column_safe(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Clean a single numeric column using functional approach."""
    if column in df.columns:
        result_df = df.copy()
        result_df[column] = pd.to_numeric(result_df[column], errors='coerce').fillna(0.0)
        return result_df
    return df


def clean_integer_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Clean a single integer column using functional approach - converts to integers without decimals."""
    if column in df.columns:
        result_df = df.copy()
        # First convert to numeric, then to integer (removing decimals)
        result_df[column] = pd.to_numeric(result_df[column], errors='coerce').fillna(0.0).astype(int)
        return result_df
    return df


def clean_bpin_column(df: pd.DataFrame) -> pd.DataFrame:
    """Clean BPIN column specifically - keeps as string for alphanumeric codes or converts to integer."""
    if 'bpin' in df.columns:
        result_df = df.copy()
        # Try to convert to numeric, but if it fails, keep as string (for alphanumeric BPIN codes)
        def process_bpin(value):
            if pd.isna(value) or value is None:
                return None
            str_value = str(value).strip()
            if str_value == '' or str_value.lower() in ['nan', 'null']:
                return None
            # Try to convert to integer if it's purely numeric
            if str_value.replace('.', '').replace(',', '').isdigit():
                try:
                    return int(float(str_value.replace(',', '.')))
                except (ValueError, TypeError):
                    return str_value
            else:
                # Keep as string for alphanumeric codes
                return str_value
        
        result_df['bpin'] = result_df['bpin'].apply(process_bpin)
        return result_df
    return df


def clean_boolean_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Clean a single boolean column using functional approach."""
    if column in df.columns:
        result_df = df.copy()
        result_df[column] = result_df[column].astype(bool)
        return result_df
    return df


def clean_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize data types using functional composition."""
    
    # Define column types
    text_columns = ['nickname_detalle', 'direccion', 'descripcion_intervencion', 'identificador', 'nickname']
    # Variables monetarias que deben ser enteros (sin decimales)  
    integer_monetary_columns = ['presupuesto_base', 'ppto_base']
    # Variables numéricas enteras
    integer_columns = ['bpin']
    # Variables numéricas que pueden tener decimales
    decimal_columns = ['avance_obra', 'avance_fisico_obra']
    boolean_columns = ['centros_gravedad']
    
    # Create cleaning pipeline
    cleaning_functions = []
    
    # Add text column cleaners
    for col in text_columns:
        cleaning_functions.append(partial(clean_text_column, column=col))
    
    # Add monetary integer column cleaners (removes decimals from monetary values)
    for col in integer_monetary_columns:
        cleaning_functions.append(partial(clean_monetary_column, column_name=col, as_integer=True))
        
    # Add integer column cleaners (converts to integers)
    for col in integer_columns:
        cleaning_functions.append(partial(clean_integer_column, column=col))
    
    # Add decimal column cleaners (allows decimals)
    for col in decimal_columns:
        cleaning_functions.append(partial(clean_numeric_column_safe, column=col))
        
    # Add boolean column cleaners
    for col in boolean_columns:
        cleaning_functions.append(partial(clean_boolean_column, column=col))
    
    # Add BPIN column cleaner
    cleaning_functions.append(clean_bpin_column)
    
    # Apply all cleaning functions using functional composition
    return pipe(df, *cleaning_functions)


def process_single_geometry(row: pd.Series, idx: int) -> Tuple[Optional[float], Optional[float], Optional[str], Optional[str]]:
    """Process a single row's geometry data."""
    geom_value = None
    
    # Try different geometry column names
    for geom_col in ['geom', 'geometry', 'geometria']:
        if geom_col in row and pd.notna(row[geom_col]):
            geom_value = row[geom_col]
            break
    
    if geom_value:
        geom_obj = parse_geojson_geometry(geom_value)
        
        if geom_obj:
            lon, lat, bounds = extract_coordinates_info(geom_obj)
            return (
                round(lon, 6) if lon is not None else None,
                round(lat, 6) if lat is not None else None,
                json.dumps(bounds) if bounds else None,
                geom_obj['type']
            )
    
    return None, None, None, None


def process_geospatial_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process geospatial data using functional approach."""
    result_df = df.copy()
    
    print("Processing geospatial data...")
    
    valid_geoms = 0
    invalid_geoms = 0
    
    # Process geometries using functional mapping
    for idx, row in result_df.iterrows():
        lon, lat, bounds, geom_type = process_single_geometry(row, idx)
        
        if geom_type:
            valid_geoms += 1
        else:
            invalid_geoms += 1
        
        # Update row data
        result_df.at[idx, 'longitude'] = lon
        result_df.at[idx, 'latitude'] = lat
        result_df.at[idx, 'geometry_bounds'] = bounds
        result_df.at[idx, 'geometry_type'] = geom_type
    
    print(f"✓ Geospatial processing: {valid_geoms} valid, {invalid_geoms} invalid geometries")
    return result_df


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
            
            # Create properties - Include ALL columns except redundant coordinate columns
            properties = {}
            # Exclude only redundant coordinate fields (geometry is handled separately)
            excluded_columns = ['geom', 'lat', 'lon', 'longitude', 'latitude']
            
            for col, value in row.items():
                if col not in excluded_columns:
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


def load_json_data(file_path: str) -> Optional[pd.DataFrame]:
    """Load JSON data and convert to DataFrame."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        print(f"✓ Loaded {len(df)} records from {os.path.basename(file_path)}")
        return df
    
    except Exception as e:
        print(f"✗ Error loading {file_path}: {e}")
        return None


def unidades_proyecto_transformer(
    data_directory: str = "app_inputs/unidades_proyecto_input", 
    data: Optional[Union[List[Dict], pd.DataFrame]] = None
) -> Optional[pd.DataFrame]:
    """
    Transform project units data using functional programming approach.
    Can work with either file-based data or in-memory data for better flexibility.
    
    Args:
        data_directory: Path to the directory containing JSON files (fallback)
        data: Optional in-memory data to process directly
        
    Returns:
        DataFrame with processed unidades de proyecto data or None if failed
    """
    
    # If data is provided in memory, process it directly (no temp files needed)
    if data is not None:
        print("🚀 Processing data in memory (no temporary files)")
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            print(f"❌ Unsupported data type: {type(data)}")
            return None
        
        return process_in_memory(df, _process_unidades_proyecto_dataframe)
    
    # Fallback to file-based processing if no in-memory data provided
    print(f"📁 Processing data from files in: {data_directory}")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    directory_path = os.path.join(current_dir, data_directory)
    
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"Directory not found: {directory_path}")
    
    print(f"Loading data from: {directory_path}")
    
    # Define file path for JSON input
    json_path = os.path.join(directory_path, "unidades_proyecto.json")
    
    # Check if file exists
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"File not found: {json_path}")
    
    # Load JSON file using functional approach
    df_unidades_proyecto = load_json_data(json_path)
    
    if df_unidades_proyecto is None:
        return None
    
    # Process using the internal function
    return _process_unidades_proyecto_dataframe(df_unidades_proyecto)


def _process_unidades_proyecto_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Internal function to process unidades proyecto dataframe using functional pipeline.
    This function can be used with both file-based and in-memory processing.
    
    Args:
        df: Input dataframe to process
        
    Returns:
        Processed dataframe
    """
    print("\n" + "="*60)
    print("PROCESSING UNIDADES DE PROYECTO DATA")
    print("="*60)
    
    # Create processing pipeline using functional composition
    # IMPORTANT: upid generation MUST be first to ensure unique IDs
    processing_pipeline = compose(
        lambda df: generate_upid_for_records(df),
        lambda df: add_computed_columns(df),
        lambda df: clean_data_types(df),
        lambda df: process_geospatial_data(df)
    )
    
    # Apply the complete processing pipeline
    print("Applying functional processing pipeline...")
    processed_df = processing_pipeline(df)
    
    print(f"✓ Processing completed: {len(processed_df)} rows, {len(processed_df.columns)} columns")
    
    # ================================
    # DATA SUMMARY
    # ================================
    print("\n" + "="*60)
    print("DATA TRANSFORMATION SUMMARY")
    print("="*60)
    
    print(f"\nUnidades de Proyecto:")
    print(f"  Rows: {len(processed_df)}")
    print(f"  Total columns: {len(processed_df.columns)}")
    print(f"  Original columns preserved: {len([col for col in df.columns if col in processed_df.columns])}")
    print(f"  New computed columns: {len([col for col in processed_df.columns if col not in df.columns])}")
    print(f"  Valid geometries: {processed_df['geometry_type'].notna().sum()}")
    
    # Show column summary
    print(f"\nColumn summary:")
    print(f"  Text columns: {len([col for col in processed_df.columns if processed_df[col].dtype == 'object'])}")
    print(f"  Numeric columns: {len([col for col in processed_df.columns if pd.api.types.is_numeric_dtype(processed_df[col])])}")
    print(f"  Boolean columns: {len([col for col in processed_df.columns if processed_df[col].dtype == 'bool'])}")
    
    return processed_df


def save_unidades_proyecto_geojson(df_unidades_proyecto: pd.DataFrame, output_directory: str = "app_outputs/unidades_proyecto_outputs") -> bool:
    """
    Save the processed unidades de proyecto data as GeoJSON file using functional approach.
    
    Args:
        df_unidades_proyecto: Processed unidades de proyecto dataframe
        output_directory: Directory to save output files
        
    Returns:
        True if successful, False otherwise
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
        # Create GeoJSON FeatureCollection using functional approach
        feature_collection = create_feature_collection(df_unidades_proyecto)
        
        # Save GeoJSON file
        geojson_filepath = os.path.join(output_dir, "unidades_proyecto.geojson")
        
        # Use functional approach for file saving
        success = pipe(
            feature_collection,
            lambda data: save_json_file(data, geojson_filepath),
            lambda result: log_file_save_result(result, geojson_filepath, len(feature_collection['features']))
        )
        
        return success
        
    except Exception as e:
        print(f"✗ Failed to save GeoJSON file: {e}")
        return False


def save_json_file(data: Dict[str, Any], filepath: str) -> bool:
    """Save JSON data to file with proper encoding."""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Error saving file: {e}")
        return False


def log_file_save_result(success: bool, filepath: str, feature_count: int) -> bool:
    """Log the result of file save operation."""
    if success:
        file_size = os.path.getsize(filepath) / 1024  # Size in KB
        print(f"✓ Successfully saved: {os.path.basename(filepath)}")
        print(f"  - Features: {feature_count}")
        print(f"  - File size: {file_size:.1f} KB")
        print(f"  - Location: {filepath}")
    return success

def transform_and_save_unidades_proyecto() -> Optional[pd.DataFrame]:
    """
    Main function to transform and save unidades de proyecto data.
    Implements complete functional pipeline for transformation and output generation.
    """
    try:
        print("="*80)
        print("UNIDADES DE PROYECTO FUNCTIONAL TRANSFORMATION")
        print("="*80)
        
        # Transform the data using functional pipeline
        df_processed = unidades_proyecto_transformer()
        
        if df_processed is not None:
            print("\n" + "="*60)
            print("TRANSFORMATION COMPLETED SUCCESSFULLY")
            print("="*60)
            
            # Display sample data
            print(f"\nSample processed data:")
            print(df_processed.head(2).to_string())
            print(f"\nTotal columns: {len(df_processed.columns)}")
            
            # Save the processed data as GeoJSON using functional approach
            success = save_unidades_proyecto_geojson(df_processed)
            
            if success:
                print("\n" + "="*60)
                print("GEOJSON FILE SAVED SUCCESSFULLY")
                print("="*60)
                return df_processed
            else:
                print("Warning: GeoJSON save failed, but transformation was successful")
                return df_processed
        
        else:
            print("Error: Data transformation failed")
            return None
        
    except Exception as e:
        print(f"Error in transformation pipeline: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """Main function for testing the transformer."""
    return transform_and_save_unidades_proyecto()


if __name__ == "__main__":
    """
    Main execution block for testing the transformation pipeline.
    """
    print("Starting unidades de proyecto transformation process...")
    
    # Run the complete transformation pipeline
    df_result = main()
    
    if df_result is not None:
        print("\n" + "="*60)
        print("TRANSFORMATION PIPELINE COMPLETED")
        print("="*60)
        print(f"✓ Processed data: {len(df_result)} records")
        print(f"✓ Total columns: {len(df_result.columns)}")
        print(f"✓ GeoJSON file saved: unidades_proyecto.geojson")
        
    else:
        print("\n" + "="*60)
        print("TRANSFORMATION PIPELINE FAILED")
        print("="*60)
        print("✗ Could not process unidades de proyecto data")
