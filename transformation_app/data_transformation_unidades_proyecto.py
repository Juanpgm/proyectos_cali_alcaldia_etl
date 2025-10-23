# -*- coding: utf-8 -*-
"""
Data transformation module for project units (unidades de proyecto) with robust geospatial processing.

This module implements functional programming patterns for clean, scalable, and reusable transformations.
Key features include:

- Robust coordinate processing that handles string/array inconsistencies
- Support for reference fields that can be lists or strings (referencia_proceso, referencia_contrato, url_proceso)
- Functional programming approach with composable transformations
- Comprehensive error handling and data validation
- Clean, maintainable code without duplication

Author: AI Assistant
Version: 2.0 (Enhanced with robust coordinate and reference processing)
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
try:
    from temp_file_manager import process_in_memory, TempFileManager
except ImportError:
    # Fallback in case of import issues
    process_in_memory = None
    TempFileManager = None


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
    """Clean a monetary column using functional approach with robust type handling."""
    if column_name in df.columns:
        df = df.copy()
        
        print(f"Cleaning monetary column: {column_name}")
        
        # Apply the cleaning function to all values
        df[column_name] = df[column_name].apply(clean_monetary_value)
        
        # Ensure it's numeric and handle any remaining issues
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').fillna(0.0)
        
        # Final validation - ensure no negative values for monetary columns
        negative_mask = df[column_name] < 0
        if negative_mask.any():
            negative_count = negative_mask.sum()
            print(f"  Warning: Found {negative_count} negative values in {column_name}, converting to 0.00")
            df.loc[negative_mask, column_name] = 0.0
        
        # Report statistics
        positive_values = (df[column_name] > 0).sum()
        zero_values = (df[column_name] == 0).sum()
        total_values = len(df[column_name])
        
        print(f"  {column_name} validation results:")
        print(f"    Positive values: {positive_values}")
        print(f"    Zero values: {zero_values}")
        print(f"    Total values: {total_values}")
        
        # Convert to integer if requested (removes decimals)
        if as_integer:
            df[column_name] = df[column_name].astype('int64')
        
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
    """Clean monetary values by removing currency symbols and thousands separators with robust type handling."""
    if pd.isna(value) or value is None:
        return 0.00
    
    # Convert to string first, handling different input types
    if isinstance(value, (int, float)):
        # If already numeric, ensure it's positive and return
        numeric_value = float(value)
        if numeric_value < 0:
            print(f"    Warning: Negative monetary value {numeric_value} converted to 0.00")
            return 0.00
        return round(numeric_value, 2)
    
    str_value = str(value).strip()
    
    # Handle special cases
    if str_value in ['-', '', 'nan', 'null', 'NaN', 'NULL', 'None']:
        return 0.00
    
    # Remove currency symbols, spaces, and common prefixes
    cleaned = str_value.replace('$', '').replace('COP', '').replace('USD', '').replace(' ', '').replace('\t', '').strip()
    
    # Handle the case where cleaned string is just "-" or empty after cleaning
    if cleaned in ['-', '', '+']:
        return 0.00
    
    # Handle negative signs - for monetary values, we'll convert to positive
    is_negative = cleaned.startswith('-')
    if is_negative:
        cleaned = cleaned[1:]  # Remove negative sign
        print(f"    Warning: Negative monetary value '{str_value}' converted to positive")
    
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
        
        # Final validation before conversion
        if not cleaned or cleaned == '.':
            return 0.00
        
        # Convert to float
        result = float(cleaned)
        
        # Ensure positive value for monetary amounts
        if result < 0:
            print(f"    Warning: Negative result {result} from '{str_value}' converted to 0.00")
            result = 0.00
        
        return round(result, 2)  # Always return with 2 decimal places
        
    except (ValueError, TypeError) as e:
        print(f"    Error cleaning monetary value '{str_value}': {e} - Setting to 0.00")
        return 0.00


# Robust coordinate processing functions
def normalize_coordinate_value(coord: Any) -> Optional[float]:
    """
    Normalize a coordinate value handling various formats and edge cases.
    
    Args:
        coord: Coordinate value in various formats (string, number, etc.)
        
    Returns:
        Float coordinate value or None if invalid
    """
    if coord is None or pd.isna(coord):
        return None
    
    try:
        # Handle string coordinates that might have formatting issues
        if isinstance(coord, str):
            # Remove extra whitespace and convert comma decimal separator
            coord = coord.strip().replace(',', '.')
            if coord == '' or coord.lower() in ['nan', 'null', 'none']:
                return None
        
        # Convert to float
        coord_float = float(coord)
        
        # Validate coordinate ranges (assuming WGS84 geographic coordinates)
        if -180 <= coord_float <= 180:
            return coord_float
        else:
            print(f"Warning: Coordinate {coord_float} out of valid range [-180, 180]")
            return None
            
    except (ValueError, TypeError, AttributeError):
        print(f"Warning: Could not parse coordinate '{coord}' - {type(coord)}")
        return None


def normalize_coordinates_array(coords: Any) -> Optional[List[float]]:
    """
    Normalize coordinates array handling string representations and mixed formats.
    
    Args:
        coords: Coordinates in various formats (string, list, etc.)
        
    Returns:
        List of float coordinates or None if invalid
    """
    if coords is None or pd.isna(coords):
        return None
    
    try:
        # Handle string representation of coordinates
        if isinstance(coords, str):
            # Try to parse as JSON first
            try:
                coords = json.loads(coords)
            except json.JSONDecodeError:
                # Try to parse as comma-separated values
                if ',' in coords:
                    coords = coords.split(',')
                else:
                    return None
        
        # Ensure we have a list/array
        if not isinstance(coords, (list, tuple, np.ndarray)):
            return None
        
        # Normalize each coordinate
        normalized = []
        for coord in coords:
            normalized_coord = normalize_coordinate_value(coord)
            if normalized_coord is not None:
                normalized.append(normalized_coord)
        
        # Return normalized coordinates if we have at least 2 (lon, lat)
        return normalized if len(normalized) >= 2 else None
        
    except Exception as e:
        print(f"Warning: Error normalizing coordinates array '{coords}': {e}")
        return None


def parse_geojson_geometry(geom_str: str) -> Optional[Dict[str, Any]]:
    """
    Parse GeoJSON geometry string with robust error handling for various formats.
    
    Args:
        geom_str: String containing GeoJSON geometry
        
    Returns:
        Dict containing parsed geometry or None if invalid
    """
    if pd.isna(geom_str) or not geom_str or str(geom_str).strip() == '':
        return None
    
    try:
        # Handle string coordinates that need to be converted to proper GeoJSON
        geom_str = str(geom_str).strip()
        
        # Try to parse as JSON first
        try:
            geom_obj = json.loads(geom_str)
        except json.JSONDecodeError:
            # If not valid JSON, try to interpret as coordinate string
            if ',' in geom_str and not geom_str.startswith('{'):
                # Might be a simple coordinate string like "lon,lat"
                coords = normalize_coordinates_array(geom_str)
                if coords and len(coords) >= 2:
                    return {
                        "type": "Point",
                        "coordinates": coords[:2]  # Take first two as [lon, lat]
                    }
            return None
        
        # Validate required fields
        if not isinstance(geom_obj, dict) or 'type' not in geom_obj:
            return None
        
        # If coordinates field exists, normalize it
        if 'coordinates' in geom_obj:
            coords = geom_obj['coordinates']
            geom_obj['coordinates'] = normalize_geojson_coordinates(coords, geom_obj['type'])
            
            if geom_obj['coordinates'] is None:
                return None
        
        # Validate geometry type
        valid_types = ['Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon']
        if geom_obj['type'] not in valid_types:
            return None
        
        return geom_obj
    
    except Exception as e:
        print(f"Warning: Error parsing geometry '{geom_str[:50]}...': {e}")
        return None


def normalize_geojson_coordinates(coords: Any, geom_type: str) -> Optional[Any]:
    """
    Normalize GeoJSON coordinates based on geometry type.
    
    Args:
        coords: Coordinates in various formats
        geom_type: GeoJSON geometry type
        
    Returns:
        Normalized coordinates or None if invalid
    """
    if coords is None:
        return None
    
    try:
        if geom_type == 'Point':
            # Point coordinates should be [lon, lat] or [lon, lat, elevation]
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                # If already a proper coordinate array, normalize each value
                normalized_coords = []
                for coord in coords[:2]:  # Take only lon, lat
                    normalized_coord = normalize_coordinate_value(coord)
                    if normalized_coord is not None:
                        normalized_coords.append(normalized_coord)
                return normalized_coords if len(normalized_coords) == 2 else None
            else:
                # Try to parse as coordinate array string
                normalized = normalize_coordinates_array(coords)
                return normalized[:2] if normalized and len(normalized) >= 2 else None
        
        elif geom_type == 'LineString':
            # LineString coordinates should be [[lon1, lat1], [lon2, lat2], ...]
            if isinstance(coords, (list, tuple)):
                normalized_coords = []
                for coord_pair in coords:
                    normalized_pair = normalize_coordinates_array(coord_pair)
                    if normalized_pair and len(normalized_pair) >= 2:
                        normalized_coords.append(normalized_pair[:2])
                return normalized_coords if len(normalized_coords) >= 2 else None
            return None
        
        elif geom_type == 'Polygon':
            # Polygon coordinates should be [[[lon1, lat1], [lon2, lat2], ...]]
            if isinstance(coords, (list, tuple)) and len(coords) > 0:
                normalized_rings = []
                for ring in coords:
                    if isinstance(ring, (list, tuple)):
                        normalized_ring = []
                        for coord_pair in ring:
                            normalized_pair = normalize_coordinates_array(coord_pair)
                            if normalized_pair and len(normalized_pair) >= 2:
                                normalized_ring.append(normalized_pair[:2])
                        if len(normalized_ring) >= 3:  # Minimum for a valid ring
                            normalized_rings.append(normalized_ring)
                return normalized_rings if len(normalized_rings) > 0 else None
            return None
        
        # For other geometry types, return as-is for now
        return coords
        
    except Exception as e:
        print(f"Warning: Error normalizing coordinates for {geom_type}: {e}")
        return None


def extract_coordinates_info(geom_obj: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[Dict]]:
    """
    Extract coordinate information from geometry object with robust error handling.
    
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
            # For Point: coordinates are [lon, lat]
            if coords and len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                if lon is not None and lat is not None:
                    bounds = {'min_lon': lon, 'max_lon': lon, 'min_lat': lat, 'max_lat': lat}
                    return lon, lat, bounds
        
        elif geom_type == 'LineString':
            # For LineString: coordinates are [[lon1, lat1], [lon2, lat2], ...]
            if coords and len(coords) > 0:
                valid_coords = [coord for coord in coords if coord and len(coord) >= 2]
                if valid_coords:
                    lons = [coord[0] for coord in valid_coords if coord[0] is not None]
                    lats = [coord[1] for coord in valid_coords if coord[1] is not None]
                    
                    if lons and lats:
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
            if coords and len(coords) > 0 and coords[0]:
                exterior_ring = coords[0]
                valid_coords = [coord for coord in exterior_ring if coord and len(coord) >= 2]
                if valid_coords:
                    lons = [coord[0] for coord in valid_coords if coord[0] is not None]
                    lats = [coord[1] for coord in valid_coords if coord[1] is not None]
                    
                    if lons and lats:
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
    
    except (IndexError, TypeError, ValueError, AttributeError) as e:
        print(f"Warning: Error extracting coordinates from {geom_type}: {e}")
        return None, None, None


# Reference processing functions for handling list/string inconsistencies
def normalize_reference_value(value: Any) -> Optional[Union[str, List[str]]]:
    """
    Normalize reference values that can be either strings or lists.
    
    Args:
        value: Reference value in various formats
        
    Returns:
        Normalized reference value (string for single, list for multiple)
    """
    # Handle None first
    if value is None:
        return None
    
    # Handle pandas null values for scalars only (not arrays)
    try:
        if not isinstance(value, (list, tuple, np.ndarray)) and pd.isna(value):
            return None
    except (ValueError, TypeError):
        # pd.isna() can fail with arrays, continue with other checks
        pass
    
    try:
        # Handle string representations of lists
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value.lower() in ['nan', 'null', 'none']:
                return None
            
            # Try to parse as JSON if it looks like a list
            if value.startswith('[') and value.endswith(']'):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        # Filter out empty/null values and return as list if multiple items
                        filtered = [str(item).strip() for item in parsed if item and str(item).strip()]
                        if len(filtered) > 1:
                            return filtered
                        elif len(filtered) == 1:
                            return filtered[0]
                        else:
                            return None
                except json.JSONDecodeError:
                    # If JSON parsing fails, treat as regular string
                    pass
            
            # Handle comma-separated values
            if ',' in value and not value.startswith('http'):  # Don't split URLs
                items = [item.strip() for item in value.split(',') if item.strip()]
                if len(items) > 1:
                    return items
                elif len(items) == 1:
                    return items[0]
                else:
                    return None
            
            # Return as single string
            return value
        
        # Handle actual lists
        elif isinstance(value, (list, tuple)):
            # Filter out empty/null values
            filtered = [str(item).strip() for item in value if item and str(item).strip() and str(item).lower() not in ['nan', 'null', 'none']]
            if len(filtered) > 1:
                return filtered
            elif len(filtered) == 1:
                return filtered[0]
            else:
                return None
        
        # For other types, convert to string
        else:
            str_value = str(value).strip()
            return str_value if str_value and str_value.lower() not in ['nan', 'null', 'none'] else None
            
    except Exception as e:
        print(f"Warning: Error normalizing reference value '{value}': {e}")
        return str(value) if value else None


def process_reference_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process reference columns that can contain lists or strings.
    
    Args:
        df: DataFrame with reference columns
        
    Returns:
        DataFrame with normalized reference columns
    """
    result_df = df.copy()
    
    # Reference columns that can be lists or strings
    reference_columns = ['referencia_proceso', 'referencia_contrato', 'url_proceso']
    
    for col in reference_columns:
        if col in result_df.columns:
            print(f"Processing reference column: {col}")
            result_df[col] = result_df[col].apply(normalize_reference_value)
            
            # Count different types of values
            single_values = sum(1 for val in result_df[col].dropna() if isinstance(val, str))
            list_values = sum(1 for val in result_df[col].dropna() if isinstance(val, list))
            null_values = result_df[col].isnull().sum()
            
            print(f"  - Single values: {single_values}")
            print(f"  - List values: {list_values}")
            print(f"  - Null values: {null_values}")
    
    return result_df


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
    
    # Add reference column processor first (handles complex list/string formats)
    cleaning_functions.append(process_reference_columns)
    
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


def create_point_from_coordinates(row: pd.Series, idx: int) -> Optional[Dict[str, Any]]:
    """
    Create Point geometry from lat/lon coordinates with robust validation.
    
    Args:
        row: DataFrame row containing coordinate data
        idx: Row index for logging
        
    Returns:
        Point geometry dict or None if invalid
    """
    lat_val = None
    lon_val = None
    
    # Try to get latitude using functional approach
    lat_val = pipe(
        ['lat', 'latitude'],
        lambda cols: next((normalize_coordinate_value(row.get(col)) for col in cols if col in row and pd.notna(row[col])), None)
    )
    
    # Try to get longitude using functional approach
    lon_val = pipe(
        ['lon', 'longitude'],
        lambda cols: next((normalize_coordinate_value(row.get(col)) for col in cols if col in row and pd.notna(row[col])), None)
    )
    
    # Create Point geometry if we have both lat and lon
    if lat_val is not None and lon_val is not None:
        # Validate coordinate ranges (assuming WGS84)
        if -90 <= lat_val <= 90 and -180 <= lon_val <= 180:
            geom_obj = {
                "type": "Point",
                "coordinates": [lon_val, lat_val]  # GeoJSON format: [longitude, latitude]
            }
            print(f"  Created Point geometry from lat/lon for row {idx}: [{lon_val}, {lat_val}]")
            return geom_obj
        else:
            print(f"  Invalid lat/lon coordinates for row {idx}: lat={lat_val}, lon={lon_val}")
    
    return None


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
                geom_obj = create_point_from_coordinates(row, idx)
            
            # Create properties - Include ALL columns except redundant coordinate columns
            properties = {}
            # Exclude only redundant coordinate fields (geometry is handled separately)
            excluded_columns = ['geom', 'lat', 'lon', 'longitude', 'latitude']
            
            for col, value in row.items():
                if col not in excluded_columns:
                    # Handle different data types for JSON serialization
                    if value is None:
                        properties[col] = None
                    elif isinstance(value, list):
                        # Handle reference lists properly for JSON serialization
                        try:
                            properties[col] = [str(item) for item in value if item is not None]
                        except:
                            properties[col] = str(value)
                    else:
                        # Check for pandas null values safely
                        try:
                            if pd.isna(value):
                                properties[col] = None
                                continue
                        except (ValueError, TypeError):
                            # pd.isna() can fail with some types, continue with other checks
                            pass
                        
                        if isinstance(value, (np.int64, np.int32, pd.Int64Dtype)):
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
                            properties[col] = str(value) if value is not None else None            # Create feature (geometry can be null for records without any coordinate info)
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


def print_data_summary(original_df: pd.DataFrame, processed_df: pd.DataFrame) -> None:
    """
    Print comprehensive data transformation summary using functional approach.
    
    Args:
        original_df: Original dataframe before processing
        processed_df: Processed dataframe after transformation
    """
    print("\n" + "="*60)
    print("DATA TRANSFORMATION SUMMARY")
    print("="*60)
    
    # Calculate metrics using functional approach
    metrics = {
        'rows': len(processed_df),
        'total_columns': len(processed_df.columns),
        'original_columns_preserved': len([col for col in original_df.columns if col in processed_df.columns]),
        'new_computed_columns': len([col for col in processed_df.columns if col not in original_df.columns]),
        'valid_geometries': processed_df['geometry_type'].notna().sum() if 'geometry_type' in processed_df.columns else 0,
        'text_columns': len([col for col in processed_df.columns if processed_df[col].dtype == 'object']),
        'numeric_columns': len([col for col in processed_df.columns if pd.api.types.is_numeric_dtype(processed_df[col])]),
        'boolean_columns': len([col for col in processed_df.columns if processed_df[col].dtype == 'bool'])
    }
    
    # Print summary using functional formatting
    print(f"\nUnidades de Proyecto:")
    for key, value in metrics.items():
        formatted_key = key.replace('_', ' ').title()
        print(f"  {formatted_key}: {value}")
    
    print(f"\nColumn summary:")
    for col_type in ['text', 'numeric', 'boolean']:
        key = f"{col_type}_columns"
        print(f"  {col_type.title()} columns: {metrics[key]}")


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
    # Order is important: upid generation MUST be first to ensure unique IDs
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
    
    # Print comprehensive summary
    print_data_summary(df, processed_df)
    
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


def test_robust_transformations():
    """
    Test the robust transformation functions with various problematic formats.
    """
    print("\n" + "="*60)
    print("TESTING ROBUST TRANSFORMATION FUNCTIONS")
    print("="*60)
    
    # Test coordinate normalization
    test_coords = [
        "[-74.123456, 3.987654]",  # Valid JSON array
        "-74,123456, 3,987654",    # European decimal format
        "[-74.123456, 3.987654, 100]",  # With elevation
        "invalid_coord",           # Invalid format
        None,                      # Null value
        "",                        # Empty string
        "[not_a_number, 3.5]"     # Mixed valid/invalid
    ]
    
    print("Testing coordinate normalization:")
    for coord in test_coords:
        result = normalize_coordinates_array(coord)
        print(f"  '{coord}' -> {result}")
    
    # Test reference normalization
    test_refs = [
        "REF-001",                 # Single string
        '["REF-001", "REF-002"]',  # JSON array
        "REF-001, REF-002",        # Comma separated
        ["REF-001", "REF-002"],    # Actual list
        None,                      # Null value
        "",                        # Empty string
        "http://example.com/url"   # URL (should not be split)
    ]
    
    print("\nTesting reference normalization:")
    for ref in test_refs:
        result = normalize_reference_value(ref)
        print(f"  {ref} -> {result}")
    
    print("\n✓ Robust transformation tests completed")


def main():
    """Main function for testing the transformer."""
    # Just run the main transformation without robustness tests
    return transform_and_save_unidades_proyecto()


if __name__ == "__main__":
    """
    Main execution block for testing the transformation pipeline.
    """
    print("Starting unidades de proyecto transformation process...")
    
    # Run the complete transformation pipeline with robustness tests
    df_result = main()
    
    if df_result is not None:
        print("\n" + "="*60)
        print("TRANSFORMATION PIPELINE COMPLETED")
        print("="*60)
        print(f"✓ Processed data: {len(df_result)} records")
        print(f"✓ Total columns: {len(df_result.columns)}")
        print(f"✓ GeoJSON file saved: unidades_proyecto.geojson")
        print(f"✓ Robust coordinate and reference processing implemented")
        
    else:
        print("\n" + "="*60)
        print("TRANSFORMATION PIPELINE FAILED")
        print("="*60)
        print("✗ Could not process unidades de proyecto data")
