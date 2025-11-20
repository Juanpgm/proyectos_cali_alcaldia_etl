# -*- coding: utf-8 -*-
"""
Data transformation module for project units (unidades de proyecto).

This module implements functional programming patterns for clean, scalable, and reusable transformations.
Key features include:

- Support for reference fields that can be lists or strings (referencia_proceso, referencia_contrato, url_proceso)
- Functional programming approach with composable transformations
- Comprehensive geospatial processing and validation
- Spatial intersections with administrative boundaries
- Coordinate validation and correction
- Date standardization and validation
- Semantic normalization (estados, tipos de intervención)
- Title case conversion for Spanish text
- Comprehensive error handling and data validation
- Automatic metrics generation and quality reporting
- GeoJSON export with proper coordinate format
- Clean, maintainable code without duplication

Author: AI Assistant
Version: 4.0 (Complete - Full geospatial pipeline with validation)
"""

import os
import sys
import pandas as pd
import geopandas as gpd
import json
import numpy as np
import re
import unicodedata
from typing import Optional, Dict, List, Any, Tuple, Union, Callable
from datetime import datetime, timedelta
from functools import reduce, partial, wraps
from pathlib import Path
from shapely.geometry import Point
from difflib import get_close_matches

# Add utils and extraction_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'extraction_app'))

try:
    from temp_file_manager import process_in_memory, TempFileManager
except ImportError:
    # Fallback in case of import issues
    process_in_memory = None
    TempFileManager = None

# Import data extraction function for Google Drive
try:
    from data_extraction_unidades_proyecto import extract_unidades_proyecto_data
    EXTRACTION_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ Warning: Could not import extraction module: {e}")
    print("   Falling back to file-based processing")
    extract_unidades_proyecto_data = None
    EXTRACTION_AVAILABLE = False


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
    """Add computed columns for metadata."""
    result_df = df.copy()
    
    # Add computed columns without modifying original data
    new_columns = {
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


# Semantic data cleaning functions
def normalize_estado_values(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize estado values to standardized labels with business rules.
    
    Business Rules:
    - If avance_obra is 0 (cero, (cero), (0), 0, 0.0, etc.), set estado to 'En Alistamiento'
    - Otherwise, normalize estado based on text patterns
    """
    if 'estado' in df.columns:
        result_df = df.copy()
        
        # Standardize all estado values (case-insensitive)
        def standardize_estado(row):
            val = row.get('estado')
            avance_obra = row.get('avance_obra')
            
            # REGLA DE NEGOCIO: Si avance_obra es cero, establecer "En Alistamiento"
            if avance_obra is not None:
                avance_str = str(avance_obra).strip().lower()
                # Verificar si es cero en diferentes formatos
                if avance_str in ['cero', '(cero)', '(0)', '0', '0.0', '0,0']:
                    return 'En Alistamiento'
            
            # Si avance_obra no es cero, normalizar estado por texto
            if pd.isna(val) or val is None:
                return val
            
            val_str = str(val).strip().lower()
            
            # Map all variations to standard values (mantener capitalización estándar)
            if 'socializaci' in val_str or 'alistamiento' in val_str:
                return 'En Alistamiento'
            elif 'ejecuci' in val_str:
                return 'En Ejecución'
            elif 'finalizado' in val_str or 'terminado' in val_str:
                return 'Terminado'
            else:
                # Return original if no match (preserve unknown states)
                return val
        
        result_df['estado'] = result_df.apply(standardize_estado, axis=1)
        return result_df
    return df


def normalize_tipo_intervencion_values(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize tipo_intervencion values."""
    if 'tipo_intervencion' in df.columns:
        result_df = df.copy()
        replacements = {
            'Adecuaciones': 'Adecuaciones y Mantenimientos',
            'Mantenimiento': 'Adecuaciones y Mantenimientos',
            'Rehabilitación / Reforzamiento': 'Rehabilitación - Reforzamiento'
        }
        result_df['tipo_intervencion'] = result_df['tipo_intervencion'].replace(replacements)
        return result_df
    return df


def title_case_spanish(text: str) -> str:
    """Convert text to title case following Spanish conventions."""
    if pd.isna(text) or text is None or str(text).strip() == '':
        return text
    
    text = str(text).strip()
    
    connectors = {
        'a', 'ante', 'bajo', 'con', 'contra', 'de', 'del', 'desde', 'durante',
        'e', 'el', 'en', 'entre', 'hacia', 'hasta', 'la', 'las', 'lo', 'los',
        'mediante', 'para', 'por', 'según', 'sin', 'sobre', 'tras', 'y', 'o', 'u', 'mi'
    }
    
    acronyms = {
        'ie', 'i.e', 'i.e.', 'ips', 'eps', 'uts', 'cad', 'secop', 'bpin', 'upid',
        'tic', 'tio', 'rrhh', 'pqrs', 'sst', 'covid', 'onu', 'oit', 'dian',
        'dane', 'dnp', 'sgr', 'poa', 'poai', 'iva', 'nit', 'rut', 'sisben'
    }
    
    words = text.split()
    result = []
    
    for i, word in enumerate(words):
        word_lower = word.lower().replace('.', '')
        
        if word_lower in acronyms:
            result.append(word.upper())
        elif i > 0 and word_lower in connectors:
            result.append(word.lower())
        else:
            result.append(word.capitalize())
    
    return ' '.join(result)


def apply_title_case_to_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Apply title case to relevant text columns."""
    columns_to_transform = ['nombre_up', 'nombre_up_detalle', 'direccion', 'tipo_equipamiento', 'identificador']
    result_df = df.copy()
    
    for col in columns_to_transform:
        if col in result_df.columns:
            result_df[col] = result_df[col].apply(title_case_spanish)
    
    return result_df


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
    data: Optional[Union[List[Dict], pd.DataFrame]] = None,
    use_extraction: bool = True
) -> Optional[gpd.GeoDataFrame]:
    """
    Transform project units data using functional programming approach.
    Can work with either file-based data, in-memory data, or Google Drive extraction.
    
    Args:
        data_directory: Path to the directory containing JSON files (fallback)
        data: Optional in-memory data to process directly
        use_extraction: If True, attempts to extract fresh data from Google Drive (default: True)
        
    Returns:
        GeoDataFrame with processed unidades de proyecto data or None if failed
    """
    
    # Priority 1: Try to extract fresh data from Google Drive
    if use_extraction and data is None and EXTRACTION_AVAILABLE and extract_unidades_proyecto_data is not None:
        try:
            print("=" * 80)
            print("EXTRACTING FRESH DATA FROM GOOGLE DRIVE")
            print("=" * 80)
            print()
            
            df = extract_unidades_proyecto_data()
            
            if df is not None and not df.empty:
                print()
                print(f"✓ Extracted {len(df):,} records from Google Drive")
                print(f"✓ Total columns: {len(df.columns)}")
                print()
                
                if process_in_memory:
                    return process_in_memory(df, _process_unidades_proyecto_dataframe)
                else:
                    return _process_unidades_proyecto_dataframe(df)
            else:
                print("⚠️ Extraction returned empty data, falling back to file-based processing")
                
        except Exception as e:
            print(f"⚠️ Error during Google Drive extraction: {e}")
            print("   Falling back to file-based processing")
            import traceback
            traceback.print_exc()
    
    # Priority 2: If data is provided in memory, process it directly
    if data is not None:
        print("🚀 Processing data in memory (no temporary files)")
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            print(f"❌ Unsupported data type: {type(data)}")
            return None
        
        if process_in_memory:
            return process_in_memory(df, _process_unidades_proyecto_dataframe)
        else:
            return _process_unidades_proyecto_dataframe(df)
    
    # Priority 3: Fallback to file-based processing
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


def convert_to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert DataFrame to GeoDataFrame with proper geometry."""
    if 'lat' not in df.columns or 'lon' not in df.columns:
        print("⚠ Warning: lat/lon columns not found, skipping geodataframe conversion")
        return df
    
    gdf = df.copy()
    
    def safe_convert_to_float(value):
        if pd.isna(value) or value is None:
            return None
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return None
    
    gdf['lat_numeric'] = gdf['lat'].apply(safe_convert_to_float)
    gdf['lon_numeric'] = gdf['lon'].apply(safe_convert_to_float)
    
    valid_coords = gdf['lat_numeric'].notna() & gdf['lon_numeric'].notna()
    
    if valid_coords.sum() == 0:
        print("⚠ No valid coordinates found")
        return df
    
    gdf.loc[valid_coords, 'geometry'] = gdf.loc[valid_coords].apply(
        lambda row: Point(row['lon_numeric'], row['lat_numeric']), axis=1
    )
    
    gdf = gpd.GeoDataFrame(gdf, geometry='geometry', crs='EPSG:4326')
    gdf.drop(columns=['lat_numeric', 'lon_numeric'], inplace=True, errors='ignore')
    
    print(f"✓ GeoDataFrame created: {valid_coords.sum()} geometries")
    return gdf


def fix_coordinate_format(coord_value, coord_type='lat'):
    """Fix coordinate format ensuring proper structure."""
    if pd.isna(coord_value) or coord_value is None:
        return None
    
    try:
        coord_str = str(coord_value).strip().replace(' ', '').replace(',', '.')
        coord_float = float(coord_str)
        
        if coord_type == 'lat':
            if 3.0 <= coord_float <= 4.0:
                return round(coord_float, 10)
            elif 0 < coord_float < 1:
                return round(3.0 + coord_float, 10)
        elif coord_type == 'lon':
            if -77.0 <= coord_float <= -76.0:
                return round(coord_float, 10)
            elif 76.0 < coord_float < 77.0:
                return round(-coord_float, 10)
            elif 0 < coord_float < 1:
                return round(-76.0 - coord_float, 10)
    except (ValueError, TypeError):
        pass
    
    return None


def correct_coordinate_formats(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Correct coordinate formats for Cali region."""
    if 'lat' not in gdf.columns or 'lon' not in gdf.columns:
        return gdf
    
    result_gdf = gdf.copy()
    result_gdf['lat'] = result_gdf['lat'].apply(lambda x: fix_coordinate_format(x, 'lat'))
    result_gdf['lon'] = result_gdf['lon'].apply(lambda x: fix_coordinate_format(x, 'lon'))
    
    valid_coords = result_gdf['lat'].notna() & result_gdf['lon'].notna()
    
    if valid_coords.sum() > 0:
        result_gdf.loc[valid_coords, 'geometry'] = result_gdf.loc[valid_coords].apply(
            lambda row: Point(row['lon'], row['lat']), axis=1
        )
        result_gdf = gpd.GeoDataFrame(result_gdf, geometry='geometry', crs='EPSG:4326')
    
    print(f"✓ Coordinates corrected: {valid_coords.sum()} valid")
    return result_gdf


def create_final_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Create final geometry and remove lat/lon columns."""
    if 'lat' not in gdf.columns or 'lon' not in gdf.columns:
        return gdf
    
    result_gdf = gdf.copy()
    valid_coords = result_gdf['lat'].notna() & result_gdf['lon'].notna()
    
    if valid_coords.sum() > 0:
        # Note: Store as lat, lon in geometry for later spatial ops
        result_gdf.loc[valid_coords, 'geometry'] = result_gdf.loc[valid_coords].apply(
            lambda row: Point(row['lat'], row['lon']), axis=1
        )
        result_gdf = gpd.GeoDataFrame(result_gdf, geometry='geometry', crs='EPSG:4326')
    
    result_gdf.drop(columns=['lat', 'lon'], inplace=True, errors='ignore')
    print(f"✓ Final geometry created, lat/lon columns removed")
    return result_gdf


def perform_spatial_intersection(gdf: gpd.GeoDataFrame, basemap_name: str, output_column: str) -> gpd.GeoDataFrame:
    """Perform spatial intersection with basemap."""
    current_dir = Path(__file__).parent.parent
    basemap_path = current_dir / 'basemaps' / f'{basemap_name}.geojson'
    
    if not basemap_path.exists():
        print(f"⚠ Basemap not found: {basemap_path}")
        return gdf
    
    basemap_gdf = gpd.read_file(basemap_path)
    
    if gdf.crs != basemap_gdf.crs:
        basemap_gdf = basemap_gdf.to_crs(gdf.crs)
    
    gdf_temp = gdf.copy()
    valid_geom = gdf_temp['geometry'].notna()
    
    if valid_geom.sum() == 0:
        gdf[output_column] = None
        return gdf
    
    # Swap coordinates for spatial operations: Point(lat, lon) -> Point(lon, lat)
    gdf_temp.loc[valid_geom, 'geometry'] = gdf_temp.loc[valid_geom, 'geometry'].apply(
        lambda geom: Point(geom.y, geom.x) if geom else None
    )
    
    column_name = 'barrio_vereda' if 'barrio_vereda' in basemap_gdf.columns else 'comuna_corregimiento'
    gdf_joined = gpd.sjoin(gdf_temp, basemap_gdf[['geometry', column_name]], how='left', predicate='within')
    
    gdf[output_column] = gdf_joined[f'{column_name}_right'] if f'{column_name}_right' in gdf_joined.columns else gdf_joined.get(column_name)
    
    if 'index_right' in gdf.columns:
        gdf.drop(columns=['index_right'], inplace=True)
    
    print(f"✓ Spatial intersection completed: {output_column} ({gdf[output_column].notna().sum()} assigned)")
    return gdf


def normalize_text(text):
    """Normalize text by removing accents and converting to uppercase."""
    if pd.isna(text) or text is None:
        return ""
    text = str(text).strip()
    text = unicodedata.normalize('NFD', text)
    text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')
    return ' '.join(text.upper().split())


def normalize_comuna_value(value):
    """Normalize comuna values to standard format."""
    if pd.isna(value) or value is None or value == "":
        return None
    
    text = str(value).strip().upper()
    
    if text.startswith("COMUNA"):
        parts = text.split()
        if len(parts) >= 2:
            try:
                num = int(parts[1])
                if num > 22:
                    return "RURAL"
                elif num < 10:
                    return f"COMUNA {num:02d}"
                else:
                    return f"COMUNA {num}"
            except ValueError:
                pass
    
    return value


def find_best_match(value, standard_values, threshold=0.6):
    """Find best matching value from standard values."""
    if pd.isna(value) or value is None or value == "":
        return None
    
    normalized_value = normalize_text(value)
    normalized_standards = {normalize_text(std): std for std in standard_values if pd.notna(std)}
    
    matches = get_close_matches(normalized_value, normalized_standards.keys(), n=1, cutoff=threshold)
    
    if matches:
        return normalized_standards[matches[0]]
    
    return None


def normalize_administrative_values(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalize comuna and barrio values using standard catalogs."""
    current_dir = Path(__file__).parent.parent
    
    # Load standard values
    barrios_path = current_dir / 'basemaps' / 'barrios_veredas.geojson'
    comunas_path = current_dir / 'basemaps' / 'comunas_corregimientos.geojson'
    
    standard_barrios = []
    standard_comunas = []
    
    if barrios_path.exists():
        barrios_gdf = gpd.read_file(barrios_path)
        standard_barrios = barrios_gdf['barrio_vereda'].dropna().unique().tolist()
    
    if comunas_path.exists():
        comunas_gdf = gpd.read_file(comunas_path)
        standard_comunas = comunas_gdf['comuna_corregimiento'].dropna().unique().tolist()
    
    result_gdf = gdf.copy()
    
    # Normalize comuna_corregimiento
    if 'comuna_corregimiento' in result_gdf.columns and len(standard_comunas) > 0:
        for idx in result_gdf.index:
            original = result_gdf.at[idx, 'comuna_corregimiento']
            if pd.notna(original):
                normalized = normalize_comuna_value(original)
                if normalized != original:
                    result_gdf.at[idx, 'comuna_corregimiento'] = normalized
                else:
                    best_match = find_best_match(normalized, standard_comunas, threshold=0.7)
                    if best_match and best_match != original:
                        result_gdf.at[idx, 'comuna_corregimiento'] = best_match
    
    # Normalize barrio_vereda
    if 'barrio_vereda' in result_gdf.columns and len(standard_barrios) > 0:
        for idx in result_gdf.index:
            original = result_gdf.at[idx, 'barrio_vereda']
            if pd.notna(original):
                best_match = find_best_match(original, standard_barrios, threshold=0.7)
                if best_match and best_match != original:
                    result_gdf.at[idx, 'barrio_vereda'] = best_match
    
    print("✓ Administrative values normalized")
    return result_gdf


def create_validation_column(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Create fuera_rango validation column."""
    if 'comuna_corregimiento_2' not in gdf.columns or 'barrio_vereda_2' not in gdf.columns:
        return gdf
    
    result_gdf = gdf.copy()
    result_gdf['fuera_rango'] = None
    
    valid_geom = result_gdf['geometry'].notna()
    
    comuna_matches = (
        result_gdf['comuna_corregimiento'].notna() & 
        result_gdf['comuna_corregimiento_2'].notna() &
        (result_gdf['comuna_corregimiento'] == result_gdf['comuna_corregimiento_2'])
    )
    
    barrio_matches = (
        result_gdf['barrio_vereda'].notna() & 
        result_gdf['barrio_vereda_2'].notna() &
        (result_gdf['barrio_vereda'] == result_gdf['barrio_vereda_2'])
    )
    
    aceptable = valid_geom & (comuna_matches | barrio_matches)
    fuera_rango = valid_geom & ~(comuna_matches | barrio_matches)
    
    result_gdf.loc[aceptable, 'fuera_rango'] = 'ACEPTABLE'
    result_gdf.loc[fuera_rango, 'fuera_rango'] = 'FUERA DE RANGO'
    
    print(f"✓ Validation column created: {aceptable.sum()} ACEPTABLE, {fuera_rango.sum()} FUERA DE RANGO")
    return result_gdf


def parse_date(date_value):
    """Parse various date formats and return standardized datetime."""
    if pd.isna(date_value) or date_value is None:
        return None
    
    if isinstance(date_value, datetime):
        return date_value
    
    date_str = str(date_value).strip()
    
    if date_str == '' or date_str.lower() in ['nan', 'none', 'null']:
        return None
    
    # Try Excel serial date
    try:
        date_num = float(date_str)
        if 40000 <= date_num <= 60000:
            excel_epoch = datetime(1899, 12, 30)
            return excel_epoch + timedelta(days=date_num)
    except (ValueError, TypeError):
        pass
    
    # Try common date patterns
    patterns = [
        (r'(\d{2})/(\d{2})/(\d{4})', 'DMY'),
        (r'(\d{2})-(\d{2})-(\d{4})', 'DMY'),
        (r'(\d{4})/(\d{2})/(\d{2})', 'YMD'),
        (r'(\d{4})-(\d{2})-(\d{2})', 'YMD'),
    ]
    
    for pattern, format_type in patterns:
        match = re.search(pattern, date_str)
        if match:
            groups = match.groups()
            try:
                if format_type == 'YMD':
                    year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                else:
                    day, month, year = int(groups[0]), int(groups[1]), int(groups[2])
                
                if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
                    return datetime(year, month, day)
            except (ValueError, TypeError):
                continue
    
    # Last resort: pandas
    try:
        return pd.to_datetime(date_str, errors='coerce')
    except:
        return None


def standardize_dates(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Standardize fecha_inicio and fecha_fin."""
    result_gdf = gdf.copy()
    
    if 'fecha_inicio' in result_gdf.columns:
        result_gdf['fecha_inicio_std'] = result_gdf['fecha_inicio'].apply(parse_date)
        print(f"✓ fecha_inicio standardized: {result_gdf['fecha_inicio_std'].notna().sum()} valid")
    
    if 'fecha_fin' in result_gdf.columns:
        result_gdf['fecha_fin_std'] = result_gdf['fecha_fin'].apply(parse_date)
        print(f"✓ fecha_fin standardized: {result_gdf['fecha_fin_std'].notna().sum()} valid")
    
    return result_gdf


def export_to_geojson(gdf: gpd.GeoDataFrame, output_dir: Path) -> Path:
    """Export GeoDataFrame to GeoJSON with lon, lat format (will be converted to lat, lon during Firebase load)."""
    output_dir.mkdir(exist_ok=True, parents=True)
    output_file = output_dir / 'unidades_proyecto_transformed.geojson'
    
    gdf_export = gdf.copy()
    
    # Convert geometry to standard lon, lat format (GeoJSON standard)
    # Note: Firebase load will convert to [lat, lon] for Next.js/API compatibility
    valid_geom = gdf_export['geometry'].notna()
    if valid_geom.sum() > 0:
        gdf_export.loc[valid_geom, 'geometry'] = gdf_export.loc[valid_geom, 'geometry'].apply(
            lambda geom: Point(geom.y, geom.x) if geom else None
        )
    
    # Convert datetime to string
    for col in ['fecha_inicio_std', 'fecha_fin_std']:
        if col in gdf_export.columns:
            gdf_export[col] = gdf_export[col].apply(
                lambda x: x.isoformat() if pd.notna(x) and hasattr(x, 'isoformat') else None
            )
    
    gdf_export.to_file(output_file, driver='GeoJSON')
    print(f"✓ GeoJSON exported: {output_file.name} ({output_file.stat().st_size / 1024:.2f} KB)")
    return output_file


def convert_to_native_types(obj):
    """Recursively convert numpy types to native Python types."""
    if isinstance(obj, dict):
        return {key: convert_to_native_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_native_types(item) for item in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj


def generate_metrics_log(gdf: gpd.GeoDataFrame, original_df: pd.DataFrame, output_dir: Path) -> Tuple[Dict, Path]:
    """Generate comprehensive metrics log."""
    metrics = {
        "execution_timestamp": datetime.now().isoformat(),
        "process_name": "Transformación de Unidades de Proyecto",
        "version": "4.0",
        "data_loading": {
            "total_records_loaded": len(original_df),
            "total_columns_loaded": len(original_df.columns)
        },
        "data_transformation": {
            "total_records_transformed": len(gdf),
            "total_columns_final": len(gdf.columns),
            "upid_generated": gdf['upid'].notna().sum() if 'upid' in gdf.columns else 0
        },
        "geospatial_processing": {
            "records_with_geometry": gdf['geometry'].notna().sum() if 'geometry' in gdf.columns else 0,
            "records_without_geometry": gdf['geometry'].isna().sum() if 'geometry' in gdf.columns else len(gdf)
        },
        "validation": {
            "fuera_rango_aceptable": (gdf['fuera_rango'] == 'ACEPTABLE').sum() if 'fuera_rango' in gdf.columns else 0,
            "fuera_rango_invalid": (gdf['fuera_rango'] == 'FUERA DE RANGO').sum() if 'fuera_rango' in gdf.columns else 0
        },
        "date_processing": {
            "fecha_inicio_valid": gdf['fecha_inicio_std'].notna().sum() if 'fecha_inicio_std' in gdf.columns else 0,
            "fecha_fin_valid": gdf['fecha_fin_std'].notna().sum() if 'fecha_fin_std' in gdf.columns else 0
        },
        "summary": {
            "data_quality_score": round((gdf['fuera_rango'] == 'ACEPTABLE').sum() / len(gdf) * 100, 2) if 'fuera_rango' in gdf.columns and len(gdf) > 0 else 0,
            "geometry_completeness": round(gdf['geometry'].notna().sum() / len(gdf) * 100, 2) if 'geometry' in gdf.columns and len(gdf) > 0 else 0
        }
    }
    
    # Convert numpy types to native Python types for JSON serialization
    metrics = convert_to_native_types(metrics)
    
    # Save metrics with timestamp
    output_dir.mkdir(exist_ok=True, parents=True)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    metrics_file = output_dir / f'transformation_metrics_{timestamp_str}.json'
    
    with open(metrics_file, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    
    print(f"✓ Metrics saved: {metrics_file.name}")
    return metrics, metrics_file


def generate_analysis_report(metrics_file: Path, gdf: gpd.GeoDataFrame) -> Dict[str, Path]:
    """
    Generate comprehensive analysis and recommendations report.
    
    Args:
        metrics_file: Path to the metrics JSON file
        gdf: Processed GeoDataFrame
        
    Returns:
        Dictionary with paths to generated reports
    """
    print("\n" + "="*80)
    print("GENERANDO REPORTE DE ANÁLISIS Y RECOMENDACIONES")
    print("="*80)
    print()
    
    # Load metrics
    with open(metrics_file, 'r', encoding='utf-8') as f:
        metrics_data = json.load(f)
    
    print(f"✓ Métricas cargadas desde: {metrics_file.name}")
    print()
    
    # Extract key metrics
    total_records = metrics_data.get('data_transformation', {}).get('total_records_transformed', 0)
    quality_score = metrics_data.get('summary', {}).get('data_quality_score', 0)
    geometry_completeness = metrics_data.get('summary', {}).get('geometry_completeness', 0)
    
    records_with_geometry = metrics_data.get('geospatial_processing', {}).get('records_with_geometry', 0)
    acceptable_records = metrics_data.get('validation', {}).get('fuera_rango_aceptable', 0)
    invalid_records = metrics_data.get('validation', {}).get('fuera_rango_invalid', 0)
    date_valid_inicio = metrics_data.get('date_processing', {}).get('fecha_inicio_valid', 0)
    
    date_completeness = round((date_valid_inicio / total_records * 100), 2) if total_records > 0 else 0
    
    # Calculate quality levels
    geometry_quality = "EXCELENTE" if geometry_completeness >= 95 else "BUENA" if geometry_completeness >= 85 else "REGULAR" if geometry_completeness >= 70 else "DEFICIENTE"
    spatial_quality = "EXCELENTE" if quality_score >= 90 else "BUENA" if quality_score >= 75 else "REGULAR" if quality_score >= 60 else "DEFICIENTE"
    date_quality = "EXCELENTE" if date_completeness >= 95 else "BUENA" if date_completeness >= 80 else "REGULAR" if date_completeness >= 60 else "DEFICIENTE"
    
    # Generate recommendations
    recommendations = []
    
    if geometry_completeness < 95:
        missing_geom = total_records - records_with_geometry
        recommendations.append({
            "categoria": "Datos Geoespaciales",
            "prioridad": "ALTA",
            "issue": f"{missing_geom} registros ({100-geometry_completeness:.1f}%) sin coordenadas geográficas",
            "impacto": "Limita la capacidad de análisis espacial y visualización en mapas",
            "recomendacion": "Implementar proceso de geocodificación para registros sin coordenadas usando direcciones disponibles"
        })
    
    if invalid_records > 0:
        invalid_percentage = (invalid_records / total_records) * 100
        recommendations.append({
            "categoria": "Validación Espacial",
            "prioridad": "ALTA" if invalid_percentage > 20 else "MEDIA",
            "issue": f"{invalid_records} registros ({invalid_percentage:.1f}%) con inconsistencias entre ubicación y datos administrativos",
            "impacto": "Coordenadas no coinciden con comuna/barrio declarado, indica posibles errores de georreferenciación",
            "recomendacion": "Revisar y corregir coordenadas de registros FUERA DE RANGO mediante validación manual o re-geocodificación"
        })
    
    if date_completeness < 85:
        missing_dates = total_records - date_valid_inicio
        recommendations.append({
            "categoria": "Datos Temporales",
            "prioridad": "MEDIA",
            "issue": f"{missing_dates} registros ({100-date_completeness:.1f}%) sin fecha de inicio",
            "impacto": "Dificulta análisis temporal y seguimiento de cronogramas",
            "recomendacion": "Completar fechas faltantes consultando fuentes primarias (SECOP, documentos contractuales)"
        })
    
    # Build comprehensive report
    report = {
        "metadata": {
            "titulo": "Reporte de Análisis y Recomendaciones - Transformación de Unidades de Proyecto",
            "version": "1.0",
            "fecha_generacion": datetime.now().isoformat(),
            "fecha_ejecucion_etl": metrics_data.get('execution_timestamp'),
            "archivo_metricas": str(metrics_file.name)
        },
        "resumen_ejecutivo": {
            "total_registros": total_records,
            "calidad_global": {
                "score": quality_score,
                "nivel": spatial_quality,
                "interpretacion": f"{'Excelente calidad' if quality_score >= 90 else 'Buena calidad' if quality_score >= 75 else 'Calidad aceptable' if quality_score >= 60 else 'Requiere mejoras significativas'}"
            },
            "indicadores_clave": {
                "completitud_geometrica": {
                    "porcentaje": geometry_completeness,
                    "nivel": geometry_quality,
                    "registros_con_geometria": records_with_geometry,
                    "registros_sin_geometria": total_records - records_with_geometry
                },
                "completitud_temporal": {
                    "porcentaje": date_completeness,
                    "nivel": date_quality,
                    "registros_con_fechas": date_valid_inicio,
                    "registros_sin_fechas": total_records - date_valid_inicio
                },
                "validacion_espacial": {
                    "registros_aceptables": acceptable_records,
                    "registros_invalidos": invalid_records,
                    "porcentaje_aceptable": quality_score,
                    "registros_fuera_limites": 0
                }
            }
        },
        "analisis_detallado": {
            "procesamiento_datos": {
                "registros_cargados": metrics_data.get('data_loading', {}).get('total_records_loaded', 0),
                "registros_transformados": total_records,
                "columnas_finales": metrics_data.get('data_transformation', {}).get('total_columns_final', 0),
                "upid_generados": metrics_data.get('data_transformation', {}).get('upid_generated', 0)
            },
            "procesamiento_geoespacial": {
                "registros_geocodificados": records_with_geometry,
                "registros_sin_geocodificar": total_records - records_with_geometry,
                "sistema_coordenadas": "EPSG:4326"
            }
        },
        "recomendaciones": recommendations,
        "acciones_prioritarias": [
            {
                "prioridad": i+1,
                "accion": rec["recomendacion"],
                "registros_afectados": int(rec["issue"].split()[0]) if rec["issue"].split()[0].isdigit() else 0,
                "impacto_esperado": rec["impacto"]
            }
            for i, rec in enumerate(recommendations[:5])
        ],
        "metricas_calidad": {
            "completitud": {
                "geometrica": geometry_completeness,
                "temporal": date_completeness
            },
            "consistencia": {
                "espacial": quality_score
            }
        }
    }
    
    # Save JSON report with timestamp
    report_output_dir = metrics_file.parent.parent / 'reports'
    report_output_dir.mkdir(exist_ok=True, parents=True)
    
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_json_file = report_output_dir / f'analisis_recomendaciones_{timestamp_str}.json'
    
    with open(report_json_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"✓ Reporte JSON guardado: {report_json_file.name}")
    print(f"  Tamaño: {report_json_file.stat().st_size / 1024:.2f} KB")
    print()
    
    # Generate Markdown report
    md_lines = [
        "# Reporte de Análisis y Recomendaciones",
        "## Transformación de Unidades de Proyecto",
        "",
        f"**Fecha de Generación:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"**Versión:** {report['metadata']['version']}  ",
        f"**Archivo de Métricas:** `{report['metadata']['archivo_metricas']}`",
        "",
        "---",
        "",
        "## 📊 Resumen Ejecutivo",
        "",
        f"**Total de Registros Procesados:** {report['resumen_ejecutivo']['total_registros']:,}",
        "",
        "### Calidad Global",
        f"- **Score de Calidad:** {report['resumen_ejecutivo']['calidad_global']['score']:.1f}% ({report['resumen_ejecutivo']['calidad_global']['nivel']})",
        f"- **Interpretación:** {report['resumen_ejecutivo']['calidad_global']['interpretacion']}",
        "",
        "### Indicadores Clave",
        "",
        "#### 🗺️ Completitud Geométrica",
        f"- **Nivel:** {report['resumen_ejecutivo']['indicadores_clave']['completitud_geometrica']['nivel']}",
        f"- **Porcentaje:** {report['resumen_ejecutivo']['indicadores_clave']['completitud_geometrica']['porcentaje']:.1f}%",
        f"- **Con Geometría:** {report['resumen_ejecutivo']['indicadores_clave']['completitud_geometrica']['registros_con_geometria']:,} registros",
        f"- **Sin Geometría:** {report['resumen_ejecutivo']['indicadores_clave']['completitud_geometrica']['registros_sin_geometria']:,} registros",
        "",
        "#### 📅 Completitud Temporal",
        f"- **Nivel:** {report['resumen_ejecutivo']['indicadores_clave']['completitud_temporal']['nivel']}",
        f"- **Porcentaje:** {report['resumen_ejecutivo']['indicadores_clave']['completitud_temporal']['porcentaje']:.1f}%",
        f"- **Con Fechas:** {report['resumen_ejecutivo']['indicadores_clave']['completitud_temporal']['registros_con_fechas']:,} registros",
        f"- **Sin Fechas:** {report['resumen_ejecutivo']['indicadores_clave']['completitud_temporal']['registros_sin_fechas']:,} registros",
        "",
        "#### ✅ Validación Espacial",
        f"- **Registros Aceptables:** {report['resumen_ejecutivo']['indicadores_clave']['validacion_espacial']['registros_aceptables']:,} ({report['resumen_ejecutivo']['indicadores_clave']['validacion_espacial']['porcentaje_aceptable']:.1f}%)",
        f"- **Registros Inválidos:** {report['resumen_ejecutivo']['indicadores_clave']['validacion_espacial']['registros_invalidos']:,}",
        "",
        "---",
        "",
        "## 📈 Análisis Detallado",
        "",
        "### Procesamiento de Datos",
        f"- Registros cargados: {report['analisis_detallado']['procesamiento_datos']['registros_cargados']:,}",
        f"- Registros transformados: {report['analisis_detallado']['procesamiento_datos']['registros_transformados']:,}",
        f"- Columnas finales: {report['analisis_detallado']['procesamiento_datos']['columnas_finales']}",
        f"- UPID generados: {report['analisis_detallado']['procesamiento_datos']['upid_generados']:,}",
        "",
        "### Procesamiento Geoespacial",
        f"- Geocodificados: {report['analisis_detallado']['procesamiento_geoespacial']['registros_geocodificados']:,}",
        f"- Sin geocodificar: {report['analisis_detallado']['procesamiento_geoespacial']['registros_sin_geocodificar']:,}",
        f"- Sistema de coordenadas: `{report['analisis_detallado']['procesamiento_geoespacial']['sistema_coordenadas']}`",
        "",
        "---",
        "",
        "## 🎯 Recomendaciones",
        ""
    ]
    
    for i, rec in enumerate(report['recomendaciones'], 1):
        priority_emoji = "🔴" if rec['prioridad'] == "ALTA" else "🟡" if rec['prioridad'] == "MEDIA" else "🟢"
        md_lines.extend([
            f"### {i}. {rec['categoria']} {priority_emoji}",
            f"**Prioridad:** {rec['prioridad']}  ",
            f"**Problema:** {rec['issue']}  ",
            f"**Impacto:** {rec['impacto']}  ",
            f"**Recomendación:** {rec['recomendacion']}",
            ""
        ])
    
    if report['acciones_prioritarias']:
        md_lines.extend([
            "---",
            "",
            "## ⚡ Acciones Prioritarias",
            ""
        ])
        
        for action in report['acciones_prioritarias']:
            md_lines.extend([
                f"### Prioridad {action['prioridad']}",
                f"**Acción:** {action['accion']}  ",
                f"**Registros Afectados:** {action['registros_afectados']:,}  ",
                f"**Impacto Esperado:** {action['impacto_esperado']}",
                ""
            ])
    
    md_lines.extend([
        "---",
        "",
        "## 📊 Métricas de Calidad",
        "",
        "### Completitud",
        f"- **Geométrica:** {report['metricas_calidad']['completitud']['geometrica']:.1f}%",
        f"- **Temporal:** {report['metricas_calidad']['completitud']['temporal']:.1f}%",
        "",
        "### Consistencia",
        f"- **Espacial:** {report['metricas_calidad']['consistencia']['espacial']:.1f}%",
        "",
        "---",
        "",
        f"*Reporte generado automáticamente - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"
    ])
    
    # Save Markdown report with timestamp
    report_md_file = report_output_dir / f'analisis_recomendaciones_{timestamp_str}.md'
    with open(report_md_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(md_lines))
    
    print(f"✓ Reporte Markdown guardado: {report_md_file.name}")
    print(f"  Tamaño: {report_md_file.stat().st_size / 1024:.2f} KB")
    print()
    
    print("=" * 80)
    print("✓ GENERACIÓN DE REPORTES COMPLETADA")
    print("=" * 80)
    print()
    print(f"📂 Archivos generados:")
    print(f"   - JSON: {report_json_file}")
    print(f"   - Markdown: {report_md_file}")
    print()
    print(f"📊 Resumen:")
    print(f"   - Calidad Global: {quality_score:.1f}% ({spatial_quality})")
    print(f"   - Recomendaciones: {len(recommendations)}")
    print(f"   - Acciones Prioritarias: {len(report['acciones_prioritarias'])}")
    
    return {
        'json': report_json_file,
        'markdown': report_md_file
    }


def _process_unidades_proyecto_dataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Internal function to process unidades proyecto dataframe using functional pipeline.
    This function can be used with both file-based and in-memory processing.
    
    Args:
        df: Input dataframe to process
        
    Returns:
        Processed GeoDataFrame
    """
    print("\n" + "="*60)
    print("PROCESSING UNIDADES DE PROYECTO DATA")
    print("="*60)
    
    # Drop unnecessary columns first
    df_clean = df.copy()
    df_clean.drop(columns=['MicroTIO', 'dataframe', 'microtio', 'centros_gravedad'], inplace=True, errors='ignore')
    
    # Phase 1: Basic data cleaning and transformation
    print("\n[Phase 1: Basic Transformation]")
    basic_pipeline = compose(
        lambda df: generate_upid_for_records(df),
        lambda df: add_computed_columns(df),
        lambda df: clean_data_types(df),
        lambda df: normalize_estado_values(df),
        lambda df: normalize_tipo_intervencion_values(df),
        lambda df: apply_title_case_to_text_fields(df)
    )
    df_transformed = basic_pipeline(df_clean)
    
    # Phase 2: Geospatial processing
    print("\n[Phase 2: Geospatial Processing]")
    gdf = convert_to_geodataframe(df_transformed)
    
    if isinstance(gdf, gpd.GeoDataFrame):
        gdf = correct_coordinate_formats(gdf)
        gdf = create_final_geometry(gdf)
        
        # Phase 3: Spatial intersections
        print("\n[Phase 3: Spatial Intersections]")
        gdf = perform_spatial_intersection(gdf, 'barrios_veredas', 'barrio_vereda_2')
        gdf = perform_spatial_intersection(gdf, 'comunas_corregimientos', 'comuna_corregimiento_2')
        
        # Phase 4: Normalization and validation
        print("\n[Phase 4: Normalization & Validation]")
        gdf = normalize_administrative_values(gdf)
        gdf = create_validation_column(gdf)
    
    # Phase 5: Date standardization
    print("\n[Phase 5: Date Standardization]")
    gdf = standardize_dates(gdf)
    
    # Phase 6: Export and metrics
    print("\n[Phase 6: Export & Metrics]")
    current_dir = Path(__file__).parent.parent
    output_dir = current_dir / 'app_outputs'
    logs_dir = output_dir / 'logs'
    
    output_file = export_to_geojson(gdf, output_dir)
    metrics, metrics_file = generate_metrics_log(gdf, df, logs_dir)
    
    # Generate comprehensive analysis and recommendations report
    report_files = generate_analysis_report(metrics_file, gdf)
    
    print(f"\n✓ Processing completed: {len(gdf)} rows, {len(gdf.columns)} columns")
    print(f"✓ Quality score: {metrics['summary']['data_quality_score']:.1f}%")
    print(f"✓ Geometry completeness: {metrics['summary']['geometry_completeness']:.1f}%")
    print(f"\n📁 Output files:")
    print(f"   - GeoJSON: {output_file}")
    print(f"   - Metrics: {metrics_file}")
    print(f"   - Report (JSON): {report_files['json']}")
    print(f"   - Report (Markdown): {report_files['markdown']}")
    
    return gdf


def transform_and_save_unidades_proyecto(
    data: Optional[pd.DataFrame] = None, 
    use_extraction: bool = True
) -> Optional[gpd.GeoDataFrame]:
    """
    Main function to transform and save unidades de proyecto data.
    Implements complete functional pipeline for transformation and output generation.
    
    Args:
        data: Optional DataFrame with extracted data (if provided, skips extraction)
        use_extraction: If True and data is None, extracts from Google Drive
    """
    try:
        print("="*80)
        print("UNIDADES DE PROYECTO FUNCTIONAL TRANSFORMATION")
        print("="*80)
        
        # Transform the data using functional pipeline
        # Pass data and use_extraction to avoid duplicate extraction
        gdf_processed = unidades_proyecto_transformer(data=data, use_extraction=use_extraction)
        
        if gdf_processed is not None:
            print("\n" + "="*60)
            print("TRANSFORMATION COMPLETED SUCCESSFULLY")
            print("="*60)
            
            # Display summary
            print(f"\nTotal records: {len(gdf_processed):,}")
            print(f"Total columns: {len(gdf_processed.columns)}")
            
            if isinstance(gdf_processed, gpd.GeoDataFrame):
                print(f"Geometries: {gdf_processed['geometry'].notna().sum():,}")
                if 'fuera_rango' in gdf_processed.columns:
                    print(f"Quality (ACEPTABLE): {(gdf_processed['fuera_rango'] == 'ACEPTABLE').sum():,}")
            
            return gdf_processed
        
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
    gdf_result = main()
    
    if gdf_result is not None:
        print("\n" + "="*60)
        print("TRANSFORMATION PIPELINE COMPLETED")
        print("="*60)
        print(f"✓ Processed data: {len(gdf_result):,} records")
        print(f"✓ Total columns: {len(gdf_result.columns)}")
        
        if isinstance(gdf_result, gpd.GeoDataFrame):
            print(f"✓ GeoDataFrame type: {type(gdf_result).__name__}")
            print(f"✓ Geometries: {gdf_result['geometry'].notna().sum():,}")
        
        print(f"✓ Data transformation completed successfully")
        
        # Upload outputs to S3
        try:
            print("\n" + "="*80)
            print("UPLOADING OUTPUTS TO S3")
            print("="*80)
            
            # Import S3Uploader
            sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
            from s3_uploader import S3Uploader
            
            # Initialize uploader
            uploader = S3Uploader("aws_credentials.json")
            
            # Upload all outputs
            current_dir = Path(__file__).parent.parent
            output_dir = current_dir / 'app_outputs'
            
            upload_results = uploader.upload_all_outputs(
                output_dir=output_dir,
                upload_data=True,
                upload_logs=True,
                upload_reports=True
            )
            
            print("\n" + "="*80)
            print("S3 UPLOAD COMPLETED")
            print("="*80)
            
            # Optional: Trigger Cloud Function to load to Firestore
            trigger_cloud_function = os.environ.get('TRIGGER_CLOUD_FUNCTION', 'false').lower() == 'true'
            cloud_function_url = os.environ.get('CLOUD_FUNCTION_URL', '')
            
            if trigger_cloud_function and cloud_function_url:
                try:
                    print("\n" + "="*80)
                    print("TRIGGERING CLOUD FUNCTION - FIRESTORE LOAD")
                    print("="*80)
                    
                    import requests
                    response = requests.post(cloud_function_url, timeout=600)
                    
                    if response.status_code == 200:
                        result = response.json()
                        print("\n✅ Cloud Function ejecutada exitosamente")
                        
                        if 'stats' in result and 'unidades_proyecto' in result['stats']:
                            stats = result['stats']['unidades_proyecto']
                            print(f"  • Documentos nuevos: {stats.get('new', 0)}")
                            print(f"  • Documentos actualizados: {stats.get('updated', 0)}")
                            print(f"  • Documentos sin cambios: {stats.get('unchanged', 0)}")
                        
                        if 'stats' in result and 'logs' in result['stats']:
                            print(f"  • Logs cargados: {result['stats']['logs'].get('new', 0) + result['stats']['logs'].get('updated', 0)}")
                        
                        if 'stats' in result and 'reports' in result['stats']:
                            print(f"  • Reportes cargados: {result['stats']['reports'].get('new', 0) + result['stats']['reports'].get('updated', 0)}")
                    else:
                        print(f"\n⚠️ Cloud Function retornó código: {response.status_code}")
                        print(f"Respuesta: {response.text[:500]}")
                        
                except Exception as cf_error:
                    print(f"\n⚠️ Error al ejecutar Cloud Function: {cf_error}")
                    print("Datos guardados en S3, pero no se cargaron a Firestore")
            
        except Exception as e:
            print("\n" + "="*80)
            print("S3 UPLOAD FAILED")
            print("="*80)
            print(f"✗ Error uploading to S3: {e}")
            import traceback
            traceback.print_exc()
            print("\n⚠️ Transformation was successful, but S3 upload failed")
        
    else:
        print("\n" + "="*60)
        print("TRANSFORMATION PIPELINE FAILED")
        print("="*60)
        print("✗ Could not process unidades de proyecto data")
