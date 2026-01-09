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

# Load standard categories from JSON
def load_standard_categories() -> Dict[str, List[str]]:
    """Load standard categories from JSON file."""
    current_dir = Path(__file__).parent.parent
    categories_path = current_dir / 'app_inputs' / 'unidades_proyecto_input' / 'defaults' / 'unidades_proyecto_std_categories.json'
    
    try:
        with open(categories_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARNING] Warning: Could not load standard categories: {e}")
        return {}

# Global variable to cache standard categories
STANDARD_CATEGORIES = load_standard_categories()

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
    print(f"[WARNING] Warning: Could not import extraction module: {e}")
    print("   Falling back to file-based processing")
    extract_unidades_proyecto_data = None
    EXTRACTION_AVAILABLE = False

# Import geospatial clustering module
try:
    # Try relative import first (when used as module)
    from .geospatial_clustering import (
        agrupar_datos_geoespacial,
        convert_unidades_to_dataframe
    )
    CLUSTERING_AVAILABLE = True
except (ImportError, ValueError):
    try:
        # Try direct import (when run as script)
        from geospatial_clustering import (
            agrupar_datos_geoespacial,
            convert_unidades_to_dataframe
        )
        CLUSTERING_AVAILABLE = True
    except ImportError as e:
        print(f"[WARNING] Warning: Could not import clustering module: {e}")
        print("   Falling back to simple UPID generation")
        agrupar_datos_geoespacial = None
        convert_unidades_to_dataframe = None
        CLUSTERING_AVAILABLE = False


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
def clean_monetary_column(df: pd.DataFrame, column_name: str, as_integer: bool = False) -> pd.DataFrame:
    """Clean a monetary column using functional approach with robust type handling.
    
    Args:
        df: DataFrame to clean
        column_name: Name of the monetary column to clean
        as_integer: If True, converts to int64 (no decimals). If False, keeps as float64 with 2 decimals.
    
    Returns:
        Cleaned DataFrame with monetary column properly formatted
    """
    if column_name in df.columns:
        df = df.copy()
        
        print(f"Cleaning monetary column: {column_name} (as_integer={as_integer})")
        
        # Apply the cleaning function to all values
        df[column_name] = df[column_name].apply(clean_monetary_value)
        
        # Ensure it's numeric and handle any remaining issues
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').fillna(0.0)
        
        # Final validation - ensure no negative values for monetary columns
        negative_mask = df[column_name] < 0
        if negative_mask.any():
            negative_count = negative_mask.sum()
            print(f"  Warning: Found {negative_count} negative values in {column_name}, converting to 0")
            df.loc[negative_mask, column_name] = 0.0
        
        # Convert to integer if requested (removes all decimals)
        if as_integer:
            df[column_name] = df[column_name].round(0).astype('int64')
            print(f"  [OK] Converted to int64 (no decimals)")
        else:
            # Keep as float but ensure consistent precision
            df[column_name] = df[column_name].round(2).astype('float64')
            print(f"  [OK] Kept as float64 (2 decimals)")
        
        # Report statistics
        positive_values = (df[column_name] > 0).sum()
        zero_values = (df[column_name] == 0).sum()
        total_values = len(df[column_name])
        
        print(f"  {column_name} validation results:")
        print(f"    Positive values: {positive_values}")
        print(f"    Zero values: {zero_values}")
        print(f"    Total values: {total_values}")
        
    return df


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
            # Check if it's likely thousands separators (multiple dots or number after last dot equals 3 digits)
            parts = cleaned.split('.')
            if len(parts) > 2 or (len(parts) == 2 and len(parts[1]) == 3):
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
def generate_upid_for_records_simple(df: pd.DataFrame) -> pd.DataFrame:
    """
    LEGACY: Simple UPID generation (one UPID per row).
    This function is kept for backward compatibility or if clustering is disabled.
    
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
    
    print(f"[OK] UPID Generation (Simple):")
    print(f"  - Existing upids preserved: {len(existing_upids) - new_upids_count}")
    print(f"  - New upids generated: {new_upids_count}")
    print(f"  - Total upids: {len(existing_upids)}")
    print(f"  - Next available number: UNP-{next_consecutive}")
    
    return result_df


def generate_upid_for_records(df: pd.DataFrame, use_clustering: bool = True) -> pd.DataFrame:
    """
    NEW: Generate UPIDs using geospatial clustering (recommended).
    
    Groups interventions into project units based on physical location and textual similarity,
    then assigns UPIDs to the consolidated units. Each UPID represents a distinct project unit
    that can contain multiple interventions.
    
    Features:
    - DBSCAN clustering for nearby locations (< 20m)
    - Fuzzy matching for records without GPS coordinates
    - Subsidios exclusion (each subsidy is independent)
    - nombre_up_detalle differentiation (different details = different units)
    
    Args:
        df: DataFrame with intervention data
        use_clustering: If True, uses geospatial clustering; if False, uses simple generation
        
    Returns:
        DataFrame with upid column and n_intervenciones populated
    """
    if not use_clustering or not CLUSTERING_AVAILABLE:
        print("[WARNING] Clustering disabled or unavailable, using simple UPID generation")
        return generate_upid_for_records_simple(df)
    
    print(f"\n{'='*80}")
    print(f"🌍 GENERATING UPIDs WITH GEOSPATIAL CLUSTERING")
    print(f"{'='*80}")
    
    try:
        # Aplicar clustering geoespacial
        unidades_dict = agrupar_datos_geoespacial(df)
        
        # Convertir de vuelta a DataFrame plano
        df_with_upids = convert_unidades_to_dataframe(unidades_dict)
        
        print(f"\n[OK] Clustering completado:")
        print(f"   • Unidades de proyecto: {df_with_upids['upid'].nunique()}")
        print(f"   • Intervenciones totales: {len(df_with_upids)}")
        print(f"   • Promedio intervenciones/unidad: {len(df_with_upids) / df_with_upids['upid'].nunique():.2f}")
        
        return df_with_upids
        
    except Exception as e:
        print(f"[ERROR] Error en clustering geoespacial: {e}")
        import traceback
        traceback.print_exc()
        print("\n[WARNING] Fallback to simple UPID generation")
        return generate_upid_for_records_simple(df)


def add_computed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add computed columns for metadata."""
    result_df = df.copy()
    
    # Add computed columns without modifying original data
    new_columns = {
        'processed_timestamp': datetime.now().isoformat()
    }
    
    for col, default_value in new_columns.items():
        result_df[col] = default_value
    
    print(f"[OK] Added computed columns: {list(new_columns.keys())}")
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


def clean_avance_obra_column(df: pd.DataFrame, column: str = 'avance_obra') -> pd.DataFrame:
    """Clean avance_obra column - removes special characters and ensures exactly 2 decimal places.
    
    Handles:
    - Percentage signs (%)
    - Parentheses ()
    - Commas as decimal separators
    - Text like 'cero'
    - Spaces and other special characters
    
    Returns values rounded to exactly 2 decimal places, always as float.
    """
    if column not in df.columns:
        return df
    
    result_df = df.copy()
    
    def clean_avance_value(val):
        if pd.isna(val) or val is None:
            return 0.00
        
        # Si ya es numérico, procesarlo directamente
        if isinstance(val, (int, float)):
            numeric_val = float(val)
            # Limitar al rango 0-100
            numeric_val = max(0.0, min(100.0, numeric_val))
            return round(numeric_val, 2)
        
        # Convertir a string
        val_str = str(val).strip().lower()
        
        # Manejar valores vacíos
        if val_str in ['', 'nan', 'none', 'null']:
            return 0.00
        
        # Reemplazar texto especial
        val_str = val_str.replace('cero', '0')
        
        # Eliminar caracteres especiales: %, $, paréntesis, espacios
        val_str = val_str.replace('%', '')
        val_str = val_str.replace('$', '')
        val_str = val_str.replace('(', '')
        val_str = val_str.replace(')', '')
        val_str = val_str.replace(' ', '')
        
        # Reemplazar coma por punto (formato decimal europeo)
        val_str = val_str.replace(',', '.')
        
        # Eliminar cualquier caracter no numérico excepto punto y signo negativo
        val_str = ''.join(c for c in val_str if c.isdigit() or c == '.' or c == '-')
        
        # Manejar múltiples puntos (tomar solo el primero)
        if val_str.count('.') > 1:
            parts = val_str.split('.')
            val_str = parts[0] + '.' + ''.join(parts[1:])
        
        # Manejar casos vacíos después de limpieza
        if not val_str or val_str in ['.', '-', '-.']:
            return 0.00
        
        # Convertir a número
        try:
            numeric_val = float(val_str)
            # Limitar al rango 0-100
            numeric_val = max(0.0, min(100.0, numeric_val))
            # Redondear a exactamente 2 decimales
            return round(numeric_val, 2)
        except ValueError:
            return 0.00
    
    result_df[column] = result_df[column].apply(clean_avance_value)
    
    # Asegurar que la columna sea float64
    result_df[column] = result_df[column].astype('float64')
    
    # Verificar valores fuera de rango (0-100) - debería ser 0 después de la limpieza
    out_of_range = result_df[(result_df[column] < 0) | (result_df[column] > 100)]
    if len(out_of_range) > 0:
        print(f"[WARNING]  Advertencia: {len(out_of_range)} valores de '{column}' fuera del rango 0-100 fueron ajustados")
        result_df[column] = result_df[column].clip(0, 100).round(2)
    
    print(f"[OK] Columna '{column}' limpiada: valores numéricos con 2 decimales (rango 0-100)")
    
    return result_df


def clean_integer_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Clean a single integer column - converts to integers without decimals.
    
    For 'ano' column, validates that values are valid years (2024-2030).
    """
    if column in df.columns:
        result_df = df.copy()
        # Convert to numeric, then to integer (removing decimals)
        result_df[column] = pd.to_numeric(result_df[column], errors='coerce').fillna(0).astype('int64')
        
        # Validación específica para columna 'ano'
        if column == 'ano':
            valid_years = [2024, 2025, 2026, 2027, 2028, 2029, 2030]
            
            # Contar años inválidos
            invalid_mask = ~result_df[column].isin(valid_years + [0])
            invalid_count = invalid_mask.sum()
            
            if invalid_count > 0:
                print(f"[WARNING]  Advertencia: {invalid_count} registros con años fuera del rango válido (2024-2030)")
            
            # Asignar año por defecto (2024) a valores inválidos
            result_df.loc[invalid_mask, column] = 2024
            
            # Verificar valores cero
            zero_mask = result_df[column] == 0
            zero_count = zero_mask.sum()
            if zero_count > 0:
                print(f"[WARNING]  Advertencia: {zero_count} registros sin año, asignando 2024")
                result_df.loc[zero_mask, column] = 2024
            
            print(f"[OK] Columna 'ano' validada: todos los valores son enteros entre 2024-2030")
        
        return result_df
    return df


def clean_bpin_column(df: pd.DataFrame) -> pd.DataFrame:
    """Clean BPIN column - keeps as string for alphanumeric codes or converts to integer."""
    if 'bpin' in df.columns:
        result_df = df.copy()
        def process_bpin(value):
            if pd.isna(value) or value is None:
                return None
            str_value = str(value).strip()
            if str_value == '' or str_value.lower() in ['nan', 'null']:
                return None
            # Try to convert to integer if it's purely numeric
            if str_value.replace('.', '').replace(',', '').isdigit():
                try:
                    # Remove any separators and convert directly to int
                    clean_num = str_value.replace('.', '').replace(',', '')
                    return int(clean_num)
                except (ValueError, TypeError):
                    return str_value
            else:
                # Keep as string for alphanumeric codes
                return str_value
        
        result_df['bpin'] = result_df['bpin'].apply(process_bpin)
        return result_df
    return df


def clean_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize data types using functional composition."""
    
    # Define column types
    text_columns = ['nickname_detalle', 'direccion', 'descripcion_intervencion', 'identificador', 'nickname']
    # Variables monetarias que deben ser enteros (sin decimales)  
    integer_monetary_columns = ['presupuesto_base', 'ppto_base']
    # Variables numéricas enteras (sin decimales)
    integer_columns = ['bpin', 'ano']
    # Variables numéricas que pueden tener decimales
    decimal_columns = ['avance_obra', 'avance_fisico_obra']
    
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
        # Usar limpieza específica para avance_obra y avance_fisico_obra
        if col in ['avance_obra', 'avance_fisico_obra']:
            cleaning_functions.append(partial(clean_avance_obra_column, column=col))
        else:
            cleaning_functions.append(partial(clean_numeric_column_safe, column=col))
    
    # Add BPIN column cleaner
    cleaning_functions.append(clean_bpin_column)
    
    # Apply all cleaning functions using functional composition
    return pipe(df, *cleaning_functions)


# Semantic data cleaning functions
def normalize_estado_values(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize estado values to standardized labels with business rules.
    
    Business Rules:
    - Valid states from JSON: "En alistamiento", "En ejecución", "Terminado", "Suspendido", "Inaugurado"
    - Special values "Suspendido" and "Inaugurado" are preserved as-is (no modification)
    - If avance_obra is 0, set estado to 'En alistamiento'
    - If avance_obra is 100%, set estado to 'Terminado'
    - Otherwise, normalize estado based on text patterns for the three main states
    - Unknown states are normalized to the closest valid state or default to 'En ejecución'
    """
    if 'estado' in df.columns:
        result_df = df.copy()
        
        # Get standard estado values from JSON
        standard_estados = set(STANDARD_CATEGORIES.get('estado', []))
        # Track unknown states for reporting
        unknown_states = set()
        
        # Estados especiales que NUNCA deben modificarse
        PRESERVED_STATES = {'Suspendido', 'Inaugurado'}
        
        # Standardize all estado values (case-insensitive)
        def standardize_estado(row):
            val = row.get('estado')
            avance_obra = row.get('avance_obra')
            
            # Si no hay valor de estado, determinar por avance_obra
            if pd.isna(val) or val is None or str(val).strip() == '':
                # Si no hay avance_obra, asumir alistamiento
                if pd.isna(avance_obra) or avance_obra is None:
                    return 'En alistamiento'
                # Determinar por avance_obra
                try:
                    avance_numeric = float(str(avance_obra).strip().replace(',', '.').replace('cero', '0').replace('(', '').replace(')', ''))
                    if avance_numeric == 0.0:
                        return 'En alistamiento'
                    elif avance_numeric >= 100.0:
                        return 'Terminado'
                    else:
                        return 'En ejecución'
                except (ValueError, TypeError):
                    return 'En alistamiento'
            
            val_str = str(val).strip()
            val_lower = val_str.lower()
            
            # PRIORIDAD 1: Preservar valores especiales (sin modificar) - RETORNAR INMEDIATAMENTE
            if val_str in PRESERVED_STATES or val_lower in {'suspendido', 'inaugurado'}:
                return val_lower.title()  # Retorna "Suspendido" o "Inaugurado"
            
            # PRIORIDAD 2: Aplicar lógica de avance_obra para los tres estados principales
            if avance_obra is not None:
                try:
                    avance_numeric = float(str(avance_obra).strip().replace(',', '.').replace('cero', '0').replace('(', '').replace(')', ''))
                    
                    # Si es exactamente 0, es alistamiento
                    if avance_numeric == 0.0:
                        return 'En alistamiento'
                    
                    # Si es 100 o más, está terminado
                    if avance_numeric >= 100.0:
                        return 'Terminado'
                    
                    # Si tiene avance entre 0 y 100, continuar con normalización
                    
                except (ValueError, TypeError):
                    pass
            
            # PRIORIDAD 3: Normalizar basado en patrones de texto para los tres estados principales
            if 'socializaci' in val_lower or 'alistamiento' in val_lower or 'planeaci' in val_lower or 'preparaci' in val_lower or 'por iniciar' in val_lower:
                return 'En alistamiento'
            elif 'ejecuci' in val_lower or 'proceso' in val_lower or 'construcci' in val_lower or 'desarrollo' in val_lower:
                return 'En ejecución'
            elif 'finalizado' in val_lower or 'terminado' in val_lower or 'completado' in val_lower or 'concluido' in val_lower or 'entregado' in val_lower or 'liquidaci' in val_lower:
                return 'Terminado'
            else:
                # Log unknown state for reporting
                unknown_states.add(val_str)
                
                # Default basado en avance_obra si está disponible
                try:
                    if avance_obra is not None:
                        avance_numeric = float(str(avance_obra).strip().replace(',', '.'))
                        if avance_numeric >= 100:
                            return 'Terminado'
                        elif avance_numeric == 0:
                            return 'En alistamiento'
                except:
                    pass
                
                return 'En ejecución'  # Default state
        
        result_df['estado'] = result_df.apply(standardize_estado, axis=1)
        
        # Report unknown states found
        if unknown_states:
            print(f"[WARNING] WARNING: Found {len(unknown_states)} unknown estado values that were normalized:")
            for state in sorted(unknown_states):
                count = (df['estado'].astype(str) == state).sum()
                print(f"   - '{state}' ({count} occurrences)")
        
        # Validate that only valid states remain
        final_states = set(result_df['estado'].dropna().unique())
        invalid_final = final_states - standard_estados
        
        if invalid_final:
            print(f"[ERROR] ERROR: Invalid estados still present after normalization: {invalid_final}")
        else:
            print(f"[OK] Estados normalizados exitosamente. Estados válidos: {sorted(final_states)}")
            for state in sorted(final_states):
                count = (result_df['estado'] == state).sum()
                print(f"   - '{state}': {count} registros")
        
        return result_df
    return df


def validate_and_normalize_category(value: Any, category_name: str, threshold: float = 0.7) -> Optional[str]:
    """Validate and normalize a categorical value using fuzzy matching.
    
    Args:
        value: The value to validate
        category_name: Name of the category in STANDARD_CATEGORIES
        threshold: Minimum similarity score for fuzzy matching (0.0-1.0)
        
    Returns:
        Standardized value or None if value is null
    """
    if pd.isna(value) or value is None or str(value).strip() == '':
        return None
    
    val_str = str(value).strip()
    standard_values = STANDARD_CATEGORIES.get(category_name, [])
    
    if not standard_values:
        print(f"[WARNING] Warning: No standard values found for category '{category_name}'")
        return val_str
    
    # Check for exact match (case-insensitive)
    for std_val in standard_values:
        if val_str.lower() == std_val.lower():
            return std_val  # Return with standard capitalization
    
    # Try fuzzy matching
    matches = get_close_matches(val_str, standard_values, n=1, cutoff=threshold)
    if matches:
        return matches[0]
    
    # No match found - return original value
    return val_str


def normalize_categorical_column(df: pd.DataFrame, column_name: str, threshold: float = 0.7) -> pd.DataFrame:
    """Normalize a categorical column using standard categories from JSON.
    
    Args:
        df: DataFrame to process
        column_name: Name of the column to normalize
        threshold: Minimum similarity score for fuzzy matching
        
    Returns:
        DataFrame with normalized column
    """
    if column_name not in df.columns:
        return df
    
    result_df = df.copy()
    
    # Track normalization statistics
    total_values = result_df[column_name].notna().sum()
    changed_values = 0
    unknown_values = set()
    
    # Get standard values for this category
    standard_values = set(STANDARD_CATEGORIES.get(column_name, []))
    
    if not standard_values:
        print(f"[WARNING] Warning: No standard values found for '{column_name}', skipping normalization")
        return result_df
    
    # Normalize each value
    for idx in result_df.index:
        original_val = result_df.at[idx, column_name]
        if pd.notna(original_val) and original_val is not None:
            normalized_val = validate_and_normalize_category(original_val, column_name, threshold)
            
            if normalized_val != original_val:
                result_df.at[idx, column_name] = normalized_val
                changed_values += 1
            
            # Track unknown values (not in standard list)
            if normalized_val not in standard_values:
                unknown_values.add(f"{original_val} -> {normalized_val}")
    
    # Report results
    print(f"[OK] Normalized '{column_name}':")
    print(f"   - Total values: {total_values}")
    print(f"   - Changed: {changed_values}")
    print(f"   - Unknown: {len(unknown_values)}")
    
    if unknown_values:
        print(f"   [WARNING] Values not matching standard categories:")
        for unknown in sorted(unknown_values)[:5]:  # Show first 5
            print(f"      - {unknown}")
        if len(unknown_values) > 5:
            print(f"      ... and {len(unknown_values) - 5} more")
    
    return result_df


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
    """Apply title case to relevant text columns.
    
    NOTE: Does NOT apply title case to 'estado' and 'tipo_intervencion' 
    as these fields have standardized values that must maintain exact capitalization.
    """
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
        print(f"[OK] Loaded {len(df)} records from {os.path.basename(file_path)}")
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
                print(f"[OK] Extracted {len(df):,} records from Google Drive")
                print(f"[OK] Total columns: {len(df.columns)}")
                print()
                
                if process_in_memory:
                    return process_in_memory(df, _process_unidades_proyecto_dataframe)
                else:
                    return _process_unidades_proyecto_dataframe(df)
            else:
                print("[WARNING] Extraction returned empty data, falling back to file-based processing")
                
        except Exception as e:
            print(f"[WARNING] Error during Google Drive extraction: {e}")
            print("   Falling back to file-based processing")
            import traceback
            traceback.print_exc()
    
    # Priority 2: If data is provided in memory, process it directly
    if data is not None:
        print("[START] Processing data in memory (no temporary files)")
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            print(f"[ERROR] Unsupported data type: {type(data)}")
            return None
        
        if process_in_memory:
            return process_in_memory(df, _process_unidades_proyecto_dataframe)
        else:
            return _process_unidades_proyecto_dataframe(df)
    
    # Priority 3: Fallback to file-based processing
    print(f"[FILE] Processing data from files in: {data_directory}")
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


def convert_to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert DataFrame to GeoDataFrame with proper geometry validation."""
    if 'lat' not in df.columns or 'lon' not in df.columns:
        print("⚠ Warning: lat/lon columns not found, skipping geodataframe conversion")
        return df
    
    gdf = df.copy()
    
    def safe_convert_to_float(value):
        """Convierte valor a float, validando que sea numérico."""
        if pd.isna(value) or value is None:
            return None
        try:
            val_str = str(value).strip()
            if val_str in ['', 'nan', 'None', 'null']:
                return None
            return float(val_str)
        except (ValueError, TypeError):
            return None
    
    # Convertir lat/lon a numéricos
    gdf['lat_numeric'] = gdf['lat'].apply(safe_convert_to_float)
    gdf['lon_numeric'] = gdf['lon'].apply(safe_convert_to_float)
    
    # Validar que están en rangos válidos para Cali
    def is_valid_lat(lat):
        if lat is None or pd.isna(lat):
            return False
        return 3.0 <= lat <= 4.0  # Rango válido para Cali
    
    def is_valid_lon(lon):
        if lon is None or pd.isna(lon):
            return False
        return -77.0 <= lon <= -76.0  # Rango válido para Cali
    
    # Aplicar validaciones
    valid_lat = gdf['lat_numeric'].apply(is_valid_lat)
    valid_lon = gdf['lon_numeric'].apply(is_valid_lon)
    valid_coords = valid_lat & valid_lon
    
    # Reportar coordenadas inválidas
    invalid_count = (~valid_coords).sum()
    if invalid_count > 0:
        print(f"[WARNING]  Advertencia: {invalid_count} registros con coordenadas inválidas o fuera de rango")
        invalid_lats = gdf[~valid_lat & gdf['lat_numeric'].notna()]['lat_numeric'].unique()
        invalid_lons = gdf[~valid_lon & gdf['lon_numeric'].notna()]['lon_numeric'].unique()
        if len(invalid_lats) > 0:
            print(f"   Latitudes fuera de rango (3.0-4.0): {invalid_lats[:5]}")
        if len(invalid_lons) > 0:
            print(f"   Longitudes fuera de rango (-77.0 a -76.0): {invalid_lons[:5]}")
    
    if valid_coords.sum() == 0:
        print("⚠ No valid coordinates found")
        gdf.drop(columns=['lat_numeric', 'lon_numeric'], inplace=True, errors='ignore')
        return df
    
    # Crear geometría solo para coordenadas válidas en formato GeoJSON estándar: Point(lon, lat)
    gdf.loc[valid_coords, 'geometry'] = gdf.loc[valid_coords].apply(
        lambda row: Point(row['lon_numeric'], row['lat_numeric']), axis=1
    )
    
    # Convertir a GeoDataFrame
    gdf = gpd.GeoDataFrame(gdf, geometry='geometry', crs='EPSG:4326')
    gdf.drop(columns=['lat_numeric', 'lon_numeric'], inplace=True, errors='ignore')
    
    print(f"[OK] GeoDataFrame created: {valid_coords.sum()} valid geometries ({valid_coords.sum()/len(gdf)*100:.1f}%)")
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
    
    print(f"[OK] Coordinates corrected: {valid_coords.sum()} valid")
    return result_gdf


def create_final_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Create final geometry and PRESERVE lat/lon columns with validation.
    
    IMPORTANTE: 
    - Las columnas lat/lon se mantienen para uso posterior
    - Valida que la geometría sea consistente con lat/lon
    - Reconstruye geometría si es necesaria
    """
    if 'lat' not in gdf.columns or 'lon' not in gdf.columns:
        print("[WARNING] Warning: lat/lon columns not found")
        return gdf
    
    result_gdf = gdf.copy()
    
    # Validar coordenadas
    valid_lat = result_gdf['lat'].notna() & (result_gdf['lat'].apply(lambda x: isinstance(x, (int, float)) and 3.0 <= x <= 4.0))
    valid_lon = result_gdf['lon'].notna() & (result_gdf['lon'].apply(lambda x: isinstance(x, (int, float)) and -77.0 <= x <= -76.0))
    valid_coords = valid_lat & valid_lon
    
    if valid_coords.sum() == 0:
        print("[WARNING] No valid coordinates found in lat/lon columns")
        return result_gdf
    
    # Crear/actualizar geometría para coordenadas válidas en formato GeoJSON: Point(lon, lat)
    result_gdf.loc[valid_coords, 'geometry'] = result_gdf.loc[valid_coords].apply(
        lambda row: Point(row['lon'], row['lat']), axis=1
    )
    
    # Asegurar que es GeoDataFrame
    result_gdf = gpd.GeoDataFrame(result_gdf, geometry='geometry', crs='EPSG:4326')
    
    # Validar consistencia entre geometry y lat/lon
    inconsistent = 0
    for idx in result_gdf[valid_coords].index:
        geom = result_gdf.at[idx, 'geometry']
        lat = result_gdf.at[idx, 'lat']
        lon = result_gdf.at[idx, 'lon']
        if geom is not None and pd.notna(geom):
            # Verificar que geometry coincide con lat/lon (con tolerancia de 0.000001)
            if abs(geom.x - lon) > 0.000001 or abs(geom.y - lat) > 0.000001:
                inconsistent += 1
                # Corregir geometría
                result_gdf.at[idx, 'geometry'] = Point(lon, lat)
    
    if inconsistent > 0:
        print(f"[WARNING]  {inconsistent} geometrías reconstruidas por inconsistencia con lat/lon")
    
    # [OK] CRÍTICO: NO eliminar lat/lon - se necesitan para reestructuración posterior
    print(f"[OK] Final geometry validated: {valid_coords.sum()} valid geometries ({valid_coords.sum()/len(result_gdf)*100:.1f}%)")
    print(f"  - lat/lon columns preserved for export")
    
    return result_gdf


def consolidate_coordinates_by_upid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consolida y valida coordenadas (lat/lon) a nivel de unidad de proyecto.
    Después del clustering, múltiples intervenciones comparten el mismo UPID.
    Esta función:
    1. Valida y corrige formatos de coordenadas
    2. Detecta y corrige coordenadas invertidas
    3. Consolida coordenadas por UPID
    DEBE ejecutarse ANTES de create_final_geometry().
    
    Args:
        df: DataFrame con lat/lon y UPIDs
        
    Returns:
        DataFrame con lat/lon validadas y consolidadas por UPID
    """
    if 'upid' not in df.columns:
        print("[WARNING] No hay columna 'upid', saltando consolidación de coordenadas")
        return df
    
    if 'lat' not in df.columns or 'lon' not in df.columns:
        print("[WARNING] No hay columnas 'lat' y 'lon', saltando consolidación de coordenadas")
        return df
    
    result_df = df.copy()
    
    print(f"\n🔗 Consolidando coordenadas (lat/lon) por UPID...")
    
    # PASO 1: Validar y corregir coordenadas usando CoordinateValidator
    from utils.coordinate_validator import CoordinateValidator
    
    validator = CoordinateValidator(verbose=True)
    
    # Agrupar por UPID y validar coordenadas
    upid_coord_map = {}
    
    for upid in result_df['upid'].unique():
        if pd.isna(upid):
            continue
        
        # Obtener todas las filas con este UPID
        upid_mask = result_df['upid'] == upid
        upid_group = result_df[upid_mask]
        
        # Tomar la primera coordenada del grupo (puede ser inválida aún)
        first_row = upid_group.iloc[0]
        lat_original = first_row.get('lat')
        lon_original = first_row.get('lon')
        
        # Validar y corregir
        lat_corrected, lon_corrected, metadata = validator.validate_and_correct_coordinate(
            lat_original, lon_original, str(upid)
        )
        
        upid_coord_map[upid] = {
            'lat': lat_corrected,
            'lon': lon_corrected,
            'is_valid': metadata['is_valid'],
            'corrections': ','.join(metadata['corrections_applied']) if metadata['corrections_applied'] else None,
            'warnings': ','.join(metadata['warnings']) if metadata['warnings'] else None
        }
    
    # PASO 2: Aplicar las coordenadas corregidas a todas las filas del mismo UPID
    for upid, coords in upid_coord_map.items():
        upid_mask = result_df['upid'] == upid
        result_df.loc[upid_mask, 'lat'] = coords['lat']
        result_df.loc[upid_mask, 'lon'] = coords['lon']
    
    # PASO 3: Imprimir estadísticas
    unique_upids = result_df['upid'].nunique()
    upids_with_coords = len([c for c in upid_coord_map.values() if c['lat'] is not None and c['lon'] is not None])
    upids_with_valid_coords = len([c for c in upid_coord_map.values() if c['is_valid']])
    
    stats = validator.get_statistics()
    
    print(f"\n   [OK] Coordenadas consolidadas:")
    print(f"      • Unidades totales: {unique_upids}")
    print(f"      • Unidades con coordenadas: {upids_with_coords}")
    print(f"      • Unidades con coordenadas válidas: {upids_with_valid_coords}")
    print(f"      • Cobertura: {upids_with_coords/unique_upids*100:.1f}%")
    
    if stats['inverted_coords_fixed'] > 0:
        print(f"\n   [CONFIG] Correcciones aplicadas:")
        print(f"      • Coordenadas invertidas: {stats['inverted_coords_fixed']}")
        print(f"      • Separadores decimales: {stats['decimal_separator_fixed']}")
    
    if stats['out_of_range_cali'] > 0:
        print(f"\n   [WARNING]  Advertencias:")
        print(f"      • Unidades fuera de Cali: {stats['out_of_range_cali']}")
    
    return result_df


def consolidate_geometry_by_upid(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    [LEGACY] Consolida geometrías a nivel de unidad de proyecto.
    NOTA: Esta función es redundante si consolidate_coordinates_by_upid() se ejecutó correctamente.
    Se mantiene por compatibilidad pero debería ejecutarse consolidate_coordinates_by_upid() antes de create_final_geometry().
    
    Args:
        gdf: GeoDataFrame con geometrías individuales y UPIDs
        
    Returns:
        GeoDataFrame con geometrías consolidadas por UPID
    """
    if 'upid' not in gdf.columns:
        print("[WARNING] No hay columna 'upid', saltando consolidación de geometría")
        return gdf
    
    result_gdf = gdf.copy()
    
    print(f"\n🔗 Consolidando geometrías por UPID...")
    
    # Agrupar por UPID y tomar la primera geometría válida de cada grupo
    upid_geometry_map = {}
    
    for upid in result_gdf['upid'].unique():
        if pd.isna(upid):
            continue
        
        # Obtener todas las filas con este UPID
        upid_mask = result_gdf['upid'] == upid
        upid_group = result_gdf[upid_mask]
        
        # Tomar la primera geometría válida del grupo
        valid_geoms = upid_group[upid_group['geometry'].notna()]
        
        if len(valid_geoms) > 0:
            # Usar la primera geometría válida
            upid_geometry_map[upid] = valid_geoms.iloc[0]['geometry']
        else:
            upid_geometry_map[upid] = None
    
    # Aplicar la geometría consolidada a todas las filas del mismo UPID
    for upid, geometry in upid_geometry_map.items():
        upid_mask = result_gdf['upid'] == upid
        result_gdf.loc[upid_mask, 'geometry'] = geometry
    
    # Contar cuántas unidades tienen geometría
    unique_upids = result_gdf['upid'].nunique()
    upids_with_geom = len([g for g in upid_geometry_map.values() if g is not None])
    
    print(f"   [OK] Geometrías consolidadas:")
    print(f"      • Unidades totales: {unique_upids}")
    print(f"      • Unidades con geometría: {upids_with_geom}")
    print(f"      • Cobertura: {upids_with_geom/unique_upids*100:.1f}%")
    
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
        print(f"⚠ No valid geometries for spatial intersection: {output_column}")
        gdf[output_column] = None
        return gdf
    
    # Las geometrías ya están en formato correcto Point(lon, lat)
    # NO intercambiar coordenadas - usar geometrías tal como están
    
    column_name = 'barrio_vereda' if 'barrio_vereda' in basemap_gdf.columns else 'comuna_corregimiento'
    
    # Realizar spatial join con las geometrías válidas
    gdf_joined = gpd.sjoin(gdf_temp[valid_geom], basemap_gdf[['geometry', column_name]], how='left', predicate='within')
    
    # Asignar resultados solo a las filas con geometría válida
    if f'{column_name}_right' in gdf_joined.columns:
        gdf.loc[valid_geom, output_column] = gdf_joined[f'{column_name}_right'].values
    elif column_name in gdf_joined.columns:
        gdf.loc[valid_geom, output_column] = gdf_joined[column_name].values
    else:
        gdf[output_column] = None
    
    # Rellenar con None las filas sin geometría
    gdf.loc[~valid_geom, output_column] = None
    
    if 'index_right' in gdf.columns:
        gdf.drop(columns=['index_right'], inplace=True)
    
    assigned_count = gdf[output_column].notna().sum()
    print(f"[OK] Spatial intersection completed: {output_column} ({assigned_count} assigned)")
    
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
    """Normalize comuna values to match basemap format exactly: 'COMUNA 01', 'COMUNA 02', etc."""
    if pd.isna(value) or value is None or value == "":
        return None
    
    text = str(value).strip().upper()
    
    # Manejar variaciones de "Comuna X" o "COMUNA X"
    if "COMUNA" in text:
        # Extraer el número de la comuna
        import re
        match = re.search(r'\d+', text)
        if match:
            num = int(match.group())
            # SIEMPRE usar formato con dos dígitos: COMUNA 01, COMUNA 02, etc.
            return f"COMUNA {num:02d}"
    
    # Si no es una comuna, devolver el valor original sin modificar
    return value


def find_best_match(value, standard_values, threshold=0.6):
    """Find best matching value from standard values using exact and fuzzy matching."""
    if pd.isna(value) or value is None or value == "":
        return None
    
    # Primero intentar coincidencia exacta (ignora mayús/minús y espacios extra)
    normalized_value = normalize_text(value)
    normalized_standards = {normalize_text(std): std for std in standard_values if pd.notna(std)}
    
    # Coincidencia exacta normalizada
    if normalized_value in normalized_standards:
        return normalized_standards[normalized_value]
    
    # Si no hay coincidencia exacta, usar fuzzy matching con threshold más alto
    matches = get_close_matches(normalized_value, normalized_standards.keys(), n=1, cutoff=max(threshold, 0.85))
    
    if matches:
        return normalized_standards[matches[0]]
    
    # Si no se encuentra coincidencia, devolver None para que el valor original se preserve
    return None


def normalize_administrative_values(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalize all comuna and barrio columns to match standard basemap values exactly."""
    current_dir = Path(__file__).parent.parent
    
    # Load standard values from basemaps
    barrios_path = current_dir / 'basemaps' / 'barrios_veredas.geojson'
    comunas_path = current_dir / 'basemaps' / 'comunas_corregimientos.geojson'
    
    standard_barrios = []
    standard_comunas = []
    
    if barrios_path.exists():
        barrios_gdf = gpd.read_file(barrios_path)
        standard_barrios = barrios_gdf['barrio_vereda'].dropna().unique().tolist()
        print(f"  Loaded {len(standard_barrios)} standard barrios/veredas from basemap")
    
    if comunas_path.exists():
        comunas_gdf = gpd.read_file(comunas_path)
        standard_comunas = comunas_gdf['comuna_corregimiento'].dropna().unique().tolist()
        print(f"  Loaded {len(standard_comunas)} standard comunas/corregimientos from basemap")
    
    result_gdf = gdf.copy()
    
    # All comuna columns to normalize (including spatial intersection results)
    comuna_columns = ['comuna_corregimiento', 'comuna_corregimiento_2']
    # All barrio columns to normalize (including spatial intersection results)
    barrio_columns = ['barrio_vereda', 'barrio_vereda_2']
    
    # Normalize all comuna columns
    for col in comuna_columns:
        if col in result_gdf.columns and len(standard_comunas) > 0:
            normalized_count = 0
            for idx in result_gdf.index:
                original = result_gdf.at[idx, col]
                if pd.notna(original):
                    # First apply comuna normalization rules
                    normalized = normalize_comuna_value(original)
                    # Then find best match from standard values
                    best_match = find_best_match(normalized, standard_comunas, threshold=0.7)
                    if best_match:
                        if best_match != original:
                            normalized_count += 1
                        result_gdf.at[idx, col] = best_match
            if normalized_count > 0:
                print(f"  Normalized {normalized_count} values in '{col}' to standard basemap values")
    
    # Normalize all barrio columns
    for col in barrio_columns:
        if col in result_gdf.columns and len(standard_barrios) > 0:
            normalized_count = 0
            no_match_count = 0
            for idx in result_gdf.index:
                original = result_gdf.at[idx, col]
                if pd.notna(original):
                    # Limpiar saltos de línea y espacios extra
                    cleaned = str(original).replace('\n', ' ').strip()
                    cleaned = ' '.join(cleaned.split())
                    
                    # Find best match from standard values
                    best_match = find_best_match(cleaned, standard_barrios, threshold=0.85)
                    if best_match:
                        if best_match != original:
                            normalized_count += 1
                        result_gdf.at[idx, col] = best_match
                    else:
                        # Si no hay match, limpiar el valor pero mantenerlo
                        result_gdf.at[idx, col] = cleaned
                        no_match_count += 1
            if normalized_count > 0:
                print(f"  Normalized {normalized_count} values in '{col}' to standard basemap values")
            if no_match_count > 0:
                print(f"  [WARNING]  {no_match_count} values in '{col}' not found in basemap (kept original)")
    
    print("[OK] Administrative values normalized to basemap standards")
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
    
    print(f"[OK] Validation column created: {aceptable.sum()} ACEPTABLE, {fuera_rango.sum()} FUERA DE RANGO")
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
    
    # Reject values that are clearly not dates (contain letters, are too long, etc.)
    # Dates should be numeric or contain only date separators
    if len(date_str) > 50:  # Dates shouldn't be this long
        return None
    
    # Check if it contains too many alphabetic characters (like barrio names)
    alpha_count = sum(c.isalpha() for c in date_str)
    if alpha_count > 3:  # Allow for month abbreviations like "Jan", "Feb"
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
    
    # Only process if the column exists and contains actual date-like values
    if 'fecha_inicio' in result_gdf.columns:
        # Check if the column actually contains date-like values
        sample_values = result_gdf['fecha_inicio'].dropna().head(10)
        if len(sample_values) > 0:
            # Apply parse_date which now validates the content
            result_gdf['fecha_inicio_std'] = result_gdf['fecha_inicio'].apply(parse_date)
            valid_count = result_gdf['fecha_inicio_std'].notna().sum()
            print(f"[OK] fecha_inicio standardized: {valid_count} valid")
            
            # Warn if conversion rate is very low (possible wrong column)
            total_non_null = result_gdf['fecha_inicio'].notna().sum()
            if total_non_null > 0 and valid_count / total_non_null < 0.1:
                print(f"  [WARNING] Warning: Low conversion rate ({valid_count}/{total_non_null}), column may not contain dates")
    
    if 'fecha_fin' in result_gdf.columns:
        # Check if the column actually contains date-like values
        sample_values = result_gdf['fecha_fin'].dropna().head(10)
        if len(sample_values) > 0:
            result_gdf['fecha_fin_std'] = result_gdf['fecha_fin'].apply(parse_date)
            valid_count = result_gdf['fecha_fin_std'].notna().sum()
            print(f"[OK] fecha_fin standardized: {valid_count} valid")
            
            # Warn if conversion rate is very low
            total_non_null = result_gdf['fecha_fin'].notna().sum()
            if total_non_null > 0 and valid_count / total_non_null < 0.1:
                print(f"  [WARNING] Warning: Low conversion rate ({valid_count}/{total_non_null}), column may not contain dates")
    
    return result_gdf


def infer_missing_categorical_values(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Infiere valores faltantes de clase_up y tipo_equipamiento basándose en
    nombre_centro_gestor, nombre_up y nombre_up_detalle cuando están ausentes (NaN).
    
    Esta función es crítica para que la detección de frentes activos funcione
    correctamente con datos incompletos.
    
    Reglas de inferencia:
    1. Si nombre_up contiene "IPS" y tipo_equipamiento es NaN:
       → tipo_equipamiento = "IPS"
       → clase_up = "Obras equipamientos" (si también es NaN)
    
    2. Si nombre_centro_gestor = "Secretaría de Salud Pública":
       → clase_up = "Obras equipamientos" (si es NaN)
       → tipo_equipamiento = "IPS" (si es NaN y no se infirió en regla 1)
    
    Args:
        gdf: GeoDataFrame con los datos
        
    Returns:
        GeoDataFrame con valores inferidos
    """
    result_gdf = gdf.copy()
    
    # Asegurar que las columnas existen
    if 'clase_up' not in result_gdf.columns:
        result_gdf['clase_up'] = None
    if 'tipo_equipamiento' not in result_gdf.columns:
        result_gdf['tipo_equipamiento'] = None
    
    # Contador de inferencias
    inferencias = {
        'tipo_equipamiento_ips': 0,
        'clase_up_ips': 0,
        'clase_up_salud': 0,
        'tipo_equipamiento_salud': 0
    }
    
    # Regla 1: Detectar IPS por nombre_up
    if 'nombre_up' in result_gdf.columns:
        # Buscar registros que contengan "IPS" en nombre_up (case-insensitive)
        mask_ips_nombre = (
            result_gdf['nombre_up'].fillna('').str.upper().str.contains('IPS', regex=False) &
            (result_gdf['tipo_equipamiento'].isna() | (result_gdf['tipo_equipamiento'] == ''))
        )
        
        if mask_ips_nombre.sum() > 0:
            result_gdf.loc[mask_ips_nombre, 'tipo_equipamiento'] = 'IPS'
            inferencias['tipo_equipamiento_ips'] = mask_ips_nombre.sum()
            
            # También asignar clase_up si está vacío
            mask_clase_ips = mask_ips_nombre & (
                result_gdf['clase_up'].isna() | 
                (result_gdf['clase_up'] == '')
            )
            if mask_clase_ips.sum() > 0:
                result_gdf.loc[mask_clase_ips, 'clase_up'] = 'Obras equipamientos'
                inferencias['clase_up_ips'] = mask_clase_ips.sum()
    
    # Regla 2: Secretaría de Salud Pública (para casos que no se cubrieron con regla 1)
    if 'nombre_centro_gestor' in result_gdf.columns:
        mask_salud_publica = (
            (result_gdf['nombre_centro_gestor'] == 'Secretaría de Salud Pública') &
            (result_gdf['clase_up'].isna() | (result_gdf['clase_up'] == ''))
        )
        
        if mask_salud_publica.sum() > 0:
            result_gdf.loc[mask_salud_publica, 'clase_up'] = 'Obras equipamientos'
            inferencias['clase_up_salud'] = mask_salud_publica.sum()
            
            # También inferir tipo_equipamiento si está vacío (y no se infirió como IPS)
            mask_tipo_vacio = mask_salud_publica & (
                result_gdf['tipo_equipamiento'].isna() | 
                (result_gdf['tipo_equipamiento'] == '')
            )
            if mask_tipo_vacio.sum() > 0:
                result_gdf.loc[mask_tipo_vacio, 'tipo_equipamiento'] = 'IPS'
                inferencias['tipo_equipamiento_salud'] = mask_tipo_vacio.sum()
    
    # Reportar inferencias realizadas
    total_inferencias = sum(inferencias.values())
    if total_inferencias > 0:
        print(f"[OK] Valores categóricos inferidos:")
        if inferencias['tipo_equipamiento_ips'] > 0:
            print(f"   - tipo_equipamiento 'IPS' detectado por nombre: {inferencias['tipo_equipamiento_ips']} registros")
        if inferencias['clase_up_ips'] > 0:
            print(f"   - clase_up 'Obras equipamientos' para registros IPS: {inferencias['clase_up_ips']} registros")
        if inferencias['clase_up_salud'] > 0:
            print(f"   - clase_up 'Obras equipamientos' para Salud Pública: {inferencias['clase_up_salud']} registros")
        if inferencias['tipo_equipamiento_salud'] > 0:
            print(f"   - tipo_equipamiento 'IPS' para Salud Pública: {inferencias['tipo_equipamiento_salud']} registros")
    else:
        print("[OK] No se requirieron inferencias de valores categóricos")
    
    return result_gdf


def add_frente_activo(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Agrega la columna 'frente_activo' basada en condiciones de estado, clase_up, 
    tipo_equipamiento y tipo_intervencion.
    
    Lógica:
    - 'Frente activo': registros en 'En ejecución' + clase_up en ('Obras equipamientos', 'Obra vial', 'Espacio Público')
      y excluyendo tipo_equipamiento ('Vivienda mejoramiento', 'Vivienda nueva', 'Adquisición de predios', 'Señalización vial')
      y excluyendo tipo_intervencion ('Mantenimiento', 'Estudios y diseños', 'Transferencia directa')
    - 'Inactivo': mismas condiciones pero con estado 'Suspendido'
    - 'No aplica': todos los demás casos
    """
    result_gdf = gdf.copy()
    
    # Inicializar columna con 'No aplica' por defecto
    result_gdf['frente_activo'] = 'No aplica'
    
    # Definir listas de valores a excluir
    tipos_equipamiento_excluidos = [
        'Vivienda mejoramiento', 
        'Vivienda nueva', 
        'Adquisición de predios', 
        'Señalización vial'
    ]
    
    tipos_intervencion_excluidos = [
        'Mantenimiento', 
        'Estudios y diseños', 
        'Transferencia directa'
    ]
    
    # Definir clases válidas para frente activo
    clases_validas = ['Obras equipamientos', 'Obra vial', 'Espacio Público']
    
    # Condiciones base para frente activo (sin considerar el estado todavía)
    # Filtro 1: clase_up debe estar en las clases válidas
    condicion_clase = result_gdf['clase_up'].isin(clases_validas) if 'clase_up' in result_gdf.columns else pd.Series([False] * len(result_gdf))
    
    # Filtro 2: tipo_equipamiento NO debe estar en la lista de excluidos
    condicion_tipo_equipamiento = ~result_gdf['tipo_equipamiento'].isin(tipos_equipamiento_excluidos) if 'tipo_equipamiento' in result_gdf.columns else pd.Series([True] * len(result_gdf))
    
    # Filtro 3: tipo_intervencion NO debe estar en la lista de excluidos
    condicion_tipo_intervencion = ~result_gdf['tipo_intervencion'].isin(tipos_intervencion_excluidos) if 'tipo_intervencion' in result_gdf.columns else pd.Series([True] * len(result_gdf))
    
    # Combinar todas las condiciones base
    condiciones_base = condicion_clase & condicion_tipo_equipamiento & condicion_tipo_intervencion
    
    # Aplicar lógica según estado
    if 'estado' in result_gdf.columns:
        # Frente activo: condiciones base + estado 'En ejecución'
        frente_activo_mask = condiciones_base & (result_gdf['estado'] == 'En ejecución')
        result_gdf.loc[frente_activo_mask, 'frente_activo'] = 'Frente activo'
        
        # Inactivo: condiciones base + estado 'Suspendido'
        inactivo_mask = condiciones_base & (result_gdf['estado'] == 'Suspendido')
        result_gdf.loc[inactivo_mask, 'frente_activo'] = 'Inactivo'
    
    # Reportar estadísticas
    frente_activo_count = (result_gdf['frente_activo'] == 'Frente activo').sum()
    inactivo_count = (result_gdf['frente_activo'] == 'Inactivo').sum()
    no_aplica_count = (result_gdf['frente_activo'] == 'No aplica').sum()
    
    print(f"[OK] Columna 'frente_activo' agregada:")
    print(f"   - Frente activo: {frente_activo_count} registros")
    print(f"   - Inactivo: {inactivo_count} registros")
    print(f"   - No aplica: {no_aplica_count} registros")
    
    return result_gdf


def restructure_by_upid(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Reestructura el GeoDataFrame para que cada feature represente una unidad de proyecto.
    Las intervenciones se agrupan en un array dentro de properties.
    Geometry se mantiene solo a nivel de unidad de proyecto.
    
    Args:
        gdf: GeoDataFrame con una fila por intervención
        
    Returns:
        GeoDataFrame con una fila por unidad de proyecto
    """
    print(f"\n[SYNC] Reestructurando GeoJSON por unidades de proyecto...")
    
    # Campos a nivel de unidad de proyecto (no varían por intervención)
    unidad_fields = [
        'upid', 'n_intervenciones', 'nombre_up', 'nombre_up_detalle',
        'nombre_centro_gestor', 'direccion', 'tipo_equipamiento', 'clase_up',
        'comuna_corregimiento', 'comuna_corregimiento_2',
        'barrio_vereda', 'barrio_vereda_2',
        'identificador', 'bpin', 'centros_gravedad',
        'lat', 'lon',  # [OK] CRÍTICO: Preservar coordenadas lat/lon
        'geometry'
    ]
    
    # Campos a nivel de intervención (varían por cada intervención)
    intervencion_fields = [
        'intervencion_id', 'referencia_proceso', 'referencia_contrato', 'url_proceso',
        'bpin', 'estado', 'tipo_intervencion', 'fuente_financiacion',
        'presupuesto_base', 'ano', 'avance_obra', 'avance_financiero',
        'fecha_inicio', 'fecha_fin', 'fecha_inicio_std', 'fecha_fin_std',
        'frente_activo', 'fuera_rango', 'processed_timestamp'
    ]
    
    unidades_list = []
    
    for upid in gdf['upid'].unique():
        if pd.isna(upid):
            continue
        
        # Filtrar todas las intervenciones de esta unidad
        upid_mask = gdf['upid'] == upid
        upid_group = gdf[upid_mask]
        
        # Crear registro de unidad
        unidad = {}
        
        # Tomar campos de unidad del primer registro (son iguales para todos)
        first_row = upid_group.iloc[0]
        for field in unidad_fields:
            if field in gdf.columns:
                valor = first_row[field]
                
                # Preservar lat/lon si son numéricos (sin validar rango aquí)
                # Es mejor preservar coordenadas que perderlas completamente
                # Si no están disponibles como columnas, extraerlas de geometry
                if field == 'lat':
                    if pd.notna(valor) and isinstance(valor, (int, float)):
                        unidad[field] = float(valor)
                    elif 'geometry' in first_row:
                        # Intentar extraer de geometry
                        geom = first_row.get('geometry')
                        if pd.notna(geom) and geom is not None and hasattr(geom, 'y'):
                            unidad[field] = float(geom.y)
                        else:
                            unidad[field] = None
                    else:
                        unidad[field] = None
                elif field == 'lon':
                    if pd.notna(valor) and isinstance(valor, (int, float)):
                        unidad[field] = float(valor)
                    elif 'geometry' in first_row:
                        # Intentar extraer de geometry
                        geom = first_row.get('geometry')
                        if pd.notna(geom) and geom is not None and hasattr(geom, 'x'):
                            unidad[field] = float(geom.x)
                        else:
                            unidad[field] = None
                    else:
                        unidad[field] = None
                # Preservar geometry siempre que sea válida (sin validar rango)
                elif field == 'geometry':
                    if pd.notna(valor) and valor is not None and hasattr(valor, 'x') and hasattr(valor, 'y'):
                        # Preservar geometry siempre que tenga coordenadas
                        # La validación de rango se hace en el campo 'fuera_rango'
                        unidad[field] = valor
                    else:
                        # Si no hay geometry pero hay lat/lon, intentar reconstruir aquí
                        lat = first_row.get('lat') if 'lat' in first_row else None
                        lon = first_row.get('lon') if 'lon' in first_row else None
                        if pd.notna(lat) and pd.notna(lon) and isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                            from shapely.geometry import Point
                            unidad[field] = Point(float(lon), float(lat))
                        else:
                            unidad[field] = None
                else:
                    unidad[field] = valor
        
        # Crear array de intervenciones
        intervenciones = []
        for idx, row in upid_group.iterrows():
            intervencion = {}
            for field in intervencion_fields:
                if field in gdf.columns:
                    valor = row[field]
                    
                    # Manejo especial para presupuesto_base (siempre entero, sin decimales)
                    if field == 'presupuesto_base':
                        if pd.isna(valor) or valor is None:
                            intervencion[field] = 0
                        else:
                            try:
                                intervencion[field] = int(float(valor))
                            except (ValueError, TypeError):
                                intervencion[field] = 0
                    
                    # Manejo especial para avance_obra (máximo 2 decimales)
                    elif field == 'avance_obra':
                        if pd.isna(valor) or valor is None:
                            intervencion[field] = 0.0
                        else:
                            try:
                                intervencion[field] = round(float(valor), 2)
                            except (ValueError, TypeError):
                                intervencion[field] = 0.0
                    
                    # Convertir tipos numpy a tipos nativos Python (otros campos)
                    elif isinstance(valor, (list, np.ndarray)):
                        # Es un array o lista
                        if isinstance(valor, np.ndarray):
                            intervencion[field] = valor.tolist()
                        else:
                            intervencion[field] = valor
                    elif isinstance(valor, (np.integer, np.int64, np.int32)):
                        intervencion[field] = int(valor)
                    elif isinstance(valor, (np.floating, np.float64, np.float32)):
                        if pd.isna(valor):
                            intervencion[field] = None
                        else:
                            intervencion[field] = float(valor)
                    elif pd.notna(valor):
                        intervencion[field] = valor
                    else:
                        intervencion[field] = None
            
            intervenciones.append(intervencion)
        
        # Agregar array de intervenciones a la unidad
        unidad['intervenciones'] = intervenciones
        
        # Agregar métricas agregadas a nivel de unidad (para consultas frontend)
        # Estas son SUM de todas las intervenciones de la unidad
        presupuestos = [i.get('presupuesto_base', 0) for i in intervenciones if i.get('presupuesto_base', 0) > 0]
        unidad['presupuesto_base'] = sum(presupuestos) if presupuestos else 0
        
        # Avance promedio ponderado por presupuesto (o promedio simple si no hay presupuestos)
        avances_con_ppto = [(i.get('avance_obra', 0), i.get('presupuesto_base', 0)) 
                            for i in intervenciones 
                            if i.get('presupuesto_base', 0) > 0]
        if avances_con_ppto:
            total_ppto = sum(p for _, p in avances_con_ppto)
            if total_ppto > 0:
                unidad['avance_obra'] = sum(a * p for a, p in avances_con_ppto) / total_ppto
            else:
                avances = [i.get('avance_obra', 0) for i in intervenciones]
                unidad['avance_obra'] = sum(avances) / len(avances) if avances else 0.0
        else:
            avances = [i.get('avance_obra', 0) for i in intervenciones]
            unidad['avance_obra'] = sum(avances) / len(avances) if avances else 0.0
        
        # Redondear avance a 2 decimales
        unidad['avance_obra'] = round(unidad['avance_obra'], 2)
        
        unidades_list.append(unidad)
    
    # Crear nuevo GeoDataFrame con estructura de unidades
    gdf_unidades = gpd.GeoDataFrame(unidades_list, crs=gdf.crs)
    
    # Validar consistencia entre geometry y lat/lon
    geometries_validas = gdf_unidades['geometry'].notna().sum()
    coords_validas = (gdf_unidades['lat'].notna() & gdf_unidades['lon'].notna()).sum()
    
    # Reconstruir geometría si falta pero hay lat/lon válidas
    # También actualizar lat/lon si faltan pero hay geometry válida
    geometrias_reconstruidas = 0
    coords_reconstruidas = 0
    
    for idx in gdf_unidades.index:
        geom = gdf_unidades.at[idx, 'geometry']
        lat = gdf_unidades.at[idx, 'lat']
        lon = gdf_unidades.at[idx, 'lon']
        
        # Caso 1: No hay geometría pero hay coordenadas -> crear geometría
        if (geom is None or pd.isna(geom)) and pd.notna(lat) and pd.notna(lon):
            if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                gdf_unidades.at[idx, 'geometry'] = Point(float(lon), float(lat))
                geometrias_reconstruidas += 1
        
        # Caso 2: Hay geometría pero no hay lat/lon -> extraer de geometría
        elif pd.notna(geom) and hasattr(geom, 'x') and hasattr(geom, 'y'):
            if pd.isna(lat) or not isinstance(lat, (int, float)):
                gdf_unidades.at[idx, 'lat'] = float(geom.y)
                coords_reconstruidas += 1
            if pd.isna(lon) or not isinstance(lon, (int, float)):
                gdf_unidades.at[idx, 'lon'] = float(geom.x)
                if pd.isna(lat) or not isinstance(lat, (int, float)):
                    # Solo contar una vez por registro
                    pass
                else:
                    coords_reconstruidas += 1
    
    if geometrias_reconstruidas > 0:
        print(f"   [CONFIG] {geometrias_reconstruidas} geometrías reconstruidas desde lat/lon")
    if coords_reconstruidas > 0:
        print(f"   [CONFIG] {coords_reconstruidas} coordenadas lat/lon extraídas desde geometry")
    
    # Actualizar estadísticas después de reconstrucción
    geometries_validas_final = gdf_unidades['geometry'].notna().sum()
    coords_validas_final = (gdf_unidades['lat'].notna() & gdf_unidades['lon'].notna()).sum()
    
    print(f"   [OK] Estructura reestructurada:")
    print(f"      • Unidades de proyecto: {len(gdf_unidades)}")
    print(f"      • Unidades con geometry válida: {geometries_validas_final}")
    print(f"      • Unidades con lat/lon válidas: {coords_validas_final}")
    print(f"      • Total intervenciones: {sum(len(u['intervenciones']) for u in unidades_list)}")
    
    return gdf_unidades


def export_to_geojson(gdf: gpd.GeoDataFrame, output_dir: Path) -> Path:
    """Export GeoDataFrame to GeoJSON ensuring geometry is valid for map visualization.
    
    IMPORTANTE:
    - Las columnas lat/lon NO se incluyen en properties (solo en geometry)
    - La geometría se valida y reconstruye si es necesario
    - Formato GeoJSON estándar: [lon, lat] para visualización en mapas
    """
    output_dir.mkdir(exist_ok=True, parents=True)
    output_file = output_dir / 'unidades_proyecto_transformed.geojson'
    
    # Reestructurar: una feature por unidad de proyecto (no por intervención)
    gdf_restructured = restructure_by_upid(gdf)
    
    gdf_export = gdf_restructured.copy()
    
    # [OK] CRÍTICO: Asegurar que todas las geometrías sean válidas antes de exportar
    # Si hay lat/lon pero no geometry, reconstruir (sin validar rango estricto)
    geometries_reconstruidas = 0
    for idx in gdf_export.index:
        geom = gdf_export.at[idx, 'geometry']
        lat = gdf_export.at[idx, 'lat'] if 'lat' in gdf_export.columns else None
        lon = gdf_export.at[idx, 'lon'] if 'lon' in gdf_export.columns else None
        
        # Si no hay geometry válida pero hay coordenadas, reconstruir
        if (geom is None or pd.isna(geom) or not hasattr(geom, 'x')) and pd.notna(lat) and pd.notna(lon):
            if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                gdf_export.at[idx, 'geometry'] = Point(float(lon), float(lat))
                geometries_reconstruidas += 1
    
    if geometries_reconstruidas > 0:
        print(f"   [CONFIG] {geometries_reconstruidas} geometrías reconstruidas desde lat/lon")
    
    gdf_export = gpd.GeoDataFrame(gdf_export, geometry='geometry', crs='EPSG:4326')
    
    # Convertir la columna 'intervenciones' (lista de dicts) a JSON string para exportación
    # GeoPandas no maneja correctamente listas de diccionarios complejos
    if 'intervenciones' in gdf_export.columns:
        import json
        for idx in gdf_export.index:
            intervenciones = gdf_export.at[idx, 'intervenciones']
            if isinstance(intervenciones, list):
                # Mantener como lista (GeoJSON lo manejará en properties)
                # Solo asegurar que esté serializable
                gdf_export.at[idx, 'intervenciones'] = intervenciones
    
    # Ensure all string columns are properly encoded in UTF-8
    for col in gdf_export.columns:
        if col != 'geometry' and col != 'intervenciones' and gdf_export[col].dtype == 'object':
            # Process each value individually
            for idx in gdf_export.index:
                val = gdf_export.at[idx, col]
                # Check if it's a scalar value first, not an array or list
                if not isinstance(val, (list, np.ndarray)):
                    if pd.notna(val) and isinstance(val, str):
                        try:
                            # Ensure proper UTF-8 encoding
                            gdf_export.at[idx, col] = val.encode('utf-8', errors='replace').decode('utf-8')
                        except:
                            pass  # Keep original value if encoding fails
    
    # [OK] NO convertir/invertir geometry - ya está en formato correcto Point(lon, lat)
    # La geometría se creó correctamente en create_final_geometry() como Point(lon, lat)
    # Mantenerla tal cual para GeoJSON estándar
    
    # Convert datetime to string (only for actual datetime columns)
    for col in ['fecha_inicio_std', 'fecha_fin_std']:
        if col in gdf_export.columns:
            for idx in gdf_export.index:
                val = gdf_export.at[idx, col]
                if pd.notna(val) and hasattr(val, 'isoformat'):
                    gdf_export.at[idx, col] = val.isoformat()
    
    # Construir GeoJSON manualmente para manejar correctamente arrays de intervenciones
    import json
    
    features = []
    for idx, row in gdf_export.iterrows():
        # Crear feature
        feature = {
            "type": "Feature",
            "geometry": None,
            "properties": {}
        }
        
        # Agregar geometry si existe (con validación y fallback a lat/lon)
        geom = row.get('geometry')
        lat = row.get('lat')
        lon = row.get('lon')
        
        # Intentar usar geometry primero
        if pd.notna(geom) and geom is not None and hasattr(geom, 'x') and hasattr(geom, 'y'):
            try:
                # Validar que las coordenadas estén en rangos válidos
                if -77.0 <= geom.x <= -76.0 and 3.0 <= geom.y <= 4.0:
                    # GeoJSON estándar: [lon, lat] = [x, y]
                    feature['geometry'] = {
                        "type": "Point",
                        "coordinates": [geom.x, geom.y]  # lon, lat
                    }
                else:
                    # Geometry fuera de rango, intentar con lat/lon
                    if pd.notna(lat) and pd.notna(lon) and -77.0 <= lon <= -76.0 and 3.0 <= lat <= 4.0:
                        feature['geometry'] = {
                            "type": "Point",
                            "coordinates": [lon, lat]  # lon, lat
                        }
            except Exception as e:
                # Error al procesar geometry, intentar con lat/lon
                if pd.notna(lat) and pd.notna(lon) and -77.0 <= lon <= -76.0 and 3.0 <= lat <= 4.0:
                    feature['geometry'] = {
                        "type": "Point",
                        "coordinates": [lon, lat]  # lon, lat
                    }
        # Si no hay geometry válida, usar lat/lon directamente
        elif pd.notna(lat) and pd.notna(lon):
            try:
                if -77.0 <= lon <= -76.0 and 3.0 <= lat <= 4.0:
                    feature['geometry'] = {
                        "type": "Point",
                        "coordinates": [lon, lat]  # lon, lat
                    }
            except:
                pass  # Sin geometría válida
        
        # Agregar properties (INCLUIR lat/lon como campos redundantes para análisis)
        for col in gdf_export.columns:
            if col not in ['geometry']:  # Solo excluir geometry, incluir lat/lon
                valor = row[col]
                
                # Manejar diferentes tipos de datos
                if col == 'intervenciones':
                    # Las intervenciones ya están como lista de dicts
                    # Convertir fechas en intervenciones a string
                    intervenciones_clean = []
                    if isinstance(valor, list):
                        for interv in valor:
                            if isinstance(interv, dict):
                                interv_clean = {}
                                for k, v in interv.items():
                                    if hasattr(v, 'isoformat'):  # datetime
                                        interv_clean[k] = v.isoformat()
                                    else:
                                        interv_clean[k] = v
                                intervenciones_clean.append(interv_clean)
                            else:
                                intervenciones_clean.append(interv)
                        feature['properties'][col] = intervenciones_clean
                    else:
                        feature['properties'][col] = []
                elif isinstance(valor, (list, np.ndarray)):
                    feature['properties'][col] = valor.tolist() if isinstance(valor, np.ndarray) else valor
                elif isinstance(valor, (np.integer, np.int64, np.int32)):
                    feature['properties'][col] = int(valor)
                elif isinstance(valor, (np.floating, np.float64, np.float32)):
                    feature['properties'][col] = None if pd.isna(valor) else float(valor)
                elif hasattr(valor, 'isoformat'):  # datetime
                    feature['properties'][col] = valor.isoformat()
                elif pd.isna(valor):
                    feature['properties'][col] = None
                else:
                    feature['properties'][col] = valor
        
        features.append(feature)
    
    # Construir estructura GeoJSON
    geojson_data = {
        "type": "FeatureCollection",
        "features": features
    }
    
    # Escribir a archivo con UTF-8
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(geojson_data, f, ensure_ascii=False, indent=2)
    
    # Calcular estadísticas de geometrías exportadas
    features_con_geometry = sum(1 for f in features if f['geometry'] is not None)
    features_sin_geometry = len(features) - features_con_geometry
    
    # Validar que las geometrías sean visualizables (coordenadas en rangos válidos)
    geometrias_visualizables = 0
    for f in features:
        if f['geometry'] is not None and 'coordinates' in f['geometry']:
            coords = f['geometry']['coordinates']
            if len(coords) == 2:
                lon, lat = coords
                if -77.0 <= lon <= -76.0 and 3.0 <= lat <= 4.0:
                    geometrias_visualizables += 1
    
    print(f"[OK] GeoJSON exported: {output_file.name} ({output_file.stat().st_size / 1024:.2f} KB)")
    print(f"  - Total features: {len(features)}")
    print(f"  - Con geometría: {features_con_geometry} ({features_con_geometry/len(features)*100:.1f}%)")
    print(f"  - Geometrías visualizables en mapa: {geometrias_visualizables} ({geometrias_visualizables/len(features)*100:.1f}%)")
    print(f"  - Sin geometría: {features_sin_geometry}")
    print(f"  [OK] Columnas lat/lon INCLUIDAS en properties como campos redundantes")
    
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
    
    print(f"[OK] Metrics saved: {metrics_file.name}")
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
    
    print(f"[OK] Métricas cargadas desde: {metrics_file.name}")
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
    
    print(f"[OK] Reporte JSON guardado: {report_json_file.name}")
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
        "## [DATA] Resumen Ejecutivo",
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
        "#### [OK] Validación Espacial",
        f"- **Registros Aceptables:** {report['resumen_ejecutivo']['indicadores_clave']['validacion_espacial']['registros_aceptables']:,} ({report['resumen_ejecutivo']['indicadores_clave']['validacion_espacial']['porcentaje_aceptable']:.1f}%)",
        f"- **Registros Inválidos:** {report['resumen_ejecutivo']['indicadores_clave']['validacion_espacial']['registros_invalidos']:,}",
        "",
        "---",
        "",
        "## [STATS] Análisis Detallado",
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
        "## [SUCCESS] Recomendaciones",
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
        "## [DATA] Métricas de Calidad",
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
    
    print(f"[OK] Reporte Markdown guardado: {report_md_file.name}")
    print(f"  Tamaño: {report_md_file.stat().st_size / 1024:.2f} KB")
    print()
    
    print("=" * 80)
    print("[OK] GENERACIÓN DE REPORTES COMPLETADA")
    print("=" * 80)
    print()
    print(f"📂 Archivos generados:")
    print(f"   - JSON: {report_json_file}")
    print(f"   - Markdown: {report_md_file}")
    print()
    print(f"[DATA] Resumen:")
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
        lambda df: normalize_categorical_column(df, 'clase_up', threshold=0.75),
        lambda df: normalize_categorical_column(df, 'tipo_equipamiento', threshold=0.75),
        lambda df: normalize_categorical_column(df, 'tipo_intervencion', threshold=0.75),
        lambda df: normalize_categorical_column(df, 'fuente_financiacion', threshold=0.75),
        lambda df: apply_title_case_to_text_fields(df)
    )
    df_transformed = basic_pipeline(df_clean)
    
    # Phase 1.5: Consolidate coordinates by UPID (BEFORE creating geometry)
    df_transformed = consolidate_coordinates_by_upid(df_transformed)
    
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
    
    # Phase 5.4: Infer missing categorical values
    print("\n[Phase 5.4: Inferencia de Valores Categóricos]")
    gdf = infer_missing_categorical_values(gdf)
    
    # Phase 5.5: Add frente_activo column
    print("\n[Phase 5.5: Frente Activo]")
    gdf = add_frente_activo(gdf)
    
    # Phase 6: Export and metrics
    print("\n[Phase 6: Export & Metrics]")
    current_dir = Path(__file__).parent.parent
    output_dir = current_dir / 'app_outputs'
    logs_dir = output_dir / 'logs'
    
    output_file = export_to_geojson(gdf, output_dir)
    metrics, metrics_file = generate_metrics_log(gdf, df, logs_dir)
    
    # Generate comprehensive analysis and recommendations report
    report_files = generate_analysis_report(metrics_file, gdf)
    
    print(f"\n[OK] Processing completed: {len(gdf)} rows, {len(gdf.columns)} columns")
    print(f"[OK] Quality score: {metrics['summary']['data_quality_score']:.1f}%")
    print(f"[OK] Geometry completeness: {metrics['summary']['geometry_completeness']:.1f}%")
    print(f"\n[FILE] Output files:")
    print(f"   - GeoJSON: {output_file}")
    print(f"   - Metrics: {metrics_file}")
    print(f"   - Report (JSON): {report_files['json']}")
    print(f"   - Report (Markdown): {report_files['markdown']}")
    
    return gdf


def transform_and_save_unidades_proyecto(
    data: Optional[pd.DataFrame] = None, 
    use_extraction: bool = True,
    upload_to_s3: bool = True
) -> Optional[gpd.GeoDataFrame]:
    """
    Main function to transform and save unidades de proyecto data.
    Implements complete functional pipeline for transformation and output generation.
    
    Args:
        data: Optional DataFrame with extracted data (if provided, skips extraction)
        use_extraction: If True and data is None, extracts from Google Drive
        upload_to_s3: If True, uploads outputs to S3 after transformation
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
            
            # Upload to S3 if requested
            if upload_to_s3:
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
                    
                    # CRÍTICO: NO eliminar GeoJSON después de subir a S3
                    # El pipeline necesita el archivo para verificación incremental y carga a Firebase
                    upload_results = uploader.upload_all_outputs(
                        output_dir=output_dir,
                        upload_data=True,
                        upload_logs=True,
                        upload_reports=True,
                        delete_data_after_upload=False  # NO eliminar GeoJSON (pipeline lo necesita)
                    )
                    
                    print("\n" + "="*80)
                    print("S3 UPLOAD COMPLETED")
                    print("="*80)
                    
                except Exception as e:
                    print("\n" + "="*80)
                    print("S3 UPLOAD FAILED")
                    print("="*80)
                    print(f"✗ Error uploading to S3: {e}")
                    import traceback
                    traceback.print_exc()
                    print("\n[WARNING] Transformation was successful, but S3 upload failed")
            
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
        print(f"[OK] Processed data: {len(gdf_result):,} records")
        print(f"[OK] Total columns: {len(gdf_result.columns)}")
        
        if isinstance(gdf_result, gpd.GeoDataFrame):
            print(f"[OK] GeoDataFrame type: {type(gdf_result).__name__}")
            print(f"[OK] Geometries: {gdf_result['geometry'].notna().sum():,}")
        
        print(f"[OK] Data transformation completed successfully")
        
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
            
            # Load data to Firebase/Firestore
            try:
                print("\n" + "="*80)
                print("LOADING DATA TO FIREBASE/FIRESTORE")
                print("="*80)
                
                # Import the loading module
                sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'load_app'))
                from data_loading_unidades_proyecto import load_unidades_proyecto_to_firebase
                
                # Execute the loading from S3 (uses default: /current compressed version)
                success = load_unidades_proyecto_to_firebase(
                    use_s3=True,
                    batch_size=100
                )
                
                if success:
                    print("\n[OK] Datos cargados exitosamente a Firebase/Firestore")
                else:
                    print("\n[WARNING] No se pudieron cargar los datos a Firebase/Firestore")
                    
            except Exception as load_error:
                print(f"\n[WARNING] Error al cargar datos a Firebase: {load_error}")
                print("Datos guardados en S3, pero no se cargaron a Firestore")
                import traceback
                traceback.print_exc()
            
        except Exception as e:
            print("\n" + "="*80)
            print("S3 UPLOAD FAILED")
            print("="*80)
            print(f"✗ Error uploading to S3: {e}")
            import traceback
            traceback.print_exc()
            print("\n[WARNING] Transformation was successful, but S3 upload failed")
        
    else:
        print("\n" + "="*60)
        print("TRANSFORMATION PIPELINE FAILED")
        print("="*60)
        print("✗ Could not process unidades de proyecto data")
