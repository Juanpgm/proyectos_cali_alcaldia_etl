# -*- coding: utf-8 -*-
"""
Data extraction module for project units (unidades de proyecto) from Google Sheets.
Implements functional programming patterns for clean, scalable, and reusable data extraction.
Uses Workload Identity Federation for secure authentication.
"""

import os
import json
import sys
import pandas as pd
import gspread
from typing import Optional, List, Callable, Any
from functools import reduce, wraps

# Add database config to path for centralized configuration
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'database'))

try:
    from config import get_sheets_client, open_spreadsheet_by_url, SHEETS_CONFIG
except ImportError as e:
    print(f"Warning: Could not import from config module: {e}")
    # Fallback imports or alternative configuration
    get_sheets_client = None
    open_spreadsheet_by_url = None
    SHEETS_CONFIG = {}


# Functional composition utilities
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


def safe_execute(func: Callable, default_value: Any = None) -> Callable:
    """Decorator to safely execute functions with error handling."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Error in {func.__name__}: {e}")
            return default_value
    return wrapper


# Pure functions for data processing using centralized configuration
# Note: get_sheets_client is now imported from config.py


def extract_sheet_id(url: str) -> str:
    """Extract sheet ID from Google Sheets URL using functional approach."""
    if '/d/' not in url:
        raise ValueError(f"Invalid Google Sheets URL format: {url}")
    
    return pipe(
        url,
        lambda x: x.split('/d/')[1],
        lambda x: x.split('/edit')[0] if '/edit' in x else x.split('/')[0]
    )


@safe_execute
def connect_to_sheet(sheet_url: str) -> Optional[gspread.Spreadsheet]:
    """Connect to a Google Spreadsheet using Workload Identity."""
    return open_spreadsheet_by_url(sheet_url)


@safe_execute
def get_sheet_data(spreadsheet: gspread.Spreadsheet, worksheet_name: str) -> Optional[List[List[str]]]:
    """Get all data from a specific worksheet using centralized configuration."""
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        return worksheet.get_all_values()
    except Exception as e:
        print(f"Error accessing worksheet '{worksheet_name}': {e}")
        return None


def clean_column_name(col_name: str) -> str:
    """Clean and normalize column names using functional transformations."""
    transformations = [
        str.strip,
        str.lower,
        lambda x: x.replace(' ', '_'),
        lambda x: x.replace('.', '_'),
        lambda x: x.replace('(', ''),
        lambda x: x.replace(')', ''),
        lambda x: x.replace('-', '_'),
        lambda x: x.replace('ñ', 'n'),
        lambda x: x.replace('á', 'a'),
        lambda x: x.replace('é', 'e'),
        lambda x: x.replace('í', 'i'),
        lambda x: x.replace('ó', 'o'),
        lambda x: x.replace('ú', 'u')
    ]
    
    return pipe(col_name, *transformations)


def normalize_columns(columns: List[str]) -> List[str]:
    """Normalize all column names in a list using map."""
    return list(map(clean_column_name, columns))


def filter_empty_rows(data: List[List[str]]) -> List[List[str]]:
    """Filter out completely empty rows from data."""
    return list(filter(lambda row: any(cell.strip() for cell in row), data))


def convert_to_dataframe(data: List[List[str]]) -> pd.DataFrame:
    """Convert raw sheet data to pandas DataFrame using functional approach."""
    if not data:
        return pd.DataFrame()
    
    # Functional pipeline for data conversion
    headers = normalize_columns(data[0])
    rows = filter_empty_rows(data[1:])
    
    # Create DataFrame and replace empty strings with NaN
    return pipe(
        pd.DataFrame(rows, columns=headers),
        lambda df: df.replace('', pd.NA)
    )


def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """Validate that DataFrame contains required columns."""
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"Warning: Missing required columns: {missing_columns}")
        print(f"Available columns: {list(df.columns)}")
        return False
    
    return True


def standardize_data_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize data structure while preserving ALL original columns.
    Uses functional approach to add standardized column aliases.
    """
    # Create a copy to avoid mutations
    standardized_df = df.copy()
    
    # Column standardization mapping
    column_mapping = {
        'fuente_de_financiacion': 'fuente_financiacion',
        'subclase_obra': 'subclase',
        'ppto_base': 'presupuesto_base',
        'avance_fisico_obra': 'avance_obra',
        'geometry': 'geom',
        'geometria': 'geom'
    }
    
    # Add standardized aliases using functional approach
    for source_col, target_col in column_mapping.items():
        if source_col in standardized_df.columns and target_col not in standardized_df.columns:
            standardized_df[target_col] = standardized_df[source_col]
    
    # Ensure required columns exist with default values
    required_defaults = {
        'bpin': 0,
        'presupuesto_base': 0,
        'avance_obra': 0,
        'centros_gravedad': False
    }
    
    for col, default_value in required_defaults.items():
        if col not in standardized_df.columns:
            standardized_df[col] = default_value
    
    print(f"✓ Data standardization complete: {len(standardized_df.columns)} columns")
    return standardized_df


def create_output_directory(base_path: str) -> str:
    """Create output directory structure and return absolute path."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, base_path)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def save_as_json(data: pd.DataFrame, output_path: str) -> bool:
    """Save DataFrame as JSON file with proper encoding."""
    try:
        # Convert DataFrame to JSON-serializable format
        json_data = data.to_dict('records')
        
        # Save with proper encoding
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False, default=str)
        
        file_size = os.path.getsize(output_path) / 1024  # Size in KB
        print(f"✓ JSON saved: {os.path.basename(output_path)} ({len(json_data)} records, {file_size:.1f} KB)")
        return True
    
    except Exception as e:
        print(f"✗ Error saving JSON: {e}")
        return False


# Main extraction pipeline using functional composition with Workload Identity
def create_extraction_pipeline() -> Callable[[str, str], Optional[pd.DataFrame]]:
    """
    Create a reusable extraction pipeline using Workload Identity Federation.
    Returns a configured extraction function.
    """
    
    def extraction_pipeline(sheet_url: str, worksheet_name: str) -> Optional[pd.DataFrame]:
        """Functional pipeline for secure data extraction."""
        
        try:
            print("="*80)
            print("FUNCTIONAL DATA EXTRACTION PIPELINE (WORKLOAD IDENTITY)")
            print("="*80)
            
            # Step 1: Secure authentication using Workload Identity
            print("\n1. Authenticating with Workload Identity Federation...")
            client = get_sheets_client()
            if not client:
                print("✗ Authentication failed")
                print("🔧 Run: gcloud auth application-default login --scopes=https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive.readonly")
                return None
            print("✓ Authentication successful with Workload Identity")
            
            # Step 2: Data extraction pipeline
            print(f"\n2. Extracting data from: {worksheet_name}")
            spreadsheet = connect_to_sheet(sheet_url)
            if not spreadsheet:
                return None
            
            raw_data = get_sheet_data(spreadsheet, worksheet_name)
            if not raw_data:
                return None
            
            # Mostrar título de forma segura
            title_display = spreadsheet.title[:20] + "***" if len(spreadsheet.title) > 20 else spreadsheet.title
            print(f"✓ Extracted {len(raw_data)} rows from '{title_display}'")
            
            # Step 3: Data transformation pipeline
            print(f"\n3. Converting and standardizing data...")
            df = pipe(
                raw_data,
                convert_to_dataframe,
                standardize_data_structure
            )
            
            print(f"✓ Created DataFrame: {len(df)} rows, {len(df.columns)} columns")
            
            return df
            
        except Exception as e:
            print(f"✗ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    return extraction_pipeline


def extract_unidades_proyecto_data(
    sheet_url: str = None,
    worksheet_name: str = None
) -> Optional[pd.DataFrame]:
    """
    Extract unidades de proyecto data directly to memory (no file saving).
    Perfect for in-memory processing without temporary files.
    
    Args:
        sheet_url: Google Sheets URL (uses config if None)
        worksheet_name: Worksheet name (uses config if None)
        
    Returns:
        DataFrame with extracted data or None if failed
    """
    
    # Use centralized configuration if parameters not provided
    if sheet_url is None:
        sheet_url = SHEETS_CONFIG['unidades_proyecto']['url']
    if worksheet_name is None:
        worksheet_name = SHEETS_CONFIG['unidades_proyecto']['worksheet']
    
    print("🚀 Extracting data directly to memory (no temporary files)")
    
    # Create extraction pipeline
    extract_data = create_extraction_pipeline()
    
    # Extract data
    df = extract_data(sheet_url, worksheet_name)
    
    if df is not None:
        print(f"✓ Extraction completed successfully!")
        print(f"  - Records extracted: {len(df)}")
        print(f"  - Data ready for in-memory processing")
        return df
    else:
        print(f"✗ Data extraction failed")
        return None


def extract_and_save_unidades_proyecto(
    sheet_url: str = None,
    worksheet_name: str = None
) -> Optional[pd.DataFrame]:
    """
    Main function to extract unidades de proyecto data and save as JSON.
    Implements complete functional pipeline using centralized configuration.
    """
    
    # Use centralized configuration if parameters not provided
    if sheet_url is None:
        sheet_url = SHEETS_CONFIG['unidades_proyecto']['url']
    if worksheet_name is None:
        worksheet_name = SHEETS_CONFIG['unidades_proyecto']['worksheet']
    
    # Create extraction pipeline
    extract_data = create_extraction_pipeline()
    
    # Extract data
    df = extract_data(sheet_url, worksheet_name)
    
    if df is not None:
        # Create output directory and save JSON
        output_dir = create_output_directory("../transformation_app/app_inputs/unidades_proyecto_input")
        json_path = os.path.join(output_dir, "unidades_proyecto.json")
        
        # Save as JSON
        success = save_as_json(df, json_path)
        
        if success:
            print(f"\n✓ Extraction completed successfully!")
            print(f"  - Records extracted: {len(df)}")
            # Mostrar solo el nombre del archivo, no la ruta completa por seguridad
            print(f"  - JSON file: {os.path.basename(json_path)}")
            return df
        else:
            print(f"\n✗ Failed to save JSON file")
            return None
    
    else:
        print(f"\n✗ Data extraction failed")
        return None


# Utility functions for logging and monitoring
def log_pipeline_step(step_name: str) -> Callable:
    """Decorator for logging pipeline steps."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            print(f"📊 {step_name}...")
            result = func(*args, **kwargs)
            if result is not None:
                if isinstance(result, pd.DataFrame):
                    print(f"✓ {step_name}: {len(result)} rows, {len(result.columns)} columns")
                else:
                    print(f"✓ {step_name}: completed")
            else:
                print(f"✗ {step_name}: failed")
            return result
        return wrapper
    return decorator


if __name__ == "__main__":
    """
    Main execution block for testing the extraction pipeline.
    """
    print("Starting Google Sheets extraction process...")
    
    # Run the complete extraction pipeline
    df_result = extract_and_save_unidades_proyecto()
    
    if df_result is not None:
        print("\n" + "="*60)
        print("EXTRACTION COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"✓ Extracted data: {len(df_result)} records")
        print(f"✓ Columns: {len(df_result.columns)}")
        print(f"✓ JSON file saved in: transformation_app/app_inputs/unidades_proyecto_input/")
        
        # Show sample of the extracted data
        print(f"\nSample data (first 2 records):")
        print(df_result.head(2).to_string())
        
    else:
        print("\n" + "="*60)
        print("EXTRACTION FAILED")
        print("="*60)
        print("✗ Could not extract data from Google Sheets")
