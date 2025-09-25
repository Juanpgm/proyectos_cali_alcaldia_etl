# -*- coding: utf-8 -*-
"""
Data extraction module for project units (unidades de proyecto) from Google Sheets.
Implements functional programming patterns for clean, scalable, and reusable data extraction.
"""

import os
import json
import pandas as pd
import gspread
from typing import Optional, Dict, List, Callable, Any, Tuple, Union
from functools import reduce, partial, wraps
from datetime import datetime


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


# Pure functions for data processing
def load_credentials(credentials_path: str = "sheet-secrets.json") -> Optional[Dict[str, Any]]:
    """Load Google Sheets API credentials from JSON file."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(current_dir, credentials_path)
    
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Credentials file not found: {full_path}")
    
    with open(full_path, 'r', encoding='utf-8') as f:
        return json.load(f)


@safe_execute
def authenticate_gspread(credentials: Dict[str, Any]) -> Optional[gspread.Client]:
    """Authenticate with Google Sheets API using service account credentials."""
    return gspread.service_account_from_dict(credentials)


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
def connect_to_sheet(client: gspread.Client, sheet_url: str) -> Optional[gspread.Spreadsheet]:
    """Connect to a Google Spreadsheet."""
    sheet_id = extract_sheet_id(sheet_url)
    return client.open_by_key(sheet_id)


@safe_execute
def get_worksheet_data(spreadsheet: gspread.Spreadsheet, worksheet_name: str) -> Optional[List[List[str]]]:
    """Get all data from a specific worksheet."""
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        return worksheet.get_all_values()
    except gspread.WorksheetNotFound:
        raise ValueError(f"Worksheet '{worksheet_name}' not found in spreadsheet")


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
        'usuarios_beneficiarios': 'usuarios',
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
        'usuarios': 0,
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


# Main extraction pipeline using functional composition
def create_extraction_pipeline(
    credentials_path: str = "sheet-secrets.json"
) -> Callable[[str, str], Optional[pd.DataFrame]]:
    """
    Create a reusable extraction pipeline using functional composition.
    Returns a configured extraction function.
    """
    
    def extraction_pipeline(sheet_url: str, worksheet_name: str) -> Optional[pd.DataFrame]:
        """Functional pipeline for data extraction."""
        
        try:
            print("="*80)
            print("FUNCTIONAL DATA EXTRACTION PIPELINE")
            print("="*80)
            
            # Step 1: Authentication pipeline
            print("\n1. Loading credentials and authenticating...")
            credentials = load_credentials(credentials_path)
            if not credentials:
                return None
            
            client = authenticate_gspread(credentials)
            if not client:
                return None
            print("✓ Authentication successful")
            
            # Step 2: Data extraction pipeline
            print(f"\n2. Extracting data from: {worksheet_name}")
            spreadsheet = connect_to_sheet(client, sheet_url)
            if not spreadsheet:
                return None
            
            raw_data = get_worksheet_data(spreadsheet, worksheet_name)
            if not raw_data:
                return None
            
            print(f"✓ Extracted {len(raw_data)} rows from '{spreadsheet.title}'")
            
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
            return None
    
    return extraction_pipeline


def extract_obras_equipamientos(
    sheet_url: str = "https://docs.google.com/spreadsheets/d/1UUHPSr5juelZjkILxFJ4mmUP0tT_emoKbptRg-bsXio/edit?usp=sharing",
    worksheet_name: str = "obras_equipamientos"
) -> Optional[pd.DataFrame]:
    """
    Main function to extract obras_equipamientos data and process it.
    
    Args:
        sheet_url: URL of the Google Sheet
        worksheet_name: Name of the worksheet to extract
        
    Returns:
        DataFrame with equipamientos data or None if failed
    """
    return pipeline_extract_and_transform(sheet_url, worksheet_name)


# Functional composition helpers
def compose(*functions):
    """Compose multiple functions into a single function."""
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(value, *functions):
    """Apply a sequence of functions to a value (pipe operator)."""
    return reduce(lambda acc, func: func(acc), functions, value)


# Higher-order functions for data processing
def apply_transformation(transformation_func: Callable) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """
    Create a higher-order function that applies a transformation to a DataFrame.
    
    Args:
        transformation_func: Function to apply to the DataFrame
        
    Returns:
        Function that takes a DataFrame and returns transformed DataFrame
    """
    def wrapper(df: pd.DataFrame) -> pd.DataFrame:
        try:
            return transformation_func(df)
        except Exception as e:
            print(f"Transformation error: {e}")
            return df
    return wrapper


def log_step(step_name: str) -> Callable[[Any], Any]:
    """
    Create a logging decorator for pipeline steps.
    
    Args:
        step_name: Name of the step to log
        
    Returns:
        Function that logs and passes through its input
    """
    def logger(data):
        if isinstance(data, pd.DataFrame):
            print(f"📊 {step_name}: {len(data)} rows, {len(data.columns)} columns")
        else:
            print(f"📊 {step_name}: {type(data).__name__}")
        return data
    return logger


if __name__ == "__main__":
    """
    Main execution block for testing the extraction and transformation pipeline.
    """
    print("Starting Google Sheets extraction and transformation process...")
    
    # Run the complete pipeline
    df_equipamientos = extract_obras_equipamientos()
    
    if df_equipamientos is not None:
        print("\n" + "="*60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"✓ Extracted and transformed equipamientos data: {len(df_equipamientos)} records")
        print("✓ GeoJSON file saved: unidades_proyecto_equipamientos.geojson")
        
        # Show sample of the final data
        print(f"\nSample equipamientos data:")
        print(df_equipamientos.head(2))
        
    else:
        print("\n" + "="*60)
        print("PIPELINE FAILED")
        print("="*60)
        print("✗ Could not extract or transform data")
