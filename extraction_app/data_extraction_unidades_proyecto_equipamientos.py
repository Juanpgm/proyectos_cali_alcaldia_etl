# -*- coding: utf-8 -*-
"""
Data extraction module for project units (unidades de proyecto) from Google Sheets.
Implements functional programming patterns for data extraction and transformation.
"""

import os
import json
import pandas as pd
import gspread
from typing import Optional, Dict, List, Callable, Any, Tuple
from functools import reduce, partial
from datetime import datetime
import sys

# Add the transformation_app directory to the path to import the transformer
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'transformation_app'))
from data_transformation_unidades_proyecto import unidades_proyecto_transformer, save_equipamientos_geojson


def load_credentials(credentials_path: str = "sheet-secrets.json") -> Dict[str, Any]:
    """
    Load Google Sheets API credentials from JSON file.
    
    Args:
        credentials_path: Path to the credentials JSON file
        
    Returns:
        Dictionary containing credentials
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(current_dir, credentials_path)
    
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Credentials file not found: {full_path}")
    
    with open(full_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def authenticate_gspread(credentials: Dict[str, Any]) -> gspread.Client:
    """
    Authenticate with Google Sheets API using service account credentials.
    
    Args:
        credentials: Google service account credentials
        
    Returns:
        Authenticated gspread client
    """
    return gspread.service_account_from_dict(credentials)


def extract_sheet_id(url: str) -> str:
    """
    Extract sheet ID from Google Sheets URL.
    
    Args:
        url: Google Sheets URL
        
    Returns:
        Sheet ID string
    """
    # Extract ID from URLs like:
    # https://docs.google.com/spreadsheets/d/SHEET_ID/edit...
    if '/d/' in url and '/edit' in url:
        return url.split('/d/')[1].split('/edit')[0]
    elif '/d/' in url:
        return url.split('/d/')[1].split('/')[0]
    else:
        raise ValueError(f"Invalid Google Sheets URL format: {url}")


def connect_to_sheet(client: gspread.Client, sheet_url: str) -> gspread.Spreadsheet:
    """
    Connect to a Google Spreadsheet.
    
    Args:
        client: Authenticated gspread client
        sheet_url: URL of the Google Sheet
        
    Returns:
        Spreadsheet object
    """
    sheet_id = extract_sheet_id(sheet_url)
    return client.open_by_key(sheet_id)


def get_worksheet_data(spreadsheet: gspread.Spreadsheet, worksheet_name: str) -> List[List[str]]:
    """
    Get all data from a specific worksheet.
    
    Args:
        spreadsheet: Google Spreadsheet object
        worksheet_name: Name of the worksheet to extract
        
    Returns:
        List of lists containing all cell values
    """
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        return worksheet.get_all_values()
    except gspread.WorksheetNotFound:
        raise ValueError(f"Worksheet '{worksheet_name}' not found in spreadsheet")


def clean_column_name(col_name: str) -> str:
    """
    Clean and normalize column names.
    
    Args:
        col_name: Original column name
        
    Returns:
        Cleaned column name
    """
    return (col_name.strip()
            .lower()
            .replace(' ', '_')
            .replace('.', '_')
            .replace('(', '')
            .replace(')', '')
            .replace('-', '_')
            .replace('ñ', 'n')
            .replace('á', 'a')
            .replace('é', 'e')
            .replace('í', 'i')
            .replace('ó', 'o')
            .replace('ú', 'u'))


def normalize_columns(columns: List[str]) -> List[str]:
    """
    Normalize all column names in a list.
    
    Args:
        columns: List of column names
        
    Returns:
        List of normalized column names
    """
    return list(map(clean_column_name, columns))


def filter_empty_rows(data: List[List[str]]) -> List[List[str]]:
    """
    Filter out completely empty rows from data.
    
    Args:
        data: List of lists containing row data
        
    Returns:
        Filtered data without empty rows
    """
    return [row for row in data if any(cell.strip() for cell in row)]


def convert_to_dataframe(data: List[List[str]]) -> pd.DataFrame:
    """
    Convert raw sheet data to pandas DataFrame.
    
    Args:
        data: List of lists containing sheet data
        
    Returns:
        Pandas DataFrame with normalized columns
    """
    if not data:
        return pd.DataFrame()
    
    # First row contains headers
    headers = normalize_columns(data[0])
    
    # Rest are data rows, filter empty ones
    rows = filter_empty_rows(data[1:])
    
    # Create DataFrame
    df = pd.DataFrame(rows, columns=headers)
    
    # Replace empty strings with NaN
    df = df.replace('', pd.NA)
    
    return df


def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Validate that DataFrame contains required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        True if all required columns are present
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"Warning: Missing required columns: {missing_columns}")
        print(f"Available columns: {list(df.columns)}")
        return False
    
    return True


def standardize_equipamientos_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preserve ALL original columns and add standardized column names where needed.
    This ensures no data loss from the original Google Sheets data.
    
    Args:
        df: Raw equipamientos DataFrame
        
    Returns:
        DataFrame with ALL original columns preserved plus standardized versions
    """
    # Start with a copy of the original DataFrame to preserve ALL columns
    standardized_df = df.copy()
    
    # Add standardized column aliases for key fields (without removing originals)
    column_standardization = {
        'fuente_de_financiacion': 'fuente_financiacion',  # Create alias if needed
        'usuarios_beneficiarios': 'usuarios',  # Create alias if needed  
        'subclase_obra': 'subclase',  # Create alias if needed
        'ppto_base': 'presupuesto_base',  # Create alias if needed
        'avance_fisico_obra': 'avance_obra',  # Create alias if needed
        'geometry': 'geom',  # Create alias if needed
        'geometria': 'geom'  # Create alias if needed
    }
    
    # Add standardized aliases only if the source column exists and target doesn't
    for source_col, target_col in column_standardization.items():
        if source_col in standardized_df.columns and target_col not in standardized_df.columns:
            standardized_df[target_col] = standardized_df[source_col]
    
    # Ensure required columns exist with default values if missing
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
    
    print(f"✓ Preserved ALL {len(standardized_df.columns)} columns from original data")
    print(f"  Original columns: {len(df.columns)}")
    print(f"  Additional standardized columns: {len(standardized_df.columns) - len(df.columns)}")
    
    return standardized_df


def save_dataframe_as_excel(df: pd.DataFrame, output_path: str, sheet_name: str = 'Sheet1') -> None:
    """
    Save DataFrame as Excel file.
    
    Args:
        df: DataFrame to save
        output_path: Path where to save the Excel file
        sheet_name: Name of the Excel sheet
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"✓ Saved Excel file: {output_path} ({len(df)} rows)")


def create_input_directory_structure(base_dir: str = "../transformation_app/app_inputs/unidades_proyecto_input") -> str:
    """
    Create the required input directory structure for the transformation module.
    
    Args:
        base_dir: Base directory path
        
    Returns:
        Absolute path to the input directory
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(current_dir, base_dir)
    
    # Create directory if it doesn't exist
    os.makedirs(input_dir, exist_ok=True)
    
    return input_dir


def pipeline_extract_and_transform(
    sheet_url: str,
    worksheet_name: str,
    credentials_path: str = "sheet-secrets.json"
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Complete pipeline to extract data from Google Sheets and transform it.
    Implements functional programming pattern with composition.
    
    Args:
        sheet_url: URL of the Google Sheet
        worksheet_name: Name of the worksheet to extract
        credentials_path: Path to credentials file
        
    Returns:
        Tuple of (equipamientos_df, vial_df) or (None, None) if failed
    """
    try:
        print("="*80)
        print("FUNCTIONAL DATA EXTRACTION AND TRANSFORMATION PIPELINE")
        print("="*80)
        
        # Step 1: Load credentials and authenticate
        print("\n1. Loading credentials and authenticating...")
        credentials = load_credentials(credentials_path)
        client = authenticate_gspread(credentials)
        print("✓ Authentication successful")
        
        # Step 2: Connect to spreadsheet
        print(f"\n2. Connecting to spreadsheet...")
        spreadsheet = connect_to_sheet(client, sheet_url)
        print(f"✓ Connected to: {spreadsheet.title}")
        
        # Step 3: Extract data from worksheet
        print(f"\n3. Extracting data from worksheet: {worksheet_name}")
        raw_data = get_worksheet_data(spreadsheet, worksheet_name)
        print(f"✓ Extracted {len(raw_data)} rows")
        
        # Step 4: Convert to DataFrame using functional approach
        print(f"\n4. Converting to DataFrame...")
        df = convert_to_dataframe(raw_data)
        print(f"✓ Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
        print(f"✓ Columns: {list(df.columns)}")
        
        # Step 5: Standardize data structure
        print(f"\n5. Standardizing data structure...")
        standardized_df = standardize_equipamientos_data(df)
        print(f"✓ Standardized DataFrame with {len(standardized_df.columns)} columns")
        
        # Step 6: Prepare input files for transformation module
        print(f"\n6. Preparing input files for transformation...")
        input_dir = create_input_directory_structure()
        
        # Save as Excel file (required by transformation module)
        equipamientos_path = os.path.join(input_dir, "obras_equipamientos.xlsx")
        save_dataframe_as_excel(standardized_df, equipamientos_path, "equipamientos")
        
        # Step 7: Run transformation module
        print(f"\n7. Running transformation module...")
        df_equipamientos_transformed = unidades_proyecto_transformer(
            data_directory="app_inputs/unidades_proyecto_input"
        )
        
        if df_equipamientos_transformed is not None:
            print(f"✓ Transformation successful!")
            print(f"  - Equipamientos: {len(df_equipamientos_transformed)} rows")
            
            # Step 8: Save transformed data as GeoJSON
            print(f"\n8. Saving GeoJSON data...")
            success = save_equipamientos_geojson(df_equipamientos_transformed)
            
            if success:
                return df_equipamientos_transformed
            else:
                print("✗ Failed to save GeoJSON")
                return None
        else:
            print("✗ Transformation failed")
            return None
        
    except Exception as e:
        print(f"✗ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return None, None


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
