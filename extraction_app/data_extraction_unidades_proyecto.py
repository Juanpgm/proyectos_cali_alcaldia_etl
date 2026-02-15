# -*- coding: utf-8 -*-
"""
Data extraction module for project units (unidades de proyecto) from Google Drive Excel files.
Implements functional programming patterns for clean, scalable, and reusable data extraction.
Uses Workload Identity Federation for secure authentication.
Reads multiple Excel files from a Google Drive folder and concatenates them into a single DataFrame.
"""

import os
import json
import sys
import pandas as pd
import io
from typing import Optional, List, Callable, Any, Dict
from functools import reduce, wraps

# Add database config to path for centralized configuration
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'database'))

try:
    from config import (
        get_drive_service, 
        list_excel_files_in_folder, 
        download_excel_file,
        DRIVE_FOLDER_ID
    )
except ImportError as e:
    print(f"Warning: Could not import from config module: {e}")
    # Fallback imports or alternative configuration
    get_drive_service = None
    list_excel_files_in_folder = None
    download_excel_file = None
    DRIVE_FOLDER_ID = None


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
# Note: get_drive_service and related functions are now imported from config.py


@safe_execute
def read_excel_file_to_dataframe(file_buffer: io.BytesIO, file_name: str) -> Optional[pd.DataFrame]:
    """
    Lee un archivo Excel desde un buffer en memoria y retorna un DataFrame.
    Lee BPIN como string para evitar problemas de overflow con números grandes.
    
    Args:
        file_buffer: Buffer con el contenido del archivo Excel
        file_name: Nombre del archivo (para logging)
        
    Returns:
        DataFrame con los datos del Excel o None si falla
    """
    try:
        # Definir tipos de dato específicos para columnas problemáticas
        # BPIN se lee como string para evitar overflow de int32
        dtype_spec = {
            'BPIN': str,
            'bpin': str,
            'Bpin': str
        }
        
        # Intentar leer el Excel (probará automáticamente con openpyxl y xlrd)
        df = pd.read_excel(file_buffer, engine='openpyxl', dtype=dtype_spec)
        
        if df.empty:
            print(f"[WARNING]  Archivo aparentemente vacío: {file_name} (0 filas)")
            return None
        
        name_display = file_name[:30] + "..." if len(file_name) > 30 else file_name
        print(f"[OK] Leído: {name_display} ({len(df)} filas, {len(df.columns)} columnas)")
        
        # Verificar que tenga al menos algunas filas de datos (no solo headers)
        if len(df) < 2:
            print(f"[WARNING]  Archivo con muy pocas filas: {file_name} ({len(df)} filas) - posible archivo de solo headers")
            return None
            
        return df
        
    except Exception as e:
        # Intentar con xlrd para archivos .xls más antiguos
        try:
            file_buffer.seek(0)  # Volver al inicio
            
            # Intentar con dtype_spec para xlrd también
            dtype_spec = {
                'BPIN': str,
                'bpin': str,
                'Bpin': str
            }
            
            df = pd.read_excel(file_buffer, engine='xlrd', dtype=dtype_spec)
            
            if df.empty:
                print(f"[WARNING]  Archivo aparentemente vacío: {file_name} (0 filas)")
                return None
            
            name_display = file_name[:30] + "..." if len(file_name) > 30 else file_name
            print(f"[OK] Leído (xlrd): {name_display} ({len(df)} filas, {len(df.columns)} columnas)")
            
            # Verificar que tenga al menos algunas filas de datos (no solo headers)
            if len(df) < 2:
                print(f"[WARNING]  Archivo con muy pocas filas: {file_name} ({len(df)} filas) - posible archivo de solo headers")
                return None
                
            return df
        except Exception as e2:
            print(f"[ERROR] Error leyendo {file_name}: {e}")
            print(f"   También falló con xlrd: {e2}")
            return None


@safe_execute
def get_excel_files_from_drive(folder_id: str) -> List[Dict[str, Any]]:
    """
    Obtiene lista de archivos Excel desde una carpeta de Google Drive.
    
    Args:
        folder_id: ID de la carpeta de Google Drive
        
    Returns:
        Lista de diccionarios con información de archivos
    """
    if not folder_id:
        print("[ERROR] No se proporcionó ID de carpeta de Drive")
        return []
    
    files = list_excel_files_in_folder(folder_id)
    return files if files else []


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


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza los nombres de columnas de un DataFrame.
    También normaliza coordenadas con formato europeo (comas decimales).
    
    Args:
        df: DataFrame con columnas a normalizar
        
    Returns:
        DataFrame con columnas normalizadas
    """
    if df.empty:
        return df
    
    df_copy = df.copy()
    df_copy.columns = normalize_columns(df_copy.columns.astype(str).tolist())
    
    # Reemplazar valores vacíos con NaN
    df_copy = df_copy.replace('', pd.NA)
    
    # CRÍTICO: Normalizar coordenadas con formato europeo (comas → puntos)
    # Las coordenadas pueden venir como strings con comas decimales: "3,479621"
    coord_columns = [col for col in df_copy.columns if col in ['lat', 'lon', 'latitud', 'longitud']]
    
    for col in coord_columns:
        if col in df_copy.columns:
            # Función de conversión segura
            def convert_european_decimal(value):
                if pd.isna(value) or value == '' or value is None:
                    return None
                # Si ya es numérico, retornar tal cual
                if isinstance(value, (int, float)):
                    return value
                # Convertir a string y reemplazar comas por puntos
                str_val = str(value).strip()
                if str_val == '' or str_val.lower() in ['nan', 'none', 'null']:
                    return None
                # Reemplazar coma por punto
                str_val = str_val.replace(',', '.')
                # Intentar convertir a float
                try:
                    return float(str_val)
                except (ValueError, TypeError):
                    return None
            
            # Aplicar conversión
            df_copy[col] = df_copy[col].apply(convert_european_decimal)
            print(f"[CONFIG] Columna '{col}' normalizada: comas → puntos decimales")
    
    return df_copy


def concatenate_dataframes(dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Concatena múltiples DataFrames verticalmente (uno debajo del otro).
    
    Args:
        dataframes: Lista de DataFrames a concatenar
        
    Returns:
        DataFrame concatenado con todos los datos
    """
    if not dataframes:
        print("[WARNING]  No hay DataFrames para concatenar")
        return pd.DataFrame()
    
    # Filtrar DataFrames vacíos
    valid_dfs = [df for df in dataframes if not df.empty]
    
    if not valid_dfs:
        print("[WARNING]  Todos los DataFrames están vacíos")
        return pd.DataFrame()
    
    # Concatenar verticalmente (axis=0)
    concatenated = pd.concat(valid_dfs, axis=0, ignore_index=True)
    
    print(f"[OK] Concatenados {len(valid_dfs)} DataFrames:")
    print(f"   - Total de filas: {len(concatenated)}")
    print(f"   - Total de columnas: {len(concatenated.columns)}")
    
    return concatenated


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
    Detects and fixes percentage values stored as decimals (divided by 100).
    """
    # Create a copy to avoid mutations
    standardized_df = df.copy()
    
    # Handle empty column name that contains centro gestor data
    if '' in standardized_df.columns:
        # Rename empty column to 'nombre_centro_gestor'
        standardized_df = standardized_df.rename(columns={'': 'nombre_centro_gestor'})
        print(f"[OK] Renamed empty column to 'nombre_centro_gestor'")
    
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
    
    # FIX: Detect and correct percentage values stored as decimals (0.01 instead of 1)
    # This happens in some Excel files where percentages are stored divided by 100
    percentage_columns = ['avance_obra', 'avance_fisico_obra']
    
    for col in percentage_columns:
        if col in standardized_df.columns:
            # First convert to numeric, coercing errors to NaN
            standardized_df[col] = pd.to_numeric(standardized_df[col], errors='coerce')
            
            # Smart detection: check if ANY values are in decimal format (0.XX instead of XX%)
            # This handles mixed data from different Excel sources
            non_zero_values = standardized_df[col].dropna()
            non_zero_values = non_zero_values[non_zero_values != 0]
            
            if len(non_zero_values) > 0:
                # Check if there are values between 0 and 1 (excluding 0)
                # These are likely percentages stored as decimals
                decimal_values = non_zero_values[(non_zero_values > 0) & (non_zero_values <= 1.0)]
                
                if len(decimal_values) > 0:
                    # We have values that look like decimals (0.XX)
                    # Check if there are also values > 1 (normal percentages)
                    normal_values = non_zero_values[non_zero_values > 1.0]
                    
                    if len(normal_values) > 0:
                        # Mixed format: some decimals, some normal percentages
                        # Only multiply the decimal values by 100
                        print(f"[WARNING]  Detected MIXED percentage formats in '{col}'")
                        print(f"   - Decimal values (0-1): {len(decimal_values)}")
                        print(f"   - Normal values (>1): {len(normal_values)}")
                        print(f"   Multiplying decimal values by 100")
                        
                        # Create a mask for values that need correction
                        mask = (standardized_df[col] > 0) & (standardized_df[col] <= 1.0)
                        standardized_df.loc[mask, col] = standardized_df.loc[mask, col] * 100
                        
                        print(f"   [OK] Corrected {mask.sum()} values")
                    else:
                        # All non-zero values are decimals
                        print(f"[WARNING]  All percentage values are decimals in '{col}' (max={decimal_values.max():.4f})")
                        print(f"   Multiplying all values by 100")
                        standardized_df[col] = standardized_df[col] * 100
                        print(f"   [OK] Corrected: new max = {standardized_df[col].max():.2f}")
    
    # CRÍTICO: Normalizar coordenadas nuevamente después de concatenación
    # Pandas puede cambiar los tipos al concatenar DataFrames
    coord_columns = [col for col in standardized_df.columns if col in ['lat', 'lon', 'latitud', 'longitud']]
    
    for col in coord_columns:
        if col in standardized_df.columns:
            # Función de conversión segura (misma que en normalize_dataframe_columns)
            def convert_european_decimal(value):
                if pd.isna(value) or value == '' or value is None:
                    return None
                # Si ya es numérico, retornar tal cual
                if isinstance(value, (int, float)):
                    return value
                # Convertir a string y reemplazar comas por puntos
                str_val = str(value).strip()
                if str_val == '' or str_val.lower() in ['nan', 'none', 'null']:
                    return None
                # Reemplazar coma por punto
                str_val = str_val.replace(',', '.')
                # Intentar convertir a float
                try:
                    return float(str_val)
                except (ValueError, TypeError):
                    return None
            
            # Aplicar conversión
            standardized_df[col] = standardized_df[col].apply(convert_european_decimal)
            # Forzar tipo numérico float64
            standardized_df[col] = standardized_df[col].astype('float64')
            print(f"[CONFIG] Post-concatenación: Columna '{col}' normalizada a float64")
    
    # Ensure required columns exist with default values (only if they don't exist)
    required_defaults = {
        'presupuesto_base': 0,
        'avance_obra': 0,
        'centros_gravedad': False
    }
    
    for col, default_value in required_defaults.items():
        if col not in standardized_df.columns:
            standardized_df[col] = default_value
    
    # Handle BPIN column separately - preserve existing data
    # After normalization, BPIN becomes 'bpin' in lowercase
    if 'bpin' not in standardized_df.columns:
        # Only create if it doesn't exist - never overwrite
        standardized_df['bpin'] = None
        print(f"[WARNING]  Warning: No BPIN column found in data")
    
    print(f"[OK] Data standardization complete: {len(standardized_df.columns)} columns")
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
        print(f"[OK] JSON saved: {os.path.basename(output_path)} ({len(json_data)} records, {file_size:.1f} KB)")
        return True
    
    except Exception as e:
        print(f"✗ Error saving JSON: {e}")
        return False


# Main extraction pipeline using functional composition with Workload Identity
def create_extraction_pipeline() -> Callable[[str], Optional[pd.DataFrame]]:
    """
    Create a reusable extraction pipeline using Workload Identity Federation.
    Extracts data from multiple Excel files in a Google Drive folder.
    Returns a configured extraction function.
    """
    
    def extraction_pipeline(folder_id: str) -> Optional[pd.DataFrame]:
        """Functional pipeline for secure data extraction from Google Drive Excel files."""
        
        try:
            print("="*80)
            print("FUNCTIONAL DATA EXTRACTION PIPELINE - GOOGLE DRIVE EXCEL FILES")
            print("="*80)
            
            # Step 1: Secure authentication using Workload Identity
            print("\n1. Authenticating with Workload Identity Federation...")
            service = get_drive_service()
            if not service:
                print("✗ Authentication failed")
                print("[CONFIG] Run: gcloud auth application-default login --scopes=https://www.googleapis.com/auth/drive.readonly")
                return None
            print("[OK] Authentication successful with Workload Identity")
            
            # Step 2: List Excel files in Drive folder
            print(f"\n2. Listing Excel files in Drive folder...")
            folder_display = folder_id[:10] + "***" if len(folder_id) > 10 else folder_id
            print(f"   Folder ID: {folder_display}")
            
            excel_files = get_excel_files_from_drive(folder_id)
            if not excel_files:
                print("✗ No Excel files found in folder")
                return None
            
            print(f"[OK] Found {len(excel_files)} Excel files")
            
            # Step 3: Download and read each Excel file
            print(f"\n3. Downloading and reading Excel files...")
            dataframes = []
            
            for i, file_info in enumerate(excel_files, 1):
                file_id = file_info['id']
                file_name = file_info['name']
                
                print(f"\n   [{i}/{len(excel_files)}] Processing: {file_name}")
                
                # Download file to memory
                file_buffer = download_excel_file(file_id, file_name)
                if not file_buffer:
                    print(f"   [ERROR]  Download failed for {file_name}")
                    continue
                
                print(f"   [OK] Downloaded {file_name} ({len(file_buffer.getvalue())} bytes)")
                
                # Read Excel to DataFrame
                df = read_excel_file_to_dataframe(file_buffer, file_name)
                if df is not None and not df.empty:
                    print(f"   [OK] DataFrame created: {len(df)} rows, {len(df.columns)} columns")
                    
                    # Normalize column names
                    df = normalize_dataframe_columns(df)
                    
                    # CRITICAL FIX: Add nombre_centro_gestor from filename if not present
                    # Extract centro gestor name from filename (remove .xlsx/.xls extension)
                    centro_gestor = file_name.replace('.xlsx', '').replace('.xls', '').strip()
                    
                    # Only add if column doesn't exist or has null values
                    if 'nombre_centro_gestor' not in df.columns or df['nombre_centro_gestor'].isna().all():
                        df['nombre_centro_gestor'] = centro_gestor
                        print(f"   [OK] Added nombre_centro_gestor: '{centro_gestor}'")
                    
                    # Check for coordinates
                    lat_cols = [col for col in df.columns if 'lat' in col.lower()]
                    lon_cols = [col for col in df.columns if 'lon' in col.lower()]
                    coord_count = 0
                    if lat_cols and lon_cols:
                        lat_col = lat_cols[0]
                        lon_col = lon_cols[0]
                        coord_count = df[lat_col].notna().sum() if lat_col in df.columns else 0
                    
                    print(f"   [INFO] Coordinates: {coord_count} rows with lat/lon data")
                    dataframes.append(df)
                else:
                    print(f"   [ERROR]  Failed to create DataFrame from {file_name} - df is None or empty")
            
            if not dataframes:
                print("\n✗ No valid data extracted from any file")
                return None
            
            # Step 4: Concatenate all DataFrames
            print(f"\n4. Concatenating data from all files...")
            combined_df = concatenate_dataframes(dataframes)
            
            if combined_df.empty:
                print("✗ Combined DataFrame is empty")
                return None
            
            # Step 5: Standardize data structure
            print(f"\n5. Standardizing data structure...")
            final_df = standardize_data_structure(combined_df)
            
            print(f"\n[OK] Extraction completed successfully!")
            print(f"   - Files processed: {len(dataframes)}")
            print(f"   - Total rows: {len(final_df)}")
            print(f"   - Total columns: {len(final_df.columns)}")
            
            return final_df
            
        except Exception as e:
            print(f"✗ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    return extraction_pipeline


def extract_unidades_proyecto_data(
    folder_id: str = None
) -> Optional[pd.DataFrame]:
    """
    Extract unidades de proyecto data from Google Drive Excel files directly to memory.
    Perfect for in-memory processing without temporary files.
    
    Args:
        folder_id: Google Drive folder ID (uses config if None)
        
    Returns:
        DataFrame with extracted data or None if failed
    """
    
    # Use centralized configuration if parameter not provided
    if folder_id is None:
        folder_id = DRIVE_FOLDER_ID
    
    if not folder_id:
        print("[ERROR] No folder ID provided and DRIVE_FOLDER_ID not configured")
        return None
    
    print("[START] Extracting data from Google Drive Excel files directly to memory")
    
    # Create extraction pipeline
    extract_data = create_extraction_pipeline()
    
    # Extract data
    df = extract_data(folder_id)
    
    if df is not None:
        print(f"\n[OK] Extraction completed successfully!")
        print(f"  - Records extracted: {len(df)}")
        print(f"  - Data ready for in-memory processing")
        return df
    else:
        print(f"\n✗ Data extraction failed")
        return None


def extract_and_save_unidades_proyecto(
    folder_id: str = None
) -> Optional[pd.DataFrame]:
    """
    Main function to extract unidades de proyecto data from Google Drive and save as JSON.
    Implements complete functional pipeline using centralized configuration.
    
    Args:
        folder_id: Google Drive folder ID (uses config if None)
        
    Returns:
        DataFrame with extracted data or None if failed
    """
    
    # Use centralized configuration if parameter not provided
    if folder_id is None:
        folder_id = DRIVE_FOLDER_ID
    
    if not folder_id:
        print("[ERROR] No folder ID provided and DRIVE_FOLDER_ID not configured")
        return None
    
    # Create extraction pipeline
    extract_data = create_extraction_pipeline()
    
    # Extract data
    df = extract_data(folder_id)
    
    if df is not None:
        # Create output directory and save JSON
        output_dir = create_output_directory("../transformation_app/app_inputs/unidades_proyecto_input")
        json_path = os.path.join(output_dir, "unidades_proyecto.json")
        
        # Save as JSON
        success = save_as_json(df, json_path)
        
        if success:
            print(f"\n[OK] Extraction completed successfully!")
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
            print(f"[DATA] {step_name}...")
            result = func(*args, **kwargs)
            if result is not None:
                if isinstance(result, pd.DataFrame):
                    print(f"[OK] {step_name}: {len(result)} rows, {len(result.columns)} columns")
                else:
                    print(f"[OK] {step_name}: completed")
            else:
                print(f"✗ {step_name}: failed")
            return result
        return wrapper
    return decorator


if __name__ == "__main__":
    """
    Main execution block for testing the extraction pipeline.
    """
    print("Starting Google Drive Excel extraction process...")
    
    # Run the complete extraction pipeline
    df_result = extract_and_save_unidades_proyecto()
    
    if df_result is not None:
        print("\n" + "="*60)
        print("EXTRACTION COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"[OK] Extracted data: {len(df_result)} records")
        print(f"[OK] Columns: {len(df_result.columns)}")
        print(f"[OK] JSON file saved in: transformation_app/app_inputs/unidades_proyecto_input/")
        
        # Show sample of the extracted data
        print(f"\nSample data (first 2 records):")
        print(df_result.head(2).to_string())
        
        # Show column names
        print(f"\nColumn names ({len(df_result.columns)} columns):")
        for i, col in enumerate(df_result.columns, 1):
            print(f"  {i}. {col}")
        
    else:
        print("\n" + "="*60)
        print("EXTRACTION FAILED")
        print("="*60)
        print("✗ Could not extract data from Google Drive Excel files")
