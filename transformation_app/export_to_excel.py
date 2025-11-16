# -*- coding: utf-8 -*-
"""
Export transformed unidades de proyecto data to Excel for visual inspection.

This script loads the processed GeoJSON file and exports it to Excel format
for easy visualization and analysis.

Author: AI Assistant
Version: 1.0
"""

import os
import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

def load_geojson_to_dataframe(geojson_path: str) -> pd.DataFrame:
    """
    Load GeoJSON file and convert to DataFrame.
    Extracts properties from features AND includes geometry as JSON string.
    
    Args:
        geojson_path: Path to the GeoJSON file
        
    Returns:
        DataFrame with all features, properties, and geometry column
    """
    print(f"\n📂 Loading GeoJSON file: {geojson_path}")
    
    if not os.path.exists(geojson_path):
        raise FileNotFoundError(f"GeoJSON file not found: {geojson_path}")
    
    # Load GeoJSON
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    
    # Extract features
    features = geojson_data.get('features', [])
    print(f"✓ Found {len(features)} features")
    
    # Convert to DataFrame - extract properties AND geometry
    records = []
    for feature in features:
        # Get properties
        record = feature.get('properties', {}).copy()
        
        # Add geometry as JSON string (for Excel export)
        geometry = feature.get('geometry')
        if geometry:
            # Convert geometry to JSON string
            record['geometry'] = json.dumps(geometry, ensure_ascii=False)
        else:
            record['geometry'] = None
        
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Remove any temporary processing columns if they exist
    columns_to_remove = [
        'geometry_type',
        'geometry_bounds', 
        'processed_timestamp',
        'geometry_json',
        'longitude',
        'latitude'
    ]
    
    existing_cols_to_remove = [col for col in columns_to_remove if col in df.columns]
    if existing_cols_to_remove:
        df = df.drop(columns=existing_cols_to_remove)
        print(f"✓ Removed temporary processing columns: {', '.join(existing_cols_to_remove)}")
    
    print(f"✓ Created DataFrame: {len(df)} rows, {len(df.columns)} columns")
    
    return df


def export_to_excel(df: pd.DataFrame, output_path: str, sheet_name: str = 'Unidades Proyecto') -> bool:
    """
    Export DataFrame to Excel with formatting.
    
    Args:
        df: DataFrame to export
        output_path: Path for the output Excel file
        sheet_name: Name of the Excel sheet
        
    Returns:
        True if successful, False otherwise
    """
    print(f"\n💾 Exporting to Excel: {output_path}")
    
    try:
        # Create Excel writer with xlsxwriter engine for formatting
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            # Write DataFrame to Excel
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            
            # Define formats
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1,
                'align': 'center',
                'valign': 'vcenter'
            })
            
            # Format header row
            for col_num, col_name in enumerate(df.columns):
                worksheet.write(0, col_num, col_name, header_format)
                
                # Auto-adjust column width based on content
                max_len = max(
                    df[col_name].astype(str).apply(len).max(),
                    len(str(col_name))
                )
                # Cap at 50 characters for readability
                max_len = min(max_len, 50)
                worksheet.set_column(col_num, col_num, max_len + 2)
            
            # Freeze first row
            worksheet.freeze_panes(1, 0)
            
            # Add filters
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
        
        file_size = os.path.getsize(output_path) / 1024  # KB
        print(f"✓ Excel file created successfully!")
        print(f"  - Rows: {len(df)}")
        print(f"  - Columns: {len(df.columns)}")
        print(f"  - File size: {file_size:.1f} KB")
        print(f"  - Location: {output_path}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error exporting to Excel: {e}")
        import traceback
        traceback.print_exc()
        return False


def create_summary_sheet(df: pd.DataFrame, output_path: str) -> bool:
    """
    Create an Excel file with multiple sheets including a summary.
    
    Args:
        df: DataFrame with the data
        output_path: Path for the output Excel file
        
    Returns:
        True if successful, False otherwise
    """
    print(f"\n📊 Creating Excel file with summary: {output_path}")
    
    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Define formats
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1,
                'align': 'center',
                'valign': 'vcenter'
            })
            
            title_format = workbook.add_format({
                'bold': True,
                'font_size': 14,
                'align': 'left',
                'valign': 'vcenter'
            })
            
            # Sheet 1: Full Data
            df.to_excel(writer, sheet_name='Datos Completos', index=False)
            worksheet1 = writer.sheets['Datos Completos']
            
            # Format headers
            for col_num, col_name in enumerate(df.columns):
                worksheet1.write(0, col_num, col_name, header_format)
                max_len = min(max(df[col_name].astype(str).apply(len).max(), len(str(col_name))), 50)
                worksheet1.set_column(col_num, col_num, max_len + 2)
            
            worksheet1.freeze_panes(1, 0)
            worksheet1.autofilter(0, 0, len(df), len(df.columns) - 1)
            
            # Sheet 2: Summary Statistics
            summary_data = []
            
            # General statistics
            summary_data.append(['ESTADÍSTICAS GENERALES', ''])
            summary_data.append(['Total de registros', len(df)])
            summary_data.append(['Total de columnas', len(df.columns)])
            summary_data.append(['Fecha de exportación', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
            summary_data.append(['', ''])
            
            # Geometry statistics
            if 'geometry_type' in df.columns:
                summary_data.append(['ESTADÍSTICAS DE GEOMETRÍA', ''])
                geom_counts = df['geometry_type'].value_counts()
                for geom_type, count in geom_counts.items():
                    summary_data.append([f'Geometrías {geom_type}', count])
                summary_data.append(['', ''])
            
            # Validation statistics
            if 'comunas_corregimientos_val_s2' in df.columns:
                summary_data.append(['VALIDACIÓN DE COMUNAS', ''])
                comuna_val = df['comunas_corregimientos_val_s2'].value_counts()
                for val, count in comuna_val.items():
                    summary_data.append([f'{val}', count])
                summary_data.append(['', ''])
            
            if 'barrio_vereda_val_s2' in df.columns:
                summary_data.append(['VALIDACIÓN DE BARRIOS', ''])
                barrio_val = df['barrio_vereda_val_s2'].value_counts()
                for val, count in barrio_val.items():
                    summary_data.append([f'{val}', count])
                summary_data.append(['', ''])
            
            # Estado distribution
            if 'estado' in df.columns:
                summary_data.append(['DISTRIBUCIÓN POR ESTADO', ''])
                estado_counts = df['estado'].value_counts()
                for estado, count in estado_counts.items():
                    summary_data.append([f'{estado}', count])
                summary_data.append(['', ''])
            
            # Sector distribution
            if 'sector' in df.columns:
                summary_data.append(['DISTRIBUCIÓN POR SECTOR', ''])
                sector_counts = df['sector'].value_counts().head(10)
                for sector, count in sector_counts.items():
                    summary_data.append([f'{sector}', count])
                summary_data.append(['', ''])
            
            # Column list
            summary_data.append(['LISTA DE COLUMNAS', ''])
            for col in df.columns:
                summary_data.append([col, df[col].dtype])
            
            # Write summary to sheet
            summary_df = pd.DataFrame(summary_data, columns=['Descripción', 'Valor'])
            summary_df.to_excel(writer, sheet_name='Resumen', index=False)
            
            worksheet2 = writer.sheets['Resumen']
            worksheet2.set_column(0, 0, 40)
            worksheet2.set_column(1, 1, 30)
            
            # Format header
            for col_num in range(2):
                worksheet2.write(0, col_num, summary_df.columns[col_num], header_format)
            
        file_size = os.path.getsize(output_path) / 1024
        print(f"✓ Excel file with summary created successfully!")
        print(f"  - File size: {file_size:.1f} KB")
        print(f"  - Location: {output_path}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error creating summary Excel: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main execution function."""
    print("="*80)
    print("EXPORTAR UNIDADES DE PROYECTO A EXCEL")
    print("="*80)
    
    # Get current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define paths
    geojson_path = os.path.join(
        current_dir, 
        'app_outputs', 
        'unidades_proyecto_outputs', 
        'unidades_proyecto.geojson'
    )
    
    output_dir = os.path.join(current_dir, 'app_outputs', 'unidades_proyecto_outputs')
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Define output Excel paths
    excel_simple_path = os.path.join(output_dir, 'unidades_proyecto_simple.xlsx')
    excel_summary_path = os.path.join(output_dir, 'unidades_proyecto_completo.xlsx')
    
    try:
        # Load GeoJSON
        df = load_geojson_to_dataframe(geojson_path)
        
        # Export simple version
        print("\n" + "="*80)
        print("EXPORTANDO VERSIÓN SIMPLE")
        print("="*80)
        export_to_excel(df, excel_simple_path)
        
        # Export version with summary
        print("\n" + "="*80)
        print("EXPORTANDO VERSIÓN CON RESUMEN")
        print("="*80)
        create_summary_sheet(df, excel_summary_path)
        
        print("\n" + "="*80)
        print("✓ EXPORTACIÓN COMPLETADA EXITOSAMENTE")
        print("="*80)
        print(f"\nArchivos generados:")
        print(f"  1. {excel_simple_path}")
        print(f"  2. {excel_summary_path}")
        
    except Exception as e:
        print(f"\n✗ Error durante la exportación: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
