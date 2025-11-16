# -*- coding: utf-8 -*-
"""
Script to export quality report from transformed data (without geocoding results).
Shows original variables transformed without comuna_corregimiento, barrio_vereda, or geometry.
"""

import sys
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Set UTF-8 encoding for stdout/stderr (fixes PowerShell encoding issues)
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

# Load environment variables
env_path = Path(__file__).parent / '.env.prod'
if env_path.exists():
    load_dotenv(env_path)
    print(f"[OK] Loaded environment from {env_path}")

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

# Import transformation function
from data_transformation_unidades_proyecto import unidades_proyecto_transformer

def export_quality_report():
    """
    Export quality report showing transformed variables without geocoding data.
    Excludes: comuna_corregimiento, barrio_vereda, geometry columns.
    """
    
    print("="*80)
    print("QUALITY REPORT EXPORT - TRANSFORMED DATA (NO GEOCODING)")
    print("="*80)
    
    print(f"\n{'='*80}")
    print("STEP 1: LOADING TRANSFORMED DATA (WITHOUT GEOCODING)")
    print(f"{'='*80}")
    
    try:
        # Load unidades_proyecto.geojson which has transformed data WITHOUT geocoding
        geojson_path = Path(__file__).parent / 'transformation_app' / 'app_outputs' / 'unidades_proyecto_outputs' / 'unidades_proyecto.geojson'
        
        if not geojson_path.exists():
            print(f"\n❌ ERROR: File not found: {geojson_path}")
            print("   Run the transformation pipeline first:")
            print("   python transformation_app/data_transformation_unidades_proyecto.py")
            return False
        
        # Read GeoJSON file
        import json
        with open(geojson_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        # Convert GeoJSON features to DataFrame
        features = geojson_data['features']
        records = []
        for feature in features:
            props = feature['properties'].copy()
            records.append(props)
        
        df_transformed = pd.DataFrame(records)
        
        print(f"\n✅ Successfully loaded {len(df_transformed)} transformed records")
        print(f"   Total columns: {len(df_transformed.columns)}")
        print(f"   Source: unidades_proyecto.geojson (already transformed, no geocoding)")
        
    except Exception as e:
        print(f"\n❌ ERROR loading data: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print(f"\n{'='*80}")
    print("STEP 2: PREPARING QUALITY REPORT")
    print(f"{'='*80}")
    
    # Exclude columns: comuna_corregimiento, barrio_vereda, geometry, and all geocoding validation columns
    columns_to_exclude = [
        'comuna_corregimiento',
        'barrio_vereda',
        'geometry',
        'geometry_val_s2',
        'barrio_vereda_val',
        'barrio_vereda_val_s2',
        'barrio_vereda_val_s3',
        'comunas_corregimientos_val',
        'comunas_corregimientos_val_s2',
        'comuna_corregimiento_val_s3',
        'corregir',
        'lat',
        'lon',
        'microtio',
        'dataframe',
        'longitude',
        'latitude',
        'geometry_bounds',
        'geometry_type'
    ]
    
    # Get all columns except excluded ones
    available_columns = [col for col in df_transformed.columns if col not in columns_to_exclude]
    
    print(f"\n📋 Columns to include in report:")
    print(f"   Total columns: {len(available_columns)}")
    print(f"   Excluded columns: {len(columns_to_exclude)} (geocoding and geometry related)")
    
    # Create export dataframe
    export_df = df_transformed[available_columns].copy()
    
    # Ensure upid is first column
    if 'upid' in export_df.columns:
        cols = ['upid'] + [col for col in export_df.columns if col != 'upid']
        export_df = export_df[cols]
        print(f"   ✅ UPID column preserved as first column")
    
    print(f"\n{'='*80}")
    print("STEP 3: EXPORTING TO EXCEL (ONE FILE PER CENTRO GESTOR)")
    print(f"{'='*80}")
    
    # Create output directory
    output_dir = Path(__file__).parent / 'test_outputs' / 'quality_reports_by_centro'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Group by nombre_centro_gestor
    if 'nombre_centro_gestor' not in export_df.columns:
        print(f"\n❌ ERROR: Column 'nombre_centro_gestor' not found")
        return False
    
    # Get unique centros gestores
    centros_gestores = export_df['nombre_centro_gestor'].dropna().unique()
    print(f"\n📊 Found {len(centros_gestores)} unique centros gestores")
    
    files_generated = []
    
    # Export one file per centro gestor
    try:
        for i, centro in enumerate(centros_gestores, 1):
            # Filter data for this centro
            centro_df = export_df[export_df['nombre_centro_gestor'] == centro].copy()
            
            # Create safe filename (remove special characters)
            safe_filename = str(centro).replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_').replace('?', '_').replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_')
            safe_filename = safe_filename.strip()[:100]  # Limit filename length
            
            output_file = output_dir / f"{safe_filename}_temp_validar.xlsx"
            
            # Save to Excel
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                centro_df.to_excel(writer, sheet_name='Quality Report', index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets['Quality Report']
                for idx, col in enumerate(centro_df.columns, 1):
                    max_length = max(
                        centro_df[col].astype(str).map(len).max(),
                        len(str(col))
                    )
                    adjusted_width = min(max_length + 2, 50)
                    column_letter = worksheet.cell(1, idx).column_letter
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            files_generated.append({
                'centro': centro,
                'file': output_file.name,
                'records': len(centro_df)
            })
            
            print(f"   [{i:2d}/{len(centros_gestores)}] ✅ {safe_filename}.xlsx ({len(centro_df)} records)")
        
        print(f"\n{'='*80}")
        print("EXPORT SUMMARY")
        print(f"{'='*80}")
        print(f"✅ Generated {len(files_generated)} Excel files")
        print(f"� Location: {output_dir}")
        print(f"📊 Total records: {len(export_df)}")
        print(f"📋 Columns per file: {len(export_df.columns)}")
        
        print(f"\n📋 Files generated:")
        for item in files_generated:
            print(f"   • {item['file']} ({item['records']} records)")
        
        print(f"\n{'='*80}")
        print("REPORT SUMMARY")
        print(f"{'='*80}")
        print(f"✅ Reports show transformed data WITHOUT geocoding results")
        print(f"✅ Excluded: comuna_corregimiento, barrio_vereda, geometry")
        print(f"✅ Focus: Quality of other transformed variables")
        print(f"✅ UPID preserved for tracking")
        print(f"✅ One file per Centro Gestor for easy distribution")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR exporting to Excel: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("Starting quality report export...")
    success = export_quality_report()
    
    if success:
        print("\n✅ Quality report export completed successfully!")
    else:
        print("\n❌ Quality report export failed!")
        sys.exit(1)
