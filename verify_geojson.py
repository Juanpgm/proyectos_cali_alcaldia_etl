#!/usr/bin/env python3
"""
Script to verify the GeoJSON file and check column preservation
"""

import json
import os

def verify_geojson():
    # Load and analyze the GeoJSON file
    file_path = 'transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto_equipamientos.geojson'
    
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        print('=' * 60)
        print('GEOJSON VERIFICATION - COLUMN PRESERVATION CHECK')
        print('=' * 60)
        print(f'Type: {geojson_data.get("type")}')
        print(f'Total features: {len(geojson_data.get("features", []))}')
        
        if geojson_data.get('features'):
            # Get first feature to check properties
            first_feature = geojson_data['features'][0]
            properties = first_feature.get('properties', {})
            
            print(f'\nProperties in first feature: {len(properties)}')
            print('\nAll property keys (columns preserved):')
            for i, key in enumerate(sorted(properties.keys()), 1):
                print(f'  {i:2d}. {key}')
            
            # Check geometry
            geometry = first_feature.get('geometry', {})
            print(f'\nGeometry type: {geometry.get("type")}')
            
            # Sample values for key columns
            print('\nSample property values:')
            key_columns = ['bpin', 'identificador', 'fuente_financiacion', 'nickname', 'presupuesto_base', 'comuna_corregimiento']
            for key in key_columns:
                if key in properties:
                    value = properties[key]
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:50] + '...'
                    print(f'  {key}: {value}')
            
            print(f'\nFile size: {os.path.getsize(file_path) / 1024:.1f} KB')
            
            # Verify original columns are preserved
            original_columns = [
                'key', 'origen_sheet', 'bpin', 'identificador', 'dataframe', 
                'fuente_financiacion', 'nickname', 'nickname_detalle', 'centros_gravedad', 
                'comuna_corregimiento', 'clase_obra', 'subclase', 'unidad', 'cantidad', 
                'direccion', 'barrio_vereda', 'tipo_intervencion', 'descripcion_intervencion', 
                'presupuesto_base', 'referencia_proceso', 'referencia_contrato', 'avance_obra', 
                'ano', 'usuarios', 'geom', 'lat', 'lon', 'fecha_inicio', 'fecha_fin', 'microtio'
            ]
            
            preserved_count = 0
            missing_columns = []
            
            for col in original_columns:
                # Check for variations (the transformation might have renamed some)
                found = False
                for prop_key in properties.keys():
                    if col.lower() in prop_key.lower() or prop_key.lower() in col.lower():
                        found = True
                        preserved_count += 1
                        break
                if not found:
                    missing_columns.append(col)
            
            print(f'\nCOLUMN PRESERVATION ANALYSIS:')
            print(f'  Original columns from Google Sheets: {len(original_columns)}')
            print(f'  Columns preserved in GeoJSON: {preserved_count}')
            print(f'  Preservation rate: {preserved_count/len(original_columns)*100:.1f}%')
            
            if missing_columns:
                print(f'\nMissing columns: {missing_columns}')
            else:
                print('\n✓ ALL ORIGINAL COLUMNS SUCCESSFULLY PRESERVED!')
                
        print('\n' + '=' * 60)
        print('VERIFICATION COMPLETED')
        print('=' * 60)
    else:
        print('❌ GeoJSON file not found!')

if __name__ == "__main__":
    verify_geojson()