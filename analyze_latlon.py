#!/usr/bin/env python3
"""
Analyze lat/lon data availability
"""

import pandas as pd
import json

def analyze_lat_lon_data():
    print('=== ANÁLISIS DE DATOS LAT/LON ===')
    
    # Cargar el archivo Excel para analizar lat/lon
    excel_path = 'transformation_app/app_inputs/unidades_proyecto_input/obras_equipamientos.xlsx'
    df = pd.read_excel(excel_path)
    
    print(f'📊 Total registros en Excel: {len(df)}')
    
    # Verificar columnas lat/lon
    lat_cols = [col for col in df.columns if 'lat' in col.lower()]
    lon_cols = [col for col in df.columns if 'lon' in col.lower()]
    
    print(f'📍 Columnas que contienen "lat": {lat_cols}')
    print(f'📍 Columnas que contienen "lon": {lon_cols}')
    
    # Verificar datos en estas columnas
    if 'lat' in df.columns and 'lon' in df.columns:
        valid_lat = df['lat'].notna().sum()
        valid_lon = df['lon'].notna().sum()
        both_valid = ((df['lat'].notna()) & (df['lon'].notna())).sum()
        
        print(f'📊 Registros con lat válida: {valid_lat}')
        print(f'📊 Registros con lon válida: {valid_lon}') 
        print(f'📊 Registros con ambos lat/lon válidos: {both_valid}')
        
        # Mostrar algunos ejemplos
        print(f'\n📋 EJEMPLOS DE DATOS LAT/LON:')
        valid_coords = df[(df['lat'].notna()) & (df['lon'].notna())].head(3)
        for idx, row in valid_coords.iterrows():
            print(f'  Registro {idx}: lat={row["lat"]}, lon={row["lon"]}')
            
        # Verificar registros sin geom pero con lat/lon
        if 'geom' in df.columns:
            no_geom_but_coords = df[(df['geom'].isna()) & (df['lat'].notna()) & (df['lon'].notna())]
            print(f'\n🔍 Registros SIN geom pero CON lat/lon: {len(no_geom_but_coords)}')
            
            if len(no_geom_but_coords) > 0:
                print('  Ejemplos:')
                for idx, row in no_geom_but_coords.head(3).iterrows():
                    print(f'    Registro {idx}: lat={row["lat"]}, lon={row["lon"]}')
                    
            # Verificar registros con geom válida
            valid_geom = df['geom'].notna().sum()
            print(f'\n📊 Registros con geom válida: {valid_geom}')
            print(f'📊 Registros sin geom: {len(df) - valid_geom}')
    else:
        print('❌ No se encontraron columnas lat/lon en el DataFrame')
        
    # Mostrar todas las columnas para debug
    print(f'\n📝 TODAS LAS COLUMNAS DISPONIBLES:')
    for i, col in enumerate(df.columns, 1):
        print(f'  {i:2d}. {col}')

if __name__ == "__main__":
    analyze_lat_lon_data()