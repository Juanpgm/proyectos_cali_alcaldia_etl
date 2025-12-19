#!/usr/bin/env python3
"""
Analizar qué intervenciones no tienen coordenadas
"""
import pandas as pd
import sys
import os

# Agregar directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extraction_app.data_extraction_unidades_proyecto import extract_unidades_proyecto_data

def main():
    print("Extrayendo datos...")
    df = extract_unidades_proyecto_data()
    
    print(f'\n📊 Análisis de coordenadas GPS:')
    print(f'   • Total intervenciones: {len(df)}')
    print(f'   • Con lat: {df["lat"].notna().sum()}')
    print(f'   • Con lon: {df["lon"].notna().sum()}')
    
    con_coords = (df['lat'].notna() & df['lon'].notna()).sum()
    print(f'   • Con ambas coordenadas: {con_coords} ({con_coords/len(df)*100:.1f}%)')
    
    sin_coords = df[df['lat'].isna() | df['lon'].isna()]
    print(f'   • Sin coordenadas: {len(sin_coords)} ({len(sin_coords)/len(df)*100:.1f}%)')
    
    print(f'\n🏢 Tipos de equipamiento SIN coordenadas (top 10):')
    tipo_counts = sin_coords['tipo_equipamiento'].value_counts().head(10)
    for tipo, count in tipo_counts.items():
        print(f'   • {tipo}: {count} intervenciones')
    
    print(f'\n🏛️ Dependencias SIN coordenadas (top 10):')
    dep_counts = sin_coords['dependencia'].value_counts().head(10)
    for dep, count in dep_counts.items():
        print(f'   • {dep}: {count} intervenciones')

if __name__ == '__main__':
    main()
