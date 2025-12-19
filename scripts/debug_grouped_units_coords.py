#!/usr/bin/env python3
"""
Debug script: Analizar por qué las unidades agrupadas no tienen coordenadas
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
    
    # Revisar algunos nombres específicos de las unidades agrupadas
    problematic_names = [
        "I.E. Luis Fernando Caicedo",
        "Teatro Municipal Enrique Buenaventura",
        "Biblioteca Pública Centro Cultural Nuevo Latir",
        "Parque Parroquia Santa Teresa de Jesús"
    ]
    
    print(f"\n🔍 Analizando unidades problemáticas:\n")
    
    for name in problematic_names:
        # Buscar registros con este nombre (usando contains)
        mask = df['nombre_up'].str.contains(name, case=False, na=False)
        matches = df[mask]
        
        if len(matches) > 0:
            print(f"📍 {name}:")
            print(f"   • Total intervenciones encontradas: {len(matches)}")
            print(f"   • Con lat: {matches['lat'].notna().sum()}")
            print(f"   • Con lon: {matches['lon'].notna().sum()}")
            
            # Mostrar muestra de coordenadas
            coords_sample = matches[['nombre_up', 'lat', 'lon']].head(3)
            print(f"   • Muestra de coordenadas:")
            for idx, row in coords_sample.iterrows():
                lat_val = f"{row['lat']:.6f}" if pd.notna(row['lat']) else "NULL"
                lon_val = f"{row['lon']:.6f}" if pd.notna(row['lon']) else "NULL"
                print(f"      - {row['nombre_up'][:50]}: lat={lat_val}, lon={lon_val}")
            print()
        else:
            print(f"⚠️  {name}: NO ENCONTRADO\n")
    
    # Análisis general de tipos de equipamiento agrupados vs subsidios
    print("\n" + "="*80)
    print("📊 ANÁLISIS GENERAL: Coordenadas por tipo de fuente")
    print("="*80 + "\n")
    
    # Identificar subsidios
    subsidio_keywords = ['subsidio', 'mejoramiento de vivienda', 'lote con servicios']
    subsidio_mask = df['tipo_intervencion'].str.contains('|'.join(subsidio_keywords), case=False, na=False)
    
    df_subsidios = df[subsidio_mask]
    df_otros = df[~subsidio_mask]
    
    print(f"💰 Subsidios:")
    print(f"   • Total: {len(df_subsidios)}")
    print(f"   • Con coordenadas: {(df_subsidios['lat'].notna() & df_subsidios['lon'].notna()).sum()}")
    print(f"   • Sin coordenadas: {(df_subsidios['lat'].isna() | df_subsidios['lon'].isna()).sum()}")
    
    print(f"\n🏗️  Otros (agrupables):")
    print(f"   • Total: {len(df_otros)}")
    print(f"   • Con coordenadas: {(df_otros['lat'].notna() & df_otros['lon'].notna()).sum()}")
    print(f"   • Sin coordenadas: {(df_otros['lat'].isna() | df_otros['lon'].isna()).sum()}")
    
    # Encontrar los casos sin coordenadas en agrupables
    sin_coords = df_otros[df_otros['lat'].isna() | df_otros['lon'].isna()]
    print(f"\n🚫 Agrupables SIN coordenadas ({len(sin_coords)}):")
    print(sin_coords[['nombre_up', 'tipo_equipamiento', 'tipo_intervencion']].head(10))

if __name__ == '__main__':
    main()
