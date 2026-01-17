# -*- coding: utf-8 -*-
"""
Script para diagnosticar coordenadas en extracción
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extraction_app.data_extraction_unidades_proyecto import extract_and_save_unidades_proyecto

def main():
    print("="*80)
    print("DIAGNÓSTICO DE COORDENADAS EN EXTRACCIÓN")
    print("="*80)
    
    # Extraer datos
    df = extract_and_save_unidades_proyecto()
    
    print(f"\n[STATS] Después de extracción:")
    print(f"  Total registros: {len(df)}")
    print(f"  Con lat: {df['lat'].notna().sum()} ({df['lat'].notna().sum()*100/len(df):.1f}%)")
    print(f"  Con lon: {df['lon'].notna().sum()} ({df['lon'].notna().sum()*100/len(df):.1f}%)")
    
    # Registros sin coordenadas
    sin_coords = df[(df['lat'].isna()) | (df['lon'].isna())]
    print(f"\n[WARNING] Registros sin coordenadas: {len(sin_coords)}")
    
    if len(sin_coords) > 0:
        print("\n[DETAIL] Ejemplos (primeros 10):")
        for idx, row in sin_coords.head(10).iterrows():
            print(f"  - {row.get('nombre_corto', 'N/A')}")
            print(f"    Centro: {row.get('nombre_centro_gestor', 'N/A')}")
            print(f"    lat: {row.get('lat')}, lon: {row.get('lon')}")
    
    # Ver valores de lat/lon para ver si hay problemas
    print(f"\n[INFO] Estadísticas de lat:")
    print(df['lat'].describe())
    
    print(f"\n[INFO] Estadísticas de lon:")
    print(df['lon'].describe())
    
    print("\n" + "="*80)

if __name__ == "__main__":
    main()
