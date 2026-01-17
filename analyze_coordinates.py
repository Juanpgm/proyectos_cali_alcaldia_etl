# -*- coding: utf-8 -*-
"""
Script para diagnosticar por qué se rechazan coordenadas en la transformación
"""
import os
import sys
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Simular la lógica de validación de coordenadas
def analyze_coordinates():
    """Analiza las coordenadas del archivo JSON extraído"""
    
    json_file = "unidades_proyecto.json"
    
    if not os.path.exists(json_file):
        print(f"[ERROR] No existe {json_file}")
        print("[INFO] Ejecuta primero la extracción")
        return
    
    print("="*80)
    print("ANÁLISIS DE COORDENADAS")
    print("="*80)
    
    # Cargar datos
    df = pd.read_json(json_file)
    
    print(f"\n[DATA] Total registros: {len(df)}")
    print(f"[DATA] Con campo 'lat': {df['lat'].notna().sum()}")
    print(f"[DATA] Con campo 'lon': {df['lon'].notna().sum()}")
    
    # Analizar valores de lat
    print(f"\n[ANALYSIS] Análisis de latitudes:")
    lat_not_null = df[df['lat'].notna()]
    
    if len(lat_not_null) > 0:
        # Convertir a numérico
        def safe_float(val):
            try:
                return float(str(val).replace(',', '.').strip())
            except:
                return None
        
        lat_not_null['lat_numeric'] = lat_not_null['lat'].apply(safe_float)
        lat_numeric = lat_not_null['lat_numeric'].dropna()
        
        print(f"  Valores convertibles a float: {len(lat_numeric)}")
        print(f"  Mínimo: {lat_numeric.min()}")
        print(f"  Máximo: {lat_numeric.max()}")
        print(f"  Promedio: {lat_numeric.mean():.4f}")
        
        # Contar por rangos
        in_range = ((lat_numeric >= 2.5) & (lat_numeric <= 4.5)).sum()
        out_range = len(lat_numeric) - in_range
        
        print(f"  En rango válido (2.5-4.5): {in_range} ({in_range*100/len(lat_numeric):.1f}%)")
        print(f"  Fuera de rango: {out_range} ({out_range*100/len(lat_numeric):.1f}%)")
        
        if out_range > 0:
            out_of_range_values = lat_numeric[(lat_numeric < 2.5) | (lat_numeric > 4.5)]
            print(f"\n  [DETAIL] Ejemplos fuera de rango:")
            for val in out_of_range_values.head(10):
                print(f"    {val}")
    
    # Analizar valores de lon
    print(f"\n[ANALYSIS] Análisis de longitudes:")
    lon_not_null = df[df['lon'].notna()]
    
    if len(lon_not_null) > 0:
        lon_not_null['lon_numeric'] = lon_not_null['lon'].apply(safe_float)
        lon_numeric = lon_not_null['lon_numeric'].dropna()
        
        print(f"  Valores convertibles a float: {len(lon_numeric)}")
        print(f"  Mínimo: {lon_numeric.min()}")
        print(f"  Máximo: {lon_numeric.max()}")
        print(f"  Promedio: {lon_numeric.mean():.4f}")
        
        # Contar por rangos
        in_range = ((lon_numeric >= -77.5) & (lon_numeric <= -75.5)).sum()
        out_range = len(lon_numeric) - in_range
        
        print(f"  En rango válido (-77.5 a -75.5): {in_range} ({in_range*100/len(lon_numeric):.1f}%)")
        print(f"  Fuera de rango: {out_range} ({out_range*100/len(lon_numeric):.1f}%)")
        
        if out_range > 0:
            out_of_range_values = lon_numeric[(lon_numeric < -77.5) | (lon_numeric > -75.5)]
            print(f"\n  [DETAIL] Ejemplos fuera de rango:")
            for val in out_of_range_values.head(10):
                print(f"    {val}")
    
    # Verificar pares válidos
    print(f"\n[ANALYSIS] Pares de coordenadas:")
    both = df[(df['lat'].notna()) & (df['lon'].notna())]
    print(f"  Registros con ambas coordenadas: {len(both)}")
    
    if len(both) > 0:
        both['lat_numeric'] = both['lat'].apply(safe_float)
        both['lon_numeric'] = both['lon'].apply(safe_float)
        
        valid_pairs = both[
            (both['lat_numeric'].notna()) & 
            (both['lon_numeric'].notna()) &
            (both['lat_numeric'] >= 2.5) & 
            (both['lat_numeric'] <= 4.5) &
            (both['lon_numeric'] >= -77.5) & 
            (both['lon_numeric'] <= -75.5)
        ]
        
        print(f"  Pares válidos (en rango): {len(valid_pairs)} ({len(valid_pairs)*100/len(both):.1f}%)")
        print(f"  Pares inválidos: {len(both) - len(valid_pairs)} ({(len(both) - len(valid_pairs))*100/len(both):.1f}%)")
        
        # Mostrar ejemplos de inválidos
        if len(both) - len(valid_pairs) > 0:
            invalid = both[~both.index.isin(valid_pairs.index)]
            print(f"\n  [DETAIL] Ejemplos de pares inválidos:")
            for idx, row in invalid.head(10).iterrows():
                print(f"    Nombre: {row.get('nombre_corto', 'N/A')}")
                print(f"    Centro: {row.get('nombre_centro_gestor', 'N/A')}")
                print(f"    lat: {row.get('lat')} -> {row.get('lat_numeric')}")
                print(f"    lon: {row.get('lon')} -> {row.get('lon_numeric')}")
                print()
    
    print("="*80)

if __name__ == "__main__":
    analyze_coordinates()
