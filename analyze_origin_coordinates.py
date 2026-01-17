# -*- coding: utf-8 -*-
"""
Script para analizar coordenadas en los archivos Excel de origen
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extraction_app.data_extraction_unidades_proyecto import extract_and_save_unidades_proyecto
import pandas as pd

def analyze_origin_coordinates():
    """Analiza las coordenadas usando los datos extraídos"""
    
    print("="*80)
    print("ANÁLISIS DE COORDENADAS EN DATOS EXTRAÍDOS")
    print("="*80)
    
    # Extraer datos
    print("\n1. Extrayendo datos de Google Drive...")
    df = extract_and_save_unidades_proyecto()
    
    if df is None or df.empty:
        print("[ERROR] No se pudieron extraer datos")
        return
    
    print(f"[OK] Datos extraídos: {len(df)} registros")
    
    # Analizar coordenadas
    total_registros = len(df)
    
    # Verificar si existen las columnas
    if 'lat' not in df.columns or 'lon' not in df.columns:
        print("[ERROR] No se encontraron columnas 'lat' y 'lon'")
        print(f"Columnas disponibles: {list(df.columns)}")
        return
    
    # Normalizar a numérico
    df['lat_num'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon_num'] = pd.to_numeric(df['lon'], errors='coerce')
    
    # Estadísticas básicas
    total_con_lat = df['lat_num'].notna().sum()
    total_con_lon = df['lon_num'].notna().sum()
    total_con_ambos = ((df['lat_num'].notna()) & (df['lon_num'].notna())).sum()
    
    print(f"\n2. Estadísticas de coordenadas:")
    print(f"   Total registros: {total_registros}")
    print(f"   Con lat: {total_con_lat} ({total_con_lat*100/total_registros:.1f}%)")
    print(f"   Con lon: {total_con_lon} ({total_con_lon*100/total_registros:.1f}%)")
    print(f"   Con ambos: {total_con_ambos} ({total_con_ambos*100/total_registros:.1f}%)")
    
    # Rangos de coordenadas
    if total_con_ambos > 0:
        lats_valid = df['lat_num'].dropna()
        lons_valid = df['lon_num'].dropna()
        
        print(f"\n3. Rangos de coordenadas:")
        print(f"   Latitud: min={lats_valid.min():.6f}, max={lats_valid.max():.6f}")
        print(f"   Longitud: min={lons_valid.min():.6f}, max={lons_valid.max():.6f}")
        
        # Validar rango Cali (2.5-4.5, -77.5 a -75.5)
        lat_en_rango = ((df['lat_num'] >= 2.5) & (df['lat_num'] <= 4.5)).sum()
        lon_en_rango = ((df['lon_num'] >= -77.5) & (df['lon_num'] <= -75.5)).sum()
        pares_en_rango = ((df['lat_num'] >= 2.5) & (df['lat_num'] <= 4.5) & 
                         (df['lon_num'] >= -77.5) & (df['lon_num'] <= -75.5)).sum()
        
        print(f"\n4. Validación de rangos (Cali: lat 2.5-4.5, lon -77.5 a -75.5):")
        print(f"   Latitudes en rango: {lat_en_rango}/{total_con_lat} ({lat_en_rango*100/total_con_lat:.1f}%)")
        print(f"   Longitudes en rango: {lon_en_rango}/{total_con_lon} ({lon_en_rango*100/total_con_lon:.1f}%)")
        print(f"   Pares válidos: {pares_en_rango}/{total_con_ambos} ({pares_en_rango*100/total_con_ambos:.1f}%)")
        
        # Analizar registros fuera de rango
        fuera_rango = df[(df['lat_num'].notna()) & (df['lon_num'].notna()) & 
                        ~((df['lat_num'] >= 2.5) & (df['lat_num'] <= 4.5) & 
                          (df['lon_num'] >= -77.5) & (df['lon_num'] <= -75.5))]
        
        if len(fuera_rango) > 0:
            print(f"\n5. Registros fuera de rango de Cali ({len(fuera_rango)} registros):")
            print(f"   Ejemplos:")
            for idx, row in fuera_rango.head(10).iterrows():
                print(f"     lat: {row['lat_num']:.6f}, lon: {row['lon_num']:.6f}")
                if 'nombre_corto' in row:
                    print(f"     Nombre: {row['nombre_corto']}")
                if 'nombre_centro_gestor' in row:
                    print(f"     Centro: {row['nombre_centro_gestor']}")
                print()
    
    # Resumen general
    print("\n" + "="*80)
    print("RESUMEN GENERAL")
    print("="*80)
    print(f"Total registros: {total_registros}")
    print(f"Con lat: {total_con_lat} ({total_con_lat*100/total_registros:.1f}%)")
    print(f"Con lon: {total_con_lon} ({total_con_lon*100/total_registros:.1f}%)")
    print(f"Con ambos (lat y lon): {total_con_ambos} ({total_con_ambos*100/total_registros:.1f}%)")
    print(f"Coordenadas válidas (rango Cali): {total_pares_validos} ({total_pares_validos*100/total_registros:.1f}%)")
    
    # Archivos con menor cobertura
    print("\n" + "="*80)
    print("ARCHIVOS CON MENOR COBERTURA DE COORDENADAS")
    print("="*80)
    detalles_ordenados = sorted(detalles_por_archivo, key=lambda x: x['porcentaje_validos'])
    for detalle in detalles_ordenados[:5]:
        print(f"\n{detalle['archivo'][:50]}:")
        print(f"  Registros: {detalle['registros']}")
        print(f"  Con ambos: {detalle['con_ambos']} ({detalle['porcentaje_con_ambos']:.1f}%)")
        print(f"  Válidos: {detalle['pares_validos']} ({detalle['porcentaje_validos']:.1f}%)")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    analyze_origin_coordinates()
