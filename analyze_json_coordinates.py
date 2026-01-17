# -*- coding: utf-8 -*-
"""
Analizar coordenadas directamente del JSON extraído
"""
import json
import pandas as pd

def analyze_extracted_json():
    """Analiza coordenadas del JSON ya extraído"""
    
    print("="*80)
    print("ANÁLISIS DETALLADO DE COORDENADAS EN JSON EXTRAÍDO")
    print("="*80)
    
    # Leer JSON
    json_path = 'cloud_functions/transformation_app/app_inputs/unidades_proyecto_input/unidades_proyecto.json'
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    print(f"\n1. Registros totales: {len(df)}")
    
    # Analizar coordenadas
    print(f"\n2. Análisis de coordenadas:")
    
    # Convertir a numérico para análisis
    df['lat_num'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon_num'] = pd.to_numeric(df['lon'], errors='coerce')
    
    total_con_ambos = ((df['lat_num'].notna()) & (df['lon_num'].notna())).sum()
    print(f"   Registros con lat y lon: {total_con_ambos} ({total_con_ambos*100/len(df):.1f}%)")
    
    # Rangos Cali
    en_rango = ((df['lat_num'] >= 2.5) & (df['lat_num'] <= 4.5) & 
               (df['lon_num'] >= -77.5) & (df['lon_num'] <= -75.5)).sum()
    
    print(f"   Pares en rango Cali: {en_rango} ({en_rango*100/total_con_ambos:.1f}%)")
    
    # Analizar fuera de rango
    fuera_rango = df[(df['lat_num'].notna()) & (df['lon_num'].notna()) & 
                    ~((df['lat_num'] >= 2.5) & (df['lat_num'] <= 4.5) & 
                      (df['lon_num'] >= -77.5) & (df['lon_num'] <= -75.5))]
    
    print(f"\n3. Registros fuera de rango: {len(fuera_rango)}")
    
    if len(fuera_rango) > 0:
        # Agrupar por Centro Gestor
        por_centro = fuera_rango.groupby('nombre_centro_gestor').size().sort_values(ascending=False)
        
        print(f"\n4. Distribución por Centro Gestor:")
        for centro, count in por_centro.items():
            print(f"   - {centro}: {count} registros")
        
        # Analizar patrones de valores
        print(f"\n5. Patrones de valores fuera de rango:")
        
        # Latitudes
        lats_fuera = fuera_rango['lat_num'].dropna()
        print(f"\n   Latitudes fuera de rango:")
        print(f"   - Min: {lats_fuera.min()}")
        print(f"   - Max: {lats_fuera.max()}")
        print(f"   - Promedio: {lats_fuera.mean()}")
        
        # Detectar patrones
        lats_muy_grandes = (lats_fuera > 100).sum()
        lats_formato_dms = ((lats_fuera > 10) & (lats_fuera < 100)).sum()
        
        print(f"   - Valores > 100 (posiblemente mal formato): {lats_muy_grandes}")
        print(f"   - Valores 10-100 (posible DMS): {lats_formato_dms}")
        
        # Longitudes
        lons_fuera = fuera_rango['lon_num'].dropna()
        print(f"\n   Longitudes fuera de rango:")
        print(f"   - Min: {lons_fuera.min()}")
        print(f"   - Max: {lons_fuera.max()}")
        print(f"   - Promedio: {lons_fuera.mean()}")
        
        lons_muy_grandes = (abs(lons_fuera) > 180).sum()
        lons_formato_dms = ((abs(lons_fuera) > 100) & (abs(lons_fuera) < 180)).sum()
        
        print(f"   - Valores > |180| (posiblemente mal formato): {lons_muy_grandes}")
        print(f"   - Valores 100-180 (posible DMS): {lons_formato_dms}")
        
        # Mostrar ejemplos específicos
        print(f"\n6. Ejemplos de coordenadas problemáticas:")
        
        # Ordenar por magnitud de lat
        ejemplos = fuera_rango.nlargest(15, 'lat_num')[['lat', 'lon', 'nombre_corto', 'nombre_centro_gestor']]
        
        for idx, row in ejemplos.iterrows():
            print(f"\n   Nombre: {row['nombre_corto'][:60]}")
            print(f"   Centro: {row['nombre_centro_gestor']}")
            print(f"   lat original: {row['lat']}")
            print(f"   lon original: {row['lon']}")
            print(f"   lat numérico: {row['lat']}")
            print(f"   lon numérico: {row['lon']}")


if __name__ == '__main__':
    analyze_extracted_json()
