# -*- coding: utf-8 -*-
"""
Script de diagnóstico para identificar registros con geometría en 0,0
y determinar el origen del problema.
"""

import pandas as pd
import geopandas as gpd
import json
from pathlib import Path

def analyze_geojson_coordinates():
    """Analiza el GeoJSON para encontrar registros con coordenadas 0,0."""
    
    current_dir = Path(__file__).parent
    # Probar múltiples ubicaciones
    possible_files = [
        current_dir / 'app_outputs' / 'unidades_proyecto.geojson',
        current_dir / 'context' / 'unidades_proyecto.geojson',
        current_dir / 'test_outputs' / 'capas_epsg4326' / 'geojson' / 'Unidades_Proyecto_EPSG4326.geojson'
    ]
    
    geojson_file = None
    for file_path in possible_files:
        if file_path.exists():
            geojson_file = file_path
            break
    
    if geojson_file is None:
        print(f"❌ Archivo no encontrado en ninguna ubicación:")
        for f in possible_files:
            print(f"   - {f}")
        return
    
    print(f"📂 Analizando: {geojson_file}")
    print("="*80)
    
    # Cargar GeoJSON
    with open(geojson_file, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    
    features = geojson_data.get('features', [])
    print(f"\n📊 Total de features: {len(features)}")
    
    # Encontrar registros con 0,0
    registros_00 = []
    registros_validos = []
    registros_sin_geometry = []
    
    for idx, feature in enumerate(features):
        properties = feature.get('properties', {})
        geometry = feature.get('geometry')
        
        upid = properties.get('upid', f'Index-{idx}')
        nombre = properties.get('nombre_up', 'Sin nombre')
        lat = properties.get('lat')
        lon = properties.get('lon')
        
        if geometry is None:
            registros_sin_geometry.append({
                'upid': upid,
                'nombre': nombre,
                'lat': lat,
                'lon': lon
            })
        elif geometry.get('type') == 'Point':
            coords = geometry.get('coordinates', [])
            if len(coords) >= 2:
                lon_geom, lat_geom = coords[0], coords[1]
                
                if lon_geom == 0 and lat_geom == 0:
                    registros_00.append({
                        'upid': upid,
                        'nombre': nombre,
                        'lat_property': lat,
                        'lon_property': lon,
                        'lat_geometry': lat_geom,
                        'lon_geometry': lon_geom
                    })
                else:
                    registros_validos.append({
                        'upid': upid,
                        'lat': lat_geom,
                        'lon': lon_geom
                    })
    
    # Reportar resultados
    print(f"\n✅ Registros con geometría válida: {len(registros_validos)}")
    print(f"⚠️  Registros sin geometría: {len(registros_sin_geometry)}")
    print(f"❌ Registros con coordenadas 0,0: {len(registros_00)}")
    
    if registros_00:
        print("\n" + "="*80)
        print("REGISTROS CON COORDENADAS 0,0")
        print("="*80)
        
        df_00 = pd.DataFrame(registros_00)
        print(f"\n{df_00.to_string(index=False)}")
        
        # Análisis de lat/lon en properties
        print("\n" + "-"*80)
        print("ANÁLISIS DE lat/lon EN PROPERTIES")
        print("-"*80)
        
        lat_nulos = df_00['lat_property'].isna().sum()
        lon_nulos = df_00['lon_property'].isna().sum()
        lat_cero = (df_00['lat_property'] == 0).sum()
        lon_cero = (df_00['lon_property'] == 0).sum()
        
        print(f"lat property = None/NaN: {lat_nulos}")
        print(f"lon property = None/NaN: {lon_nulos}")
        print(f"lat property = 0: {lat_cero}")
        print(f"lon property = 0: {lon_cero}")
        
        # Mostrar valores únicos de lat/lon
        print(f"\nValores únicos de lat_property: {df_00['lat_property'].unique()}")
        print(f"Valores únicos de lon_property: {df_00['lon_property'].unique()}")
    
    if registros_sin_geometry:
        print("\n" + "="*80)
        print(f"REGISTROS SIN GEOMETRÍA ({len(registros_sin_geometry)} registros)")
        print("="*80)
        
        df_sin_geom = pd.DataFrame(registros_sin_geometry)
        print(f"\n{df_sin_geom.head(10).to_string(index=False)}")

def check_data_before_export():
    """Verifica los datos antes de la exportación."""
    
    # Esta función requiere que se ejecute desde el pipeline
    # Solo documenta lo que debería verificarse
    
    print("\n" + "="*80)
    print("RECOMENDACIONES DE VERIFICACIÓN EN EL PIPELINE")
    print("="*80)
    
    checks = [
        "1. Verificar valores de lat/lon después de convert_to_geodataframe()",
        "2. Verificar valores de lat/lon después de correct_coordinate_formats()",
        "3. Verificar geometrías después de create_final_geometry()",
        "4. Verificar que no se aplique fillna(0) a columnas lat/lon",
        "5. Verificar que fix_coordinate_format() retorne None y no 0 para valores inválidos",
        "6. Agregar logs para rastrear cuándo lat/lon se convierten en 0"
    ]
    
    for check in checks:
        print(f"   {check}")

if __name__ == "__main__":
    print("🔍 DIAGNÓSTICO DE GEOMETRÍAS 0,0")
    print("="*80)
    
    analyze_geojson_coordinates()
    check_data_before_export()
    
    print("\n" + "="*80)
    print("✅ Diagnóstico completado")
    print("="*80)
