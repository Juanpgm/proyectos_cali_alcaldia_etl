# -*- coding: utf-8 -*-
"""
Script para agregar función de extracción de lat/lon desde geometría existente.
Este módulo corrige el problema donde las geometrías existen en el GeoJSON
pero los campos lat/lon no están en las propiedades.
"""

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from typing import Optional


def extract_lat_lon_from_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Extrae campos lat/lon desde la geometría existente si no están presentes.
    
    Esta función es crítica para el pipeline porque algunos GeoJSON tienen geometría
    pero no tienen lat/lon explícitos en las propiedades.
    
    Args:
        gdf: GeoDataFrame con geometría potencialmente sin lat/lon
        
    Returns:
        GeoDataFrame con lat/lon extraídos de la geometría
    """
    result_gdf = gdf.copy()
    
    # Verificar si ya existen las columnas
    has_lat = 'lat' in result_gdf.columns
    has_lon = 'lon' in result_gdf.columns
    
    if has_lat and has_lon:
        # Verificar cuántos valores son nulos
        null_lat = result_gdf['lat'].isna().sum()
        null_lon = result_gdf['lon'].isna().sum()
        
        if null_lat == 0 and null_lon == 0:
            print("[OK] lat/lon ya existen y están completos")
            return result_gdf
        
        print(f"[INFO] Encontrados {null_lat} lat nulos y {null_lon} lon nulos")
    
    # Crear columnas si no existen
    if not has_lat:
        result_gdf['lat'] = None
    if not has_lon:
        result_gdf['lon'] = None
    
    # Extraer coordenadas de la geometría
    extracted_count = 0
    for idx in result_gdf.index:
        # Solo extraer si lat o lon son nulos/vacíos
        lat = result_gdf.at[idx, 'lat']
        lon = result_gdf.at[idx, 'lon']
        
        # Verificar si necesita extracción
        needs_extraction = (
            pd.isna(lat) or lat is None or 
            pd.isna(lon) or lon is None
        )
        
        if needs_extraction and 'geometry' in result_gdf.columns:
            geom = result_gdf.at[idx, 'geometry']
            
            if geom is not None and not pd.isna(geom):
                # Intentar extraer coordenadas según el tipo de geometría
                try:
                    if hasattr(geom, 'geom_type'):
                        if geom.geom_type == 'Point':
                            # Para Point: coordenadas directas
                            result_gdf.at[idx, 'lon'] = round(geom.x, 10)
                            result_gdf.at[idx, 'lat'] = round(geom.y, 10)
                            extracted_count += 1
                        elif geom.geom_type in ['LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon']:
                            # Para geometrías complejas: usar centroide
                            centroid = geom.centroid
                            result_gdf.at[idx, 'lon'] = round(centroid.x, 10)
                            result_gdf.at[idx, 'lat'] = round(centroid.y, 10)
                            extracted_count += 1
                except Exception as e:
                    print(f"[WARNING] Error extrayendo coordenadas del índice {idx}: {e}")
    
    print(f"[OK] Extraídas coordenadas de geometría para {extracted_count} registros")
    
    # Validar que las coordenadas están en rango válido para Cali
    valid_lat = (result_gdf['lat'].notna()) & (result_gdf['lat'] >= 3.0) & (result_gdf['lat'] <= 4.0)
    valid_lon = (result_gdf['lon'].notna()) & (result_gdf['lon'] >= -77.0) & (result_gdf['lon'] <= -76.0)
    valid_coords = valid_lat & valid_lon
    
    print(f"[OK] Coordenadas válidas: {valid_coords.sum()} de {len(result_gdf)} ({valid_coords.sum()/len(result_gdf)*100:.1f}%)")
    
    return result_gdf


def test_extraction():
    """Prueba la función con el archivo de descarga."""
    import json
    
    print("="*60)
    print("TEST: Extracción de lat/lon desde geometría")
    print("="*60)
    
    # Leer el GeoJSON
    with open('context/unidades_proyecto_descarga.geojson', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Convertir a GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(data['features'], crs='EPSG:4326')
    
    print(f"\nAntes de la extracción:")
    print(f"  Total features: {len(gdf)}")
    print(f"  Columnas: {list(gdf.columns[:10])}")
    print(f"  ¿Tiene 'lat'? {'lat' in gdf.columns}")
    print(f"  ¿Tiene 'lon'? {'lon' in gdf.columns}")
    print(f"  ¿Tiene 'geometry'? {'geometry' in gdf.columns}")
    
    if 'geometry' in gdf.columns:
        valid_geom = gdf['geometry'].notna().sum()
        print(f"  Geometrías válidas: {valid_geom}")
    
    # Aplicar extracción
    gdf_with_coords = extract_lat_lon_from_geometry(gdf)
    
    print(f"\nDespués de la extracción:")
    print(f"  ¿Tiene 'lat'? {'lat' in gdf_with_coords.columns}")
    print(f"  ¿Tiene 'lon'? {'lon' in gdf_with_coords.columns}")
    
    if 'lat' in gdf_with_coords.columns:
        valid_lat = gdf_with_coords['lat'].notna().sum()
        print(f"  lat válidos: {valid_lat}")
    
    if 'lon' in gdf_with_coords.columns:
        valid_lon = gdf_with_coords['lon'].notna().sum()
        print(f"  lon válidos: {valid_lon}")
    
    # Verificar UNP-11 específicamente
    if 'upid' in gdf_with_coords.columns:
        unp11 = gdf_with_coords[gdf_with_coords['upid'] == 'UNP-11']
        if len(unp11) > 0:
            print(f"\nUNP-11 después de extracción:")
            print(f"  lat: {unp11.iloc[0]['lat']}")
            print(f"  lon: {unp11.iloc[0]['lon']}")
            print(f"  geometry: {unp11.iloc[0]['geometry']}")


if __name__ == "__main__":
    test_extraction()
