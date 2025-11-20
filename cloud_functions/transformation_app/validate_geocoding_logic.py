# -*- coding: utf-8 -*-
"""
Script de validación de la lógica de reverse geocoding

Valida:
1. Exclusión de registros con geometry nulo/REVISAR
2. Correcta extracción de valores de la API (neighborhood y sublocality)
3. Filtrado por columna 'corregir'

Uso:
    python validate_geocoding_logic.py

Author: AI Assistant
Version: 1.0
"""

import os
import sys
import json
from pathlib import Path
import pandas as pd

# Cargar variables de entorno
try:
    from dotenv import load_dotenv
    project_root = Path(__file__).parent.parent
    env_local = project_root / '.env.local'
    env_prod = project_root / '.env.prod'
    
    if env_local.exists():
        load_dotenv(env_local)
        print(f"✓ Variables de entorno cargadas desde: {env_local.name}\n")
    elif env_prod.exists():
        load_dotenv(env_prod)
        print(f"✓ Variables de entorno cargadas desde: {env_prod.name}\n")
except ImportError:
    pass

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import googlemaps

def validate_geocoding_logic():
    """Validar la lógica completa de reverse geocoding."""
    
    print("="*80)
    print("VALIDACIÓN DE LÓGICA DE REVERSE GEOCODING")
    print("="*80)
    
    # 1. Cargar datos
    gdf_path = Path(__file__).parent / 'app_outputs' / 'unidades_proyecto_outputs' / 'gdf_geolocalizar.xlsx'
    
    print(f"\n📂 PASO 1: CARGANDO DATOS")
    print(f"   Ruta: {gdf_path}")
    
    if not gdf_path.exists():
        print(f"   ❌ Archivo no encontrado")
        return
    
    df = pd.read_excel(gdf_path)
    print(f"   ✅ Cargados {len(df)} registros totales")
    
    # 2. Validar columnas necesarias
    print(f"\n📋 PASO 2: VALIDANDO COLUMNAS")
    required_columns = ['upid', 'geometry', 'corregir', 'barrio_vereda', 'comuna_corregimiento']
    
    for col in required_columns:
        if col in df.columns:
            print(f"   ✅ Columna '{col}' presente")
        else:
            print(f"   ❌ Columna '{col}' NO encontrada")
            return
    
    # 3. Analizar filtro por 'corregir'
    print(f"\n🔍 PASO 3: ANALIZANDO FILTRO 'corregir'")
    corregir_counts = df['corregir'].value_counts()
    print(f"   Distribución de valores en 'corregir':")
    for value, count in corregir_counts.items():
        print(f"      - '{value}': {count} registros")
    
    df_to_process = df[df['corregir'] == 'INTENTAR GEORREFERENCIAR'].copy()
    print(f"\n   📍 Registros que cumplen filtro 'INTENTAR GEORREFERENCIAR': {len(df_to_process)}")
    
    # 4. Analizar geometrías
    print(f"\n🌐 PASO 4: ANALIZANDO GEOMETRÍAS")
    
    # Analizar valores en geometry
    geometry_null = df_to_process['geometry'].isna().sum()
    geometry_revisar = (df_to_process['geometry'] == 'REVISAR').sum()
    geometry_error = (df_to_process['geometry'] == 'ERROR').sum()
    geometry_valid = len(df_to_process) - geometry_null - geometry_revisar - geometry_error
    
    print(f"   Análisis de geometrías en registros a procesar:")
    print(f"      - Nulos (NaN/None): {geometry_null}")
    print(f"      - 'REVISAR': {geometry_revisar}")
    print(f"      - 'ERROR': {geometry_error}")
    print(f"      - Válidas (JSON): {geometry_valid}")
    
    # 5. Validar exclusión
    print(f"\n🚫 PASO 5: VALIDANDO LÓGICA DE EXCLUSIÓN")
    
    excluded_count = geometry_null + geometry_revisar + geometry_error
    processable_count = geometry_valid
    
    print(f"   ❌ Registros que DEBEN SER EXCLUIDOS: {excluded_count}")
    print(f"   ✅ Registros que DEBEN SER PROCESADOS: {processable_count}")
    
    # 6. Verificar geometrías válidas
    print(f"\n📍 PASO 6: VERIFICANDO PARSEO DE GEOMETRÍAS VÁLIDAS")
    
    valid_geometries = []
    invalid_geometries = []
    
    for idx, row in df_to_process.head(20).iterrows():  # Solo primeros 20 para validación
        geometry = row['geometry']
        
        # Aplicar la misma lógica que en google_maps_geocoder.py
        if pd.isna(geometry) or not geometry or geometry in ['ERROR', 'REVISAR', 'null', 'NULL']:
            invalid_geometries.append({
                'upid': row['upid'],
                'reason': 'Excluido (nulo/ERROR/REVISAR)'
            })
            continue
        
        # Intentar parsear JSON
        if isinstance(geometry, str):
            try:
                geom_obj = json.loads(geometry)
                if geom_obj and 'coordinates' in geom_obj:
                    coords = geom_obj['coordinates']
                    if isinstance(coords, list) and len(coords) >= 2:
                        lon, lat = coords[0], coords[1]
                        valid_geometries.append({
                            'upid': row['upid'],
                            'lat': lat,
                            'lon': lon,
                            'nombre': row.get('nombre_up', 'N/A')
                        })
                    else:
                        invalid_geometries.append({
                            'upid': row['upid'],
                            'reason': 'Coordenadas inválidas'
                        })
                else:
                    invalid_geometries.append({
                        'upid': row['upid'],
                        'reason': 'Sin campo coordinates'
                    })
            except json.JSONDecodeError:
                invalid_geometries.append({
                    'upid': row['upid'],
                    'reason': 'Error parseando JSON'
                })
        else:
            invalid_geometries.append({
                'upid': row['upid'],
                'reason': 'Geometry no es string'
            })
    
    print(f"   ✅ Geometrías válidas (parseables): {len(valid_geometries)}")
    print(f"   ❌ Geometrías inválidas/excluidas: {len(invalid_geometries)}")
    
    if valid_geometries:
        print(f"\n   📊 Muestra de geometrías válidas (primeras 5):")
        for geom in valid_geometries[:5]:
            print(f"      {geom['upid']}: ({geom['lat']:.6f}, {geom['lon']:.6f}) - {geom['nombre']}")
    
    if invalid_geometries:
        print(f"\n   ⚠️  Muestra de geometrías excluidas (primeras 5):")
        for geom in invalid_geometries[:5]:
            print(f"      {geom['upid']}: {geom['reason']}")
    
    # 7. Probar API con un registro válido
    if valid_geometries:
        print(f"\n🌍 PASO 7: PROBANDO EXTRACCIÓN DE API CON REGISTRO REAL")
        
        api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        if not api_key:
            print(f"   ❌ GOOGLE_MAPS_API_KEY no configurado")
            return
        
        test_geom = valid_geometries[0]
        print(f"\n   📍 Registro de prueba:")
        print(f"      UPID: {test_geom['upid']}")
        print(f"      Nombre: {test_geom['nombre']}")
        print(f"      Coordenadas: ({test_geom['lat']}, {test_geom['lon']})")
        
        # Inicializar cliente
        gmaps = googlemaps.Client(key=api_key)
        
        # Hacer llamada
        print(f"\n   🔄 Llamando a Google Maps API...")
        try:
            results = gmaps.reverse_geocode((test_geom['lat'], test_geom['lon']))
            
            if results:
                print(f"   ✅ API respondió con {len(results)} resultados")
                
                # Extraer valores según la lógica configurada
                print(f"\n   🔍 EXTRAYENDO VALORES:")
                
                # Buscar 'neighborhood' para comuna_corregimiento_val_s3
                comuna_found = None
                barrio_found = None
                
                for result in results:
                    address_components = result.get('address_components', [])
                    
                    # Buscar neighborhood
                    if not comuna_found:
                        for component in address_components:
                            types = component.get('types', [])
                            if 'neighborhood' in types:
                                comuna_found = component.get('long_name')
                                break
                    
                    # Buscar sublocality
                    if not barrio_found:
                        for component in address_components:
                            types = component.get('types', [])
                            if 'sublocality' in types:
                                barrio_found = component.get('long_name')
                                break
                    
                    if comuna_found and barrio_found:
                        break
                
                print(f"\n   📊 VALORES EXTRAÍDOS:")
                print(f"      barrio_vereda_val_s3 (de 'sublocality'): {barrio_found or 'NO ENCONTRADO'}")
                print(f"      comuna_corregimiento_val_s3 (de 'neighborhood'): {comuna_found or 'NO ENCONTRADO'}")
                
                # Comparar con valores originales
                original_row = df[df['upid'] == test_geom['upid']].iloc[0]
                print(f"\n   📋 COMPARACIÓN CON VALORES ORIGINALES:")
                print(f"      Barrio/Vereda original: {original_row.get('barrio_vereda', 'N/A')}")
                print(f"      Barrio/Vereda extraído: {barrio_found or 'NO ENCONTRADO'}")
                print(f"      Comuna/Corregimiento original: {original_row.get('comuna_corregimiento', 'N/A')}")
                print(f"      Comuna/Corregimiento extraído: {comuna_found or 'NO ENCONTRADO'}")
                
            else:
                print(f"   ⚠️  API no devolvió resultados")
                
        except Exception as e:
            print(f"   ❌ Error en llamada API: {e}")
            import traceback
            traceback.print_exc()
    
    # 8. Resumen final
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN DE VALIDACIÓN")
    print(f"{'='*80}")
    print(f"✅ Total de registros: {len(df)}")
    print(f"✅ Filtrados por 'corregir': {len(df_to_process)}")
    print(f"❌ Con geometry nulo/REVISAR/ERROR: {excluded_count}")
    print(f"✅ Con geometry válida (procesables): {processable_count}")
    print(f"\n💡 La función procesará {processable_count} registros de {len(df_to_process)} filtrados")
    print(f"   (excluyendo {excluded_count} sin geometría válida)")

if __name__ == '__main__':
    validate_geocoding_logic()
