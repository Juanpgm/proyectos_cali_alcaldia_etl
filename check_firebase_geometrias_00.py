# -*- coding: utf-8 -*-
"""
Script para verificar geometrías con coordenadas 0,0 en Firebase.
"""

import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from pathlib import Path

def initialize_firebase():
    """Inicializar conexión a Firebase."""
    current_dir = Path(__file__).parent
    cred_path = current_dir / 'target-credentials.json'
    
    if not firebase_admin._apps:
        cred = credentials.Certificate(str(cred_path))
        firebase_admin.initialize_app(cred)
    
    return firestore.client()

def check_geometrias_00():
    """Verificar geometrías con coordenadas 0,0 en Firebase."""
    
    print("🔍 VERIFICACIÓN DE GEOMETRÍAS 0,0 EN FIREBASE")
    print("="*80)
    
    db = initialize_firebase()
    
    # Obtener colección unidades_proyecto
    unidades_ref = db.collection('unidades_proyecto')
    docs = unidades_ref.stream()
    
    registros_00 = []
    registros_validos = []
    registros_sin_geometry = []
    registros_lat_lon_00 = []
    total = 0
    
    for doc in docs:
        total += 1
        data = doc.to_dict()
        
        upid = data.get('upid', doc.id)
        nombre = data.get('nombre_up', 'Sin nombre')
        geometry = data.get('geometry')
        lat = data.get('lat')
        lon = data.get('lon')
        
        # Verificar si lat/lon están en 0
        if lat == 0 or lon == 0:
            registros_lat_lon_00.append({
                'upid': upid,
                'nombre': nombre[:50],
                'lat': lat,
                'lon': lon,
                'tiene_geometry': geometry is not None
            })
        
        # Verificar geometry
        if geometry is None:
            registros_sin_geometry.append({
                'upid': upid,
                'nombre': nombre[:50],
                'lat': lat,
                'lon': lon
            })
        elif isinstance(geometry, dict):
            coords = geometry.get('coordinates', [])
            if len(coords) >= 2:
                lon_geom, lat_geom = coords[0], coords[1]
                
                if lon_geom == 0 and lat_geom == 0:
                    registros_00.append({
                        'upid': upid,
                        'nombre': nombre[:50],
                        'lat_field': lat,
                        'lon_field': lon,
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
    print(f"\n📊 Total de documentos: {total}")
    print(f"✅ Registros con geometría válida: {len(registros_validos)}")
    print(f"⚠️  Registros sin geometry: {len(registros_sin_geometry)}")
    print(f"❌ Registros con geometry en 0,0: {len(registros_00)}")
    print(f"❌ Registros con lat/lon en 0: {len(registros_lat_lon_00)}")
    
    # Mostrar registros con geometry 0,0
    if registros_00:
        print("\n" + "="*80)
        print("REGISTROS CON GEOMETRY EN 0,0")
        print("="*80)
        
        df_00 = pd.DataFrame(registros_00)
        print(f"\n{df_00.to_string(index=False)}")
        
        # Análisis de lat/lon fields
        print("\n" + "-"*80)
        print("ANÁLISIS DE CAMPOS lat/lon")
        print("-"*80)
        
        print(f"lat field = None: {df_00['lat_field'].isna().sum()}")
        print(f"lon field = None: {df_00['lon_field'].isna().sum()}")
        print(f"lat field = 0: {(df_00['lat_field'] == 0).sum()}")
        print(f"lon field = 0: {(df_00['lon_field'] == 0).sum()}")
        
        print(f"\nValores únicos de lat_field: {df_00['lat_field'].unique()}")
        print(f"Valores únicos de lon_field: {df_00['lon_field'].unique()}")
    
    # Mostrar registros con lat/lon en 0
    if registros_lat_lon_00:
        print("\n" + "="*80)
        print(f"REGISTROS CON lat/lon FIELDS EN 0 ({len(registros_lat_lon_00)} registros)")
        print("="*80)
        
        df_lat_lon_00 = pd.DataFrame(registros_lat_lon_00)
        print(f"\n{df_lat_lon_00.head(20).to_string(index=False)}")
        
        # Analizar casos
        tiene_geometry = df_lat_lon_00['tiene_geometry'].sum()
        print(f"\nDe estos registros con lat/lon=0:")
        print(f"  - Con geometry: {tiene_geometry}")
        print(f"  - Sin geometry: {len(registros_lat_lon_00) - tiene_geometry}")
    
    # Mostrar registros sin geometry
    if registros_sin_geometry:
        print("\n" + "="*80)
        print(f"REGISTROS SIN GEOMETRY ({len(registros_sin_geometry)} registros)")
        print("="*80)
        
        df_sin_geom = pd.DataFrame(registros_sin_geometry)
        print(f"\n{df_sin_geom.head(10).to_string(index=False)}")
        
        # Analizar lat/lon
        lat_validos = ((df_sin_geom['lat'] > 3.0) & (df_sin_geom['lat'] < 4.0)).sum()
        lon_validos = ((df_sin_geom['lon'] > -77.0) & (df_sin_geom['lon'] < -76.0)).sum()
        
        print(f"\nDe estos registros sin geometry:")
        print(f"  - Con lat válido (3.0-4.0): {lat_validos}")
        print(f"  - Con lon válido (-77.0 a -76.0): {lon_validos}")
    
    # Análisis de muestra de válidos
    if registros_validos:
        print("\n" + "="*80)
        print(f"MUESTRA DE REGISTROS VÁLIDOS (primeros 5 de {len(registros_validos)})")
        print("="*80)
        
        df_validos = pd.DataFrame(registros_validos)
        print(f"\n{df_validos.head(5).to_string(index=False)}")

if __name__ == "__main__":
    try:
        check_geometrias_00()
        
        print("\n" + "="*80)
        print("✅ Verificación completada")
        print("="*80)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
