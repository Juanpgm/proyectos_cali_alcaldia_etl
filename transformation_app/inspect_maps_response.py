# -*- coding: utf-8 -*-
"""
Script para inspeccionar la respuesta completa de Google Maps API

Muestra todos los valores disponibles en la respuesta de reverse geocoding
para un registro específico.

Uso:
    python inspect_maps_response.py

Author: AI Assistant
Version: 1.0
"""

import os
import sys
import json
from pathlib import Path

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

import pandas as pd
import googlemaps

def inspect_reverse_geocoding():
    """Inspeccionar respuesta completa de Google Maps API."""
    
    print("="*80)
    print("INSPECCIÓN DE RESPUESTA DE GOOGLE MAPS API - REVERSE GEOCODING")
    print("="*80)
    
    # Load gdf_geolocalizar
    gdf_path = Path(__file__).parent / 'app_outputs' / 'unidades_proyecto_outputs' / 'gdf_geolocalizar.xlsx'
    
    print(f"\n📂 Cargando datos desde: {gdf_path}")
    df = pd.read_excel(gdf_path)
    
    # Filtrar registros que necesitan georreferenciación
    df_to_process = df[df['corregir'] == 'INTENTAR GEORREFERENCIAR'].copy()
    
    if len(df_to_process) == 0:
        print("❌ No hay registros para procesar")
        return
    
    # Tomar el primer registro
    first_record = df_to_process.iloc[0]
    
    print(f"\n📍 Primer registro a procesar:")
    print(f"   UPID: {first_record['upid']}")
    print(f"   Nombre: {first_record['nombre_up']}")
    print(f"   Dirección: {first_record.get('direccion', 'N/A')}")
    print(f"   Barrio/Vereda original: {first_record.get('barrio_vereda', 'N/A')}")
    print(f"   Comuna/Corregimiento original: {first_record.get('comuna_corregimiento', 'N/A')}")
    
    # Parse geometry
    geometry_str = first_record['geometry']
    if pd.isna(geometry_str):
        print("\n❌ No hay geometría disponible")
        return
    
    try:
        geometry = json.loads(geometry_str)
        coordinates = geometry['coordinates']
        lon, lat = coordinates[0], coordinates[1]
        print(f"   Coordenadas: {lat}, {lon}")
    except Exception as e:
        print(f"\n❌ Error parseando geometría: {e}")
        return
    
    # Initialize Google Maps client
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    if not api_key:
        print("\n❌ GOOGLE_MAPS_API_KEY no configurado")
        return
    
    print(f"\n🔐 Inicializando cliente de Google Maps...")
    gmaps = googlemaps.Client(key=api_key)
    
    # Make API call
    print(f"\n🌐 Haciendo llamada a Google Maps API...")
    print(f"   reverse_geocode(({lat}, {lon}))")
    
    try:
        results = gmaps.reverse_geocode((lat, lon))
        
        if not results:
            print("\n❌ No se obtuvieron resultados")
            return
        
        print(f"\n✅ Resultados obtenidos: {len(results)} ubicaciones encontradas")
        
        # Mostrar el primer resultado completo
        print("\n" + "="*80)
        print("PRIMER RESULTADO COMPLETO (JSON)")
        print("="*80)
        print(json.dumps(results[0], indent=2, ensure_ascii=False))
        
        # Extraer address_components
        print("\n" + "="*80)
        print("ADDRESS COMPONENTS DETALLADOS")
        print("="*80)
        
        for i, component in enumerate(results[0].get('address_components', []), 1):
            print(f"\nComponente {i}:")
            print(f"   long_name: {component.get('long_name')}")
            print(f"   short_name: {component.get('short_name')}")
            print(f"   types: {component.get('types')}")
        
        # Mostrar formatted_address
        print("\n" + "="*80)
        print("DIRECCIONES FORMATEADAS")
        print("="*80)
        for i, result in enumerate(results, 1):
            print(f"\n{i}. {result.get('formatted_address')}")
            print(f"   Types: {result.get('types')}")
        
        # Análisis de tipos disponibles
        print("\n" + "="*80)
        print("TIPOS DE COMPONENTES DISPONIBLES EN TODOS LOS RESULTADOS")
        print("="*80)
        
        all_types = set()
        for result in results:
            for component in result.get('address_components', []):
                all_types.update(component.get('types', []))
        
        print("\nTipos únicos encontrados:")
        for tipo in sorted(all_types):
            print(f"   - {tipo}")
        
    except Exception as e:
        print(f"\n❌ Error en la llamada API: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    inspect_reverse_geocoding()
