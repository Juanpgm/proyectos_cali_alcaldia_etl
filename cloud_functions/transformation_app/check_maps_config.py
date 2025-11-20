# -*- coding: utf-8 -*-
"""
Script de verificación y configuración para Google Maps API

Este script ayuda a verificar la configuración necesaria para el reverse geocoding.

Uso:
    python check_maps_config.py

Author: AI Assistant
Version: 1.0
"""

import os
import sys
from pathlib import Path

# Cargar variables de entorno
try:
    from dotenv import load_dotenv
    # Buscar archivos .env en el directorio raíz del proyecto
    project_root = Path(__file__).parent.parent
    
    # Intentar cargar .env.local primero (desarrollo), luego .env.prod (producción)
    env_local = project_root / '.env.local'
    env_prod = project_root / '.env.prod'
    
    if env_local.exists():
        load_dotenv(env_local)
        print(f"✓ Cargado: {env_local}")
    elif env_prod.exists():
        load_dotenv(env_prod)
        print(f"✓ Cargado: {env_prod}")
    else:
        print(f"⚠️  No se encontraron archivos .env.local o .env.prod")
except ImportError:
    print("⚠️  python-dotenv no instalado, usando variables de entorno del sistema")

def check_google_maps_config():
    """Verificar configuración de Google Maps API."""
    
    print("="*80)
    print("VERIFICACIÓN DE CONFIGURACIÓN - GOOGLE MAPS API")
    print("="*80)
    
    # Check if API key is configured
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    
    print("\n1. Verificando GOOGLE_MAPS_API_KEY...")
    
    if api_key:
        print(f"   ✅ API Key configurado")
        print(f"   Longitud: {len(api_key)} caracteres")
        print(f"   Primeros 10 caracteres: {api_key[:10]}...")
    else:
        print(f"   ❌ API Key NO configurado")
        print(f"\n   📝 Para configurar:")
        print(f"   1. Obtén una API Key de Google Maps:")
        print(f"      https://console.cloud.google.com/google/maps-apis/")
        print(f"      - Habilita 'Geocoding API'")
        print(f"      - Crea credenciales > API Key")
        print(f"   2. Agrega la API Key a tus archivos .env:")
        print(f"      - .env.prod (producción)")
        print(f"      - .env.local (desarrollo local)")
        print(f"   3. Formato:")
        print(f"      GOOGLE_MAPS_API_KEY=tu-api-key-aqui")
        return False
    
    # Check if googlemaps is installed
    print("\n2. Verificando librería googlemaps...")
    
    try:
        import googlemaps
        print(f"   ✅ googlemaps instalado (versión {googlemaps.__version__})")
    except ImportError:
        print(f"   ❌ googlemaps NO instalado")
        print(f"\n   📝 Para instalar:")
        print(f"   pip install googlemaps")
        return False
    
    # Check ADC authentication
    print("\n3. Verificando Application Default Credentials (ADC)...")
    
    try:
        from google.auth import default
        
        credentials, project = default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        
        print(f"   ✅ ADC configurado")
        print(f"   Proyecto: {project}")
        
    except Exception as e:
        print(f"   ⚠️  ADC no disponible: {e}")
        print(f"\n   📝 Para configurar ADC (opcional pero recomendado):")
        print(f"   gcloud auth application-default login")
        print(f"\n   Nota: ADC es opcional si tienes el API Key configurado")
    
    # Test Google Maps connection
    print("\n4. Probando conexión con Google Maps API...")
    
    try:
        import googlemaps
        
        gmaps = googlemaps.Client(key=api_key)
        
        # Test with a simple geocoding request (Cali, Colombia)
        test_result = gmaps.reverse_geocode((3.4516, -76.5320))
        
        if test_result:
            print(f"   ✅ Conexión exitosa con Google Maps API")
            print(f"   Resultado de prueba: {test_result[0]['formatted_address'][:50]}...")
        else:
            print(f"   ⚠️  Conexión exitosa pero sin resultados")
        
    except googlemaps.exceptions.ApiError as e:
        print(f"   ❌ Error de API: {e}")
        print(f"\n   Posibles causas:")
        print(f"   - API Key inválido")
        print(f"   - Geocoding API no habilitado en el proyecto")
        print(f"   - Cuota excedida")
        return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False
    
    # Check gdf_geolocalizar file
    print("\n5. Verificando archivo gdf_geolocalizar.xlsx...")
    
    gdf_path = os.path.join(
        os.path.dirname(__file__),
        'app_outputs',
        'unidades_proyecto_outputs',
        'gdf_geolocalizar.xlsx'
    )
    
    if os.path.exists(gdf_path):
        file_size = os.path.getsize(gdf_path) / 1024
        print(f"   ✅ Archivo encontrado")
        print(f"   Ruta: {gdf_path}")
        print(f"   Tamaño: {file_size:.1f} KB")
        
        # Count records to process
        try:
            import pandas as pd
            df = pd.read_excel(gdf_path)
            
            if 'corregir' in df.columns:
                to_process = (df['corregir'] == 'INTENTAR GEORREFERENCIAR').sum()
                total = len(df)
                print(f"   Registros totales: {total}")
                print(f"   Registros a procesar: {to_process}")
            else:
                print(f"   Registros totales: {len(df)}")
        except Exception as e:
            print(f"   ⚠️  No se pudo leer el archivo: {e}")
    else:
        print(f"   ❌ Archivo NO encontrado")
        print(f"   Ruta esperada: {gdf_path}")
        print(f"\n   📝 Para generar el archivo:")
        print(f"   python data_transformation_unidades_proyecto.py")
        return False
    
    print("\n" + "="*80)
    print("✅ CONFIGURACIÓN COMPLETA")
    print("="*80)
    print("\n🚀 Todo listo para ejecutar reverse geocoding!")
    print("\nPara ejecutar:")
    print("   python data_transformation_unidades_proyecto.py")
    print("\nO para ejecutar solo el reverse geocoding:")
    print("   python run_reverse_geocoding.py")
    
    return True


if __name__ == "__main__":
    success = check_google_maps_config()
    
    if not success:
        print("\n" + "="*80)
        print("⚠️  CONFIGURACIÓN INCOMPLETA")
        print("="*80)
        print("\nPor favor, sigue las instrucciones anteriores para completar la configuración.")
        sys.exit(1)
