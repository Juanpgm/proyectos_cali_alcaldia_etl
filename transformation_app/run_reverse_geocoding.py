# -*- coding: utf-8 -*-
"""
Script para ejecutar reverse geocoding en gdf_geolocalizar.xlsx

Este script procesa el archivo gdf_geolocalizar.xlsx y agrega las columnas:
- barrio_vereda_val_s3: Barrio/Vereda obtenido de Google Maps API
- comuna_corregimiento_val_s3: Comuna/Corregimiento obtenido de Google Maps API

Usa Application Default Credentials (ADC) con Google Maps API.

Uso:
    # Procesar primeros 10 registros (prueba)
    python run_reverse_geocoding.py --test
    
    # Procesar todos los registros que necesitan georreferenciación
    python run_reverse_geocoding.py
    
    # Procesar con número específico de registros
    python run_reverse_geocoding.py --max-requests 50

Author: AI Assistant
Version: 1.0
"""

import os
import sys
import argparse
from pathlib import Path

# Cargar variables de entorno ANTES de cualquier otra cosa
try:
    from dotenv import load_dotenv
    # Buscar archivos .env en el directorio raíz del proyecto
    project_root = Path(__file__).parent.parent
    
    # Intentar cargar .env.local primero (desarrollo), luego .env.prod (producción)
    env_local = project_root / '.env.local'
    env_prod = project_root / '.env.prod'
    
    if env_local.exists():
        load_dotenv(env_local)
        print(f"✓ Variables de entorno cargadas desde: {env_local.name}")
    elif env_prod.exists():
        load_dotenv(env_prod)
        print(f"✓ Variables de entorno cargadas desde: {env_prod.name}")
    else:
        print(f"⚠️  No se encontraron archivos .env.local o .env.prod")
except ImportError:
    print("⚠️  python-dotenv no instalado, usando variables de entorno del sistema")

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from utils.google_maps_geocoder import reverse_geocode_gdf_geolocalizar
except ImportError as e:
    print(f"❌ Error importing google_maps_geocoder: {e}")
    print("   Make sure you have installed googlemaps:")
    print("   pip install googlemaps")
    sys.exit(1)


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Reverse geocoding para gdf_geolocalizar.xlsx usando Google Maps API'
    )
    
    parser.add_argument(
        '--test',
        action='store_true',
        help='Modo prueba: procesar solo los primeros 10 registros'
    )
    
    parser.add_argument(
        '--max-requests',
        type=int,
        default=None,
        help='Número máximo de solicitudes a la API (para pruebas o control de costos)'
    )
    
    parser.add_argument(
        '--input',
        type=str,
        default=None,
        help='Ruta al archivo de entrada (por defecto: gdf_geolocalizar.xlsx)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Ruta al archivo de salida (por defecto: sobrescribe el archivo de entrada)'
    )
    
    args = parser.parse_args()
    
    # Determine input file
    if args.input:
        input_path = args.input
    else:
        input_path = os.path.join(
            os.path.dirname(__file__),
            'app_outputs',
            'unidades_proyecto_outputs',
            'gdf_geolocalizar.xlsx'
        )
    
    # Check if file exists
    if not os.path.exists(input_path):
        print(f"❌ Error: Input file not found: {input_path}")
        print("\n💡 Ejecuta primero:")
        print("   python data_transformation_unidades_proyecto.py")
        sys.exit(1)
    
    # Determine max requests
    max_requests = None
    if args.test:
        max_requests = 10
        print("🧪 MODO PRUEBA: Procesando primeros 10 registros")
    elif args.max_requests:
        max_requests = args.max_requests
        print(f"⚠️  LÍMITE: Procesando máximo {max_requests} registros")
    
    # Run reverse geocoding
    try:
        result_df = reverse_geocode_gdf_geolocalizar(
            input_file=input_path,
            output_file=args.output,
            max_requests=max_requests
        )
        
        print("\n" + "="*80)
        print("✅ REVERSE GEOCODING COMPLETADO EXITOSAMENTE")
        print("="*80)
        
        # Show sample results
        if 'barrio_vereda_val_s3' in result_df.columns:
            print("\n📊 Muestra de resultados (primeros 5 registros con georreferenciación):")
            sample_cols = [
                'upid', 
                'nombre_up', 
                'barrio_vereda_val_s3', 
                'comuna_corregimiento_val_s3'
            ]
            
            # Filter records that were processed
            processed = result_df[result_df['corregir'] == 'INTENTAR GEORREFERENCIAR']
            
            if len(processed) > 0:
                print(processed[sample_cols].head(5).to_string())
            else:
                print("   No hay registros procesados")
        
        print("\n💡 El archivo ha sido actualizado con las nuevas columnas:")
        print("   - barrio_vereda_val_s3")
        print("   - comuna_corregimiento_val_s3")
        
        if not args.test and max_requests is None:
            print("\n✅ Todos los registros han sido procesados")
        else:
            remaining = len(result_df[result_df['corregir'] == 'INTENTAR GEORREFERENCIAR'])
            if max_requests:
                remaining_unprocessed = remaining - max_requests
                if remaining_unprocessed > 0:
                    print(f"\n⚠️  Quedan aproximadamente {remaining_unprocessed} registros por procesar")
                    print("   Para procesar todos, ejecuta:")
                    print("   python run_reverse_geocoding.py")
        
    except Exception as e:
        print(f"\n❌ Error durante el reverse geocoding: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
