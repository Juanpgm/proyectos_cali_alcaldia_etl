"""
Cloud Function Principal: Pipeline ETL Serverless Completo
Ejecuta extracción, transformación y carga a Firebase cada hora
Usa módulos específicos para cada etapa del pipeline
"""

import functions_framework
from flask import jsonify
import os
import sys
import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Agregar paths para importar módulos del proyecto
# En Cloud Functions, los módulos están en el mismo directorio
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'transformation_app'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'load_app'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'extraction_app'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))


@functions_framework.http
def etl_pipeline_hourly(request):
    """
    HTTP Cloud Function para ejecutar pipeline ETL completo cada hora.
    Ejecuta: Extracción -> Transformación -> Carga a Firebase
    
    Trigger:
    - Cloud Scheduler con cron: '0 * * * *' (cada hora desde medianoche)
    - HTTP POST/GET manual para testing
    
    Args:
        request (flask.Request): Request HTTP
        
    Returns:
        JSON con resultados de la operación completa
    """
    result = {
        'success': False,
        'timestamp': datetime.now().isoformat(),
        'function': 'etl_pipeline_hourly',
        'pipeline_stages': {
            'extraction': {'success': False, 'records': 0},
            'transformation': {'success': False, 'records': 0},
            'load': {'success': False, 'records_uploaded': 0}
        },
        'errors': []
    }
    
    try:
        print("="*80)
        print("🚀 INICIANDO PIPELINE ETL SERVERLESS COMPLETO")
        print(f"⏰ Hora de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        # ========== ETAPA 1: EXTRACCIÓN Y TRANSFORMACIÓN ==========
        print("\n" + "="*80)
        print("📥 ETAPA 1 & 2: EXTRACCIÓN Y TRANSFORMACIÓN")
        print("="*80)
        
        try:
            # Importar función de transformación (incluye extracción integrada)
            from transformation_app.data_transformation_unidades_proyecto import transform_and_save_unidades_proyecto
            
            # Ejecutar transformación (incluye extracción desde Google Drive)
            print("\n🔄 Ejecutando extracción y transformación...")
            gdf_result = transform_and_save_unidades_proyecto()
            
            if gdf_result is not None and len(gdf_result) > 0:
                result['pipeline_stages']['extraction']['success'] = True
                result['pipeline_stages']['extraction']['records'] = len(gdf_result)
                result['pipeline_stages']['transformation']['success'] = True
                result['pipeline_stages']['transformation']['records'] = len(gdf_result)
                
                print(f"\n✅ Transformación completada: {len(gdf_result)} registros")
            else:
                result['errors'].append("Transformación no produjo datos válidos")
                return jsonify(result), 500
                
        except Exception as e:
            print(f"\n❌ Error en extracción/transformación: {e}")
            import traceback
            traceback.print_exc()
            result['errors'].append(f"Extraction/Transformation error: {str(e)}")
            return jsonify(result), 500
        
        # ========== ETAPA 3: CARGA A FIREBASE ==========
        print("\n" + "="*80)
        print("📤 ETAPA 3: CARGA A FIREBASE")
        print("="*80)
        
        try:
            # Importar función de carga específica
            from load_app.data_loading_unidades_proyecto import load_unidades_proyecto_to_firebase
            
            # Buscar el archivo GeoJSON generado por transformación
            transformation_output_dir = Path(__file__).parent.parent / 'transformation_app' / 'app_outputs' / 'unidades_proyecto_outputs'
            geojson_file = transformation_output_dir / 'unidades_proyecto.geojson'
            
            if not geojson_file.exists():
                # Buscar en app_outputs (ruta alternativa)
                alt_output_dir = Path(__file__).parent.parent / 'app_outputs'
                geojson_file = alt_output_dir / 'unidades_proyecto_transformed.geojson'
            
            if not geojson_file.exists():
                result['errors'].append(f"Archivo GeoJSON no encontrado en rutas esperadas")
                return jsonify(result), 500
            
            print(f"\n📂 Usando archivo: {geojson_file}")
            
            # Ejecutar carga a Firebase con actualización selectiva
            print("\n🔥 Cargando a Firebase con actualizaciones selectivas por upid...")
            load_success = load_unidades_proyecto_to_firebase(
                input_file=str(geojson_file),
                collection_name="unidades_proyecto",
                batch_size=100
            )
            
            if load_success:
                result['pipeline_stages']['load']['success'] = True
                result['pipeline_stages']['load']['records_uploaded'] = len(gdf_result)
                result['success'] = True
                print("\n✅ Carga a Firebase completada exitosamente")
            else:
                result['errors'].append("Carga a Firebase falló")
                return jsonify(result), 500
                
        except Exception as e:
            print(f"\n❌ Error en carga a Firebase: {e}")
            import traceback
            traceback.print_exc()
            result['errors'].append(f"Firebase load error: {str(e)}")
            return jsonify(result), 500
        
        # ========== RESUMEN FINAL ==========
        print("\n" + "="*80)
        print("📊 RESUMEN DEL PIPELINE ETL")
        print("="*80)
        print(f"✅ Extracción: {result['pipeline_stages']['extraction']['records']} registros")
        print(f"✅ Transformación: {result['pipeline_stages']['transformation']['records']} registros")
        print(f"✅ Carga Firebase: {result['pipeline_stages']['load']['records_uploaded']} registros")
        print("="*80)
        print("🎉 PIPELINE ETL COMPLETADO EXITOSAMENTE")
        print("="*80)
        
        return jsonify(result), 200
        
    except Exception as e:
        print(f"\n❌ Error general en pipeline: {e}")
        import traceback
        traceback.print_exc()
        
        result['errors'].append(str(e))
        return jsonify(result), 500


@functions_framework.http
def manual_trigger(request):
    """
    Trigger manual para ejecutar el pipeline ETL bajo demanda.
    
    Uso:
        POST/GET a la URL de la función para ejecutar pipeline completo
    
    Returns:
        JSON con resultado de la operación
    """
    try:
        print("🎯 Trigger manual del pipeline ETL iniciado")
        print(f"⏰ Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Ejecutar pipeline completo
        return etl_pipeline_hourly(request)
        
    except Exception as e:
        print(f"❌ Error en trigger manual: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500


# ========== NOTIFICATION TRIGGERS ==========
# These functions are intended to be deployed as Background Cloud Functions (Firestore Triggers)
# They are not HTTP functions.

from notifications import on_unidades_proyecto_write, on_contrato_write
