# -*- coding: utf-8 -*-
"""
RPC Contratos Emprestito ETL Pipeline

Pipeline completo de Extracción, Transformación y Carga (ETL) para contratos RPC.
Implementa programación funcional para un código limpio, eficiente y escalable.

Funcionalidades:
- Extracción de datos desde PDFs con OCR y Gemini AI
- Transformación y validación de datos
- Carga batch optimizada a Firebase Firestore
- Logging detallado y manejo de errores
- Procesamiento de directorio completo o archivos individuales

Flujo del pipeline:
1. EXTRACCIÓN: PDF → Texto (OCR) → Datos estructurados (Gemini AI)
2. TRANSFORMACIÓN: Validación, normalización, enriquecimiento
3. CARGA: Batch upload a Firestore colección "rpc_contratos_emprestito"
"""

import os
import sys
import json
from typing import Dict, List, Any, Optional, Callable, Tuple
from functools import reduce, wraps
from pathlib import Path
from datetime import datetime

# Add project paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import pipeline modules
from extraction_app.data_extraction_rpc_contratos import (
    extract_rpc_from_pdf,
    extract_rpc_from_directory,
    save_extracted_data
)
from transformation_app.data_transformation_rpc_contratos import (
    transform_rpc_data,
    transform_rpc_batch,
    save_transformed_data
)
from load_app.data_loading_rpc_contratos import (
    load_rpc_to_firebase,
    get_collection_stats
)
from utils.pdf_processing import check_tesseract_installation
from database.config import secure_log


# Functional programming utilities
def compose(*functions: Callable) -> Callable:
    """Compose multiple functions into a single function."""
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(value: Any, *functions: Callable) -> Any:
    """Apply a sequence of functions to a value (pipe operator)."""
    return reduce(lambda acc, func: func(acc), functions, value)


def safe_execute(default_value: Any = None) -> Callable:
    """Decorator to safely execute functions with error handling."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"❌ Error en {func.__name__}: {e}")
                import traceback
                traceback.print_exc()
                return default_value
        return wrapper
    return decorator


def log_step(step_name: str) -> Callable:
    """Decorador para logging de pasos del pipeline."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            print(f"\n{'='*70}")
            print(f"📊 PASO: {step_name}")
            print(f"{'='*70}")
            start_time = datetime.now()
            
            result = func(*args, **kwargs)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Check success
            is_success = result is not None and (
                not isinstance(result, list) or len(result) > 0
            )
            
            if is_success:
                print(f"✅ {step_name} completado en {duration:.2f}s")
            else:
                print(f"❌ {step_name} falló después de {duration:.2f}s")
            
            return result
        return wrapper
    return decorator


# Pipeline functions
@log_step("EXTRACCIÓN DE DATOS DESDE PDFs")
@safe_execute(default_value=[])
def run_extraction(
    pdf_source: str,
    output_dir: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Ejecuta el proceso de extracción de datos desde PDFs.
    
    Args:
        pdf_source: Ruta a archivo PDF o directorio con PDFs
        output_dir: Directorio para guardar datos extraídos (opcional)
        
    Returns:
        Lista de datos extraídos o [] si falla
    """
    source_path = Path(pdf_source)
    
    # Check if source exists
    if not source_path.exists():
        print(f"❌ Fuente no encontrada: {pdf_source}")
        return []
    
    # Process directory or single file
    if source_path.is_dir():
        print(f"📁 Procesando directorio: {pdf_source}")
        extracted_data = extract_rpc_from_directory(pdf_source)
    elif source_path.is_file() and source_path.suffix.lower() == '.pdf':
        print(f"📄 Procesando archivo: {pdf_source}")
        result = extract_rpc_from_pdf(pdf_source)
        extracted_data = [result] if result else []
    else:
        print(f"❌ Fuente inválida: debe ser un PDF o directorio")
        return []
    
    # Save extracted data if output_dir specified
    if output_dir and extracted_data:
        save_extracted_data(
            extracted_data,
            output_dir,
            "rpc_contratos_extracted"
        )
    
    return extracted_data


@log_step("TRANSFORMACIÓN Y VALIDACIÓN DE DATOS")
@safe_execute(default_value=([], {}))
def run_transformation(
    extracted_data: List[Dict[str, Any]],
    output_dir: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Ejecuta el proceso de transformación y validación.
    
    Args:
        extracted_data: Datos extraídos desde PDFs
        output_dir: Directorio para guardar datos transformados (opcional)
        
    Returns:
        Tupla de (datos_transformados, estadísticas) o ([], {}) si falla
    """
    if not extracted_data:
        print("⚠️ No hay datos para transformar")
        return [], {}
    
    # Transform batch
    transformed_data, stats = transform_rpc_batch(extracted_data)
    
    # Save transformed data if output_dir specified
    if output_dir and transformed_data:
        save_transformed_data(
            transformed_data,
            output_dir,
            "rpc_contratos_transformed"
        )
    
    return transformed_data, stats


@log_step("CARGA A FIREBASE FIRESTORE")
@safe_execute(default_value=False)
def run_load(
    transformed_data: List[Dict[str, Any]],
    collection_name: str = "rpc_contratos_emprestito",
    update_existing: bool = True
) -> bool:
    """
    Ejecuta el proceso de carga a Firebase.
    
    Args:
        transformed_data: Datos transformados y validados
        collection_name: Nombre de la colección en Firestore
        update_existing: Si actualizar documentos existentes
        
    Returns:
        True si exitoso, False si falla
    """
    if not transformed_data:
        print("⚠️ No hay datos para cargar")
        return False
    
    return load_rpc_to_firebase(
        transformed_data,
        collection_name,
        update_existing=update_existing
    )


# Pipeline principal
def create_rpc_contratos_pipeline() -> Callable:
    """
    Crea el pipeline completo para contratos RPC.
    
    Returns:
        Función del pipeline configurada
    """
    
    def pipeline(
        pdf_source: str,
        collection_name: str = "rpc_contratos_emprestito",
        save_intermediate: bool = True,
        update_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Pipeline ETL completo para contratos RPC.
        
        Args:
            pdf_source: Ruta a PDF o directorio con PDFs
            collection_name: Nombre de la colección en Firestore
            save_intermediate: Si guardar archivos intermedios (JSON/CSV)
            update_existing: Si actualizar documentos existentes en Firestore
            
        Returns:
            Diccionario con resultados del pipeline
        """
        pipeline_start = datetime.now()
        
        results = {
            'success': False,
            'start_time': pipeline_start.isoformat(),
            'end_time': None,
            'duration_seconds': None,
            'pdf_source': pdf_source,
            'collection_name': collection_name,
            'extraction_success': False,
            'transformation_success': False,
            'load_success': False,
            'pdfs_processed': 0,
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'transformation_stats': {},
            'errors': []
        }
        
        try:
            print("🚀 INICIANDO PIPELINE ETL RPC CONTRATOS EMPRESTITO")
            print("="*80)
            print(f"⏰ Inicio: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"📁 Fuente: {pdf_source}")
            print(f"🗂️ Colección destino: {collection_name}")
            
            # Check prerequisites
            print("\n🔍 Verificando requisitos previos...")
            if not check_tesseract_installation():
                print("⚠️ Tesseract no disponible - OCR puede fallar")
            
            gemini_key = os.getenv('GEMINI_API_KEY')
            if not gemini_key:
                results['errors'].append("GEMINI_API_KEY no configurada")
                print("❌ GEMINI_API_KEY no configurada")
                return results
            
            print("✅ Requisitos verificados")
            
            # Setup output directory for intermediate files
            output_dir = None
            if save_intermediate:
                output_dir = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "extraction_app",
                    "app_outputs",
                    "rpc_contratos_outputs"
                )
                Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # PASO 1: Extracción
            extracted_data = run_extraction(pdf_source, output_dir)
            
            if not extracted_data:
                results['errors'].append("Fallo en extracción de datos")
                return results
            
            results['extraction_success'] = True
            results['records_extracted'] = len(extracted_data)
            
            # Count PDFs processed
            source_path = Path(pdf_source)
            if source_path.is_dir():
                results['pdfs_processed'] = len(list(source_path.glob("*.pdf")))
            else:
                results['pdfs_processed'] = 1
            
            # PASO 2: Transformación
            transformed_data, trans_stats = run_transformation(
                extracted_data,
                output_dir
            )
            
            if not transformed_data:
                results['errors'].append("Fallo en transformación de datos")
                return results
            
            results['transformation_success'] = True
            results['records_transformed'] = len(transformed_data)
            results['transformation_stats'] = trans_stats
            
            # PASO 3: Carga a Firebase
            load_success = run_load(
                transformed_data,
                collection_name,
                update_existing
            )
            
            results['load_success'] = load_success
            
            if load_success:
                results['records_loaded'] = len(transformed_data)
            
            # Calcular resultados finales
            pipeline_end = datetime.now()
            results['end_time'] = pipeline_end.isoformat()
            results['duration_seconds'] = (pipeline_end - pipeline_start).total_seconds()
            results['success'] = (
                results['extraction_success'] and
                results['transformation_success'] and
                results['load_success']
            )
            
            return results
            
        except Exception as e:
            results['errors'].append(f"Error general del pipeline: {str(e)}")
            pipeline_end = datetime.now()
            results['end_time'] = pipeline_end.isoformat()
            results['duration_seconds'] = (pipeline_end - pipeline_start).total_seconds()
            return results
    
    return pipeline


def print_pipeline_summary(results: Dict[str, Any]):
    """
    Imprime un resumen detallado de los resultados del pipeline.
    
    Args:
        results: Diccionario con los resultados del pipeline
    """
    print(f"\n{'='*80}")
    print("📊 RESUMEN DEL PIPELINE ETL RPC CONTRATOS")
    print("="*80)
    
    # Estado general
    status_icon = "✅" if results['success'] else "❌"
    print(f"{status_icon} Estado general: {'EXITOSO' if results['success'] else 'FALLIDO'}")
    
    # Información básica
    print(f"📁 Fuente: {results.get('pdf_source', 'N/A')}")
    print(f"🗂️ Colección: {results.get('collection_name', 'N/A')}")
    
    # Tiempos
    if results['start_time']:
        start_time = datetime.fromisoformat(results['start_time'])
        print(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if results['end_time']:
        end_time = datetime.fromisoformat(results['end_time'])
        print(f"🏁 Fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if results['duration_seconds']:
        duration = results['duration_seconds']
        minutes = int(duration // 60)
        seconds = int(duration % 60)
        print(f"⏱️ Duración: {minutes}m {seconds}s")
    
    # Pasos del pipeline
    print(f"\n🔄 Pasos ejecutados:")
    steps = [
        ("Extracción de PDFs", results['extraction_success']),
        ("Transformación y validación", results['transformation_success']),
        ("Carga a Firebase", results['load_success'])
    ]
    
    for step_name, success in steps:
        icon = "✅" if success else "❌"
        print(f"  {icon} {step_name}")
    
    # Estadísticas de datos
    print(f"\n📈 Estadísticas:")
    print(f"  📄 PDFs procesados: {results.get('pdfs_processed', 0)}")
    print(f"  📥 Registros extraídos: {results.get('records_extracted', 0)}")
    print(f"  🔄 Registros transformados: {results.get('records_transformed', 0)}")
    print(f"  📤 Registros cargados: {results.get('records_loaded', 0)}")
    
    # Estadísticas de transformación
    if results.get('transformation_stats'):
        stats = results['transformation_stats']
        print(f"\n📊 Detalles de transformación:")
        print(f"  ✅ Exitosos: {stats.get('successful', 0)}")
        print(f"  ⚠️ Con advertencias: {stats.get('with_warnings', 0)}")
        print(f"  ❌ Fallidos: {stats.get('failed', 0)}")
    
    # Errores
    if results.get('errors'):
        print(f"\n❌ Errores encontrados:")
        for i, error in enumerate(results['errors'], 1):
            print(f"  {i}. {error}")
    
    # Mensaje final
    if results['success']:
        print(f"\n🎉 Pipeline completado exitosamente!")
        print(f"✨ {results.get('records_loaded', 0)} contratos RPC cargados a Firebase")
    else:
        print(f"\n💥 Pipeline falló. Revisa los errores arriba.")
    
    print("="*80)


def run_rpc_contratos_pipeline(
    pdf_source: str,
    collection_name: str = "rpc_contratos_emprestito",
    save_intermediate: bool = True,
    update_existing: bool = True
) -> bool:
    """
    Función principal para ejecutar el pipeline completo de contratos RPC.
    
    Args:
        pdf_source: Ruta a PDF o directorio con PDFs
        collection_name: Nombre de la colección en Firestore
        save_intermediate: Si guardar archivos intermedios
        update_existing: Si actualizar documentos existentes
        
    Returns:
        True si el pipeline fue exitoso, False en caso contrario
    """
    # Crear y ejecutar pipeline
    pipeline = create_rpc_contratos_pipeline()
    results = pipeline(
        pdf_source,
        collection_name,
        save_intermediate,
        update_existing
    )
    
    # Mostrar resumen
    print_pipeline_summary(results)
    
    # Mostrar estadísticas de la colección
    if results['success']:
        print(f"\n{'='*80}")
        print("📊 ESTADÍSTICAS DE LA COLECCIÓN")
        print("="*80)
        stats = get_collection_stats(collection_name)
        
        if stats:
            for key, value in stats.items():
                if isinstance(value, float):
                    print(f"  {key}: ${value:,.2f}" if 'valor' in key.lower() else f"  {key}: {value:.2f}")
                else:
                    print(f"  {key}: {value}")
    
    return results['success']


def main():
    """Función principal para probar el pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Pipeline ETL para contratos RPC desde PDFs a Firebase'
    )
    parser.add_argument(
        'pdf_source',
        help='Ruta a archivo PDF o directorio con PDFs'
    )
    parser.add_argument(
        '--collection',
        default='rpc_contratos_emprestito',
        help='Nombre de la colección en Firestore (default: rpc_contratos_emprestito)'
    )
    parser.add_argument(
        '--no-save-intermediate',
        action='store_true',
        help='No guardar archivos intermedios JSON/CSV'
    )
    parser.add_argument(
        '--no-update',
        action='store_true',
        help='No actualizar documentos existentes'
    )
    
    args = parser.parse_args()
    
    return run_rpc_contratos_pipeline(
        pdf_source=args.pdf_source,
        collection_name=args.collection,
        save_intermediate=not args.no_save_intermediate,
        update_existing=not args.no_update
    )


if __name__ == "__main__":
    """
    Bloque de ejecución principal para probar el pipeline completo.
    
    Uso:
        python rpc_contratos_emprestito_pipeline.py <pdf_o_directorio>
        python rpc_contratos_emprestito_pipeline.py context/ --collection rpc_test
    """
    print("🚀 Iniciando pipeline ETL de Contratos RPC...")
    
    # Ejecutar pipeline completo
    success = main()
    
    if success:
        print("\n🎯 PIPELINE COMPLETADO EXITOSAMENTE")
        print("✨ Datos de contratos RPC procesados y cargados a Firebase")
    else:
        print("\n💥 PIPELINE FALLÓ")
        print("🔧 Revisa los errores y logs anteriores")
    
    # Código de salida para scripts automatizados
    sys.exit(0 if success else 1)
