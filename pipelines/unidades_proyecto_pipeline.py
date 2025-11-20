# -*- coding: utf-8 -*-
"""
Unidades de Proyecto ETL Pipeline

Pipeline completo de Extracción, Transformación y Carga (ETL) para unidades de proyecto.
Implementa programación funcional para un código limpio, eficiente y escalable.
Incluye verificación incremental para cargar solo datos nuevos o modificados.

Funcionalidades:
- Extracción de datos desde Google Sheets con autenticación segura
- Transformación de datos con procesamiento geoespacial
- Verificación de cambios contra Firebase (carga incremental)
- Carga batch optimizada a Firebase Firestore
- Logging detallado y manejo de errores
- Compatible con GitHub Actions para automatización
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Callable
from functools import reduce, partial, wraps
import hashlib

# Agregar rutas necesarias al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar módulos del proyecto
from extraction_app.data_extraction_unidades_proyecto import extract_and_save_unidades_proyecto
from transformation_app.data_transformation_unidades_proyecto import transform_and_save_unidades_proyecto
from transformation_app.data_transformation_unidades_proyecto_infraestructura import main as transform_infraestructura
from load_app.data_loading_unidades_proyecto import load_unidades_proyecto_to_firebase
from load_app.data_loading_unidades_proyecto_infraestructura import load_infraestructura_vial_to_firebase
from load_app.data_loading_quality_control import load_quality_reports_to_firebase
from database.config import get_firestore_client, secure_log

# Importar módulos de control de calidad
from utils.quality_control import validate_geojson, DataQualityValidator
from utils.quality_reporter import QualityReporter
from utils.quality_s3_exporter import export_quality_reports_to_s3
from utils.quality_control_firebase import run_quality_control_on_firebase_data


# Utilidades de programación funcional
def compose(*functions: Callable) -> Callable:
    """Compone múltiples funciones en una sola función."""
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(value: Any, *functions: Callable) -> Any:
    """Aplica una secuencia de funciones a un valor (operador pipe)."""
    return reduce(lambda acc, func: func(acc), functions, value)


def curry(func: Callable) -> Callable:
    """Convierte una función a una versión currificada para aplicación parcial."""
    @wraps(func)
    def curried(*args, **kwargs):
        if len(args) + len(kwargs) >= func.__code__.co_argcount:
            return func(*args, **kwargs)
        return lambda *more_args, **more_kwargs: curried(*(args + more_args), **dict(kwargs, **more_kwargs))
    return curried


def safe_execute(func: Callable, default_value: Any = None) -> Callable:
    """Ejecuta funciones de forma segura con manejo de errores."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            print(f"❌ Error en {func.__name__}: {e}")
            import traceback
            traceback.print_exc()
            return default_value
    return wrapper


def log_step(step_name: str) -> Callable:
    """Decorador para logging de pasos del pipeline."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            print(f"\n{'='*60}")
            print(f"📊 PASO: {step_name}")
            print(f"{'='*60}")
            start_time = datetime.now()
            
            result = func(*args, **kwargs)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Mejor validación que maneja DataFrames
            is_success = False
            if result is not None:
                if hasattr(result, 'empty'):  # Es un DataFrame
                    is_success = not result.empty
                elif isinstance(result, bool):
                    is_success = result
                elif isinstance(result, (list, dict, str)):
                    is_success = len(result) > 0
                else:
                    is_success = True  # Para otros tipos no nulos
            
            if is_success:
                print(f"✅ {step_name} completado en {duration:.2f}s")
            else:
                print(f"❌ {step_name} falló después de {duration:.2f}s")
            
            return result
        return wrapper
    return decorator


# Funciones para verificación incremental de datos
def calculate_record_hash(record: Dict[str, Any]) -> str:
    """
    Calcula un hash único para un registro para detectar cambios.
    Maneja tanto features GeoJSON como documentos de Firebase.
    
    Args:
        record: Diccionario con los datos del registro (GeoJSON feature o documento Firebase)
        
    Returns:
        Hash MD5 del registro como string
    """
    # Determinar si es un GeoJSON feature o documento de Firebase
    if record.get('type') == 'Feature':
        # Es un GeoJSON feature - usar solo las propiedades para comparar
        properties = record.get('properties', {})
        geometry = record.get('geometry')
        
        # Crear objeto consistente para hash
        hash_data = {
            'properties': properties,
            'geometry': geometry
        }
    else:
        # Es un documento de Firebase - extraer propiedades y geometría
        properties = record.get('properties', {})
        geometry = record.get('geometry')
        
        hash_data = {
            'properties': properties,
            'geometry': geometry
        }
    
    # Excluir campos de metadata que cambian automáticamente
    excluded_fields = ['created_at', 'updated_at', 'processed_timestamp', 
                      'has_geometry', 'geometry_type']
    
    # Limpiar las propiedades de campos de metadata
    if 'properties' in hash_data and hash_data['properties']:
        cleaned_properties = {
            k: v for k, v in hash_data['properties'].items() 
            if k not in excluded_fields and v is not None
        }
        hash_data['properties'] = cleaned_properties
    
    # Convertir a JSON string ordenado para hash consistente
    record_str = json.dumps(hash_data, sort_keys=True, default=str)
    
    # Calcular hash MD5
    return hashlib.md5(record_str.encode('utf-8')).hexdigest()


@safe_execute
def get_existing_firebase_data(collection_name: str = "unidades_proyecto") -> Dict[str, Dict[str, Any]]:
    """
    Obtiene los datos existentes en Firebase para comparación.
    
    Args:
        collection_name: Nombre de la colección en Firebase
        
    Returns:
        Diccionario con {doc_id: {data_hash, last_updated}} o {} si falla
    """
    print(f"🔍 Obteniendo datos existentes de Firebase colección '{collection_name}'...")
    
    try:
        db = get_firestore_client()
        if not db:
            print("❌ No se pudo conectar a Firebase")
            return {}
        
        collection_ref = db.collection(collection_name)
        
        # Obtener solo los campos necesarios para comparación (más eficiente)
        existing_data = {}
        
        docs = collection_ref.stream()
        doc_count = 0
        
        for doc in docs:
            doc_data = doc.to_dict()
            
            # Calcular hash de los datos existentes
            data_hash = calculate_record_hash(doc_data)
            
            existing_data[doc.id] = {
                'hash': data_hash,
                'updated_at': doc_data.get('updated_at'),
                'properties': doc_data.get('properties', {})
            }
            
            doc_count += 1
        
        print(f"✅ Obtenidos {doc_count} registros existentes de Firebase")
        return existing_data
        
    except Exception as e:
        print(f"❌ Error obteniendo datos de Firebase: {e}")
        return {}


def compare_and_filter_changes(
    new_features: List[Dict[str, Any]], 
    existing_data: Dict[str, Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Compara datos nuevos con existentes y filtra solo los cambios.
    
    Args:
        new_features: Lista de features nuevos desde la transformación
        existing_data: Datos existentes en Firebase
        
    Returns:
        Tupla de (features_to_upload, change_summary)
    """
    print(f"🔄 Comparando {len(new_features)} registros nuevos con {len(existing_data)} existentes...")
    
    features_to_upload = []
    change_summary = {
        'new_records': 0,
        'modified_records': 0,
        'unchanged_records': 0,
        'total_processed': len(new_features)
    }
    
    for feature in new_features:
        try:
            # Obtener ID del documento
            properties = feature.get('properties', {})
            doc_id = None
            
            # Buscar ID usando la misma lógica que en load_app
            if properties.get('upid'):
                doc_id = str(properties['upid']).strip()
            elif properties.get('identificador'):
                doc_id = f"ID-{str(properties['identificador']).strip()}"
            elif properties.get('bpin'):
                doc_id = f"BPIN-{str(properties['bpin']).strip()}"
            
            if not doc_id:
                # Si no hay ID, es un registro nuevo
                features_to_upload.append(feature)
                change_summary['new_records'] += 1
                continue
            
            # Calcular hash del nuevo registro
            new_hash = calculate_record_hash(feature)
            
            # Verificar si existe en Firebase
            if doc_id in existing_data:
                existing_hash = existing_data[doc_id]['hash']
                
                if new_hash != existing_hash:
                    # Registro modificado
                    features_to_upload.append(feature)
                    change_summary['modified_records'] += 1
                else:
                    # Registro sin cambios
                    change_summary['unchanged_records'] += 1
            else:
                # Registro nuevo
                features_to_upload.append(feature)
                change_summary['new_records'] += 1
                
        except Exception as e:
            print(f"⚠️ Error comparando registro: {e}")
            # En caso de error, incluir el registro para estar seguros
            features_to_upload.append(feature)
            change_summary['new_records'] += 1
    
    print(f"📊 Resumen de cambios:")
    print(f"  ➕ Nuevos: {change_summary['new_records']}")
    print(f"  🔄 Modificados: {change_summary['modified_records']}")
    print(f"  ✅ Sin cambios: {change_summary['unchanged_records']}")
    print(f"  📤 Total a cargar: {len(features_to_upload)}")
    
    return features_to_upload, change_summary


def create_incremental_geojson(
    features_to_upload: List[Dict[str, Any]], 
    output_path: str
) -> bool:
    """
    Crea un archivo GeoJSON con solo los registros que necesitan actualizarse.
    
    Args:
        features_to_upload: Lista de features a cargar
        output_path: Ruta donde guardar el archivo GeoJSON
        
    Returns:
        True si se guardó exitosamente, False en caso contrario
    """
    try:
        # Crear FeatureCollection incremental
        incremental_geojson = {
            "type": "FeatureCollection",
            "features": features_to_upload,
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "feature_count": len(features_to_upload),
                "type": "incremental_update"
            }
        }
        
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Guardar archivo
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(incremental_geojson, f, indent=2, ensure_ascii=False, default=str)
        
        file_size_kb = os.path.getsize(output_path) / 1024
        print(f"💾 GeoJSON incremental guardado: {os.path.basename(output_path)} ({file_size_kb:.1f} KB)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error guardando GeoJSON incremental: {e}")
        return False


# Funciones principales del pipeline
@log_step("EXTRACCIÓN DE DATOS")
@safe_execute
def run_extraction() -> Optional[pd.DataFrame]:
    """
    Ejecuta el proceso de extracción de datos desde Google Sheets.
    
    Returns:
        DataFrame con los datos extraídos o None si falla
    """
    return extract_and_save_unidades_proyecto()


@log_step("TRANSFORMACIÓN DE DATOS")
@safe_execute
def run_transformation(extracted_data: Optional[pd.DataFrame] = None) -> Optional[pd.DataFrame]:
    """
    Ejecuta el proceso de transformación de datos.
    
    Args:
        extracted_data: DataFrame con datos ya extraídos (evita extracción duplicada)
    
    Returns:
        DataFrame con los datos transformados o None si falla
    """
    # Si hay datos extraídos, pasarlos a la transformación y desactivar extracción
    if extracted_data is not None:
        return transform_and_save_unidades_proyecto(data=extracted_data, use_extraction=False, upload_to_s3=True)
    else:
        return transform_and_save_unidades_proyecto(upload_to_s3=True)


@log_step("TRANSFORMACIÓN DE INFRAESTRUCTURA")
@safe_execute
def run_transformation_infraestructura() -> bool:
    """
    Ejecuta el proceso de transformación de datos de infraestructura vial.
    
    Returns:
        True si la transformación fue exitosa, False en caso contrario
    """
    return transform_infraestructura()


@log_step("VERIFICACIÓN INCREMENTAL")
@safe_execute
def verify_and_prepare_incremental_load(
    geojson_path: str,
    collection_name: str = "unidades_proyecto"
) -> Tuple[Optional[str], Optional[Dict[str, int]]]:
    """
    Verifica cambios y prepara carga incremental.
    
    Args:
        geojson_path: Ruta al archivo GeoJSON con todos los datos
        collection_name: Nombre de la colección en Firebase
        
    Returns:
        Tupla de (ruta_geojson_incremental, resumen_cambios) o (None, None) si falla
    """
    try:
        # Cargar GeoJSON completo
        if not os.path.exists(geojson_path):
            print(f"❌ Archivo GeoJSON no encontrado: {geojson_path}")
            return None, None
        
        with open(geojson_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        new_features = geojson_data.get('features', [])
        if not new_features:
            print("⚠️ No hay features en el GeoJSON")
            return None, None
        
        # Obtener datos existentes de Firebase
        existing_data = get_existing_firebase_data(collection_name)
        
        # Comparar y filtrar cambios
        features_to_upload, change_summary = compare_and_filter_changes(new_features, existing_data)
        
        if not features_to_upload:
            print("✅ No hay cambios para cargar. Todos los datos están actualizados.")
            return None, change_summary
        
        # Crear GeoJSON incremental
        incremental_path = geojson_path.replace('.geojson', '_incremental.geojson')
        success = create_incremental_geojson(features_to_upload, incremental_path)
        
        if success:
            return incremental_path, change_summary
        else:
            return None, None
            
    except Exception as e:
        print(f"❌ Error en verificación incremental: {e}")
        return None, None


@log_step("CARGA INCREMENTAL A FIREBASE")
@safe_execute
def run_incremental_load(incremental_geojson_path: str, collection_name: str = "unidades_proyecto", use_s3: bool = True) -> bool:
    """
    Ejecuta la carga incremental a Firebase desde S3 o archivo local.
    
    Args:
        incremental_geojson_path: Ruta al GeoJSON con solo los cambios (fallback si S3 falla)
        collection_name: Nombre de la colección en Firebase
        use_s3: Si True, intenta cargar desde S3 primero (default: True)
        
    Returns:
        True si la carga fue exitosa, False en caso contrario
    """
    if not use_s3:
        # Modo legacy: solo archivo local
        if not incremental_geojson_path or not os.path.exists(incremental_geojson_path):
            print("❌ Archivo GeoJSON incremental no disponible")
            return False
    
    return load_unidades_proyecto_to_firebase(
        input_file=incremental_geojson_path,
        collection_name=collection_name,
        batch_size=100,
        use_s3=use_s3,
        s3_key="up-geodata/unidades_proyecto_transformed/current/unidades_proyecto_transformed.geojson.gz"  # Usar versión CURRENT desde S3
    )


@log_step("CARGA INFRAESTRUCTURA A FIREBASE")
@safe_execute
def run_load_infraestructura(collection_name: str = "unidades_proyecto") -> bool:
    """
    Ejecuta la carga de datos de infraestructura vial a Firebase.
    
    Args:
        collection_name: Nombre de la colección en Firebase
        
    Returns:
        True si la carga fue exitosa, False en caso contrario
    """
    # El módulo de infraestructura carga desde context/unidades_proyecto.geojson
    return load_infraestructura_vial_to_firebase(
        input_file=None,  # Usa ruta por defecto
        collection_name=collection_name,
        batch_size=100
    )


@log_step("CONTROL DE CALIDAD DE DATOS")
@safe_execute
def run_quality_control(
    geojson_path: str,
    enable_firebase_upload: bool = True,
    enable_s3_upload: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Ejecuta validaciones de control de calidad sobre datos transformados.
    
    Este paso NO altera los datos originales, solo genera reportes detallados
    que se cargan a Firebase y S3 para administración y corrección de datos.
    
    Args:
        geojson_path: Ruta al archivo GeoJSON transformado
        enable_firebase_upload: Si True, carga reportes a Firebase
        enable_s3_upload: Si True, exporta reportes a S3
        
    Returns:
        Diccionario con estadísticas de control de calidad o None si falla
    """
    try:
        print(f"\n📋 Ejecutando validaciones ISO 19157...")
        
        # 1. Validar GeoJSON completo
        validation_result = validate_geojson(geojson_path, verbose=False)
        
        if not validation_result or 'issues' not in validation_result:
            print("⚠️ No se pudieron generar reportes de calidad")
            return None
        
        print(f"  ✓ Validados: {validation_result['total_records']} registros")
        print(f"  ✓ Problemas detectados: {validation_result['total_issues']}")
        print(f"  ✓ Score de calidad: {validation_result['statistics']['quality_score']:.2f}/100")
        
        # 2. Generar reportes detallados
        print(f"\n📊 Generando reportes detallados...")
        
        reporter = QualityReporter()
        
        # Reporte por registro
        record_reports = reporter.generate_record_level_report(validation_result['issues'])
        print(f"  ✓ Reportes por registro: {len(record_reports)}")
        
        # Contar registros totales por centro gestor
        import json
        with open(geojson_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        total_by_centro = {}
        for feature in geojson_data.get('features', []):
            centro = feature.get('properties', {}).get('nombre_centro_gestor', 'Sin Centro Gestor')
            total_by_centro[centro] = total_by_centro.get(centro, 0) + 1
        
        # Reporte por centro gestor
        centro_reports = reporter.generate_centro_gestor_report(
            validation_result['issues'],
            total_by_centro
        )
        print(f"  ✓ Reportes por centro gestor: {len(centro_reports)}")
        
        # Reporte resumen
        summary_report = reporter.generate_summary_report(
            record_reports,
            centro_reports,
            validation_result['total_records'],
            validation_result['statistics']
        )
        print(f"  ✓ Reporte resumen generado")
        
        # 3. Cargar a Firebase (si está habilitado)
        if enable_firebase_upload:
            print(f"\n🔥 Cargando reportes a Firebase...")
            firebase_stats = load_quality_reports_to_firebase(
                record_reports=record_reports,
                centro_reports=centro_reports,
                summary_report=summary_report,
                batch_size=100,
                verbose=False
            )
            print(f"  ✓ Cargados a Firebase: {firebase_stats.get('records_loaded', 0) + firebase_stats.get('centros_loaded', 0) + firebase_stats.get('summary_loaded', 0)} documentos")
        
        # 4. Exportar a S3 (si está habilitado)
        if enable_s3_upload:
            print(f"\n☁️  Exportando reportes a S3...")
            try:
                s3_stats = export_quality_reports_to_s3(
                    record_reports=record_reports,
                    centro_reports=centro_reports,
                    summary_report=summary_report,
                    validation_stats=validation_result['statistics'],
                    report_id=reporter.report_id,
                    verbose=False
                )
                print(f"  ✓ Exportados a S3: {s3_stats.get('files_uploaded', 0)} archivos")
            except Exception as e:
                print(f"  ⚠️ No se pudo exportar a S3: {e}")
                print(f"  ℹ️  Los reportes siguen disponibles en Firebase")
        
        # 5. Retornar estadísticas
        return {
            'quality_score': validation_result['statistics']['quality_score'],
            'total_issues': validation_result['total_issues'],
            'records_with_issues': validation_result['records_with_issues'],
            'severity_counts': validation_result['statistics']['by_severity'],
            'dimension_counts': validation_result['statistics']['by_dimension'],
            'report_id': reporter.report_id,
            'firebase_uploaded': enable_firebase_upload,
            's3_uploaded': enable_s3_upload
        }
        
    except Exception as e:
        print(f"❌ Error en control de calidad: {e}")
        import traceback
        traceback.print_exc()
        return None


# Pipeline principal
def create_unidades_proyecto_pipeline() -> Callable[[], Dict[str, Any]]:
    """
    Crea el pipeline completo para unidades de proyecto.
    
    Returns:
        Función del pipeline configurada
    """
    
    def pipeline(collection_name: str = "unidades_proyecto") -> Dict[str, Any]:
        """
        Pipeline ETL completo para unidades de proyecto con carga incremental.
        
        Args:
            collection_name: Nombre de la colección en Firebase
            
        Returns:
            Diccionario con resultados del pipeline
        """
        pipeline_start = datetime.now()
        
        results = {
            'success': False,
            'start_time': pipeline_start.isoformat(),
            'end_time': None,
            'duration_seconds': None,
            'extraction_success': False,
            'transformation_success': False,
            'incremental_check_success': False,
            'load_success': False,
            'records_processed': 0,
            'records_uploaded': 0,
            'change_summary': None,
            'errors': []
        }
        
        try:
            print("🚀 INICIANDO PIPELINE ETL UNIDADES DE PROYECTO")
            print("="*80)
            print(f"⏰ Inicio: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"🗂️ Colección destino: {collection_name}")
            
            # PASO 1: Extracción
            extraction_result = run_extraction()
            if extraction_result is None or (hasattr(extraction_result, 'empty') and extraction_result.empty):
                results['errors'].append("Fallo en extracción de datos")
                return results
            
            results['extraction_success'] = True
            results['records_processed'] = len(extraction_result) if extraction_result is not None else 0
            
            # PASO 2: Transformación (pasar datos extraídos para evitar duplicación)
            transformation_result = run_transformation(extracted_data=extraction_result)
            if transformation_result is None or (hasattr(transformation_result, 'empty') and transformation_result.empty):
                results['errors'].append("Fallo en transformación de datos")
                return results
            
            results['transformation_success'] = True
            
            # PASO 3: Verificación incremental
            geojson_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "transformation_app",
                "app_outputs",
                "unidades_proyecto_outputs",
                "unidades_proyecto.geojson"
            )
            
            incremental_path, change_summary = verify_and_prepare_incremental_load(
                geojson_path, collection_name
            )
            
            results['incremental_check_success'] = True
            results['change_summary'] = change_summary
            
            # PASO 4: Carga incremental (solo si hay cambios)
            if incremental_path and change_summary:
                load_success = run_incremental_load(incremental_path, collection_name)
                results['load_success'] = load_success
                
                if load_success and change_summary:
                    results['records_uploaded'] = (
                        change_summary.get('new_records', 0) + 
                        change_summary.get('modified_records', 0)
                    )
                
                # Limpiar archivo incremental temporal
                try:
                    if os.path.exists(incremental_path):
                        os.remove(incremental_path)
                        print(f"🗑️ Archivo temporal eliminado: {os.path.basename(incremental_path)}")
                except:
                    pass
                    
            else:
                print("✅ No hay cambios para cargar - datos actualizados")
                results['load_success'] = True  # No había nada que cargar
                results['records_uploaded'] = 0
            
            # PASO 5: Control de Calidad sobre datos COMPLETOS en Firebase
            # (Se ejecuta DESPUÉS de la carga para validar el conjunto completo)
            if results['load_success']:
                print(f"\n{'='*60}")
                print("📊 PASO 5: CONTROL DE CALIDAD (DATOS COMPLETOS)")
                print("="*60)
                
                # Esperar 3 segundos para que Firebase complete conversiones internas
                print("\n⏳ Esperando 3 segundos para que Firebase complete conversiones...")
                import time
                time.sleep(3)
                print("   ✓ Continuando con análisis de calidad\n")
                
                quality_result = run_quality_control_on_firebase_data(
                    collection_name=collection_name,
                    enable_firebase_upload=True,
                    enable_s3_upload=True,
                    verbose=True
                )
                
                if quality_result:
                    results['quality_control'] = quality_result
                    print(f"\n✅ Control de calidad completado")
                else:
                    print(f"\n⚠️ Control de calidad falló, pero el pipeline continúa")
            
            # Calcular resultados finales
            pipeline_end = datetime.now()
            results['end_time'] = pipeline_end.isoformat()
            results['duration_seconds'] = (pipeline_end - pipeline_start).total_seconds()
            results['success'] = (
                results['extraction_success'] and 
                results['transformation_success'] and 
                results['incremental_check_success'] and 
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
    print("📊 RESUMEN DEL PIPELINE ETL")
    print("="*80)
    
    # Estado general
    status_icon = "✅" if results['success'] else "❌"
    print(f"{status_icon} Estado general: {'EXITOSO' if results['success'] else 'FALLIDO'}")
    
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
        ("Extracción", results['extraction_success']),
        ("Transformación", results['transformation_success']),
        ("Verificación incremental", results['incremental_check_success']),
        ("Carga a Firebase", results['load_success'])
    ]
    
    for step_name, success in steps:
        icon = "✅" if success else "❌"
        print(f"  {icon} {step_name}")
    
    # Estadísticas de datos
    print(f"\n📈 Estadísticas:")
    print(f"  📥 Registros procesados: {results.get('records_processed', 0)}")
    print(f"  📤 Registros cargados: {results.get('records_uploaded', 0)}")
    
    # Resumen de cambios
    if results.get('change_summary'):
        summary = results['change_summary']
        print(f"\n🔄 Resumen de cambios:")
        print(f"  ➕ Nuevos: {summary.get('new_records', 0)}")
        print(f"  🔄 Modificados: {summary.get('modified_records', 0)}")
        print(f"  ✅ Sin cambios: {summary.get('unchanged_records', 0)}")
    
    # Errores
    if results.get('errors'):
        print(f"\n❌ Errores encontrados:")
        for i, error in enumerate(results['errors'], 1):
            print(f"  {i}. {error}")
    
    # Mensaje final
    if results['success']:
        print(f"\n🎉 Pipeline completado exitosamente!")
        if results.get('records_uploaded', 0) > 0:
            print(f"✨ {results['records_uploaded']} registros actualizados en Firebase")
        else:
            print("✨ Todos los datos estaban actualizados")
    else:
        print(f"\n💥 Pipeline falló. Revisa los errores arriba.")
    
    print("="*80)


def run_unidades_proyecto_pipeline(collection_name: str = "unidades_proyecto") -> bool:
    """
    Función principal para ejecutar el pipeline completo de unidades de proyecto.
    
    Args:
        collection_name: Nombre de la colección en Firebase
        
    Returns:
        True si el pipeline fue exitoso, False en caso contrario
    """
    # Crear y ejecutar pipeline
    pipeline = create_unidades_proyecto_pipeline()
    results = pipeline(collection_name)
    
    # Mostrar resumen
    print_pipeline_summary(results)
    
    return results['success']


def main():
    """Función principal para pruebas del pipeline."""
    return run_unidades_proyecto_pipeline()


if __name__ == "__main__":
    """
    Bloque de ejecución principal para probar el pipeline completo.
    """
    print("🚀 Iniciando pipeline ETL de Unidades de Proyecto...")
    
    # Ejecutar pipeline completo
    success = main()
    
    if success:
        print("\n🎯 PIPELINE COMPLETADO EXITOSAMENTE")
        print("✨ Datos de unidades de proyecto actualizados")
    else:
        print("\n💥 PIPELINE FALLÓ")
        print("🔧 Revisa los errores y logs anteriores")
    
    # Código de salida para scripts automatizados
    sys.exit(0 if success else 1)
