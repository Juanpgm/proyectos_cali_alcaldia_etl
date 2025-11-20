"""
Data Loading Module for Budget Projects (BP) - Firebase

Este módulo carga los datos característicos de proyectos presupuestales
desde archivos JSON transformados hacia Firebase Firestore.
"""

import json
import os
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime

# Importar el módulo de configuración de Firebase
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.config import get_firestore_client, test_connection, setup_firebase


def get_json_file_path() -> str:
    """
    Obtiene la ruta al archivo JSON de datos característicos de proyectos.
    
    Returns:
        str: Ruta absoluta al archivo JSON
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(
        base_dir, 
        "transformation_app", 
        "app_outputs", 
        "ejecucion_presupuestal_outputs", 
        "datos_caracteristicos_proyectos.json"
    )


def load_json_data(file_path: str) -> List[Dict[str, Any]]:
    """
    Carga los datos desde el archivo JSON.
    
    Args:
        file_path (str): Ruta al archivo JSON
        
    Returns:
        List[Dict[str, Any]]: Lista de registros de proyectos
        
    Raises:
        FileNotFoundError: Si el archivo no existe
        json.JSONDecodeError: Si el archivo no es JSON válido
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Archivo JSON no encontrado: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        if not isinstance(data, list):
            raise ValueError("El archivo JSON debe contener una lista de registros")
        
        print(f"📊 Datos cargados exitosamente: {len(data)} registros")
        return data
        
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Error al decodificar JSON: {e}")


def prepare_document_for_firestore(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepara un registro para ser almacenado en Firestore.
    Firestore tiene restricciones sobre tipos de datos y valores None.
    
    Args:
        record (Dict[str, Any]): Registro original
        
    Returns:
        Dict[str, Any]: Registro preparado para Firestore
    """
    prepared_record = {}
    
    for key, value in record.items():
        # Reemplazar valores None por cadenas vacías o valores por defecto
        if value is None:
            if key in ['cod_sector', 'cod_producto', 'validador_cuipo']:
                prepared_record[key] = ""
            elif key in ['nombre_dimension', 'nombre_linea_estrategica']:
                prepared_record[key] = ""
            else:
                prepared_record[key] = ""
        else:
            prepared_record[key] = value
    
    # Agregar metadatos de carga
    prepared_record['fecha_carga'] = datetime.now().isoformat()
    prepared_record['origen_archivo'] = "datos_caracteristicos_proyectos.json"
    
    return prepared_record


def upload_data_to_firestore(data: List[Dict[str, Any]], collection_name: str = "proyectos_presupuestales") -> bool:
    """
    Carga los datos a Firestore usando lotes optimizados.
    
    Args:
        data (List[Dict[str, Any]]): Datos a cargar
        collection_name (str): Nombre de la colección en Firestore
        
    Returns:
        bool: True si la carga fue exitosa, False en caso contrario
    """
    try:
        # Obtener cliente de Firestore
        client = get_firestore_client()
        collection_ref = client.collection(collection_name)
        
        success_count = 0
        error_count = 0
        batch_size = 500  # Tamaño de lote optimizado para Firestore
        
        print(f"🚀 Iniciando carga de {len(data)} registros a Firestore...")
        print(f"📦 Procesando en lotes de {batch_size} registros")
        
        # Procesar registros en lotes
        for i in range(0, len(data), batch_size):
            batch = client.batch()
            batch_data = data[i:i + batch_size]
            batch_errors = 0
            
            for record in batch_data:
                try:
                    # Preparar documento para Firestore
                    prepared_record = prepare_document_for_firestore(record)
                    
                    # Usar BPIN como ID del documento (debe ser único)
                    if 'bpin' not in prepared_record or not prepared_record['bpin']:
                        print(f"⚠️  Registro sin BPIN válido, generando ID automático")
                        doc_ref = collection_ref.document()
                    else:
                        doc_id = str(prepared_record['bpin']).strip()
                        doc_ref = collection_ref.document(doc_id)
                    
                    # Agregar al batch
                    batch.set(doc_ref, prepared_record)
                    
                except Exception as e:
                    print(f"❌ Error preparando registro {record.get('bpin', 'N/A')}: {e}")
                    batch_errors += 1
            
            # Ejecutar el batch si hay documentos válidos
            if len(batch_data) - batch_errors > 0:
                try:
                    batch.commit()
                    batch_success = len(batch_data) - batch_errors
                    success_count += batch_success
                    error_count += batch_errors
                    print(f"✅ Lote {i//batch_size + 1}: {batch_success} registros cargados")
                    
                except Exception as e:
                    print(f"❌ Error ejecutando lote {i//batch_size + 1}: {e}")
                    error_count += len(batch_data)
            else:
                error_count += len(batch_data)
                print(f"❌ Lote {i//batch_size + 1}: sin registros válidos para cargar")
        
        print(f"\n📈 Resumen de carga:")
        print(f"   ✅ Registros exitosos: {success_count}")
        print(f"   ❌ Registros con error: {error_count}")
        print(f"   📊 Total procesados: {len(data)}")
        print(f"   📋 Colección: {collection_name}")
        
        return error_count == 0
        
    except Exception as e:
        print(f"❌ Error general en la carga a Firestore: {e}")
        return False


def verify_data_upload(collection_name: str = "proyectos_presupuestales") -> Dict[str, Any]:
    """
    Verifica que los datos se hayan cargado correctamente en Firestore.
    
    Args:
        collection_name (str): Nombre de la colección a verificar
        
    Returns:
        Dict[str, Any]: Estadísticas de verificación
    """
    try:
        db = get_firestore_client()
        collection_ref = db.collection(collection_name)
        
        # Contar documentos
        docs = collection_ref.stream()
        doc_count = sum(1 for _ in docs)
        
        # Obtener muestra de documentos
        sample_docs = list(collection_ref.limit(3).stream())
        sample_data = [doc.to_dict() for doc in sample_docs]
        
        verification_info = {
            "total_documentos": doc_count,
            "coleccion": collection_name,
            "muestra_documentos": sample_data,
            "verificacion_exitosa": doc_count > 0
        }
        
        print(f"\n🔍 Verificación de datos cargados:")
        print(f"   📊 Total documentos en '{collection_name}': {doc_count}")
        print(f"   ✅ Carga verificada: {'Sí' if doc_count > 0 else 'No'}")
        
        if sample_data:
            print(f"   📋 Muestra de documentos (primeros 3):")
            for i, doc in enumerate(sample_data, 1):
                print(f"      {i}. BPIN: {doc.get('bpin', 'N/A')} - {doc.get('nombre_proyecto', 'N/A')[:50]}...")
        
        return verification_info
        
    except Exception as e:
        print(f"❌ Error en la verificación: {e}")
        return {
            "total_documentos": 0,
            "coleccion": collection_name,
            "verificacion_exitosa": False,
            "error": str(e)
        }


def load_budget_projects_data() -> Optional[Dict[str, Any]]:
    """
    Función principal para cargar datos de proyectos presupuestales a Firebase.
    
    Returns:
        Optional[Dict[str, Any]]: Información del resultado de la carga
    """
    try:
        print("🔥 Iniciando carga de datos de proyectos presupuestales a Firebase")
        
        # Configurar y verificar conexión Firebase
        if not setup_firebase():
            print("❌ No se pudo establecer conexión con Firebase")
            return None
        
        print(f"✅ Conexión a Firebase establecida correctamente")
        
        # Obtener ruta del archivo JSON
        json_file_path = get_json_file_path()
        print(f"� Archivo a procesar: {json_file_path}")
        
        # Verificar que el archivo existe
        if not os.path.exists(json_file_path):
            print(f"❌ Archivo no encontrado: {json_file_path}")
            print("� Ejecuta primero los scripts de transformación para generar el archivo")
            return {
                "status": "error",
                "error": "Archivo de datos no encontrado"
            }
        
        # Cargar datos JSON
        data = load_json_data(json_file_path)
        
        if not data:
            print("❌ No hay datos para cargar")
            return {
                "status": "error",
                "error": "No hay datos en el archivo JSON"
            }
        
        # Cargar datos a Firestore
        upload_success = upload_data_to_firestore(data)
        
        # Verificar carga
        verification_info = verify_data_upload()
        
        # Determinar estado final
        if upload_success and verification_info["verificacion_exitosa"]:
            return {
                "status": "success",
                "records_processed": len(data),
                "collection": "proyectos_presupuestales",
                "verification": verification_info
            }
        elif verification_info["verificacion_exitosa"]:
            return {
                "status": "partial_success",
                "records_processed": len(data),
                "message": "Algunos registros no se pudieron cargar, pero hay datos en Firebase",
                "verification": verification_info
            }
        else:
            return {
                "status": "error",
                "records_processed": len(data),
                "error": "No se pudieron cargar los datos"
            }
            
    except Exception as e:
        print(f"💥 Error crítico en la carga: {e}")
        return {
            "status": "error",
            "error": str(e)
        }


# Ejecutar carga si se ejecuta este módulo directamente
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 CARGA DE DATOS DE PROYECTOS PRESUPUESTALES A FIREBASE")
    print("=" * 60)
    
    result = load_budget_projects_data()
    
    if result:
        print("\n" + "=" * 60)
        print("📋 RESUMEN FINAL:")
        print("=" * 60)
        print(f"Estado: {result['status']}")
        if 'records_processed' in result:
            print(f"Registros procesados: {result['records_processed']}")
        if 'firebase_project' in result:
            print(f"Proyecto Firebase: {result['firebase_project']}")
        if 'collection' in result:
            print(f"Colección: {result['collection']}")
        print("=" * 60)
    else:
        print("\n💥 La carga de datos falló completamente")