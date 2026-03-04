"""
Data Loading Module for Budget Projects (BP) - Firebase

Este módulo carga los datos característicos de proyectos presupuestales
desde archivos JSON transformados hacia Firebase Firestore.
"""

import json
import os
import sys
import hashlib
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


def resolve_document_id(record: Dict[str, Any]) -> Optional[str]:
    """Resuelve un ID estable para permitir UPSERT."""
    bpin = str(record.get('bpin', '')).strip()
    if bpin:
        return bpin

    fallback_parts = [
        str(record.get('bp', '')).strip(),
        str(record.get('nombre_proyecto', '')).strip(),
        str(record.get('nombre_actividad', '')).strip(),
        str(record.get('anio', '')).strip(),
        str(record.get('nombre_centro_gestor', '')).strip(),
    ]
    fallback_seed = "|".join(part for part in fallback_parts if part)
    if not fallback_seed:
        return None

    fallback_hash = hashlib.md5(fallback_seed.encode('utf-8')).hexdigest()[:16]
    return f"NO_BPIN_{fallback_hash}"


def compute_payload_hash(record: Dict[str, Any]) -> str:
    """Calcula hash estable del payload para detectar cambios reales."""
    excluded_keys = {'fecha_carga', 'origen_archivo', 'payload_hash'}
    clean_record = {k: v for k, v in record.items() if k not in excluded_keys}
    raw = json.dumps(clean_record, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(raw.encode('utf-8')).hexdigest()


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
        skipped_count = 0
        inserted_count = 0
        updated_count = 0
        batch_size = 500  # Tamaño de lote optimizado para Firestore
        
        print(f"🚀 Iniciando carga de {len(data)} registros a Firestore...")
        print(f"📦 Procesando en lotes de {batch_size} registros")
        
        # Procesar registros en lotes
        for i in range(0, len(data), batch_size):
            batch_data = data[i:i + batch_size]
            batch_errors = 0

            prepared_items: List[Dict[str, Any]] = []
            refs_to_fetch = []

            for record in batch_data:
                try:
                    prepared_record = prepare_document_for_firestore(record)
                    doc_id = resolve_document_id(prepared_record)
                    if not doc_id:
                        print("⚠️  Registro sin identificador estable, se omite para UPSERT")
                        skipped_count += 1
                        continue

                    payload_hash = compute_payload_hash(prepared_record)
                    prepared_record['payload_hash'] = payload_hash

                    doc_ref = collection_ref.document(doc_id)
                    prepared_items.append(
                        {
                            'doc_ref': doc_ref,
                            'doc_id': doc_id,
                            'payload_hash': payload_hash,
                            'record': prepared_record,
                        }
                    )
                    refs_to_fetch.append(doc_ref)
                except Exception as e:
                    print(f"❌ Error preparando registro {record.get('bpin', 'N/A')}: {e}")
                    batch_errors += 1

            if not prepared_items:
                print(f"⚠️  Lote {i//batch_size + 1}: sin candidatos UPSERT")
                error_count += batch_errors
                continue

            existing_docs = list(client.get_all(refs_to_fetch))
            existing_hash_by_id = {}
            for doc in existing_docs:
                if doc.exists:
                    doc_data = doc.to_dict() or {}
                    existing_hash_by_id[doc.id] = doc_data.get('payload_hash')

            batch = client.batch()
            writes_in_batch = 0

            for item in prepared_items:
                current_hash = existing_hash_by_id.get(item['doc_id'])
                if current_hash == item['payload_hash']:
                    skipped_count += 1
                    continue

                batch.set(item['doc_ref'], item['record'])
                writes_in_batch += 1

                if item['doc_id'] in existing_hash_by_id:
                    updated_count += 1
                else:
                    inserted_count += 1

            if writes_in_batch > 0:
                try:
                    batch.commit()
                    batch_success = writes_in_batch
                    success_count += batch_success
                    error_count += batch_errors
                    print(
                        f"✅ Lote {i//batch_size + 1}: {batch_success} escritos "
                        f"(insertados/actualizados), {batch_errors} errores, {skipped_count} sin cambios acumulados"
                    )
                except Exception as e:
                    print(f"❌ Error ejecutando lote {i//batch_size + 1}: {e}")
                    error_count += writes_in_batch + batch_errors
            else:
                error_count += batch_errors
                print(f"⏭️  Lote {i//batch_size + 1}: sin cambios para aplicar")
        
        print(f"\n📈 Resumen de carga:")
        print(f"   ✅ Registros escritos (insert/update): {success_count}")
        print(f"   🆕 Insertados: {inserted_count}")
        print(f"   🔄 Actualizados: {updated_count}")
        print(f"   ⏭️ Sin cambios: {skipped_count}")
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


def load_budget_projects_data(collection_name: str = "proyectos_presupuestales") -> Optional[Dict[str, Any]]:
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
        upload_success = upload_data_to_firestore(data, collection_name=collection_name)
        
        # Verificar carga
        verification_info = verify_data_upload(collection_name=collection_name)
        
        # Determinar estado final
        if upload_success and verification_info["verificacion_exitosa"]:
            return {
                "status": "success",
                "records_processed": len(data),
                "collection": collection_name,
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