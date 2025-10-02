"""
Data Loading Module for Procesos Empréstito - Firebase

Este módulo carga únicamente los datos de procesos de empréstito
desde archivos JSON transformados hacia Firebase Firestore.
"""

import json
import os
import sys
import hashlib
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from tqdm import tqdm

# Importar el módulo de configuración de Firebase
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.config import get_firestore_client, test_connection, setup_firebase


# Funciones para verificación incremental de datos
def calculate_record_hash(record: Dict[str, Any]) -> str:
    """
    Calcula un hash único para un registro para detectar cambios.
    
    Args:
        record: Diccionario con los datos del registro
        
    Returns:
        Hash MD5 del registro como string
    """
    try:
        # Crear una copia limpia del registro sin campos de metadatos
        hash_data = {
            k: v for k, v in record.items() 
            if k not in ['updated_at', 'created_at', 'data_hash', 'fecha_carga', 'origen_archivo']
        }
        
        # Convertir a JSON string ordenado para hash consistente
        record_str = json.dumps(hash_data, sort_keys=True, default=str)
        
        # Calcular hash MD5
        return hashlib.md5(record_str.encode('utf-8')).hexdigest()
        
    except Exception as e:
        print(f"⚠️ Error calculando hash para registro: {e}")
        return ""


def get_existing_firebase_data(collection_name: str) -> Dict[str, str]:
    """
    Obtiene los hashes de los datos existentes en Firebase para comparación.
    
    Args:
        collection_name: Nombre de la colección en Firebase
        
    Returns:
        Diccionario con {doc_id: hash} o {} si falla
    """
    print(f"🔍 Obteniendo datos existentes de Firebase colección '{collection_name}'...")
    
    try:
        db = get_firestore_client()
        if not db:
            print("❌ No se pudo conectar a Firebase")
            return {}
        
        collection_ref = db.collection(collection_name)
        existing_data = {}
        
        # Obtener solo los campos necesarios para comparación (más eficiente)
        docs = collection_ref.stream()
        doc_count = 0
        
        for doc in docs:
            doc_data = doc.to_dict()
            
            # Si ya tiene hash almacenado, usarlo; si no, calcularlo
            if 'data_hash' in doc_data:
                existing_data[doc.id] = doc_data['data_hash']
            else:
                # Calcular hash de los datos existentes
                existing_data[doc.id] = calculate_record_hash(doc_data)
            
            doc_count += 1
        
        print(f"✅ Obtenidos {doc_count} registros existentes de Firebase")
        return existing_data
        
    except Exception as e:
        print(f"❌ Error obteniendo datos de Firebase: {e}")
        return {}


def compare_and_filter_changes(
    new_records: List[Dict[str, Any]], 
    existing_data: Dict[str, str],
    collection_name: str
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Compara datos nuevos con existentes y filtra solo los cambios.
    
    Args:
        new_records: Lista de registros nuevos
        existing_data: Hashes de datos existentes en Firebase
        collection_name: Nombre de la colección para generar doc_id
        
    Returns:
        Tupla de (records_to_upload, change_summary)
    """
    print(f"🔄 Comparando {len(new_records)} registros nuevos con {len(existing_data)} existentes...")
    
    records_to_upload = []
    change_summary = {
        'new_records': 0,
        'modified_records': 0,
        'unchanged_records': 0,
        'total_processed': len(new_records)
    }
    
    for record in new_records:
        try:
            # Generar ID del documento usando la misma lógica que en load_to_firestore
            doc_id = generate_document_id(record, collection_name)
            
            # Calcular hash del nuevo registro
            new_hash = calculate_record_hash(record)
            
            # Verificar si existe en Firebase
            if doc_id in existing_data:
                existing_hash = existing_data[doc_id]
                
                if new_hash != existing_hash:
                    # Registro modificado - añadir hash al registro
                    record['data_hash'] = new_hash
                    records_to_upload.append(record)
                    change_summary['modified_records'] += 1
                else:
                    # Registro sin cambios
                    change_summary['unchanged_records'] += 1
            else:
                # Registro nuevo - añadir hash al registro
                record['data_hash'] = new_hash
                records_to_upload.append(record)
                change_summary['new_records'] += 1
                
        except Exception as e:
            print(f"⚠️ Error comparando registro: {e}")
            # En caso de error, incluir el registro para estar seguros
            record['data_hash'] = calculate_record_hash(record)
            records_to_upload.append(record)
            change_summary['new_records'] += 1
    
    print(f"📊 Resumen de cambios para {collection_name}:")
    print(f"  ➕ Nuevos: {change_summary['new_records']}")
    print(f"  🔄 Modificados: {change_summary['modified_records']}")
    print(f"  ✅ Sin cambios: {change_summary['unchanged_records']}")
    print(f"  📤 Total a cargar: {len(records_to_upload)}")
    
    return records_to_upload, change_summary


def get_data_files() -> Dict[str, str]:
    """
    Obtiene las rutas al archivo JSON de datos transformados de procesos.
    
    Returns:
        Dict[str, str]: Diccionario con la ruta del archivo de procesos
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    emprestito_outputs_dir = os.path.join(
        base_dir, 
        "transformation_app", 
        "app_outputs", 
        "emprestito_outputs"
    )
    
    return {
        'procesos': os.path.join(emprestito_outputs_dir, "procesos_secop_emprestito_transformed.json")
    }


def load_json_data(file_path: str, data_type: str) -> List[Dict[str, Any]]:
    """
    Carga los datos desde un archivo JSON.
    
    Args:
        file_path (str): Ruta al archivo JSON
        data_type (str): Tipo de datos (procesos)
        
    Returns:
        List[Dict[str, Any]]: Lista de registros
        
    Raises:
        FileNotFoundError: Si el archivo no existe
        json.JSONDecodeError: Si el archivo no es JSON válido
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Archivo {data_type} no encontrado: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        if not isinstance(data, list):
            raise ValueError(f"El archivo {data_type} debe contener una lista de registros")
        
        print(f"📊 Datos de {data_type} cargados exitosamente: {len(data)} registros")
        return data
        
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Error al decodificar JSON de {data_type}: {e}")


def prepare_document_for_firestore(record: Dict[str, Any], data_type: str) -> Dict[str, Any]:
    """
    Prepara un registro para ser almacenado en Firestore.
    Firestore tiene restricciones sobre tipos de datos y valores None.
    
    Args:
        record (Dict[str, Any]): Registro original
        data_type (str): Tipo de datos (procesos)
        
    Returns:
        Dict[str, Any]: Registro preparado para Firestore
    """
    prepared_record = {}
    
    for key, value in record.items():
        # Reemplazar valores None por valores apropiados
        if value is None:
            if key in ['bpin', 'proceso_compra', 'valor_total_adjudicacion', 'precio_base', 'ppi']:
                prepared_record[key] = 0
            elif key in ['nombre_centro_gestor', 'referencia_proceso', 'entidad', 'nombre_procedimiento']:
                prepared_record[key] = ""
            elif key in ['fecha_publicacion', 'fecha_ultima_publicaci', 'fecha_recepcion', 'fecha_apertura_efectiva']:
                prepared_record[key] = ""
            else:
                prepared_record[key] = ""
        else:
            prepared_record[key] = value
    
    # Agregar metadatos de carga
    prepared_record['fecha_carga'] = datetime.now().isoformat()
    prepared_record['updated_at'] = datetime.now().isoformat()
    prepared_record['origen_archivo'] = f"{data_type}_secop_emprestito_transformed.json"
    prepared_record['tipo_datos'] = data_type
    
    return prepared_record


def generate_document_id(record: Dict[str, Any], collection_name: str) -> str:
    """
    Genera un ID único para el documento basado en la referencia del proceso.
    
    Args:
        record (Dict[str, Any]): Registro de datos
        collection_name (str): Nombre de la colección
        
    Returns:
        str: ID único para el documento
    """
    # Para procesos, usar referencia_proceso como identificador principal
    if record.get('referencia_proceso'):
        return f"PROCESO-{record['referencia_proceso']}"
    elif record.get('proceso_compra'):
        return f"PROCESO-{record['proceso_compra']}"
    elif record.get('id_proceso'):
        return f"PROCESO-{record['id_proceso']}"
    else:
        # Usar timestamp como fallback
        timestamp = int(datetime.now().timestamp())
        return f"PROCESO-{timestamp}"


def upload_data_to_firestore(data: List[Dict[str, Any]], 
                           collection_name: str, 
                           data_type: str) -> bool:
    """
    Carga los datos a Firestore usando lotes optimizados.
    
    Args:
        data (List[Dict[str, Any]]): Datos a cargar
        collection_name (str): Nombre de la colección en Firestore
        data_type (str): Tipo de datos (procesos)
        
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
        
        print(f"🚀 Iniciando carga de {len(data)} {data_type} a Firestore...")
        print(f"📦 Procesando en lotes de {batch_size} registros")
        print(f"📋 Colección destino: {collection_name}")
        
        # Procesar registros en lotes con barra de progreso
        total_batches = (len(data) + batch_size - 1) // batch_size
        
        with tqdm(total=total_batches, desc=f"Cargando {data_type}") as pbar:
            for i in range(0, len(data), batch_size):
                batch = client.batch()
                batch_data = data[i:i + batch_size]
                batch_errors = 0
                
                for record in batch_data:
                    try:
                        # Preparar documento para Firestore
                        prepared_record = prepare_document_for_firestore(record, data_type)
                        
                        # Generar ID único para el documento
                        doc_id = generate_document_id(record, collection_name)
                        doc_ref = collection_ref.document(doc_id)
                        
                        # Agregar al batch
                        batch.set(doc_ref, prepared_record)
                        
                    except Exception as e:
                        print(f"❌ Error preparando registro {data_type}: {e}")
                        batch_errors += 1
                
                # Ejecutar el batch si hay documentos válidos
                if len(batch_data) - batch_errors > 0:
                    try:
                        batch.commit()
                        batch_success = len(batch_data) - batch_errors
                        success_count += batch_success
                        error_count += batch_errors
                        
                    except Exception as e:
                        print(f"❌ Error ejecutando lote {i//batch_size + 1}: {e}")
                        error_count += len(batch_data)
                else:
                    error_count += len(batch_data)
                
                pbar.update(1)
        
        print(f"\n📈 Resumen de carga de {data_type}:")
        print(f"   ✅ Registros exitosos: {success_count}")
        print(f"   ❌ Registros con error: {error_count}")
        print(f"   📊 Total procesados: {len(data)}")
        print(f"   📋 Colección: {collection_name}")
        
        return error_count == 0
        
    except Exception as e:
        print(f"❌ Error general en la carga de {data_type} a Firestore: {e}")
        return False


def verify_data_upload(collection_name: str, data_type: str) -> Dict[str, Any]:
    """
    Verifica que los datos se hayan cargado correctamente en Firestore.
    
    Args:
        collection_name (str): Nombre de la colección a verificar
        data_type (str): Tipo de datos verificados
        
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
            "tipo_datos": data_type,
            "muestra_documentos": sample_data,
            "verificacion_exitosa": doc_count > 0
        }
        
        print(f"\n🔍 Verificación de {data_type} cargados:")
        print(f"   📊 Total documentos en '{collection_name}': {doc_count}")
        print(f"   ✅ Carga verificada: {'Sí' if doc_count > 0 else 'No'}")
        
        if sample_data:
            print(f"   📋 Muestra de {data_type} (primeros 3):")
            for i, doc in enumerate(sample_data, 1):
                ref = doc.get('referencia_proceso', 'N/A')
                procedimiento = doc.get('nombre_procedimiento', 'N/A')[:50]
                estado = doc.get('estado_procedimiento', 'N/A')
                print(f"      {i}. Ref: {ref}")
                print(f"         Procedimiento: {procedimiento}...")
                print(f"         Estado: {estado}")
        
        return verification_info
        
    except Exception as e:
        print(f"❌ Error en la verificación de {data_type}: {e}")
        return {
            "total_documentos": 0,
            "coleccion": collection_name,
            "tipo_datos": data_type,
            "verificacion_exitosa": False,
            "error": str(e)
        }


def load_procesos_emprestito_data() -> Optional[Dict[str, Any]]:
    """
    Función principal para cargar datos de procesos empréstito a Firebase.
    
    Returns:
        Optional[Dict[str, Any]]: Información del resultado de la carga
    """
    try:
        print("🔥 Iniciando carga de datos de procesos empréstito a Firebase")
        print("="*80)
        
        # Configurar y verificar conexión Firebase
        if not setup_firebase():
            print("❌ No se pudo establecer conexión con Firebase")
            return None
        
        print(f"✅ Conexión a Firebase establecida correctamente")
        
        # Obtener rutas de archivos
        data_files = get_data_files()
        print(f"\n📁 Archivo a procesar:")
        for data_type, file_path in data_files.items():
            print(f"   {data_type}: {file_path}")
        
        # Verificar que el archivo existe
        missing_files = []
        for data_type, file_path in data_files.items():
            if not os.path.exists(file_path):
                missing_files.append((data_type, file_path))
        
        if missing_files:
            print(f"\n❌ Archivo faltante:")
            for data_type, file_path in missing_files:
                print(f"   {data_type}: {file_path}")
            print("🔧 Ejecuta primero el script de transformación para generar el archivo")
            return {
                "status": "error",
                "error": "Archivo de datos no encontrado",
                "missing_files": missing_files
            }
        
        results = {
            "status": "success",
            "procesos": None,
            "total_records": 0
        }
        
        # Cargar procesos con verificación incremental
        print(f"\n{'='*20} CARGANDO PROCESOS {'='*20}")
        try:
            procesos_data = load_json_data(data_files['procesos'], 'procesos')
            if procesos_data:
                # Obtener datos existentes de Firebase
                existing_procesos = get_existing_firebase_data("procesos_emprestito")
                
                # Comparar y filtrar solo cambios
                procesos_to_upload, procesos_changes = compare_and_filter_changes(
                    procesos_data, 
                    existing_procesos,
                    "procesos_emprestito"
                )
                
                # Cargar solo registros nuevos o modificados
                if procesos_to_upload:
                    procesos_success = upload_data_to_firestore(
                        procesos_to_upload, 
                        "procesos_emprestito", 
                        "procesos"
                    )
                else:
                    print("✅ No hay procesos nuevos o modificados para cargar")
                    procesos_success = True
                
                procesos_verification = verify_data_upload("procesos_emprestito", "procesos")
                
                results["procesos"] = {
                    "records_processed": len(procesos_data),
                    "records_uploaded": len(procesos_to_upload),
                    "change_summary": procesos_changes,
                    "upload_success": procesos_success,
                    "verification": procesos_verification
                }
                results["total_records"] += len(procesos_data)
        except Exception as e:
            print(f"❌ Error cargando procesos: {e}")
            results["procesos"] = {"error": str(e)}
        
        # Determinar estado final basado solo en procesos
        procesos_ok = results["procesos"] and results["procesos"].get("upload_success", False)
        
        if procesos_ok:
            results["status"] = "success"
        else:
            results["status"] = "error"
        
        return results
        
    except Exception as e:
        print(f"💥 Error crítico en la carga: {e}")
        return {
            "status": "error",
            "error": str(e)
        }


# Ejecutar carga si se ejecuta este módulo directamente
if __name__ == "__main__":
    print("=" * 80)
    print("🚀 CARGA DE DATOS DE PROCESOS EMPRÉSTITO A FIREBASE")
    print("=" * 80)
    
    result = load_procesos_emprestito_data()
    
    if result:
        print("\n" + "=" * 80)
        print("📋 RESUMEN FINAL:")
        print("=" * 80)
        print(f"Estado general: {result['status']}")
        
        if result.get('procesos'):
            procesos_info = result['procesos']
            if 'records_processed' in procesos_info:
                print(f"📦 Procesos procesados: {procesos_info['records_processed']}")
                print(f"📤 Procesos cargados: {procesos_info.get('records_uploaded', 0)}")
                if 'change_summary' in procesos_info:
                    changes = procesos_info['change_summary']
                    print(f"   ➕ Nuevos: {changes.get('new_records', 0)}")
                    print(f"   🔄 Modificados: {changes.get('modified_records', 0)}")
                    print(f"   ✅ Sin cambios: {changes.get('unchanged_records', 0)}")
                print(f"✅ Procesos cargados: {'Sí' if procesos_info.get('upload_success') else 'No'}")
        
        if result.get('total_records'):
            print(f"📊 Total registros: {result['total_records']}")
        
        print(f"📋 Colección: procesos_emprestito")
        print("=" * 80)
    else:
        print("\n💥 La carga de datos falló completamente")
