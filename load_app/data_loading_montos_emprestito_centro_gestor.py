"""
Data Loading Module for Montos Empréstito por Centro Gestor - Firebase

Este módulo carga los datos de montos asignados de empréstito por centro gestor
desde archivo Excel hacia Firebase Firestore.

Colección: montos_emprestito_asginados_centro_gestor
Origen: app_inputs/emprestito_inputs/asignado_banco_centro_gestor/asignado_banco_centro_gestor.xlsx
"""

import pandas as pd
import os
import sys
import hashlib
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from tqdm import tqdm

# Importar el módulo de configuración de Firebase
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.config import get_firestore_client, test_connection, setup_firebase


# Configuración
COLLECTION_NAME = "montos_emprestito_asignados_centro_gestor"
INPUT_FILE = "app_inputs/emprestito_inputs/asignado_banco_centro_gestor/asignado_banco_centro_gestor.xlsx"


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
            if k not in ['updated_at', 'created_at', 'data_hash']
        }
        
        # Convertir a JSON string ordenado para hash consistente
        import json
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
    Compara registros nuevos con existentes y filtra solo los que han cambiado.
    
    Args:
        new_records: Lista de registros nuevos a comparar
        existing_data: Diccionario con {doc_id: hash} de registros existentes
        collection_name: Nombre de la colección (para el doc_id)
        
    Returns:
        Tupla (registros_a_actualizar, estadísticas)
    """
    print(f"🔄 Comparando {len(new_records)} registros con Firebase...")
    
    records_to_update = []
    stats = {
        'nuevos': 0,
        'modificados': 0,
        'sin_cambios': 0
    }
    
    for record in new_records:
        # Generar ID único basado en banco + centro_gestor + bp + año
        doc_id = f"{record['banco']}_{record['bp']}_{record['anio']}"
        doc_id = doc_id.replace(' ', '_').replace('/', '_').replace('.', '_')
        
        # Calcular hash del nuevo registro
        new_hash = calculate_record_hash(record)
        
        # Añadir el hash al registro
        record['data_hash'] = new_hash
        record['doc_id'] = doc_id
        
        # Comparar con existente
        if doc_id not in existing_data:
            # Registro nuevo
            stats['nuevos'] += 1
            records_to_update.append(record)
        elif existing_data[doc_id] != new_hash:
            # Registro modificado
            stats['modificados'] += 1
            records_to_update.append(record)
        else:
            # Sin cambios
            stats['sin_cambios'] += 1
    
    print(f"📊 Análisis de cambios:")
    print(f"   🆕 Nuevos: {stats['nuevos']}")
    print(f"   ✏️ Modificados: {stats['modificados']}")
    print(f"   ⏭️ Sin cambios: {stats['sin_cambios']}")
    print(f"   📤 Total a actualizar: {len(records_to_update)}")
    
    return records_to_update, stats


def load_excel_data(file_path: str) -> pd.DataFrame:
    """
    Carga datos desde archivo Excel.
    
    Args:
        file_path: Ruta al archivo Excel
        
    Returns:
        DataFrame con los datos
    """
    print(f"📂 Cargando datos desde: {file_path}")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
    
    df = pd.read_excel(file_path)
    print(f"✅ Cargados {len(df)} registros desde Excel")
    print(f"📋 Columnas: {df.columns.tolist()}")
    
    return df


def transform_data(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Transforma el DataFrame en lista de diccionarios para Firebase.
    
    Args:
        df: DataFrame con datos
        
    Returns:
        Lista de diccionarios preparados para Firebase
    """
    print("🔄 Transformando datos...")
    
    records = []
    timestamp = datetime.now().isoformat()
    
    for idx, row in df.iterrows():
        record = {
            'banco': str(row['banco']).strip(),
            'nombre_centro_gestor': str(row['nombre_centro_gestor']).strip(),
            'bp': str(row['bp']).strip(),
            'anio': int(row['anio']),
            'monto_programado': float(row['monto_programado']),
            'created_at': timestamp,
            'updated_at': timestamp
        }
        
        records.append(record)
    
    print(f"✅ Transformados {len(records)} registros")
    return records


def upload_to_firebase(
    records: List[Dict[str, Any]], 
    collection_name: str,
    batch_size: int = 500
) -> Dict[str, int]:
    """
    Sube registros a Firebase en lotes.
    
    Args:
        records: Lista de registros a subir
        collection_name: Nombre de la colección
        batch_size: Tamaño del lote (máximo 500 para Firestore)
        
    Returns:
        Diccionario con estadísticas de carga
    """
    print(f"🚀 Iniciando carga a Firebase: {collection_name}")
    
    db = get_firestore_client()
    if not db:
        print("❌ No se pudo conectar a Firebase")
        return {'success': 0, 'errors': 0}
    
    stats = {'success': 0, 'errors': 0}
    
    # Procesar en lotes
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    for i in tqdm(range(0, len(records), batch_size), 
                  desc="Cargando lotes", 
                  total=total_batches):
        
        batch_records = records[i:i + batch_size]
        batch = db.batch()
        
        for record in batch_records:
            try:
                doc_id = record.pop('doc_id')  # Extraer el ID
                doc_ref = db.collection(collection_name).document(doc_id)
                
                # Actualizar timestamp
                record['updated_at'] = datetime.now().isoformat()
                
                batch.set(doc_ref, record, merge=True)
                
            except Exception as e:
                print(f"⚠️ Error preparando registro {record.get('bp', 'unknown')}: {e}")
                stats['errors'] += 1
        
        # Commit del batch
        try:
            batch.commit()
            stats['success'] += len(batch_records) - (stats['errors'] - 
                                                      (i // batch_size * 
                                                       sum([1 for r in records[:i] if r])))
        except Exception as e:
            print(f"❌ Error en commit del batch {i//batch_size + 1}: {e}")
            stats['errors'] += len(batch_records)
    
    print(f"\n✅ Carga completada:")
    print(f"   ✔️ Exitosos: {stats['success']}")
    print(f"   ❌ Errores: {stats['errors']}")
    
    return stats


def load_montos_emprestito_centro_gestor(
    file_path: Optional[str] = None,
    collection_name: Optional[str] = None,
    skip_comparison: bool = False
) -> Dict[str, Any]:
    """
    Función principal para cargar datos de montos de empréstito por centro gestor.
    
    Args:
        file_path: Ruta al archivo Excel (usa default si es None)
        collection_name: Nombre de la colección (usa default si es None)
        skip_comparison: Si True, salta la comparación y carga todo
        
    Returns:
        Diccionario con resultados de la operación
    """
    print("=" * 80)
    print("CARGA DE MONTOS EMPRÉSTITO POR CENTRO GESTOR A FIREBASE")
    print("=" * 80)
    
    # Usar valores por defecto si no se especifican
    file_path = file_path or INPUT_FILE
    collection_name = collection_name or COLLECTION_NAME
    
    try:
        # 1. Cargar datos desde Excel
        df = load_excel_data(file_path)
        
        # 2. Transformar datos
        records = transform_data(df)
        
        if not records:
            print("⚠️ No hay registros para cargar")
            return {'status': 'no_data', 'records': 0}
        
        # 3. Comparar con Firebase (si no se salta)
        if not skip_comparison:
            existing_data = get_existing_firebase_data(collection_name)
            records_to_upload, comparison_stats = compare_and_filter_changes(
                records, existing_data, collection_name
            )
        else:
            print("⏩ Saltando comparación - cargando todos los registros")
            records_to_upload = records
            comparison_stats = {'nuevos': len(records), 'modificados': 0, 'sin_cambios': 0}
        
        # 4. Subir a Firebase
        if records_to_upload:
            upload_stats = upload_to_firebase(records_to_upload, collection_name)
            
            return {
                'status': 'success',
                'total_records': len(records),
                'uploaded': len(records_to_upload),
                'comparison': comparison_stats,
                'upload': upload_stats
            }
        else:
            print("✨ No hay cambios que subir a Firebase")
            return {
                'status': 'no_changes',
                'total_records': len(records),
                'uploaded': 0,
                'comparison': comparison_stats
            }
            
    except Exception as e:
        print(f"❌ Error en el proceso: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}


if __name__ == "__main__":
    """
    Ejecutar script directamente para cargar datos.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Cargar montos de empréstito por centro gestor a Firebase')
    parser.add_argument('--file', type=str, help='Ruta al archivo Excel')
    parser.add_argument('--collection', type=str, help='Nombre de la colección en Firebase')
    parser.add_argument('--skip-comparison', action='store_true', 
                       help='Saltar comparación y cargar todo')
    
    args = parser.parse_args()
    
    # Ejecutar carga
    result = load_montos_emprestito_centro_gestor(
        file_path=args.file,
        collection_name=args.collection,
        skip_comparison=args.skip_comparison
    )
    
    print("\n" + "=" * 80)
    print(f"RESULTADO: {result['status'].upper()}")
    print("=" * 80)
