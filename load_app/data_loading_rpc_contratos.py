# -*- coding: utf-8 -*-
"""
Firebase Data Loading Module for RPC Contratos

Carga datos de contratos RPC a Firebase Firestore:
- Operaciones batch para eficiencia
- Manejo de duplicados
- Generación de IDs únicos
- Metadata automática
- Programación funcional para operaciones seguras

Colección: rpc_contratos_emprestito
"""

import os
import sys
from typing import Dict, List, Any, Optional, Callable, Tuple
from functools import wraps, reduce
from datetime import datetime
import time
import hashlib

# Add project paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.config import get_firestore_client, secure_log, BATCH_SIZE
from tqdm import tqdm


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
                return default_value
        return wrapper
    return decorator


# Document ID generation
@safe_execute(default_value=None)
def generate_document_id(rpc_data: Dict[str, Any]) -> Optional[str]:
    """
    Generate unique document ID for RPC contract.
    
    Priority:
    1. numero_rpc (if available)
    2. Hash of key fields
    
    Args:
        rpc_data: RPC contract data
        
    Returns:
        Unique document ID or None
    """
    # Try numero_rpc first
    numero_rpc = rpc_data.get('numero_rpc')
    if numero_rpc:
        # Clean and use as ID
        doc_id = str(numero_rpc).strip().replace('/', '-').replace(' ', '_')
        return doc_id
    
    # Fallback: hash of key fields
    key_fields = {
        'documento_identificacion': rpc_data.get('documento_identificacion'),
        'contrato_rpc': rpc_data.get('contrato_rpc'),
        'fecha_contabilizacion': rpc_data.get('fecha_contabilizacion'),
        'valor_rpc': rpc_data.get('valor_rpc')
    }
    
    # Create hash
    hash_string = '-'.join([str(v) for v in key_fields.values() if v is not None])
    
    if hash_string:
        hash_id = hashlib.md5(hash_string.encode()).hexdigest()[:12]
        return f"RPC-{hash_id}"
    
    return None


# Document preparation
@safe_execute(default_value=None)
def prepare_document_for_firestore(rpc_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Prepare RPC data for Firestore storage.
    
    Args:
        rpc_data: Transformed RPC data
        
    Returns:
        Document ready for Firestore or None
    """
    # Remove validation info (not needed in Firestore)
    document = {k: v for k, v in rpc_data.items() if k != 'validation'}
    
    # Add Firestore timestamps
    document['created_at'] = datetime.now().isoformat()
    document['updated_at'] = datetime.now().isoformat()
    
    # Ensure metadata exists
    if 'metadata' not in document:
        document['metadata'] = {}
    
    document['metadata']['loaded_to_firestore_at'] = datetime.now().isoformat()
    
    return document


# Duplicate checking
@safe_execute(default_value=False)
def check_document_exists(
    collection_ref,
    doc_id: str
) -> bool:
    """
    Check if document already exists in Firestore.
    
    Args:
        collection_ref: Firestore collection reference
        doc_id: Document ID to check
        
    Returns:
        True if exists, False otherwise
    """
    doc_ref = collection_ref.document(doc_id)
    doc = doc_ref.get()
    
    return doc.exists


# Batch upload
@safe_execute(default_value=(0, 0, []))
def upload_batch_to_firestore(
    collection_ref,
    documents: List[Tuple[str, Dict[str, Any]]],
    update_existing: bool = True
) -> Tuple[int, int, List[str]]:
    """
    Upload a batch of documents to Firestore.
    
    Args:
        collection_ref: Firestore collection reference
        documents: List of (doc_id, document_data) tuples
        update_existing: Whether to update existing documents
        
    Returns:
        Tuple of (created_count, updated_count, errors)
    """
    if not documents:
        return 0, 0, []
    
    batch = collection_ref._client.batch()
    created_count = 0
    updated_count = 0
    errors = []
    
    for doc_id, doc_data in documents:
        try:
            doc_ref = collection_ref.document(doc_id)
            
            # Check if exists
            exists = check_document_exists(collection_ref, doc_id)
            
            if exists and update_existing:
                # Update existing document
                doc_data['updated_at'] = datetime.now().isoformat()
                batch.set(doc_ref, doc_data, merge=True)
                updated_count += 1
            elif not exists:
                # Create new document
                batch.set(doc_ref, doc_data)
                created_count += 1
            
        except Exception as e:
            errors.append(f"Error en documento {doc_id}: {e}")
    
    # Commit batch
    try:
        batch.commit()
    except Exception as e:
        errors.append(f"Error en commit de batch: {e}")
        return 0, 0, errors
    
    return created_count, updated_count, errors


# Main loading function
@secure_log
@safe_execute(default_value=False)
def load_rpc_to_firebase(
    rpc_data_list: List[Dict[str, Any]],
    collection_name: str = "rpc_contratos_emprestito",
    batch_size: int = BATCH_SIZE,
    update_existing: bool = True
) -> bool:
    """
    Load RPC contracts to Firebase Firestore.
    
    Args:
        rpc_data_list: List of transformed RPC data
        collection_name: Firestore collection name
        batch_size: Number of documents per batch
        update_existing: Whether to update existing documents
        
    Returns:
        True if successful, False otherwise
    """
    print(f"\n{'='*70}")
    print(f"📤 Cargando {len(rpc_data_list)} contratos RPC a Firebase")
    print(f"📁 Colección: {collection_name}")
    print("="*70)
    
    # Get Firestore client
    db = get_firestore_client()
    if not db:
        print("❌ No se pudo conectar a Firebase")
        return False
    
    collection_ref = db.collection(collection_name)
    
    # Prepare documents
    print("\n1️⃣ Preparando documentos...")
    documents_to_upload = []
    
    for rpc_data in rpc_data_list:
        # Generate document ID
        doc_id = generate_document_id(rpc_data)
        
        if not doc_id:
            print(f"⚠️ No se pudo generar ID para documento, saltando...")
            continue
        
        # Prepare document
        document = prepare_document_for_firestore(rpc_data)
        
        if not document:
            print(f"⚠️ No se pudo preparar documento {doc_id}, saltando...")
            continue
        
        documents_to_upload.append((doc_id, document))
    
    if not documents_to_upload:
        print("❌ No hay documentos para cargar")
        return False
    
    print(f"✅ {len(documents_to_upload)} documentos preparados")
    
    # Upload in batches
    print(f"\n2️⃣ Cargando en batches de {batch_size}...")
    
    total_created = 0
    total_updated = 0
    all_errors = []
    
    # Split into batches
    for i in range(0, len(documents_to_upload), batch_size):
        batch = documents_to_upload[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(documents_to_upload) + batch_size - 1) // batch_size
        
        print(f"📦 Batch {batch_num}/{total_batches} ({len(batch)} documentos)...")
        
        created, updated, errors = upload_batch_to_firestore(
            collection_ref,
            batch,
            update_existing
        )
        
        total_created += created
        total_updated += updated
        all_errors.extend(errors)
        
        if errors:
            print(f"⚠️ Errores en batch: {len(errors)}")
        
        # Small delay between batches
        if i + batch_size < len(documents_to_upload):
            time.sleep(0.5)
    
    # Summary
    print(f"\n{'='*70}")
    print("📊 RESUMEN DE CARGA")
    print("="*70)
    print(f"➕ Creados: {total_created}")
    print(f"🔄 Actualizados: {total_updated}")
    print(f"❌ Errores: {len(all_errors)}")
    print(f"📄 Total procesados: {len(documents_to_upload)}")
    
    if all_errors:
        print(f"\n⚠️ Errores encontrados:")
        for error in all_errors[:5]:  # Show first 5 errors
            print(f"   - {error}")
        if len(all_errors) > 5:
            print(f"   ... y {len(all_errors) - 5} más")
    
    success = len(all_errors) == 0
    
    if success:
        print("\n✅ Carga completada exitosamente")
    else:
        print("\n⚠️ Carga completada con errores")
    
    return success


# Query functions
@safe_execute(default_value=[])
def get_all_rpc_contracts(
    collection_name: str = "rpc_contratos_emprestito",
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Get all RPC contracts from Firestore.
    
    Args:
        collection_name: Firestore collection name
        limit: Maximum number of documents to retrieve
        
    Returns:
        List of RPC contract dictionaries
    """
    print(f"📥 Obteniendo contratos RPC de colección '{collection_name}'...")
    
    db = get_firestore_client()
    if not db:
        return []
    
    collection_ref = db.collection(collection_name)
    
    # Add limit if specified
    if limit:
        docs = collection_ref.limit(limit).stream()
    else:
        docs = collection_ref.stream()
    
    contracts = []
    for doc in docs:
        contract = doc.to_dict()
        contract['doc_id'] = doc.id
        contracts.append(contract)
    
    print(f"✅ Obtenidos {len(contracts)} contratos")
    
    return contracts


@safe_execute(default_value=None)
def get_rpc_contract_by_id(
    doc_id: str,
    collection_name: str = "rpc_contratos_emprestito"
) -> Optional[Dict[str, Any]]:
    """
    Get a specific RPC contract by document ID.
    
    Args:
        doc_id: Document ID
        collection_name: Firestore collection name
        
    Returns:
        RPC contract dictionary or None
    """
    db = get_firestore_client()
    if not db:
        return None
    
    doc_ref = db.collection(collection_name).document(doc_id)
    doc = doc_ref.get()
    
    if doc.exists:
        contract = doc.to_dict()
        contract['doc_id'] = doc.id
        return contract
    
    return None


@safe_execute(default_value=[])
def query_rpc_by_beneficiary(
    documento_identificacion: str,
    collection_name: str = "rpc_contratos_emprestito"
) -> List[Dict[str, Any]]:
    """
    Query RPC contracts by beneficiary document ID.
    
    Args:
        documento_identificacion: Beneficiary document ID
        collection_name: Firestore collection name
        
    Returns:
        List of matching RPC contracts
    """
    db = get_firestore_client()
    if not db:
        return []
    
    collection_ref = db.collection(collection_name)
    
    query = collection_ref.where('documento_identificacion', '==', documento_identificacion)
    
    docs = query.stream()
    
    contracts = []
    for doc in docs:
        contract = doc.to_dict()
        contract['doc_id'] = doc.id
        contracts.append(contract)
    
    return contracts


# Collection statistics
@safe_execute(default_value={})
def get_collection_stats(
    collection_name: str = "rpc_contratos_emprestito"
) -> Dict[str, Any]:
    """
    Get statistics about the RPC contracts collection.
    
    Args:
        collection_name: Firestore collection name
        
    Returns:
        Dictionary with collection statistics
    """
    print(f"📊 Obteniendo estadísticas de '{collection_name}'...")
    
    db = get_firestore_client()
    if not db:
        return {}
    
    collection_ref = db.collection(collection_name)
    docs = list(collection_ref.stream())
    
    if not docs:
        return {
            'total_documents': 0,
            'message': 'Colección vacía'
        }
    
    # Calculate statistics
    total_valor = 0
    beneficiaries = set()
    centros_gestores = set()
    
    for doc in docs:
        data = doc.to_dict()
        
        # Sum values
        if data.get('valor_rpc'):
            total_valor += float(data['valor_rpc'])
        
        # Collect unique beneficiaries
        if data.get('documento_identificacion'):
            beneficiaries.add(data['documento_identificacion'])
        
        # Collect unique centros gestores
        if data.get('nombre_centro_gestor'):
            centros_gestores.add(data['nombre_centro_gestor'])
    
    stats = {
        'total_documents': len(docs),
        'total_valor_rpc': total_valor,
        'unique_beneficiaries': len(beneficiaries),
        'unique_centros_gestores': len(centros_gestores),
        'average_valor_rpc': total_valor / len(docs) if docs else 0
    }
    
    print(f"✅ Estadísticas calculadas")
    return stats


if __name__ == "__main__":
    """Prueba del módulo de carga."""
    print("🧪 Módulo de Carga RPC Contratos a Firebase")
    print("="*70)
    
    # Test connection
    db = get_firestore_client()
    if db:
        print("✅ Conexión a Firebase exitosa")
        
        # Get stats if collection exists
        stats = get_collection_stats()
        if stats:
            print(f"\n📊 Estadísticas actuales:")
            for key, value in stats.items():
                print(f"   {key}: {value}")
    else:
        print("❌ No se pudo conectar a Firebase")
    
    print("\n✅ Módulo cargado correctamente")
    print("💡 Uso:")
    print("   from load_app.data_loading_rpc_contratos import load_rpc_to_firebase")
    print("   load_rpc_to_firebase(transformed_data_list)")
