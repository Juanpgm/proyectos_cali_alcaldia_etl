# -*- coding: utf-8 -*-
"""
Script para cargar datos de montos empréstito asignados por centro gestor a Firebase
"""

import sys
import pandas as pd
from datetime import datetime
sys.path.append('database')
from config import get_firestore_client

print('='*80)
print('CARGA DE MONTOS EMPRESTITO ASIGNADOS POR CENTRO GESTOR')
print('='*80)

# Ruta al archivo Excel
excel_path = 'app_inputs/emprestito_inputs/asignado_banco_centro_gestor/asignado_banco_centro_gestor.xlsx'

print(f'\n[INFO] Leyendo archivo Excel: {excel_path}')

try:
    # Leer el archivo Excel
    df = pd.read_excel(excel_path)
    print(f'[OK] Archivo leído exitosamente')
    print(f'[INFO] Total filas: {len(df)}')
    print(f'[INFO] Columnas: {list(df.columns)}')
    
    # Mostrar primeras filas
    print(f'\n[INFO] Primeras 3 filas:')
    print(df.head(3).to_string())
    
    # Conectar a Firebase
    print(f'\n[INFO] Conectando a Firebase...')
    db = get_firestore_client()
    
    if not db:
        print('[ERROR] No se pudo conectar a Firebase')
        sys.exit(1)
    
    print('[OK] Conectado a Firebase')
    
    # Nombre de la colección
    collection_name = 'montos_emprestito_asignados_centro_gestor'
    collection_ref = db.collection(collection_name)
    
    # Limpiar colección existente usando batch delete para eficiencia
    print(f'\n[INFO] Limpiando colección existente...')
    docs = list(collection_ref.stream())
    deleted_count = len(docs)
    
    if deleted_count > 0:
        # Eliminar en lotes de 500 (límite de Firestore)
        batch_delete_size = 500
        for i in range(0, deleted_count, batch_delete_size):
            batch_docs = docs[i:i+batch_delete_size]
            delete_batch = db.batch()
            for doc in batch_docs:
                delete_batch.delete(doc.reference)
            delete_batch.commit()
            print(f'  [OK] Eliminados {len(batch_docs)} documentos (lote {i//batch_delete_size + 1})')
    
    print(f'[OK] Total eliminados: {deleted_count} documentos antiguos')
    
    # Convertir DataFrame a lista de diccionarios
    print(f'\n[INFO] Procesando datos...')
    records = df.to_dict('records')
    
    # Limpiar valores NaN y convertir a tipos JSON serializables
    clean_records = []
    for record in records:
        clean_record = {}
        for key, value in record.items():
            # Convertir NaN a None
            if pd.isna(value):
                clean_record[key] = None
            # Convertir tipos numéricos numpy a Python nativos
            elif hasattr(value, 'item'):
                clean_record[key] = value.item()
            else:
                clean_record[key] = value
        
        # Agregar timestamps
        clean_record['created_at'] = datetime.now().isoformat()
        clean_record['updated_at'] = datetime.now().isoformat()
        
        clean_records.append(clean_record)
    
    # Cargar a Firebase en lotes
    print(f'\n[INFO] Cargando {len(clean_records)} registros a Firebase...')
    batch_size = 100
    total_batches = (len(clean_records) + batch_size - 1) // batch_size
    
    uploaded_count = 0
    for i in range(0, len(clean_records), batch_size):
        batch = clean_records[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        
        # Usar batch write para eficiencia
        firebase_batch = db.batch()
        
        for record in batch:
            # Usar un ID basado en el índice o crear uno automático
            doc_ref = collection_ref.document()
            firebase_batch.set(doc_ref, record)
            uploaded_count += 1
        
        firebase_batch.commit()
        print(f'  [OK] Lote {batch_num}/{total_batches} cargado ({len(batch)} docs)')
    
    print(f'\n[SUCCESS] Carga completada exitosamente!')
    print(f'[INFO] Total documentos cargados: {uploaded_count}')
    print(f'[INFO] Colección: {collection_name}')
    
    # Verificar carga
    print(f'\n[INFO] Verificando carga...')
    final_docs = list(collection_ref.stream())
    print(f'[OK] Documentos en Firebase: {len(final_docs)}')
    
    # Mostrar un documento de ejemplo
    if len(final_docs) > 0:
        print(f'\n[INFO] Ejemplo de documento cargado:')
        sample_doc = final_docs[0].to_dict()
        for key, value in list(sample_doc.items())[:10]:
            print(f'  - {key}: {value}')
    
    print(f'\n{"="*80}')
    print(f'RESUMEN:')
    print(f'  - Archivo procesado: {excel_path}')
    print(f'  - Filas en Excel: {len(df)}')
    print(f'  - Documentos cargados: {uploaded_count}')
    print(f'  - Colección Firebase: {collection_name}')
    print(f'{"="*80}')
    
except FileNotFoundError:
    print(f'[ERROR] Archivo no encontrado: {excel_path}')
    sys.exit(1)
except Exception as e:
    print(f'[ERROR] Error durante la carga: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
