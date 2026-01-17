# -*- coding: utf-8 -*-
"""
Script para cargar datos de Cuentas por Pagar del Empréstito a Firebase
"""

import sys
import pandas as pd
from datetime import datetime
sys.path.append('database')
from config import get_firestore_client

print('='*80)
print('CARGA DE CUENTAS POR PAGAR - EMPRESTITO')
print('='*80)

# Ruta al archivo Excel
excel_path = 'app_inputs/emprestito_inputs/cuentas_por_pagar/CXP_emprestito.xlsx'

print(f'\n[INFO] Leyendo archivo Excel: {excel_path}')

try:
    # Leer el archivo Excel
    df = pd.read_excel(excel_path)
    print(f'[OK] Archivo leído exitosamente')
    print(f'[INFO] Total filas originales: {len(df)}')
    print(f'[INFO] Columnas: {list(df.columns)}')
    
    # Mostrar primeras filas
    print(f'\n[INFO] Primeras 3 filas (datos originales):')
    print(df.head(3).to_string())
    
    # Verificar que exista la columna 'valor_por_pagar'
    if 'valor_por_pagar' not in df.columns:
        print(f'[ERROR] No se encontró la columna "valor_por_pagar" en el archivo')
        print(f'[ERROR] Columnas disponibles: {list(df.columns)}')
        sys.exit(1)
    
    # Verificar que exista una columna para BP
    # Buscar columnas que puedan contener BP
    bp_column = None
    possible_bp_columns = ['bp', 'BP', 'banco_proyectos', 'banco_proyecto', 'Banco Proyectos', 'Banco Proyecto']
    
    for col in possible_bp_columns:
        if col in df.columns:
            bp_column = col
            break
    
    if not bp_column:
        print(f'[ERROR] No se encontró una columna de BP (Banco de Proyectos)')
        print(f'[ERROR] Columnas disponibles: {list(df.columns)}')
        sys.exit(1)
    
    print(f'[INFO] Columna BP identificada: {bp_column}')
    
    # Convertir valor_por_pagar a número entero sin decimales
    print(f'\n[INFO] Procesando valores...')
    
    # Reemplazar valores nulos por 0
    df['valor_por_pagar'] = df['valor_por_pagar'].fillna(0)
    
    # Convertir a numérico (por si hay strings) y luego a entero
    df['valor_por_pagar'] = pd.to_numeric(df['valor_por_pagar'], errors='coerce').fillna(0)
    df['valor_por_pagar'] = df['valor_por_pagar'].astype(int)
    
    print(f'[OK] Valores convertidos a enteros')
    print(f'\n[INFO] Estadísticas de valores por pagar:')
    print(f'  - Total suma: ${df["valor_por_pagar"].sum():,}')
    print(f'  - Promedio: ${df["valor_por_pagar"].mean():,.0f}')
    print(f'  - Mínimo: ${df["valor_por_pagar"].min():,}')
    print(f'  - Máximo: ${df["valor_por_pagar"].max():,}')
    
    # Agrupar por BP y sumar los valores
    print(f'\n[INFO] Agrupando por BP y sumando valores...')
    
    # Crear una copia con las columnas necesarias y eliminar filas con BP nulo
    df_grouped = df[[bp_column, 'valor_por_pagar']].copy()
    df_grouped = df_grouped[df_grouped[bp_column].notna()]
    
    # Renombrar columna BP a nombre estándar para simplificar
    df_grouped = df_grouped.rename(columns={bp_column: 'bp'})
    
    # Agrupar por BP y sumar
    df_agrupado = df_grouped.groupby('bp', as_index=False).agg({
        'valor_por_pagar': 'sum'
    })
    
    # Contar registros originales por BP para incluir en el resultado
    df_conteo = df_grouped.groupby('bp', as_index=False).size()
    df_conteo = df_conteo.rename(columns={'size': 'cantidad_registros'})
    
    # Unir conteo con suma
    df_agrupado = df_agrupado.merge(df_conteo, on='bp', how='left')
    
    print(f'[OK] Agrupación completada')
    print(f'[INFO] Total BPs únicos: {len(df_agrupado)}')
    print(f'[INFO] Registros originales: {len(df)}')
    print(f'[INFO] Reducción: {len(df) - len(df_agrupado)} registros')
    
    # Mostrar datos agrupados
    print(f'\n[INFO] Primeros 5 BPs agrupados:')
    print(df_agrupado.head().to_string())
    
    # Verificar que la suma total se mantiene
    print(f'\n[INFO] Verificación de integridad:')
    print(f'  - Suma original: ${df["valor_por_pagar"].sum():,}')
    print(f'  - Suma agrupada: ${df_agrupado["valor_por_pagar"].sum():,}')
    
    if df['valor_por_pagar'].sum() == df_agrupado['valor_por_pagar'].sum():
        print(f'[OK] ✓ Las sumas coinciden - Integridad verificada')
    else:
        print(f'[WARNING] ⚠ Las sumas no coinciden - Revisar agrupación')
    
    # Conectar a Firebase
    print(f'\n[INFO] Conectando a Firebase...')
    db = get_firestore_client()
    
    if not db:
        print('[ERROR] No se pudo conectar a Firebase')
        sys.exit(1)
    
    print('[OK] Conectado a Firebase')
    
    # Nombre de la colección
    collection_name = 'cuentas_por_pagar_emprestito'
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
    
    # Convertir DataFrame agrupado a lista de diccionarios
    print(f'\n[INFO] Preparando datos para carga...')
    records = df_agrupado.to_dict('records')
    
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
                clean_record[key] = int(value.item()) if key == 'valor_por_pagar' or key == 'cantidad_registros' else value.item()
            else:
                clean_record[key] = int(value) if key == 'valor_por_pagar' or key == 'cantidad_registros' else value
        
        # Agregar timestamps y metadata
        clean_record['created_at'] = datetime.now().isoformat()
        clean_record['updated_at'] = datetime.now().isoformat()
        clean_record['fuente'] = 'CXP_emprestito.xlsx'
        
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
            # Usar BP como ID del documento para facilitar búsquedas
            bp_id = str(record['bp']).replace('/', '_').replace(' ', '_')
            doc_ref = collection_ref.document(bp_id)
            firebase_batch.set(doc_ref, record)
            uploaded_count += 1
        
        firebase_batch.commit()
        print(f'  [OK] Lote {batch_num}/{total_batches} cargado ({len(batch)} docs)')
    
    print(f'\n[SUCCESS] ✓ Carga completada exitosamente!')
    print(f'[INFO] Total documentos cargados: {uploaded_count}')
    print(f'[INFO] Colección: {collection_name}')
    
    # Verificar carga
    print(f'\n[INFO] Verificando carga en Firebase...')
    final_docs = list(collection_ref.stream())
    print(f'[OK] Documentos en Firebase: {len(final_docs)}')
    
    # Verificar suma total en Firebase
    total_firebase = 0
    for doc in final_docs:
        doc_data = doc.to_dict()
        total_firebase += doc_data.get('valor_por_pagar', 0)
    
    print(f'[INFO] Total en Firebase: ${total_firebase:,}')
    
    # Mostrar algunos documentos de ejemplo
    if len(final_docs) > 0:
        print(f'\n[INFO] Ejemplos de documentos cargados:')
        for i, doc in enumerate(final_docs[:3]):
            sample_doc = doc.to_dict()
            print(f'\n  Documento {i+1} (ID: {doc.id}):')
            print(f'    - BP: {sample_doc.get("bp")}')
            print(f'    - Valor por pagar: ${sample_doc.get("valor_por_pagar", 0):,}')
            print(f'    - Cantidad registros originales: {sample_doc.get("cantidad_registros", 0)}')
            print(f'    - Fuente: {sample_doc.get("fuente")}')
    
    print(f'\n{"="*80}')
    print(f'RESUMEN FINAL:')
    print(f'  - Archivo procesado: {excel_path}')
    print(f'  - Filas originales en Excel: {len(df)}')
    print(f'  - BPs únicos: {len(df_agrupado)}')
    print(f'  - Documentos cargados a Firebase: {uploaded_count}')
    print(f'  - Colección Firebase: {collection_name}')
    print(f'  - Total por pagar (original): ${df["valor_por_pagar"].sum():,}')
    print(f'  - Total por pagar (agrupado): ${df_agrupado["valor_por_pagar"].sum():,}')
    print(f'  - Total por pagar (Firebase): ${total_firebase:,}')
    print(f'{"="*80}')
    
except FileNotFoundError:
    print(f'[ERROR] Archivo no encontrado: {excel_path}')
    print(f'[ERROR] Verifique que el archivo existe en la ruta especificada')
    sys.exit(1)
except Exception as e:
    print(f'[ERROR] Error durante la carga: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
