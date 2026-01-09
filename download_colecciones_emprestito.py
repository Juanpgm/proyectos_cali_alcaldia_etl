# -*- coding: utf-8 -*-
"""
Descargar colecciones de Firebase relacionadas con empréstito a Excel
"""

import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client

def download_collection_to_df(db, collection_name):
    """
    Descargar una colección completa de Firebase a DataFrame
    
    Args:
        db: Cliente de Firestore
        collection_name: Nombre de la colección
    
    Returns:
        DataFrame con los datos de la colección
    """
    print(f'\n📦 Descargando colección: {collection_name}...')
    
    try:
        docs = db.collection(collection_name).stream()
        
        records = []
        for doc in docs:
            data = doc.to_dict()
            data['doc_id'] = doc.id  # Agregar el ID del documento
            
            # Convertir listas y diccionarios anidados a strings para Excel
            # También manejar timestamps con timezone
            for key, value in list(data.items()):
                if isinstance(value, (list, dict)):
                    data[key] = str(value)
                # Remover timezone de datetime si tiene
                elif hasattr(value, 'tzinfo') and value.tzinfo is not None:
                    data[key] = value.replace(tzinfo=None)
            
            records.append(data)
        
        print(f'   ✅ Registros descargados: {len(records)}')
        
        if records:
            df = pd.DataFrame(records)
            # Mover doc_id a la primera columna
            if 'doc_id' in df.columns:
                cols = ['doc_id'] + [c for c in df.columns if c != 'doc_id']
                df = df[cols]
            return df
        else:
            print(f'   ⚠️  No se encontraron registros en {collection_name}')
            return pd.DataFrame()
            
    except Exception as e:
        print(f'   ❌ Error al descargar {collection_name}: {str(e)}')
        return pd.DataFrame()

def download_emprestito_collections():
    """
    Descargar todas las colecciones relacionadas con empréstito
    """
    print('🔄 Conectando a Firebase...')
    db = get_firestore_client()
    
    # Definir las colecciones a descargar
    collections = [
        'ordenes_compra',
        'contratos_emprestito',
        'procesos_emprestito',
        'proyecciones_emprestito',
        'convenios_transferencias_emprestito',
        'reportes_contratos'
    ]
    
    # Descargar cada colección
    dataframes = {}
    for collection_name in collections:
        df = download_collection_to_df(db, collection_name)
        if not df.empty:
            dataframes[collection_name] = df
    
    # Guardar en Excel con múltiples hojas
    if dataframes:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'colecciones_emprestito_{timestamp}.xlsx'
        
        print(f'\n💾 Guardando archivo Excel: {filename}')
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for collection_name, df in dataframes.items():
                # Usar el nombre de la colección como nombre de la hoja
                # Excel limita los nombres de hoja a 31 caracteres
                sheet_name = collection_name[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f'   ✅ Hoja "{sheet_name}": {len(df)} registros, {len(df.columns)} columnas')
        
        print(f'\n✅ Archivo guardado exitosamente: {filename}')
        
        # Mostrar resumen detallado
        print(f'\n📊 Resumen general:')
        print(f'   📄 Hojas creadas: {len(dataframes)}')
        total_records = sum(len(df) for df in dataframes.values())
        print(f'   📝 Total de registros: {total_records}')
        
        return filename
    else:
        print('\n⚠️  No se descargaron datos de ninguna colección')
        return None

if __name__ == "__main__":
    try:
        filename = download_emprestito_collections()
        if filename:
            print(f'\n🎉 Descarga completada exitosamente')
            print(f'📁 Archivo: {filename}')
    except Exception as e:
        print(f'\n❌ Error durante la ejecución: {str(e)}')
        import traceback
        traceback.print_exc()
