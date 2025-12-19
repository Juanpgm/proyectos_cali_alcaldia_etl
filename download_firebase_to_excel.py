# -*- coding: utf-8 -*-
"""
Descargar registros de unidades_proyecto de Firebase a Excel
"""

import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client

def download_to_excel():
    print('Conectando a Firebase...')
    db = get_firestore_client()
    
    print('Descargando registros de unidades_proyecto...')
    docs = db.collection('unidades_proyecto').stream()
    
    records = []
    for doc in docs:
        data = doc.to_dict()
        data['doc_id'] = doc.id
        
        # Procesar geometry para que el Excel sea legible
        if 'geometry' in data:
            geom = data.pop('geometry')
            if geom and isinstance(geom, dict):
                data['has_geometry'] = True
                data['geometry_type'] = geom.get('type', 'Unknown')
                # Guardar coordenadas como string
                if 'coordinates' in geom:
                    data['coordinates'] = str(geom.get('coordinates'))
            else:
                data['has_geometry'] = False
                data['geometry_type'] = None
        
        # Convertir listas y diccionarios anidados a strings para Excel
        for key, value in list(data.items()):
            if isinstance(value, (list, dict)):
                data[key] = str(value)
        
        records.append(data)
    
    print(f'Registros descargados: {len(records)}')
    
    # Crear DataFrame
    df = pd.DataFrame(records)
    
    # Ordenar columnas importantes primero
    priority_cols = [
        'doc_id', 'upid', 'nickname', 'nombre_centro_gestor', 
        'estado', 'avance_obra', 'tipo_equipamiento', 'direccion',
        'bpin', 'ano', 'presupuesto_base', 'has_geometry', 'geometry_type'
    ]
    other_cols = [c for c in df.columns if c not in priority_cols]
    ordered_cols = [c for c in priority_cols if c in df.columns] + sorted(other_cols)
    df = df[ordered_cols]
    
    # Guardar en Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'unidades_proyecto_firebase_{timestamp}.xlsx'
    
    df.to_excel(filename, index=False, engine='openpyxl')
    
    print(f'\n✅ Archivo guardado: {filename}')
    print(f'   Total columnas: {len(df.columns)}')
    print(f'   Total registros: {len(df)}')
    
    # Mostrar resumen por estado
    if 'estado' in df.columns:
        print(f'\n📊 Resumen por estado:')
        for estado, count in df['estado'].value_counts().items():
            print(f'   - {estado}: {count}')
    
    return filename

if __name__ == "__main__":
    download_to_excel()
