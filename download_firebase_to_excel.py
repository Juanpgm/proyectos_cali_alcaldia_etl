# -*- coding: utf-8 -*-
"""
Descargar registros de unidades_proyecto e intervenciones de Firebase a Excel
"""

import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client

def download_unidades_proyecto(db):
    """Descargar registros de unidades_proyecto"""
    print('\n📦 Descargando registros de unidades_proyecto...')
    docs = db.collection('unidades_proyecto').stream()
    
    records = []
    intervenciones_all = []
    
    for doc in docs:
        data = doc.to_dict()
        data['doc_id'] = doc.id
        
        # Extraer intervenciones antes de procesarlas
        intervenciones = data.get('intervenciones', [])
        if isinstance(intervenciones, list) and intervenciones:
            for idx, interv in enumerate(intervenciones):
                if isinstance(interv, dict):
                    interv_copy = interv.copy()
                    interv_copy['upid'] = data.get('upid', '')
                    interv_copy['unidad_doc_id'] = doc.id
                    interv_copy['unidad_nickname'] = data.get('nickname', '')
                    interv_copy['unidad_nombre_centro_gestor'] = data.get('nombre_centro_gestor', '')
                    interv_copy['intervencion_index'] = idx + 1
                    intervenciones_all.append(interv_copy)
        
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
        
        # Contar intervenciones
        data['n_intervenciones'] = len(intervenciones) if isinstance(intervenciones, list) else 0
        
        # Convertir listas y diccionarios anidados a strings para Excel
        for key, value in list(data.items()):
            if isinstance(value, (list, dict)):
                data[key] = str(value)
        
        records.append(data)
    
    print(f'   ✅ Registros descargados: {len(records)}')
    print(f'   ✅ Intervenciones extraídas: {len(intervenciones_all)}')
    
    # Crear DataFrame de unidades
    df_unidades = pd.DataFrame(records)
    
    # Ordenar columnas importantes primero
    priority_cols = [
        'doc_id', 'upid', 'nickname', 'nombre_centro_gestor', 
        'estado', 'avance_obra', 'tipo_equipamiento', 'direccion',
        'bpin', 'ano', 'presupuesto_base', 'n_intervenciones',
        'has_geometry', 'geometry_type'
    ]
    other_cols = [c for c in df_unidades.columns if c not in priority_cols]
    ordered_cols = [c for c in priority_cols if c in df_unidades.columns] + sorted(other_cols)
    df_unidades = df_unidades[ordered_cols]
    
    # Crear DataFrame de intervenciones
    df_intervenciones = pd.DataFrame(intervenciones_all)
    if not df_intervenciones.empty:
        # Ordenar columnas de intervenciones
        priority_interv_cols = [
            'upid', 'unidad_doc_id', 'unidad_nickname', 'unidad_nombre_centro_gestor',
            'intervencion_index', 'ano', 'bpin', 'centro_gestor', 'proyecto',
            'valor_presupuesto_inicial', 'valor_adiciones', 'valor_contratoactual'
        ]
        other_interv_cols = [c for c in df_intervenciones.columns if c not in priority_interv_cols]
        ordered_interv_cols = [c for c in priority_interv_cols if c in df_intervenciones.columns] + sorted(other_interv_cols)
        df_intervenciones = df_intervenciones[ordered_interv_cols]
    
    return df_unidades, df_intervenciones

def download_to_excel():
    print('🔄 Conectando a Firebase...')
    db = get_firestore_client()
    
    # Descargar unidades_proyecto con sus intervenciones
    df_unidades, df_intervenciones = download_unidades_proyecto(db)
    
    # Guardar en Excel con múltiples hojas
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'firebase_completo_{timestamp}.xlsx'
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # Hoja 1: Unidades de Proyecto
        df_unidades.to_excel(writer, sheet_name='Unidades_Proyecto', index=False)
        
        # Hoja 2: Intervenciones (expandidas)
        if not df_intervenciones.empty:
            df_intervenciones.to_excel(writer, sheet_name='Intervenciones', index=False)
    
    print(f'\n✅ Archivo guardado: {filename}')
    print(f'\n📊 Resumen:')
    print(f'   📄 Hoja "Unidades_Proyecto":')
    print(f'      • Columnas: {len(df_unidades.columns)}')
    print(f'      • Registros: {len(df_unidades)}')
    
    if not df_intervenciones.empty:
        print(f'   📄 Hoja "Intervenciones":')
        print(f'      • Columnas: {len(df_intervenciones.columns)}')
        print(f'      • Registros: {len(df_intervenciones)}')
    
    # Mostrar resumen por estado
    if 'estado' in df_unidades.columns:
        print(f'\n📈 Resumen por estado (Unidades):')
        for estado, count in df_unidades['estado'].value_counts().items():
            print(f'   - {estado}: {count}')
    
    # Mostrar resumen por año
    if 'ano' in df_intervenciones.columns and not df_intervenciones.empty:
        print(f'\n📅 Resumen por año (Intervenciones):')
        for ano, count in df_intervenciones['ano'].value_counts().sort_index().items():
            print(f'   - {ano}: {count}')
    
    return filename

if __name__ == "__main__":
    download_to_excel()
