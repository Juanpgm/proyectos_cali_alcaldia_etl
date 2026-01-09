# -*- coding: utf-8 -*-
"""
Script para inspeccionar la estructura de datos en Firebase
"""

import sys
import json
sys.path.append('database')
from config import get_firestore_client

print('='*80)
print('INSPECCION DE ESTRUCTURA EN FIREBASE')
print('='*80)

# Conectar a Firebase
db = get_firestore_client()

if db:
    # Obtener un documento de ejemplo
    docs = list(db.collection('unidades_proyecto').limit(1).stream())
    
    if docs:
        doc = docs[0]
        data = doc.to_dict()
        
        print(f'\n[OK] Documento ID: {doc.id}')
        print(f'\n[INFO] Estructura del documento:')
        print(json.dumps(data, indent=2, default=str, ensure_ascii=False)[:3000])  # Primeros 3000 caracteres
        
        # Mostrar claves principales
        print(f'\n[INFO] Claves principales en el documento:')
        for key in data.keys():
            value_type = type(data[key]).__name__
            if isinstance(data[key], list):
                print(f'  - {key}: {value_type} (len={len(data[key])})')
            elif isinstance(data[key], dict):
                print(f'  - {key}: {value_type} (keys={list(data[key].keys())[:5]}...)')
            else:
                print(f'  - {key}: {value_type}')
        
        # Verificar si existe el array de intervenciones
        if 'intervenciones' in data:
            intervenciones = data['intervenciones']
            print(f'\n[OK] Array intervenciones encontrado: {len(intervenciones)} elementos')
            
            if len(intervenciones) > 0:
                print(f'\n[INFO] Estructura de una intervencion:')
                interv_sample = intervenciones[0]
                for key in interv_sample.keys():
                    print(f'  - {key}: {type(interv_sample[key]).__name__}')
        
    else:
        print('[ERROR] No se encontraron documentos en Firebase')
else:
    print('[ERROR] No se pudo conectar a Firebase')
