# -*- coding: utf-8 -*-
"""
Script para verificar geometría de UPIDs específicos en Firebase.
"""

import os
import sys
from database.config import get_firestore_client

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_all_upids_in_firebase():
    """Verifica TODOS los UPIDs en Firebase buscando geometrías inválidas."""
    
    print("🔍 Verificando TODOS los UPIDs en Firebase...\n")
    
    db = get_firestore_client()
    if not db:
        print("❌ No se pudo conectar a Firebase")
        return []
    
    collection_ref = db.collection('unidades_proyecto')
    
    invalid_geometries = []
    valid_geometries = []
    total = 0
    
    # Obtener todos los documentos
    docs = collection_ref.stream()
    
    for doc in docs:
        total += 1
        data = doc.to_dict()
        upid = doc.id
        
        has_geometry = data.get('has_geometry', False)
        geometry = data.get('geometry')
        geometry_type = data.get('geometry_type', 'None')
        nombre = data.get('properties', {}).get('nombre_up', 'N/A')
        
        if not has_geometry or not geometry or geometry_type == 'None':
            invalid_geometries.append({
                'upid': upid,
                'nombre': nombre,
                'has_geometry': has_geometry,
                'geometry_type': geometry_type,
                'properties': data.get('properties', {})
            })
        else:
            valid_geometries.append(upid)
    
    print(f"\n📊 Resumen:")
    print(f"   Total UPIDs en Firebase: {total}")
    print(f"   UPIDs con geometría válida: {len(valid_geometries)}")
    print(f"   UPIDs con geometría inválida: {len(invalid_geometries)}")
    
    if invalid_geometries:
        print(f"\n⚠️  Lista completa de UPIDs con geometría inválida:")
        for item in invalid_geometries:
            print(f"   - {item['upid']}: {item['nombre']}")
    
    return invalid_geometries

if __name__ == "__main__":
    invalid_upids = check_all_upids_in_firebase()
    
    # Guardar lista de UPIDs inválidos para corrección
    if invalid_upids:
        output_file = "invalid_geometry_upids.json"
        import json
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(invalid_upids, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Lista guardada en: {output_file}")
