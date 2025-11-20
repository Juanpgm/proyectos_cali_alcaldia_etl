#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Migración: Agregar geometry_type a documentos existentes
"""

import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client
from tqdm import tqdm


def migrate_geometry_type():
    """Agregar geometry_type a todos los documentos que tengan geometry"""
    
    print("="*80)
    print("MIGRACIÓN: Agregar geometry_type a documentos")
    print("="*80)
    
    db = get_firestore_client()
    if not db:
        print("❌ No se pudo conectar a Firebase")
        return False
    
    collection_ref = db.collection('unidades_proyecto')
    
    # Obtener todos los documentos
    print("\n📥 Obteniendo documentos...")
    docs = collection_ref.stream()
    
    updates_needed = []
    already_ok = 0
    
    for doc in docs:
        data = doc.to_dict()
        geometry = data.get('geometry')
        geometry_type = data.get('geometry_type')
        
        # Si tiene geometry pero no tiene geometry_type o está incorrecto
        if geometry and isinstance(geometry, dict):
            geom_type = geometry.get('type')
            
            if not geometry_type or geometry_type != geom_type:
                updates_needed.append({
                    'id': doc.id,
                    'geometry_type': geom_type,
                    'has_geometry': True
                })
            else:
                already_ok += 1
        elif not geometry:
            # No tiene geometry
            if data.get('has_geometry') != False:
                updates_needed.append({
                    'id': doc.id,
                    'geometry_type': None,
                    'has_geometry': False
                })
            else:
                already_ok += 1
    
    print(f"\n📊 Análisis:")
    print(f"   • Documentos correctos: {already_ok}")
    print(f"   • Documentos a actualizar: {len(updates_needed)}")
    
    if len(updates_needed) == 0:
        print("\n✅ Todos los documentos ya tienen geometry_type correcto")
        return True
    
    # Actualizar documentos
    print(f"\n🔄 Actualizando {len(updates_needed)} documentos...")
    
    batch = db.batch()
    batch_count = 0
    updated_count = 0
    
    with tqdm(total=len(updates_needed), desc="Actualizando") as pbar:
        for update in updates_needed:
            doc_ref = collection_ref.document(update['id'])
            
            update_data = {
                'geometry_type': update['geometry_type'],
                'has_geometry': update['has_geometry'],
                'updated_at': datetime.now().isoformat()
            }
            
            batch.update(doc_ref, update_data)
            batch_count += 1
            
            # Commit cada 500 documentos
            if batch_count >= 500:
                batch.commit()
                updated_count += batch_count
                batch = db.batch()
                batch_count = 0
            
            pbar.update(1)
        
        # Commit final
        if batch_count > 0:
            batch.commit()
            updated_count += batch_count
    
    print(f"\n✅ Actualizados {updated_count} documentos")
    
    # Verificar resultados
    print("\n🔍 Verificando resultados...")
    
    point_count = collection_ref.where('geometry_type', '==', 'Point').count().get()[0][0].value
    line_count = collection_ref.where('geometry_type', '==', 'LineString').count().get()[0][0].value
    multiline_count = collection_ref.where('geometry_type', '==', 'MultiLineString').count().get()[0][0].value
    
    print(f"\n📊 Estadísticas finales:")
    print(f"   • Point: {point_count}")
    print(f"   • LineString: {line_count}")
    print(f"   • MultiLineString: {multiline_count}")
    
    return True


if __name__ == "__main__":
    success = migrate_geometry_type()
    sys.exit(0 if success else 1)
