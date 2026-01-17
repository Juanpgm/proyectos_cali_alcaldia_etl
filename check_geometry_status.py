# -*- coding: utf-8 -*-
"""
Script temporal para verificar el estado de las geometrías en Firebase
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client

def check_geometry_status():
    """Verifica cuántos registros tienen geometry en Firebase"""
    try:
        db = get_firestore_client()
        if not db:
            print("[ERROR] No se pudo conectar a Firebase")
            return
        
        collection_ref = db.collection('unidades_proyecto')
        
        # Contar TODOS los registros
        print("[INFO] Contando todos los registros...")
        total_count = 0
        with_geometry_count = 0
        without_geometry_count = 0
        
        # Procesar en lotes
        last_doc = None
        batch_size = 1000
        
        while True:
            if last_doc:
                query = collection_ref.limit(batch_size).start_after(last_doc)
            else:
                query = collection_ref.limit(batch_size)
            
            docs = list(query.stream())
            if not docs:
                break
            
            for doc in docs:
                data = doc.to_dict()
                has_geom = data.get('has_geometry', False)
                total_count += 1
                
                if has_geom:
                    with_geometry_count += 1
                else:
                    without_geometry_count += 1
            
            last_doc = docs[-1]
            
            if len(docs) < batch_size:
                break
        
        print(f"\n[STATS] Estadísticas completas:")
        print(f"  Total registros: {total_count}")
        print(f"  Con geometry: {with_geometry_count} ({with_geometry_count*100/total_count if total_count > 0 else 0:.1f}%)")
        print(f"  Sin geometry: {without_geometry_count} ({without_geometry_count*100/total_count if total_count > 0 else 0:.1f}%)")
        
        # Mostrar algunos ejemplos sin geometry
        if without_geometry_count > 0:
            print(f"\n[DETAIL] Ejemplos de registros SIN geometry (primeros 5):")
            docs_without_geom = list(collection_ref.where('has_geometry', '==', False).limit(5).stream())
            
            for doc in docs_without_geom:
                data = doc.to_dict()
                geometry = data.get('geometry')
                lat = data.get('lat')
                lon = data.get('lon')
                
                print(f"\n  UPID: {doc.id}")
                print(f"  Nombre: {data.get('nombre_corto', 'N/A')}")
                print(f"  Centro Gestor: {data.get('nombre_centro_gestor', 'N/A')}")
                print(f"  Geometry field: {geometry}")
                print(f"  lat: {lat}, lon: {lon}")
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_geometry_status()
