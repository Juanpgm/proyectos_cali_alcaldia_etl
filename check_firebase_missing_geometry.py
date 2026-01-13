# -*- coding: utf-8 -*-
"""
Script para verificar registros sin geometry en Firebase y repararlos.

Este script:
1. Identifica todos los registros sin geometry en la colección unidades_proyecto
2. Intenta reconstruir geometry desde lat/lon
3. Actualiza los registros en Firebase con geometry reconstruida
4. Genera reporte de registros que no pudieron ser reparados
"""

import sys
import os
from typing import Dict, List, Any

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client


def check_and_fix_missing_geometry(collection_name: str = "unidades_proyecto", fix: bool = False):
    """
    Verifica y opcionalmente repara registros sin geometry en Firebase.
    
    Args:
        collection_name: Nombre de la colección en Firebase
        fix: Si True, intenta reparar los registros; si False, solo reporta
    """
    print(f"\n{'='*80}")
    print(f"VERIFICACIÓN DE REGISTROS SIN GEOMETRY EN FIREBASE")
    print(f"Colección: {collection_name}")
    print(f"Modo: {'REPARACIÓN' if fix else 'SOLO LECTURA'}")
    print(f"{'='*80}\n")
    
    try:
        db = get_firestore_client()
        if not db:
            print("[ERROR] No se pudo conectar a Firebase")
            return
        
        collection_ref = db.collection(collection_name)
        
        # Obtener todos los documentos
        print("📥 Obteniendo documentos de Firebase...")
        docs = list(collection_ref.stream())
        total_docs = len(docs)
        print(f"   Total documentos: {total_docs}\n")
        
        # Clasificar documentos
        sin_geometry = []
        con_geometry_valida = []
        con_lat_lon_sin_geometry = []
        sin_coordenadas = []
        
        for doc in docs:
            doc_id = doc.id
            data = doc.to_dict()
            
            geometry = data.get('geometry')
            lat = data.get('lat')
            lon = data.get('lon')
            
            # Verificar si tiene geometry válida
            has_valid_geometry = (
                geometry is not None and 
                isinstance(geometry, dict) and
                geometry.get('type') == 'Point' and
                geometry.get('coordinates') is not None and
                len(geometry.get('coordinates', [])) == 2
            )
            
            # Verificar si tiene lat/lon válidas
            has_valid_coords = (
                lat is not None and lon is not None and
                isinstance(lat, (int, float)) and isinstance(lon, (int, float)) and
                not (lat == 0 and lon == 0)  # Excluir coordenadas en (0,0)
            )
            
            if has_valid_geometry:
                con_geometry_valida.append(doc_id)
            elif has_valid_coords:
                # Tiene coordenadas pero no geometry
                con_lat_lon_sin_geometry.append({
                    'id': doc_id,
                    'lat': lat,
                    'lon': lon,
                    'upid': data.get('upid'),
                    'nombre_up': data.get('nombre_up')
                })
                sin_geometry.append(doc_id)
            else:
                # No tiene ni geometry ni coordenadas válidas
                sin_coordenadas.append({
                    'id': doc_id,
                    'upid': data.get('upid'),
                    'nombre_up': data.get('nombre_up')
                })
                sin_geometry.append(doc_id)
        
        # Mostrar estadísticas
        print(f"📊 ESTADÍSTICAS:")
        print(f"   ✅ Con geometry válida: {len(con_geometry_valida)} ({len(con_geometry_valida)/total_docs*100:.1f}%)")
        print(f"   ⚠️  Sin geometry: {len(sin_geometry)} ({len(sin_geometry)/total_docs*100:.1f}%)")
        print(f"      - Con lat/lon disponibles: {len(con_lat_lon_sin_geometry)}")
        print(f"      - Sin coordenadas: {len(sin_coordenadas)}\n")
        
        # Mostrar registros problemáticos
        if con_lat_lon_sin_geometry:
            print(f"🔧 REGISTROS REPARABLES (tienen lat/lon pero no geometry):")
            for i, item in enumerate(con_lat_lon_sin_geometry[:10], 1):
                print(f"   {i}. {item['upid']} - {item['nombre_up'][:50]}")
                print(f"      Coordenadas: lat={item['lat']}, lon={item['lon']}")
            if len(con_lat_lon_sin_geometry) > 10:
                print(f"   ... y {len(con_lat_lon_sin_geometry) - 10} más\n")
            else:
                print()
        
        if sin_coordenadas:
            print(f"❌ REGISTROS NO REPARABLES (no tienen ni geometry ni coordenadas):")
            for i, item in enumerate(sin_coordenadas[:10], 1):
                print(f"   {i}. {item['upid']} - {item['nombre_up'][:50]}")
            if len(sin_coordenadas) > 10:
                print(f"   ... y {len(sin_coordenadas) - 10} más\n")
            else:
                print()
        
        # Reparar si está habilitado
        if fix and con_lat_lon_sin_geometry:
            print(f"\n🔨 INICIANDO REPARACIÓN...")
            print(f"   Reparando {len(con_lat_lon_sin_geometry)} registros...\n")
            
            reparados = 0
            fallidos = 0
            
            for item in con_lat_lon_sin_geometry:
                try:
                    doc_ref = collection_ref.document(item['id'])
                    
                    # Crear geometry desde lat/lon
                    # GeoJSON estándar: [lon, lat]
                    geometry_field = {
                        'type': 'Point',
                        'coordinates': [float(item['lon']), float(item['lat'])]
                    }
                    
                    # Actualizar documento
                    doc_ref.update({
                        'geometry': geometry_field,
                        'has_geometry': True
                    })
                    
                    reparados += 1
                    if reparados % 10 == 0:
                        print(f"   Reparados: {reparados}/{len(con_lat_lon_sin_geometry)}")
                
                except Exception as e:
                    print(f"   ❌ Error reparando {item['id']}: {e}")
                    fallidos += 1
            
            print(f"\n✅ REPARACIÓN COMPLETADA:")
            print(f"   Reparados exitosamente: {reparados}")
            print(f"   Fallidos: {fallidos}")
        
        elif fix and not con_lat_lon_sin_geometry:
            print("\n✅ No hay registros reparables. Todos los registros están correctos.")
        
        print(f"\n{'='*80}")
        
        return {
            'total': total_docs,
            'con_geometry': len(con_geometry_valida),
            'sin_geometry': len(sin_geometry),
            'reparables': len(con_lat_lon_sin_geometry),
            'no_reparables': len(sin_coordenadas)
        }
        
    except Exception as e:
        print(f"[ERROR] Error verificando Firebase: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Verificar y reparar registros sin geometry en Firebase')
    parser.add_argument('--fix', action='store_true', help='Reparar registros (por defecto solo reporta)')
    parser.add_argument('--collection', default='unidades_proyecto', help='Nombre de la colección')
    
    args = parser.parse_args()
    
    result = check_and_fix_missing_geometry(
        collection_name=args.collection,
        fix=args.fix
    )
    
    if result:
        print(f"\n📈 RESUMEN FINAL:")
        print(f"   Total: {result['total']}")
        print(f"   Con geometry: {result['con_geometry']} ({result['con_geometry']/result['total']*100:.1f}%)")
        print(f"   Sin geometry: {result['sin_geometry']} ({result['sin_geometry']/result['total']*100:.1f}%)")
