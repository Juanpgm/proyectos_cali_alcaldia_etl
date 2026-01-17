# -*- coding: utf-8 -*-
"""
Script para generar reporte de geometry en unidades de proyecto de Firebase
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client
import json

def generar_reporte_geometry():
    """Genera reporte detallado de unidades con y sin geometry."""
    
    print("="*80)
    print("REPORTE DE GEOMETRY - UNIDADES DE PROYECTO")
    print("="*80)
    print()
    
    # Conectar a Firebase
    db = get_firestore_client()
    collection_name = 'unidades_proyecto'
    
    print(f"📥 Obteniendo datos de Firebase (colección: {collection_name})...")
    docs = db.collection(collection_name).stream()
    
    # Contadores
    total = 0
    con_geometry = 0
    sin_geometry = 0
    geometry_null = 0
    geometry_invalida = 0
    
    # Listas para análisis
    unidades_sin_geometry = []
    unidades_con_lat_lon_null = []
    
    print("🔍 Analizando documentos...")
    
    for doc in docs:
        total += 1
        data = doc.to_dict()
        upid = data.get('upid', doc.id)
        geometry = data.get('geometry')
        
        # Verificar si tiene geometry válida
        if geometry is None:
            sin_geometry += 1
            geometry_null += 1
            unidades_sin_geometry.append({
                'upid': upid,
                'nombre_centro_gestor': data.get('nombre_centro_gestor', 'N/A'),
                'nombre_up': data.get('nombre_up', 'N/A'),
                'tipo_equipamiento': data.get('tipo_equipamiento', 'N/A'),
                'razon': 'geometry es null'
            })
        elif isinstance(geometry, dict):
            geom_type = geometry.get('type')
            coords = geometry.get('coordinates')
            
            # Verificar si coordinates es válido
            if coords is None:
                sin_geometry += 1
                geometry_invalida += 1
                unidades_sin_geometry.append({
                    'upid': upid,
                    'nombre_centro_gestor': data.get('nombre_centro_gestor', 'N/A'),
                    'nombre_up': data.get('nombre_up', 'N/A'),
                    'tipo_equipamiento': data.get('tipo_equipamiento', 'N/A'),
                    'razon': 'coordinates es null'
                })
            elif geom_type == 'Point':
                # Verificar si es [null, null] o similar
                if not isinstance(coords, list) or len(coords) != 2:
                    sin_geometry += 1
                    geometry_invalida += 1
                    unidades_sin_geometry.append({
                        'upid': upid,
                        'nombre_centro_gestor': data.get('nombre_centro_gestor', 'N/A'),
                        'nombre_up': data.get('nombre_up', 'N/A'),
                        'tipo_equipamiento': data.get('tipo_equipamiento', 'N/A'),
                        'razon': f'coordinates inválidas: {coords}'
                    })
                elif coords[0] is None or coords[1] is None:
                    sin_geometry += 1
                    unidades_con_lat_lon_null.append({
                        'upid': upid,
                        'nombre_centro_gestor': data.get('nombre_centro_gestor', 'N/A'),
                        'nombre_up': data.get('nombre_up', 'N/A'),
                        'tipo_equipamiento': data.get('tipo_equipamiento', 'N/A'),
                        'coordinates': coords
                    })
                else:
                    con_geometry += 1
            elif geom_type == 'LineString':
                # Para LineString verificar que tenga al menos 2 puntos
                if not isinstance(coords, list) or len(coords) < 2:
                    sin_geometry += 1
                    geometry_invalida += 1
                    unidades_sin_geometry.append({
                        'upid': upid,
                        'nombre_centro_gestor': data.get('nombre_centro_gestor', 'N/A'),
                        'nombre_up': data.get('nombre_up', 'N/A'),
                        'tipo_equipamiento': data.get('tipo_equipamiento', 'N/A'),
                        'razon': f'LineString con menos de 2 puntos'
                    })
                else:
                    con_geometry += 1
            else:
                con_geometry += 1
        else:
            sin_geometry += 1
            geometry_invalida += 1
            unidades_sin_geometry.append({
                'upid': upid,
                'nombre_centro_gestor': data.get('nombre_centro_gestor', 'N/A'),
                'nombre_up': data.get('nombre_up', 'N/A'),
                'tipo_equipamiento': data.get('tipo_equipamiento', 'N/A'),
                'razon': f'geometry no es dict: {type(geometry)}'
            })
        
        # Mostrar progreso cada 100 docs
        if total % 100 == 0:
            print(f"   Procesados: {total}...", end='\r')
    
    print()
    print()
    print("="*80)
    print("📊 RESUMEN DE RESULTADOS")
    print("="*80)
    print()
    print(f"📌 Total de unidades de proyecto en Firebase: {total:,}")
    print()
    print(f"✅ Unidades CON geometry válida: {con_geometry:,} ({con_geometry/total*100:.1f}%)")
    print(f"❌ Unidades SIN geometry válida: {sin_geometry:,} ({sin_geometry/total*100:.1f}%)")
    print()
    print("📋 Desglose de unidades sin geometry:")
    print(f"   • geometry es null: {geometry_null:,}")
    print(f"   • coordinates es null o inválido: {geometry_invalida:,}")
    print(f"   • coordinates con valores null: {len(unidades_con_lat_lon_null):,}")
    print()
    
    # Análisis por centro gestor
    print("="*80)
    print("📊 ANÁLISIS POR CENTRO GESTOR")
    print("="*80)
    print()
    
    centros_sin_geometry = {}
    for unidad in unidades_sin_geometry:
        centro = unidad['nombre_centro_gestor']
        if centro not in centros_sin_geometry:
            centros_sin_geometry[centro] = 0
        centros_sin_geometry[centro] += 1
    
    # Ordenar por cantidad
    centros_ordenados = sorted(centros_sin_geometry.items(), key=lambda x: x[1], reverse=True)
    
    for centro, cantidad in centros_ordenados:
        porcentaje = (cantidad / sin_geometry * 100) if sin_geometry > 0 else 0
        print(f"   • {centro}: {cantidad:,} unidades sin geometry ({porcentaje:.1f}%)")
    
    print()
    
    # Guardar reporte detallado
    output_file = 'app_outputs/logs/reporte_geometry_firebase.json'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    reporte = {
        'fecha_reporte': __import__('datetime').datetime.now().isoformat(),
        'resumen': {
            'total_unidades': total,
            'con_geometry': con_geometry,
            'sin_geometry': sin_geometry,
            'porcentaje_con_geometry': round(con_geometry/total*100, 2) if total > 0 else 0,
            'porcentaje_sin_geometry': round(sin_geometry/total*100, 2) if total > 0 else 0
        },
        'desglose_sin_geometry': {
            'geometry_null': geometry_null,
            'coordinates_invalidas': geometry_invalida,
            'coordinates_con_null': len(unidades_con_lat_lon_null)
        },
        'por_centro_gestor': centros_sin_geometry,
        'unidades_sin_geometry': unidades_sin_geometry[:50],  # Primeras 50 para no hacer el archivo muy grande
        'unidades_con_lat_lon_null': unidades_con_lat_lon_null[:20]  # Primeras 20
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2)
    
    print("="*80)
    print("📁 ARCHIVOS GENERADOS")
    print("="*80)
    print()
    print(f"✅ Reporte JSON guardado: {output_file}")
    print()
    
    # Mostrar algunas unidades sin geometry como ejemplo
    if unidades_sin_geometry:
        print("="*80)
        print("🔍 EJEMPLOS DE UNIDADES SIN GEOMETRY (primeras 10)")
        print("="*80)
        print()
        for i, unidad in enumerate(unidades_sin_geometry[:10], 1):
            print(f"{i}. UPID: {unidad['upid']}")
            print(f"   Centro Gestor: {unidad['nombre_centro_gestor']}")
            print(f"   Nombre UP: {unidad['nombre_up'][:80]}...")
            print(f"   Tipo Equipamiento: {unidad['tipo_equipamiento']}")
            print(f"   Razón: {unidad['razon']}")
            print()
    
    print("="*80)
    print("✅ REPORTE COMPLETADO")
    print("="*80)


if __name__ == '__main__':
    generar_reporte_geometry()
