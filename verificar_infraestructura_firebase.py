# -*- coding: utf-8 -*-
"""
Script para verificar los registros de infraestructura (LineString) en Firebase
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client
import json

def verificar_infraestructura():
    """Verifica registros de infraestructura con geometría LineString."""
    
    print("="*80)
    print("VERIFICACIÓN DE REGISTROS DE INFRAESTRUCTURA")
    print("="*80)
    print()
    
    # Conectar a Firebase
    db = get_firestore_client()
    collection_name = 'unidades_proyecto'
    
    print(f"📥 Obteniendo datos de Firebase (colección: {collection_name})...")
    docs = db.collection(collection_name).stream()
    
    # Contadores
    total = 0
    tipo_vias = 0
    linestring = 0
    linestring_valido = 0
    linestring_invalido = 0
    
    # Listas para análisis
    registros_linestring = []
    registros_linestring_invalidos = []
    
    print("🔍 Analizando documentos...")
    
    for doc in docs:
        total += 1
        data = doc.to_dict()
        upid = data.get('upid', doc.id)
        tipo_equipamiento = data.get('tipo_equipamiento')
        geometry = data.get('geometry')
        
        # Contar tipo_equipamiento = 'Vias'
        if tipo_equipamiento == 'Vias':
            tipo_vias += 1
        
        # Verificar geometrías LineString
        if geometry and isinstance(geometry, dict):
            geom_type = geometry.get('type')
            
            if geom_type == 'LineString':
                linestring += 1
                coords = geometry.get('coordinates')
                
                # Verificar si coordinates es válido
                coords_valido = False
                if coords:
                    # Puede ser un string JSON o un array
                    if isinstance(coords, str):
                        try:
                            parsed_coords = json.loads(coords)
                            if isinstance(parsed_coords, list) and len(parsed_coords) >= 2:
                                coords_valido = True
                        except:
                            pass
                    elif isinstance(coords, list) and len(coords) >= 2:
                        coords_valido = True
                
                if coords_valido:
                    linestring_valido += 1
                    registros_linestring.append({
                        'upid': upid,
                        'nombre_centro_gestor': data.get('nombre_centro_gestor', 'N/A'),
                        'nombre_up': data.get('nombre_up', 'N/A')[:80],
                        'tipo_equipamiento': tipo_equipamiento,
                        'coords_type': 'string' if isinstance(coords, str) else 'array',
                        'num_points': len(json.loads(coords)) if isinstance(coords, str) else len(coords)
                    })
                else:
                    linestring_invalido += 1
                    registros_linestring_invalidos.append({
                        'upid': upid,
                        'nombre_centro_gestor': data.get('nombre_centro_gestor', 'N/A'),
                        'nombre_up': data.get('nombre_up', 'N/A')[:80],
                        'tipo_equipamiento': tipo_equipamiento,
                        'coords': str(coords)[:200]
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
    print(f"🛣️  Unidades con tipo_equipamiento='Vias': {tipo_vias:,}")
    print()
    print(f"📐 Geometrías LineString:")
    print(f"   • Total: {linestring:,}")
    print(f"   • Válidas: {linestring_valido:,}")
    print(f"   • Inválidas: {linestring_invalido:,}")
    print()
    
    if linestring > 0:
        porcentaje = (linestring_valido / linestring * 100)
        print(f"✅ Cobertura de LineString válidas: {porcentaje:.1f}%")
    print()
    
    # Mostrar ejemplos
    if registros_linestring:
        print("="*80)
        print("🔍 EJEMPLOS DE LINESTRING VÁLIDAS (primeras 5)")
        print("="*80)
        print()
        for i, reg in enumerate(registros_linestring[:5], 1):
            print(f"{i}. UPID: {reg['upid']}")
            print(f"   Centro Gestor: {reg['nombre_centro_gestor']}")
            print(f"   Nombre UP: {reg['nombre_up']}")
            print(f"   Tipo Equipamiento: {reg['tipo_equipamiento']}")
            print(f"   Tipo Coords: {reg['coords_type']}")
            print(f"   Número de puntos: {reg['num_points']}")
            print()
    
    if registros_linestring_invalidos:
        print("="*80)
        print("❌ EJEMPLOS DE LINESTRING INVÁLIDAS")
        print("="*80)
        print()
        for i, reg in enumerate(registros_linestring_invalidos[:5], 1):
            print(f"{i}. UPID: {reg['upid']}")
            print(f"   Centro Gestor: {reg['nombre_centro_gestor']}")
            print(f"   Nombre UP: {reg['nombre_up']}")
            print(f"   Coords: {reg['coords']}")
            print()
    
    # Guardar reporte
    output_file = 'app_outputs/logs/reporte_infraestructura_firebase.json'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    reporte = {
        'fecha_reporte': __import__('datetime').datetime.now().isoformat(),
        'resumen': {
            'total_unidades': total,
            'tipo_vias': tipo_vias,
            'linestring_total': linestring,
            'linestring_valido': linestring_valido,
            'linestring_invalido': linestring_invalido,
            'porcentaje_valido': round(linestring_valido/linestring*100, 2) if linestring > 0 else 0
        },
        'registros_linestring_validos': registros_linestring[:100],  # Primeros 100
        'registros_linestring_invalidos': registros_linestring_invalidos
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2)
    
    print("="*80)
    print("📁 ARCHIVOS GENERADOS")
    print("="*80)
    print()
    print(f"✅ Reporte JSON guardado: {output_file}")
    print()
    print("="*80)
    print("✅ VERIFICACIÓN COMPLETADA")
    print("="*80)


if __name__ == '__main__':
    verificar_infraestructura()
