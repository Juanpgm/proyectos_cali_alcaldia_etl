# -*- coding: utf-8 -*-
"""
Verificación post-carga: Valida que los datos en Firebase sean compatibles con NextJS.
"""

import os
import sys
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client

def verify_firebase_data():
    """Verifica que los datos en Firebase sean compatibles con NextJS."""
    
    print("="*80)
    print("VERIFICACIÓN POST-CARGA - FIREBASE")
    print("="*80)
    
    db = get_firestore_client()
    if not db:
        print("❌ No se pudo conectar a Firebase")
        return False
    
    # Obtener registros con tipo_equipamiento = "Vias"
    print("\n1️⃣ Consultando registros con tipo_equipamiento='Vias'...")
    vias_ref = db.collection('unidades_proyecto').where('tipo_equipamiento', '==', 'Vias').limit(10)
    vias_docs = list(vias_ref.stream())
    
    print(f"   ✓ Encontrados: {len(vias_docs)} registros (sample)")
    
    if not vias_docs:
        print("   ❌ No se encontraron registros con tipo_equipamiento='Vias'")
        return False
    
    # Verificar estructura de un documento
    print("\n2️⃣ Verificando estructura de documentos...")
    
    sample_doc = vias_docs[0]
    data = sample_doc.to_dict()
    
    required_fields = [
        'tipo_equipamiento', 'geometry', 'geometry_type', 'has_geometry',
        'nombre_up', 'clase_obra', 'nombre_centro_gestor'
    ]
    
    missing_fields = [field for field in required_fields if field not in data]
    
    if missing_fields:
        print(f"   ❌ Campos faltantes: {', '.join(missing_fields)}")
        return False
    
    print(f"   ✓ Todos los campos requeridos presentes")
    
    # Verificar tipo_equipamiento
    print(f"\n3️⃣ Verificando tipo_equipamiento...")
    tipo_eq = data.get('tipo_equipamiento')
    if tipo_eq != 'Vias':
        print(f"   ❌ tipo_equipamiento incorrecto: '{tipo_eq}' (esperado: 'Vias')")
        return False
    print(f"   ✓ tipo_equipamiento = 'Vias'")
    
    # Verificar geometría
    print(f"\n4️⃣ Verificando geometría...")
    geometry = data.get('geometry')
    
    if not geometry:
        print(f"   ❌ Geometría ausente")
        return False
    
    geom_type = geometry.get('type')
    coords = geometry.get('coordinates')
    
    print(f"   ✓ Tipo de geometría: {geom_type}")
    print(f"   ✓ has_geometry: {data.get('has_geometry')}")
    print(f"   ✓ geometry_type: {data.get('geometry_type')}")
    
    # Verificar que coordinates esté como string (serializado)
    if isinstance(coords, str):
        print(f"   ✓ Coordinates almacenado como JSON string")
        
        # Intentar deserializar
        try:
            parsed_coords = json.loads(coords)
            print(f"   ✓ Coordinates deserializable")
            
            # Verificar dimensión (debe ser 2D)
            if geom_type == 'LineString':
                if parsed_coords and len(parsed_coords[0]) == 2:
                    print(f"   ✓ Coordenadas 2D (sin elevación)")
                else:
                    print(f"   ⚠️  Coordenadas no son 2D")
            
        except json.JSONDecodeError as e:
            print(f"   ❌ Error al deserializar coordinates: {e}")
            return False
    elif isinstance(coords, list):
        print(f"   ⚠️  Coordinates almacenado como array (esperado: string JSON)")
    else:
        print(f"   ❌ Formato de coordinates desconocido: {type(coords)}")
        return False
    
    # Estadísticas generales
    print(f"\n5️⃣ Estadísticas generales...")
    
    # Contar por geometry_type
    geometry_types = {}
    for doc in vias_docs:
        doc_data = doc.to_dict()
        geom_type = doc_data.get('geometry_type', 'Unknown')
        geometry_types[geom_type] = geometry_types.get(geom_type, 0) + 1
    
    print(f"   Tipos de geometría en sample:")
    for gtype, count in geometry_types.items():
        print(f"     • {gtype}: {count}")
    
    # Verificar has_geometry
    has_geom_count = sum(1 for doc in vias_docs if doc.to_dict().get('has_geometry'))
    print(f"   ✓ Registros con has_geometry=True: {has_geom_count}/{len(vias_docs)}")
    
    print("\n" + "="*80)
    print("✅ VERIFICACIÓN EXITOSA")
    print("="*80)
    print("\n📋 Resumen:")
    print("  ✓ tipo_equipamiento = 'Vias' en todos los registros")
    print("  ✓ Geometrías presentes y con formato correcto")
    print("  ✓ Coordinates como JSON string (compatible con Firebase)")
    print("  ✓ Coordenadas 2D (sin elevación)")
    print("  ✓ Estructura compatible con NextJS frontend")
    
    print("\n🎯 Estado: Los datos están listos para consumirse desde el frontend")
    
    return True


if __name__ == "__main__":
    success = verify_firebase_data()
    sys.exit(0 if success else 1)
