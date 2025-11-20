#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Revisar estructura de geometrías en Firebase - diagnóstico completo
"""

import sys
import os
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client


def check_geometry_structure():
    """Revisar estructura de geometrías en Firebase"""
    
    print("="*80)
    print("DIAGNÓSTICO DE ESTRUCTURA DE GEOMETRÍAS EN FIREBASE")
    print("="*80)
    
    db = get_firestore_client()
    if not db:
        print("❌ No se pudo conectar a Firebase")
        return
    
    # Obtener ejemplos de cada tipo
    print("\n🔍 Obteniendo ejemplos...\n")
    
    # Ejemplo Point
    point_docs = db.collection('unidades_proyecto').where('geometry_type', '==', 'Point').limit(2).stream()
    
    print("="*80)
    print("EJEMPLO 1: Point (tipo original)")
    print("="*80)
    
    for doc in point_docs:
        data = doc.to_dict()
        print(f"\n📍 Documento: {doc.id}")
        print(f"   Tipo: {data.get('geometry_type')}")
        print(f"   Tiene geometry: {data.get('has_geometry')}")
        print(f"   Tipo equipamiento: {data.get('tipo_equipamiento')}")
        
        geometry = data.get('geometry')
        if geometry:
            print(f"\n   Geometry completo:")
            print(f"   {json.dumps(geometry, indent=6, ensure_ascii=False)}")
            
            coords = geometry.get('coordinates')
            print(f"\n   Tipo de coordinates: {type(coords)}")
            print(f"   Valor coordinates: {coords}")
            
            if isinstance(coords, list):
                print(f"   ✅ Es array (correcto para Point)")
            else:
                print(f"   ⚠️  NO es array")
        break
    
    # Ejemplo LineString
    line_docs = db.collection('unidades_proyecto').where('geometry_type', '==', 'LineString').limit(2).stream()
    
    print("\n" + "="*80)
    print("EJEMPLO 2: LineString (infraestructura vial)")
    print("="*80)
    
    for doc in line_docs:
        data = doc.to_dict()
        print(f"\n📍 Documento: {doc.id}")
        print(f"   Tipo: {data.get('geometry_type')}")
        print(f"   Tiene geometry: {data.get('has_geometry')}")
        print(f"   Tipo equipamiento: {data.get('tipo_equipamiento')}")
        
        geometry = data.get('geometry')
        if geometry:
            print(f"\n   Geometry completo:")
            print(f"   {json.dumps(geometry, indent=6, ensure_ascii=False)}")
            
            coords = geometry.get('coordinates')
            print(f"\n   Tipo de coordinates: {type(coords)}")
            
            if isinstance(coords, str):
                print(f"   ✅ Es string JSON (esperado para LineString)")
                print(f"   Longitud del string: {len(coords)} caracteres")
                print(f"   Primeros 200 caracteres: {coords[:200]}...")
                
                # Intentar parsear
                try:
                    parsed = json.loads(coords)
                    print(f"   ✅ Se puede parsear correctamente")
                    print(f"   Número de puntos: {len(parsed)}")
                    if len(parsed) > 0:
                        print(f"   Primer punto: {parsed[0]}")
                        if len(parsed) > 1:
                            print(f"   Último punto: {parsed[-1]}")
                except Exception as e:
                    print(f"   ❌ Error al parsear: {e}")
            else:
                print(f"   ⚠️  NO es string (tipo: {type(coords)})")
                print(f"   Valor: {coords}")
        break
    
    # Contar totales
    print("\n" + "="*80)
    print("ESTADÍSTICAS GENERALES")
    print("="*80)
    
    total_docs = db.collection('unidades_proyecto').count().get()[0][0].value
    
    # Contar por tipo
    point_count = db.collection('unidades_proyecto').where('geometry_type', '==', 'Point').count().get()[0][0].value
    line_count = db.collection('unidades_proyecto').where('geometry_type', '==', 'LineString').count().get()[0][0].value
    multiline_count = db.collection('unidades_proyecto').where('geometry_type', '==', 'MultiLineString').count().get()[0][0].value
    no_geom_count = db.collection('unidades_proyecto').where('has_geometry', '==', False).count().get()[0][0].value
    
    print(f"\n📊 Total documentos: {total_docs}")
    print(f"   • Point: {point_count}")
    print(f"   • LineString: {line_count}")
    print(f"   • MultiLineString: {multiline_count}")
    print(f"   • Sin geometría: {no_geom_count}")
    
    print("\n" + "="*80)
    print("RECOMENDACIONES PARA NEXT.JS")
    print("="*80)
    print("""
    El frontend debe manejar ambos formatos:
    
    ```typescript
    function parseGeometry(geometry: any) {
      if (!geometry || !geometry.type) return null;
      
      let coordinates = geometry.coordinates;
      
      // Si coordinates es string, parsear
      if (typeof coordinates === 'string') {
        try {
          coordinates = JSON.parse(coordinates);
        } catch (e) {
          console.error('Error parsing coordinates:', e);
          return null;
        }
      }
      
      // Ahora coordinates es array para todos los tipos
      return {
        type: geometry.type,
        coordinates: coordinates
      };
    }
    ```
    """)


if __name__ == "__main__":
    check_geometry_structure()
