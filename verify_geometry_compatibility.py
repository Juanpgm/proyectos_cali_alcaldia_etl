#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Verificar compatibilidad de geometrías con Next.js
Revisa que todas las geometrías en Firebase estén en formato GeoJSON estándar
"""

import sys
import os
import json

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client


def verify_geometry_type(geometry: dict, doc_id: str) -> dict:
    """
    Verificar que una geometría sea compatible con GeoJSON estándar.
    
    Returns:
        Dict con resultado de validación
    """
    result = {
        'valid': False,
        'geom_type': None,
        'coord_format': None,
        'issues': []
    }
    
    if not geometry:
        result['issues'].append("Geometría ausente")
        return result
    
    if not isinstance(geometry, dict):
        result['issues'].append(f"Geometría no es dict: {type(geometry)}")
        return result
    
    geom_type = geometry.get('type')
    coordinates = geometry.get('coordinates')
    
    result['geom_type'] = geom_type
    
    if not geom_type:
        result['issues'].append("Falta campo 'type'")
        return result
    
    if not coordinates:
        result['issues'].append("Falta campo 'coordinates'")
        return result
    
    # Verificar formato de coordenadas según tipo de geometría
    try:
        if geom_type == 'Point':
            # Point debe ser [lon, lat]
            if not isinstance(coordinates, list) or len(coordinates) != 2:
                result['issues'].append(f"Point debe tener 2 coordenadas, tiene {len(coordinates) if isinstance(coordinates, list) else 'N/A'}")
            elif not all(isinstance(c, (int, float)) for c in coordinates):
                result['issues'].append("Point coordenadas deben ser números")
            else:
                lon, lat = coordinates
                # Validar rango de Cali
                if -78 < lon < -75 and 2 < lat < 5:
                    result['valid'] = True
                    result['coord_format'] = '[lon, lat] ✓'
                else:
                    result['issues'].append(f"Coordenadas fuera de rango de Cali: [{lon}, {lat}]")
        
        elif geom_type == 'LineString':
            # LineString puede ser array o JSON string (Firebase)
            coords_data = coordinates
            if isinstance(coordinates, str):
                try:
                    coords_data = json.loads(coordinates)
                except:
                    result['issues'].append("LineString coordenadas string no es JSON válido")
                    return result
            
            if not isinstance(coords_data, list):
                result['issues'].append(f"LineString coordenadas debe ser array o JSON string, es: {type(coords_data)}")
            elif len(coords_data) < 2:
                result['issues'].append(f"LineString debe tener al menos 2 puntos, tiene {len(coords_data)}")
            else:
                # Verificar primer punto
                first_point = coords_data[0]
                if not isinstance(first_point, list) or len(first_point) != 2:
                    result['issues'].append(f"LineString punto debe ser [lon, lat], es: {first_point}")
                elif not all(isinstance(c, (int, float)) for c in first_point):
                    result['issues'].append("LineString coordenadas deben ser números")
                else:
                    lon, lat = first_point
                    if -78 < lon < -75 and 2 < lat < 5:
                        result['valid'] = True
                        format_type = "JSON string" if isinstance(coordinates, str) else "array"
                        result['coord_format'] = f'[[lon, lat], ...] ✓ ({len(coords_data)} puntos, {format_type})'
                    else:
                        result['issues'].append(f"Coordenadas fuera de rango: [{lon}, {lat}]")
        
        elif geom_type == 'Polygon':
            # Polygon puede ser array o JSON string (Firebase)
            coords_data = coordinates
            if isinstance(coordinates, str):
                try:
                    coords_data = json.loads(coordinates)
                except:
                    result['issues'].append("Polygon coordenadas string no es JSON válido")
                    return result
            
            if not isinstance(coords_data, list):
                result['issues'].append(f"Polygon coordenadas debe ser array o JSON string, es: {type(coords_data)}")
            elif len(coords_data) == 0:
                result['issues'].append("Polygon debe tener al menos 1 ring")
            else:
                # Verificar primer ring
                first_ring = coords_data[0]
                if not isinstance(first_ring, list):
                    result['issues'].append(f"Polygon ring debe ser array, es: {type(first_ring)}")
                elif len(first_ring) < 3:
                    result['issues'].append(f"Polygon ring debe tener al menos 3 puntos, tiene {len(first_ring)}")
                else:
                    # Verificar primer punto del primer ring
                    first_point = first_ring[0]
                    if not isinstance(first_point, list) or len(first_point) != 2:
                        result['issues'].append(f"Polygon punto debe ser [lon, lat], es: {first_point}")
                    elif not all(isinstance(c, (int, float)) for c in first_point):
                        result['issues'].append("Polygon coordenadas deben ser números")
                    else:
                        lon, lat = first_point
                        if -78 < lon < -75 and 2 < lat < 5:
                            result['valid'] = True
                            format_type = "JSON string" if isinstance(coordinates, str) else "array"
                            result['coord_format'] = f'[[[lon, lat], ...], ...] ✓ ({len(coords_data)} rings, {format_type})'
                        else:
                            result['issues'].append(f"Coordenadas fuera de rango: [{lon}, {lat}]")
        
        elif geom_type in ['MultiPoint', 'MultiLineString', 'MultiPolygon']:
            # Tipos complejos - pueden ser array o JSON string (Firebase)
            coords_data = coordinates
            if isinstance(coordinates, str):
                try:
                    coords_data = json.loads(coordinates)
                except:
                    result['issues'].append(f"{geom_type} coordenadas string no es JSON válido")
                    return result
            
            if not isinstance(coords_data, list):
                result['issues'].append(f"{geom_type} coordenadas debe ser array o JSON string, es: {type(coords_data)}")
            elif len(coords_data) == 0:
                result['issues'].append(f"{geom_type} debe tener al menos 1 elemento")
            else:
                result['valid'] = True
                format_type = "JSON string" if isinstance(coordinates, str) else "array"
                result['coord_format'] = f'{geom_type} con {len(coords_data)} elementos ✓ ({format_type})'
        
        else:
            result['issues'].append(f"Tipo de geometría desconocido: {geom_type}")
    
    except Exception as e:
        result['issues'].append(f"Error verificando geometría: {str(e)}")
    
    return result


def verify_firebase_geometries(collection_name: str = "unidades_proyecto", limit: int = 100):
    """
    Verificar todas las geometrías en Firebase para compatibilidad con Next.js
    """
    print("="*80)
    print("VERIFICACIÓN DE GEOMETRÍAS PARA COMPATIBILIDAD NEXT.JS")
    print("="*80)
    print(f"Colección: {collection_name}")
    print(f"Límite: {limit} documentos\n")
    
    db = get_firestore_client()
    if not db:
        print("❌ No se pudo conectar a Firebase")
        return
    
    # Obtener documentos
    docs_ref = db.collection(collection_name).limit(limit)
    docs = docs_ref.stream()
    
    # Estadísticas
    stats = {
        'total': 0,
        'with_geometry': 0,
        'without_geometry': 0,
        'valid_geometries': 0,
        'invalid_geometries': 0,
        'by_type': {},
        'issues_count': {}
    }
    
    invalid_examples = []
    
    print("🔍 Analizando geometrías...\n")
    
    for doc in docs:
        stats['total'] += 1
        data = doc.to_dict()
        geometry = data.get('geometry')
        
        if geometry:
            stats['with_geometry'] += 1
            
            # Verificar geometría
            validation = verify_geometry_type(geometry, doc.id)
            
            # Actualizar estadísticas por tipo
            if validation['geom_type']:
                stats['by_type'][validation['geom_type']] = stats['by_type'].get(validation['geom_type'], 0) + 1
            
            if validation['valid']:
                stats['valid_geometries'] += 1
            else:
                stats['invalid_geometries'] += 1
                
                # Registrar problemas
                for issue in validation['issues']:
                    stats['issues_count'][issue] = stats['issues_count'].get(issue, 0) + 1
                
                # Guardar ejemplo
                if len(invalid_examples) < 5:
                    invalid_examples.append({
                        'doc_id': doc.id,
                        'geom_type': validation['geom_type'],
                        'issues': validation['issues'],
                        'geometry': geometry
                    })
        else:
            stats['without_geometry'] += 1
    
    # Imprimir resultados
    print("="*80)
    print("RESULTADOS")
    print("="*80)
    
    print(f"\n📊 Resumen General:")
    print(f"  • Total documentos analizados: {stats['total']}")
    print(f"  • Con geometría: {stats['with_geometry']}")
    print(f"  • Sin geometría: {stats['without_geometry']}")
    print(f"  • Geometrías válidas: {stats['valid_geometries']} ({stats['valid_geometries']/max(stats['with_geometry'], 1)*100:.1f}%)")
    print(f"  • Geometrías inválidas: {stats['invalid_geometries']} ({stats['invalid_geometries']/max(stats['with_geometry'], 1)*100:.1f}%)")
    
    if stats['by_type']:
        print(f"\n📍 Tipos de Geometría:")
        for geom_type, count in sorted(stats['by_type'].items()):
            print(f"  • {geom_type}: {count}")
    
    if stats['issues_count']:
        print(f"\n⚠️ Problemas Encontrados:")
        for issue, count in sorted(stats['issues_count'].items(), key=lambda x: x[1], reverse=True):
            print(f"  • {issue}: {count} casos")
    
    if invalid_examples:
        print(f"\n❌ Ejemplos de Geometrías Inválidas (primeros 5):")
        for i, example in enumerate(invalid_examples, 1):
            print(f"\n  {i}. Documento: {example['doc_id']}")
            print(f"     Tipo: {example['geom_type']}")
            print(f"     Problemas:")
            for issue in example['issues']:
                print(f"       - {issue}")
            print(f"     Geometría:")
            print(f"       {json.dumps(example['geometry'], indent=2, ensure_ascii=False)}")
    
    # Determinar si hay problemas críticos
    if stats['invalid_geometries'] > 0:
        print(f"\n❌ ACCIÓN REQUERIDA: {stats['invalid_geometries']} geometrías necesitan corrección")
        return False
    else:
        print(f"\n✅ TODAS LAS GEOMETRÍAS SON COMPATIBLES CON NEXT.JS")
        return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Verificar compatibilidad de geometrías con Next.js')
    parser.add_argument('--collection', default='unidades_proyecto', help='Nombre de la colección de Firebase')
    parser.add_argument('--limit', type=int, default=100, help='Número máximo de documentos a verificar')
    
    args = parser.parse_args()
    
    verify_firebase_geometries(args.collection, args.limit)
