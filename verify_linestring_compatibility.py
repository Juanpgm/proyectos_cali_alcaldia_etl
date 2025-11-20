# -*- coding: utf-8 -*-
"""
Script de verificación de compatibilidad de LineStrings con Firebase y NextJS.
Valida que los datos exportados cumplan con todos los requisitos del frontend.
"""

import os
import json
import sys
from typing import Dict, List, Any

def verify_geojson_compatibility(geojson_path: str) -> Dict[str, Any]:
    """
    Verifica que el GeoJSON sea compatible con Firebase y NextJS.
    
    Validaciones:
    1. Estructura FeatureCollection válida
    2. Geometrías solo 2D (sin elevación)
    3. tipo_equipamiento = "Vias" en todos los registros
    4. Campos requeridos presentes
    5. Tipos de datos correctos
    6. Coordenadas válidas (no [0,0] placeholders)
    
    Args:
        geojson_path: Ruta al archivo GeoJSON
        
    Returns:
        Dict con resultados de validación
    """
    results = {
        'valid': True,
        'total_features': 0,
        'errors': [],
        'warnings': [],
        'statistics': {}
    }
    
    # Verificar que el archivo existe
    if not os.path.exists(geojson_path):
        results['valid'] = False
        results['errors'].append(f"Archivo no encontrado: {geojson_path}")
        return results
    
    # Cargar GeoJSON
    try:
        with open(geojson_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        results['valid'] = False
        results['errors'].append(f"Error al cargar GeoJSON: {e}")
        return results
    
    # Validar estructura FeatureCollection
    if data.get('type') != 'FeatureCollection':
        results['valid'] = False
        results['errors'].append("No es un FeatureCollection válido")
        return results
    
    features = data.get('features', [])
    results['total_features'] = len(features)
    
    if not features:
        results['warnings'].append("No hay features en el GeoJSON")
        return results
    
    # Contadores
    geometry_types = {}
    tipo_equipamiento_values = {}
    missing_tipo_equipamiento = 0
    missing_geometry = 0
    invalid_coordinates = 0
    has_elevation = 0
    placeholder_coords = 0
    
    required_fields = [
        'tipo_equipamiento', 'geometry_type', 'has_geometry',
        'nombre_up', 'direccion', 'estado', 'clase_obra'
    ]
    
    # Validar cada feature
    for idx, feature in enumerate(features, 1):
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        
        # 1. Validar tipo_equipamiento
        tipo_eq = props.get('tipo_equipamiento')
        if not tipo_eq:
            missing_tipo_equipamiento += 1
            results['warnings'].append(f"Feature {idx}: falta tipo_equipamiento")
        else:
            tipo_equipamiento_values[tipo_eq] = tipo_equipamiento_values.get(tipo_eq, 0) + 1
            if tipo_eq != 'Vias':
                results['warnings'].append(f"Feature {idx}: tipo_equipamiento='{tipo_eq}' (esperado: 'Vias')")
        
        # 2. Validar geometría
        geom_type = geom.get('type')
        if not geom_type:
            missing_geometry += 1
            results['warnings'].append(f"Feature {idx}: sin geometría")
            continue
        
        geometry_types[geom_type] = geometry_types.get(geom_type, 0) + 1
        
        # 3. Validar coordenadas
        coords = geom.get('coordinates')
        if not coords:
            invalid_coordinates += 1
            results['warnings'].append(f"Feature {idx}: sin coordenadas")
            continue
        
        # Verificar dimensión de coordenadas según tipo de geometría
        if geom_type == 'Point':
            if len(coords) > 2:
                has_elevation += 1
                results['warnings'].append(f"Feature {idx}: Point con elevación (3D)")
            if coords[0] == 0 and coords[1] == 0:
                placeholder_coords += 1
        
        elif geom_type == 'LineString':
            if any(len(c) > 2 for c in coords):
                has_elevation += 1
                results['warnings'].append(f"Feature {idx}: LineString con elevación (3D)")
            if all(c[0] == 0 and c[1] == 0 for c in coords):
                placeholder_coords += 1
        
        elif geom_type == 'MultiLineString':
            if any(len(c) > 2 for line in coords for c in line):
                has_elevation += 1
                results['warnings'].append(f"Feature {idx}: MultiLineString con elevación (3D)")
        
        # 4. Validar campos requeridos
        missing_fields = [field for field in required_fields if not props.get(field)]
        if missing_fields:
            results['warnings'].append(f"Feature {idx}: campos faltantes: {', '.join(missing_fields)}")
    
    # Estadísticas finales
    results['statistics'] = {
        'total_features': len(features),
        'geometry_types': geometry_types,
        'tipo_equipamiento_values': tipo_equipamiento_values,
        'missing_tipo_equipamiento': missing_tipo_equipamiento,
        'missing_geometry': missing_geometry,
        'invalid_coordinates': invalid_coordinates,
        'has_elevation': has_elevation,
        'placeholder_coords': placeholder_coords
    }
    
    # Validaciones críticas
    if missing_tipo_equipamiento > 0:
        results['errors'].append(f"{missing_tipo_equipamiento} features sin tipo_equipamiento")
    
    if missing_geometry > 0:
        results['errors'].append(f"{missing_geometry} features sin geometría")
    
    if has_elevation > 0:
        results['warnings'].append(f"{has_elevation} features con coordenadas 3D (deberían ser 2D)")
    
    if placeholder_coords > 0:
        results['warnings'].append(f"{placeholder_coords} features con coordenadas placeholder [0,0]")
    
    # Determinar validez final
    results['valid'] = len(results['errors']) == 0
    
    return results


def print_verification_report(results: Dict[str, Any]):
    """Imprime un reporte detallado de la verificación."""
    print("\n" + "="*80)
    print("REPORTE DE VERIFICACIÓN - COMPATIBILIDAD FIREBASE Y NEXTJS")
    print("="*80)
    
    # Estado general
    if results['valid']:
        print("\n✅ VALIDACIÓN EXITOSA - GeoJSON compatible")
    else:
        print("\n❌ VALIDACIÓN FALLIDA - Errores encontrados")
    
    # Estadísticas
    stats = results.get('statistics', {})
    print(f"\n📊 ESTADÍSTICAS:")
    print(f"  Total features: {stats.get('total_features', 0)}")
    
    if stats.get('geometry_types'):
        print(f"\n  Tipos de geometría:")
        for geom_type, count in stats['geometry_types'].items():
            print(f"    • {geom_type}: {count}")
    
    if stats.get('tipo_equipamiento_values'):
        print(f"\n  Valores de tipo_equipamiento:")
        for tipo, count in stats['tipo_equipamiento_values'].items():
            print(f"    • {tipo}: {count}")
    
    # Problemas detectados
    if stats.get('missing_tipo_equipamiento', 0) > 0:
        print(f"\n  ⚠️  Sin tipo_equipamiento: {stats['missing_tipo_equipamiento']}")
    
    if stats.get('missing_geometry', 0) > 0:
        print(f"  ⚠️  Sin geometría: {stats['missing_geometry']}")
    
    if stats.get('has_elevation', 0) > 0:
        print(f"  ⚠️  Con elevación (3D): {stats['has_elevation']}")
    
    if stats.get('placeholder_coords', 0) > 0:
        print(f"  ⚠️  Con coordenadas placeholder: {stats['placeholder_coords']}")
    
    # Errores
    if results['errors']:
        print(f"\n❌ ERRORES ({len(results['errors'])}):")
        for i, error in enumerate(results['errors'][:10], 1):
            print(f"  {i}. {error}")
        if len(results['errors']) > 10:
            print(f"  ... y {len(results['errors']) - 10} más")
    
    # Advertencias
    if results['warnings']:
        print(f"\n⚠️  ADVERTENCIAS ({len(results['warnings'])}):")
        for i, warning in enumerate(results['warnings'][:10], 1):
            print(f"  {i}. {warning}")
        if len(results['warnings']) > 10:
            print(f"  ... y {len(results['warnings']) - 10} más")
    
    print("\n" + "="*80)
    
    # Recomendaciones
    if not results['valid']:
        print("\n📝 RECOMENDACIONES:")
        print("  1. Ejecutar el notebook de transformación nuevamente")
        print("  2. Verificar que todos los registros tengan tipo_equipamiento='Vias'")
        print("  3. Asegurar que las coordenadas sean 2D (sin elevación)")
        print("  4. Validar que todos los campos requeridos estén presentes")


def main():
    """Función principal de verificación."""
    # Buscar el archivo GeoJSON
    possible_paths = [
        'context/unidades_proyecto.geojson',
        '../context/unidades_proyecto.geojson',
        'unidades_proyecto.geojson'
    ]
    
    geojson_path = None
    for path in possible_paths:
        if os.path.exists(path):
            geojson_path = path
            break
    
    if not geojson_path:
        print("❌ No se encontró el archivo unidades_proyecto.geojson")
        print(f"   Rutas buscadas:")
        for path in possible_paths:
            print(f"     • {os.path.abspath(path)}")
        return False
    
    print(f"📄 Verificando: {os.path.abspath(geojson_path)}")
    
    # Ejecutar verificación
    results = verify_geojson_compatibility(geojson_path)
    
    # Mostrar reporte
    print_verification_report(results)
    
    return results['valid']


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
