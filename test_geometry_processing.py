#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test de geometrías - Verificar que todas las geometrías se procesen correctamente
"""

import json
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from load_app.data_loading_unidades_proyecto_infraestructura import (
    serialize_geometry_coordinates,
    prepare_document_data
)


def test_geometry_serialization():
    """Test de serialización de todos los tipos de geometría"""
    
    print("="*80)
    print("TEST DE SERIALIZACIÓN DE GEOMETRÍAS")
    print("="*80)
    
    test_cases = [
        {
            "name": "Point",
            "geometry": {
                "type": "Point",
                "coordinates": [-76.520562, 3.4418833]
            },
            "expected_format": "array [lon, lat]"
        },
        {
            "name": "LineString",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [-76.520562, 3.4418833],
                    [-76.521234, 3.4425678],
                    [-76.522456, 3.4432890]
                ]
            },
            "expected_format": "array de [lon, lat]"
        },
        {
            "name": "LineString con elevación",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [-76.520562, 3.4418833, 1500],
                    [-76.521234, 3.4425678, 1550],
                    [-76.522456, 3.4432890, 1600]
                ]
            },
            "expected_format": "array de [lon, lat] (sin z)"
        },
        {
            "name": "Polygon",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-76.52, 3.44],
                        [-76.51, 3.44],
                        [-76.51, 3.45],
                        [-76.52, 3.45],
                        [-76.52, 3.44]
                    ]
                ]
            },
            "expected_format": "array de rings"
        },
        {
            "name": "MultiPoint",
            "geometry": {
                "type": "MultiPoint",
                "coordinates": [
                    [-76.520562, 3.4418833],
                    [-76.521234, 3.4425678]
                ]
            },
            "expected_format": "array de [lon, lat]"
        },
        {
            "name": "MultiLineString",
            "geometry": {
                "type": "MultiLineString",
                "coordinates": [
                    [
                        [-76.520562, 3.4418833],
                        [-76.521234, 3.4425678]
                    ],
                    [
                        [-76.522456, 3.4432890],
                        [-76.523678, 3.4440123]
                    ]
                ]
            },
            "expected_format": "array de LineStrings"
        },
        {
            "name": "MultiPolygon",
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [
                    [
                        [
                            [-76.52, 3.44],
                            [-76.51, 3.44],
                            [-76.51, 3.45],
                            [-76.52, 3.44]
                        ]
                    ],
                    [
                        [
                            [-76.53, 3.45],
                            [-76.52, 3.45],
                            [-76.52, 3.46],
                            [-76.53, 3.45]
                        ]
                    ]
                ]
            },
            "expected_format": "array de Polygons"
        }
    ]
    
    passed = 0
    failed = 0
    
    for test in test_cases:
        print(f"\n{'─'*80}")
        print(f"Test: {test['name']}")
        print(f"Formato esperado: {test['expected_format']}")
        
        geom_type = test['geometry']['type']
        coords = test['geometry']['coordinates']
        
        # Serializar
        result = serialize_geometry_coordinates(geom_type, coords)
        
        # Verificar resultado
        if result is None:
            print("❌ FALLO: Resultado es None")
            failed += 1
            continue
        
        print(f"✓ Tipo: {geom_type}")
        print(f"✓ Coordenadas serializadas: {json.dumps(result, indent=2)}")
        
        # Verificaciones específicas (formato interno, antes de serializar para Firebase)
        try:
            if geom_type == 'Point':
                assert isinstance(result, list), "Point debe ser array"
                assert len(result) == 2, "Point debe tener 2 coordenadas"
                assert all(isinstance(x, float) for x in result), "Coordenadas deben ser float"
                
            elif geom_type == 'LineString':
                # En formato interno (antes de Firebase), debe ser array de arrays
                assert isinstance(result, list), "LineString debe ser array"
                assert all(isinstance(p, list) and len(p) == 2 for p in result), "Cada punto debe ser [lon, lat]"
                
            elif geom_type == 'Polygon':
                assert isinstance(result, list), "Polygon debe ser array"
                assert all(isinstance(ring, list) for ring in result), "Cada ring debe ser array"
                assert all(isinstance(p, list) and len(p) == 2 for ring in result for p in ring), "Cada punto debe ser [lon, lat]"
                
            elif geom_type == 'MultiPoint':
                assert isinstance(result, list), "MultiPoint debe ser array"
                assert all(isinstance(p, list) and len(p) == 2 for p in result), "Cada punto debe ser [lon, lat]"
                
            elif geom_type == 'MultiLineString':
                assert isinstance(result, list), "MultiLineString debe ser array"
                assert all(isinstance(line, list) for line in result), "Cada línea debe ser array"
                assert all(isinstance(p, list) and len(p) == 2 for line in result for p in line), "Cada punto debe ser [lon, lat]"
                
            elif geom_type == 'MultiPolygon':
                assert isinstance(result, list), "MultiPolygon debe ser array"
                assert all(isinstance(poly, list) for poly in result), "Cada polígono debe ser array"
                assert all(isinstance(ring, list) for poly in result for ring in poly), "Cada ring debe ser array"
            
            print("✅ PASÓ: Formato correcto")
            passed += 1
            
        except AssertionError as e:
            print(f"❌ FALLO: {str(e)}")
            failed += 1
    
    # Resumen
    print(f"\n{'='*80}")
    print("RESUMEN")
    print(f"{'='*80}")
    print(f"✅ Pasaron: {passed}/{len(test_cases)}")
    print(f"❌ Fallaron: {failed}/{len(test_cases)}")
    
    return failed == 0


def test_full_document_preparation():
    """Test de preparación completa de documentos"""
    
    print(f"\n{'='*80}")
    print("TEST DE PREPARACIÓN DE DOCUMENTOS")
    print(f"{'='*80}")
    
    # Feature de ejemplo con LineString
    feature = {
        "type": "Feature",
        "properties": {
            "upid": "TEST-001",
            "nombre_up": "Vía de prueba",
            "direccion": "Calle 5 con Carrera 10",
            "bpin": "12345",
            "estado": "En ejecución",
            "presupuesto_base": 1000000000,
            "avance_obra": 45.5
        },
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [-76.520562, 3.4418833, 1500],  # Con elevación
                [-76.521234, 3.4425678, 1550],
                [-76.522456, 3.4432890, 1600]
            ]
        }
    }
    
    print("\n📝 Feature de entrada:")
    print(json.dumps(feature, indent=2, ensure_ascii=False))
    
    # Preparar documento
    document = prepare_document_data(feature)
    
    if not document:
        print("\n❌ FALLO: No se pudo preparar el documento")
        return False
    
    print("\n📄 Documento preparado:")
    print(json.dumps(document, indent=2, ensure_ascii=False, default=str))
    
    # Verificaciones
    geom_coords = document.get('geometry', {}).get('coordinates')
    geom_type = document.get('geometry', {}).get('type')
    
    # Para LineString, coordinates debe ser string JSON (Firebase)
    # Para Point, coordinates debe ser array
    is_coords_valid = (
        (geom_type == 'Point' and isinstance(geom_coords, list)) or
        (geom_type != 'Point' and isinstance(geom_coords, str))
    )
    
    checks = [
        ("Tiene upid", document.get('upid') == "TEST-001"),
        ("Tiene tipo_equipamiento", document.get('tipo_equipamiento') == "Vias"),
        ("Tiene geometry", document.get('geometry') is not None),
        ("Geometry tiene type", geom_type == "LineString"),
        ("Geometry tiene coordinates", geom_coords is not None),
        ("Coordinates en formato correcto (string para LineString)", is_coords_valid),
        ("Tiene has_geometry", document.get('has_geometry') == True),
        ("Tiene geometry_type", document.get('geometry_type') == "LineString"),
        ("Tiene created_at", 'created_at' in document),
        ("Tiene updated_at", 'updated_at' in document)
    ]
    
    print("\n🔍 Verificaciones:")
    all_passed = True
    for check_name, result in checks:
        status = "✅" if result else "❌"
        print(f"  {status} {check_name}")
        if not result:
            all_passed = False
    
    # Verificar coordenadas serializadas correctamente
    coords = document.get('geometry', {}).get('coordinates')
    geom_type = document.get('geometry', {}).get('type')
    
    if coords and geom_type:
        # LineString debe estar como JSON string para Firebase
        if geom_type == 'LineString':
            is_string = isinstance(coords, str)
            print(f"  {'✅' if is_string else '❌'} LineString coordinates serializado como JSON string")
            if not is_string:
                all_passed = False
            else:
                # Verificar que se puede parsear
                try:
                    parsed = json.loads(coords)
                    first_coord = parsed[0]
                    has_only_2d = len(first_coord) == 2
                    print(f"  {'✅' if has_only_2d else '❌'} Coordenadas parseadas son 2D (sin elevación)")
                    if not has_only_2d:
                        all_passed = False
                except:
                    print(f"  ❌ No se puede parsear coordinates JSON")
                    all_passed = False
    
    return all_passed


if __name__ == "__main__":
    print("\n" + "="*80)
    print("SUITE DE TESTS DE GEOMETRÍAS")
    print("="*80)
    
    test1 = test_geometry_serialization()
    test2 = test_full_document_preparation()
    
    print("\n" + "="*80)
    print("RESULTADO FINAL")
    print("="*80)
    
    if test1 and test2:
        print("✅ TODOS LOS TESTS PASARON")
        sys.exit(0)
    else:
        print("❌ ALGUNOS TESTS FALLARON")
        sys.exit(1)
