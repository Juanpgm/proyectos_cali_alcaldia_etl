# -*- coding: utf-8 -*-
"""
Test script to verify that the loading process preserves data quality.
Tests serialize_for_firebase function with various data types.
"""

import pandas as pd
import numpy as np
import sys
import os

# Add load_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'load_app'))

from data_loading_unidades_proyecto import serialize_for_firebase, prepare_document_data, normalize_estado_value

def test_serialize_for_firebase():
    """Test that serialize_for_firebase preserves data quality."""
    
    print("="*80)
    print("TEST: Data Quality Preservation in serialize_for_firebase")
    print("="*80)
    print()
    
    test_cases = [
        # Test estados normalizados (CRITICAL - must preserve exact capitalization)
        {
            'value': 'En Alistamiento',
            'field_name': 'estado',
            'expected': 'En Alistamiento',
            'description': 'Estado normalizado: En Alistamiento'
        },
        {
            'value': 'En Ejecución',
            'field_name': 'estado',
            'expected': 'En Ejecución',
            'description': 'Estado normalizado: En Ejecución'
        },
        {
            'value': 'Terminado',
            'field_name': 'estado',
            'expected': 'Terminado',
            'description': 'Estado normalizado: Terminado'
        },
        
        # Test tipo_intervencion (should also preserve)
        {
            'value': 'Obra nueva',
            'field_name': 'tipo_intervencion',
            'expected': 'Obra nueva',
            'description': 'Tipo intervención normalizado'
        },
        
        # Test strings with "T" but not date fields (should NOT convert)
        {
            'value': 'BARRIO TEJADA',
            'field_name': 'barrio_vereda',
            'expected': 'BARRIO TEJADA',
            'description': 'String con T pero no es fecha'
        },
        
        # Test date fields (should convert)
        {
            'value': '2024-01-15T00:00:00',
            'field_name': 'fecha_inicio_std',
            'expected': '2024-01-15',
            'description': 'Fecha ISO con T'
        },
        {
            'value': '2024-01-15 00:00:00',
            'field_name': 'fecha_fin_std',
            'expected': '2024-01-15',
            'description': 'Fecha con espacio'
        },
        
        # Test numeric values
        {
            'value': np.int64(12345),
            'field_name': 'bpin',
            'expected': 12345,
            'description': 'Numpy int64'
        },
        {
            'value': np.float64(50.5),
            'field_name': 'avance_obra',
            'expected': 50.5,
            'description': 'Numpy float64'
        },
        
        # Test lists (reference fields)
        {
            'value': ['REF-001', 'REF-002'],
            'field_name': 'referencia_proceso',
            'expected': ['REF-001', 'REF-002'],
            'description': 'Lista de referencias'
        },
        
        # Test None values
        {
            'value': None,
            'field_name': 'any_field',
            'expected': None,
            'description': 'Valor None'
        },
        
        # Test boolean
        {
            'value': True,
            'field_name': 'has_geometry',
            'expected': True,
            'description': 'Valor booleano'
        }
    ]
    
    passed = 0
    failed = 0
    
    print("RUNNING TESTS:")
    print("-" * 80)
    
    for i, test in enumerate(test_cases, 1):
        value = test['value']
        field_name = test['field_name']
        expected = test['expected']
        description = test['description']
        
        # Serialize value
        result = serialize_for_firebase(value, field_name=field_name)
        
        # Compare result
        if result == expected:
            print(f"✅ Test {i:2d}: {description}")
            print(f"           Input: {repr(value)} → Output: {repr(result)}")
            passed += 1
        else:
            print(f"❌ Test {i:2d}: {description}")
            print(f"           Input: {repr(value)}")
            print(f"           Expected: {repr(expected)}")
            print(f"           Got: {repr(result)}")
            failed += 1
        print()
    
    print("="*80)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    print("="*80)
    
    return failed == 0


def test_normalize_estado_in_load():
    """Test that estado normalization works in the load module."""
    
    print()
    print("="*80)
    print("TEST: Estado Normalization in Load Module")
    print("="*80)
    print()
    
    test_cases = [
        # (input_estado, avance_obra, expected_output)
        ('Finalizado', 100, 'Terminado'),
        ('En liquidación', 100, 'Terminado'),
        ('En Alistamiento', 0, 'En Alistamiento'),
        ('En Ejecución', 50, 'En Ejecución'),
        ('Socialización', 0, 'En Alistamiento'),
        ('Por iniciar', None, 'En Alistamiento'),
        ('Suspendido', 25, 'En Ejecución'),  # Unknown defaults to En Ejecución
    ]
    
    print("TESTING NORMALIZATION:")
    print("-" * 80)
    
    all_passed = True
    for input_estado, avance, expected in test_cases:
        result = normalize_estado_value(input_estado, avance)
        passed = result == expected
        all_passed = all_passed and passed
        
        symbol = '✅' if passed else '❌'
        print(f"{symbol} '{input_estado}' (avance={avance}) → '{result}' (expected: '{expected}')")
    
    print()
    return all_passed


def test_prepare_document_data():
    """Test that prepare_document_data normalizes estados automatically."""
    
    print()
    print("="*80)
    print("TEST: Estado Auto-Normalization in prepare_document_data")
    print("="*80)
    print()
    
    # Test cases with estados that need normalization
    test_cases = [
        {
            'name': 'Estado ya normalizado',
            'estado': 'En Ejecución',
            'avance_obra': 50.5,
            'expected': 'En Ejecución'
        },
        {
            'name': 'Finalizado debe convertirse',
            'estado': 'Finalizado',
            'avance_obra': 100,
            'expected': 'Terminado'
        },
        {
            'name': 'En liquidación debe convertirse',
            'estado': 'En liquidación',
            'avance_obra': 100,
            'expected': 'Terminado'
        },
        {
            'name': 'Socialización debe convertirse',
            'estado': 'Socialización',
            'avance_obra': 0,
            'expected': 'En Alistamiento'
        }
    ]
    
    all_passed = True
    
    for test_case in test_cases:
        print(f"\nTest: {test_case['name']}")
        print("-" * 80)
        
        # Create test GeoJSON feature
        test_feature = {
            'type': 'Feature',
            'properties': {
                'upid': 'UNP-TEST-001',
                'nombre_up': 'Proyecto de Prueba',
                'estado': test_case['estado'],
                'avance_obra': test_case['avance_obra'],
                'barrio_vereda': 'BARRIO TEJADA',
                'fecha_inicio_std': '2024-01-15T00:00:00'
            },
            'geometry': {
                'type': 'Point',
                'coordinates': [-76.5, 3.4]
            }
        }
        
        print(f"  Input estado: '{test_case['estado']}'")
        print(f"  Input avance_obra: {test_case['avance_obra']}")
        
        # Prepare document
        document = prepare_document_data(test_feature)
        
        if not document:
            print("  ❌ FAIL: prepare_document_data returned None")
            all_passed = False
            continue
        
        actual_estado = document.get('estado')
        expected_estado = test_case['expected']
        
        print(f"  Output estado: '{actual_estado}'")
        print(f"  Expected: '{expected_estado}'")
        
        passed = actual_estado == expected_estado
        all_passed = all_passed and passed
        
        print(f"  {'✅ PASS' if passed else '❌ FAIL'}")
    
    print()
    print("="*80)
    if all_passed:
        print("✅ ALL TESTS PASSED: Auto-normalization working correctly")
    else:
        print("❌ SOME TESTS FAILED")
    print("="*80)
    
    return all_passed


if __name__ == "__main__":
    print("Starting data quality preservation tests...")
    print()
    
    # Run tests
    test1_passed = test_serialize_for_firebase()
    test2_passed = test_normalize_estado_in_load()
    test3_passed = test_prepare_document_data()
    
    print()
    print("="*80)
    if test1_passed and test2_passed and test3_passed:
        print("✅ ALL TESTS PASSED: Data quality is preserved and estados are auto-normalized during loading")
    else:
        print("❌ SOME TESTS FAILED: Data quality may be compromised")
    print("="*80)
    
    sys.exit(0 if (test1_passed and test2_passed and test3_passed) else 1)
