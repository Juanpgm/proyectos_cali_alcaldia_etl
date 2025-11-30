# -*- coding: utf-8 -*-
"""
Test para verificar que los estados 'Inaugurado' y 'Suspendido' se preservan correctamente.
"""

import pandas as pd
import sys
import os

# Add transformation_app to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'transformation_app'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'load_app'))

from data_transformation_unidades_proyecto import normalize_estado_values
from data_loading_unidades_proyecto import normalize_estado_value

def test_preserved_states():
    """Test que los estados Inaugurado y Suspendido NO se modifican sin importar el avance_obra."""
    
    # Crear DataFrame de prueba con diferentes escenarios
    test_data = pd.DataFrame({
        'estado': [
            'Inaugurado',      # Debe permanecer como Inaugurado
            'inaugurado',      # Debe normalizarse a Inaugurado
            'INAUGURADO',      # Debe normalizarse a Inaugurado
            'Suspendido',      # Debe permanecer como Suspendido
            'suspendido',      # Debe normalizarse a Suspendido
            'SUSPENDIDO',      # Debe normalizarse a Suspendido
            'Terminado',       # Debe permanecer como Terminado
            'En ejecución',    # Debe permanecer
            'En alistamiento', # Debe permanecer
        ],
        'avance_obra': [
            100,    # Inaugurado con avance 100 - NO debe cambiar a Terminado
            100,    # inaugurado con avance 100 - NO debe cambiar a Terminado
            50,     # INAUGURADO con avance 50 - NO debe cambiar
            0,      # Suspendido con avance 0 - NO debe cambiar a En alistamiento
            100,    # suspendido con avance 100 - NO debe cambiar a Terminado
            50,     # SUSPENDIDO con avance 50 - NO debe cambiar
            100,    # Terminado - debe permanecer
            50,     # En ejecución - debe permanecer
            0,      # En alistamiento - debe permanecer
        ]
    })
    
    print("=" * 80)
    print("TEST: Preservación de estados Inaugurado y Suspendido")
    print("=" * 80)
    
    print("\n📊 Datos de entrada:")
    print(test_data.to_string(index=True))
    
    # Aplicar normalización
    result_df = normalize_estado_values(test_data)
    
    print("\n📊 Datos después de normalización:")
    print(result_df.to_string(index=True))
    
    # Verificaciones
    print("\n" + "=" * 80)
    print("VERIFICACIONES:")
    print("=" * 80)
    
    errors = []
    
    # Test 1: Inaugurado debe permanecer como Inaugurado (índices 0, 1, 2)
    for i in [0, 1, 2]:
        expected = 'Inaugurado'
        actual = result_df.loc[i, 'estado']
        status = "✅ PASS" if actual == expected else "❌ FAIL"
        print(f"{status} - Fila {i}: estado original='{test_data.loc[i, 'estado']}', avance={test_data.loc[i, 'avance_obra']} → esperado='{expected}', obtenido='{actual}'")
        if actual != expected:
            errors.append(f"Fila {i}: esperado '{expected}', obtenido '{actual}'")
    
    # Test 2: Suspendido debe permanecer como Suspendido (índices 3, 4, 5)
    for i in [3, 4, 5]:
        expected = 'Suspendido'
        actual = result_df.loc[i, 'estado']
        status = "✅ PASS" if actual == expected else "❌ FAIL"
        print(f"{status} - Fila {i}: estado original='{test_data.loc[i, 'estado']}', avance={test_data.loc[i, 'avance_obra']} → esperado='{expected}', obtenido='{actual}'")
        if actual != expected:
            errors.append(f"Fila {i}: esperado '{expected}', obtenido '{actual}'")
    
    # Test 3: Terminado debe permanecer como Terminado (índice 6)
    expected = 'Terminado'
    actual = result_df.loc[6, 'estado']
    status = "✅ PASS" if actual == expected else "❌ FAIL"
    print(f"{status} - Fila 6: estado original='{test_data.loc[6, 'estado']}', avance={test_data.loc[6, 'avance_obra']} → esperado='{expected}', obtenido='{actual}'")
    if actual != expected:
        errors.append(f"Fila 6: esperado '{expected}', obtenido '{actual}'")
    
    # Resumen
    print("\n" + "=" * 80)
    if errors:
        print(f"❌ TEST FALLIDO - {len(errors)} errores encontrados:")
        for error in errors:
            print(f"   - {error}")
        return False
    else:
        print("✅ TODOS LOS TESTS DE TRANSFORMACIÓN PASARON")
        return True


def test_loading_normalize_estado():
    """Test que la función de carga también preserva Inaugurado y Suspendido."""
    
    print("\n" + "=" * 80)
    print("TEST: Función normalize_estado_value (módulo de carga)")
    print("=" * 80)
    
    test_cases = [
        ('Inaugurado', 100, 'Inaugurado'),
        ('inaugurado', 100, 'Inaugurado'),
        ('INAUGURADO', 50, 'Inaugurado'),
        ('Suspendido', 0, 'Suspendido'),
        ('suspendido', 100, 'Suspendido'),
        ('SUSPENDIDO', 50, 'Suspendido'),
        ('Terminado', 100, 'Terminado'),
        ('En ejecución', 50, 'En ejecución'),
    ]
    
    errors = []
    
    for estado, avance, expected in test_cases:
        actual = normalize_estado_value(estado, avance)
        status = "✅ PASS" if actual == expected else "❌ FAIL"
        print(f"{status} - estado='{estado}', avance={avance} → esperado='{expected}', obtenido='{actual}'")
        if actual != expected:
            errors.append(f"estado='{estado}': esperado '{expected}', obtenido '{actual}'")
    
    print("\n" + "=" * 80)
    if errors:
        print(f"❌ TEST FALLIDO - {len(errors)} errores encontrados:")
        for error in errors:
            print(f"   - {error}")
        return False
    else:
        print("✅ TODOS LOS TESTS DE CARGA PASARON")
        return True


if __name__ == "__main__":
    success1 = test_preserved_states()
    success2 = test_loading_normalize_estado()
    
    print("\n" + "=" * 80)
    if success1 and success2:
        print("✅ TODOS LOS TESTS PASARON")
    else:
        print("❌ ALGUNOS TESTS FALLARON")
    print("=" * 80)
    
    sys.exit(0 if (success1 and success2) else 1)
