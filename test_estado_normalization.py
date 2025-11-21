# -*- coding: utf-8 -*-
"""
Test script to verify estado normalization is working correctly.
This script tests the normalize_estado_values function with various edge cases.
"""

import pandas as pd
import sys
import os

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

from data_transformation_unidades_proyecto import normalize_estado_values

def test_estado_normalization():
    """Test estado normalization with various input values."""
    
    print("="*80)
    print("TEST: Estado Normalization")
    print("="*80)
    print()
    
    # Create test data with various estado values
    test_data = {
        'estado': [
            # Valid states (should remain as-is after normalization)
            'En alistamiento',
            'En ejecución',
            'Terminado',
            
            # Common variations
            'EN ALISTAMIENTO',
            'EN EJECUCIÓN',
            'terminado',
            'En Ejecución',
            
            # Partial matches (should be normalized)
            'Socialización',
            'En socialización',
            'Alistamiento',
            'Ejecución',
            'En proceso',
            'Finalizado',
            'Completado',
            'En liquidación',
            
            # Edge cases
            None,
            '',
            'Sin estado',
            'Pendiente',
            'Por iniciar',
            'Otro valor extraño',
            
            # Special values (should be preserved as-is)
            'Suspendido',
            'SUSPENDIDO',
            'suspendido',
            'Inaugurado',
            'INAUGURADO',
            'inaugurado',
            
            # Invalid/unknown states
            'Cancelado',
            'En revisión',
        ],
        'avance_obra': [
            # Corresponding avance_obra values
            0, 50, 100,  # Valid states
            0, 50, 100, 50,  # Variations
            0, 0, 0, 50, 50, 100, 100, 100,  # Partial matches
            None, 0, 0, 0, 0, 50,  # Edge cases
            50, 50, 50, 100, 100, 100,  # Special values (should be preserved)
            0, 50  # Invalid states
        ]
    }
    
    df = pd.DataFrame(test_data)
    
    print("ORIGINAL DATA:")
    print("-" * 80)
    for idx, row in df.iterrows():
        print(f"{idx:2d}. estado='{row['estado']}', avance_obra={row['avance_obra']}")
    print()
    
    # Apply normalization
    print("APPLYING NORMALIZATION...")
    print("-" * 80)
    df_normalized = normalize_estado_values(df)
    print()
    
    print("NORMALIZED DATA:")
    print("-" * 80)
    for idx, row in df_normalized.iterrows():
        original = test_data['estado'][idx]
        normalized = row['estado']
        avance = row['avance_obra']
        
        # Check if it changed
        changed = " → " if str(original) != str(normalized) else " = "
        print(f"{idx:2d}. '{original}' {changed} '{normalized}' (avance: {avance})")
    print()
    
    # Validation
    print("VALIDATION:")
    print("-" * 80)
    # Include special values in valid states
    valid_states = {'En alistamiento', 'En ejecución', 'Terminado', 'Suspendido', 'Inaugurado'}
    unique_states = set(df_normalized['estado'].dropna().unique())
    
    print(f"Unique states after normalization: {sorted(unique_states)}")
    print()
    
    invalid_states = unique_states - valid_states
    if invalid_states:
        print(f"❌ FAIL: Found invalid estados: {invalid_states}")
        print()
        print("Records with invalid estados:")
        for state in invalid_states:
            mask = df_normalized['estado'] == state
            print(f"\n  Estado: '{state}'")
            for idx in df_normalized[mask].index:
                print(f"    - Row {idx}: original='{test_data['estado'][idx]}', avance={df_normalized.loc[idx, 'avance_obra']}")
        return False
    else:
        print(f"✅ PASS: All estados are valid!")
        print()
        print("Distribution:")
        for state in sorted(unique_states):
            count = (df_normalized['estado'] == state).sum()
            percentage = (count / len(df_normalized)) * 100
            print(f"  - {state}: {count} ({percentage:.1f}%)")
        
        # Special validation: Check that special values are preserved
        print()
        print("Special values validation:")
        suspendido_count = (df_normalized['estado'] == 'Suspendido').sum()
        inaugurado_count = (df_normalized['estado'] == 'Inaugurado').sum()
        
        # Count how many should be preserved (case-insensitive)
        expected_suspendido = sum(1 for s in test_data['estado'] if isinstance(s, str) and s.lower() == 'suspendido')
        expected_inaugurado = sum(1 for s in test_data['estado'] if isinstance(s, str) and s.lower() == 'inaugurado')
        
        if suspendido_count == expected_suspendido:
            print(f"  ✅ 'Suspendido' preserved correctly: {suspendido_count}/{expected_suspendido}")
        else:
            print(f"  ❌ 'Suspendido' not preserved: {suspendido_count}/{expected_suspendido}")
            return False
            
        if inaugurado_count == expected_inaugurado:
            print(f"  ✅ 'Inaugurado' preserved correctly: {inaugurado_count}/{expected_inaugurado}")
        else:
            print(f"  ❌ 'Inaugurado' not preserved: {inaugurado_count}/{expected_inaugurado}")
            return False
        
        return True

if __name__ == "__main__":
    success = test_estado_normalization()
    
    print()
    print("="*80)
    if success:
        print("✅ TEST PASSED: Estado normalization is working correctly")
    else:
        print("❌ TEST FAILED: Estado normalization has issues")
    print("="*80)
    
    sys.exit(0 if success else 1)
