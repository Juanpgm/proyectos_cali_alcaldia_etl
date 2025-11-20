# -*- coding: utf-8 -*-
"""
Script de prueba para validar normalización de estados en infraestructura vial.
Verifica que solo existan 3 estados válidos:
- "En alistamiento"
- "En ejecución"  
- "Terminado"
"""

import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from load_app.data_loading_unidades_proyecto_infraestructura import normalize_estado_value

def test_normalize_estado_infraestructura():
    """Test all estado normalization scenarios for infrastructure data."""
    
    print("="*80)
    print("PRUEBA DE NORMALIZACIÓN DE ESTADOS - INFRAESTRUCTURA VIAL")
    print("="*80)
    print()
    
    # Test cases with different estado variations
    test_cases = [
        # (estado_input, avance_obra, expected_output, description)
        ("En Socialización", None, "En alistamiento", "Socialización → alistamiento"),
        ("En Alistamiento", None, "En alistamiento", "Alistamiento → alistamiento"),
        ("planeación", None, "En alistamiento", "Planeación → alistamiento"),
        ("Por iniciar", None, "En alistamiento", "Por iniciar → alistamiento"),
        
        ("En Ejecución", None, "En ejecución", "Ejecución → ejecución"),
        ("en proceso", None, "En ejecución", "En proceso → ejecución"),
        ("construcción", None, "En ejecución", "Construcción → ejecución"),
        ("En desarrollo", None, "En ejecución", "Desarrollo → ejecución"),
        
        ("Terminado", None, "Terminado", "Terminado correcto"),
        ("Finalizado", None, "Terminado", "Finalizado → Terminado"),
        ("completado", None, "Terminado", "Completado → Terminado"),
        ("En liquidación", None, "Terminado", "En liquidación → Terminado"),
        ("Entregado", None, "Terminado", "Entregado → Terminado"),
        
        # Business rules: avance_obra = 0
        ("En Ejecución", 0, "En alistamiento", "Ejecución + avance 0 → alistamiento"),
        ("cualquier cosa", "0", "En alistamiento", "Desconocido + avance 0 → alistamiento"),
        
        # Business rules: avance_obra = 100
        ("En Ejecución", 100, "Terminado", "Ejecución + avance 100 → Terminado"),
        ("En Alistamiento", "100", "Terminado", "Alistamiento + avance 100 → Terminado"),
        
        # Business rules: avance_obra between 0 and 100 with None estado
        ("", 50, "En ejecución", "Sin estado + avance 50 → ejecución"),
        (None, 50, "En ejecución", "None + avance 50 → ejecución"),
        (None, 0, "En alistamiento", "None + avance 0 → alistamiento"),
        (None, 100, "Terminado", "None + avance 100 → Terminado"),
        
        # Unknown states default to ejecución
        ("estado desconocido", None, "En ejecución", "Desconocido → ejecución (default)"),
        ("XYZ", None, "En ejecución", "XYZ → ejecución (default)"),
        
        # Edge cases
        ("", None, "En alistamiento", "String vacío sin avance → alistamiento"),
        (None, None, "En alistamiento", "None sin avance → alistamiento"),
    ]
    
    print(f"Ejecutando {len(test_cases)} pruebas...\n")
    
    passed = 0
    failed = 0
    
    for i, (estado_input, avance_obra, expected, description) in enumerate(test_cases, 1):
        result = normalize_estado_value(estado_input, avance_obra)
        status = "✓" if result == expected else "✗"
        
        if result == expected:
            passed += 1
            print(f"{status} Test {i:2d}: {description}")
            print(f"           Input: '{estado_input}' (avance: {avance_obra})")
            print(f"           Output: '{result}'")
        else:
            failed += 1
            print(f"{status} Test {i:2d}: {description}")
            print(f"           Input: '{estado_input}' (avance: {avance_obra})")
            print(f"           Expected: '{expected}'")
            print(f"           Got: '{result}'")
        print()
    
    # Summary
    print("="*80)
    print("RESUMEN DE PRUEBAS")
    print("="*80)
    print(f"Total de pruebas: {len(test_cases)}")
    print(f"✓ Pasadas: {passed}")
    print(f"✗ Fallidas: {failed}")
    print(f"Tasa de éxito: {(passed/len(test_cases)*100):.1f}%")
    print()
    
    # Validation: check valid estados
    print("="*80)
    print("VALIDACIÓN DE ESTADOS VÁLIDOS")
    print("="*80)
    
    valid_estados = {"En alistamiento", "En ejecución", "Terminado"}
    all_results = [normalize_estado_value(estado, avance) for estado, avance, _, _ in test_cases]
    unique_results = set(all_results)
    
    print(f"Estados únicos producidos: {sorted([str(e) for e in unique_results])}")
    print(f"Estados válidos esperados: {sorted([str(e) for e in valid_estados])}")
    
    invalid_results = unique_results - valid_estados
    if invalid_results:
        print(f"\n❌ ERROR: Se produjeron estados inválidos: {invalid_results}")
        return False
    else:
        print(f"\n✅ ÉXITO: Solo se produjeron estados válidos")
        print(f"   - Estados sin None: {sorted([e for e in unique_results if e is not None])}")
        return True


if __name__ == "__main__":
    print("\nIniciando pruebas de normalización de estados para infraestructura vial...\n")
    
    success = test_normalize_estado_infraestructura()
    
    if success:
        print("\n" + "="*80)
        print("✅ TODAS LAS PRUEBAS PASARON")
        print("="*80)
        print("La normalización de estados funciona correctamente para infraestructura.")
        print("Solo se producen 3 estados válidos:")
        print("  1. En alistamiento")
        print("  2. En ejecución")
        print("  3. Terminado")
    else:
        print("\n" + "="*80)
        print("❌ ALGUNAS PRUEBAS FALLARON")
        print("="*80)
        print("Revisar la función normalize_estado_value en:")
        print("load_app/data_loading_unidades_proyecto_infraestructura.py")
