# -*- coding: utf-8 -*-
"""
Test Rápido de Correcciones de Validación
==========================================

Verifica que las reglas de validación usen los valores correctos.

Author: ETL QA Team
Date: November 2025
"""

import sys
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

from utils.quality_control import DataQualityValidator


def test_validation_rules():
    """Verifica que las reglas de validación usen valores correctos."""
    
    print("\n" + "="*80)
    print("🧪 TEST: VERIFICACIÓN DE REGLAS DE VALIDACIÓN")
    print("="*80)
    
    validator = DataQualityValidator()
    
    # Test 1: Verificar estados válidos
    print("\n📋 Test 1: Estados válidos")
    print(f"   Valores: {validator.VALID_ESTADOS}")
    
    expected_estados = {'En alistamiento', 'En ejecución', 'Terminado'}
    if validator.VALID_ESTADOS == expected_estados:
        print("   ✅ CORRECTO - Estados con mayúsculas apropiadas")
    else:
        print("   ❌ ERROR - Estados incorrectos")
        print(f"      Esperado: {expected_estados}")
        print(f"      Actual: {validator.VALID_ESTADOS}")
        return False
    
    # Test 2: Verificar tipos de intervención
    print("\n📋 Test 2: Tipos de intervención válidos")
    print(f"   Valores: {validator.VALID_TIPOS_INTERVENCION}")
    
    if 'Obra nueva' in validator.VALID_TIPOS_INTERVENCION:
        print("   ✅ CORRECTO - 'Obra nueva' incluida")
    else:
        print("   ❌ ERROR - 'Obra nueva' NO está incluida")
        return False
    
    if 'Construcción Nueva' in validator.VALID_TIPOS_INTERVENCION:
        print("   ✅ CORRECTO - 'Construcción Nueva' incluida como alias")
    else:
        print("   ⚠️  ADVERTENCIA - 'Construcción Nueva' no incluida")
    
    # Test 3: Validar registro con "Obra nueva"
    print("\n📋 Test 3: Validación de registro con 'Obra nueva'")
    
    test_record = {
        'upid': 'UP-TEST',
        'nombre_up': 'Test UP',
        'estado': 'En alistamiento',
        'avance_obra': 0,
        'ano': 2025,
        'nombre_centro_gestor': 'Test Centro',
        'comuna_corregimiento': 'Comuna 1',
        'tipo_intervencion': 'Obra nueva'  # Este valor debe ser válido
    }
    
    issues = validator.validate_record(test_record, 0)
    
    # Filtrar problemas de tipo_intervencion
    tipo_issues = [i for i in issues if i.field_name == 'tipo_intervencion']
    
    if len(tipo_issues) == 0:
        print("   ✅ CORRECTO - 'Obra nueva' se valida correctamente")
    else:
        print("   ❌ ERROR - 'Obra nueva' genera problema de validación")
        for issue in tipo_issues:
            print(f"      {issue.details}")
        return False
    
    # Test 4: Validar registro con estado "En alistamiento" y avance 0%
    print("\n📋 Test 4: Validación de 'En alistamiento' con avance 0%")
    
    test_record_2 = {
        'upid': 'UP-TEST-2',
        'nombre_up': 'Test UP 2',
        'estado': 'En alistamiento',
        'avance_obra': 0,
        'ano': 2025,
        'nombre_centro_gestor': 'Test Centro',
        'comuna_corregimiento': 'Comuna 1',
        'tipo_intervencion': 'Mejoramiento'
    }
    
    issues_2 = validator.validate_record(test_record_2, 0)
    
    # Filtrar problemas de estado relacionados con avance
    estado_issues = [
        i for i in issues_2 
        if i.field_name == 'estado' and 'avance' in i.details.lower()
    ]
    
    if len(estado_issues) == 0:
        print("   ✅ CORRECTO - Estado 'En alistamiento' con 0% no genera error")
    else:
        print("   ❌ ERROR - Estado 'En alistamiento' con 0% genera error falso positivo")
        for issue in estado_issues:
            print(f"      {issue.details}")
        return False
    
    # Test 5: Verificar regla LC001
    print("\n📋 Test 5: Descripción de regla LC001")
    rule_lc001 = validator.rules.get('LC001')
    if rule_lc001:
        print(f"   Regla: {rule_lc001.name}")
        print(f"   Descripción: {rule_lc001.description[:100]}...")
        
        # Verificar que la descripción use las mayúsculas correctas
        if 'alistamiento' in rule_lc001.description.lower():
            print("   ✅ CORRECTO - Descripción usa términos apropiados")
        else:
            print("   ⚠️  ADVERTENCIA - Revisar descripción de regla")
    
    print("\n" + "="*80)
    print("✅ TODAS LAS PRUEBAS PASARON")
    print("="*80)
    print("\n📊 Resumen:")
    print("   ✓ Estados válidos correctos: En alistamiento, En ejecución, Terminado")
    print("   ✓ 'Obra nueva' reconocida como tipo de intervención válido")
    print("   ✓ No hay falsos positivos para 'En alistamiento' + 0%")
    print("\n✨ Reglas de validación actualizadas correctamente")
    
    return True


if __name__ == "__main__":
    try:
        success = test_validation_rules()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Error durante test: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
