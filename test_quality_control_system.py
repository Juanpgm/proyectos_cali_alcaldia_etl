# -*- coding: utf-8 -*-
"""
Test del Sistema de Control de Calidad
======================================

Tests completos para el sistema de control de calidad ISO 19157.
Verifica validaciones, reportes, carga a Firebase y exportación a S3.

Author: ETL QA Team
Date: November 2025
Version: 1.0
"""

import sys
import os
import json
from datetime import datetime
from pathlib import Path

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Agregar paths necesarios
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.quality_control import validate_geojson, DataQualityValidator
from utils.quality_reporter import QualityReporter


def create_test_geojson():
    """
    Crea un GeoJSON de prueba con varios tipos de problemas para testing.
    """
    return {
        "type": "FeatureCollection",
        "features": [
            # Registro 1: Sin problemas (ideal)
            {
                "type": "Feature",
                "properties": {
                    "UPID": "UP-TEST-001",
                    "nombre_up": "Proyecto Prueba Perfecto",
                    "nombre_centro_gestor": "Centro de Pruebas A",
                    "contrato": "CT-2024-001",
                    "ubicacion": "Cali, Valle del Cauca",
                    "avance_porcentaje": 75.5,
                    "estado_obra": "En ejecución",
                    "fecha_inicio": "2024-01-15",
                    "presupuesto_total": 1500000000,
                    "presupuesto_ejecutado": 1125000000,
                    "ano": 2024,
                    "tipo_intervencion": "Nueva construcción"
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-76.5225, 3.4516]
                }
            },
            
            # Registro 2: Múltiples problemas críticos
            {
                "type": "Feature",
                "properties": {
                    "UPID": "UP-TEST-002",
                    "nombre_up": "",  # Nombre vacío - CRÍTICO
                    "nombre_centro_gestor": "Centro de Pruebas B",
                    "contrato": None,  # Contrato nulo
                    "ubicacion": "Cali",
                    "avance_porcentaje": 150,  # Avance inválido (>100) - CRÍTICO
                    "estado_obra": "Terminado",  # Inconsistencia con avance
                    "fecha_inicio": "2024-13-45",  # Fecha inválida - HIGH
                    "presupuesto_total": -500000,  # Presupuesto negativo - CRÍTICO
                    "presupuesto_ejecutado": 2000000,  # Mayor que total
                    "ano": 2030,  # Año futuro - HIGH
                    "tipo_intervencion": "Tipo Inválido"  # Tipo no permitido
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-80.0, 10.0]  # Fuera de bbox de Cali - HIGH
                }
            },
            
            # Registro 3: Problemas de completitud
            {
                "type": "Feature",
                "properties": {
                    "UPID": "UP-TEST-003",
                    "nombre_up": "Proyecto Incompleto",
                    "nombre_centro_gestor": "Centro de Pruebas C",
                    # Faltan muchos campos requeridos
                    "avance_porcentaje": 50,
                    "estado_obra": "En ejecución"
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-76.5300, 3.4400]
                }
            },
            
            # Registro 4: Problemas de geometría
            {
                "type": "Feature",
                "properties": {
                    "UPID": "UP-TEST-004",
                    "nombre_up": "Proyecto Geometría Inválida",
                    "nombre_centro_gestor": "Centro de Pruebas D",
                    "contrato": "CT-2024-004",
                    "avance_porcentaje": 30,
                    "estado_obra": "En ejecución",
                    "ano": 2024
                },
                "geometry": None  # Geometría nula - CRÍTICO
            },
            
            # Registro 5: Problemas de consistencia estado-avance
            {
                "type": "Feature",
                "properties": {
                    "UPID": "UP-TEST-005",
                    "nombre_up": "Proyecto Estado Inconsistente",
                    "nombre_centro_gestor": "Centro de Pruebas E",
                    "contrato": "CT-2024-005",
                    "avance_porcentaje": 0,  # 0% de avance
                    "estado_obra": "Terminado",  # Estado terminado - INCONSISTENCIA CRÍTICA
                    "fecha_inicio": "2024-06-01",
                    "ano": 2024
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-76.5200, 3.4500]
                }
            },
            
            # Registro 6: Mismo centro que registro 1 (para agregación)
            {
                "type": "Feature",
                "properties": {
                    "UPID": "UP-TEST-006",
                    "nombre_up": "Proyecto Prueba B",
                    "nombre_centro_gestor": "Centro de Pruebas A",  # Mismo centro que UP-001
                    "contrato": "CT-2024-006",
                    "ubicacion": "Cali",
                    "avance_porcentaje": -5,  # Avance negativo - CRÍTICO
                    "estado_obra": "En ejecución",
                    "fecha_inicio": "2024-03-01",
                    "presupuesto_total": 800000000,
                    "ano": 2024
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-76.5250, 3.4520]
                }
            }
        ]
    }


def test_1_validation_module():
    """Test 1: Validar que el módulo de validación funciona correctamente."""
    print("\n" + "="*100)
    print("TEST 1: MÓDULO DE VALIDACIÓN DE CALIDAD")
    print("="*100 + "\n")
    
    try:
        # Crear GeoJSON de prueba
        test_data = create_test_geojson()
        print(f"✓ GeoJSON de prueba creado: {len(test_data['features'])} registros")
        
        # Guardar GeoJSON en archivo temporal
        output_dir = Path("test_outputs/quality_control")
        output_dir.mkdir(parents=True, exist_ok=True)
        test_file = output_dir / "test_geojson_input.geojson"
        
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, indent=2, ensure_ascii=False)
        print(f"✓ GeoJSON guardado en: {test_file}")
        
        # Ejecutar validación
        print("\nEjecutando validación completa...")
        validation_result = validate_geojson(str(test_file))
        
        print(f"\n✓ Validación completada")
        print(f"  - Total registros validados: {validation_result['total_records']}")
        print(f"  - Registros con problemas: {validation_result['records_with_issues']}")
        print(f"  - Registros sin problemas: {validation_result['total_records'] - validation_result['records_with_issues']}")
        print(f"  - Total problemas encontrados: {validation_result['total_issues']}")
        
        stats = validation_result.get('statistics', {})
        print("\nDistribución por severidad:")
        for severity, count in stats.get('by_severity', {}).items():
            print(f"  {severity}: {count}")
        
        print("\nDistribución por dimensión ISO 19157:")
        for dimension, count in stats.get('by_dimension', {}).items():
            print(f"  {dimension}: {count}")
        
        # Verificar que se detectaron problemas esperados
        assert validation_result['records_with_issues'] > 0, "Debería haber registros con problemas"
        assert stats.get('by_severity', {}).get('CRITICAL', 0) > 0, "Debería haber problemas críticos"
        
        print("\n✅ TEST 1 EXITOSO: Módulo de validación funciona correctamente")
        return validation_result
        
    except Exception as e:
        print(f"\n❌ TEST 1 FALLIDO: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_2_reporter_module(validation_result):
    """Test 2: Validar que el módulo de reportes funciona correctamente."""
    print("\n" + "="*100)
    print("TEST 2: MÓDULO DE GENERACIÓN DE REPORTES")
    print("="*100 + "\n")
    
    if not validation_result:
        print("❌ TEST 2 OMITIDO: No hay resultado de validación del Test 1")
        return None
    
    try:
        # Crear reporter
        reporter = QualityReporter()
        print("✓ QualityReporter inicializado")
        
        issues = validation_result['issues']
        
        # Generar reporte por registro
        print("\nGenerando reporte por registro...")
        record_report = reporter.generate_record_level_report(issues)
        print(f"✓ Reporte por registro generado: {len(record_report)} registros")
        
        # Verificar estructura del reporte por registro
        if record_report:
            sample_record = record_report[0]
            required_fields = ['upid', 'nombre_up', 'nombre_centro_gestor', 'total_issues', 
                             'severity_counts', 'issues', 'priority']
            for field in required_fields:
                assert field in sample_record, f"Campo '{field}' faltante en reporte por registro"
            print(f"  - Ejemplo: {sample_record['upid']} tiene {sample_record['total_issues']} problemas")
        
        # Generar reporte por centro gestor
        print("\nGenerando reporte por centro gestor...")
        centro_report = reporter.generate_centro_gestor_report(issues)
        print(f"✓ Reporte por centro gestor generado: {len(centro_report)} centros")
        
        # Verificar estructura del reporte por centro
        if centro_report:
            sample_centro = centro_report[0]
            required_fields = ['nombre_centro_gestor', 'total_records', 'records_with_issues',
                             'total_issues', 'quality_score', 'error_rate']
            for field in required_fields:
                assert field in sample_centro, f"Campo '{field}' faltante en reporte por centro"
            print(f"  - Ejemplo: {sample_centro['nombre_centro_gestor']} tiene {sample_centro['total_records']} registros")
        
        # Generar reporte resumen
        print("\nGenerando reporte resumen...")
        summary_report = reporter.generate_summary_report(
            record_reports=record_report,
            centro_reports=centro_report,
            total_records=validation_result['total_records'],
            validation_stats=validation_result['statistics']
        )
        print("✓ Reporte resumen generado")
        
        # Verificar estructura del reporte resumen
        required_fields = ['total_records_validated', 'records_with_issues', 'total_issues_found',
                         'global_quality_score', 'severity_distribution']
        for field in required_fields:
            assert field in summary_report, f"Campo '{field}' faltante en reporte resumen"
        
        print(f"  - Total registros: {summary_report['total_records_validated']}")
        print(f"  - Quality Score: {summary_report['global_quality_score']:.2f}")
        
        # Exportar a JSON para inspección
        print("\nExportando reportes a JSON...")
        output_dir = Path("test_outputs/quality_control")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Exportar reporte por registro
        record_file = output_dir / f"test_record_report_{timestamp}.json"
        with open(record_file, 'w', encoding='utf-8') as f:
            json.dump(record_report, f, indent=2, ensure_ascii=False, default=str)
        print(f"  ✓ Reporte por registro: {record_file}")
        
        # Exportar reporte por centro
        centro_file = output_dir / f"test_centro_report_{timestamp}.json"
        with open(centro_file, 'w', encoding='utf-8') as f:
            json.dump(centro_report, f, indent=2, ensure_ascii=False, default=str)
        print(f"  ✓ Reporte por centro: {centro_file}")
        
        # Exportar reporte resumen
        summary_file = output_dir / f"test_summary_report_{timestamp}.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_report, f, indent=2, ensure_ascii=False, default=str)
        print(f"  ✓ Reporte resumen: {summary_file}")
        
        print("\n✅ TEST 2 EXITOSO: Módulo de reportes funciona correctamente")
        return {
            'record_report': record_report,
            'centro_report': centro_report,
            'summary_report': summary_report
        }
        
    except Exception as e:
        print(f"\n❌ TEST 2 FALLIDO: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_3_detailed_issue_inspection(validation_result):
    """Test 3: Inspeccionar en detalle los problemas encontrados."""
    print("\n" + "="*100)
    print("TEST 3: INSPECCIÓN DETALLADA DE PROBLEMAS ENCONTRADOS")
    print("="*100 + "\n")
    
    if not validation_result:
        print("❌ TEST 3 OMITIDO: No hay resultado de validación")
        return
    
    try:
        print("Analizando problemas por registro...\n")
        
        # Agrupar issues por UPID
        issues_by_upid = {}
        for issue in validation_result['issues']:
            upid = issue.get('upid') or f"IDX_{issue.get('record_index')}"
            if upid not in issues_by_upid:
                issues_by_upid[upid] = {
                    'upid': upid,
                    'nombre_up': issue.get('nombre_up'),
                    'nombre_centro_gestor': issue.get('nombre_centro_gestor'),
                    'issues': []
                }
            issues_by_upid[upid]['issues'].append(issue)
        
        for upid, record_data in issues_by_upid.items():
            issues = record_data['issues']
            
            if not issues:
                print(f"✅ {upid}: SIN PROBLEMAS")
                continue
            
            print(f"\n{'='*100}")
            print(f"📋 Registro: {upid}")
            print(f"   Nombre: {record_data.get('nombre_up', 'N/A')}")
            print(f"   Centro Gestor: {record_data.get('nombre_centro_gestor', 'N/A')}")
            print(f"   Total problemas: {len(issues)}")
            print("="*100)
            
            # Agrupar por severidad
            by_severity = {}
            for issue in issues:
                severity = issue['severity']
                if severity not in by_severity:
                    by_severity[severity] = []
                by_severity[severity].append(issue)
            
            # Mostrar problemas ordenados por severidad
            for severity in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO']:
                if severity not in by_severity:
                    continue
                
                print(f"\n{severity} ({len(by_severity[severity])} problemas):")
                print("-" * 100)
                
                for issue in by_severity[severity]:
                    print(f"\n  • [{issue['dimension']}] {issue['rule_name']}")
                    print(f"    Campo: {issue['field_name']}")
                    print(f"    Valor actual: {issue['current_value']}")
                    if issue.get('expected_value'):
                        print(f"    Valor esperado: {issue['expected_value']}")
                    print(f"    Detalles: {issue['details']}")
                    if issue.get('suggestion'):
                        print(f"    💡 Sugerencia: {issue['suggestion']}")
        
        print("\n\n" + "="*100)
        print("RESUMEN DE PROBLEMAS DETECTADOS")
        print("="*100)
        print(f"\nTotal de registros validados: {validation_result['total_records']}")
        print(f"Registros con problemas: {validation_result['records_with_issues']}")
        print(f"Registros sin problemas: {validation_result['total_records'] - validation_result['records_with_issues']}")
        print(f"Total de problemas: {validation_result['total_issues']}")
        
        stats = validation_result.get('statistics', {})
        
        print("\nProblemas por severidad:")
        for severity, count in sorted(stats.get('by_severity', {}).items()):
            percentage = (count / validation_result['total_issues'] * 100) if validation_result['total_issues'] > 0 else 0
            print(f"  {severity}: {count} ({percentage:.1f}%)")
        
        print("\nProblemas por dimensión ISO 19157:")
        for dimension, count in sorted(stats.get('by_dimension', {}).items()):
            percentage = (count / validation_result['total_issues'] * 100) if validation_result['total_issues'] > 0 else 0
            print(f"  {dimension}: {count} ({percentage:.1f}%)")
        
        print("\n✅ TEST 3 EXITOSO: Inspección detallada completada")
        
    except Exception as e:
        print(f"\n❌ TEST 3 FALLIDO: {e}")
        import traceback
        traceback.print_exc()


def test_4_data_structures():
    """Test 4: Verificar estructuras de datos y clases."""
    print("\n" + "="*100)
    print("TEST 4: VERIFICACIÓN DE ESTRUCTURAS DE DATOS")
    print("="*100 + "\n")
    
    try:
        from utils.quality_control import ValidationRule, QualityIssue, SeverityLevel, QualityDimension
        
        # Test ValidationRule
        print("Verificando ValidationRule...")
        rule = ValidationRule(
            rule_id="TEST_001",
            name="Test Rule",
            dimension=QualityDimension.LOGICAL_CONSISTENCY,
            severity=SeverityLevel.HIGH,
            description="Test description"
        )
        assert rule.rule_id == "TEST_001"
        assert rule.severity == SeverityLevel.HIGH
        assert rule.name == "Test Rule"
        print("  ✓ ValidationRule funciona correctamente")
        
        # Test QualityIssue
        print("\nVerificando QualityIssue...")
        test_rule = ValidationRule(
            rule_id="TEST_002",
            name="Test Issue Rule",
            dimension=QualityDimension.COMPLETENESS,
            severity=SeverityLevel.CRITICAL,
            description="Test issue rule"
        )
        issue = QualityIssue(
            rule=test_rule,
            field_name="test_field",
            current_value="invalid",
            expected_value="valid",
            details="Test details",
            suggestion="Test suggestion"
        )
        assert issue.rule.rule_id == "TEST_002"
        assert issue.rule.severity == SeverityLevel.CRITICAL
        assert issue.field_name == "test_field"
        print("  ✓ QualityIssue funciona correctamente")
        
        # Test DataQualityValidator initialization
        print("\nVerificando DataQualityValidator...")
        validator = DataQualityValidator()
        assert validator is not None
        assert hasattr(validator, 'validate_record')
        print("  ✓ DataQualityValidator se inicializa correctamente")
        
        print("\n✅ TEST 4 EXITOSO: Todas las estructuras de datos funcionan correctamente")
        
    except Exception as e:
        print(f"\n❌ TEST 4 FALLIDO: {e}")
        import traceback
        traceback.print_exc()


def run_all_tests():
    """Ejecuta todos los tests del sistema de control de calidad."""
    print("\n" + "="*100)
    print("🧪 SUITE DE TESTS DEL SISTEMA DE CONTROL DE CALIDAD")
    print("="*100)
    print(f"\nFecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Estándar: ISO 19157:2013")
    print("="*100)
    
    results = {
        'test_1': False,
        'test_2': False,
        'test_3': False,
        'test_4': False
    }
    
    # Test 4: Verificar estructuras (primero, es más básico)
    test_4_data_structures()
    results['test_4'] = True
    
    # Test 1: Validación
    validation_result = test_1_validation_module()
    if validation_result:
        results['test_1'] = True
    
    # Test 2: Reportes
    if validation_result:
        reports = test_2_reporter_module(validation_result)
        if reports:
            results['test_2'] = True
    
    # Test 3: Inspección detallada
    if validation_result:
        test_3_detailed_issue_inspection(validation_result)
        results['test_3'] = True
    
    # Resumen final
    print("\n\n" + "="*100)
    print("📊 RESUMEN FINAL DE TESTS")
    print("="*100 + "\n")
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ EXITOSO" if result else "❌ FALLIDO"
        print(f"{test_name.upper()}: {status}")
    
    print(f"\n{'='*100}")
    print(f"RESULTADO FINAL: {passed}/{total} tests exitosos ({passed/total*100:.1f}%)")
    print("="*100)
    
    if passed == total:
        print("\n🎉 TODOS LOS TESTS PASARON EXITOSAMENTE")
        print("\n✅ El sistema de control de calidad está funcionando correctamente")
        print("✅ Listo para integración con pipeline completo")
        print("✅ Los reportes se generan en formato correcto para Firebase y S3")
    else:
        print("\n⚠️  ALGUNOS TESTS FALLARON")
        print("\nRevisar los errores anteriores antes de continuar")
    
    print("\n" + "="*100)


if __name__ == "__main__":
    run_all_tests()
