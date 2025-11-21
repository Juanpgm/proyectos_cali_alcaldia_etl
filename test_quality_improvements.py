# -*- coding: utf-8 -*-
"""
Script para probar las mejoras en el sistema de control de calidad:
1. Validación correcta de campo 'ano' con valores decimales (2024.0)
2. Detección de registros duplicados
3. Métricas mejoradas y más comprensibles
"""

import json
from utils.quality_control import validate_geojson, DataQualityValidator

def test_ano_validation():
    """Prueba la validación mejorada del campo 'ano'."""
    print("\n" + "="*80)
    print("PRUEBA 1: Validación de campo 'ano'")
    print("="*80)
    
    validator = DataQualityValidator()
    
    # Casos de prueba
    test_cases = [
        {"ano": "2024.0", "expected": "válido"},
        {"ano": 2024, "expected": "válido"},
        {"ano": 2024.0, "expected": "válido"},
        {"ano": "2024", "expected": "válido"},
        {"ano": "2019", "expected": "fuera de rango"},
        {"ano": "abc", "expected": "no numérico"},
        {"ano": None, "expected": "sin issues (campo opcional si no es requerido)"}
    ]
    
    for i, test in enumerate(test_cases, 1):
        record = {
            'upid': f'TEST-{i}',
            'nombre_up': f'Proyecto Test {i}',
            'estado': 'En alistamiento',
            'avance_obra': 0,
            'nombre_centro_gestor': 'Test',
            'comuna_corregimiento': 'Comuna 1',
            'tipo_intervencion': 'Obra nueva',
            'ano': test['ano']
        }
        
        issues = validator.validate_record(record)
        ano_issues = [issue for issue in issues if issue.field_name == 'ano']
        
        print(f"\nCaso {i}: ano = {test['ano']} ({type(test['ano']).__name__})")
        print(f"  Esperado: {test['expected']}")
        print(f"  Resultado: {len(ano_issues)} issue(s) detectado(s)")
        if ano_issues:
            for issue in ano_issues:
                print(f"    - {issue.details}")
        else:
            print(f"    ✅ Sin problemas detectados")


def create_test_geojson_with_duplicates():
    """Crea un GeoJSON de prueba con registros duplicados."""
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "upid": "UNP-1",
                    "nombre_up": "Proyecto A",
                    "estado": "En ejecución",
                    "avance_obra": 50,
                    "ano": "2024.0",
                    "nombre_centro_gestor": "Secretaría de Educación",
                    "comuna_corregimiento": "Comuna 1",
                    "tipo_intervencion": "Obra nueva",
                    "direccion": "Calle 1 # 2-3"
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-76.5, 3.4]
                }
            },
            {
                "type": "Feature",
                "properties": {
                    "upid": "UNP-2",  # Diferente UPID pero mismo contenido
                    "nombre_up": "Proyecto A",
                    "estado": "En ejecución",
                    "avance_obra": 50,
                    "ano": "2024.0",
                    "nombre_centro_gestor": "Secretaría de Educación",
                    "comuna_corregimiento": "Comuna 1",
                    "tipo_intervencion": "Obra nueva",
                    "direccion": "Calle 1 # 2-3"
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-76.5, 3.4]
                }
            },
            {
                "type": "Feature",
                "properties": {
                    "upid": "UNP-3",
                    "nombre_up": "Proyecto B",
                    "estado": "En alistamiento",
                    "avance_obra": 0,
                    "ano": "2025",
                    "nombre_centro_gestor": "Secretaría de Salud Pública",
                    "comuna_corregimiento": "Comuna 2",
                    "tipo_intervencion": "Mantenimiento",
                    "direccion": "Carrera 10 # 5-20"
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-76.52, 3.42]
                }
            }
        ]
    }
    
    # Guardar archivo temporal
    test_file = "test_quality_geojson_temp.json"
    with open(test_file, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2, ensure_ascii=False)
    
    return test_file


def test_duplicate_detection():
    """Prueba la detección de registros duplicados."""
    print("\n" + "="*80)
    print("PRUEBA 2: Detección de registros duplicados")
    print("="*80)
    
    # Crear GeoJSON de prueba con duplicados
    test_file = create_test_geojson_with_duplicates()
    
    # Validar
    result = validate_geojson(test_file, verbose=True)
    
    # Verificar resultados
    print("\n📊 RESULTADOS DE DETECCIÓN DE DUPLICADOS:")
    print(f"  Total de registros: {result['total_records']}")
    print(f"  Registros únicos: {result['unique_records']}")
    print(f"  Grupos de duplicados: {result['duplicate_groups']}")
    print(f"  Registros duplicados: {result['duplicate_records']}")
    
    if result['duplicate_details']:
        print(f"\n  Detalles de duplicados:")
        for i, group in enumerate(result['duplicate_details'], 1):
            print(f"    Grupo {i}: {len(group)} registros")
            for rec in group:
                print(f"      - Index: {rec['index']}, UPID: {rec['upid']}, Nombre: {rec['nombre_up']}")
    
    # Limpiar archivo temporal
    import os
    if os.path.exists(test_file):
        os.remove(test_file)
    
    print(f"\n✅ Archivo temporal eliminado: {test_file}")


def test_improved_metrics():
    """Prueba las métricas mejoradas."""
    print("\n" + "="*80)
    print("PRUEBA 3: Métricas mejoradas")
    print("="*80)
    
    # Crear GeoJSON de prueba
    test_file = create_test_geojson_with_duplicates()
    
    # Validar
    result = validate_geojson(test_file, verbose=False)
    
    stats = result['statistics']
    
    print("\n📊 MÉTRICAS MEJORADAS:")
    print(f"\n  Calidad General:")
    print(f"    Score: {stats['quality_score']:.2f}/100")
    print(f"    Rating: {stats['quality_rating']}")
    print(f"    Registros afectados: {stats['records_affected']} ({stats['records_affected_percentage']:.1f}%)")
    print(f"    Issues por registro: {stats['issues_per_record']:.2f}")
    
    print(f"\n  Issues Accionables:")
    print(f"    Critical: {stats['critical_issues']}")
    print(f"    High: {stats['high_issues']}")
    print(f"    Total accionables: {stats['actionable_issues']}")
    
    if stats['top_issues']:
        print(f"\n  Top 5 Problemas Más Frecuentes:")
        for rule_id, rule_info in list(stats['top_issues'].items())[:5]:
            print(f"    {rule_id} ({rule_info['severity']}): {rule_info['count']} ocurrencias")
            print(f"      {rule_info['name']}")
    
    print(f"\n  Campos con más problemas:")
    sorted_fields = sorted(stats['by_field'].items(), key=lambda x: x[1]['count'], reverse=True)
    for field, field_stats in sorted_fields[:5]:
        print(f"    {field}: {field_stats['count']} issues")
        print(f"      Reglas: {', '.join(field_stats['issues'][:3])}")
    
    # Limpiar archivo temporal
    import os
    if os.path.exists(test_file):
        os.remove(test_file)


if __name__ == "__main__":
    print("\n🧪 PRUEBAS DEL SISTEMA DE CONTROL DE CALIDAD MEJORADO")
    print("="*80)
    
    # Ejecutar pruebas
    test_ano_validation()
    test_duplicate_detection()
    test_improved_metrics()
    
    print("\n" + "="*80)
    print("✅ PRUEBAS COMPLETADAS")
    print("="*80)
