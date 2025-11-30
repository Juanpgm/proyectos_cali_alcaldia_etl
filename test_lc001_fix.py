# Test para verificar que LC001 ya no marca error para Inaugurado
import sys
sys.path.insert(0, 'a:/programing_workspace/proyectos_cali_alcaldia_etl')
from utils.quality_control import DataQualityValidator

validator = DataQualityValidator()

test_records = [
    {'estado': 'Inaugurado', 'avance_obra': 100, 'nombre_centro_gestor': 'Test', 'upid': 'test-1', 'geometry': None},
    {'estado': 'Terminado', 'avance_obra': 100, 'nombre_centro_gestor': 'Test', 'upid': 'test-2', 'geometry': None},
    {'estado': 'Suspendido', 'avance_obra': 50, 'nombre_centro_gestor': 'Test', 'upid': 'test-3', 'geometry': None},
    {'estado': 'En ejecución', 'avance_obra': 50, 'nombre_centro_gestor': 'Test', 'upid': 'test-4', 'geometry': None},
    {'estado': 'En alistamiento', 'avance_obra': 0, 'nombre_centro_gestor': 'Test', 'upid': 'test-5', 'geometry': None},
]

print("=== TEST LC001 - Estado vs Avance ===\n")
for record in test_records:
    result = validator.validate_record(record, None)  # None para geometría
    estado = record['estado']
    avance = record['avance_obra']
    lc001_issues = [i for i in result if i.rule.rule_id == 'LC001']
    
    status = "✅ OK" if len(lc001_issues) == 0 else "❌ ERROR"
    print(f"{status} Estado='{estado}', Avance={avance}%")
    for issue in lc001_issues:
        print(f"     -> {issue.details}")
    print()

print("=== Casos que DEBEN dar error ===\n")
error_cases = [
    {'estado': 'Terminado', 'avance_obra': 50, 'nombre_centro_gestor': 'Test', 'upid': 'err-1', 'geometry': None},  # Terminado con avance < 100
    {'estado': 'En ejecución', 'avance_obra': 0, 'nombre_centro_gestor': 'Test', 'upid': 'err-2', 'geometry': None},  # En ejecución con avance 0
    {'estado': 'Inaugurado', 'avance_obra': 50, 'nombre_centro_gestor': 'Test', 'upid': 'err-3', 'geometry': None},  # Inaugurado con avance < 100
]

for record in error_cases:
    result = validator.validate_record(record, None)  # None para geometría
    estado = record['estado']
    avance = record['avance_obra']
    lc001_issues = [i for i in result if i.rule.rule_id == 'LC001']
    
    status = "✅ DETECTADO" if len(lc001_issues) > 0 else "❌ NO DETECTADO"
    print(f"{status} Estado='{estado}', Avance={avance}%")
    for issue in lc001_issues:
        print(f"     -> {issue.details}")
    print()
