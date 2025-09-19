"""
Funciones de prueba para testing del sistema ETL
===============================================
"""

import time
import random
from typing import Dict, Any, List
from pathlib import Path


def test_extraction_basic() -> Dict[str, Any]:
    """
    Función de extracción básica para testing
    """
    print("    Ejecutando extracción básica de prueba...")
    time.sleep(0.2)  # Simular trabajo
    
    # Simular datos extraídos
    data = {
        "records": [
            {"id": 1, "nombre": "Proyecto A", "valor": 100000},
            {"id": 2, "nombre": "Proyecto B", "valor": 200000},
            {"id": 3, "nombre": "Proyecto C", "valor": 150000}
        ],
        "metadata": {
            "total_records": 3,
            "extraction_time": time.time(),
            "source": "test_database"
        }
    }
    
    print(f"    ✓ Extraídos {len(data['records'])} registros de prueba")
    return data


def test_transformation_basic(input_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Función de transformación básica para testing
    """
    print("    Ejecutando transformación básica de prueba...")
    time.sleep(0.3)  # Simular trabajo de transformación
    
    if input_data is None:
        # Datos por defecto si no se reciben datos de entrada
        input_data = {
            "records": [{"id": 1, "nombre": "Default", "valor": 50000}],
            "metadata": {"total_records": 1}
        }
    
    # Simular transformación de datos
    transformed_records = []
    for record in input_data.get("records", []):
        transformed_record = {
            "project_id": record.get("id"),
            "project_name": record.get("nombre", "").upper(),
            "budget_amount": record.get("valor", 0),
            "status": "PROCESSED",
            "category": "TEST"
        }
        transformed_records.append(transformed_record)
    
    result = {
        "transformed_records": transformed_records,
        "transformation_metadata": {
            "original_count": len(input_data.get("records", [])),
            "transformed_count": len(transformed_records),
            "transformation_time": time.time(),
            "success_rate": 100.0
        }
    }
    
    print(f"    ✓ Transformados {len(transformed_records)} registros")
    return result


def test_load_basic(input_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Función de carga básica para testing
    """
    print("    Ejecutando carga básica de prueba...")
    time.sleep(0.4)  # Simular trabajo de carga
    
    if input_data is None:
        input_data = {"transformed_records": []}
    
    records_to_load = input_data.get("transformed_records", [])
    
    # Simular inserción en base de datos
    loaded_count = 0
    for record in records_to_load:
        # Simular validación y carga
        if record.get("project_id") and record.get("project_name"):
            loaded_count += 1
    
    result = {
        "load_summary": {
            "records_received": len(records_to_load),
            "records_loaded": loaded_count,
            "records_failed": len(records_to_load) - loaded_count,
            "load_time": time.time(),
            "target_table": "test_projects"
        },
        "success": loaded_count > 0
    }
    
    print(f"    ✓ Cargados {loaded_count} registros a la base de datos")
    return result


def test_validation_basic(input_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Función de validación básica para testing
    """
    print("    Ejecutando validación básica de prueba...")
    time.sleep(0.2)
    
    if input_data is None:
        input_data = {"load_summary": {"records_loaded": 0}}
    
    load_summary = input_data.get("load_summary", {})
    records_loaded = load_summary.get("records_loaded", 0)
    
    # Simular validaciones
    validations = {
        "data_integrity": records_loaded > 0,
        "format_validation": True,
        "business_rules": records_loaded >= 1,
        "completeness": records_loaded > 0
    }
    
    passed_validations = sum(1 for v in validations.values() if v)
    total_validations = len(validations)
    
    result = {
        "validation_results": validations,
        "summary": {
            "total_validations": total_validations,
            "passed_validations": passed_validations,
            "failed_validations": total_validations - passed_validations,
            "validation_score": (passed_validations / total_validations) * 100,
            "overall_status": "PASS" if passed_validations == total_validations else "FAIL"
        }
    }
    
    print(f"    ✓ Validaciones: {passed_validations}/{total_validations} exitosas")
    return result


def test_long_running(duration: int = 3) -> Dict[str, Any]:
    """
    Función de larga duración para testing de timeout y procesos largos
    """
    print(f"    Ejecutando proceso largo de prueba ({duration}s)...")
    start_time = time.time()
    
    # Simular trabajo de larga duración con progreso
    for i in range(duration):
        time.sleep(1)
        progress = ((i + 1) / duration) * 100
        print(f"      Progreso: {progress:.0f}%")
    
    end_time = time.time()
    actual_duration = end_time - start_time
    
    result = {
        "process_info": {
            "expected_duration": duration,
            "actual_duration": actual_duration,
            "start_time": start_time,
            "end_time": end_time,
            "status": "COMPLETED"
        },
        "data": {
            "iterations": duration,
            "average_iteration_time": actual_duration / duration
        }
    }
    
    print(f"    ✓ Proceso largo completado en {actual_duration:.2f}s")
    return result


def test_dependency_task(required_data: str = "default", input_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Función que demuestra manejo de dependencias
    """
    print(f"    Ejecutando tarea con dependencias (required_data: {required_data})...")
    time.sleep(0.3)
    
    if input_data is None:
        input_data = {"default": "no dependency data"}
    
    # Procesar basado en datos de dependencia
    dependency_info = {
        "required_parameter": required_data,
        "received_input": bool(input_data),
        "input_keys": list(input_data.keys()) if input_data else [],
        "processing_status": "SUCCESS"
    }
    
    result = {
        "dependency_analysis": dependency_info,
        "processed_data": {
            "combined_info": f"Processed with {required_data}",
            "input_summary": f"Received {len(input_data)} input fields" if input_data else "No input data"
        }
    }
    
    print("    ✓ Tarea con dependencias completada")
    return result


def mock_contratos_emprestito() -> Dict[str, Any]:
    """
    Mock de extracción de contratos de empréstito
    """
    print("    Simulando extracción de contratos de empréstito...")
    time.sleep(0.5)
    
    contracts = []
    for i in range(5):
        contract = {
            "contrato_id": f"CE-{1000 + i}",
            "entidad": f"Entidad {i + 1}",
            "valor_contrato": random.randint(50000, 500000),
            "fecha_inicio": "2024-01-01",
            "estado": random.choice(["ACTIVO", "TERMINADO", "EN_PROCESO"])
        }
        contracts.append(contract)
    
    result = {
        "contratos_emprestito": contracts,
        "extraction_metadata": {
            "total_contracts": len(contracts),
            "extraction_date": time.time(),
            "source_system": "SECOP_MOCK"
        }
    }
    
    print(f"    ✓ Extraídos {len(contracts)} contratos de empréstito (mock)")
    return result


def mock_procesos_emprestito() -> Dict[str, Any]:
    """
    Mock de extracción de procesos de empréstito
    """
    print("    Simulando extracción de procesos de empréstito...")
    time.sleep(0.4)
    
    processes = []
    for i in range(8):
        process = {
            "proceso_id": f"PE-{2000 + i}",
            "nombre_proceso": f"Proceso Empréstito {i + 1}",
            "valor_estimado": random.randint(100000, 1000000),
            "modalidad": random.choice(["LICITACION", "CONTRATACION_DIRECTA", "SUBASTA"]),
            "estado": random.choice(["ABIERTO", "CERRADO", "ADJUDICADO"])
        }
        processes.append(process)
    
    result = {
        "procesos_emprestito": processes,
        "extraction_metadata": {
            "total_processes": len(processes),
            "extraction_date": time.time(),
            "source_system": "PROCUREMENT_MOCK"
        }
    }
    
    print(f"    ✓ Extraídos {len(processes)} procesos de empréstito (mock)")
    return result


def mock_ejecucion_presupuestal() -> Dict[str, Any]:
    """
    Mock de extracción de ejecución presupuestal
    """
    print("    Simulando extracción de ejecución presupuestal...")
    time.sleep(0.6)
    
    budget_items = []
    for i in range(12):
        item = {
            "presupuesto_id": f"BP-{3000 + i}",
            "programa": f"Programa {i + 1}",
            "presupuesto_inicial": random.randint(1000000, 10000000),
            "ejecutado": random.randint(500000, 8000000),
            "porcentaje_ejecucion": random.randint(30, 95),
            "vigencia": "2024"
        }
        budget_items.append(item)
    
    result = {
        "ejecucion_presupuestal": budget_items,
        "extraction_metadata": {
            "total_items": len(budget_items),
            "extraction_date": time.time(),
            "source_system": "BUDGET_MOCK"
        }
    }
    
    print(f"    ✓ Extraídos {len(budget_items)} elementos presupuestales (mock)")
    return result


def mock_transform_contratos(input_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Mock de transformación de contratos SECOP
    """
    print("    Simulando transformación de contratos SECOP...")
    time.sleep(0.5)
    
    if input_data is None:
        input_data = {"contratos_emprestito": []}
    
    contracts = input_data.get("contratos_emprestito", [])
    transformed_contracts = []
    
    for contract in contracts:
        transformed = {
            "id": contract.get("contrato_id"),
            "entity_name": contract.get("entidad"),
            "contract_value": contract.get("valor_contrato"),
            "start_date": contract.get("fecha_inicio"),
            "status_code": contract.get("estado"),
            "contract_type": "EMPRESTITO",
            "transformed_at": time.time()
        }
        transformed_contracts.append(transformed)
    
    result = {
        "transformed_contracts": transformed_contracts,
        "transformation_summary": {
            "input_count": len(contracts),
            "output_count": len(transformed_contracts),
            "transformation_rate": 100.0,
            "transformation_time": time.time()
        }
    }
    
    print(f"    ✓ Transformados {len(transformed_contracts)} contratos")
    return result


def mock_load_complete(input_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Mock de carga completa de datos
    """
    print("    Simulando carga completa de datos...")
    time.sleep(0.7)
    
    if input_data is None:
        input_data = {"transformed_contracts": []}
    
    contracts = input_data.get("transformed_contracts", [])
    
    # Simular inserción en múltiples tablas
    tables_loaded = {
        "contratos_secop": len(contracts),
        "auditoria_contratos": len(contracts),
        "estadisticas_contratos": 1 if contracts else 0
    }
    
    total_records = sum(tables_loaded.values())
    
    result = {
        "load_summary": {
            "tables_loaded": tables_loaded,
            "total_records_loaded": total_records,
            "load_time": time.time(),
            "database": "dev_etl_testing",
            "status": "SUCCESS" if total_records > 0 else "NO_DATA"
        },
        "success": total_records > 0
    }
    
    print(f"    ✓ Cargados {total_records} registros en {len(tables_loaded)} tablas")
    return result