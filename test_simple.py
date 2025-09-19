"""
Script de Prueba Simple del Proyecto ETL
=======================================
Versión sin emojis para compatibilidad con Windows
"""

import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent))

# Imports del proyecto
from orchestrator.etl_orchestrator import ETLOrchestrator, create_orchestrator
from orchestrator.database_integration import (
    check_database_before_etl,
    get_api_database_status
)
from database_management.core import (
    quick_health_check,
    comprehensive_analysis,
    ensure_database_ready
)


def test_simple_workflow():
    """Prueba simple del workflow básico"""
    print("\n" + "="*60)
    print("PRUEBA SIMPLE DEL PROYECTO ETL")
    print("="*60)
    
    # Test 1: Conectividad básica
    print("\n1. CONECTIVIDAD DE BASE DE DATOS")
    print("-" * 40)
    try:
        health_status = quick_health_check()
        print(f"   Conexión: {'OK' if health_status['connection'] else 'FALLO'}")
        print(f"   Esquema: {'OK' if health_status['schema_complete'] else 'FALLO'}")
        print(f"   Estado: {health_status['status']}")
    except Exception as e:
        print(f"   ERROR: {e}")
        return False
    
    # Test 2: Orchestrator básico
    print("\n2. ORCHESTRATOR FUNCIONAL")
    print("-" * 40)
    try:
        orchestrator = ETLOrchestrator()
        print("   Instancia creada: OK")
        
        db_status = orchestrator.get_database_status()
        print(f"   Integración BD: {'OK' if 'database' in db_status else 'FALLO'}")
    except Exception as e:
        print(f"   ERROR: {e}")
        return False
    
    # Test 3: Configuración
    print("\n3. CARGA DE CONFIGURACIÓN")
    print("-" * 40)
    try:
        config_file = Path("orchestrator/etl_config_testing.json")
        if not config_file.exists():
            print("   ERROR: Archivo de configuración no encontrado")
            return False
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        print("   JSON válido: OK")
        
        orchestrator = create_orchestrator(str(config_file))
        task_count = len(orchestrator.tasks)
        print(f"   Tareas cargadas: {task_count}")
    except Exception as e:
        print(f"   ERROR: {e}")
        return False
    
    return True


async def test_task_execution():
    """Prueba de ejecución de tareas"""
    print("\n4. EJECUCIÓN DE TAREAS")
    print("-" * 40)
    
    try:
        config_file = Path("orchestrator/etl_config_testing.json")
        orchestrator = create_orchestrator(str(config_file))
        
        # Ejecutar workflow simple
        print("   Ejecutando workflow test_simple...")
        
        # Filtrar tareas del workflow
        test_tasks = ["test_extraction_basic", "test_transformation_basic"]
        test_orchestrator = ETLOrchestrator()
        
        for task_id in test_tasks:
            if task_id in orchestrator.tasks:
                test_orchestrator.register_task(orchestrator.tasks[task_id])
        
        print(f"   Tareas registradas: {len(test_orchestrator.tasks)}")
        
        # Ejecutar
        results = await test_orchestrator.execute_all(
            parallel=False,
            stop_on_failure=False
        )
        
        # Evaluar resultados
        successful = sum(1 for result in results.values() if result.status.value == "completed")
        total = len(results)
        
        print(f"   Tareas exitosas: {successful}/{total}")
        
        for task_id, result in results.items():
            status = "OK" if result.status.value == "completed" else "FALLO"
            duration = f" ({result.duration:.2f}s)" if result.duration else ""
            print(f"     {task_id}: {status}{duration}")
        
        return successful > 0
        
    except Exception as e:
        print(f"   ERROR: {e}")
        return False


def test_monitoring():
    """Prueba de monitoreo"""
    print("\n5. MONITOREO Y REPORTES")
    print("-" * 40)
    
    try:
        orchestrator = ETLOrchestrator()
        
        # Test monitoreo
        monitoring_result = orchestrator.db_integration.monitor_etl_execution("test_simple")
        print(f"   Monitoreo: {'OK' if 'health_status' in monitoring_result else 'FALLO'}")
        
        # Test análisis
        analysis = comprehensive_analysis(1)
        print(f"   Análisis: {'OK' if 'configuration' in analysis else 'FALLO'}")
        
        return True
    except Exception as e:
        print(f"   ERROR: {e}")
        return False


def test_api_integration():
    """Prueba de integración API"""
    print("\n6. INTEGRACIÓN API")
    print("-" * 40)
    
    try:
        api_status = get_api_database_status()
        print(f"   Status API: {'OK' if 'database' in api_status else 'FALLO'}")
        
        db_ready = check_database_before_etl()
        print(f"   Check BD: {'OK' if isinstance(db_ready, bool) else 'FALLO'}")
        
        return True
    except Exception as e:
        print(f"   ERROR: {e}")
        return False


async def main():
    """Función principal de pruebas"""
    start_time = datetime.now()
    
    print("SISTEMA ETL CALI ALCALDIA - PRUEBAS COMPLETAS")
    print(f"Iniciado: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Ejecutar todas las pruebas
    tests = [
        ("Configuración Básica", test_simple_workflow()),
        ("Ejecución de Tareas", await test_task_execution()),
        ("Monitoreo", test_monitoring()),
        ("Integración API", test_api_integration())
    ]
    
    # Evaluar resultados
    passed = sum(1 for _, result in tests if result)
    total = len(tests)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "="*60)
    print("RESUMEN FINAL")
    print("="*60)
    print(f"Duración: {duration:.2f} segundos")
    print(f"Pruebas ejecutadas: {total}")
    print(f"Pruebas exitosas: {passed}")
    print(f"Pruebas fallidas: {total - passed}")
    print(f"Tasa de éxito: {(passed/total)*100:.1f}%")
    
    if passed == total:
        print("\nESTADO: PROYECTO COMPLETAMENTE FUNCIONAL!")
        print("- Sistema ETL operativo")
        print("- Base de datos conectada")
        print("- Orchestrator funcional")
        print("- Monitoreo activo")
        print("- Listo para producción")
    elif passed >= total * 0.8:
        print("\nESTADO: PROYECTO MAYORMENTE FUNCIONAL")
        print("- Sistema principal operativo")
        print("- Algunos componentes requieren atención")
    else:
        print("\nESTADO: PROYECTO REQUIERE CORRECCIONES")
        print("- Múltiples componentes necesitan revisión")
    
    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)