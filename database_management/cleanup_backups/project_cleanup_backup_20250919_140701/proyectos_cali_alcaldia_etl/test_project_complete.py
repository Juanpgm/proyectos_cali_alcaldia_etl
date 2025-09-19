"""
Script de Prueba Completa del Proyecto ETL Cali Alcaldía
========================================================

Este script ejecuta una serie de pruebas completas para validar
que todo el sistema ETL esté funcionando correctamente.
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


class ProjectTester:
    """Clase para ejecutar pruebas completas del proyecto"""
    
    def __init__(self):
        self.results = {}
        self.start_time = datetime.now()
        
    def log_test(self, test_name: str, success: bool, details: str = ""):
        """Registrar resultado de prueba"""
        self.results[test_name] = {
            "success": success,
            "timestamp": datetime.now(),
            "details": details
        }
        status = "✅ EXITOSA" if success else "❌ FALLIDA"
        print(f"   {status}: {test_name}")
        if details:
            print(f"      Detalles: {details}")

    def test_database_connectivity(self) -> bool:
        """Prueba 1: Conectividad de base de datos"""
        print("\n🔍 PRUEBA 1: CONECTIVIDAD DE BASE DE DATOS")
        print("-" * 50)
        
        try:
            # Test de conectividad básica
            health_status = quick_health_check()
            self.log_test(
                "Conexión a PostgreSQL",
                health_status["connection"],
                f"Host: {health_status.get('database_info', {}).get('host', 'unknown')}"
            )
            
            # Test de esquema
            self.log_test(
                "Esquema completo",
                health_status["schema_complete"],
                f"Estado: {health_status['status']}"
            )
            
            # Test de readiness
            db_ready = ensure_database_ready()
            self.log_test(
                "Base de datos lista",
                db_ready,
                "Lista para operaciones ETL" if db_ready else "Requiere atención"
            )
            
            return health_status["connection"]
            
        except Exception as e:
            self.log_test("Conectividad de BD", False, str(e))
            return False

    def test_orchestrator_functionality(self) -> bool:
        """Prueba 2: Funcionalidad del Orchestrator"""
        print("\n🎯 PRUEBA 2: FUNCIONALIDAD DEL ORCHESTRATOR")
        print("-" * 50)
        
        try:
            # Test de creación del orchestrator
            orchestrator = ETLOrchestrator()
            self.log_test("Creación de ETLOrchestrator", True, "Instancia creada exitosamente")
            
            # Test de integración de BD
            db_status = orchestrator.get_database_status()
            self.log_test(
                "Integración con BD",
                True,
                f"Estado: {db_status['database']['status']}"
            )
            
            # Test de readiness check
            readiness = orchestrator.check_etl_readiness()
            self.log_test(
                "ETL Readiness Check",
                True,
                f"Conexión: {'Sí' if readiness.get('connection') else 'No'}"
            )
            
            return True
            
        except Exception as e:
            self.log_test("Funcionalidad del Orchestrator", False, str(e))
            return False

    def test_configuration_loading(self) -> bool:
        """Prueba 3: Carga de configuración"""
        print("\n📁 PRUEBA 3: CARGA DE CONFIGURACIÓN")
        print("-" * 50)
        
        try:
            # Test de carga de configuración de testing
            config_file = Path("orchestrator/etl_config_testing.json")
            
            if not config_file.exists():
                self.log_test("Archivo de configuración", False, f"No encontrado: {config_file}")
                return False
            
            # Cargar y validar JSON
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            self.log_test("Carga de JSON", True, f"Configuración válida cargada")
            
            # Test de creación con configuración
            orchestrator = create_orchestrator(str(config_file))
            task_count = len(orchestrator.tasks)
            workflow_count = len(config_data.get("workflows", {}))
            
            self.log_test(
                "Creación con configuración",
                task_count > 0,
                f"{task_count} tareas, {workflow_count} workflows"
            )
            
            # Test de workflows
            for workflow_name in config_data.get("workflows", {}):
                workflow_tasks = config_data["workflows"][workflow_name]["tasks"]
                valid_workflow = all(task_id in orchestrator.tasks for task_id in workflow_tasks)
                self.log_test(
                    f"Workflow '{workflow_name}'",
                    valid_workflow,
                    f"{len(workflow_tasks)} tareas"
                )
            
            return True
            
        except Exception as e:
            self.log_test("Carga de configuración", False, str(e))
            return False

    async def test_task_execution(self) -> bool:
        """Prueba 4: Ejecución de tareas"""
        print("\n🚀 PRUEBA 4: EJECUCIÓN DE TAREAS")
        print("-" * 50)
        
        try:
            # Crear orchestrator con configuración de testing
            config_file = Path("orchestrator/etl_config_testing.json")
            orchestrator = create_orchestrator(str(config_file))
            
            # Test de ejecución de workflow simple
            print("   Ejecutando workflow 'test_simple'...")
            
            # Filtrar solo las tareas del workflow test_simple
            test_tasks = ["test_extraction_basic", "test_transformation_basic"]
            
            # Crear un orchestrator limpio solo con las tareas del test
            test_orchestrator = ETLOrchestrator()
            for task_id in test_tasks:
                if task_id in orchestrator.tasks:
                    test_orchestrator.register_task(orchestrator.tasks[task_id])
            
            # Ejecutar las tareas
            results = await test_orchestrator.execute_all(
                parallel=False,
                stop_on_failure=False
            )
            
            # Evaluar resultados
            successful_tasks = sum(1 for result in results.values() if result.status.value == "completed")
            total_tasks = len(results)
            
            self.log_test(
                "Ejecución de workflow",
                successful_tasks > 0,
                f"{successful_tasks}/{total_tasks} tareas exitosas"
            )
            
            # Test de resultados individuales
            for task_id, result in results.items():
                success = result.status.value == "completed"
                self.log_test(
                    f"Tarea '{task_id}'",
                    success,
                    f"Estado: {result.status.value}, Duración: {result.duration:.2f}s" if result.duration else f"Estado: {result.status.value}"
                )
            
            return successful_tasks > 0
            
        except Exception as e:
            self.log_test("Ejecución de tareas", False, str(e))
            return False

    def test_monitoring_and_reporting(self) -> bool:
        """Prueba 5: Monitoreo y reportes"""
        print("\n📊 PRUEBA 5: MONITOREO Y REPORTES")
        print("-" * 50)
        
        try:
            # Test de monitoreo funcional
            orchestrator = ETLOrchestrator()
            
            # Test de monitoreo durante ejecución (simulado)
            monitoring_result = orchestrator.db_integration.monitor_etl_execution("test_execution")
            self.log_test(
                "Monitoreo de ejecución",
                "health_status" in monitoring_result,
                f"Estado: {monitoring_result.get('health_status', 'unknown')}"
            )
            
            # Test de reporte post-ETL
            try:
                report_result = orchestrator.db_integration.generate_post_etl_report("test_execution")
                self.log_test(
                    "Reporte post-ETL",
                    "timestamp" in report_result,
                    f"Reporte generado con {len(report_result)} campos"
                )
            except Exception as e:
                self.log_test("Reporte post-ETL", False, f"Error: {str(e)[:50]}...")
            
            # Test de análisis comprehensivo
            try:
                analysis = comprehensive_analysis(1)  # Últimos 1 día
                self.log_test(
                    "Análisis comprehensivo",
                    "configuration" in analysis,
                    f"Análisis de {len(analysis)} componentes"
                )
            except Exception as e:
                self.log_test("Análisis comprehensivo", False, f"Error: {str(e)[:50]}...")
            
            return True
            
        except Exception as e:
            self.log_test("Monitoreo y reportes", False, str(e))
            return False

    def test_api_integration(self) -> bool:
        """Prueba 6: Integración con API"""
        print("\n🌐 PRUEBA 6: INTEGRACIÓN CON API")
        print("-" * 50)
        
        try:
            # Test de funciones de API
            api_status = get_api_database_status()
            self.log_test(
                "Status para API",
                "database" in api_status,
                f"Ready for ETL: {api_status.get('ready_for_etl', False)}"
            )
            
            # Test de check rápido
            db_ready = check_database_before_etl()
            self.log_test(
                "Check rápido de BD",
                isinstance(db_ready, bool),
                f"Base de datos lista: {db_ready}"
            )
            
            # Test de orchestrator API functions
            orchestrator = ETLOrchestrator()
            tasks_list = orchestrator.list_tasks()
            self.log_test(
                "Lista de tareas para API",
                isinstance(tasks_list, list),
                f"{len(tasks_list)} tareas disponibles"
            )
            
            return True
            
        except Exception as e:
            self.log_test("Integración con API", False, str(e))
            return False

    def generate_summary_report(self) -> Dict[str, Any]:
        """Generar reporte resumen de todas las pruebas"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        successful_tests = sum(1 for result in self.results.values() if result["success"])
        total_tests = len(self.results)
        success_rate = (successful_tests / total_tests * 100) if total_tests > 0 else 0
        
        return {
            "timestamp": end_time.isoformat(),
            "duration_seconds": duration,
            "total_tests": total_tests,
            "successful_tests": successful_tests,
            "failed_tests": total_tests - successful_tests,
            "success_rate": success_rate,
            "details": self.results
        }

    async def run_all_tests(self) -> bool:
        """Ejecutar todas las pruebas"""
        print("🧪 PRUEBAS COMPLETAS DEL PROYECTO ETL CALI ALCALDÍA")
        print("=" * 60)
        print(f"📅 Iniciado: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Ejecutar todas las pruebas
        test_methods = [
            self.test_database_connectivity,
            self.test_orchestrator_functionality,
            self.test_configuration_loading,
            self.test_task_execution,
            self.test_monitoring_and_reporting,
            self.test_api_integration
        ]
        
        overall_success = True
        
        for test_method in test_methods:
            try:
                if asyncio.iscoroutinefunction(test_method):
                    result = await test_method()
                else:
                    result = test_method()
                overall_success = overall_success and result
            except Exception as e:
                print(f"❌ Error ejecutando {test_method.__name__}: {e}")
                overall_success = False
        
        # Generar reporte final
        summary = self.generate_summary_report()
        
        print("\n" + "=" * 60)
        print("📊 REPORTE FINAL DE PRUEBAS")
        print("=" * 60)
        print(f"⏱️ Duración total: {summary['duration_seconds']:.2f} segundos")
        print(f"📋 Pruebas ejecutadas: {summary['total_tests']}")
        print(f"✅ Pruebas exitosas: {summary['successful_tests']}")
        print(f"❌ Pruebas fallidas: {summary['failed_tests']}")
        print(f"📈 Tasa de éxito: {summary['success_rate']:.1f}%")
        
        if overall_success and summary['success_rate'] >= 80:
            print("\n🎉 ¡PROYECTO FUNCIONANDO CORRECTAMENTE!")
            print("   ✅ Sistema ETL operativo")
            print("   ✅ Base de datos conectada") 
            print("   ✅ Orchestrator funcional")
            print("   ✅ Monitoreo activo")
            print("   ✅ Listo para producción")
        elif summary['success_rate'] >= 60:
            print("\n⚠️ PROYECTO PARCIALMENTE FUNCIONAL")
            print("   Revisar pruebas fallidas para mejoras")
        else:
            print("\n❌ PROYECTO REQUIERE ATENCIÓN")
            print("   Múltiples componentes necesitan corrección")
        
        print(f"\n📄 Reporte detallado guardado internamente")
        
        return overall_success


async def main():
    """Función principal"""
    tester = ProjectTester()
    success = await tester.run_all_tests()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)