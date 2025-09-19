"""
Orquestador principal para el sistema ETL de Cali Alcaldía
Este módulo coordina la ejecución de los procesos de extracción, transformación y carga
"""
import asyncio
import logging
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum
import json
import time

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent))

# Importar la nueva integración de base de datos
from orchestrator.database_integration import (
    OrchestratorDatabaseIntegration,
    check_database_before_etl,
    monitor_database_during_etl,
    generate_etl_database_report
)

class TaskStatus(Enum):
    """Estados posibles de una tarea"""
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class TaskResult:
    """Resultado de la ejecución de una tarea"""
    task_id: str
    status: TaskStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    output: Optional[str] = None
    error: Optional[str] = None
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convertir a diccionario para serialización"""
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": self.duration,
            "output": self.output,
            "error": self.error,
            "metadata": self.metadata
        }

@dataclass
class Task:
    """Definición de una tarea del ETL"""
    task_id: str
    name: str
    description: str
    module_path: str
    function_name: str
    dependencies: List[str] = field(default_factory=list)
    parameters: Dict = field(default_factory=dict)
    timeout: int = 3600  # 1 hora por defecto
    retry_count: int = 0
    critical: bool = True  # Si falla, detiene el proceso
    category: str = "general"  # extraction, transformation, load, utility
    
class ETLOrchestrator:
    """Orquestador principal del sistema ETL"""
    
    def __init__(self, log_level: str = "INFO"):
        self.tasks: Dict[str, Task] = {}
        self.results: Dict[str, TaskResult] = {}
        self.execution_order: List[str] = []
        self.logger = self._setup_logging(log_level)
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Integración de base de datos
        self.db_integration = OrchestratorDatabaseIntegration()
        self.execution_id: Optional[str] = None
        
    def _setup_logging(self, log_level: str) -> logging.Logger:
        """Configurar logging para el orquestador"""
        logger = logging.getLogger("ETL_Orchestrator")
        logger.setLevel(getattr(logging, log_level.upper()))
        
        if not logger.handlers:
            # Crear handler para consola
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            # Crear handler para archivo
            log_dir = Path("orchestrator_logs")
            log_dir.mkdir(exist_ok=True)
            
            file_handler = logging.FileHandler(
                log_dir / f"orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            )
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
            
        return logger
    
    def register_task(self, task: Task):
        """Registrar una nueva tarea"""
        self.tasks[task.task_id] = task
        self.logger.info(f"Tarea registrada: {task.task_id} - {task.name}")
    
    def register_tasks_from_config(self, config_path: str):
        """Registrar tareas desde un archivo de configuración JSON"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            for task_config in config.get('tasks', []):
                task = Task(**task_config)
                self.register_task(task)
                
            self.logger.info(f"Cargadas {len(config.get('tasks', []))} tareas desde {config_path}")
            
        except Exception as e:
            self.logger.error(f"Error cargando configuración desde {config_path}: {e}")
            raise
    
    def _calculate_execution_order(self) -> List[str]:
        """Calcular el orden de ejecución basado en dependencias"""
        visited = set()
        temp_visited = set()
        order = []
        
        def visit(task_id: str):
            if task_id in temp_visited:
                raise ValueError(f"Dependencia circular detectada en tarea: {task_id}")
            
            if task_id not in visited:
                temp_visited.add(task_id)
                
                task = self.tasks.get(task_id)
                if not task:
                    raise ValueError(f"Tarea no encontrada: {task_id}")
                
                for dep_id in task.dependencies:
                    visit(dep_id)
                
                temp_visited.remove(task_id)
                visited.add(task_id)
                order.append(task_id)
        
        for task_id in self.tasks.keys():
            if task_id not in visited:
                visit(task_id)
        
        return order
    
    def _import_and_execute_task(self, task: Task) -> TaskResult:
        """Importar módulo y ejecutar función de la tarea"""
        result = TaskResult(task_id=task.task_id, status=TaskStatus.RUNNING)
        result.start_time = datetime.now()
        
        try:
            # Importar módulo dinámicamente
            module_parts = task.module_path.split('.')
            module = __import__(task.module_path, fromlist=[module_parts[-1]])
            
            # Obtener función
            function = getattr(module, task.function_name)
            
            self.logger.info(f"Ejecutando {task.task_id}: {task.name}")
            
            # Ejecutar función con parámetros
            if task.parameters:
                output = function(**task.parameters)
            else:
                output = function()
            
            result.status = TaskStatus.COMPLETED
            result.output = str(output) if output else "Ejecutado exitosamente"
            
            self.logger.info(f"✅ Tarea completada: {task.task_id}")
            
        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            error_trace = traceback.format_exc()
            
            self.logger.error(f"❌ Error en tarea {task.task_id}: {e}")
            self.logger.debug(f"Trace completo: {error_trace}")
            
            result.metadata['error_trace'] = error_trace
        
        finally:
            result.end_time = datetime.now()
            if result.start_time:
                result.duration = (result.end_time - result.start_time).total_seconds()
        
        return result
    
    async def execute_all(self, 
                         parallel: bool = False,
                         max_workers: int = 3,
                         stop_on_failure: bool = True) -> Dict[str, TaskResult]:
        """Ejecutar todas las tareas registradas"""
        
        self.start_time = datetime.now()
        self.execution_id = f"etl_{self.start_time.strftime('%Y%m%d_%H%M%S')}"
        
        self.logger.info("=" * 60)
        self.logger.info("🚀 INICIANDO ORQUESTACIÓN ETL")
        self.logger.info("=" * 60)
        self.logger.info(f"📋 Execution ID: {self.execution_id}")
        
        try:
            # 🔍 VERIFICACIÓN PRE-ETL DE BASE DE DATOS
            self.logger.info("🔍 Verificando estado de la base de datos...")
            readiness_check = self.db_integration.check_database_readiness()
            
            if not readiness_check.get("ready", False):
                self.logger.warning("⚠️ Base de datos no está completamente lista")
                self.logger.info(f"   Connection: {'✅' if readiness_check.get('connection') else '❌'}")
                self.logger.info(f"   Schema: {'✅' if readiness_check.get('schema_complete') else '❌'}")
                
                if stop_on_failure:
                    raise RuntimeError("Base de datos no está lista para ETL. Verifique la configuración.")
                else:
                    self.logger.warning("⚠️ Continuando ETL a pesar de problemas de BD...")
            else:
                self.logger.info("✅ Base de datos lista para ETL")
            
            # 🚀 VERIFICACIONES PRE-ETL COMPLETAS
            self.logger.info("🚀 Ejecutando verificaciones pre-ETL...")
            pre_etl_check = self.db_integration.run_pre_etl_checks()
            
            if not pre_etl_check.get("can_proceed", False):
                self.logger.warning("⚠️ Verificaciones pre-ETL fallaron")
                if stop_on_failure:
                    raise RuntimeError("Verificaciones pre-ETL fallaron. ETL no puede proceder.")
            else:
                self.logger.info("✅ Verificaciones pre-ETL exitosas")
            
            # Calcular orden de ejecución
            self.execution_order = self._calculate_execution_order()
            self.logger.info(f"Orden de ejecución: {' -> '.join(self.execution_order)}")
            
            # 📊 MONITOREO DURANTE EJECUCIÓN
            if parallel:
                await self._execute_parallel(max_workers, stop_on_failure)
            else:
                await self._execute_sequential(stop_on_failure)
                
        except Exception as e:
            self.logger.error(f"Error crítico en orquestación: {e}")
            raise
        
        finally:
            self.end_time = datetime.now()
            
            # 📋 REPORTE POST-ETL
            try:
                self.logger.info("📋 Generando reporte post-ETL...")
                post_etl_report = self.db_integration.generate_post_etl_report(self.execution_id)
                self.logger.info(f"✅ Reporte post-ETL generado - Estado: {post_etl_report.get('overall_assessment', {}).get('overall_status', 'unknown').upper()}")
            except Exception as e:
                self.logger.warning(f"⚠️ Error generando reporte post-ETL: {e}")
            
            self._generate_execution_report()
        
        return self.results
    
    async def _execute_sequential(self, stop_on_failure: bool = True):
        """Ejecutar tareas secuencialmente"""
        executed_tasks = 0
        total_tasks = len(self.execution_order)
        
        for task_id in self.execution_order:
            task = self.tasks[task_id]
            
            # 📊 MONITOREO DE BASE DE DATOS CADA 3 TAREAS
            if executed_tasks > 0 and executed_tasks % 3 == 0:
                try:
                    self.logger.info(f"📊 Monitoreando base de datos (progreso: {executed_tasks}/{total_tasks})...")
                    monitoring_result = self.db_integration.monitor_etl_execution(self.execution_id)
                    health_status = monitoring_result.get('health_status', 'unknown')
                    alerts = monitoring_result.get('alerts', 0)
                    
                    if alerts > 0:
                        self.logger.warning(f"⚠️ {alerts} alertas detectadas durante ETL")
                    else:
                        self.logger.info(f"✅ Estado de BD: {health_status.upper()}")
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ Error en monitoreo de BD: {e}")
            
            # Verificar dependencias
            if not self._check_dependencies(task):
                result = TaskResult(task_id=task_id, status=TaskStatus.SKIPPED)
                result.error = "Dependencias fallaron"
                self.results[task_id] = result
                self.logger.warning(f"⏭️ Tarea omitida por dependencias: {task_id}")
                
                if task.critical and stop_on_failure:
                    self.logger.error("Deteniendo ejecución por falla crítica")
                    break
                continue
            
            # Ejecutar tarea
            result = self._import_and_execute_task(task)
            self.results[task_id] = result
            executed_tasks += 1
            
            # Verificar si continuar
            if result.status == TaskStatus.FAILED and task.critical and stop_on_failure:
                self.logger.error(f"Deteniendo ejecución por falla crítica en: {task_id}")
                break
    
    async def _execute_parallel(self, max_workers: int = 3, stop_on_failure: bool = True):
        """Ejecutar tareas en paralelo respetando dependencias"""
        semaphore = asyncio.Semaphore(max_workers)
        executing = set()
        completed = set()
        
        async def execute_task_async(task_id: str):
            async with semaphore:
                task = self.tasks[task_id]
                
                # Esperar dependencias
                while not all(dep in completed for dep in task.dependencies):
                    await asyncio.sleep(0.1)
                
                if not self._check_dependencies(task):
                    result = TaskResult(task_id=task_id, status=TaskStatus.SKIPPED)
                    result.error = "Dependencias fallaron"
                    self.results[task_id] = result
                    return
                
                # Ejecutar en thread pool para no bloquear async
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, self._import_and_execute_task, task
                )
                self.results[task_id] = result
                completed.add(task_id)
        
        # Crear tareas asyncio
        tasks_async = []
        for task_id in self.execution_order:
            task_async = asyncio.create_task(execute_task_async(task_id))
            tasks_async.append(task_async)
        
        # Ejecutar todas las tareas
        await asyncio.gather(*tasks_async, return_exceptions=True)
    
    def _check_dependencies(self, task: Task) -> bool:
        """Verificar que las dependencias se ejecutaron exitosamente"""
        for dep_id in task.dependencies:
            dep_result = self.results.get(dep_id)
            if not dep_result or dep_result.status != TaskStatus.COMPLETED:
                return False
        return True
    
    def _generate_execution_report(self):
        """Generar reporte de ejecución"""
        total_tasks = len(self.tasks)
        completed = sum(1 for r in self.results.values() if r.status == TaskStatus.COMPLETED)
        failed = sum(1 for r in self.results.values() if r.status == TaskStatus.FAILED)
        skipped = sum(1 for r in self.results.values() if r.status == TaskStatus.SKIPPED)
        
        total_duration = None
        if self.start_time and self.end_time:
            total_duration = (self.end_time - self.start_time).total_seconds()
        
        self.logger.info("=" * 60)
        self.logger.info("📊 REPORTE DE EJECUCIÓN")
        self.logger.info("=" * 60)
        self.logger.info(f"✅ Completadas: {completed}/{total_tasks}")
        self.logger.info(f"❌ Fallidas: {failed}")
        self.logger.info(f"⏭️ Omitidas: {skipped}")
        if total_duration:
            self.logger.info(f"⏱️ Duración total: {total_duration:.2f} segundos")
        
        # Guardar reporte detallado
        self._save_detailed_report()
    
    def _save_detailed_report(self):
        """Guardar reporte detallado en JSON"""
        report_dir = Path("orchestrator_reports")
        report_dir.mkdir(exist_ok=True)
        
        report = {
            "execution_id": datetime.now().strftime('%Y%m%d_%H%M%S'),
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "total_duration": (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else None,
            "execution_order": self.execution_order,
            "results": {task_id: result.to_dict() for task_id, result in self.results.items()}
        }
        
        report_file = report_dir / f"execution_report_{report['execution_id']}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"📄 Reporte guardado en: {report_file}")
    
    def get_task_status(self, task_id: str) -> Optional[TaskResult]:
        """Obtener estado de una tarea específica"""
        return self.results.get(task_id)
    
    def get_database_status(self) -> Dict[str, Any]:
        """
        Obtener estado actual de la base de datos
        
        Returns:
            Dictionary con estado de la base de datos
        """
        try:
            return self.db_integration.get_database_status_for_api()
        except Exception as e:
            self.logger.error(f"Error obteniendo estado de BD: {e}")
            return {
                "database": {
                    "connected": False,
                    "schema_complete": False,
                    "status": "error",
                    "error": str(e),
                    "last_check": datetime.now().isoformat()
                },
                "ready_for_etl": False
            }
    
    def check_etl_readiness(self) -> Dict[str, Any]:
        """
        Verificar si el sistema está listo para ejecutar ETL
        
        Returns:
            Dictionary con readiness check
        """
        try:
            return self.db_integration.check_database_readiness()
        except Exception as e:
            self.logger.error(f"Error verificando readiness: {e}")
            return {
                "ready": False,
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "details": {
                    "message": "Error checking ETL readiness"
                }
            }
    
    def list_tasks(self) -> List[Dict]:
        """Listar todas las tareas registradas"""
        return [
            {
                "task_id": task.task_id,
                "name": task.name,
                "category": task.category,
                "dependencies": task.dependencies,
                "critical": task.critical
            }
            for task in self.tasks.values()
        ]

# Función de conveniencia para crear orquestador preconfigurado
def create_orchestrator(config_file: str = None) -> ETLOrchestrator:
    """Crear orquestador con configuración opcional"""
    orchestrator = ETLOrchestrator()
    
    if config_file and Path(config_file).exists():
        orchestrator.register_tasks_from_config(config_file)
    
    return orchestrator

# Ejemplo de uso
if __name__ == "__main__":
    # Crear orquestador
    orchestrator = ETLOrchestrator()
    
    # Registrar tareas de ejemplo
    orchestrator.register_task(Task(
        task_id="extract_contratos",
        name="Extracción de Contratos",
        description="Extraer datos de contratos desde fuentes externas",
        module_path="extraction_app.data_extraction_contratos_emprestito",
        function_name="main",
        category="extraction"
    ))
    
    orchestrator.register_task(Task(
        task_id="transform_contratos",
        name="Transformación de Contratos",
        description="Transformar y limpiar datos de contratos",
        module_path="transformation_app.data_transformation_contratos_secop",
        function_name="main",
        dependencies=["extract_contratos"],
        category="transformation"
    ))
    
    # Ejecutar
    async def main():
        await orchestrator.execute_all()
    
    # Para testing local
    print("Orquestador ETL configurado correctamente")
    print("Tareas registradas:", len(orchestrator.tasks))