"""
Orquestador ETL para el proyecto Cali Alcaldía
Sistema completo de orquestación de procesos ETL

Componentes principales:
- ETLOrchestrator: Clase principal del orquestador
- Task: Definición de tareas
- CLI: Interfaz de línea de comandos
- API: Servidor REST con FastAPI
"""

from .etl_orchestrator import (
    ETLOrchestrator,
    Task,
    TaskResult,
    TaskStatus,
    create_orchestrator
)

__version__ = "1.0.0"
__author__ = "Cali Alcaldía ETL Team"
__description__ = "Sistema de orquestación ETL para procesamiento de datos"

# Exportar elementos principales
__all__ = [
    "ETLOrchestrator",
    "Task", 
    "TaskResult",
    "TaskStatus",
    "create_orchestrator"
]