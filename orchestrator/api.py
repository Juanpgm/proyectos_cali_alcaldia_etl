"""
API Web para el Orquestador ETL
Endpoints REST para ejecutar y monitorear tareas ETL
"""
import asyncio
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import sys

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent))

from orchestrator.etl_orchestrator import ETLOrchestrator, create_orchestrator, TaskStatus

app = FastAPI(
    title="Cali Alcaldía ETL Orchestrator API",
    description="API REST para orquestar procesos ETL",
    version="1.0.0"
)

# Estado global
active_orchestrators: Dict[str, ETLOrchestrator] = {}
execution_status: Dict[str, Dict] = {}

class ExecutionRequest(BaseModel):
    """Request para ejecutar tareas"""
    workflow: Optional[str] = None
    tasks: Optional[List[str]] = None
    parallel: bool = False
    max_workers: int = 3
    stop_on_failure: bool = True
    config_file: Optional[str] = None

class TaskStatusResponse(BaseModel):
    """Response del estado de una tarea"""
    task_id: str
    status: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration: Optional[float] = None
    output: Optional[str] = None
    error: Optional[str] = None

class ExecutionStatusResponse(BaseModel):
    """Response del estado de ejecución"""
    execution_id: str
    status: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    total_tasks: int
    completed: int
    failed: int
    skipped: int
    running: int
    tasks: List[TaskStatusResponse]

async def execute_orchestrator_background(
    execution_id: str,
    orchestrator: ETLOrchestrator,
    **kwargs
):
    """Ejecutar orquestador en background"""
    try:
        execution_status[execution_id] = {
            'status': 'running',
            'start_time': datetime.now().isoformat(),
            'orchestrator': orchestrator
        }
        
        results = await orchestrator.execute_all(**kwargs)
        
        execution_status[execution_id].update({
            'status': 'completed',
            'end_time': datetime.now().isoformat(),
            'results': results
        })
        
    except Exception as e:
        execution_status[execution_id].update({
            'status': 'failed',
            'end_time': datetime.now().isoformat(),
            'error': str(e)
        })

@app.get("/")
async def root():
    """Endpoint raíz con información básica"""
    return {
        "service": "Cali Alcaldía ETL Orchestrator",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "tasks": "/tasks",
            "workflows": "/workflows", 
            "execute": "/execute",
            "status": "/executions/{execution_id}",
            "health": "/health"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_executions": len([e for e in execution_status.values() if e.get('status') == 'running'])
    }

@app.get("/tasks")
async def list_tasks(category: Optional[str] = None):
    """Listar tareas disponibles"""
    try:
        config_file = str(Path(__file__).parent / "etl_config.json")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        tasks = config.get('tasks', [])
        
        if category:
            tasks = [t for t in tasks if t.get('category') == category]
        
        return {
            "tasks": tasks,
            "total": len(tasks),
            "categories": list(set(t.get('category', 'general') for t in config.get('tasks', [])))
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/workflows")
async def list_workflows():
    """Listar workflows disponibles"""
    try:
        config_file = str(Path(__file__).parent / "etl_config.json")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        workflows = config.get('workflows', {})
        
        return {
            "workflows": workflows,
            "total": len(workflows)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/execute")
async def execute_tasks(request: ExecutionRequest, background_tasks: BackgroundTasks):
    """Ejecutar tareas o workflow"""
    try:
        execution_id = f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        # Crear orquestador
        config_file = request.config_file or str(Path(__file__).parent / "etl_config.json")
        orchestrator = create_orchestrator(config_file)
        
        active_orchestrators[execution_id] = orchestrator
        
        # Preparar kwargs para ejecución
        exec_kwargs = {
            'parallel': request.parallel,
            'max_workers': request.max_workers,
            'stop_on_failure': request.stop_on_failure
        }
        
        if request.workflow:
            # Ejecutar workflow específico
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            workflows = config.get('workflows', {})
            if request.workflow not in workflows:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Workflow '{request.workflow}' no encontrado"
                )
            
            workflow = workflows[request.workflow]
            tasks_to_run = workflow['tasks']
            
            if tasks_to_run != ["all"]:
                # Filtrar tareas del workflow
                temp_orchestrator = ETLOrchestrator()
                for task_id in tasks_to_run:
                    if task_id in orchestrator.tasks:
                        temp_orchestrator.register_task(orchestrator.tasks[task_id])
                orchestrator = temp_orchestrator
                active_orchestrators[execution_id] = orchestrator
        
        elif request.tasks:
            # Ejecutar tareas específicas
            temp_orchestrator = ETLOrchestrator()
            for task_id in request.tasks:
                if task_id not in orchestrator.tasks:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Tarea '{task_id}' no encontrada"
                    )
                temp_orchestrator.register_task(orchestrator.tasks[task_id])
            orchestrator = temp_orchestrator
            active_orchestrators[execution_id] = orchestrator
        
        # Ejecutar en background
        background_tasks.add_task(
            execute_orchestrator_background,
            execution_id,
            orchestrator,
            **exec_kwargs
        )
        
        return {
            "execution_id": execution_id,
            "status": "started",
            "message": "Ejecución iniciada en background",
            "tasks_count": len(orchestrator.tasks),
            "monitor_url": f"/executions/{execution_id}"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/executions/{execution_id}")
async def get_execution_status(execution_id: str) -> ExecutionStatusResponse:
    """Obtener estado de una ejecución específica"""
    if execution_id not in execution_status:
        raise HTTPException(status_code=404, detail="Ejecución no encontrada")
    
    exec_data = execution_status[execution_id]
    orchestrator = exec_data.get('orchestrator')
    
    if not orchestrator:
        raise HTTPException(status_code=500, detail="Orquestador no disponible")
    
    # Calcular estadísticas
    results = orchestrator.results
    completed = sum(1 for r in results.values() if r.status == TaskStatus.COMPLETED)
    failed = sum(1 for r in results.values() if r.status == TaskStatus.FAILED)
    skipped = sum(1 for r in results.values() if r.status == TaskStatus.SKIPPED)
    running = sum(1 for r in results.values() if r.status == TaskStatus.RUNNING)
    
    # Convertir tareas a response format
    tasks_response = []
    for task_id, result in results.items():
        tasks_response.append(TaskStatusResponse(
            task_id=task_id,
            status=result.status.value,
            start_time=result.start_time.isoformat() if result.start_time else None,
            end_time=result.end_time.isoformat() if result.end_time else None,
            duration=result.duration,
            output=result.output,
            error=result.error
        ))
    
    return ExecutionStatusResponse(
        execution_id=execution_id,
        status=exec_data.get('status', 'unknown'),
        start_time=exec_data.get('start_time'),
        end_time=exec_data.get('end_time'),
        total_tasks=len(orchestrator.tasks),
        completed=completed,
        failed=failed,
        skipped=skipped,
        running=running,
        tasks=tasks_response
    )

@app.get("/executions")
async def list_executions():
    """Listar todas las ejecuciones"""
    executions = []
    
    for exec_id, exec_data in execution_status.items():
        orchestrator = exec_data.get('orchestrator')
        
        if orchestrator:
            results = orchestrator.results
            completed = sum(1 for r in results.values() if r.status == TaskStatus.COMPLETED)
            failed = sum(1 for r in results.values() if r.status == TaskStatus.FAILED)
            total = len(orchestrator.tasks)
        else:
            completed = failed = total = 0
        
        executions.append({
            "execution_id": exec_id,
            "status": exec_data.get('status', 'unknown'),
            "start_time": exec_data.get('start_time'),
            "end_time": exec_data.get('end_time'),
            "total_tasks": total,
            "completed": completed,
            "failed": failed
        })
    
    return {
        "executions": sorted(executions, key=lambda x: x.get('start_time', ''), reverse=True),
        "total": len(executions)
    }

@app.delete("/executions/{execution_id}")
async def cancel_execution(execution_id: str):
    """Cancelar una ejecución (limpia el estado)"""
    if execution_id not in execution_status:
        raise HTTPException(status_code=404, detail="Ejecución no encontrada")
    
    # Limpiar referencias
    execution_status.pop(execution_id, None)
    active_orchestrators.pop(execution_id, None)
    
    return {"message": f"Ejecución {execution_id} cancelada/limpiada"}

@app.get("/reports")
async def list_reports():
    """Listar reportes de ejecución disponibles"""
    reports_dir = Path("orchestrator_reports")
    
    if not reports_dir.exists():
        return {"reports": [], "total": 0}
    
    report_files = list(reports_dir.glob("execution_report_*.json"))
    reports = []
    
    for report_file in sorted(report_files, key=lambda x: x.stat().st_mtime, reverse=True):
        try:
            with open(report_file) as f:
                data = json.load(f)
            
            reports.append({
                "execution_id": data.get('execution_id'),
                "start_time": data.get('start_time'),
                "end_time": data.get('end_time'),
                "duration": data.get('total_duration'),
                "file_path": str(report_file)
            })
        except:
            continue
    
    return {
        "reports": reports,
        "total": len(reports)
    }

@app.get("/reports/{execution_id}")
async def get_report(execution_id: str):
    """Obtener reporte específico"""
    reports_dir = Path("orchestrator_reports")
    report_file = reports_dir / f"execution_report_{execution_id}.json"
    
    if not report_file.exists():
        raise HTTPException(status_code=404, detail="Reporte no encontrado")
    
    with open(report_file) as f:
        return json.load(f)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)