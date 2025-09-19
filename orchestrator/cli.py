"""
CLI para el Orquestador ETL de Cali Alcaldía
Interfaz de línea de comandos para ejecutar workflows y tareas individuales
"""
import asyncio
import argparse
import sys
import json
from pathlib import Path
from typing import List, Optional
import logging

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent))

from orchestrator.etl_orchestrator import ETLOrchestrator, create_orchestrator

class ETLCommandLineInterface:
    """CLI para el sistema ETL"""
    
    def __init__(self):
        self.orchestrator: Optional[ETLOrchestrator] = None
        self.config_file = Path(__file__).parent / "etl_config.json"
    
    def setup_parser(self) -> argparse.ArgumentParser:
        """Configurar el parser de argumentos"""
        parser = argparse.ArgumentParser(
            description="Orquestador ETL para Cali Alcaldía",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Ejemplos de uso:
  %(prog)s run --workflow full_etl
  %(prog)s run --workflow extraction_only
  %(prog)s run --tasks extract_contratos_emprestito transform_contratos_secop
  %(prog)s list-tasks
  %(prog)s list-workflows
  %(prog)s status --execution-id 20240918_143022
            """
        )
        
        subparsers = parser.add_subparsers(dest='command', help='Comandos disponibles')
        
        # Comando run
        run_parser = subparsers.add_parser('run', help='Ejecutar tareas o workflows')
        run_group = run_parser.add_mutually_exclusive_group(required=True)
        run_group.add_argument('--workflow', '-w', 
                              help='Ejecutar workflow predefinido (full_etl, extraction_only, etc.)')
        run_group.add_argument('--tasks', '-t', nargs='+',
                              help='Ejecutar tareas específicas por ID')
        run_group.add_argument('--all', action='store_true',
                              help='Ejecutar todas las tareas registradas')
        
        run_parser.add_argument('--parallel', '-p', action='store_true',
                               help='Ejecutar en paralelo (respetando dependencias)')
        run_parser.add_argument('--max-workers', type=int, default=3,
                               help='Número máximo de workers paralelos (default: 3)')
        run_parser.add_argument('--continue-on-failure', action='store_true',
                               help='Continuar ejecución aunque fallen tareas no críticas')
        run_parser.add_argument('--config', '-c',
                               help='Archivo de configuración personalizado')
        run_parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                               default='INFO', help='Nivel de logging')
        
        # Comando list-tasks
        list_tasks_parser = subparsers.add_parser('list-tasks', help='Listar tareas disponibles')
        list_tasks_parser.add_argument('--category', '-c',
                                      help='Filtrar por categoría (extraction, transformation, load, etc.)')
        list_tasks_parser.add_argument('--config',
                                      help='Archivo de configuración personalizado')
        
        # Comando list-workflows
        list_workflows_parser = subparsers.add_parser('list-workflows', help='Listar workflows disponibles')
        list_workflows_parser.add_argument('--config',
                                          help='Archivo de configuración personalizado')
        
        # Comando status
        status_parser = subparsers.add_parser('status', help='Ver estado de ejecución')
        status_parser.add_argument('--execution-id', '-e',
                                  help='ID de ejecución específica')
        status_parser.add_argument('--latest', action='store_true',
                                  help='Mostrar última ejecución')
        
        # Comando validate
        validate_parser = subparsers.add_parser('validate', help='Validar configuración')
        validate_parser.add_argument('--config',
                                    help='Archivo de configuración a validar')
        
        return parser
    
    def load_config(self, config_file: Optional[str] = None) -> dict:
        """Cargar configuración desde archivo JSON"""
        config_path = Path(config_file) if config_file else self.config_file
        
        if not config_path.exists():
            raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def setup_orchestrator(self, config_file: Optional[str] = None, log_level: str = "INFO"):
        """Configurar orquestador con archivo de configuración"""
        config_path = config_file if config_file else str(self.config_file)
        self.orchestrator = create_orchestrator(config_path)
        self.orchestrator.logger.setLevel(getattr(logging, log_level.upper()))
    
    async def run_workflow(self, workflow_name: str, config: dict, **kwargs):
        """Ejecutar un workflow predefinido"""
        workflows = config.get('workflows', {})
        
        if workflow_name not in workflows:
            available = ', '.join(workflows.keys())
            raise ValueError(f"Workflow '{workflow_name}' no encontrado. Disponibles: {available}")
        
        workflow = workflows[workflow_name]
        tasks_to_run = workflow['tasks']
        
        print(f"🚀 Ejecutando workflow: {workflow['name']}")
        print(f"📝 Descripción: {workflow['description']}")
        
        if tasks_to_run == ["all"]:
            # Ejecutar todas las tareas
            await self.orchestrator.execute_all(**kwargs)
        else:
            # Ejecutar tareas específicas
            await self.run_specific_tasks(tasks_to_run, **kwargs)
    
    async def run_specific_tasks(self, task_ids: List[str], **kwargs):
        """Ejecutar tareas específicas"""
        # Filtrar tareas registradas
        available_tasks = set(self.orchestrator.tasks.keys())
        requested_tasks = set(task_ids)
        
        # Verificar que todas las tareas existan
        missing_tasks = requested_tasks - available_tasks
        if missing_tasks:
            raise ValueError(f"Tareas no encontradas: {', '.join(missing_tasks)}")
        
        # Crear orquestador temporal solo con las tareas solicitadas
        temp_orchestrator = ETLOrchestrator(log_level=self.orchestrator.logger.level)
        
        for task_id in task_ids:
            temp_orchestrator.register_task(self.orchestrator.tasks[task_id])
        
        print(f"🎯 Ejecutando {len(task_ids)} tareas específicas:")
        for task_id in task_ids:
            task = self.orchestrator.tasks[task_id]
            print(f"  - {task_id}: {task.name}")
        
        await temp_orchestrator.execute_all(**kwargs)
    
    def list_tasks(self, category: Optional[str] = None, config_file: Optional[str] = None):
        """Listar tareas disponibles"""
        config = self.load_config(config_file)
        tasks = config.get('tasks', [])
        
        if category:
            tasks = [t for t in tasks if t.get('category') == category]
        
        print(f"📋 Tareas disponibles ({len(tasks)} total):")
        print()
        
        # Agrupar por categoría
        by_category = {}
        for task in tasks:
            cat = task.get('category', 'general')
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(task)
        
        for cat, cat_tasks in sorted(by_category.items()):
            print(f"🏷️  {cat.upper()}:")
            for task in sorted(cat_tasks, key=lambda x: x['task_id']):
                critical_marker = " ⚠️ " if task.get('critical', False) else "   "
                deps = task.get('dependencies', [])
                deps_str = f" (deps: {', '.join(deps)})" if deps else ""
                print(f"  {critical_marker}{task['task_id']}: {task['name']}{deps_str}")
            print()
    
    def list_workflows(self, config_file: Optional[str] = None):
        """Listar workflows disponibles"""
        config = self.load_config(config_file)
        workflows = config.get('workflows', {})
        
        print(f"🔄 Workflows disponibles ({len(workflows)} total):")
        print()
        
        for name, workflow in workflows.items():
            tasks_count = len(workflow['tasks']) if workflow['tasks'] != ["all"] else "todas"
            print(f"  📦 {name}")
            print(f"     {workflow['description']}")
            print(f"     Tareas: {tasks_count}")
            print()
    
    def show_status(self, execution_id: Optional[str] = None, latest: bool = False):
        """Mostrar estado de ejecución"""
        reports_dir = Path("orchestrator_reports")
        
        if not reports_dir.exists():
            print("❌ No hay reportes de ejecución disponibles")
            return
        
        report_files = list(reports_dir.glob("execution_report_*.json"))
        
        if not report_files:
            print("❌ No hay reportes de ejecución disponibles")
            return
        
        if latest:
            report_file = max(report_files, key=lambda x: x.stat().st_mtime)
        elif execution_id:
            report_file = reports_dir / f"execution_report_{execution_id}.json"
            if not report_file.exists():
                print(f"❌ Reporte no encontrado: {execution_id}")
                return
        else:
            print("📊 Reportes disponibles:")
            for rf in sorted(report_files, key=lambda x: x.stat().st_mtime, reverse=True):
                exec_id = rf.stem.replace("execution_report_", "")
                with open(rf) as f:
                    data = json.load(f)
                print(f"  🕐 {exec_id} - {data.get('start_time', 'N/A')}")
            return
        
        # Mostrar reporte específico
        with open(report_file) as f:
            report = json.load(f)
        
        print(f"📊 Reporte de Ejecución: {report['execution_id']}")
        print(f"🕐 Inicio: {report.get('start_time', 'N/A')}")
        print(f"🏁 Fin: {report.get('end_time', 'N/A')}")
        print(f"⏱️ Duración: {report.get('total_duration', 0):.2f} segundos")
        print()
        
        results = report.get('results', {})
        completed = sum(1 for r in results.values() if r['status'] == 'completed')
        failed = sum(1 for r in results.values() if r['status'] == 'failed')
        skipped = sum(1 for r in results.values() if r['status'] == 'skipped')
        
        print(f"✅ Completadas: {completed}")
        print(f"❌ Fallidas: {failed}")
        print(f"⏭️ Omitidas: {skipped}")
        print()
        
        if failed > 0:
            print("❌ Tareas fallidas:")
            for task_id, result in results.items():
                if result['status'] == 'failed':
                    print(f"  - {task_id}: {result.get('error', 'Error desconocido')}")
    
    def validate_config(self, config_file: Optional[str] = None):
        """Validar archivo de configuración"""
        try:
            config = self.load_config(config_file)
            tasks = config.get('tasks', [])
            workflows = config.get('workflows', {})
            
            print(f"✅ Configuración válida")
            print(f"📋 Tareas: {len(tasks)}")
            print(f"🔄 Workflows: {len(workflows)}")
            
            # Validar dependencias
            task_ids = {task['task_id'] for task in tasks}
            for task in tasks:
                for dep in task.get('dependencies', []):
                    if dep not in task_ids:
                        print(f"⚠️ Dependencia no encontrada: {task['task_id']} -> {dep}")
            
            print("✅ Validación completada")
            
        except Exception as e:
            print(f"❌ Error en configuración: {e}")
            sys.exit(1)
    
    async def main(self):
        """Función principal del CLI"""
        parser = self.setup_parser()
        args = parser.parse_args()
        
        if not args.command:
            parser.print_help()
            return
        
        try:
            if args.command == 'run':
                config_file = getattr(args, 'config', None)
                self.setup_orchestrator(config_file, args.log_level)
                config = self.load_config(config_file)
                
                run_kwargs = {
                    'parallel': args.parallel,
                    'max_workers': args.max_workers,
                    'stop_on_failure': not args.continue_on_failure
                }
                
                if args.workflow:
                    await self.run_workflow(args.workflow, config, **run_kwargs)
                elif args.tasks:
                    await self.run_specific_tasks(args.tasks, **run_kwargs)
                elif args.all:
                    await self.orchestrator.execute_all(**run_kwargs)
            
            elif args.command == 'list-tasks':
                self.list_tasks(getattr(args, 'category', None), getattr(args, 'config', None))
            
            elif args.command == 'list-workflows':
                self.list_workflows(getattr(args, 'config', None))
            
            elif args.command == 'status':
                self.show_status(getattr(args, 'execution_id', None), getattr(args, 'latest', False))
            
            elif args.command == 'validate':
                self.validate_config(getattr(args, 'config', None))
        
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)

def main():
    """Punto de entrada del CLI"""
    cli = ETLCommandLineInterface()
    asyncio.run(cli.main())

if __name__ == "__main__":
    main()