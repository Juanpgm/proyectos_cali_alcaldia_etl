"""
Ejemplo de uso del Orquestador ETL
Demostraciones de diferentes formas de usar el sistema de orquestación
"""
import asyncio
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent))

from orchestrator.etl_orchestrator import ETLOrchestrator, Task, create_orchestrator

async def ejemplo_basico():
    """Ejemplo básico de uso del orquestador"""
    print("=" * 60)
    print("🔧 EJEMPLO BÁSICO - Creando orquestador manual")
    print("=" * 60)
    
    # Crear orquestador
    orchestrator = ETLOrchestrator()
    
    # Registrar algunas tareas de ejemplo
    orchestrator.register_task(Task(
        task_id="test_extraction",
        name="Extracción de Prueba",
        description="Tarea de extracción para testing",
        module_path="builtins",
        function_name="len",
        category="extraction"
    ))
    
    orchestrator.register_task(Task(
        task_id="test_transformation",
        name="Transformación de Prueba", 
        description="Tarea de transformación para testing",
        module_path="builtins",
        function_name="str",
        dependencies=["test_extraction"],
        category="transformation"
    ))
    
    # Listar tareas registradas
    print("📋 Tareas registradas:")
    for task in orchestrator.list_tasks():
        print(f"  - {task['task_id']}: {task['name']} ({task['category']})")
    
    print("\n✅ Orquestador básico configurado correctamente")

async def ejemplo_con_configuracion():
    """Ejemplo usando archivo de configuración"""
    print("\n" + "=" * 60)
    print("📁 EJEMPLO CON CONFIGURACIÓN - Cargando desde JSON")
    print("=" * 60)
    
    config_file = Path(__file__).parent / "etl_config.json"
    
    if not config_file.exists():
        print(f"❌ Archivo de configuración no encontrado: {config_file}")
        return
    
    # Crear orquestador con configuración
    orchestrator = create_orchestrator(str(config_file))
    
    print(f"📊 Estadísticas:")
    print(f"  - Tareas registradas: {len(orchestrator.tasks)}")
    
    # Mostrar tareas por categoría
    tasks_by_category = {}
    for task in orchestrator.tasks.values():
        category = task.category
        if category not in tasks_by_category:
            tasks_by_category[category] = []
        tasks_by_category[category].append(task)
    
    for category, tasks in tasks_by_category.items():
        print(f"  - {category}: {len(tasks)} tareas")
    
    print("\n✅ Configuración cargada correctamente")

async def ejemplo_ejecucion_simulada():
    """Ejemplo simulando ejecución de tareas"""
    print("\n" + "=" * 60)
    print("🎯 EJEMPLO SIMULACIÓN - Mostrando flujo de ejecución")
    print("=" * 60)
    
    config_file = Path(__file__).parent / "etl_config.json"
    
    if not config_file.exists():
        print(f"❌ Archivo de configuración no encontrado: {config_file}")
        return
    
    orchestrator = create_orchestrator(str(config_file))
    
    # Calcular orden de ejecución
    try:
        order = orchestrator._calculate_execution_order()
        print(f"📈 Orden de ejecución calculado:")
        print(f"  Total de tareas: {len(order)}")
        print(f"  Secuencia: {' → '.join(order[:5])}{'...' if len(order) > 5 else ''}")
        
        # Mostrar dependencias
        print(f"\n🔗 Análisis de dependencias:")
        for task_id in order[:10]:  # Primeras 10 tareas
            task = orchestrator.tasks[task_id]
            if task.dependencies:
                print(f"  - {task_id} depende de: {', '.join(task.dependencies)}")
            else:
                print(f"  - {task_id} (sin dependencias)")
        
        print("\n✅ Análisis de flujo completado")
        
    except Exception as e:
        print(f"❌ Error en análisis: {e}")

def mostrar_cli_examples():
    """Mostrar ejemplos de uso del CLI"""
    print("\n" + "=" * 60)
    print("💻 EJEMPLOS DE USO DEL CLI")
    print("=" * 60)
    
    examples = [
        ("Listar todas las tareas", "python orchestrator/cli.py list-tasks"),
        ("Listar tareas de extracción", "python orchestrator/cli.py list-tasks --category extraction"),
        ("Listar workflows", "python orchestrator/cli.py list-workflows"),
        ("Ejecutar workflow completo", "python orchestrator/cli.py run --workflow full_etl"),
        ("Ejecutar solo extracción", "python orchestrator/cli.py run --workflow extraction_only"),
        ("Ejecutar tareas específicas", "python orchestrator/cli.py run --tasks extract_contratos_emprestito transform_contratos_secop"),
        ("Ejecutar en paralelo", "python orchestrator/cli.py run --workflow full_etl --parallel --max-workers 4"),
        ("Ver estado de ejecución", "python orchestrator/cli.py status --latest"),
        ("Validar configuración", "python orchestrator/cli.py validate")
    ]
    
    for desc, cmd in examples:
        print(f"📌 {desc}:")
        print(f"   {cmd}")
        print()

def mostrar_api_examples():
    """Mostrar ejemplos de uso de la API"""
    print("=" * 60)
    print("🌐 EJEMPLOS DE USO DE LA API")
    print("=" * 60)
    
    examples = [
        ("Iniciar API", "python orchestrator/api.py", "http://localhost:8001"),
        ("Listar tareas", "GET /tasks", ""),
        ("Listar workflows", "GET /workflows", ""),
        ("Ejecutar workflow", "POST /execute", '{"workflow": "full_etl"}'),
        ("Ejecutar tareas específicas", "POST /execute", '{"tasks": ["extract_contratos_emprestito"]}'),
        ("Ver estado", "GET /executions/{execution_id}", ""),
        ("Listar ejecuciones", "GET /executions", ""),
        ("Ver reportes", "GET /reports", "")
    ]
    
    print("🚀 Para iniciar la API:")
    print("   cd orchestrator")
    print("   python api.py")
    print("   # API disponible en http://localhost:8001")
    print("   # Documentación en http://localhost:8001/docs")
    print()
    
    print("📡 Endpoints disponibles:")
    for desc, endpoint, example in examples[1:]:
        print(f"   {desc}: {endpoint}")
        if example:
            print(f"     Ejemplo: {example}")
        print()

async def main():
    """Función principal con todos los ejemplos"""
    print("🎪 EJEMPLOS DEL ORQUESTADOR ETL - CALI ALCALDÍA")
    print("=" * 60)
    
    # Ejecutar ejemplos
    await ejemplo_basico()
    await ejemplo_con_configuracion()
    await ejemplo_ejecucion_simulada()
    
    # Mostrar ejemplos de CLI y API
    mostrar_cli_examples()
    mostrar_api_examples()
    
    print("=" * 60)
    print("✨ RESUMEN DE FUNCIONALIDADES")
    print("=" * 60)
    print("🔧 Orquestador programático: Crear y ejecutar tareas desde código")
    print("📁 Configuración JSON: Definir workflows complejos")
    print("💻 CLI: Interfaz de línea de comandos completa")
    print("🌐 API REST: Integración web con FastAPI")
    print("📊 Reportes: Seguimiento detallado de ejecuciones")
    print("🔄 Workflows: Ejecución por lotes predefinidos")
    print("⚡ Paralelo: Ejecución concurrente respetando dependencias")
    print("📝 Logging: Registro completo de actividades")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())