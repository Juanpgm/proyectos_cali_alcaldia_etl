"""
Script de demostración del Orquestador ETL
Ejecuta una demostración completa del sistema de orquestación
"""
import asyncio
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent))

from orchestrator.etl_orchestrator import create_orchestrator

async def demo_testing_mode():
    """Demostración usando el modo testing"""
    print("🎭" + "=" * 60)
    print("🧪 DEMOSTRACIÓN - MODO TESTING")
    print("=" * 63)
    
    # Configuración de testing
    config_file = str(Path(__file__).parent / "etl_config_testing.json")
    
    # Crear orquestador con configuración de testing
    orchestrator = create_orchestrator(config_file)
    
    print(f"📊 Estadísticas del orquestador:")
    print(f"   - Tareas registradas: {len(orchestrator.tasks)}")
    
    # Mostrar tareas por categoría
    tasks_by_category = {}
    for task in orchestrator.tasks.values():
        category = task.category
        if category not in tasks_by_category:
            tasks_by_category[category] = []
        tasks_by_category[category].append(task.task_id)
    
    for category, task_ids in tasks_by_category.items():
        print(f"   - {category}: {len(task_ids)} tareas")
    
    print(f"\n🚀 Ejecutando workflow de testing básico...")
    
    # Crear orquestador temporal con solo tareas básicas
    from orchestrator.etl_orchestrator import ETLOrchestrator
    demo_orchestrator = ETLOrchestrator()
    
    # Agregar solo tareas del workflow básico
    basic_tasks = ["test_extraction_basic", "test_transformation_basic", "test_load_basic"]
    for task_id in basic_tasks:
        if task_id in orchestrator.tasks:
            demo_orchestrator.register_task(orchestrator.tasks[task_id])
    
    # Ejecutar
    try:
        results = await demo_orchestrator.execute_all()
        
        print(f"\n✅ Demostración completada exitosamente!")
        print(f"📈 Resultados:")
        
        for task_id, result in results.items():
            status_emoji = "✅" if result.status.value == "completed" else "❌"
            duration = f"{result.duration:.2f}s" if result.duration else "N/A"
            print(f"   {status_emoji} {task_id}: {result.status.value} ({duration})")
            
    except Exception as e:
        print(f"❌ Error en demostración: {e}")

def show_cli_help():
    """Mostrar ayuda del CLI"""
    print("\n🎯" + "=" * 60)
    print("💻 COMANDOS CLI DISPONIBLES")
    print("=" * 63)
    
    commands = [
        ("Listar tareas", "python orchestrator/cli.py list-tasks"),
        ("Listar workflows", "python orchestrator/cli.py list-workflows"),
        ("Testing básico", "python orchestrator/cli.py run --workflow test_basic --config orchestrator/etl_config_testing.json"),
        ("Mock ETL completo", "python orchestrator/cli.py run --workflow test_mock_etl --config orchestrator/etl_config_testing.json"),
        ("ETL real completo", "python orchestrator/cli.py run --workflow full_etl"),
        ("Solo extracción", "python orchestrator/cli.py run --workflow extraction_only"),
        ("Paralelo", "python orchestrator/cli.py run --workflow test_basic --parallel --config orchestrator/etl_config_testing.json"),
        ("Ver estado", "python orchestrator/cli.py status --latest"),
        ("Validar config", "python orchestrator/cli.py validate")
    ]
    
    for desc, cmd in commands:
        print(f"📌 {desc}:")
        print(f"   {cmd}")
        print()

def show_api_info():
    """Mostrar información de la API"""
    print("🌐" + "=" * 60)
    print("🔗 SERVIDOR API REST")
    print("=" * 63)
    
    print("🚀 Para iniciar la API:")
    print("   cd orchestrator")
    print("   python api.py")
    print("   # Disponible en: http://localhost:8001")
    print("   # Documentación: http://localhost:8001/docs")
    print()
    
    print("📡 Endpoints principales:")
    endpoints = [
        ("GET /", "Información general"),
        ("GET /health", "Health check"),
        ("GET /tasks", "Listar tareas"),
        ("GET /workflows", "Listar workflows"),
        ("POST /execute", "Ejecutar workflow/tareas"),
        ("GET /executions", "Listar ejecuciones"),
        ("GET /executions/{id}", "Estado de ejecución"),
        ("GET /reports", "Reportes disponibles")
    ]
    
    for endpoint, desc in endpoints:
        print(f"   {endpoint:<25} - {desc}")
    print()

def show_file_structure():
    """Mostrar estructura de archivos"""
    print("📁" + "=" * 60)
    print("📂 ESTRUCTURA DE ARCHIVOS")
    print("=" * 63)
    
    structure = [
        "orchestrator/",
        "├── etl_orchestrator.py      # 🎯 Módulo principal",
        "├── cli.py                   # 💻 Interfaz CLI",
        "├── api.py                   # 🌐 Servidor API",
        "├── examples.py              # 📚 Ejemplos",
        "├── etl_config.json          # ⚙️ Configuración principal",
        "├── etl_config_testing.json  # 🧪 Config testing",
        "└── README.md               # 📖 Documentación",
        "",
        "Directorios generados:",
        "├── orchestrator_logs/       # 📝 Logs",
        "└── orchestrator_reports/    # 📊 Reportes"
    ]
    
    for line in structure:
        print(f"   {line}")
    print()

async def main():
    """Función principal de demostración"""
    print("🎪🎭 DEMOSTRACIÓN DEL ORQUESTADOR ETL - CALI ALCALDÍA 🎭🎪")
    print("=" * 70)
    
    # Verificar que existe la configuración
    config_file = Path(__file__).parent / "etl_config_testing.json"
    if not config_file.exists():
        print(f"❌ Archivo de configuración no encontrado: {config_file}")
        print("   Asegúrate de que todos los archivos del orquestador estén presentes")
        return
    
    # Mostrar información general
    show_file_structure()
    
    # Ejecutar demostración
    await demo_testing_mode()
    
    # Mostrar ayuda
    show_cli_help()
    show_api_info()
    
    print("🎯" + "=" * 60)
    print("✨ RESUMEN DE FUNCIONALIDADES")
    print("=" * 63)
    
    features = [
        "🔧 Orquestación programática desde Python",
        "📁 Configuración flexible con archivos JSON", 
        "💻 CLI completo para todas las operaciones",
        "🌐 API REST con FastAPI para integración web",
        "📊 Reportes detallados y logging avanzado",
        "⚡ Ejecución paralela respetando dependencias",
        "🔄 Workflows predefinidos para casos comunes",
        "🧪 Modo testing con funciones mock",
        "📝 Documentación completa y ejemplos",
        "🚨 Manejo robusto de errores y recuperación"
    ]
    
    for feature in features:
        print(f"   {feature}")
    
    print("\n🎉" + "=" * 60)
    print("🚀 ¡ORQUESTADOR LISTO PARA USAR!")
    print("=" * 63)
    
    print("📋 Próximos pasos recomendados:")
    print("   1. Explorar con: python orchestrator/cli.py list-tasks")
    print("   2. Testing: python orchestrator/cli.py run --workflow test_basic --config orchestrator/etl_config_testing.json")
    print("   3. API: python orchestrator/api.py")
    print("   4. Leer documentación: orchestrator/README.md")
    print("   5. Personalizar: orchestrator/etl_config.json")
    
    print("\n🎭 ¡Que tengas una excelente orquestación de datos! 🎭")

if __name__ == "__main__":
    asyncio.run(main())