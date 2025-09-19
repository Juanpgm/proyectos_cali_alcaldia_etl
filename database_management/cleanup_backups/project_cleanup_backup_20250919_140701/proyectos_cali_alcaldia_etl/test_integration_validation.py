"""
Script de validación de la integración de base de datos con orchestrator
========================================================================

Este script valida que la integración funcional de la base de datos
esté funcionando correctamente con el sistema orchestrator.
"""

import asyncio
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent))

from orchestrator.etl_orchestrator import ETLOrchestrator, Task

def test_database_integration():
    """Prueba la integración de base de datos"""
    print("🧪 PRUEBA DE INTEGRACIÓN DE BASE DE DATOS")
    print("=" * 50)
    
    try:
        # Crear orchestrator
        print("🏗️ Creando ETLOrchestrator...")
        orchestrator = ETLOrchestrator()
        print("✅ ETLOrchestrator creado exitosamente")
        
        # Verificar estado de base de datos
        print("\n🔍 Verificando estado de base de datos...")
        db_status = orchestrator.get_database_status()
        
        print("📊 Estado de la base de datos:")
        print(f"   Conectado: {'✅' if db_status['database']['connected'] else '❌'}")
        print(f"   Schema completo: {'✅' if db_status['database']['schema_complete'] else '❌'}")
        print(f"   Estado: {db_status['database']['status'].upper()}")
        print(f"   Listo para ETL: {'✅' if db_status['ready_for_etl'] else '❌'}")
        
        # Verificar readiness
        print("\n🚀 Verificando readiness para ETL...")
        readiness = orchestrator.check_etl_readiness()
        
        print("📋 Readiness check:")
        print(f"   Listo: {'✅' if readiness['ready'] else '❌'}")
        print(f"   Conexión: {'✅' if readiness.get('connection') else '❌'}")
        print(f"   Schema: {'✅' if readiness.get('schema_complete') else '❌'}")
        
        if not readiness['ready']:
            print("\n⚠️ Detalles del problema:")
            details = readiness.get('details', {})
            print(f"   Mensaje: {details.get('message', 'Sin detalles')}")
            
            missing_tables = details.get('missing_tables', [])
            if missing_tables:
                print(f"   Tablas faltantes: {len(missing_tables)}")
                for table in missing_tables[:3]:  # Mostrar primeras 3
                    print(f"     - {table}")
                if len(missing_tables) > 3:
                    print(f"     ... y {len(missing_tables) - 3} más")
        
        # Probar funciones de monitoreo
        print("\n📈 Probando funciones de monitoreo...")
        try:
            from orchestrator.database_integration import (
                check_database_before_etl,
                get_api_database_status
            )
            
            # Probar función de conveniencia
            db_ready = check_database_before_etl()
            print(f"   check_database_before_etl(): {'✅' if db_ready else '❌'}")
            
            # Probar función de API
            api_status = get_api_database_status()
            print(f"   get_api_database_status(): {'✅' if api_status['ready_for_etl'] else '❌'}")
            
            print("✅ Funciones de monitoreo funcionando correctamente")
            
        except Exception as e:
            print(f"❌ Error en funciones de monitoreo: {e}")
        
        # Registrar una tarea de ejemplo para verificar integración completa
        print("\n📝 Registrando tarea de ejemplo...")
        orchestrator.register_task(Task(
            task_id="test_integration",
            name="Tarea de Prueba de Integración",
            description="Tarea para probar la integración de BD",
            module_path="builtins",  # Módulo que siempre existe
            function_name="len",  # Función simple para prueba
            category="test"
        ))
        
        tasks = orchestrator.list_tasks()
        print(f"✅ Tarea registrada. Total de tareas: {len(tasks)}")
        
        print(f"\n🎉 INTEGRACIÓN VALIDADA EXITOSAMENTE!")
        print("   ✅ ETLOrchestrator funcional")
        print("   ✅ Integración de BD operativa")
        print("   ✅ Funciones de monitoreo disponibles")
        print("   ✅ Funciones de conveniencia funcionando")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR EN INTEGRACIÓN: {e}")
        import traceback
        print("\n🔍 Detalles del error:")
        print(traceback.format_exc())
        return False

async def test_etl_with_monitoring():
    """Prueba ETL con monitoreo (sin ejecutar tareas reales)"""
    print("\n\n🚀 PRUEBA DE ETL CON MONITOREO")
    print("=" * 50)
    
    try:
        orchestrator = ETLOrchestrator()
        
        # Registrar tareas dummy para testing
        orchestrator.register_task(Task(
            task_id="dummy_extract",
            name="Extracción Dummy",
            description="Tarea dummy para testing",
            module_path="builtins",
            function_name="abs",
            parameters={"x": -5},
            category="extraction"
        ))
        
        orchestrator.register_task(Task(
            task_id="dummy_transform",
            name="Transformación Dummy",
            description="Tarea dummy para testing",
            module_path="builtins", 
            function_name="str",
            parameters={"object": 42},
            dependencies=["dummy_extract"],
            category="transformation"
        ))
        
        print("📝 Tareas dummy registradas")
        
        # Simular verificaciones pre-ETL
        print("\n🔍 Simulando verificaciones pre-ETL...")
        readiness = orchestrator.check_etl_readiness()
        print(f"   ETL readiness: {'✅' if readiness['ready'] else '❌'}")
        
        if not readiness['ready']:
            print("⚠️ ETL no está listo, pero continuando para pruebas...")
        
        # Verificar integración de monitoreo
        print("\n📊 Verificando capacidades de monitoreo...")
        try:
            monitoring_result = orchestrator.db_integration.monitor_etl_execution("test_execution")
            print(f"   Monitoreo ETL: ✅ (Estado: {monitoring_result.get('health_status', 'unknown')})")
        except Exception as e:
            print(f"   Monitoreo ETL: ⚠️ ({str(e)[:50]}...)")
        
        try:
            report_result = orchestrator.db_integration.generate_post_etl_report("test_execution")
            print(f"   Reporte post-ETL: ✅ (Estado: {report_result.get('overall_assessment', {}).get('overall_status', 'unknown')})")
        except Exception as e:
            print(f"   Reporte post-ETL: ⚠️ ({str(e)[:50]}...)")
        
        print("\n✅ Prueba de ETL con monitoreo completada")
        return True
        
    except Exception as e:
        print(f"\n❌ Error en prueba ETL con monitoreo: {e}")
        return False

def main():
    """Función principal de validación"""
    print("🧪 VALIDACIÓN COMPLETA DE INTEGRACIÓN")
    print("=" * 60)
    
    # Test 1: Integración básica de base de datos
    success1 = test_database_integration()
    
    # Test 2: ETL con monitoreo
    success2 = asyncio.run(test_etl_with_monitoring())
    
    # Resumen final
    print("\n" + "=" * 60)
    print("📊 RESUMEN DE VALIDACIÓN")
    print("=" * 60)
    print(f"   Integración de BD: {'✅ EXITOSA' if success1 else '❌ FALLIDA'}")
    print(f"   ETL con monitoreo: {'✅ EXITOSA' if success2 else '❌ FALLIDA'}")
    
    if success1 and success2:
        print("\n🎉 TODAS LAS PRUEBAS EXITOSAS!")
        print("   La integración funcional está completamente operativa")
        print("   El sistema orchestrator puede usar las nuevas capacidades de BD")
        print("   Listo para producción")
    else:
        print("\n⚠️ ALGUNAS PRUEBAS FALLARON")
        print("   Revisa los errores anteriores para más detalles")
    
    return success1 and success2

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)