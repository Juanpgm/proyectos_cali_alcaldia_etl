#!/usr/bin/env python3
"""
Script de prueba local para simular el workflow de refresh manual
"""
import os
import sys
from datetime import datetime

# Agregar path del proyecto
sys.path.append('.')

def test_connections():
    """Prueba las conexiones necesarias."""
    print("🔗 Probando conexiones...")
    
    try:
        from database.config import test_connection, test_sheets_connection
        
        # Test Firebase
        firebase_ok = test_connection()
        print(f"🔥 Firebase: {'✅ OK' if firebase_ok else '❌ FAIL'}")
        
        # Test Google Sheets
        sheets_ok = test_sheets_connection()
        print(f"📊 Sheets: {'✅ OK' if sheets_ok else '❌ FAIL'}")
        
        return firebase_ok and sheets_ok
        
    except Exception as e:
        print(f"❌ Error probando conexiones: {e}")
        return False

def run_pipeline(force_full_sync=False, debug_mode=False):
    """Ejecuta el pipeline de unidades de proyecto."""
    print("🏗️ Ejecutando pipeline de Unidades de Proyecto...")
    
    try:
        # Configurar variables de entorno si es necesario
        if force_full_sync:
            os.environ['SKIP_INCREMENTAL_CHECK'] = 'true'
            print("🔄 Forzando sincronización completa")
        
        if debug_mode:
            os.environ['SECURE_LOGGING'] = 'false'
            print("🐛 Modo debug activado")
        
        # Importar y ejecutar pipeline
        from pipelines.unidades_proyecto_pipeline import run_unidades_proyecto_pipeline
        
        success = run_unidades_proyecto_pipeline()
        
        if success:
            print("✅ Pipeline ejecutado exitosamente")
        else:
            print("❌ Pipeline falló")
            
        return success
        
    except Exception as e:
        print(f"💥 Error ejecutando pipeline: {e}")
        if debug_mode:
            import traceback
            traceback.print_exc()
        return False

def test_mode():
    """Ejecuta en modo prueba (solo conexiones)."""
    print("🧪 MODO PRUEBA - Solo verificando conexiones")
    print("=" * 80)
    print(f"📅 Timestamp: {datetime.now().isoformat()}")
    
    try:
        success = test_connections()
        
        if success:
            print("✅ Todas las conexiones funcionan correctamente")
            print("🎉 Test completado exitosamente")
            return True
        else:
            print("❌ Algunas conexiones fallaron")
            return False
            
    except Exception as e:
        print(f"💥 Error en modo prueba: {e}")
        return False

def production_mode(force_full_sync=False, debug_mode=False):
    """Ejecuta en modo producción (pipeline completo)."""
    print("🚀 MODO PRODUCCIÓN - Pipeline completo")
    print("=" * 80)
    print(f"📅 Timestamp: {datetime.now().isoformat()}")
    print(f"🔄 Force Full Sync: {force_full_sync}")
    print(f"🐛 Debug Mode: {debug_mode}")
    
    try:
        # Primero probar conexiones
        if not test_connections():
            print("❌ Fallo en conexiones iniciales")
            return False
        
        # Ejecutar pipeline
        success = run_pipeline(force_full_sync, debug_mode)
        
        if success:
            print("🎉 Pipeline completado exitosamente")
            return True
        else:
            print("❌ Pipeline falló")
            return False
            
    except Exception as e:
        print(f"💥 Error en modo producción: {e}")
        return False

def main():
    """Función principal con menú interactivo."""
    print("🔄 Test Local - Unidades Proyecto Manual Refresh")
    print("=" * 80)
    print("1. 🧪 Modo PRUEBA (solo test conexiones)")
    print("2. 🚀 Modo PRODUCCIÓN normal")
    print("3. 🔄 Modo PRODUCCIÓN con sync completo")
    print("4. 🐛 Modo PRODUCCIÓN con debug")
    print("5. ❌ Salir")
    print()
    
    choice = input("Selecciona una opción (1-5): ").strip()
    
    if choice == "1":
        success = test_mode()
        
    elif choice == "2":
        success = production_mode()
        
    elif choice == "3":
        success = production_mode(force_full_sync=True)
        
    elif choice == "4":
        success = production_mode(debug_mode=True)
        
    elif choice == "5":
        print("👋 ¡Hasta luego!")
        return
        
    else:
        print("❌ Opción inválida")
        return
    
    # Mostrar resultado final
    if success:
        print(f"\n🎉 Operación completada exitosamente a las {datetime.now().strftime('%H:%M:%S')}")
    else:
        print(f"\n❌ Operación falló a las {datetime.now().strftime('%H:%M:%S')}")
        sys.exit(1)

if __name__ == "__main__":
    main()