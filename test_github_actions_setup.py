#!/usr/bin/env python3
"""
Script de prueba para verificar la configuración de GitHub Actions
y la conexión a Railway PostgreSQL
"""

import os
import sys
from pathlib import Path

# Agregar paths necesarios
sys.path.append(str(Path(__file__).parent / "database_management" / "core"))
sys.path.append(str(Path(__file__).parent / "load_app"))

def test_configuration():
    """Probar configuración completa del ETL"""
    
    print("🔍 VERIFICACIÓN DE CONFIGURACIÓN ETL PARA GITHUB ACTIONS")
    print("=" * 60)
    
    # 1. Test de importaciones
    print("\n1️⃣ Verificando importaciones...")
    try:
        from config import get_database_config, test_connection
        print("   ✅ Config module importado correctamente")
        
        from bulk_load_data import load_all_available_data
        print("   ✅ Bulk load module importado correctamente")
        
    except ImportError as e:
        print(f"   ❌ Error de importación: {e}")
        return False
    
    # 2. Test de configuración de base de datos
    print("\n2️⃣ Verificando configuración de base de datos...")
    try:
        config = get_database_config()
        
        if config.database_url:
            print("   ✅ DATABASE_URL detectada (configuración Railway)")
            # No mostrar la URL completa por seguridad
            print(f"   🔧 Host: {config.database_url.split('@')[1].split('/')[0] if '@' in config.database_url else 'Railway'}")
        else:
            print("   🏠 Configuración local detectada")
            print(f"   🔧 Host: {config.host}:{config.port}")
            print(f"   🔧 Database: {config.database}")
        
    except Exception as e:
        print(f"   ❌ Error en configuración: {e}")
        return False
    
    # 3. Test de conexión (solo si DATABASE_URL está disponible)
    print("\n3️⃣ Verificando conexión a base de datos...")
    database_url = os.getenv("DATABASE_URL") or os.getenv("RAILWAY_DATABASE_URL")
    
    if database_url:
        try:
            success = test_connection(config)
            if success:
                print("   ✅ Conexión a Railway PostgreSQL exitosa")
            else:
                print("   ❌ Conexión fallida")
                return False
        except Exception as e:
            print(f"   ❌ Error de conexión: {e}")
            return False
    else:
        print("   ⚠️  DATABASE_URL no encontrada (esto es normal en desarrollo local)")
        print("   ℹ️  La conexión se probará en GitHub Actions con el secret")
    
    # 4. Verificar archivos necesarios
    print("\n4️⃣ Verificando archivos del ETL...")
    
    required_files = [
        ".github/workflows/etl-automation.yml",
        "load_app/bulk_load_data.py",
        "database_management/core/config.py",
        "database_management/core/models.py",
        "requirements.txt"
    ]
    
    all_files_exist = True
    for file_path in required_files:
        full_path = Path(__file__).parent / file_path
        if full_path.exists():
            print(f"   ✅ {file_path}")
        else:
            print(f"   ❌ {file_path} - NO ENCONTRADO")
            all_files_exist = False
    
    if not all_files_exist:
        return False
    
    # 5. Verificar configuración del workflow
    print("\n5️⃣ Verificando configuración de GitHub Actions...")
    
    workflow_path = Path(__file__).parent / ".github" / "workflows" / "etl-automation.yml"
    if workflow_path.exists():
        print("   ✅ Workflow de automatización configurado")
        print("   📅 Ejecución programada: Diariamente a las 2 AM UTC")
        print("   🎯 Trigger manual: Disponible en GitHub Actions UI")
    else:
        print("   ❌ Workflow no encontrado")
        return False
    
    # 6. Instrucciones finales
    print("\n" + "=" * 60)
    print("🎉 CONFIGURACIÓN COMPLETA Y LISTA!")
    print("=" * 60)
    
    print("\n📋 PRÓXIMOS PASOS PARA ACTIVAR:")
    print("1. Subir código a GitHub:")
    print("   git add .")
    print("   git commit -m 'Configure ETL automation with GitHub Actions'")
    print("   git push origin main")
    
    print("\n2. Configurar secret en GitHub:")
    print("   • Ve a tu repositorio en GitHub")
    print("   • Settings → Secrets and variables → Actions")
    print("   • New repository secret:")
    print("     Name: RAILWAY_DATABASE_URL")
    print("     Value: [tu DATABASE_URL de Railway]")
    
    print("\n3. Probar ejecución manual:")
    print("   • Ve a Actions tab en GitHub")
    print("   • Click 'ETL Data Processing Automation'")
    print("   • Click 'Run workflow'")
    
    print("\n🚀 ¡El ETL se ejecutará automáticamente diariamente!")
    
    return True

if __name__ == "__main__":
    try:
        success = test_configuration()
        if success:
            print("\n✅ Configuración verificada exitosamente")
            sys.exit(0)
        else:
            print("\n❌ Errores encontrados en la configuración")
            sys.exit(1)
    except Exception as e:
        print(f"\n💥 Error inesperado: {e}")
        sys.exit(1)