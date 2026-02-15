#!/usr/bin/env python3
"""
Script para configurar automáticamente la service account para Google Drive
"""

import subprocess
import sys
import os
import json
from pathlib import Path

def get_gcloud_path():
    """Obtener la ruta completa de gcloud"""
    # Intentar encontrar gcloud en ubicaciones comunes
    possible_paths = [
        r"C:\Users\juanp\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
        r"C:\Program Files\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd",
        "gcloud"  # Si está en PATH
    ]
    
    for path in possible_paths:
        try:
            result = subprocess.run([path, "version"], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print(f"✅ gcloud encontrado en: {path}")
                return path
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            continue
    
    print("❌ gcloud no encontrado. Instala Google Cloud SDK:")
    print("   https://cloud.google.com/sdk/docs/install")
    return None

def create_service_account(gcloud_path):
    """Crear service account en calitrack-44403"""
    project_id = "calitrack-44403"
    sa_name = "unidades-proyecto-bot"
    sa_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
    
    print(f"🤖 Creando service account: {sa_name}")
    
    try:
        # Crear service account
        result = subprocess.run([
            gcloud_path, 'iam', 'service-accounts', 'create', sa_name,
            f'--project={project_id}',
            '--description=Bot para leer documentos Excel de unidades de proyecto',
            '--display-name=Unidades Proyecto Bot'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Service account creada: {sa_email}")
        else:
            print(f"⚠️ Service account ya existe o error: {result.stderr}")
    except Exception as e:
        print(f"❌ Error creando service account: {e}")
        return None
    
    return sa_email

def download_service_account_key(gcloud_path, sa_email):
    """Descargar clave JSON de la service account"""
    project_id = "calitrack-44403"
    key_path = Path("env/unidades_proyecto_service_account.json")
    
    # Crear directorio si no existe
    key_path.parent.mkdir(exist_ok=True)
    
    print(f"🔑 Descargando clave para: {sa_email}")
    
    try:
        result = subprocess.run([
            gcloud_path, 'iam', 'service-accounts', 'keys', 'create', str(key_path),
            f'--iam-account={sa_email}',
            f'--project={project_id}'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Clave descargada: {key_path}")
            return True
        else:
            print(f"❌ Error descargando clave: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_drive_access():
    """Probar acceso a Google Drive"""
    print("🔍 Probando acceso a Google Drive...")
    
    try:
        # Intentar importar y usar la configuración
        sys.path.append('database')
        from config import get_drive_service, list_excel_files_in_folder
        
        # Configurar folder ID
        folder_id = "10LoPbAG7nGmxiFwKQllT3djzsXhGHZur"
        
        # Probar servicio
        service = get_drive_service()
        if not service:
            print("❌ No se pudo obtener servicio de Drive")
            return False
        
        # Probar listado de archivos
        files = list_excel_files_in_folder(folder_id)
        print(f"✅ Encontrados {len(files)} archivos Excel en la carpeta")
        
        for file in files[:3]:  # Mostrar solo los primeros 3
            print(f"   📄 {file['name']}")
        
        if len(files) > 3:
            print(f"   ... y {len(files) - 3} archivos más")
            
        return True
        
    except ImportError as e:
        print(f"❌ Error importando módulos: {e}")
        return False
    except Exception as e:
        print(f"❌ Error accediendo a Drive: {e}")
        return False

def main():
    print("🚀 CONFIGURACIÓN DE SERVICE ACCOUNT PARA GOOGLE DRIVE")
    print("=" * 60)
    
    # 1. Encontrar gcloud
    gcloud_path = get_gcloud_path()
    if not gcloud_path:
        return False
    
    # 2. Crear service account
    sa_email = create_service_account(gcloud_path)
    if not sa_email:
        print("❌ No se pudo crear la service account")
        return False
    
    # 3. Descargar clave
    if not download_service_account_key(gcloud_path, sa_email):
        print("❌ No se pudo descargar la clave")
        return False
    
    # 4. Mostrar instrucciones de compartir
    print("\n📤 COMPARTIR CARPETA DE DRIVE")
    print("=" * 40)
    print("Para completar la configuración:")
    print("1. Abre: https://drive.google.com/drive/folders/10LoPbAG7nGmxiFwKQllT3djzsXhGHZur")
    print("2. Haz clic en 'Compartir' (Share)")
    print(f"3. Agrega este email como 'Viewer': {sa_email}")
    print("4. Haz clic en 'Send'")
    
    print("\n⏳ Esperando a que compartas la carpeta...")
    input("Presiona Enter cuando hayas compartido la carpeta...")
    
    # 5. Probar acceso
    print("\n🔍 PROBANDO ACCESO")
    print("=" * 30)
    if test_drive_access():
        print("\n🎉 ¡CONFIGURACIÓN COMPLETADA!")
        print("✅ La service account puede acceder a los archivos Excel")
        print("✅ El sistema está listo para extraer datos")
    else:
        print("\n❌ Error en la configuración")
        print("💡 Verifica que hayas compartido la carpeta correctamente")
        return False
    
    return True
    print("\n🔍 PROBANDO ACCESO")
    print("=" * 30)
    if test_drive_access():
        print("\n🎉 ¡CONFIGURACIÓN COMPLETADA!")
        print("✅ La service account puede acceder a los archivos Excel")
        print("✅ El sistema está listo para extraer datos")
    else:
        print("\n❌ Error en la configuración")
        print("💡 Verifica que hayas compartido la carpeta correctamente")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
    
    print(f"\n📝 PRÓXIMOS PASOS:")
    print("1. Ejecutar extracción: python extraction_app/data_extraction_unidades_proyecto.py")
    print("2. Verificar datos extraídos")
    print("3. Continuar con pipeline ETL")