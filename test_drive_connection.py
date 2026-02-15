#!/usr/bin/env python3
"""
Script de prueba rápido para verificar acceso a Google Drive
"""

import sys
import os
sys.path.append('database')

from config import get_drive_service, list_excel_files_in_folder

def test_drive_connection():
    """Prueba rápida de conexión a Google Drive"""
    print("🔍 Probando conexión a Google Drive...")
    
    # ID de la carpeta objetivo
    folder_id = "10LoPbAG7nGmxiFwKQllT3djzsXhGHZur"
    
    try:
        # Obtener servicio de Drive
        service = get_drive_service()
        if not service:
            print("❌ No se pudo obtener servicio de Google Drive")
            print("💡 Verifica:")
            print("  - Que la service account esté configurada")
            print("  - Que SERVICE_ACCOUNT_FILE apunte al archivo correcto")
            print("  - Que tengas permisos en el proyecto calitrack-44403")
            return False
        
        print("✅ Servicio de Google Drive obtenido")
        
        # Listar archivos Excel en la carpeta
        print(f"📁 Buscando archivos Excel en carpeta: {folder_id}")
        files = list_excel_files_in_folder(folder_id)
        
        if not files:
            print("⚠️ No se encontraron archivos Excel en la carpeta")
            print("💡 Verifica:")
            print("  - Que la carpeta tenga archivos .xlsx")
            print("  - Que la service account tenga permisos de lectura")
            print("  - Que la carpeta esté compartida con la service account")
            return False
        
        print(f"✅ Encontrados {len(files)} archivos Excel:")
        for i, file in enumerate(files, 1):
            print(f"  {i}. 📄 {file['name']} (ID: {file['id'][:20]}...)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("💡 Revisa la configuración de autenticación")
        return False

if __name__ == "__main__":
    success = test_drive_connection()
    if success:
        print("\n🎉 ¡Conexión exitosa!")
        print("📝 Puedes proceder a ejecutar la extracción de datos")
    else:
        print("\n❌ Conexión fallida")
        print("📋 Ejecuta setup_drive_bot.py para configurar")
        sys.exit(1)