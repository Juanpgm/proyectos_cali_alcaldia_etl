#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de prueba para verificar la extracción de Google Drive.
"""

import sys
import os

# Agregar paths necesarios
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def test_imports():
    """Prueba que todas las importaciones funcionen correctamente."""
    print("=" * 80)
    print("PRUEBA DE IMPORTACIONES - GOOGLE DRIVE EXTRACTION")
    print("=" * 80)
    
    try:
        print("\n1. Importando módulo de configuración...")
        from database.config import (
            get_drive_service,
            list_excel_files_in_folder,
            download_excel_file,
            DRIVE_FOLDER_ID
        )
        print("✅ Módulo de configuración importado correctamente")
        print(f"   DRIVE_FOLDER_ID configurado: {'Sí' if DRIVE_FOLDER_ID else 'No'}")
        
    except Exception as e:
        print(f"❌ Error importando configuración: {e}")
        return False
    
    try:
        print("\n2. Importando módulo de extracción...")
        from extraction_app.data_extraction_unidades_proyecto import (
            extract_unidades_proyecto_data,
            extract_and_save_unidades_proyecto
        )
        print("✅ Módulo de extracción importado correctamente")
        
    except Exception as e:
        print(f"❌ Error importando extracción: {e}")
        return False
    
    print("\n3. Verificando dependencias...")
    try:
        import pandas as pd
        print(f"✅ pandas {pd.__version__}")
    except ImportError:
        print("❌ pandas no instalado")
        return False
    
    try:
        import openpyxl
        print(f"✅ openpyxl instalado")
    except ImportError:
        print("❌ openpyxl no instalado")
        return False
    
    try:
        import xlrd
        print(f"✅ xlrd instalado")
    except ImportError:
        print("❌ xlrd no instalado")
        return False
    
    try:
        from googleapiclient.discovery import build
        print(f"✅ google-api-python-client instalado")
    except ImportError:
        print("❌ google-api-python-client no instalado")
        return False
    
    print("\n" + "=" * 80)
    print("✅ TODAS LAS IMPORTACIONES EXITOSAS")
    print("=" * 80)
    print("\n📝 Para ejecutar la extracción:")
    print("   python extraction_app/data_extraction_unidades_proyecto.py")
    print("\n📝 Para usar en código:")
    print("   from extraction_app.data_extraction_unidades_proyecto import extract_unidades_proyecto_data")
    print("   df = extract_unidades_proyecto_data()")
    
    return True


def test_authentication():
    """Prueba la autenticación con Google Drive."""
    print("\n" + "=" * 80)
    print("PRUEBA DE AUTENTICACIÓN - GOOGLE DRIVE")
    print("=" * 80)
    
    try:
        from database.config import get_drive_service
        
        print("\nIntentando conectar con Google Drive...")
        service = get_drive_service()
        
        if service:
            print("✅ Autenticación exitosa con Google Drive")
            return True
        else:
            print("❌ No se pudo autenticar con Google Drive")
            print("\n💡 Para autenticar, ejecuta:")
            print("   gcloud auth application-default login --scopes=https://www.googleapis.com/auth/drive.readonly")
            return False
            
    except Exception as e:
        print(f"❌ Error en autenticación: {e}")
        print("\n💡 Para autenticar, ejecuta:")
        print("   gcloud auth application-default login --scopes=https://www.googleapis.com/auth/drive.readonly")
        return False


def test_list_files():
    """Prueba listado de archivos en la carpeta de Drive."""
    print("\n" + "=" * 80)
    print("PRUEBA DE LISTADO DE ARCHIVOS")
    print("=" * 80)
    
    try:
        from database.config import DRIVE_FOLDER_ID, list_excel_files_in_folder
        
        if not DRIVE_FOLDER_ID:
            print("❌ DRIVE_FOLDER_ID no configurado")
            print("💡 Configura la variable de entorno DRIVE_UNIDADES_PROYECTO_FOLDER_ID")
            return False
        
        print(f"\nListando archivos en carpeta: {DRIVE_FOLDER_ID[:10]}***")
        files = list_excel_files_in_folder(DRIVE_FOLDER_ID)
        
        if files:
            print(f"✅ Encontrados {len(files)} archivos Excel")
            for i, file in enumerate(files, 1):
                print(f"   {i}. {file['name']}")
            return True
        else:
            print("⚠️  No se encontraron archivos Excel en la carpeta")
            print("   Verifica:")
            print("   - Que la carpeta contenga archivos .xlsx o .xls")
            print("   - Que tengas permisos de lectura en la carpeta")
            return False
            
    except Exception as e:
        print(f"❌ Error listando archivos: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\n🚀 Iniciando pruebas de extracción desde Google Drive...\n")
    
    # Test 1: Importaciones
    if not test_imports():
        print("\n❌ Prueba de importaciones falló")
        sys.exit(1)
    
    # Test 2: Autenticación
    if not test_authentication():
        print("\n⚠️  Autenticación no completada")
        print("   Configura la autenticación antes de ejecutar la extracción completa")
    
    # Test 3: Listado de archivos
    if not test_list_files():
        print("\n⚠️  No se pudieron listar archivos")
        print("   Verifica la configuración y permisos de Google Drive")
    
    print("\n" + "=" * 80)
    print("PRUEBAS COMPLETADAS")
    print("=" * 80)
    print("\n✅ El sistema está listo para extraer datos desde Google Drive")
    print("   Ejecuta: python extraction_app/data_extraction_unidades_proyecto.py")
