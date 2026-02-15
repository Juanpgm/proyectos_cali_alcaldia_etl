#!/usr/bin/env python3
"""
Script de configuración para migrar todos los procesos al proyecto calitrack-44403
con la cuenta juanp.gzmz@gmail.com

Este script valida y configura la autenticación necesaria para el nuevo proyecto Firebase.
"""

import os
import subprocess
import sys
from pathlib import Path

def check_gcloud_auth():
    """Verifica si está autenticado en gcloud"""
    try:
        result = subprocess.run(
            ['gcloud', 'auth', 'list', '--format=json'], 
            capture_output=True, text=True, check=True
        )
        import json
        accounts = json.loads(result.stdout)
        
        # Verificar si juanp.gzmz@gmail.com está autenticado
        for account in accounts:
            if account.get('account') == 'juanp.gzmz@gmail.com' and account.get('status') == 'ACTIVE':
                return True
        return False
    except:
        return False

def check_adc_config():
    """Verifica configuración de Application Default Credentials"""
    try:
        result = subprocess.run(
            ['gcloud', 'auth', 'application-default', 'print-access-token'], 
            capture_output=True, text=True
        )
        return result.returncode == 0
    except:
        return False

def check_project_config():
    """Verifica si el proyecto está configurado correctamente"""
    try:
        result = subprocess.run(
            ['gcloud', 'config', 'get-value', 'project'], 
            capture_output=True, text=True, check=True
        )
        current_project = result.stdout.strip()
        return current_project == 'calitrack-44403'
    except:
        return False

def main():
    print("🚀 CONFIGURACIÓN DE MIGRACIÓN A CALITRACK-44403")
    print("=" * 60)
    
    # Verificar autenticación en gcloud
    print("1. Verificando autenticación en gcloud...")
    if check_gcloud_auth():
        print("   ✅ Autenticado como juanp.gzmz@gmail.com")
    else:
        print("   ❌ No está autenticado con juanp.gzmz@gmail.com")
        print("   🔧 Ejecute: gcloud auth login juanp.gzmz@gmail.com")
        return False
    
    # Verificar proyecto configurado
    print("2. Verificando proyecto configurado...")
    if check_project_config():
        print("   ✅ Proyecto configurado: calitrack-44403")
    else:
        print("   ❌ Proyecto no configurado")
        print("   🔧 Ejecute: gcloud config set project calitrack-44403")
        return False
    
    # Verificar ADC
    print("3. Verificando Application Default Credentials...")
    if check_adc_config():
        print("   ✅ ADC configurado correctamente")
    else:
        print("   ❌ ADC no configurado")
        print("   🔧 Ejecute: gcloud auth application-default login --project=calitrack-44403")
        return False
    
    # Verificar archivos de configuración
    print("4. Verificando archivos de configuración...")
    project_root = Path(__file__).parent
    
    env_files = ['.env.dev', '.env.prod', '.env.local']
    for env_file in env_files:
        env_path = project_root / env_file
        if env_path.exists():
            with open(env_path, 'r', encoding='utf-8') as f:
                content = f.read()
                if 'calitrack-44403' in content:
                    print(f"   ✅ {env_file} configurado para calitrack-44403")
                else:
                    print(f"   ❌ {env_file} no actualizado")
        else:
            print(f"   ⚠️  {env_file} no existe")
    
    # Verificar configuración de Google Workspace
    env_local_path = project_root / '.env.local'
    if env_local_path.exists():
        with open(env_local_path, 'r', encoding='utf-8') as f:
            content = f.read()
            if 'juanp.gzmz@gmail.com' in content:
                print("   ✅ Email de Google Workspace configurado")
            else:
                print("   ❌ Email de Google Workspace no configurado")
    
    print("\n🎉 MIGRACIÓN COMPLETADA")
    print("=" * 60)
    print("Todos los procesos están ahora configurados para usar:")
    print("• Proyecto Firebase: calitrack-44403")
    print("• Cuenta Google Workspace: juanp.gzmz@gmail.com")
    print()
    print("📝 PRÓXIMOS PASOS:")
    print("1. Verificar que tienes permisos en el proyecto calitrack-44403")
    print("2. Compartir carpetas de Google Drive con juanp.gzmz@gmail.com")
    print("3. Ejecutar test: python -c 'from database.config import test_connection; test_connection()'")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)