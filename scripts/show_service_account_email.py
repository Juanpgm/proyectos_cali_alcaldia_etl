# -*- coding: utf-8 -*-
"""
Script to display Service Account email so you can share the Google Drive folder with it.
"""

import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
project_root = Path(__file__).parent.parent

# Detectar rama actual de Git y cargar .env correspondiente
try:
    import subprocess
    result = subprocess.run(
        ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
        capture_output=True,
        text=True,
        timeout=5,
        cwd=project_root
    )
    current_branch = result.stdout.strip() if result.returncode == 0 else 'main'
except Exception:
    current_branch = 'main'

# Cargar el .env correspondiente
if current_branch == 'dev':
    env_path = project_root / '.env.dev'
elif current_branch == 'main':
    env_path = project_root / '.env.prod'
else:
    env_path = project_root / '.env.dev'

if env_path.exists():
    load_dotenv(env_path)

# Cargar .env.local
env_local_path = project_root / '.env.local'
if env_local_path.exists():
    load_dotenv(env_local_path, override=True)

# Get Service Account file path
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')

print("\n" + "="*80)
print("CONFIGURACIÓN DE PERMISOS PARA GOOGLE DRIVE")
print("="*80)

if not SERVICE_ACCOUNT_FILE:
    print("❌ SERVICE_ACCOUNT_FILE no está configurado en .env")
    print("\n💡 Configura la variable en .env.prod o .env.local:")
    print(f"   SERVICE_ACCOUNT_FILE=target-credentials.json")
    print("="*80)
    exit(1)

# Resolve path
sa_path = project_root / SERVICE_ACCOUNT_FILE
if not sa_path.exists():
    print(f"❌ Service Account file not found: {sa_path}")
    print("\n💡 Verifica que el archivo existe y la ruta en .env es correcta")
    print("="*80)
    exit(1)

# Read Service Account email
try:
    with open(sa_path, 'r', encoding='utf-8') as f:
        sa_data = json.load(f)
    
    sa_email = sa_data.get('client_email')
    project_id = sa_data.get('project_id')
    
    if not sa_email:
        print("❌ No se encontró 'client_email' en el Service Account JSON")
        exit(1)
    
    print(f"\n📧 Service Account Email:")
    print(f"   {sa_email}")
    print(f"\n🔧 Project ID: {project_id}")
    
    print("\n" + "="*80)
    print("PASOS PARA COMPARTIR LA CARPETA DE GOOGLE DRIVE")
    print("="*80)
    print("\n1️⃣  Copia el email del Service Account:")
    print(f"   👉 {sa_email}")
    
    print("\n2️⃣  Ve a tu carpeta en Google Drive:")
    print("   https://drive.google.com/drive/folders/1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-")
    
    print("\n3️⃣  Haz clic derecho en la carpeta → 'Compartir' → 'Compartir con otros'")
    
    print("\n4️⃣  Pega el email del Service Account en el campo de personas")
    
    print("\n5️⃣  Asigna permisos: 'Editor' (para que pueda crear/actualizar archivos)")
    
    print("\n6️⃣  Desactiva 'Notificar a las personas' (no es necesario)")
    
    print("\n7️⃣  Haz clic en 'Enviar'")
    
    print("\n" + "="*80)
    print("⚠️  IMPORTANTE:")
    print("="*80)
    print("• El Service Account NO recibirá email (es una cuenta de servicio)")
    print("• Los archivos aparecerán en la carpeta como si los creara esta cuenta")
    print("• Necesitas permisos de 'Editor' o 'Propietario' en la carpeta")
    
    print("\n" + "="*80)
    print("✅ Una vez compartida la carpeta, ejecuta:")
    print("   python scripts/run_export_to_drive.py")
    print("="*80 + "\n")
    
except Exception as e:
    print(f"❌ Error leyendo Service Account: {e}")
    exit(1)
