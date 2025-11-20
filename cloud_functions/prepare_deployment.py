"""
Script para preparar el paquete de deployment de Cloud Function.
Copia todos los módulos necesarios al directorio cloud_functions.
"""

import shutil
from pathlib import Path

def prepare_deployment():
    """Prepara el directorio para deployment copiando módulos necesarios."""
    
    # Directorios base
    project_root = Path(__file__).parent.parent
    cloud_functions_dir = Path(__file__).parent
    
    # Lista de directorios a copiar
    modules_to_copy = [
        'transformation_app',
        'load_app',
        'extraction_app',
        'utils',
        'basemaps'
    ]
    
    print("🔧 Preparando deployment package...")
    print(f"📂 Proyecto: {project_root}")
    print(f"📦 Destino: {cloud_functions_dir}")
    print()
    
    # Copiar cada módulo
    for module_name in modules_to_copy:
        source = project_root / module_name
        dest = cloud_functions_dir / module_name
        
        if source.exists():
            # Eliminar destino si existe
            if dest.exists():
                print(f"🗑️  Eliminando {module_name} existente...")
                shutil.rmtree(dest)
            
            # Copiar módulo
            print(f"📋 Copiando {module_name}...")
            shutil.copytree(source, dest, ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '.pytest_cache'))
            print(f"   ✅ {module_name} copiado")
        else:
            print(f"   ⚠️  {module_name} no encontrado, saltando...")
    
    # Copiar archivos de configuración necesarios
    config_files = [
        'target-credentials.json',
        'sheets-service-account.json'
    ]
    
    print()
    print("📄 Copiando archivos de configuración...")
    for config_file in config_files:
        source = project_root / config_file
        dest = cloud_functions_dir / config_file
        
        if source.exists():
            shutil.copy2(source, dest)
            print(f"   ✅ {config_file} copiado")
        else:
            print(f"   ⚠️  {config_file} no encontrado")
    
    print()
    print("✅ Deployment package preparado exitosamente!")
    print()
    print("📋 Próximos pasos:")
    print("   1. cd cloud_functions")
    print("   2. gcloud functions deploy etl-pipeline-hourly \\")
    print("      --gen2 --runtime=python311 --region=us-central1 \\")
    print("      --source=. --entry-point=etl_pipeline_hourly \\")
    print("      --trigger-http --allow-unauthenticated \\")
    print("      --memory=2048MB --timeout=540s --max-instances=1 \\")
    print("      --project=unidad-cumplimiento-aa245")

if __name__ == '__main__':
    prepare_deployment()
