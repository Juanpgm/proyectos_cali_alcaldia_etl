#!/usr/bin/env python3
"""
Script de Limpieza de Archivos Obsoletos - Solo Código Proyecto
==============================================================

Este script identifica y elimina ÚNICAMENTE archivos obsoletos del código del
proyecto, excluyendo completamente el entorno virtual y archivos del sistema.

Autor: Sistema ETL Alcaldía de Cali
Versión: 1.0.1
"""

import os
import sys
import shutil
from pathlib import Path
from typing import List, Dict, Set, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import json
import re
import argparse

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cleanup_project_files.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ==========================================
# CONFIGURACIÓN ESPECÍFICA PARA PROYECTO
# ==========================================

@dataclass(frozen=True)
class ProjectCleanupConfig:
    """Configuración inmutable para limpieza solo del proyecto"""
    project_root: Path
    backup_dir: Path
    dry_run: bool = True
    days_old: int = 7
    keep_latest: int = 2
    
    # Directorios que NUNCA se tocan
    excluded_dirs: Tuple[str, ...] = (
        'env',
        '.git',
        'node_modules',
        '.vscode',
        '__pycache__',
        '.pytest_cache'
    )
    
    # Archivos críticos que nunca se eliminan
    critical_files: Tuple[str, ...] = (
        'requirements.txt',
        'README.md',
        'REPORTE_FINAL.md',
        '__init__.py',
        'main.py',
        'config.py',
        'setup.py',
        '.gitignore',
        'etl_config.json',
        'etl_config_testing.json'
    )

# Patrones específicos de archivos obsoletos DEL PROYECTO
PROJECT_OBSOLETE_PATTERNS = {
    # Archivos de test temporales y demos
    'test_temp_files': [
        'test_basic_*.py',
        'test_simple_*.py', 
        'test_data_load_*.py',
        'test_integration_*.py',
        'test_project_*.py',
        'test_caracteristicas_*.py',
        'demo_*.py',
        'check_*.py',
        'temp_*.py',
        'resumen_*.py'
    ],
    
    # Logs del proyecto específicos
    'project_logs': [
        'caracteristicas_proyectos_demo.log',
        'data_load_test_simple.log',
        '*.log'
    ],
    
    # Reportes antiguos del proyecto
    'project_reports': [
        'data_load_test_report_*.json',
        'simple_test_report_*.json',
        'orchestrator_reports/*.json',
        'monitoring_report_*.json'
    ],
    
    # Archivos de cache del proyecto (no del env)
    'project_cache': [
        '__pycache__/**/*.pyc',
        '**/__pycache__/**/*.pyc'
    ]
}

def is_excluded_path(file_path: Path, config: ProjectCleanupConfig) -> bool:
    """Verifica si un archivo está en un directorio excluido"""
    path_parts = file_path.parts
    
    for excluded_dir in config.excluded_dirs:
        if excluded_dir in path_parts:
            return True
    
    return False

def analyze_project_files(root_dir: Path, config: ProjectCleanupConfig) -> Dict[str, List[Path]]:
    """Analiza archivos del proyecto excluyendo directorios del sistema"""
    categorized_files = {category: [] for category in PROJECT_OBSOLETE_PATTERNS.keys()}
    
    for pattern_category, patterns in PROJECT_OBSOLETE_PATTERNS.items():
        for pattern in patterns:
            # Búsqueda con filtro de exclusión
            if '**' in pattern:
                pattern_path = pattern.replace('**/', '')
                for file_path in root_dir.rglob(pattern_path):
                    if (file_path.is_file() and 
                        not is_excluded_path(file_path, config)):
                        categorized_files[pattern_category].append(file_path)
            else:
                for file_path in root_dir.rglob(pattern):
                    if (file_path.is_file() and 
                        not is_excluded_path(file_path, config)):
                        categorized_files[pattern_category].append(file_path)
    
    return categorized_files

def find_project_duplicates(root_dir: Path, config: ProjectCleanupConfig) -> Dict[str, List[Path]]:
    """Encuentra duplicados solo en archivos del proyecto"""
    file_groups = {}
    
    for file_path in root_dir.rglob('*'):
        if (not file_path.is_file() or 
            is_excluded_path(file_path, config)):
            continue
            
        # Agrupar por nombre base (sin timestamp)
        base_name = re.sub(r'_\d{8}_\d{6}', '', file_path.stem)
        suffix = file_path.suffix
        key = f"{base_name}{suffix}"
        
        if key not in file_groups:
            file_groups[key] = []
        file_groups[key].append(file_path)
    
    # Filtrar solo grupos con múltiples archivos
    duplicates = {k: v for k, v in file_groups.items() if len(v) > 1}
    
    return duplicates

def find_project_timestamped_files(root_dir: Path, config: ProjectCleanupConfig) -> List[Path]:
    """Encuentra archivos con timestamp antiguos solo en el proyecto"""
    timestamp_pattern = re.compile(r'.*_(\d{8})_(\d{6})\..*')
    timestamped_files = {}
    
    for file_path in root_dir.rglob('*'):
        if (not file_path.is_file() or 
            is_excluded_path(file_path, config)):
            continue
            
        match = timestamp_pattern.match(file_path.name)
        if match:
            date_str, time_str = match.groups()
            try:
                file_date = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
                
                # Agrupar por tipo de archivo
                base_name = re.sub(r'_\d{8}_\d{6}', '', file_path.stem)
                if base_name not in timestamped_files:
                    timestamped_files[base_name] = []
                
                timestamped_files[base_name].append((file_path, file_date))
                
            except ValueError:
                continue
    
    # Determinar archivos a eliminar
    files_to_remove = []
    cutoff_date = datetime.now() - timedelta(days=config.days_old)
    
    for base_name, files in timestamped_files.items():
        # Ordenar por fecha (más reciente primero)
        files_sorted = sorted(files, key=lambda x: x[1], reverse=True)
        
        # Mantener los más recientes
        to_keep = files_sorted[:config.keep_latest]
        to_check = files_sorted[config.keep_latest:]
        
        # Eliminar archivos antiguos después del límite
        for file_path, file_date in to_check:
            if file_date < cutoff_date:
                files_to_remove.append(file_path)
    
    return files_to_remove

def run_project_cleanup(config: ProjectCleanupConfig) -> Dict[str, any]:
    """Ejecuta limpieza solo en archivos del proyecto"""
    logger.info("🧹 Iniciando limpieza de archivos obsoletos del PROYECTO")
    
    results = {
        'started_at': datetime.now().isoformat(),
        'config': {
            'project_root': str(config.project_root),
            'dry_run': config.dry_run,
            'days_old': config.days_old,
            'keep_latest': config.keep_latest,
            'excluded_dirs': list(config.excluded_dirs)
        },
        'analysis': {},
        'cleanup_results': {},
        'files_processed': 0
    }
    
    try:
        logger.info("📊 Analizando archivos del proyecto...")
        categorized_files = analyze_project_files(config.project_root, config)
        
        logger.info("🔍 Buscando duplicados en el proyecto...")
        duplicate_groups = find_project_duplicates(config.project_root, config)
        
        logger.info("📅 Buscando archivos timestamped antiguos del proyecto...")
        old_timestamped = find_project_timestamped_files(config.project_root, config)
        
        # Consolidar archivos a eliminar
        all_files_to_remove = set()
        
        # Agregar archivos de patrones específicos
        for category, files in categorized_files.items():
            all_files_to_remove.update(files)
        
        # Agregar duplicados (mantener solo el más reciente)
        for group_name, files in duplicate_groups.items():
            if len(files) > 1:
                files_sorted = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)
                all_files_to_remove.update(files_sorted[config.keep_latest:])
        
        all_files_to_remove.update(old_timestamped)
        
        # Filtrar archivos críticos
        files_to_remove = []
        for file_path in all_files_to_remove:
            if file_path.name not in config.critical_files:
                files_to_remove.append(file_path)
            else:
                logger.info(f"🔒 Protegiendo archivo crítico: {file_path.name}")
        
        # Guardar análisis
        results['analysis'] = {
            'categorized_files': {k: len(v) for k, v in categorized_files.items()},
            'duplicate_groups': len(duplicate_groups),
            'old_timestamped': len(old_timestamped),
            'total_files_to_remove': len(files_to_remove),
            'critical_files_protected': len(all_files_to_remove) - len(files_to_remove)
        }
        
        logger.info(f"📋 Archivos del proyecto identificados para eliminación: {len(files_to_remove)}")
        
        # Crear backup si no es dry run
        if not config.dry_run and files_to_remove:
            logger.info("💾 Creando backup de archivos del proyecto...")
            backup_success = backup_project_files(files_to_remove, config.backup_dir)
            if not backup_success:
                logger.error("❌ Falló el backup. Abortando limpieza.")
                results['success'] = False
                return results
        
        # Eliminar archivos
        logger.info("🗑️ Eliminando archivos del proyecto...")
        cleanup_results = safe_remove_project_files(files_to_remove, config.dry_run)
        
        # Limpiar directorios vacíos del proyecto
        logger.info("📁 Limpiando directorios vacíos del proyecto...")
        empty_dirs_removed = cleanup_empty_project_directories(config.project_root, config)
        
        # Guardar resultados
        results['cleanup_results'] = cleanup_results
        results['cleanup_results']['empty_dirs_removed'] = empty_dirs_removed
        results['files_processed'] = len(files_to_remove)
        results['success'] = True
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Error durante la limpieza del proyecto: {e}")
        results['success'] = False
        results['error'] = str(e)
        return results

def backup_project_files(files_to_backup: List[Path], backup_dir: Path) -> bool:
    """Crea backup específico de archivos del proyecto"""
    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_subdir = backup_dir / f"project_cleanup_backup_{timestamp}"
        backup_subdir.mkdir()
        
        for file_path in files_to_backup:
            try:
                # Mantener estructura relativa desde project root
                project_root = file_path.parents[len(file_path.parents) - 2]  # Aproximación
                relative_path = file_path.relative_to(project_root)
                backup_path = backup_subdir / relative_path
                backup_path.parent.mkdir(parents=True, exist_ok=True)
                
                shutil.copy2(file_path, backup_path)
            except Exception as e:
                logger.warning(f"No se pudo hacer backup de {file_path}: {e}")
        
        logger.info(f"💾 Backup del proyecto creado en: {backup_subdir}")
        return True
        
    except Exception as e:
        logger.error(f"Error creando backup del proyecto: {e}")
        return False

def safe_remove_project_files(files_to_remove: List[Path], dry_run: bool = True) -> Dict[str, int]:
    """Elimina archivos del proyecto de manera segura"""
    results = {'removed': 0, 'skipped': 0, 'errors': 0}
    
    for file_path in files_to_remove:
        try:
            if not file_path.exists():
                results['skipped'] += 1
                continue
            
            if dry_run:
                logger.info(f"[DRY RUN] Eliminaría archivo del proyecto: {file_path.relative_to(file_path.anchor)}")
                results['removed'] += 1
            else:
                file_path.unlink()
                logger.info(f"✅ Eliminado archivo del proyecto: {file_path.relative_to(file_path.anchor)}")
                results['removed'] += 1
                
        except Exception as e:
            logger.error(f"Error eliminando archivo del proyecto {file_path}: {e}")
            results['errors'] += 1
    
    return results

def cleanup_empty_project_directories(root_dir: Path, config: ProjectCleanupConfig) -> int:
    """Limpia directorios vacíos solo del proyecto"""
    removed_count = 0
    
    for dir_path in root_dir.rglob('*'):
        if (dir_path.is_dir() and 
            not any(dir_path.iterdir()) and  # Directorio vacío
            not is_excluded_path(dir_path, config)):
            
            try:
                if config.dry_run:
                    logger.info(f"[DRY RUN] Eliminaría directorio vacío del proyecto: {dir_path.relative_to(root_dir)}")
                else:
                    dir_path.rmdir()
                    logger.info(f"✅ Directorio vacío eliminado: {dir_path.relative_to(root_dir)}")
                removed_count += 1
            except Exception as e:
                logger.warning(f"No se pudo eliminar directorio del proyecto {dir_path}: {e}")
    
    return removed_count

def print_project_cleanup_summary(results: Dict[str, any]) -> None:
    """Imprime resumen de la limpieza del proyecto"""
    print("\\n" + "=" * 80)
    print("🧹 LIMPIEZA DE ARCHIVOS OBSOLETOS - SOLO CÓDIGO DEL PROYECTO")
    print("=" * 80)
    
    print(f"Iniciado: {results.get('started_at', 'N/A')}")
    print(f"Estado: {'✅ EXITOSO' if results.get('success') else '❌ CON ERRORES'}")
    
    if 'config' in results:
        config = results['config']
        print(f"Modo: {'🔍 DRY RUN (Simulación)' if config.get('dry_run') else '🗑️ EJECUCIÓN REAL'}")
        print(f"Directorios excluidos: {', '.join(config.get('excluded_dirs', []))}")
    
    if 'analysis' in results:
        analysis = results['analysis']
        print("\\n📊 ANÁLISIS DEL PROYECTO:")
        print(f"  • Archivos por categorías: {sum(analysis.get('categorized_files', {}).values())}")
        print(f"  • Grupos de duplicados: {analysis.get('duplicate_groups', 0)}")
        print(f"  • Archivos timestamped antiguos: {analysis.get('old_timestamped', 0)}")
        print(f"  • Archivos críticos protegidos: {analysis.get('critical_files_protected', 0)}")
        print(f"  • TOTAL A ELIMINAR: {analysis.get('total_files_to_remove', 0)}")
    
    if 'cleanup_results' in results:
        cleanup = results['cleanup_results']
        print("\\n🗑️ RESULTADOS DE LIMPIEZA:")
        print(f"  • Archivos eliminados: {cleanup.get('removed', 0)}")
        print(f"  • Archivos saltados: {cleanup.get('skipped', 0)}")
        print(f"  • Errores: {cleanup.get('errors', 0)}")
        print(f"  • Directorios vacíos eliminados: {cleanup.get('empty_dirs_removed', 0)}")
    
    print("\\n" + "=" * 80)

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description='Limpieza de archivos obsoletos del proyecto')
    parser.add_argument('--execute', action='store_true', 
                       help='Ejecutar limpieza real (por defecto es dry-run)')
    parser.add_argument('--days-old', type=int, default=7,
                       help='Días antiguos para considerar archivos obsoletos')
    parser.add_argument('--keep-latest', type=int, default=2,
                       help='Número de archivos más recientes a mantener')
    
    args = parser.parse_args()
    
    # Configurar limpieza del proyecto
    project_root = Path(__file__).parent.parent.parent
    backup_dir = project_root / "database_management" / "cleanup_backups"
    
    config = ProjectCleanupConfig(
        project_root=project_root,
        backup_dir=backup_dir,
        dry_run=not args.execute,
        days_old=args.days_old,
        keep_latest=args.keep_latest
    )
    
    # Ejecutar limpieza del proyecto
    results = run_project_cleanup(config)
    
    # Mostrar resumen
    print_project_cleanup_summary(results)
    
    # Guardar resultados
    results_file = config.backup_dir / f"project_cleanup_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    results_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Convertir Path a string para JSON
    results_serializable = json.loads(json.dumps(results, default=str))
    
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results_serializable, f, indent=2, ensure_ascii=False)
    
    logger.info(f"📄 Resultados guardados en: {results_file}")
    
    return results.get('success', False)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)