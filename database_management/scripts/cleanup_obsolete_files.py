#!/usr/bin/env python3
"""
Script de Limpieza de Archivos Obsoletos - Alcaldía de Santiago de Cali ETL
===========================================================================

Este script identifica y elimina archivos obsoletos, duplicados, residuales
e inutilizados del sistema ETL, siguiendo las mejores prácticas de
programación funcional.

Características:
- Identificación automática de archivos obsoletos
- Backup de archivos antes de eliminación  
- Logs detallados de las operaciones
- Modo dry-run para simulación
- Configuración flexible

Autor: Sistema ETL Alcaldía de Cali
Versión: 1.0.0
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
        logging.FileHandler('cleanup_obsolete_files.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ==========================================
# CONFIGURACIÓN DE LIMPIEZA
# ==========================================

@dataclass(frozen=True)
class CleanupConfig:
    """Configuración inmutable para limpieza"""
    project_root: Path
    backup_dir: Path
    dry_run: bool = True
    days_old: int = 7
    keep_latest: int = 3
    exclude_patterns: Tuple[str, ...] = (
        "latest_*",
        "__pycache__",
        "*.pyc",
        ".git*",
        "requirements.txt",
        "README.md"
    )

# Patrones de archivos obsoletos
OBSOLETE_PATTERNS = {
    # Archivos de test temporales
    'test_files': [
        'test_*.py',
        '*_test.py', 
        'demo_*.py',
        'check_*.py',
        'temp_*.py'
    ],
    
    # Logs antiguos
    'old_logs': [
        '*.log',
        'orchestrator_logs/*.log',
        '*.json.log'
    ],
    
    # Reportes antiguos
    'old_reports': [
        '*_report_*.json',
        'orchestrator_reports/*.json'
    ],
    
    # Archivos de cache
    'cache_files': [
        '__pycache__/**/*',
        '*.pyc',
        '*.pyo',
        '.pytest_cache/**/*'
    ],
    
    # Archivos duplicados con timestamp
    'timestamped_files': [
        '*_20[0-9][0-9][0-1][0-9][0-3][0-9]_[0-2][0-9][0-5][0-9][0-5][0-9].*'
    ]
}

# Archivos críticos que nunca se deben eliminar
CRITICAL_FILES = {
    'requirements.txt',
    'README.md', 
    '__init__.py',
    'main.py',
    'config.py',
    'setup.py',
    '.gitignore'
}

# ==========================================
# FUNCIONES DE ANÁLISIS
# ==========================================

def analyze_file_patterns(root_dir: Path) -> Dict[str, List[Path]]:
    """Analiza y categoriza archivos según patrones"""
    categorized_files = {category: [] for category in OBSOLETE_PATTERNS.keys()}
    
    for pattern_category, patterns in OBSOLETE_PATTERNS.items():
        for pattern in patterns:
            # Búsqueda recursiva con patrón
            if '**' in pattern:
                # Patrón recursivo
                pattern_path = pattern.replace('**/', '')
                for file_path in root_dir.rglob(pattern_path):
                    if file_path.is_file():
                        categorized_files[pattern_category].append(file_path)
            else:
                # Patrón simple
                for file_path in root_dir.rglob(pattern):
                    if file_path.is_file():
                        categorized_files[pattern_category].append(file_path)
    
    return categorized_files

def find_duplicate_files(root_dir: Path) -> Dict[str, List[Path]]:
    """Encuentra archivos duplicados basados en contenido y nombre"""
    file_groups = {}
    
    for file_path in root_dir.rglob('*'):
        if not file_path.is_file():
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

def find_old_timestamped_files(root_dir: Path, days_old: int, keep_latest: int) -> List[Path]:
    """Encuentra archivos con timestamp antiguos"""
    timestamp_pattern = re.compile(r'.*_(\d{8})_(\d{6})\..*')
    timestamped_files = {}
    
    for file_path in root_dir.rglob('*'):
        if not file_path.is_file():
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
    cutoff_date = datetime.now() - timedelta(days=days_old)
    
    for base_name, files in timestamped_files.items():
        # Ordenar por fecha (más reciente primero)
        files_sorted = sorted(files, key=lambda x: x[1], reverse=True)
        
        # Mantener los más recientes
        to_keep = files_sorted[:keep_latest]
        to_check = files_sorted[keep_latest:]
        
        # Eliminar archivos antiguos después del límite
        for file_path, file_date in to_check:
            if file_date < cutoff_date:
                files_to_remove.append(file_path)
    
    return files_to_remove

def find_unused_files(root_dir: Path) -> List[Path]:
    """Encuentra archivos que probablemente no se usan"""
    unused_files = []
    
    # Buscar archivos con patrones específicos de archivos no utilizados
    unused_patterns = [
        'demo_*.py',
        'test_*.py',
        'temp_*.py',
        'backup_*',
        '*_backup.*',
        '*_old.*',
        '*_deprecated.*',
        'unused_*'
    ]
    
    for pattern in unused_patterns:
        for file_path in root_dir.rglob(pattern):
            if (file_path.is_file() and 
                file_path.name not in CRITICAL_FILES and
                not any(exclude in str(file_path) for exclude in ['latest_', '__pycache__'])):
                unused_files.append(file_path)
    
    return unused_files

def identify_disconnected_files(root_dir: Path) -> List[Path]:
    """Identifica archivos desconectados del flujo principal"""
    disconnected = []
    
    # Archivos de configuración huérfanos
    config_files = list(root_dir.rglob('*.json'))
    
    # Verificar archivos de configuración que no se referencian
    main_configs = {'etl_config.json', 'etl_config_testing.json'}
    
    for config_file in config_files:
        if (config_file.name not in main_configs and
            'latest_' not in config_file.name and
            'report' not in config_file.name.lower() and
            config_file.stat().st_size < 1000):  # Archivos muy pequeños
            disconnected.append(config_file)
    
    return disconnected

# ==========================================
# FUNCIONES DE LIMPIEZA
# ==========================================

def backup_files(files_to_backup: List[Path], backup_dir: Path) -> bool:
    """Crea backup de archivos antes de eliminarlos"""
    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_subdir = backup_dir / f"cleanup_backup_{timestamp}"
        backup_subdir.mkdir()
        
        for file_path in files_to_backup:
            # Mantener estructura de directorios relativa
            relative_path = file_path.relative_to(file_path.anchor)
            backup_path = backup_subdir / relative_path
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            shutil.copy2(file_path, backup_path)
        
        logger.info(f"Backup creado en: {backup_subdir}")
        return True
        
    except Exception as e:
        logger.error(f"Error creando backup: {e}")
        return False

def safe_remove_files(files_to_remove: List[Path], dry_run: bool = True) -> Dict[str, int]:
    """Elimina archivos de manera segura"""
    results = {'removed': 0, 'skipped': 0, 'errors': 0}
    
    for file_path in files_to_remove:
        try:
            # Verificar que no es un archivo crítico
            if file_path.name in CRITICAL_FILES:
                logger.warning(f"Saltando archivo crítico: {file_path}")
                results['skipped'] += 1
                continue
            
            # Verificar que el archivo existe
            if not file_path.exists():
                results['skipped'] += 1
                continue
            
            if dry_run:
                logger.info(f"[DRY RUN] Eliminaría: {file_path}")
                results['removed'] += 1
            else:
                file_path.unlink()
                logger.info(f"Eliminado: {file_path}")
                results['removed'] += 1
                
        except Exception as e:
            logger.error(f"Error eliminando {file_path}: {e}")
            results['errors'] += 1
    
    return results

def cleanup_empty_directories(root_dir: Path, dry_run: bool = True) -> int:
    """Limpia directorios vacíos"""
    removed_count = 0
    
    # Buscar directorios vacíos (excluyendo .git y similares)
    for dir_path in root_dir.rglob('*'):
        if (dir_path.is_dir() and 
            not any(dir_path.iterdir()) and  # Directorio vacío
            '.git' not in str(dir_path) and
            '__pycache__' not in str(dir_path)):
            
            try:
                if dry_run:
                    logger.info(f"[DRY RUN] Eliminaría directorio vacío: {dir_path}")
                else:
                    dir_path.rmdir()
                    logger.info(f"Directorio vacío eliminado: {dir_path}")
                removed_count += 1
            except Exception as e:
                logger.warning(f"No se pudo eliminar directorio {dir_path}: {e}")
    
    return removed_count

# ==========================================
# FUNCIÓN PRINCIPAL
# ==========================================

def run_cleanup(config: CleanupConfig) -> Dict[str, any]:
    """Ejecuta el proceso completo de limpieza"""
    logger.info("🧹 Iniciando limpieza de archivos obsoletos")
    
    results = {
        'started_at': datetime.now().isoformat(),
        'config': {
            'project_root': str(config.project_root),
            'dry_run': config.dry_run,
            'days_old': config.days_old,
            'keep_latest': config.keep_latest
        },
        'analysis': {},
        'cleanup_results': {},
        'files_processed': 0
    }
    
    try:
        # 1. Análisis de archivos por patrones
        logger.info("📊 Analizando archivos por patrones...")
        categorized_files = analyze_file_patterns(config.project_root)
        
        # 2. Buscar duplicados
        logger.info("🔍 Buscando archivos duplicados...")
        duplicate_groups = find_duplicate_files(config.project_root)
        
        # 3. Buscar archivos con timestamp antiguos
        logger.info("📅 Buscando archivos timestamped antiguos...")
        old_timestamped = find_old_timestamped_files(
            config.project_root, 
            config.days_old, 
            config.keep_latest
        )
        
        # 4. Buscar archivos no utilizados
        logger.info("🔍 Buscando archivos no utilizados...")
        unused_files = find_unused_files(config.project_root)
        
        # 5. Buscar archivos desconectados
        logger.info("🔗 Buscando archivos desconectados...")
        disconnected_files = identify_disconnected_files(config.project_root)
        
        # Consolidar archivos a eliminar
        all_files_to_remove = set()
        
        # Agregar archivos de patrones específicos
        for category, files in categorized_files.items():
            if category in ['old_logs', 'old_reports', 'cache_files']:
                all_files_to_remove.update(files)
        
        # Agregar duplicados (mantener solo el más reciente)
        for group_name, files in duplicate_groups.items():
            if len(files) > 1:
                # Ordenar por fecha de modificación, mantener el más reciente
                files_sorted = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)
                all_files_to_remove.update(files_sorted[config.keep_latest:])
        
        all_files_to_remove.update(old_timestamped)
        all_files_to_remove.update(unused_files)
        all_files_to_remove.update(disconnected_files)
        
        # Convertir a lista
        files_to_remove = list(all_files_to_remove)
        
        # Guardar análisis
        results['analysis'] = {
            'categorized_files': {k: len(v) for k, v in categorized_files.items()},
            'duplicate_groups': len(duplicate_groups),
            'old_timestamped': len(old_timestamped),
            'unused_files': len(unused_files),
            'disconnected_files': len(disconnected_files),
            'total_files_to_remove': len(files_to_remove)
        }
        
        logger.info(f"📋 Archivos identificados para eliminación: {len(files_to_remove)}")
        
        # 6. Crear backup si no es dry run
        if not config.dry_run and files_to_remove:
            logger.info("💾 Creando backup de archivos...")
            if not backup_files(files_to_remove, config.backup_dir):
                logger.error("❌ Falló el backup. Abortando limpieza.")
                results['success'] = False
                return results
        
        # 7. Eliminar archivos
        logger.info("🗑️ Eliminando archivos...")
        cleanup_results = safe_remove_files(files_to_remove, config.dry_run)
        
        # 8. Limpiar directorios vacíos
        logger.info("📁 Limpiando directorios vacíos...")
        empty_dirs_removed = cleanup_empty_directories(config.project_root, config.dry_run)
        
        # Guardar resultados
        results['cleanup_results'] = cleanup_results
        results['cleanup_results']['empty_dirs_removed'] = empty_dirs_removed
        results['files_processed'] = len(files_to_remove)
        results['success'] = True
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Error durante la limpieza: {e}")
        results['success'] = False
        results['error'] = str(e)
        return results

def print_cleanup_summary(results: Dict[str, any]) -> None:
    """Imprime resumen de la limpieza"""
    print("\\n" + "=" * 80)
    print("🧹 LIMPIEZA DE ARCHIVOS OBSOLETOS - ALCALDÍA DE SANTIAGO DE CALI ETL")
    print("=" * 80)
    
    print(f"Iniciado: {results.get('started_at', 'N/A')}")
    print(f"Estado: {'✅ EXITOSO' if results.get('success') else '❌ CON ERRORES'}")
    
    if 'config' in results:
        config = results['config']
        print(f"Modo: {'🔍 DRY RUN (Simulación)' if config.get('dry_run') else '🗑️ EJECUCIÓN REAL'}")
    
    if 'analysis' in results:
        analysis = results['analysis']
        print("\\n📊 ANÁLISIS:")
        print(f"  • Archivos por categorías: {sum(analysis.get('categorized_files', {}).values())}")
        print(f"  • Grupos de duplicados: {analysis.get('duplicate_groups', 0)}")
        print(f"  • Archivos timestamped antiguos: {analysis.get('old_timestamped', 0)}")
        print(f"  • Archivos no utilizados: {analysis.get('unused_files', 0)}")
        print(f"  • Archivos desconectados: {analysis.get('disconnected_files', 0)}")
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
    parser = argparse.ArgumentParser(description='Limpieza de archivos obsoletos')
    parser.add_argument('--execute', action='store_true', 
                       help='Ejecutar limpieza real (por defecto es dry-run)')
    parser.add_argument('--days-old', type=int, default=7,
                       help='Días antiguos para considerar archivos obsoletos')
    parser.add_argument('--keep-latest', type=int, default=3,
                       help='Número de archivos más recientes a mantener')
    
    args = parser.parse_args()
    
    # Configurar limpieza
    project_root = Path(__file__).parent.parent.parent
    backup_dir = project_root / "database_management" / "cleanup_backups"
    
    config = CleanupConfig(
        project_root=project_root,
        backup_dir=backup_dir,
        dry_run=not args.execute,
        days_old=args.days_old,
        keep_latest=args.keep_latest
    )
    
    # Ejecutar limpieza
    results = run_cleanup(config)
    
    # Mostrar resumen
    print_cleanup_summary(results)
    
    # Guardar resultados
    results_file = config.backup_dir / f"cleanup_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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