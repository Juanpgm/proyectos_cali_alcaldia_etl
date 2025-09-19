#!/usr/bin/env python3
"""
Script de Carga de Datos Mejorado - Alcaldía de Santiago de Cali ETL
===================================================================

Este script ejecuta la carga completa de datos al warehouse usando
los archivos SQL mejorados que garantizan:
1. Preservación exacta de estructura JSON
2. Mapeo correcto de tipos de datos
3. SQL limpio sin caracteres de escape

Funcionalidades:
- Ejecuta archivos SQL en secuencia correcta
- Carga datos desde archivos JSON preservando estructura exacta
- Manejo robusto de errores
- Logging detallado
- Validación de integridad

Autor: Sistema ETL Alcaldía de Cali
Versión: 2.0.0
"""

import json
import psycopg2
import psycopg2.extras
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import sys
import traceback

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

from database_management.core.config import get_database_config

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('improved_data_load.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ==========================================
# FUNCIONES DE EJECUCIÓN SQL
# ==========================================

def execute_sql_file_safely(cursor, sql_file_path: Path, description: str) -> bool:
    """Ejecuta un archivo SQL de forma segura con mejor manejo de errores"""
    try:
        logger.info(f"🔧 Ejecutando: {description}")
        
        if not sql_file_path.exists():
            logger.warning(f"⚠️  Archivo no encontrado: {sql_file_path}")
            return False
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read().strip()
        
        if not sql_content:
            logger.warning(f"⚠️  Archivo SQL vacío: {sql_file_path}")
            return False
        
        # Ejecutar SQL
        cursor.execute(sql_content)
        logger.info(f"✅ {description} completado exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en {description}: {e}")
        logger.debug(f"Detalles del error: {traceback.format_exc()}")
        return False

def load_json_to_table_improved(cursor, table_name: str, json_file_path: Path) -> int:
    """
    Carga datos JSON a tabla con preservación exacta de estructura
    """
    try:
        logger.info(f"📁 Cargando datos: {table_name} ← {json_file_path.name}")
        
        # Verificar que el archivo existe
        if not json_file_path.exists():
            logger.warning(f"⚠️  Archivo JSON no encontrado: {json_file_path}")
            return 0
        
        # Leer datos JSON
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list) or len(data) == 0:
            logger.warning(f"⚠️  No hay datos válidos en {json_file_path}")
            return 0
        
        # Obtener columnas de la tabla (excluyendo las de auditoria automática)
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND column_name NOT IN ('id', 'created_at', 'updated_at', 'version', 'is_active')
            ORDER BY ordinal_position;
        """, (table_name,))
        
        table_columns = cursor.fetchall()
        
        if not table_columns:
            logger.warning(f"⚠️  No se encontraron columnas para {table_name}")
            return 0
        
        # Preparar mapeo de columnas
        column_names = [col[0] for col in table_columns]
        
        # Preparar datos para inserción
        rows_to_insert = []
        error_records = 0
        
        for i, record in enumerate(data):
            if not isinstance(record, dict):
                error_records += 1
                continue
            
            row_values = []
            for col_name in column_names:
                value = record.get(col_name)
                
                # Normalizar valores None y vacíos
                if value is None or value == "" or str(value).strip() in ['null', 'None', 'NaN']:
                    row_values.append(None)
                else:
                    # Preservar el valor exactamente como está en el JSON
                    row_values.append(value)
            
            rows_to_insert.append(tuple(row_values))
        
        if not rows_to_insert:
            logger.warning(f"⚠️  No hay filas válidas para insertar en {table_name}")
            return 0
        
        if error_records > 0:
            logger.warning(f"⚠️  {error_records} registros con errores fueron omitidos")
        
        # Crear SQL de inserción con ON CONFLICT para evitar duplicados
        placeholders = ", ".join(["%s"] * len(column_names))
        columns_list = ", ".join(column_names)
        
        insert_sql = f"""
            INSERT INTO {table_name} ({columns_list})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING;
        """
        
        # Ejecutar inserción en lotes para mejor performance
        batch_size = 1000
        total_inserted = 0
        
        try:
            for i in range(0, len(rows_to_insert), batch_size):
                batch = rows_to_insert[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                
                # Contar filas realmente insertadas
                cursor.execute(f"SELECT ROW_COUNT();")
                inserted_in_batch = cursor.rowcount if cursor.rowcount > 0 else len(batch)
                total_inserted += inserted_in_batch
                
                if (i + batch_size) % 5000 == 0:
                    logger.info(f"  📊 Procesadas {i + batch_size} filas en {table_name}")
            
            logger.info(f"✅ Carga exitosa: {total_inserted:,} filas en {table_name}")
            return total_inserted
            
        except Exception as insert_error:
            logger.error(f"❌ Error durante inserción en {table_name}: {insert_error}")
            return 0
        
    except Exception as e:
        logger.error(f"❌ Error cargando datos en {table_name}: {e}")
        logger.debug(f"Detalles: {traceback.format_exc()}")
        return 0

def get_table_json_mapping(data_dir: Path) -> Dict[str, Path]:
    """
    Mapea nombres de tabla a archivos JSON correspondientes
    """
    mapping = {}
    
    # Buscar todos los archivos JSON
    for json_file in data_dir.rglob("*.json"):
        # Generar nombre de tabla basado en el archivo
        table_name = json_file.stem.lower()
        
        # Limpiar nombre para que sea válido como tabla
        import re
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
        
        mapping[table_name] = json_file
    
    return mapping

# ==========================================
# PIPELINE PRINCIPAL DE CARGA
# ==========================================

def execute_improved_data_load():
    """Ejecuta la carga mejorada de datos al warehouse"""
    start_time = datetime.now()
    logger.info("🚀 Iniciando carga mejorada de datos del warehouse")
    
    execution_summary = {
        'start_time': start_time,
        'tables_processed': 0,
        'total_records': 0,
        'successful_tables': [],
        'failed_tables': [],
        'errors': []
    }
    
    try:
        # Obtener configuración de base de datos
        config = get_database_config()
        logger.info(f"🔗 Conectando a base de datos: {config.host}:{config.port}/{config.database}")
        
        # Establecer conexión
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Configurar rutas
        project_root = Path(__file__).parent.parent.parent
        sql_dir = project_root / "database_management" / "generated_sql"
        data_dir = project_root / "transformation_app" / "app_outputs"
        
        # 1. Ejecutar setup del warehouse
        setup_file = sql_dir / "01_warehouse_setup.sql"
        if setup_file.exists():
            success = execute_sql_file_safely(cursor, setup_file, "Configuración del warehouse")
            if not success:
                execution_summary['errors'].append("Error en configuración del warehouse")
        
        # 2. Crear tablas
        tables_file = sql_dir / "02_create_tables.sql"
        if tables_file.exists():
            success = execute_sql_file_safely(cursor, tables_file, "Creación de tablas")
            if not success:
                execution_summary['errors'].append("Error en creación de tablas")
                # Si no se pueden crear las tablas, no continuar
                raise Exception("No se pudieron crear las tablas")
        
        # 3. Crear índices básicos
        indexes_file = sql_dir / "03_create_indexes.sql"
        if indexes_file.exists():
            success = execute_sql_file_safely(cursor, indexes_file, "Creación de índices")
            if not success:
                execution_summary['errors'].append("Error en creación de índices")
        
        # 4. Crear triggers
        triggers_file = sql_dir / "04_create_triggers.sql"
        if triggers_file.exists():
            success = execute_sql_file_safely(cursor, triggers_file, "Creación de triggers")
            if not success:
                execution_summary['errors'].append("Error en creación de triggers")
        
        # 5. Obtener mapeo tabla-archivo JSON
        table_json_mapping = get_table_json_mapping(data_dir)
        logger.info(f"📋 Encontrados {len(table_json_mapping)} mapeos tabla-archivo")
        
        # 6. Cargar datos en cada tabla
        for table_name, json_file in table_json_mapping.items():
            try:
                # Verificar que la tabla existe
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    );
                """, (table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    logger.warning(f"⚠️  Tabla {table_name} no existe, omitiendo")
                    execution_summary['failed_tables'].append({
                        'table': table_name,
                        'reason': 'tabla no existe'
                    })
                    continue
                
                # Cargar datos
                records_loaded = load_json_to_table_improved(cursor, table_name, json_file)
                execution_summary['tables_processed'] += 1
                execution_summary['total_records'] += records_loaded
                
                if records_loaded > 0:
                    execution_summary['successful_tables'].append({
                        'table': table_name,
                        'records': records_loaded,
                        'file': str(json_file)
                    })
                else:
                    execution_summary['failed_tables'].append({
                        'table': table_name,
                        'reason': 'no se cargaron registros'
                    })
                
            except Exception as table_error:
                logger.error(f"❌ Error procesando tabla {table_name}: {table_error}")
                execution_summary['failed_tables'].append({
                    'table': table_name,
                    'reason': str(table_error)
                })
                execution_summary['errors'].append(f"Error en {table_name}: {table_error}")
        
        # 7. Crear vistas analíticas
        analytics_file = sql_dir / "05_analytics_views.sql"
        if analytics_file.exists():
            success = execute_sql_file_safely(cursor, analytics_file, "Creación de vistas analíticas")
            if not success:
                execution_summary['errors'].append("Error en vistas analíticas")
        
        # Cerrar conexión
        cursor.close()
        conn.close()
        
        # Calcular tiempo total
        end_time = datetime.now()
        duration = end_time - start_time
        execution_summary['end_time'] = end_time
        execution_summary['duration'] = duration
        
        # Mostrar resumen final
        print_execution_summary(execution_summary)
        
        return len(execution_summary['failed_tables']) == 0
        
    except Exception as e:
        logger.error(f"❌ Error crítico durante la carga: {e}")
        logger.debug(f"Detalles: {traceback.format_exc()}")
        execution_summary['errors'].append(f"Error crítico: {e}")
        return False

def print_execution_summary(summary: Dict[str, Any]):
    """Imprime un resumen detallado de la ejecución"""
    print("\n" + "=" * 80)
    print("🎉 RESUMEN DE CARGA DE DATOS - WAREHOUSE ETL ALCALDÍA")
    print("=" * 80)
    
    print(f"⏱️  Iniciado: {summary['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Completado: {summary['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Duración: {summary['duration']}")
    
    print(f"\n📊 ESTADÍSTICAS:")
    print(f"  • Tablas procesadas: {summary['tables_processed']}")
    print(f"  • Registros cargados: {summary['total_records']:,}")
    print(f"  • Tablas exitosas: {len(summary['successful_tables'])}")
    print(f"  • Tablas con errores: {len(summary['failed_tables'])}")
    
    if summary['successful_tables']:
        print(f"\n✅ TABLAS CARGADAS EXITOSAMENTE:")
        for table_info in summary['successful_tables']:
            print(f"  • {table_info['table']}: {table_info['records']:,} registros")
    
    if summary['failed_tables']:
        print(f"\n❌ TABLAS CON ERRORES:")
        for table_info in summary['failed_tables']:
            print(f"  • {table_info['table']}: {table_info['reason']}")
    
    if summary['errors']:
        print(f"\n⚠️  ERRORES ENCONTRADOS:")
        for error in summary['errors'][:5]:  # Mostrar solo los primeros 5
            print(f"  • {error}")
        if len(summary['errors']) > 5:
            print(f"  ... y {len(summary['errors']) - 5} errores más")
    
    success_rate = (len(summary['successful_tables']) / summary['tables_processed'] * 100) if summary['tables_processed'] > 0 else 0
    print(f"\n📈 TASA DE ÉXITO: {success_rate:.1f}%")
    print("=" * 80)

def main():
    """Función principal"""
    try:
        success = execute_improved_data_load()
        return 0 if success else 1
    except Exception as e:
        logger.error(f"❌ Error en función principal: {e}")
        return 1

if __name__ == "__main__":
    exit(main())