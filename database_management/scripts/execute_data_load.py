#!/usr/bin/env python3
"""
Script de Carga de Datos - Alcaldía de Santiago de Cali ETL
==========================================================

Este script ejecuta la carga completa de datos al warehouse:
1. Configura la base de datos
2. Crea tablas
3. Carga datos desde archivos JSON
4. Crea índices y vistas analíticas

Autor: Sistema ETL Alcaldía de Cali
Versión: 1.0.0
"""

import json
import psycopg2
import psycopg2.extras
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import sys
import os

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

from database_management.core.config import get_database_config

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_load_execution.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def execute_sql_file(cursor, sql_file_path: Path, description: str) -> bool:
    """Ejecuta un archivo SQL completo"""
    try:
        logger.info(f"🔧 {description}...")
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Limpiar y ejecutar SQL
        if sql_content.strip():
            cursor.execute(sql_content)
            logger.info(f"✅ {description} completado")
            return True
        else:
            logger.warning(f"⚠️  Archivo SQL vacío: {sql_file_path}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error en {description}: {e}")
        return False

def create_table_manually(cursor, table_name: str, schema: Dict[str, Any]) -> bool:
    """Crea una tabla manualmente basada en el esquema analizado"""
    try:
        logger.info(f"📊 Creando tabla: {table_name}")
        
        # Campos básicos del warehouse
        columns = [
            "id UUID PRIMARY KEY DEFAULT gen_random_uuid()",
            "created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL",
            "updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL",
            "version INTEGER DEFAULT 1 NOT NULL",
            "is_active BOOLEAN DEFAULT true NOT NULL"
        ]
        
        # Mapeo de tipos mejorado
        type_mapping = {
            'string': 'TEXT',
            'integer': 'BIGINT',
            'float': 'DOUBLE PRECISION',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'datetime': 'TIMESTAMP WITH TIME ZONE',
            'timestamp': 'TIMESTAMP WITH TIME ZONE'
        }
        
        # Procesar campos del esquema
        for field in schema.get('fields', []):
            field_name = field['name']
            field_data_type = field.get('data_type', field.get('type', 'string'))
            is_nullable = field.get('nullable', True)
            max_length = field.get('max_length')
            
            # Mapear tipo
            if field_data_type == 'varchar' and max_length and max_length <= 255:
                sql_type = f"VARCHAR({max_length})"
            elif field_data_type in ['varchar', 'string']:
                sql_type = "TEXT"
            elif field_data_type in ['int', 'integer']:
                sql_type = "BIGINT"
            elif field_data_type in ['float', 'double', 'numeric']:
                sql_type = "DOUBLE PRECISION"
            elif field_data_type == 'boolean':
                sql_type = "BOOLEAN"
            elif field_data_type == 'date':
                sql_type = "DATE"
            elif field_data_type in ['datetime', 'timestamp']:
                sql_type = "TIMESTAMP WITH TIME ZONE"
            else:
                sql_type = "TEXT"
            
            # Construir definición de columna
            null_constraint = "" if is_nullable else " NOT NULL"
            column_def = f"{field_name} {sql_type}{null_constraint}"
            columns.append(column_def)
        
        # Crear SQL de tabla
        columns_sql = ",\n    ".join(columns)
        create_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    {columns_sql}
);
"""
        
        cursor.execute(create_sql)
        logger.info(f"✅ Tabla {table_name} creada exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creando tabla {table_name}: {e}")
        return False

def load_json_data_to_table(cursor, table_name: str, json_file_path: Path) -> int:
    """Carga datos desde archivo JSON a la tabla"""
    try:
        logger.info(f"📁 Cargando datos a {table_name} desde {json_file_path.name}")
        
        # Leer datos JSON
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list) or len(data) == 0:
            logger.warning(f"⚠️  No hay datos válidos en {json_file_path}")
            return 0
        
        # Obtener columnas de la tabla (excluyendo las automáticas)
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND column_name NOT IN ('id', 'created_at', 'updated_at', 'version', 'is_active')
            ORDER BY ordinal_position;
        """)
        
        db_columns = [row[0] for row in cursor.fetchall()]
        
        if not db_columns:
            logger.warning(f"⚠️  No se encontraron columnas para {table_name}")
            return 0
        
        # Preparar datos para inserción
        rows_to_insert = []
        for record in data:
            if isinstance(record, dict):
                row_values = []
                for col in db_columns:
                    value = record.get(col)
                    # Convertir valores None y strings vacías
                    if value == "" or value is None:
                        row_values.append(None)
                    else:
                        row_values.append(value)
                rows_to_insert.append(tuple(row_values))
        
        if not rows_to_insert:
            logger.warning(f"⚠️  No hay filas válidas para insertar en {table_name}")
            return 0
        
        # Crear SQL de inserción
        placeholders = ", ".join(["%s"] * len(db_columns))
        columns_list = ", ".join(db_columns)
        insert_sql = f"""
            INSERT INTO {table_name} ({columns_list})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING;
        """
        
        # Ejecutar inserción en lotes
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            total_inserted += len(batch)
            
            if total_inserted % 5000 == 0:
                logger.info(f"  📊 Insertadas {total_inserted} filas en {table_name}")
        
        logger.info(f"✅ Carga completada: {total_inserted} filas en {table_name}")
        return total_inserted
        
    except Exception as e:
        logger.error(f"❌ Error cargando datos en {table_name}: {e}")
        return 0

def execute_data_load():
    """Ejecuta la carga completa de datos"""
    start_time = datetime.now()
    logger.info("🚀 Iniciando carga de datos del warehouse")
    
    try:
        # Obtener configuración
        config = get_database_config()
        
        # Conexión a la base de datos
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # 1. Ejecutar setup del warehouse
        setup_file = Path('database_management/generated_sql/01_warehouse_setup.sql')
        if setup_file.exists():
            execute_sql_file(cursor, setup_file, "Configuración del warehouse")
        
        # 2. Cargar esquemas analizados
        schemas_file = Path('database_management/generated_schemas').glob('unified_schemas_*.json')
        latest_schemas_file = max(schemas_file, key=lambda x: x.stat().st_mtime)
        
        with open(latest_schemas_file, 'r', encoding='utf-8') as f:
            schemas_data = json.load(f)
        
        schemas = schemas_data.get('schemas', [])
        if isinstance(schemas, list):
            # Convertir lista a diccionario usando el nombre como clave
            schemas_dict = {schema['name']: schema for schema in schemas}
        else:
            schemas_dict = schemas
            
        logger.info(f"📋 Esquemas cargados: {len(schemas_dict)} tablas")
        
        # 3. Crear tablas y cargar datos
        total_records = 0
        successful_tables = 0
        
        # Buscar archivos JSON en transformation_app/app_outputs
        json_files_dir = Path('transformation_app/app_outputs')
        
        for schema_name, schema_info in schemas_dict.items():
            # Buscar archivo JSON correspondiente
            json_files = list(json_files_dir.rglob(f"{schema_name}.json"))
            
            if not json_files:
                logger.warning(f"⚠️  No se encontró archivo JSON para {schema_name}")
                continue
            
            json_file = json_files[0]  # Tomar el primero si hay múltiples
            
            # Crear tabla
            if create_table_manually(cursor, schema_name, schema_info):
                # Cargar datos
                records_loaded = load_json_data_to_table(cursor, schema_name, json_file)
                total_records += records_loaded
                
                if records_loaded > 0:
                    successful_tables += 1
        
        # 4. Crear índices
        indexes_file = Path('database_management/generated_sql/03_create_indexes.sql')
        if indexes_file.exists():
            execute_sql_file(cursor, indexes_file, "Creación de índices")
        
        # 5. Crear vistas analíticas
        views_file = Path('database_management/generated_sql/04_analytics_views.sql')
        if views_file.exists():
            execute_sql_file(cursor, views_file, "Creación de vistas analíticas")
        
        # Cerrar conexión
        cursor.close()
        conn.close()
        
        # Resumen final
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("\\n" + "="*80)
        logger.info("🎉 CARGA DE DATOS COMPLETADA")
        logger.info("="*80)
        logger.info(f"⏱️  Duración: {duration}")
        logger.info(f"📊 Tablas procesadas: {successful_tables}")
        logger.info(f"📈 Total de registros: {total_records:,}")
        logger.info("="*80)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error durante la carga de datos: {e}")
        return False

def main():
    """Función principal"""
    success = execute_data_load()
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())