#!/usr/bin/env python3
"""
Generador de SQL Mejorado - Alcaldía de Santiago de Cali ETL
==========================================================

Generador corregido que soluciona los problemas de:
1. Caracteres de escape en SQL
2. Mapeo incorrecto de tipos de datos JSON a PostgreSQL
3. Preservación exacta de la estructura de datos JSON

Características:
- Generación de SQL limpio sin caracteres de escape
- Mapeo inteligente de tipos basado en análisis real de datos JSON
- Preservación exacta de estructura de datos
- Compatibilidad con PostgreSQL 17.6

Autor: Sistema ETL Alcaldía de Cali
Versión: 2.0.0
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import logging
import re

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# ==========================================
# ANÁLISIS INTELIGENTE DE TIPOS
# ==========================================

def analyze_json_field_type(field_name: str, sample_values: List[Any]) -> str:
    """
    Analiza el tipo real de un campo basado en valores de muestra del JSON
    """
    if not sample_values:
        return 'TEXT'
    
    # Filtrar valores no nulos
    valid_values = [v for v in sample_values if v is not None and str(v).strip() not in ['', 'null', 'None']]
    
    if not valid_values:
        return 'TEXT'
    
    # Analizar primer valor válido como muestra
    sample_value = valid_values[0]
    
    # Detección de tipos específicos
    if isinstance(sample_value, bool):
        return 'BOOLEAN'
    
    if isinstance(sample_value, int):
        # Verificar si es timestamp (muchos dígitos)
        if sample_value > 1000000000:  # Timestamp probable
            return 'BIGINT'
        return 'INTEGER'
    
    if isinstance(sample_value, float):
        return 'DOUBLE PRECISION'
    
    if isinstance(sample_value, (list, dict)):
        return 'JSONB'
    
    # Para strings, análisis más detallado
    str_value = str(sample_value).strip()
    
    # Detección de fechas
    if re.match(r'^\d{4}-\d{2}-\d{2}$', str_value):
        return 'DATE'
    
    # Detección de timestamps/datetime
    if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', str_value):
        return 'TIMESTAMP WITH TIME ZONE'
    
    if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', str_value):
        return 'TIMESTAMP WITH TIME ZONE'
    
    # Números como string
    if str_value.replace('.', '').replace('-', '').isdigit():
        if '.' in str_value:
            return 'DOUBLE PRECISION'
        else:
            # Verificar si es un ID largo
            if len(str_value) > 10:
                return 'BIGINT'
            return 'INTEGER'
    
    # Booleanos como string
    if str_value.lower() in ['true', 'false', 'yes', 'no', 'si', 'no', '1', '0']:
        return 'BOOLEAN'
    
    # URLs
    if str_value.startswith(('http://', 'https://', 'www.')):
        return 'TEXT'
    
    # Texto largo vs VARCHAR
    max_length = max(len(str(v)) for v in valid_values if v is not None)
    
    if max_length <= 255:
        return f'VARCHAR({max_length})'
    else:
        return 'TEXT'

def load_json_sample_data(json_file_path: Path, sample_size: int = 100) -> Dict[str, List[Any]]:
    """
    Carga una muestra de datos JSON para análisis de tipos
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list) or not data:
            return {}
        
        # Tomar muestra
        sample_data = data[:sample_size]
        
        # Extraer valores por campo
        field_samples = {}
        for record in sample_data:
            if isinstance(record, dict):
                for field_name, value in record.items():
                    if field_name not in field_samples:
                        field_samples[field_name] = []
                    field_samples[field_name].append(value)
        
        return field_samples
        
    except Exception as e:
        logger.error(f"Error cargando datos de muestra desde {json_file_path}: {e}")
        return {}

# ==========================================
# GENERACIÓN DE SQL LIMPIO
# ==========================================

def sanitize_column_name(column_name: str) -> str:
    """Sanitiza nombres de columna para PostgreSQL"""
    # Reemplazar caracteres especiales con underscore
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', str(column_name))
    
    # Asegurar que no empiece con número
    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    
    # Convertir a minúsculas
    sanitized = sanitized.lower()
    
    # Evitar palabras reservadas de PostgreSQL
    reserved_words = {
        'order', 'group', 'where', 'select', 'from', 'insert', 'update', 
        'delete', 'create', 'drop', 'alter', 'table', 'index', 'view',
        'schema', 'database', 'user', 'role', 'grant', 'revoke', 'commit',
        'rollback', 'transaction', 'begin', 'end', 'case', 'when', 'then',
        'else', 'and', 'or', 'not', 'null', 'true', 'false', 'union',
        'join', 'left', 'right', 'inner', 'outer', 'full', 'on', 'using'
    }
    
    if sanitized in reserved_words:
        sanitized = f"{sanitized}_field"
    
    return sanitized

def generate_create_table_sql(table_name: str, json_file_path: Path) -> str:
    """
    Genera SQL CREATE TABLE basado directamente en los datos JSON
    """
    logger.info(f"Generando SQL para tabla: {table_name}")
    
    # Cargar muestra de datos
    field_samples = load_json_sample_data(json_file_path)
    
    if not field_samples:
        raise ValueError(f"No se pudieron cargar datos de muestra desde {json_file_path}")
    
    # Generar columnas
    columns = []
    
    # Campos base de auditoria
    columns.extend([
        "    id UUID PRIMARY KEY DEFAULT gen_random_uuid()",
        "    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL",
        "    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL",
        "    version INTEGER DEFAULT 1 NOT NULL",
        "    is_active BOOLEAN DEFAULT true NOT NULL"
    ])
    
    # Procesar campos del JSON
    for field_name, sample_values in field_samples.items():
        if field_name.lower() in ['id', 'created_at', 'updated_at', 'version', 'is_active']:
            continue
        
        clean_field_name = sanitize_column_name(field_name)
        sql_type = analyze_json_field_type(field_name, sample_values)
        
        # Verificar nulabilidad
        null_count = sum(1 for v in sample_values if v is None or str(v).strip() in ['', 'null', 'None'])
        is_nullable = null_count > 0
        
        # Construir definición de columna
        if is_nullable:
            column_def = f"    {clean_field_name} {sql_type}"
        else:
            column_def = f"    {clean_field_name} {sql_type} NOT NULL"
        
        columns.append(column_def)
    
    # Construir SQL final
    columns_formatted = ',\n'.join(columns)
    table_sql = f"""-- Tabla: {table_name}
-- Generado automáticamente desde: {json_file_path.name}
-- Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

CREATE TABLE IF NOT EXISTS {table_name} (
{columns_formatted}
);"""
    
    return table_sql

def generate_warehouse_setup_sql() -> str:
    """Genera SQL de configuración inicial del warehouse"""
    return """-- Configuración inicial del Data Warehouse
-- Alcaldía de Santiago de Cali ETL

-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Configuración de zona horaria
SET timezone = 'America/Bogota';
SET datestyle = 'ISO, YMD';

-- Función para actualizar timestamps automáticamente
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    NEW.version = OLD.version + 1;
    RETURN NEW;
END;
$$ language 'plpgsql';"""

def generate_indexes_sql(table_name: str, json_file_path: Path) -> str:
    """Genera índices básicos para una tabla"""
    field_samples = load_json_sample_data(json_file_path)
    
    indexes = []
    
    # Índices básicos de auditoria
    indexes.extend([
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_created_at ON {table_name} (created_at);",
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_updated_at ON {table_name} (updated_at);",
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_active ON {table_name} (is_active) WHERE is_active = true;"
    ])
    
    # Índices para campos comunes de búsqueda
    search_patterns = ['bpin', 'codigo', 'id_', 'numero', 'referencia', 'estado']
    
    for field_name in field_samples.keys():
        clean_field_name = sanitize_column_name(field_name)
        
        # Crear índice si el campo parece ser de búsqueda
        if any(pattern in field_name.lower() for pattern in search_patterns):
            index_name = f"idx_{table_name}_{clean_field_name}"
            indexes.append(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({clean_field_name});")
    
    return '\n'.join(indexes)

def generate_triggers_sql(table_name: str) -> str:
    """Genera triggers para una tabla"""
    return f"""-- Trigger para actualizar timestamp en {table_name}
CREATE TRIGGER update_{table_name}_modtime
    BEFORE UPDATE ON {table_name}
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();"""

# ==========================================
# PIPELINE PRINCIPAL
# ==========================================

def discover_json_files(data_dir: Path) -> List[tuple]:
    """
    Descubre archivos JSON y sugiere nombres de tabla
    """
    json_files = []
    
    for json_file in data_dir.rglob("*.json"):
        # Generar nombre de tabla basado en el nombre del archivo
        table_name = json_file.stem.lower()
        
        # Limpiar nombre de tabla
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
        
        json_files.append((table_name, json_file))
    
    return json_files

def generate_improved_sql_warehouse():
    """
    Función principal para generar el warehouse SQL mejorado
    """
    logger.info("🚀 Iniciando generación mejorada de SQL warehouse")
    
    # Configurar rutas
    project_root = Path(__file__).parent.parent.parent
    data_dir = project_root / "transformation_app" / "app_outputs"
    output_dir = project_root / "database_management" / "generated_sql"
    
    # Crear directorio de salida
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Descubrir archivos JSON
    json_files = discover_json_files(data_dir)
    
    if not json_files:
        raise ValueError(f"No se encontraron archivos JSON en {data_dir}")
    
    logger.info(f"📁 Encontrados {len(json_files)} archivos JSON")
    
    # Generar archivos SQL separados
    
    # 1. Setup del warehouse
    setup_sql = generate_warehouse_setup_sql()
    setup_file = output_dir / "01_warehouse_setup.sql"
    setup_file.write_text(setup_sql, encoding='utf-8')
    logger.info(f"✅ Generado: {setup_file}")
    
    # 2. Crear tablas
    tables_sql_parts = []
    for table_name, json_file in json_files:
        try:
            table_sql = generate_create_table_sql(table_name, json_file)
            tables_sql_parts.append(table_sql)
            logger.info(f"✅ SQL generado para tabla: {table_name}")
        except Exception as e:
            logger.error(f"❌ Error generando SQL para {table_name}: {e}")
            continue
    
    tables_sql = '\n\n'.join(tables_sql_parts)
    tables_file = output_dir / "02_create_tables.sql"
    tables_file.write_text(tables_sql, encoding='utf-8')
    logger.info(f"✅ Generado: {tables_file}")
    
    # 3. Crear índices
    indexes_sql_parts = []
    for table_name, json_file in json_files:
        try:
            indexes_sql = generate_indexes_sql(table_name, json_file)
            indexes_sql_parts.append(f"-- Índices para {table_name}")
            indexes_sql_parts.append(indexes_sql)
            indexes_sql_parts.append("")
        except Exception as e:
            logger.error(f"❌ Error generando índices para {table_name}: {e}")
            continue
    
    all_indexes_sql = '\n'.join(indexes_sql_parts)
    indexes_file = output_dir / "03_create_indexes.sql"
    indexes_file.write_text(all_indexes_sql, encoding='utf-8')
    logger.info(f"✅ Generado: {indexes_file}")
    
    # 4. Crear triggers
    triggers_sql_parts = []
    for table_name, json_file in json_files:
        triggers_sql = generate_triggers_sql(table_name)
        triggers_sql_parts.append(triggers_sql)
        triggers_sql_parts.append("")
    
    all_triggers_sql = '\n'.join(triggers_sql_parts)
    triggers_file = output_dir / "04_create_triggers.sql"
    triggers_file.write_text(all_triggers_sql, encoding='utf-8')
    logger.info(f"✅ Generado: {triggers_file}")
    
    # 5. Vistas analíticas básicas
    analytics_sql = f"""-- Vistas analíticas básicas
-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

-- Vista resumen de tablas
CREATE OR REPLACE VIEW analytics_table_summary AS
SELECT 
    schemaname,
    tablename,
    n_tup_ins as total_insertions,
    n_tup_upd as total_updates,
    n_tup_del as total_deletions
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY tablename;"""
    
    analytics_file = output_dir / "05_analytics_views.sql"
    analytics_file.write_text(analytics_sql, encoding='utf-8')
    logger.info(f"✅ Generado: {analytics_file}")
    
    # Resumen final
    logger.info("🎉 Generación de SQL warehouse completada exitosamente")
    logger.info(f"📊 Total de tablas: {len([t for t, j in json_files])}")
    logger.info(f"📁 Archivos generados en: {output_dir}")
    
    return True

def main():
    """Función principal"""
    try:
        success = generate_improved_sql_warehouse()
        return 0 if success else 1
    except Exception as e:
        logger.error(f"❌ Error crítico: {e}")
        return 1

if __name__ == "__main__":
    exit(main())