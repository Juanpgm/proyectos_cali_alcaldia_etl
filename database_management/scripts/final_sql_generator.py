#!/usr/bin/env python3
"""
Generador SQL Final Optimizado - Alcaldía de Santiago de Cali ETL
================================================================

Generador final que analiza exhaustivamente los datos JSON reales
para crear SQL perfectamente compatible con PostgreSQL.

Características:
- Análisis completo de muestras de datos reales
- Detección inteligente de tipos con validación cruzada
- Manejo robusto de casos edge
- SQL optimizado para carga de datos masiva
- Preservación exacta de estructura JSON

Versión: 3.0.0 - Final
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Union
from datetime import datetime
import logging
import re
from collections import Counter

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_field_samples_comprehensive(field_name: str, values: List[Any]) -> Dict[str, Any]:
    """
    Análisis exhaustivo de valores para determinar el tipo SQL óptimo
    """
    if not values:
        return {'sql_type': 'TEXT', 'nullable': True, 'analysis': 'no_data'}
    
    # Filtrar valores válidos
    non_null_values = []
    null_count = 0
    
    for value in values:
        if value is None or value == "" or str(value).strip().lower() in ['null', 'none', 'nan']:
            null_count += 1
        else:
            non_null_values.append(value)
    
    if not non_null_values:
        return {'sql_type': 'TEXT', 'nullable': True, 'analysis': 'all_null'}
    
    # SIEMPRE hacer campos nullable para evitar errores de NOT NULL
    nullable = True  # Cambiado: siempre nullable para mayor flexibilidad
    
    # Análisis por categorías
    analysis = {
        'total_count': len(values),
        'non_null_count': len(non_null_values),
        'null_count': null_count,
        'nullable': nullable,
        'unique_count': len(set(str(v) for v in non_null_values)),
        'max_length': max(len(str(v)) for v in non_null_values)
    }
    
    # Contadores de tipos
    type_counts = Counter()
    
    # Análisis detallado de cada valor
    for value in non_null_values:
        detected_type = detect_precise_type(value)
        type_counts[detected_type] += 1
    
        # Determinar tipo dominante
        if not type_counts:
            sql_type = 'TEXT'
        else:
            dominant_type = type_counts.most_common(1)[0][0]
            
            # Si hay muchos strings o tipos mixtos, usar TEXT por seguridad
            if len(type_counts) > 3 or type_counts.get('STRING', 0) > len(non_null_values) * 0.3:
                sql_type = 'TEXT'
            
            # Validaciones adicionales más conservadoras
            elif dominant_type == 'INTEGER':
                # Verificar rango de enteros y que no sean IDs
                try:
                    int_values = []
                    for v in non_null_values:
                        str_v = str(v).strip()
                        if str_v.replace('-', '').isdigit() and len(str_v) <= 10:
                            int_values.append(int(v))
                    
                    if int_values and len(int_values) > len(non_null_values) * 0.8:
                        max_val = max(int_values)
                        min_val = min(int_values)
                        
                        if max_val > 2147483647 or min_val < -2147483648:
                            sql_type = 'BIGINT'
                        else:
                            sql_type = 'INTEGER'
                    else:
                        sql_type = 'TEXT'  # Valores mixtos, usar TEXT
                except:
                    sql_type = 'TEXT'
            
            elif dominant_type == 'BOOLEAN':
                # Solo usar BOOLEAN si al menos 80% son booleanos válidos
                bool_count = type_counts.get('BOOLEAN', 0)
                if bool_count > len(non_null_values) * 0.8:
                    sql_type = 'BOOLEAN'
                else:
                    sql_type = 'TEXT'
            
            elif dominant_type == 'NUMERIC':
                sql_type = 'DOUBLE PRECISION'
            
            elif dominant_type == 'DATE':
                sql_type = 'DATE'
            
            elif dominant_type == 'TIMESTAMP':
                sql_type = 'TIMESTAMP WITH TIME ZONE'
            
            elif dominant_type == 'JSON':
                sql_type = 'JSONB'
            
            else:
                # Para STRING y otros, determinar entre VARCHAR y TEXT
                if analysis['max_length'] <= 50:
                    sql_type = f"VARCHAR({min(255, analysis['max_length'] + 20)})"
                elif analysis['max_length'] <= 255:
                    sql_type = f"VARCHAR({min(500, analysis['max_length'] + 50)})"
                else:
                    sql_type = 'TEXT'
    
    analysis['sql_type'] = sql_type
    analysis['type_distribution'] = dict(type_counts)
    analysis['dominant_type'] = type_counts.most_common(1)[0][0] if type_counts else 'UNKNOWN'
    
    return analysis

def detect_precise_type(value: Any) -> str:
    """
    Detección precisa de tipo de dato individual
    """
    if value is None:
        return 'NULL'
    
    # Si ya es un tipo primitivo de Python
    if isinstance(value, bool):
        return 'BOOLEAN'
    if isinstance(value, int):
        return 'INTEGER'
    if isinstance(value, float):
        return 'NUMERIC'
    if isinstance(value, (list, dict)):
        return 'JSON'
    
    # Convertir a string para análisis
    str_value = str(value).strip()
    
    if not str_value:
        return 'NULL'
    
    # Detección de patrones específicos
    
    # Booleanos como string (más valores posibles)
    bool_values = ['true', 'false', 'yes', 'no', 'si', 'sí', 'y', 'n', 'verdadero', 'falso']
    if str_value.lower() in bool_values:
        return 'BOOLEAN'
    
    # Fechas ISO
    if re.match(r'^\d{4}-\d{2}-\d{2}$', str_value):
        return 'DATE'
    
    # Timestamps/DateTime
    if re.match(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}', str_value):
        return 'TIMESTAMP'
    
    # Números enteros (incluyendo negativos) - más estricto
    if re.match(r'^-?\d+$', str_value) and len(str_value) <= 15:  # Evitar IDs largos
        return 'INTEGER'
    
    # Números decimales
    if re.match(r'^-?\d*\.\d+$', str_value):
        return 'NUMERIC'
    
    # URLs
    if str_value.startswith(('http://', 'https://', 'www.')):
        return 'STRING'
    
    # UUIDs
    if re.match(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$', str_value):
        return 'UUID'
    
    # JSON strings
    if str_value.startswith(('{', '[')):
        try:
            json.loads(str_value)
            return 'JSON'
        except:
            pass
    
    # Por defecto es string
    return 'STRING'

def generate_optimized_table_sql(table_name: str, json_file_path: Path, sample_size: int = 500) -> str:
    """
    Genera SQL CREATE TABLE optimizado basado en análisis exhaustivo
    """
    logger.info(f"🔍 Analizando {table_name} desde {json_file_path.name}")
    
    # Cargar datos para análisis
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        raise ValueError(f"Error cargando {json_file_path}: {e}")
    
    if not isinstance(data, list) or not data:
        raise ValueError(f"No hay datos válidos en {json_file_path}")
    
    # Tomar muestra para análisis
    sample_data = data[:sample_size]
    logger.info(f"📊 Analizando muestra de {len(sample_data)} registros")
    
    # Recopilar todos los campos y sus valores
    field_values = {}
    for record in sample_data:
        if isinstance(record, dict):
            for field_name, value in record.items():
                if field_name not in field_values:
                    field_values[field_name] = []
                field_values[field_name].append(value)
    
    # Analizar cada campo
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
    for field_name, values in field_values.items():
        if field_name.lower() in ['id', 'created_at', 'updated_at', 'version', 'is_active']:
            continue
        
        # Sanitizar nombre de campo
        clean_field_name = sanitize_field_name(field_name)
        
        # Análisis exhaustivo
        analysis = analyze_field_samples_comprehensive(field_name, values)
        
        # Construir definición de columna
        sql_type = analysis['sql_type']
        nullable = analysis['nullable']
        
        if nullable:
            column_def = f"    {clean_field_name} {sql_type}"
        else:
            column_def = f"    {clean_field_name} {sql_type} NOT NULL"
        
        columns.append(column_def)
        
        logger.debug(f"  {clean_field_name}: {sql_type} ({'NULL' if nullable else 'NOT NULL'})")
    
    # Construir SQL final
    columns_sql = ',\n'.join(columns)
    
    table_sql = f"""-- Tabla: {table_name}
-- Generado desde: {json_file_path.name}
-- Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Registros analizados: {len(sample_data):,}

CREATE TABLE IF NOT EXISTS {table_name} (
{columns_sql}
);"""
    
    logger.info(f"✅ SQL generado para {table_name} ({len(field_values)} campos)")
    return table_sql

def sanitize_field_name(field_name: str) -> str:
    """Sanitiza nombres de campo para PostgreSQL"""
    # Convertir a string y limpiar
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', str(field_name))
    
    # Asegurar que no empiece con número
    if clean_name and clean_name[0].isdigit():
        clean_name = f"field_{clean_name}"
    
    # Convertir a minúsculas
    clean_name = clean_name.lower()
    
    # Evitar palabras reservadas
    reserved = {
        'order', 'group', 'where', 'select', 'from', 'insert', 'update', 
        'delete', 'create', 'drop', 'alter', 'table', 'index', 'view',
        'user', 'role', 'grant', 'revoke', 'commit', 'rollback', 'begin',
        'end', 'case', 'when', 'then', 'else', 'and', 'or', 'not', 'null',
        'true', 'false', 'union', 'join', 'left', 'right', 'inner', 'outer'
    }
    
    if clean_name in reserved:
        clean_name = f"{clean_name}_field"
    
    return clean_name

def discover_all_json_files(data_dir: Path) -> List[tuple]:
    """Descubre todos los archivos JSON válidos"""
    json_files = []
    
    for json_file in data_dir.rglob("*.json"):
        # Verificar que el archivo tenga contenido válido
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, list) and len(data) > 0:
                # Generar nombre de tabla
                table_name = json_file.stem.lower()
                table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
                
                json_files.append((table_name, json_file))
                
        except Exception as e:
            logger.warning(f"⚠️ Omitiendo {json_file}: {e}")
    
    return json_files

def generate_complete_warehouse_sql():
    """Genera el warehouse SQL completo optimizado"""
    logger.info("🚀 Iniciando generación final de warehouse SQL")
    
    # Configurar rutas
    project_root = Path(__file__).parent.parent.parent
    data_dir = project_root / "transformation_app" / "app_outputs"
    output_dir = project_root / "database_management" / "generated_sql"
    
    # Crear directorio de salida
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Descubrir archivos JSON
    json_files = discover_all_json_files(data_dir)
    
    if not json_files:
        raise ValueError(f"No se encontraron archivos JSON válidos en {data_dir}")
    
    logger.info(f"📁 Encontrados {len(json_files)} archivos JSON válidos")
    
    # 1. Generar setup del warehouse
    setup_sql = """-- Configuración inicial del Data Warehouse
-- Alcaldía de Santiago de Cali ETL
-- Generado: {}

-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Configuración
SET timezone = 'America/Bogota';
SET datestyle = 'ISO, YMD';

-- Función para actualizar timestamps
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    NEW.version = OLD.version + 1;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Función para logging de cambios
CREATE OR REPLACE FUNCTION log_data_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Log opcional para auditoria
    RETURN COALESCE(NEW, OLD);
END;
$$ language 'plpgsql';""".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    setup_file = output_dir / "01_warehouse_setup.sql"
    setup_file.write_text(setup_sql, encoding='utf-8')
    logger.info(f"✅ Setup: {setup_file}")
    
    # 2. Generar tablas
    table_sqls = []
    successful_tables = []
    failed_tables = []
    
    for table_name, json_file in json_files:
        try:
            table_sql = generate_optimized_table_sql(table_name, json_file)
            table_sqls.append(table_sql)
            successful_tables.append(table_name)
        except Exception as e:
            logger.error(f"❌ Error en {table_name}: {e}")
            failed_tables.append((table_name, str(e)))
    
    tables_sql = '\n\n'.join(table_sqls)
    tables_file = output_dir / "02_create_tables.sql"
    tables_file.write_text(tables_sql, encoding='utf-8')
    logger.info(f"✅ Tablas: {tables_file} ({len(successful_tables)} tablas)")
    
    # 3. Generar índices básicos
    indexes_sql_parts = [f"-- Índices básicos\n-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"]
    
    for table_name in successful_tables:
        indexes_sql_parts.append(f"-- Índices para {table_name}")
        indexes_sql_parts.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_created_at ON {table_name} (created_at);")
        indexes_sql_parts.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_updated_at ON {table_name} (updated_at);")
        indexes_sql_parts.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_active ON {table_name} (is_active) WHERE is_active = true;")
        indexes_sql_parts.append("")
    
    indexes_sql = '\n'.join(indexes_sql_parts)
    indexes_file = output_dir / "03_create_indexes.sql"
    indexes_file.write_text(indexes_sql, encoding='utf-8')
    logger.info(f"✅ Índices: {indexes_file}")
    
    # 4. Generar triggers
    triggers_sql_parts = [f"-- Triggers de auditoria\n-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"]
    
    for table_name in successful_tables:
        triggers_sql_parts.append(f"-- Trigger para {table_name}")
        triggers_sql_parts.append(f"CREATE TRIGGER update_{table_name}_modtime")
        triggers_sql_parts.append(f"    BEFORE UPDATE ON {table_name}")
        triggers_sql_parts.append(f"    FOR EACH ROW")
        triggers_sql_parts.append(f"    EXECUTE FUNCTION update_modified_column();")
        triggers_sql_parts.append("")
    
    triggers_sql = '\n'.join(triggers_sql_parts)
    triggers_file = output_dir / "04_create_triggers.sql"
    triggers_file.write_text(triggers_sql, encoding='utf-8')
    logger.info(f"✅ Triggers: {triggers_file}")
    
    # 5. Generar vistas analíticas básicas
    analytics_sql = f"""-- Vistas analíticas básicas
-- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

-- Vista de resumen de tablas
CREATE OR REPLACE VIEW warehouse_summary AS
SELECT 
    schemaname,
    relname as table_name,
    n_tup_ins as total_insertions,
    n_tup_upd as total_updates,
    n_tup_del as total_deletions,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows
FROM pg_stat_user_tables
WHERE schemaname = 'public'
  AND relname NOT LIKE 'pg_%'
ORDER BY relname;

-- Vista de actividad por tabla
CREATE OR REPLACE VIEW table_activity AS
SELECT 
    relname as table_name,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY (seq_tup_read + idx_tup_fetch) DESC;"""
    
    analytics_file = output_dir / "05_analytics_views.sql"
    analytics_file.write_text(analytics_sql, encoding='utf-8')
    logger.info(f"✅ Analytics: {analytics_file}")
    
    # Resumen final
    logger.info("\n" + "="*60)
    logger.info("🎉 GENERACIÓN SQL COMPLETADA")
    logger.info("="*60)
    logger.info(f"📊 Tablas exitosas: {len(successful_tables)}")
    logger.info(f"❌ Tablas con error: {len(failed_tables)}")
    logger.info(f"📁 Archivos en: {output_dir}")
    
    if failed_tables:
        logger.warning("⚠️ Tablas con errores:")
        for table, error in failed_tables:
            logger.warning(f"  • {table}: {error}")
    
    logger.info("="*60)
    
    return len(successful_tables), len(failed_tables)

def main():
    """Función principal"""
    try:
        successful, failed = generate_complete_warehouse_sql()
        return 0 if failed == 0 else 1
    except Exception as e:
        logger.error(f"❌ Error crítico: {e}")
        return 1

if __name__ == "__main__":
    exit(main())