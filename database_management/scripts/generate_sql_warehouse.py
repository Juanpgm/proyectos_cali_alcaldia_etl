#!/usr/bin/env python3
"""
Generador de SQL para Data Warehouse - Alcaldía de Santiago de Cali ETL
=====================================================================

Este script utiliza programación funcional para generar automáticamente
scripts SQL DDL optimizados para un data warehouse, basados en los esquemas
de datos generados previamente.

Características:
- Generación de DDL para PostgreSQL con PostGIS
- Optimización para warehouse (columnar, particionado)
- Preservación de datos existentes con timestamps
- Índices optimizados para analytics
- Constraints y validaciones
- Scripts de migración seguros
- Soporte completo para datos geoespaciales

Autor: Sistema ETL Alcaldía de Cali
Versión: 1.0.0
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import logging
import re
from textwrap import dedent

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# ==========================================
# MAPEO DE TIPOS SQL
# ==========================================

POSTGRESQL_TYPE_MAPPING = {
    'varchar': 'VARCHAR',
    'text': 'TEXT',
    'integer': 'INTEGER',
    'numeric': 'NUMERIC',
    'boolean': 'BOOLEAN',
    'date': 'DATE',
    'datetime': 'TIMESTAMP WITH TIME ZONE',
    'timestamp': 'TIMESTAMP WITH TIME ZONE',
    'json': 'JSONB',  # Usar JSONB para mejor performance
    'uuid': 'UUID',
    'geometry': 'GEOMETRY',
    'email': 'VARCHAR',
    'url': 'TEXT',
    'unknown': 'TEXT'
}

# ==========================================
# FUNCIONES UTILITARIAS
# ==========================================

def sanitize_column_name(column_name: str) -> str:
    """Sanitiza nombres de columna para ser válidos en SQL"""
    # Reemplazar caracteres especiales
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)
    
    # Asegurar que no empiece con número
    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    
    # Asegurar que no sea palabra reservada de SQL
    sql_keywords = {
        'order', 'group', 'where', 'select', 'from', 'insert', 'update', 
        'delete', 'create', 'drop', 'alter', 'table', 'index', 'view',
        'schema', 'database', 'user', 'role', 'grant', 'revoke'
    }
    
    if sanitized.lower() in sql_keywords:
        sanitized = f"{sanitized}_col"
    
    return sanitized.lower()

def get_postgresql_type(field_schema: Dict[str, Any]) -> str:
    """Convierte el esquema del campo a tipo PostgreSQL"""
    data_type = field_schema.get('data_type', 'varchar')
    max_length = field_schema.get('max_length')
    precision = field_schema.get('precision')
    scale = field_schema.get('scale')
    
    base_type = POSTGRESQL_TYPE_MAPPING.get(data_type, 'TEXT')
    
    # Aplicar parámetros específicos
    if data_type == 'varchar' and max_length:
        return f"VARCHAR({max_length})"
    elif data_type == 'numeric' and precision and scale:
        return f"NUMERIC({precision}, {scale})"
    elif data_type == 'geometry':
        # Usar POINT como default, podría ser más inteligente analizando los datos
        return "GEOMETRY(POINT, 4326)"
    
    return base_type

def generate_column_definition(field_schema: Dict[str, Any]) -> str:
    """Genera la definición completa de una columna SQL"""
    field_name = sanitize_column_name(field_schema['name'])
    sql_type = get_postgresql_type(field_schema)
    nullable = field_schema.get('nullable', True)
    is_primary_key = field_schema.get('is_primary_key', False)
    default_value = field_schema.get('default_value')
    
    # Construir definición
    definition_parts = [f"    {field_name}", sql_type]
    
    if is_primary_key:
        definition_parts.append("PRIMARY KEY")
        nullable = False
    
    if not nullable:
        definition_parts.append("NOT NULL")
    
    if default_value is not None:
        if isinstance(default_value, str):
            definition_parts.append(f"DEFAULT '{default_value}'")
        else:
            definition_parts.append(f"DEFAULT {default_value}")
    
    return " ".join(definition_parts)

# ==========================================
# GENERACIÓN DE DDL
# ==========================================

def generate_table_ddl(schema: Dict[str, Any]) -> str:
    """Genera el DDL para crear una tabla"""
    table_name = schema['name']
    fields = schema.get('fields', [])
    description = schema.get('description', '')
    
    # Header
    ddl_lines = [
        f"-- ==============================================",
        f"-- Tabla: {table_name}",
        f"-- Descripción: {description}",
        f"-- Generado: {datetime.now().isoformat()}",
        f"-- ==============================================",
        "",
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
    ]
    
    # Campos base (agregar campos de auditoria)
    base_fields = [
        "    id UUID PRIMARY KEY DEFAULT gen_random_uuid()",
        "    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL",
        "    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL",
        "    version INTEGER DEFAULT 1 NOT NULL",
        "    is_active BOOLEAN DEFAULT true NOT NULL"
    ]
    
    # Agregar campos base si no existen
    existing_field_names = {sanitize_column_name(f['name']) for f in fields}
    final_fields = []
    
    for base_field in base_fields:
        field_name = base_field.split()[1]
        if field_name not in existing_field_names:
            final_fields.append(base_field)
    
    # Agregar campos del esquema
    for field in fields:
        if field['name'].lower() not in {'id', 'created_at', 'updated_at'}:
            column_def = generate_column_definition(field)
            final_fields.append(column_def)
    
    # Agregar campos al DDL
    ddl_lines.extend([field + "," for field in final_fields[:-1]])
    ddl_lines.append(final_fields[-1])  # Último sin coma
    
    ddl_lines.extend([");", ""])
    
    return "\\n".join(ddl_lines)

def generate_indexes_ddl(schema: Dict[str, Any]) -> str:
    """Genera índices para una tabla"""
    table_name = schema['name']
    fields = schema.get('fields', [])
    indexes = schema.get('indexes', [])
    
    ddl_lines = [
        f"-- Índices para tabla: {table_name}",
        ""
    ]
    
    # Índices básicos de warehouse
    basic_indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_created_at ON {table_name} (created_at);",
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_updated_at ON {table_name} (updated_at);",
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_active ON {table_name} (is_active) WHERE is_active = true;"
    ]
    
    ddl_lines.extend(basic_indexes)
    ddl_lines.append("")
    
    # Índices específicos del esquema
    for index in indexes:
        index_name = index.get('name', '')
        index_fields = index.get('fields', [])
        index_type = index.get('type', 'btree')
        
        if not index_fields:
            continue
        
        # Sanitizar nombres de campos
        sanitized_fields = [sanitize_column_name(field) for field in index_fields]
        fields_str = ", ".join(sanitized_fields)
        
        # Generar índice
        if index_type == 'gist':
            index_ddl = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING GIST ({fields_str});"
        elif index_type == 'gin':
            index_ddl = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING GIN ({fields_str});"
        else:
            index_ddl = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({fields_str});"
        
        ddl_lines.append(index_ddl)
    
    # Índices para campos comunes de analytics
    analytics_fields = ['fecha', 'estado', 'tipo', 'codigo', 'nombre', 'bpin']
    for field in fields:
        field_name = sanitize_column_name(field['name'])
        if any(pattern in field_name for pattern in analytics_fields):
            index_name = f"idx_{table_name}_{field_name}_analytics"
            ddl_lines.append(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({field_name});")
    
    ddl_lines.append("")
    return "\\n".join(ddl_lines)

def generate_constraints_ddl(schema: Dict[str, Any]) -> str:
    """Genera constraints para una tabla"""
    table_name = schema['name']
    fields = schema.get('fields', [])
    
    ddl_lines = [
        f"-- Constraints para tabla: {table_name}",
        ""
    ]
    
    # Foreign keys
    for field in fields:
        if field.get('is_foreign_key') and field.get('references'):
            field_name = sanitize_column_name(field['name'])
            references = field['references']
            constraint_name = f"fk_{table_name}_{field_name}"
            
            ddl_lines.append(f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name}")
            ddl_lines.append(f"    FOREIGN KEY ({field_name}) REFERENCES {references}")
            ddl_lines.append(f"    ON DELETE SET NULL ON UPDATE CASCADE;")
            ddl_lines.append("")
    
    # Check constraints para campos específicos
    for field in fields:
        field_name = sanitize_column_name(field['name'])
        data_type = field.get('data_type')
        
        # Constraints para campos numéricos
        if data_type in ['integer', 'numeric']:
            if 'valor' in field_name or 'monto' in field_name or 'precio' in field_name:
                constraint_name = f"chk_{table_name}_{field_name}_positive"
                ddl_lines.append(f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name}")
                ddl_lines.append(f"    CHECK ({field_name} >= 0);")
                ddl_lines.append("")
        
        # Constraints para fechas
        elif data_type in ['date', 'datetime', 'timestamp']:
            if 'fecha_inicio' in field_name:
                # Verificar que fecha_inicio sea razonable
                constraint_name = f"chk_{table_name}_{field_name}_reasonable"
                ddl_lines.append(f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name}")
                ddl_lines.append(f"    CHECK ({field_name} >= '2000-01-01');")
                ddl_lines.append("")
    
    return "\\n".join(ddl_lines)

def generate_triggers_ddl(schema: Dict[str, Any]) -> str:
    """Genera triggers para mantener timestamps actualizados"""
    table_name = schema['name']
    
    ddl = f"""
-- Trigger para actualizar timestamp automáticamente en {table_name}
CREATE OR REPLACE FUNCTION update_{table_name}_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    NEW.version = OLD.version + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_{table_name}_timestamp ON {table_name};
CREATE TRIGGER trigger_update_{table_name}_timestamp
    BEFORE UPDATE ON {table_name}
    FOR EACH ROW
    EXECUTE FUNCTION update_{table_name}_timestamp();

"""
    return ddl

def generate_comments_ddl(schema: Dict[str, Any]) -> str:
    """Genera comentarios para documentar la tabla y campos"""
    table_name = schema['name']
    fields = schema.get('fields', [])
    description = schema.get('description', '')
    
    ddl_lines = [
        f"-- Comentarios para tabla: {table_name}",
        ""
    ]
    
    # Comentario de tabla
    if description:
        ddl_lines.append(f"COMMENT ON TABLE {table_name} IS '{description}';")
        ddl_lines.append("")
    
    # Comentarios de campos
    for field in fields:
        field_name = sanitize_column_name(field['name'])
        field_description = field.get('description', f'Campo {field_name}')
        data_type = field.get('data_type', 'unknown')
        
        comment = f"{field_description} (Tipo: {data_type})"
        ddl_lines.append(f"COMMENT ON COLUMN {table_name}.{field_name} IS '{comment}';")
    
    ddl_lines.append("")
    return "\\n".join(ddl_lines)

# ==========================================
# GENERACIÓN DE SCRIPTS ESPECIALIZADOS
# ==========================================

def generate_warehouse_setup_script() -> str:
    """Genera script de configuración inicial para el warehouse"""
    script = """
-- ==============================================
-- CONFIGURACIÓN INICIAL DEL DATA WAREHOUSE
-- Alcaldía de Santiago de Cali ETL
-- ==============================================

-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Configuraciones de warehouse
SET timezone = 'America/Bogota';
SET datestyle = 'ISO, YMD';

-- Schema para datos de staging
CREATE SCHEMA IF NOT EXISTS staging;
COMMENT ON SCHEMA staging IS 'Schema para datos temporales y staging';

-- Schema para datos históricos
CREATE SCHEMA IF NOT EXISTS historical;
COMMENT ON SCHEMA historical IS 'Schema para datos históricos y auditoria';

-- Schema para vistas materializadas
CREATE SCHEMA IF NOT EXISTS analytics;
COMMENT ON SCHEMA analytics IS 'Schema para vistas materializadas y analytics';

-- Función para logging de cambios
CREATE OR REPLACE FUNCTION log_data_change()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO historical.change_log (
        table_name,
        operation,
        old_data,
        new_data,
        changed_by,
        changed_at
    ) VALUES (
        TG_TABLE_NAME,
        TG_OP,
        CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD) ELSE NULL END,
        CASE WHEN TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN row_to_json(NEW) ELSE NULL END,
        current_user,
        CURRENT_TIMESTAMP
    );
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Tabla de log de cambios
CREATE TABLE IF NOT EXISTS historical.change_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    old_data JSONB,
    new_data JSONB,
    changed_by TEXT NOT NULL,
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_change_log_table_name ON historical.change_log (table_name);
CREATE INDEX IF NOT EXISTS idx_change_log_changed_at ON historical.change_log (changed_at);
CREATE INDEX IF NOT EXISTS idx_change_log_operation ON historical.change_log (operation);

"""
    return script

def generate_analytics_views_script(schemas: List[Dict[str, Any]]) -> str:
    """Genera vistas materializadas para analytics"""
    script_lines = [
        "-- ==============================================",
        "-- VISTAS MATERIALIZADAS PARA ANALYTICS",
        "-- ==============================================",
        ""
    ]
    
    # Vista resumen de proyectos
    proyecto_tables = [s for s in schemas if 'proyecto' in s['name'].lower()]
    if proyecto_tables:
        script_lines.extend([
            "-- Vista resumen de proyectos",
            "CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.resumen_proyectos AS",
            "SELECT",
            "    COUNT(*) as total_proyectos,",
            "    SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as proyectos_activos,",
            "    EXTRACT(YEAR FROM created_at) as año,",
            "    EXTRACT(MONTH FROM created_at) as mes",
            "FROM datos_caracteristicos_proyectos",
            "GROUP BY EXTRACT(YEAR FROM created_at), EXTRACT(MONTH FROM created_at);",
            "",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_resumen_proyectos_año_mes",
            "    ON analytics.resumen_proyectos (año, mes);",
            ""
        ])
    
    # Vista resumen de contratos
    contrato_tables = [s for s in schemas if 'contrato' in s['name'].lower()]
    if contrato_tables:
        script_lines.extend([
            "-- Vista resumen de contratos",
            "CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.resumen_contratos AS",
            "SELECT",
            "    COUNT(*) as total_contratos,",
            "    SUM(valor_contrato) as valor_total,",
            "    AVG(valor_contrato) as valor_promedio,",
            "    estado_contrato,",
            "    DATE_TRUNC('month', created_at) as periodo",
            "FROM contratos_proyectos",
            "WHERE is_active = true",
            "GROUP BY estado_contrato, DATE_TRUNC('month', created_at);",
            "",
            "CREATE INDEX IF NOT EXISTS idx_resumen_contratos_estado_periodo",
            "    ON analytics.resumen_contratos (estado_contrato, periodo);",
            ""
        ])
    
    # Procedimiento para refrescar vistas
    script_lines.extend([
        "-- Función para refrescar todas las vistas materializadas",
        "CREATE OR REPLACE FUNCTION analytics.refresh_all_views()",
        "RETURNS void AS $$",
        "BEGIN",
        "    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.resumen_proyectos;",
        "    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.resumen_contratos;",
        "    ",
        "    -- Log de actualización",
        "    INSERT INTO historical.change_log (table_name, operation, changed_by, changed_at)",
        "    VALUES ('analytics_views', 'REFRESH', current_user, CURRENT_TIMESTAMP);",
        "END;",
        "$$ LANGUAGE plpgsql;",
        ""
    ])
    
    return "\\n".join(script_lines)

def generate_data_migration_script(schemas: List[Dict[str, Any]]) -> str:
    """Genera script para migrar datos existentes"""
    script_lines = [
        "-- ==============================================",
        "-- SCRIPT DE MIGRACIÓN DE DATOS EXISTENTES",
        "-- ==============================================",
        "",
        "-- Backup de tablas existentes antes de migración",
        "DO $$",
        "DECLARE",
        "    table_record RECORD;",
        "BEGIN",
        "    FOR table_record IN",
        "        SELECT table_name FROM information_schema.tables",
        "        WHERE table_schema = 'public'",
        "        AND table_type = 'BASE TABLE'",
        "    LOOP",
        "        EXECUTE format('CREATE TABLE IF NOT EXISTS historical.backup_%s AS SELECT * FROM %I;',",
        "                      table_record.table_name, table_record.table_name);",
        "        ",
        "        RAISE NOTICE 'Backup creado para tabla: %', table_record.table_name;",
        "    END LOOP;",
        "END $$;",
        ""
    ]
    
    # Migración específica por tabla
    for schema in schemas:
        table_name = schema['name']
        script_lines.extend([
            f"-- Migración para {table_name}",
            f"INSERT INTO {table_name} (",
            "    -- Mapear campos existentes a nuevos campos",
            "    -- NOTA: Personalizar según estructura existente",
            "    SELECT * FROM staging.temp_{table_name}",
            "    ON CONFLICT (id) DO UPDATE SET",
            "        updated_at = EXCLUDED.updated_at,",
            "        version = {table_name}.version + 1",
            ");",
            ""
        ])
    
    return "\\n".join(script_lines)

# ==========================================
# FUNCIÓN PRINCIPAL
# ==========================================

def load_schemas(schemas_file: Path) -> List[Dict[str, Any]]:
    """Carga los esquemas desde el archivo JSON"""
    try:
        with open(schemas_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data.get('schemas', [])
    
    except Exception as e:
        logger.error(f"Error cargando esquemas desde {schemas_file}: {e}")
        return []

def generate_complete_ddl(schemas: List[Dict[str, Any]]) -> str:
    """Genera el DDL completo para todas las tablas"""
    ddl_sections = [
        "-- ==============================================",
        "-- DDL COMPLETO PARA DATA WAREHOUSE",
        "-- Alcaldía de Santiago de Cali ETL",
        f"-- Generado: {datetime.now().isoformat()}",
        "-- ==============================================",
        "",
        generate_warehouse_setup_script(),
        ""
    ]
    
    # DDL para cada tabla
    for schema in schemas:
        table_ddl = generate_table_ddl(schema)
        indexes_ddl = generate_indexes_ddl(schema)
        constraints_ddl = generate_constraints_ddl(schema)
        triggers_ddl = generate_triggers_ddl(schema)
        comments_ddl = generate_comments_ddl(schema)
        
        ddl_sections.extend([
            table_ddl,
            indexes_ddl,
            constraints_ddl,
            triggers_ddl,
            comments_ddl,
            "\\n"
        ])
    
    # Vistas de analytics
    analytics_ddl = generate_analytics_views_script(schemas)
    ddl_sections.append(analytics_ddl)
    
    return "\\n".join(ddl_sections)

def main():
    """Función principal del generador de SQL"""
    logger.info("Iniciando generación de scripts SQL para warehouse")
    
    # Configurar rutas
    project_root = Path(__file__).parent.parent.parent
    schemas_dir = project_root / "database_management" / "generated_schemas"
    output_dir = project_root / "database_management" / "generated_sql"
    
    # Crear directorio de salida
    output_dir.mkdir(exist_ok=True)
    
    # Cargar esquemas más recientes
    schemas_file = schemas_dir / "latest_schemas.json"
    
    if not schemas_file.exists():
        logger.error(f"Archivo de esquemas no encontrado: {schemas_file}")
        logger.info("Ejecute primero: python scripts/analyze_data_schema.py")
        return False
    
    try:
        # Cargar esquemas
        schemas = load_schemas(schemas_file)
        
        if not schemas:
            logger.error("No se encontraron esquemas para procesar")
            return False
        
        logger.info(f"Procesando {len(schemas)} esquemas")
        
        # Generar timestamp para archivos
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generar DDL completo
        complete_ddl = generate_complete_ddl(schemas)
        ddl_file = output_dir / f"warehouse_ddl_{timestamp}.sql"
        
        with open(ddl_file, 'w', encoding='utf-8') as f:
            f.write(complete_ddl)
        
        logger.info(f"DDL completo generado: {ddl_file}")
        
        # Generar script de migración
        migration_script = generate_data_migration_script(schemas)
        migration_file = output_dir / f"data_migration_{timestamp}.sql"
        
        with open(migration_file, 'w', encoding='utf-8') as f:
            f.write(migration_script)
        
        logger.info(f"Script de migración generado: {migration_file}")
        
        # Generar archivos separados por tipo
        setup_file = output_dir / "01_warehouse_setup.sql"
        with open(setup_file, 'w', encoding='utf-8') as f:
            f.write(generate_warehouse_setup_script())
        
        tables_file = output_dir / "02_create_tables.sql"
        tables_ddl = []
        for schema in schemas:
            tables_ddl.append(generate_table_ddl(schema))
        
        with open(tables_file, 'w', encoding='utf-8') as f:
            f.write("\\n\\n".join(tables_ddl))
        
        indexes_file = output_dir / "03_create_indexes.sql"
        indexes_ddl = []
        for schema in schemas:
            indexes_ddl.append(generate_indexes_ddl(schema))
        
        with open(indexes_file, 'w', encoding='utf-8') as f:
            f.write("\\n\\n".join(indexes_ddl))
        
        analytics_file = output_dir / "04_analytics_views.sql"
        with open(analytics_file, 'w', encoding='utf-8') as f:
            f.write(generate_analytics_views_script(schemas))
        
        # También crear versiones latest
        latest_ddl = output_dir / "latest_warehouse_ddl.sql"
        with open(latest_ddl, 'w', encoding='utf-8') as f:
            f.write(complete_ddl)
        
        logger.info("=" * 60)
        logger.info("GENERACIÓN DE SQL COMPLETADA EXITOSAMENTE")
        logger.info(f"Total de tablas: {len(schemas)}")
        logger.info(f"DDL completo: {ddl_file}")
        logger.info(f"Migración: {migration_file}")
        logger.info(f"Setup: {setup_file}")
        logger.info(f"Tablas: {tables_file}")
        logger.info(f"Índices: {indexes_file}")
        logger.info(f"Analytics: {analytics_file}")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"Error durante la generación: {e}")
        raise

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)