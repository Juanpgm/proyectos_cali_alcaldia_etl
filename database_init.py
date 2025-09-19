"""
Database Schema Initialization Script
====================================

Este script crea las tablas faltantes en la base de datos PostgreSQL local
usando las definiciones del esquema funcional.
"""

import sys
from pathlib import Path
from typing import List, Dict, Any

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent))

from database_management.core.schema_analysis import (
    get_expected_schema,
    get_existing_tables,
    analyze_database_schema
)
from database_management.core.config import get_database_config
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def generate_create_table_sql(table_def) -> str:
    """
    Generar SQL CREATE TABLE desde definición de tabla
    
    Args:
        table_def: TableDefinition con la estructura de la tabla
        
    Returns:
        SQL string para crear la tabla
    """
    sql_parts = [f"CREATE TABLE IF NOT EXISTS {table_def.name} ("]
    
    column_definitions = []
    for col in table_def.columns:
        col_def = f"    {col.name} {col.data_type.upper()}"
        
        if not col.nullable:
            col_def += " NOT NULL"
        
        if col.default:
            col_def += f" DEFAULT {col.default}"
        
        if col.primary_key:
            col_def += " PRIMARY KEY"
        
        column_definitions.append(col_def)
    
    sql_parts.append(",\n".join(column_definitions))
    sql_parts.append(");")
    
    create_table_sql = "\n".join(sql_parts)
    
    # Agregar índices si existen - mapear nombres correctos
    index_column_mapping = {
        "idx_emp_contratos_numero_proceso": "numero_proceso",
        "idx_emp_contratos_estado": "estado",
        "idx_emp_seguimiento_numero_proceso": "numero_proceso", 
        "idx_emp_seguimiento_fecha": "fecha_seguimiento",
        "idx_emp_proyectos_codigo": "codigo_proyecto",
        "idx_emp_proyectos_estado": "estado",
        "idx_flujo_caja_fecha": "fecha",
        "idx_flujo_caja_categoria": "categoria",
        "idx_unidad_equipamientos_codigo": "codigo_unidad",
        "idx_unidad_equipamientos_tipo": "tipo_equipamiento",
        "idx_unidad_vial_codigo": "codigo_unidad",
        "idx_unidad_vial_tipo": "tipo_via"
    }
    
    index_sqls = []
    for index_name in table_def.indexes:
        column_name = index_column_mapping.get(index_name)
        if column_name:
            index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_def.name} ({column_name});"
            index_sqls.append(index_sql)
    
    if index_sqls:
        create_table_sql += "\n\n-- Índices\n" + "\n".join(index_sqls)
    
    return create_table_sql


def create_missing_tables(config=None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Crear tablas faltantes en la base de datos
    
    Args:
        config: Configuración de base de datos
        dry_run: Si True, solo genera SQL sin ejecutar
        
    Returns:
        Resultado de la operación
    """
    if config is None:
        config = get_database_config()
    
    print("🏗️ INICIALIZACIÓN DE ESQUEMA DE BASE DE DATOS")
    print("=" * 50)
    
    try:
        # Obtener esquema esperado y tablas existentes
        expected_schema = get_expected_schema()
        existing_tables = get_existing_tables(config)
        
        missing_tables = expected_schema.table_names - existing_tables
        
        print(f"📊 Estado actual:")
        print(f"   Tablas esperadas: {len(expected_schema.table_names)}")
        print(f"   Tablas existentes: {len(existing_tables)}")
        print(f"   Tablas faltantes: {len(missing_tables)}")
        
        if not missing_tables:
            print("✅ Todas las tablas ya existen!")
            return {
                "success": True,
                "message": "Schema completo - no se requieren cambios",
                "tables_created": [],
                "tables_existing": list(existing_tables)
            }
        
        print(f"\n📝 Tablas a crear:")
        for table_name in missing_tables:
            print(f"   - {table_name}")
        
        # Generar SQL para tablas faltantes
        sql_statements = []
        tables_to_create = []
        
        for table_name in missing_tables:
            table_def = expected_schema.get_table(table_name)
            if table_def:
                sql = generate_create_table_sql(table_def)
                sql_statements.append(sql)
                tables_to_create.append(table_name)
                
                print(f"\n🛠️ SQL para {table_name}:")
                print("-" * 40)
                print(sql)
                print("-" * 40)
        
        if dry_run:
            print(f"\n🔍 DRY RUN - SQL generado pero no ejecutado")
            return {
                "success": True,
                "message": "Dry run completado",
                "sql_statements": sql_statements,
                "tables_to_create": tables_to_create
            }
        
        # Ejecutar SQL
        print(f"\n🚀 Ejecutando creación de tablas...")
        
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        cursor = conn.cursor()
        
        created_tables = []
        for i, sql in enumerate(sql_statements):
            table_name = tables_to_create[i]
            try:
                print(f"   📝 Creando tabla: {table_name}...")
                cursor.execute(sql)
                created_tables.append(table_name)
                print(f"   ✅ {table_name} creada exitosamente")
                
            except Exception as e:
                print(f"   ❌ Error creando {table_name}: {e}")
                # Continuar con las demás tablas
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 Proceso completado!")
        print(f"   Tablas creadas: {len(created_tables)}")
        
        if created_tables:
            print(f"   Creadas exitosamente:")
            for table in created_tables:
                print(f"     - {table}")
        
        return {
            "success": True,
            "message": f"Creadas {len(created_tables)} tablas",
            "tables_created": created_tables,
            "sql_executed": sql_statements
        }
        
    except Exception as e:
        print(f"❌ Error en inicialización: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def verify_schema_completion(config=None) -> Dict[str, Any]:
    """
    Verificar que el esquema esté completo después de la inicialización
    
    Args:
        config: Configuración de base de datos
        
    Returns:
        Resultado de la verificación
    """
    print("\n🔍 VERIFICACIÓN DE ESQUEMA COMPLETO")
    print("=" * 50)
    
    try:
        # Ejecutar análisis completo de esquema
        analysis_report = analyze_database_schema(config)
        
        print(f"📊 Resultado del análisis:")
        print(f"   Estado general: {analysis_report.overall_status.upper()}")
        print(f"   Tablas esperadas: {len(analysis_report.expected_tables)}")
        print(f"   Tablas existentes: {len(analysis_report.existing_tables)}")
        print(f"   Tablas faltantes: {len(analysis_report.missing_tables)}")
        
        if analysis_report.missing_tables:
            print(f"\n⚠️ Tablas aún faltantes:")
            for table in analysis_report.missing_tables:
                print(f"     - {table}")
        
        if analysis_report.recommendations:
            print(f"\n💡 Recomendaciones:")
            for rec in analysis_report.recommendations:
                print(f"     - {rec}")
        
        # Determinar si el esquema está completo
        schema_complete = len(analysis_report.missing_tables) == 0
        
        if schema_complete:
            print(f"\n✅ ESQUEMA COMPLETO!")
            print(f"   Base de datos lista para operaciones ETL")
        else:
            print(f"\n⚠️ Esquema incompleto")
            print(f"   {len(analysis_report.missing_tables)} tablas aún faltantes")
        
        return {
            "success": True,
            "schema_complete": schema_complete,
            "analysis_report": analysis_report,
            "missing_tables": list(analysis_report.missing_tables),
            "existing_tables": list(analysis_report.existing_tables)
        }
        
    except Exception as e:
        print(f"❌ Error en verificación: {e}")
        return {
            "success": False,
            "error": str(e),
            "schema_complete": False
        }


def main():
    """Función principal de inicialización"""
    print("🚀 INICIALIZACIÓN DE BASE DE DATOS LOCAL")
    print("=" * 60)
    
    try:
        # Paso 1: Análisis inicial
        print("📋 Paso 1: Análisis del estado actual...")
        config = get_database_config()
        print(f"🔧 Conectando a: {config.host}:{config.port}/{config.database}")
        
        # Paso 2: Dry run para mostrar lo que se va a hacer
        print(f"\n📋 Paso 2: Generación de SQL (dry run)...")
        dry_run_result = create_missing_tables(config, dry_run=True)
        
        if not dry_run_result["success"]:
            print(f"❌ Error en dry run: {dry_run_result.get('error')}")
            return False
        
        if not dry_run_result.get("tables_to_create"):
            print("✅ No hay tablas que crear")
            return True
        
        # Confirmar ejecución
        print(f"\n❓ ¿Proceder con la creación de {len(dry_run_result['tables_to_create'])} tablas?")
        print("   Tablas a crear:", ", ".join(dry_run_result["tables_to_create"]))
        
        # Para automatización, proceder automáticamente
        print("🚀 Procediendo automáticamente...")
        
        # Paso 3: Crear tablas
        print(f"\n📋 Paso 3: Creación de tablas...")
        creation_result = create_missing_tables(config, dry_run=False)
        
        if not creation_result["success"]:
            print(f"❌ Error en creación: {creation_result.get('error')}")
            return False
        
        # Paso 4: Verificación final
        print(f"\n📋 Paso 4: Verificación final...")
        verification_result = verify_schema_completion(config)
        
        if verification_result["success"] and verification_result["schema_complete"]:
            print(f"\n🎉 ¡INICIALIZACIÓN EXITOSA!")
            print(f"   Base de datos completamente inicializada")
            print(f"   Lista para operaciones ETL")
            return True
        else:
            print(f"\n⚠️ Inicialización parcial")
            print(f"   Algunas tablas pueden requerir atención manual")
            return False
        
    except Exception as e:
        print(f"❌ Error crítico en inicialización: {e}")
        import traceback
        print(f"🔍 Detalles:")
        print(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)