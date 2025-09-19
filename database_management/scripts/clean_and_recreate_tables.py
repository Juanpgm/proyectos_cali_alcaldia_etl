#!/usr/bin/env python3
"""
LIMPIADOR Y RECREADOR DE TABLAS ULTRA FINAL
==========================================
Elimina las tablas existentes y las recrea con el esquema ultra conservador corregido.
"""

import psycopg2
import psycopg2.extras
from pathlib import Path
import sys

# Configuración del path
sys.path.append(str(Path(__file__).parent.parent.parent))

from database_management.core.config import get_database_config

def clean_and_recreate_tables():
    """Limpia y recrea todas las tablas con el esquema corregido"""
    
    config = get_database_config()
    
    try:
        # Conectar a la base de datos
        print("Conectando a PostgreSQL...")
        connection = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password
        )
        cursor = connection.cursor()
        
        # Lista de tablas a limpiar
        tables_to_clean = [
            'contratos_proyectos',
            'datos_caracteristicos_proyectos',
            'ejecucion_presupuestal',
            'movimientos_presupuestales',
            'emp_contratos_index',
            'emp_procesos',
            'emp_procesos_index',
            'emp_proyectos',
            'procesos_secop',
            'unidad_proyecto_infraestructura_equipamientos',
            'unidad_proyecto_infraestructura_vial'
        ]
        
        print("Eliminando tablas existentes...")
        for table in tables_to_clean:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                print(f"  ✓ Eliminada: {table}")
            except Exception as e:
                print(f"  ! Error eliminando {table}: {e}")
        
        connection.commit()
        print("✅ Todas las tablas eliminadas")
        
        # Ahora ejecutar el SQL corregido
        sql_file = Path(__file__).parent.parent / "generated_sql" / "02_create_tables.sql"
        
        if sql_file.exists():
            print("Recreando tablas con esquema ultra conservador...")
            sql_content = sql_file.read_text(encoding='utf-8')
            
            # Dividir por statements y ejecutar
            statements = [s.strip() for s in sql_content.split(';') if s.strip() and 'CREATE TABLE' in s]
            
            for statement in statements:
                if statement.strip():
                    try:
                        cursor.execute(statement)
                        connection.commit()
                        # Extraer nombre de tabla del statement
                        table_name = statement.split('CREATE TABLE IF NOT EXISTS ')[1].split(' (')[0]
                        print(f"  ✓ Recreada: {table_name}")
                    except Exception as e:
                        print(f"  ! Error recreando tabla: {e}")
                        connection.rollback()
        
        print("✅ Recreación de tablas completada")
        
        # Verificar las tablas
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND table_type = 'BASE TABLE'
              AND table_name NOT LIKE 'pg_%'
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        print(f"\n📊 Tablas existentes en la base de datos ({len(tables)}):")
        for table in tables:
            print(f"  • {table[0]}")
        
        cursor.close()
        connection.close()
        
        print("\n🎉 Limpieza y recreación completada exitosamente")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    clean_and_recreate_tables()