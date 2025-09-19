#!/usr/bin/env python3
import psycopg2
from database_management.core import get_database_config

def check_tables():
    config = get_database_config()
    
    with psycopg2.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """)
            
            tables = cursor.fetchall()
            
            print("\nTablas existentes en la base de datos:")
            print("=" * 50)
            for table in tables:
                print(f"  - {table[0]}")
            
            print(f"\nTotal de tablas: {len(tables)}")
            
            # Verificar tablas esperadas del ETL
            expected_tables = [
                'contratos_secop',
                'proyectos_inversion',
                'ejecucion_presupuestal',
                'paa_procurement',
                'seguimiento_pa',
                'unidades_proyecto'
            ]
            
            print("\nVerificacion de tablas esperadas:")
            print("=" * 50)
            for table in expected_tables:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = %s
                    )
                """, (table,))
                exists = cursor.fetchone()[0]
                status = "✓" if exists else "✗"
                print(f"  {status} {table}")

if __name__ == "__main__":
    check_tables()