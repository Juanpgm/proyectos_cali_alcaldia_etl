#!/usr/bin/env python3
import psycopg2
from database_management.core import get_database_config

def check_table_structure():
    config = get_database_config()
    
    with psycopg2.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password
    ) as conn:
        with conn.cursor() as cursor:
            # Verificar emp_contratos
            cursor.execute("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'emp_contratos' 
                ORDER BY ordinal_position
            """)
            
            emp_contratos_columns = cursor.fetchall()
            
            print("\nColumnas de emp_contratos:")
            print("=" * 50)
            for col in emp_contratos_columns:
                nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                print(f"  - {col[0]} ({col[1]}) {nullable}")
            
            # Verificar emp_proyectos
            cursor.execute("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'emp_proyectos' 
                ORDER BY ordinal_position
            """)
            
            emp_proyectos_columns = cursor.fetchall()
            
            print("\nColumnas de emp_proyectos:")
            print("=" * 50)
            for col in emp_proyectos_columns:
                nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                print(f"  - {col[0]} ({col[1]}) {nullable}")

if __name__ == "__main__":
    check_table_structure()