"""
Script para borrar todas las tablas de la base de datos PostgreSQL.

Este script elimina todas las tablas del esquema configurado,
deshabilitando temporalmente las restricciones de clave foránea
para evitar errores de dependencias.
"""

import sys
import logging
import os
from pathlib import Path
from typing import List, Dict, Any
import psycopg2
from psycopg2 import sql

# Cargar variables de entorno
try:
    from dotenv import load_dotenv
    
    # Buscar el archivo .env en el directorio raíz del proyecto
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
        print(f"✅ Variables de entorno cargadas desde: {env_path}")
    else:
        print("⚠️  Archivo .env no encontrado, usando variables del sistema")
        
except ImportError:
    print("⚠️  python-dotenv no disponible, usando variables del sistema")

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_database_config():
    """
    Obtener configuración de la base de datos desde variables de entorno.
    
    Returns:
        Dict con la configuración de la base de datos
    """
    # Buscar variables específicas del ETL primero, luego las estándar PostgreSQL
    host = (os.getenv("DB_HOST") or 
            os.getenv("POSTGRES_SERVER") or 
            "localhost")
    
    port = int(os.getenv("DB_PORT") or 
               os.getenv("POSTGRES_PORT") or 
               "5432")
    
    database = (os.getenv("DB_NAME") or 
                os.getenv("POSTGRES_DB") or 
                "postgres")
    
    user = (os.getenv("DB_USER") or 
            os.getenv("POSTGRES_USER") or 
            "postgres")
    
    password = (os.getenv("DB_PASSWORD") or 
                os.getenv("POSTGRES_PASSWORD") or 
                "postgres")
    
    schema = os.getenv("DB_SCHEMA", "public")
    
    # Mostrar configuración detectada (sin password)
    print(f"🔧 Configuración de BD detectada:")
    print(f"   Host: {host}")
    print(f"   Puerto: {port}")
    print(f"   Base de datos: {database}")
    print(f"   Usuario: {user}")
    print(f"   Schema: {schema}")
    print(f"   Password: {'*' * len(password) if password else '(no configurado)'}")
    
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "schema": schema
    }


def create_connection(config):
    """
    Crear conexión directa a PostgreSQL.
    
    Returns:
        psycopg2.connection: Conexión a la base de datos
    """
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            connect_timeout=10
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        raise


def get_all_tables(conn, schema: str = "public") -> List[str]:
    """
    Obtiene la lista de todas las tablas en el esquema configurado.
    
    Returns:
        List[str]: Lista de nombres de tablas
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """, (schema,))
        
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        logger.info(f"Encontradas {len(tables)} tablas en el esquema '{schema}'")
        return tables
        
    except Exception as e:
        logger.error(f"Error al obtener lista de tablas: {e}")
        raise


def drop_all_tables(conn, tables: List[str], schema: str = "public") -> Dict[str, Any]:
    """
    Elimina todas las tablas de la base de datos.
    
    Args:
        conn: Conexión a la base de datos
        tables: Lista de nombres de tablas a eliminar
        schema: Esquema de la base de datos
        
    Returns:
        Dict con los resultados de la operación
    """
    results = {
        "total_tables": len(tables),
        "dropped_successfully": [],
        "failed_to_drop": [],
        "errors": []
    }
    
    if not tables:
        logger.info("No hay tablas para eliminar")
        return results
    
    try:
        cursor = conn.cursor()
        
        # 1. Deshabilitar temporalmente las restricciones de clave foránea
        logger.info("Deshabilitando restricciones de clave foránea...")
        cursor.execute("SET session_replication_role = replica;")
        
        # 2. Eliminar todas las tablas
        logger.info(f"Iniciando eliminación de {len(tables)} tablas...")
        
        for table_name in tables:
            try:
                # Usar CASCADE para eliminar dependencias automáticamente
                drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE;").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name)
                )
                cursor.execute(drop_query)
                results["dropped_successfully"].append(table_name)
                logger.info(f"✅ Tabla eliminada: {table_name}")
                
            except Exception as e:
                error_msg = f"Error eliminando tabla {table_name}: {e}"
                results["failed_to_drop"].append(table_name)
                results["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")
        
        # 3. Rehabilitar las restricciones de clave foránea
        logger.info("Rehabilitando restricciones de clave foránea...")
        cursor.execute("SET session_replication_role = DEFAULT;")
        
        cursor.close()
        
    except Exception as e:
        logger.error(f"Error crítico durante la eliminación: {e}")
        results["errors"].append(f"Error crítico: {e}")
        raise
    
    return results


def confirm_deletion() -> bool:
    """
    Solicita confirmación del usuario antes de eliminar las tablas.
    
    Returns:
        bool: True si el usuario confirma la eliminación
    """
    print("\n" + "="*60)
    print("⚠️  ADVERTENCIA: ELIMINACIÓN DE TODAS LAS TABLAS ⚠️")
    print("="*60)
    print("Esta operación eliminará TODAS las tablas de la base de datos.")
    print("Esta acción NO ES REVERSIBLE.")
    print("Asegúrate de tener respaldos si necesitas recuperar los datos.")
    print("="*60)
    
    while True:
        response = input("\n¿Estás seguro de que quieres continuar? (sí/no): ").strip().lower()
        if response in ['sí', 'si', 's', 'yes', 'y']:
            return True
        elif response in ['no', 'n']:
            return False
        else:
            print("Por favor responde 'sí' o 'no'")


def test_connection(config) -> bool:
    """
    Probar conexión a la base de datos.
    
    Returns:
        bool: True si la conexión es exitosa
    """
    try:
        conn = create_connection(config)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        print(f"✅ Conexión exitosa!")
        print(f"   PostgreSQL version: {version}")
        return True
        
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        print(f"   Verifica que PostgreSQL esté ejecutándose en {config['host']}:{config['port']}")
        print(f"   Verifica que la base de datos '{config['database']}' exista")
        print(f"   Verifica las credenciales del usuario '{config['user']}'")
        return False


def main():
    """Función principal del script."""
    print("🗃️  Script de eliminación de tablas PostgreSQL")
    print("=" * 50)
    
    try:
        # 1. Cargar configuración
        print("📋 Cargando configuración de base de datos...")
        config = get_database_config()
        
        # 2. Probar conexión
        print("🔍 Probando conexión a la base de datos...")
        if not test_connection(config):
            print("❌ No se pudo conectar a la base de datos. Verifica la configuración.")
            return False
        
        # 3. Crear conexión
        conn = create_connection(config)
        
        # 4. Obtener lista de tablas
        print(f"📊 Obteniendo lista de tablas del esquema '{config['schema']}'...")
        tables = get_all_tables(conn, config["schema"])
        
        if not tables:
            print("ℹ️  No se encontraron tablas para eliminar.")
            conn.close()
            return True
        
        # 5. Mostrar información de las tablas a eliminar
        print(f"\n📋 Tablas encontradas ({len(tables)}):")
        for i, table in enumerate(tables, 1):
            print(f"   {i:2d}. {table}")
        
        # 6. Solicitar confirmación
        if not confirm_deletion():
            print("❌ Operación cancelada por el usuario.")
            conn.close()
            return False
        
        # 7. Eliminar tablas
        print(f"\n🗑️  Eliminando {len(tables)} tablas...")
        results = drop_all_tables(conn, tables, config["schema"])
        
        # 8. Mostrar resultados
        print("\n" + "="*50)
        print("📊 RESULTADOS DE LA ELIMINACIÓN")
        print("="*50)
        print(f"Total de tablas procesadas: {results['total_tables']}")
        print(f"Eliminadas exitosamente: {len(results['dropped_successfully'])}")
        print(f"Fallos en eliminación: {len(results['failed_to_drop'])}")
        
        if results['dropped_successfully']:
            print(f"\n✅ Tablas eliminadas exitosamente:")
            for table in results['dropped_successfully']:
                print(f"   • {table}")
        
        if results['failed_to_drop']:
            print(f"\n❌ Tablas que no se pudieron eliminar:")
            for table in results['failed_to_drop']:
                print(f"   • {table}")
        
        if results['errors']:
            print(f"\n⚠️  Errores encontrados:")
            for error in results['errors']:
                print(f"   • {error}")
        
        # 9. Verificar que se eliminaron todas las tablas
        print(f"\n🔍 Verificando eliminación...")
        remaining_tables = get_all_tables(conn, config["schema"])
        
        if not remaining_tables:
            print("✅ ¡Todas las tablas fueron eliminadas exitosamente!")
            success = True
        else:
            print(f"⚠️  Aún quedan {len(remaining_tables)} tablas:")
            for table in remaining_tables:
                print(f"   • {table}")
            success = False
        
        # 10. Cerrar conexión
        conn.close()
        
        return success
        
    except KeyboardInterrupt:
        print("\n❌ Operación interrumpida por el usuario.")
        return False
    except Exception as e:
        logger.error(f"Error crítico: {e}")
        print(f"❌ Error crítico: {e}")
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Error fatal: {e}")
        sys.exit(1)