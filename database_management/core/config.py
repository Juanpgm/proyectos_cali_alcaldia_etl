"""
Database configuration module using functional programming principles.

Provides immutable configuration objects and functions for database setup.
"""

import os
from typing import Optional, Dict, Any, NamedTuple
from pathlib import Path
from dataclasses import dataclass, field
from functools import lru_cache
import logging

# Cargar variables de entorno desde archivo .env
try:
    from dotenv import load_dotenv
    
    # Buscar el archivo .env en el directorio raíz del proyecto PRIMERO
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)  # override=True para priorizar .env
        print(f"✅ Variables de entorno cargadas desde: {env_path}")
    
    # Luego buscar en el directorio database_management como backup
    env_path_local = Path(__file__).parent.parent / ".env"
    if env_path_local.exists():
        load_dotenv(env_path_local, override=False)  # No sobrescribir las ya cargadas
        print(f"🔄 Variables adicionales desde: {env_path_local}")
    
    if not env_path.exists() and not env_path_local.exists():
        print("⚠️  Archivo .env no encontrado, usando variables del sistema")
        
except ImportError:
    print("⚠️  python-dotenv no disponible, usando variables del sistema")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatabaseConfig:
    """Immutable database configuration."""
    
    # Valores por defecto genéricos - los reales vienen del .env
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "postgres"
    schema: str = "public"
    database_url: Optional[str] = None
    
    # Connection settings
    pool_size: int = 5
    max_overflow: int = 10
    timeout: int = 30
    
    # PostGIS settings
    enable_postgis: bool = True
    postgis_version: str = "3.3"
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not (1 <= self.port <= 65535):
            raise ValueError(f"Invalid port: {self.port}")
        
        if self.pool_size <= 0:
            raise ValueError(f"Pool size must be positive: {self.pool_size}")
    
    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        if self.database_url:
            url = self.database_url
            if url.startswith("postgres://"):
                url = url.replace("postgres://", "postgresql+psycopg2://", 1)
            elif url.startswith("postgresql://"):
                url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
            return url
        
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def connection_info(self) -> Dict[str, Any]:
        """Get safe connection information (without password)."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "schema": self.schema,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "timeout": self.timeout,
            "enable_postgis": self.enable_postgis
        }


@lru_cache(maxsize=1)
def get_database_config() -> DatabaseConfig:
    """
    Get database configuration from environment variables.
    
    Busca variables en este orden de prioridad:
    1. Variables específicas del ETL (DB_*)
    2. Variables estándar PostgreSQL (POSTGRES_*)
    3. Valores por defecto
    
    Returns:
        DatabaseConfig: Immutable configuration object
    """
    # Buscar variables específicas del ETL primero, luego las estándar PostgreSQL
    host = (os.getenv("DB_HOST") or 
            os.getenv("POSTGRES_SERVER") or 
            "localhost")  # Valor por defecto genérico
    
    port = int(os.getenv("DB_PORT") or 
               os.getenv("POSTGRES_PORT") or 
               "5432")
    
    database = (os.getenv("DB_NAME") or 
                os.getenv("POSTGRES_DB") or 
                "postgres")  # Valor por defecto genérico
    
    user = (os.getenv("DB_USER") or 
            os.getenv("POSTGRES_USER") or 
            "postgres")
    
    password = (os.getenv("DB_PASSWORD") or 
                os.getenv("POSTGRES_PASSWORD") or 
                "postgres")  # Valor por defecto genérico
    
    # Mostrar configuración detectada (sin password)
    print(f"🔧 Configuración de BD detectada:")
    print(f"   Host: {host}")
    print(f"   Puerto: {port}")
    print(f"   Base de datos: {database}")
    print(f"   Usuario: {user}")
    print(f"   Password: {'*' * len(password) if password else '(no configurado)'}")
    
    return DatabaseConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        schema=os.getenv("DB_SCHEMA", "public"),
        database_url=os.getenv("DATABASE_URL"),
        
        pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
        max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
        timeout=int(os.getenv("DB_TIMEOUT", "30")),
        
        enable_postgis=os.getenv("ENABLE_POSTGIS", "true").lower() == "true",
        postgis_version=os.getenv("POSTGIS_VERSION", "3.3")
    )


def validate_config(config: DatabaseConfig) -> bool:
    """
    Validate database configuration.
    
    Args:
        config: Database configuration to validate
        
    Returns:
        bool: True if configuration is valid
        
    Raises:
        ValueError: If configuration is invalid
    """
    try:
        # Validate basic parameters
        if not config.host:
            raise ValueError("Database host cannot be empty")
        
        if not config.database:
            raise ValueError("Database name cannot be empty")
        
        if not config.user:
            raise ValueError("Database user cannot be empty")
        
        # Log configuration (without sensitive data)
        logger.info(f"Database configuration validated: {config.connection_info}")
        
        return True
        
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        raise


def create_config_from_dict(config_dict: Dict[str, Any]) -> DatabaseConfig:
    """
    Create database configuration from dictionary.
    
    Args:
        config_dict: Dictionary with configuration parameters
        
    Returns:
        DatabaseConfig: Immutable configuration object
    """
    return DatabaseConfig(**config_dict)


def merge_configs(base_config: DatabaseConfig, overrides: Dict[str, Any]) -> DatabaseConfig:
    """
    Merge configuration with overrides using functional approach.
    
    Args:
        base_config: Base configuration
        overrides: Override values
        
    Returns:
        DatabaseConfig: New configuration with merged values
    """
    base_dict = {
        "host": base_config.host,
        "port": base_config.port,
        "database": base_config.database,
        "user": base_config.user,
        "password": base_config.password,
        "schema": base_config.schema,
        "database_url": base_config.database_url,
        "pool_size": base_config.pool_size,
        "max_overflow": base_config.max_overflow,
        "timeout": base_config.timeout,
        "enable_postgis": base_config.enable_postgis,
        "postgis_version": base_config.postgis_version
    }
    
    # Update with overrides
    base_dict.update(overrides)
    
    return DatabaseConfig(**base_dict)


def test_connection(config: DatabaseConfig = None) -> bool:
    """
    Test database connection with current configuration.
    
    Args:
        config: Database configuration to test (optional)
        
    Returns:
        bool: True if connection successful
    """
    if config is None:
        config = get_database_config()
    
    try:
        import psycopg2
        
        print(f"🔍 Probando conexión a PostgreSQL...")
        print(f"   Host: {config.host}:{config.port}")
        print(f"   Base de datos: {config.database}")
        print(f"   Usuario: {config.user}")
        
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"✅ Conexión exitosa!")
        print(f"   PostgreSQL version: {version}")
        return True
        
    except ImportError:
        print("❌ psycopg2 no está instalado. Instalar con: pip install psycopg2-binary")
        return False
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        print(f"   Verifica que PostgreSQL esté ejecutándose en {config.host}:{config.port}")
        print(f"   Verifica que la base de datos '{config.database}' exista")
        print(f"   Verifica las credenciales del usuario '{config.user}'")
        return False