"""
Database manager module with functional programming approach.

Provides intelligent database management, auto-diagnosis, and self-repair capabilities.
"""

import logging
from typing import Optional, List, Dict, Any, Callable, Iterator
from pathlib import Path
import time
from functools import wraps, partial
from contextlib import contextmanager
import sqlalchemy as sa
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError, ProgrammingError

from .config import DatabaseConfig
# from .schema_generator import TableSchema  # Commented out as module doesn't exist

logger = logging.getLogger(__name__)


def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """
    Decorator for retrying database operations on failure.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Delay between retries in seconds
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (OperationalError, ProgrammingError) as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed")
                        raise last_exception
                        
            return None
        return wrapper
    return decorator


class DatabaseManager:
    """Functional database manager with self-diagnosis and repair capabilities."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._engine: Optional[sa.Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        self._is_connected = False
        
    @property
    def engine(self) -> sa.Engine:
        """Get database engine, creating if necessary."""
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine
    
    @property
    def session_factory(self) -> sessionmaker:
        """Get session factory, creating if necessary."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory
    
    def _create_engine(self) -> sa.Engine:
        """Create SQLAlchemy engine with optimal settings."""
        engine_kwargs = {
            "poolclass": QueuePool,
            "pool_size": self.config.pool_size,
            "max_overflow": self.config.max_overflow,
            "pool_pre_ping": True,  # Validate connections
            "pool_recycle": 3600,   # Recycle connections every hour
            "echo": False,          # Set to True for SQL debugging
        }
        
        try:
            engine = create_engine(self.config.connection_string, **engine_kwargs)
            logger.info(f"Created database engine: {self.config.connection_info}")
            return engine
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise
    
    @retry_on_failure(max_retries=3, delay=2.0)
    def connect(self) -> bool:
        """
        Connect to database with automatic retry.
        
        Returns:
            bool: True if connection successful
        """
        try:
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self._is_connected = True
            logger.info("Database connection established successfully")
            return True
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            self._is_connected = False
            raise
    
    def disconnect(self) -> None:
        """Disconnect from database and cleanup resources."""
        try:
            if self._engine:
                self._engine.dispose()
                self._engine = None
                self._session_factory = None
                self._is_connected = False
                logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
    
    @contextmanager
    def get_session(self) -> Iterator[Session]:
        """
        Get database session with automatic cleanup.
        
        Yields:
            Session: SQLAlchemy session
        """
        if not self._is_connected:
            self.connect()
        
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session error: {e}")
            raise
        finally:
            session.close()
    
    def execute_sql(self, sql: str, params: Optional[Dict] = None) -> Any:
        """
        Execute SQL statement safely.
        
        Args:
            sql: SQL statement to execute
            params: Optional parameters for the SQL
            
        Returns:
            Result of the SQL execution
        """
        with self.get_session() as session:
            try:
                result = session.execute(text(sql), params or {})
                return result.fetchall() if result.returns_rows else result.rowcount
            except Exception as e:
                logger.error(f"SQL execution failed: {sql[:100]}... Error: {e}")
                raise
    
    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Check if table exists in database.
        
        Args:
            table_name: Name of the table
            schema: Schema name (optional)
            
        Returns:
            bool: True if table exists
        """
        try:
            inspector = inspect(self.engine)
            schema = schema or self.config.schema
            tables = inspector.get_table_names(schema=schema)
            return table_name in tables
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def get_table_info(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Get detailed information about a table.
        
        Args:
            table_name: Name of the table
            schema: Schema name (optional)
            
        Returns:
            Dict with table information
        """
        try:
            inspector = inspect(self.engine)
            schema = schema or self.config.schema
            
            if not self.table_exists(table_name, schema):
                return {"exists": False}
            
            columns = inspector.get_columns(table_name, schema=schema)
            indexes = inspector.get_indexes(table_name, schema=schema)
            foreign_keys = inspector.get_foreign_keys(table_name, schema=schema)
            primary_key = inspector.get_pk_constraint(table_name, schema=schema)
            
            return {
                "exists": True,
                "columns": columns,
                "indexes": indexes,
                "foreign_keys": foreign_keys,
                "primary_key": primary_key,
                "column_count": len(columns),
                "index_count": len(indexes)
            }
            
        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {e}")
            return {"exists": False, "error": str(e)}
    
    # def create_table_from_schema(self, schema: TableSchema) -> bool:
    #     """
    #     Create table from schema definition.
    #     
    #     Args:
    #         schema: Table schema to create
    #         
    #     Returns:
    #         bool: True if successful
    #     """
    #     try:
    #         # Create table
    #         create_sql = schema.to_create_sql()
    #         self.execute_sql(create_sql)
    #         logger.info(f"Created table: {schema.name}")
    #         
    #         # Create indexes
    #         for index_sql in schema.to_index_sql():
    #             try:
    #                 self.execute_sql(index_sql)
    #             except Exception as e:
    #                 logger.warning(f"Index creation failed: {e}")
    #         
    #         return True
    #         
    #     except Exception as e:
    #         logger.error(f"Failed to create table {schema.name}: {e}")
    #         return False
    
    def setup_postgis(self) -> bool:
        """
        Setup PostGIS extension if enabled.
        
        Returns:
            bool: True if successful
        """
        if not self.config.enable_postgis:
            return True
        
        try:
            # Check if PostGIS is already installed
            result = self.execute_sql(
                "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'postgis')"
            )
            
            if result and result[0][0]:
                logger.info("PostGIS extension already installed")
                return True
            
            # Install PostGIS
            self.execute_sql("CREATE EXTENSION IF NOT EXISTS postgis")
            self.execute_sql("CREATE EXTENSION IF NOT EXISTS postgis_topology")
            
            logger.info("PostGIS extension installed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup PostGIS: {e}")
            return False
    
    def diagnose_database(self) -> Dict[str, Any]:
        """
        Perform comprehensive database diagnosis.
        
        Returns:
            Dict with diagnosis results
        """
        diagnosis = {
            "connection": False,
            "postgis": False,
            "tables": {},
            "performance": {},
            "issues": [],
            "recommendations": []
        }
        
        try:
            # Test connection
            diagnosis["connection"] = self._is_connected or self.connect()
            
            if not diagnosis["connection"]:
                diagnosis["issues"].append("Cannot connect to database")
                return diagnosis
            
            # Check PostGIS
            if self.config.enable_postgis:
                postgis_result = self.execute_sql(
                    "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'postgis')"
                )
                diagnosis["postgis"] = bool(postgis_result and postgis_result[0][0])
                
                if not diagnosis["postgis"]:
                    diagnosis["issues"].append("PostGIS extension not installed")
                    diagnosis["recommendations"].append("Install PostGIS extension")
            
            # Check tables
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema=self.config.schema)
            
            for table in tables:
                diagnosis["tables"][table] = self.get_table_info(table)
            
            # Performance checks
            diagnosis["performance"] = self._check_performance()
            
            logger.info(f"Database diagnosis completed: {len(diagnosis['issues'])} issues found")
            
        except Exception as e:
            diagnosis["issues"].append(f"Diagnosis failed: {e}")
            logger.error(f"Database diagnosis failed: {e}")
        
        return diagnosis
    
    def _check_performance(self) -> Dict[str, Any]:
        """Check database performance metrics."""
        try:
            # Get database size
            size_result = self.execute_sql(
                f"SELECT pg_size_pretty(pg_database_size('{self.config.database}'))"
            )
            database_size = size_result[0][0] if size_result else "unknown"
            
            # Get connection count
            conn_result = self.execute_sql(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = %s",
                {"datname": self.config.database}
            )
            connection_count = conn_result[0][0] if conn_result else 0
            
            return {
                "database_size": database_size,
                "active_connections": connection_count,
                "max_connections": self.config.pool_size + self.config.max_overflow
            }
            
        except Exception as e:
            logger.error(f"Performance check failed: {e}")
            return {"error": str(e)}
    
    def repair_database(self) -> Dict[str, Any]:
        """
        Attempt to repair common database issues.
        
        Returns:
            Dict with repair results
        """
        repair_results = {
            "actions_taken": [],
            "success": [],
            "failures": [],
            "overall_success": False
        }
        
        try:
            diagnosis = self.diagnose_database()
            
            # Repair PostGIS if needed
            if self.config.enable_postgis and not diagnosis["postgis"]:
                repair_results["actions_taken"].append("Installing PostGIS")
                if self.setup_postgis():
                    repair_results["success"].append("PostGIS installed successfully")
                else:
                    repair_results["failures"].append("Failed to install PostGIS")
            
            # Check for missing indexes (future enhancement)
            # Check for orphaned data (future enhancement)
            # Vacuum analyze if needed (future enhancement)
            
            repair_results["overall_success"] = len(repair_results["failures"]) == 0
            
        except Exception as e:
            repair_results["failures"].append(f"Repair process failed: {e}")
            logger.error(f"Database repair failed: {e}")
        
        return repair_results


def create_database_manager(config: DatabaseConfig) -> DatabaseManager:
    """
    Factory function to create database manager.
    
    Args:
        config: Database configuration
        
    Returns:
        DatabaseManager: Configured database manager
    """
    return DatabaseManager(config)


def test_connection(config: DatabaseConfig) -> bool:
    """
    Test database connection.
    
    Args:
        config: Database configuration
        
    Returns:
        bool: True if connection successful
    """
    try:
        manager = create_database_manager(config)
        return manager.connect()
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False


def get_database_stats(manager: DatabaseManager) -> Dict[str, Any]:
    """
    Get comprehensive database statistics.
    
    Args:
        manager: Database manager instance
        
    Returns:
        Dict with database statistics
    """
    try:
        with manager.get_session() as session:
            # Get table counts
            inspector = inspect(manager.engine)
            tables = inspector.get_table_names(schema=manager.config.schema)
            
            table_stats = {}
            for table in tables:
                try:
                    count_result = session.execute(text(f'SELECT COUNT(*) FROM "{table}"'))
                    table_stats[table] = count_result.scalar()
                except Exception as e:
                    table_stats[table] = f"Error: {e}"
            
            return {
                "table_count": len(tables),
                "table_stats": table_stats,
                "schema": manager.config.schema,
                "connection_info": manager.config.connection_info
            }
            
    except Exception as e:
        logger.error(f"Failed to get database stats: {e}")
        return {"error": str(e)}