"""
Core database management module for ETL system.

This module provides functional programming approach for database management,
automated schema generation, intelligent data loading, monitoring, reporting,
and schema analysis.

Integrates the best features from gestor_proyectos_db using pure functional programming.
"""

# Core database configuration and connection
from .config import DatabaseConfig, get_database_config, test_connection, validate_config

# ETL system components
from .schema_generator import SchemaGenerator, generate_schema_from_data
from .data_loader import DataLoader, load_data_functional, create_data_loader
from .database_manager import DatabaseManager, create_database_manager
from .model_generator import ModelGenerator, generate_models_from_json
from .etl_system import IntelligentETL, create_etl_system, run_etl_from_config

# Integrated monitoring, reporting, and schema analysis
from .monitoring import monitor_database, MonitoringReport, MonitoringStatus
from .reporting import generate_database_report, ReportType, DatabaseReport
from .schema_analysis import run_schema_analysis, SchemaAnalysisReport, TableStatus

# Convenience imports for common operations
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging


# ============================================================================
# UNIFIED DATABASE MANAGER WITH INTEGRATED FEATURES
# ============================================================================

class IntegratedDatabaseManager:
    """
    Unified Database Manager with Integrated Monitoring, Reporting, and Schema Analysis
    
    This class provides a clean interface to all database management operations
    while maintaining functional programming principles. It integrates the best
    features from gestor_proyectos_db with the existing ETL system.
    """
    
    def __init__(self, config=None):
        """
        Initialize integrated database manager
        
        Args:
            config: Database configuration (optional, will use .env if not provided)
        """
        self.config = config or get_database_config()
        self.logger = self._setup_logging()
        
        # Initialize ETL components
        self.etl_manager = None
        self.schema_generator = None
        self.data_loader = None
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for database manager"""
        logger = logging.getLogger('integrated_database_manager')
        
        if not logger.handlers:
            # Create logs directory
            log_dir = Path(__file__).parent.parent / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # Setup log file
            log_file = log_dir / f"integrated_db_manager_{datetime.now().strftime('%Y%m%d')}.log"
            
            handler = logging.FileHandler(log_file)
            handler.setLevel(logging.INFO)
            
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        
        return logger
    
    # ========================================================================
    # CONNECTION AND CONFIGURATION
    # ========================================================================
    
    def test_connection(self) -> bool:
        """Test database connection"""
        return test_connection(self.config)
    
    def validate_configuration(self) -> bool:
        """Validate database configuration"""
        try:
            return validate_config(self.config)
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get safe connection information (without password)"""
        return self.config.connection_info
    
    # ========================================================================
    # MONITORING OPERATIONS
    # ========================================================================
    
    def run_health_check(self, save_report: bool = True) -> MonitoringReport:
        """Run comprehensive health check"""
        self.logger.info("Running database health check")
        
        try:
            report = monitor_database(self.config, save_report)
            self.logger.info(f"Health check completed - Status: {report.overall_status.value}")
            return report
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            raise
    
    def get_system_status(self) -> str:
        """Get current system status (quick check)"""
        try:
            if self.test_connection():
                return "healthy"
            else:
                return "connection_failed"
        except Exception:
            return "error"
    
    # ========================================================================
    # REPORTING OPERATIONS
    # ========================================================================
    
    def generate_health_summary(self, days: int = 7) -> DatabaseReport:
        """Generate health summary report"""
        self.logger.info(f"Generating health summary for {days} days")
        try:
            return generate_database_report(ReportType.HEALTH_SUMMARY, days)
        except Exception as e:
            self.logger.error(f"Failed to generate health summary: {e}")
            raise
    
    def generate_performance_report(self, days: int = 7) -> DatabaseReport:
        """Generate performance analysis report"""
        self.logger.info(f"Generating performance report for {days} days")
        try:
            return generate_database_report(ReportType.PERFORMANCE_ANALYSIS, days)
        except Exception as e:
            self.logger.error(f"Failed to generate performance report: {e}")
            raise
    
    def generate_executive_summary(self, days: int = 30) -> DatabaseReport:
        """Generate executive summary report"""
        self.logger.info(f"Generating executive summary for {days} days")
        try:
            return generate_database_report(ReportType.EXECUTIVE_SUMMARY, days)
        except Exception as e:
            self.logger.error(f"Failed to generate executive summary: {e}")
            raise
    
    # ========================================================================
    # SCHEMA OPERATIONS
    # ========================================================================
    
    def analyze_schema(self, save_report: bool = True) -> SchemaAnalysisReport:
        """Analyze database schema"""
        self.logger.info("Analyzing database schema")
        try:
            report = run_schema_analysis(self.config, save_report)
            self.logger.info(f"Schema analysis completed - Status: {report.overall_status}")
            return report
        except Exception as e:
            self.logger.error(f"Schema analysis failed: {e}")
            raise
    
    def check_schema_completeness(self) -> bool:
        """Check if database schema is complete"""
        try:
            report = self.analyze_schema(save_report=False)
            return report.overall_status == "complete"
        except Exception:
            return False
    
    def get_missing_tables(self) -> List[str]:
        """Get list of missing tables"""
        try:
            report = self.analyze_schema(save_report=False)
            return list(report.missing_tables)
        except Exception:
            return []
    
    # ========================================================================
    # ETL OPERATIONS
    # ========================================================================
    
    def initialize_etl_system(self):
        """Initialize ETL system components"""
        if self.etl_manager is None:
            self.etl_manager = create_database_manager(self.config)
            self.schema_generator = SchemaGenerator(self.etl_manager)
            self.data_loader = create_data_loader(self.etl_manager)
        
        return self.etl_manager
    
    def run_etl_process(self, config_path: str = None) -> Dict[str, Any]:
        """Run ETL process"""
        self.logger.info("Running ETL process")
        
        try:
            self.initialize_etl_system()
            
            if config_path:
                return run_etl_from_config(config_path)
            else:
                # Use default ETL configuration
                etl_system = create_etl_system(self.config)
                return {"status": "ETL system initialized", "manager": etl_system}
        except Exception as e:
            self.logger.error(f"ETL process failed: {e}")
            raise
    
    # ========================================================================
    # COMPREHENSIVE OPERATIONS
    # ========================================================================
    
    def run_full_diagnostic(self, days: int = 7) -> Dict[str, Any]:
        """Run complete database diagnostic"""
        self.logger.info(f"Running full database diagnostic ({days} days)")
        
        diagnostic_results = {
            "timestamp": datetime.now().isoformat(),
            "configuration": {
                "valid": False,
                "connection": False,
                "details": {}
            },
            "health_check": None,
            "schema_analysis": None,
            "performance_summary": None,
            "etl_status": None,
            "recommendations": []
        }
        
        try:
            # 1. Validate configuration
            diagnostic_results["configuration"]["valid"] = self.validate_configuration()
            diagnostic_results["configuration"]["connection"] = self.test_connection()
            diagnostic_results["configuration"]["details"] = self.get_connection_info()
            
            if not diagnostic_results["configuration"]["connection"]:
                diagnostic_results["recommendations"].append("Fix database connection issues")
                return diagnostic_results
            
            # 2. Run health check
            health_report = self.run_health_check(save_report=False)
            diagnostic_results["health_check"] = {
                "status": health_report.overall_status.value,
                "checks_performed": len(health_report.checks),
                "alerts": len(health_report.alerts),
                "execution_time": health_report.execution_time
            }
            
            # 3. Analyze schema
            schema_report = self.analyze_schema(save_report=False)
            diagnostic_results["schema_analysis"] = {
                "status": schema_report.overall_status,
                "missing_tables": len(schema_report.missing_tables),
                "existing_tables": len(schema_report.existing_tables),
                "execution_time": schema_report.execution_time
            }
            
            # 4. Check ETL system
            try:
                self.initialize_etl_system()
                diagnostic_results["etl_status"] = {
                    "initialized": True,
                    "components": ["database_manager", "schema_generator", "data_loader"]
                }
            except Exception as e:
                diagnostic_results["etl_status"] = {
                    "initialized": False,
                    "error": str(e)
                }
            
            # 5. Generate performance summary
            try:
                perf_report = self.generate_performance_report(days)
                diagnostic_results["performance_summary"] = {
                    "status": perf_report.overall_status,
                    "average_latency": perf_report.key_metrics.get("average_latency_ms", 0),
                    "execution_time": perf_report.execution_time
                }
            except Exception as e:
                diagnostic_results["performance_summary"] = {"error": str(e)}
            
            # 6. Compile recommendations
            recommendations = []
            recommendations.extend(health_report.recommendations)
            recommendations.extend(schema_report.recommendations)
            
            # Remove duplicates and limit
            seen = set()
            unique_recommendations = []
            for rec in recommendations:
                if rec not in seen and len(unique_recommendations) < 10:
                    seen.add(rec)
                    unique_recommendations.append(rec)
            
            diagnostic_results["recommendations"] = unique_recommendations
            
            self.logger.info("Full diagnostic completed successfully")
            return diagnostic_results
            
        except Exception as e:
            self.logger.error(f"Full diagnostic failed: {e}")
            diagnostic_results["error"] = str(e)
            return diagnostic_results
    
    def ensure_system_health(self) -> bool:
        """Ensure system is healthy and operational"""
        try:
            # Quick health check
            if not self.test_connection():
                return False
            
            # Check schema completeness
            if not self.check_schema_completeness():
                self.logger.warning("Schema is incomplete")
                return False
            
            # Run health monitoring
            health_report = self.run_health_check(save_report=False)
            
            return health_report.overall_status in [MonitoringStatus.HEALTHY, MonitoringStatus.WARNING]
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def quick_health_check() -> Dict[str, Any]:
    """Convenience function: Quick health check"""
    manager = IntegratedDatabaseManager()
    
    return {
        "connection": manager.test_connection(),
        "schema_complete": manager.check_schema_completeness(),
        "status": manager.get_system_status(),
        "timestamp": datetime.now().isoformat()
    }


def comprehensive_analysis(days: int = 7) -> Dict[str, Any]:
    """Convenience function: Run comprehensive analysis"""
    manager = IntegratedDatabaseManager()
    return manager.run_full_diagnostic(days)


def ensure_database_ready() -> bool:
    """Convenience function: Ensure database is ready for operations"""
    manager = IntegratedDatabaseManager()
    return manager.ensure_system_health()


# ============================================================================
# UPDATED EXPORTS
# ============================================================================

__all__ = [
    # Core configuration and connection
    'DatabaseConfig',
    'get_database_config',
    'test_connection',
    'validate_config',
    
    # ETL system components
    'SchemaGenerator', 
    'generate_schema_from_data',
    'DataLoader',
    'load_data_functional',
    'create_data_loader',
    'DatabaseManager',
    'create_database_manager',
    'ModelGenerator',
    'generate_models_from_json',
    'IntelligentETL',
    'create_etl_system',
    'run_etl_from_config',
    
    # Integrated monitoring, reporting, and schema analysis
    'monitor_database',
    'generate_database_report',
    'run_schema_analysis',
    'MonitoringReport',
    'DatabaseReport', 
    'SchemaAnalysisReport',
    'MonitoringStatus',
    'ReportType',
    'TableStatus',
    
    # Unified manager
    'IntegratedDatabaseManager',
    
    # Convenience functions
    'quick_health_check',
    'comprehensive_analysis',
    'ensure_database_ready'
]