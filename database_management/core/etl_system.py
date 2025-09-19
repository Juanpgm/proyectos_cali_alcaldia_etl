"""
Intelligent ETL system with auto-diagnosis and self-repair capabilities.

Main orchestrator for the database management system with functional programming approach.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
import json

from .config import DatabaseConfig, get_database_config
from .database_manager import DatabaseManager, create_database_manager
from .data_loader import DataLoader, BatchLoadResult
from .schema_generator import SchemaGenerator
from .model_generator import ModelGenerator

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ETLResult:
    """Result of ETL operation."""
    success: bool
    tables_created: int
    records_loaded: int
    execution_time: float
    errors: Tuple[str, ...] = ()
    warnings: Tuple[str, ...] = ()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "tables_created": self.tables_created,
            "records_loaded": self.records_loaded,
            "execution_time": self.execution_time,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "timestamp": datetime.utcnow().isoformat()
        }


class IntelligentETL:
    """Intelligent ETL system with auto-diagnosis and repair."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or get_database_config()
        self.db_manager = create_database_manager(self.config)
        self.schema_generator = SchemaGenerator()
        self.model_generator = ModelGenerator()
        self.data_loader = DataLoader(self.db_manager)
        
        # Setup logging
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('etl_system.log')
            ]
        )
    
    def diagnose_system(self) -> Dict[str, Any]:
        """
        Perform comprehensive system diagnosis.
        
        Returns:
            Dict with diagnosis results
        """
        logger.info("Starting system diagnosis...")
        
        diagnosis = {
            "timestamp": datetime.utcnow().isoformat(),
            "config": self.config.connection_info,
            "database": {},
            "data_files": {},
            "system_health": "unknown",
            "recommendations": []
        }
        
        try:
            # Database diagnosis
            diagnosis["database"] = self.db_manager.diagnose_database()
            
            # Check data files in transformation outputs
            data_dir = Path("transformation_app/app_outputs")
            if data_dir.exists():
                diagnosis["data_files"] = self._diagnose_data_files(data_dir)
            else:
                diagnosis["data_files"]["error"] = f"Data directory not found: {data_dir}"
            
            # Overall health assessment
            diagnosis["system_health"] = self._assess_system_health(diagnosis)
            
            # Generate recommendations
            diagnosis["recommendations"] = self._generate_recommendations(diagnosis)
            
            logger.info(f"System diagnosis completed. Health: {diagnosis['system_health']}")
            
        except Exception as e:
            diagnosis["error"] = str(e)
            diagnosis["system_health"] = "critical"
            logger.error(f"System diagnosis failed: {e}")
        
        return diagnosis
    
    def _diagnose_data_files(self, data_dir: Path) -> Dict[str, Any]:
        """Diagnose data files in the outputs directory."""
        file_info = {
            "directories": [],
            "total_files": 0,
            "file_types": {},
            "largest_files": [],
            "issues": []
        }
        
        try:
            for subdir in data_dir.iterdir():
                if subdir.is_dir():
                    dir_info = {
                        "name": subdir.name,
                        "files": [],
                        "total_size": 0
                    }
                    
                    for file_path in subdir.glob("*"):
                        if file_path.is_file() and file_path.suffix.lower() in ['.json', '.geojson']:
                            size = file_path.stat().st_size
                            dir_info["files"].append({
                                "name": file_path.name,
                                "size": size,
                                "type": file_path.suffix.lower()
                            })
                            dir_info["total_size"] += size
                            file_info["total_files"] += 1
                            
                            # Track file types
                            ext = file_path.suffix.lower()
                            file_info["file_types"][ext] = file_info["file_types"].get(ext, 0) + 1
                    
                    file_info["directories"].append(dir_info)
            
            # Find largest files
            all_files = []
            for dir_info in file_info["directories"]:
                for file_data in dir_info["files"]:
                    file_data["directory"] = dir_info["name"]
                    all_files.append(file_data)
            
            file_info["largest_files"] = sorted(all_files, key=lambda x: x["size"], reverse=True)[:5]
            
        except Exception as e:
            file_info["error"] = str(e)
        
        return file_info
    
    def _assess_system_health(self, diagnosis: Dict[str, Any]) -> str:
        """Assess overall system health based on diagnosis."""
        db_health = diagnosis.get("database", {})
        
        # Critical issues
        if not db_health.get("connection", False):
            return "critical"
        
        # Major issues
        issues = len(db_health.get("issues", []))
        if issues > 3:
            return "poor"
        elif issues > 1:
            return "fair"
        elif issues > 0:
            return "good"
        else:
            return "excellent"
    
    def _generate_recommendations(self, diagnosis: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on diagnosis."""
        recommendations = []
        
        db_diagnosis = diagnosis.get("database", {})
        
        # Database recommendations
        if not db_diagnosis.get("connection", False):
            recommendations.append("Fix database connection issues")
        
        if self.config.enable_postgis and not db_diagnosis.get("postgis", False):
            recommendations.append("Install PostGIS extension")
        
        issues = db_diagnosis.get("issues", [])
        for issue in issues:
            recommendations.append(f"Resolve: {issue}")
        
        # Data file recommendations
        data_files = diagnosis.get("data_files", {})
        if "error" in data_files:
            recommendations.append("Check data files directory structure")
        elif data_files.get("total_files", 0) == 0:
            recommendations.append("No data files found - check transformation outputs")
        
        return recommendations
    
    def repair_system(self) -> Dict[str, Any]:
        """
        Attempt to repair system issues.
        
        Returns:
            Dict with repair results
        """
        logger.info("Starting system repair...")
        
        repair_results = {
            "timestamp": datetime.utcnow().isoformat(),
            "actions_performed": [],
            "successes": [],
            "failures": [],
            "overall_success": False
        }
        
        try:
            # Perform database repairs
            db_repair = self.db_manager.repair_database()
            repair_results["actions_performed"].extend(db_repair["actions_taken"])
            repair_results["successes"].extend(db_repair["success"])
            repair_results["failures"].extend(db_repair["failures"])
            
            # Additional repairs can be added here
            
            repair_results["overall_success"] = len(repair_results["failures"]) == 0
            
            logger.info(f"System repair completed. Success: {repair_results['overall_success']}")
            
        except Exception as e:
            repair_results["failures"].append(f"Repair process failed: {e}")
            logger.error(f"System repair failed: {e}")
        
        return repair_results
    
    def initialize_database(self) -> bool:
        """
        Initialize database with PostGIS and basic setup.
        
        Returns:
            bool: True if successful
        """
        logger.info("Initializing database...")
        
        try:
            # Connect to database
            if not self.db_manager.connect():
                logger.error("Failed to connect to database")
                return False
            
            # Setup PostGIS if enabled
            if self.config.enable_postgis:
                if not self.db_manager.setup_postgis():
                    logger.warning("PostGIS setup failed")
            
            logger.info("Database initialization completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            return False
    
    def process_data_directory(self, data_dir: Path) -> ETLResult:
        """
        Process all data files in a directory.
        
        Args:
            data_dir: Path to directory containing data files
            
        Returns:
            ETLResult with processing results
        """
        logger.info(f"Processing data directory: {data_dir}")
        start_time = datetime.utcnow()
        
        errors = []
        warnings = []
        total_tables_created = 0
        total_records_loaded = 0
        
        try:
            # Initialize database if needed
            if not self.initialize_database():
                errors.append("Database initialization failed")
                return ETLResult(
                    success=False,
                    tables_created=0,
                    records_loaded=0,
                    execution_time=0.0,
                    errors=tuple(errors)
                )
            
            # Process each subdirectory
            for subdir in data_dir.iterdir():
                if subdir.is_dir():
                    logger.info(f"Processing subdirectory: {subdir.name}")
                    
                    try:
                        # Load data from subdirectory
                        batch_result = self.data_loader.load_from_directory(subdir)
                        
                        # Count successful table creations
                        successful_loads = [r for r in batch_result.results if r.is_successful]
                        total_tables_created += len(successful_loads)
                        total_records_loaded += batch_result.total_loaded
                        
                        # Collect errors from failed loads
                        failed_loads = [r for r in batch_result.results if not r.is_successful]
                        for failed_load in failed_loads:
                            errors.extend(failed_load.errors)
                        
                        logger.info(
                            f"Subdirectory {subdir.name}: "
                            f"{len(successful_loads)} tables created, "
                            f"{batch_result.total_loaded} records loaded"
                        )
                        
                    except Exception as e:
                        error_msg = f"Failed to process {subdir.name}: {e}"
                        errors.append(error_msg)
                        logger.error(error_msg)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            success = len(errors) == 0 and total_records_loaded > 0
            
            logger.info(
                f"Data directory processing completed: "
                f"{total_tables_created} tables, {total_records_loaded} records "
                f"in {execution_time:.2f}s"
            )
            
            return ETLResult(
                success=success,
                tables_created=total_tables_created,
                records_loaded=total_records_loaded,
                execution_time=execution_time,
                errors=tuple(errors),
                warnings=tuple(warnings)
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            error_msg = f"Data directory processing failed: {e}"
            errors.append(error_msg)
            logger.error(error_msg)
            
            return ETLResult(
                success=False,
                tables_created=total_tables_created,
                records_loaded=total_records_loaded,
                execution_time=execution_time,
                errors=tuple(errors),
                warnings=tuple(warnings)
            )
    
    def run_full_etl(self, data_dir: Optional[Path] = None) -> ETLResult:
        """
        Run complete ETL process.
        
        Args:
            data_dir: Optional data directory path
            
        Returns:
            ETLResult with processing results
        """
        if data_dir is None:
            data_dir = Path("transformation_app/app_outputs")
        
        logger.info("Starting full ETL process...")
        
        try:
            # Diagnose system first
            diagnosis = self.diagnose_system()
            
            # Attempt repairs if needed
            if diagnosis["system_health"] in ["critical", "poor"]:
                logger.info("System health is poor, attempting repairs...")
                repair_result = self.repair_system()
                
                if not repair_result["overall_success"]:
                    logger.warning("System repair was not completely successful")
            
            # Process data
            result = self.process_data_directory(data_dir)
            
            # Save results
            self._save_etl_results(result, diagnosis)
            
            return result
            
        except Exception as e:
            logger.error(f"Full ETL process failed: {e}")
            return ETLResult(
                success=False,
                tables_created=0,
                records_loaded=0,
                execution_time=0.0,
                errors=(f"ETL process failed: {e}",)
            )
    
    def _save_etl_results(self, result: ETLResult, diagnosis: Dict[str, Any]) -> None:
        """Save ETL results to file."""
        try:
            results_dir = Path("etl_results")
            results_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            results_file = results_dir / f"etl_result_{timestamp}.json"
            
            full_results = {
                "etl_result": result.to_dict(),
                "diagnosis": diagnosis,
                "config": self.config.connection_info
            }
            
            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump(full_results, f, indent=2, ensure_ascii=False)
            
            logger.info(f"ETL results saved to: {results_file}")
            
        except Exception as e:
            logger.error(f"Failed to save ETL results: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        Get current system status.
        
        Returns:
            Dict with system status information
        """
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "config": self.config.connection_info,
            "database_connected": self.db_manager._is_connected,
            "diagnosis": self.diagnose_system()
        }


def create_etl_system(config: Optional[DatabaseConfig] = None) -> IntelligentETL:
    """
    Factory function to create ETL system.
    
    Args:
        config: Optional database configuration
        
    Returns:
        IntelligentETL: Configured ETL system
    """
    return IntelligentETL(config)


def run_etl_from_config(config_file: Optional[Path] = None) -> ETLResult:
    """
    Run ETL process from configuration file.
    
    Args:
        config_file: Optional path to configuration file
        
    Returns:
        ETLResult: Processing results
    """
    # Load configuration
    config = get_database_config()
    
    # Create and run ETL system
    etl_system = create_etl_system(config)
    return etl_system.run_full_etl()