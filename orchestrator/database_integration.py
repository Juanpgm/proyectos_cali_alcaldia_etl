"""
Database Integration for Orchestrator
=====================================

Integration module that provides database management capabilities
to the orchestrator using the new functional architecture.

This replaces any previous dependencies on gestor_proyectos_db.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

# Import from the new integrated database management core
from database_management.core import (
    IntegratedDatabaseManager,
    quick_health_check,
    comprehensive_analysis,
    ensure_database_ready,
    MonitoringStatus,
    ReportType
)


class OrchestratorDatabaseIntegration:
    """
    Database integration for the orchestrator system
    
    Provides database monitoring, reporting, and management
    capabilities using the new functional architecture.
    """
    
    def __init__(self):
        """Initialize orchestrator database integration"""
        self.db_manager = IntegratedDatabaseManager()
        self.logger = logging.getLogger('orchestrator.database')
    
    def check_database_readiness(self) -> Dict[str, Any]:
        """
        Check if database is ready for ETL operations
        
        Returns:
            Dictionary with readiness status and details
        """
        self.logger.info("Checking database readiness for orchestrator")
        
        try:
            # Quick health check
            health_status = quick_health_check()
            
            # Check if system is ready
            is_ready = ensure_database_ready()
            
            result = {
                "ready": is_ready,
                "timestamp": datetime.now().isoformat(),
                "connection": health_status["connection"],
                "schema_complete": health_status["schema_complete"],
                "status": health_status["status"],
                "details": {
                    "message": "Database ready for operations" if is_ready else "Database requires attention",
                    "health_check": health_status
                }
            }
            
            if is_ready:
                self.logger.info("✅ Database is ready for orchestrator operations")
            else:
                self.logger.warning("⚠️ Database readiness check failed")
                
                # Get missing tables if any
                missing_tables = self.db_manager.get_missing_tables()
                if missing_tables:
                    result["details"]["missing_tables"] = missing_tables
            
            return result
            
        except Exception as e:
            self.logger.error(f"Database readiness check failed: {e}")
            return {
                "ready": False,
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "details": {
                    "message": "Error checking database readiness"
                }
            }
    
    def run_pre_etl_checks(self) -> Dict[str, Any]:
        """
        Run comprehensive checks before ETL execution
        
        Returns:
            Dictionary with pre-ETL check results
        """
        self.logger.info("Running pre-ETL database checks")
        
        try:
            # Run comprehensive analysis
            analysis = comprehensive_analysis(7)
            
            # Determine if ETL can proceed
            can_proceed = (
                analysis.get("configuration", {}).get("connection", False) and
                analysis.get("health_check", {}).get("status") in ["healthy", "warning"]
            )
            
            result = {
                "can_proceed": can_proceed,
                "timestamp": datetime.now().isoformat(),
                "analysis": analysis,
                "recommendations": analysis.get("recommendations", [])
            }
            
            if can_proceed:
                self.logger.info("✅ Pre-ETL checks passed - ETL can proceed")
            else:
                self.logger.warning("⚠️ Pre-ETL checks failed - ETL should not proceed")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Pre-ETL checks failed: {e}")
            return {
                "can_proceed": False,
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
    
    def monitor_etl_execution(self, execution_id: str) -> Dict[str, Any]:
        """
        Monitor database during ETL execution
        
        Args:
            execution_id: ETL execution ID
            
        Returns:
            Dictionary with monitoring results
        """
        self.logger.info(f"Monitoring database during ETL execution: {execution_id}")
        
        try:
            # Run health check during execution
            health_report = self.db_manager.run_health_check(save_report=True)
            
            monitoring_result = {
                "execution_id": execution_id,
                "timestamp": datetime.now().isoformat(),
                "health_status": health_report.overall_status.value,
                "checks_performed": len(health_report.checks),
                "alerts": len(health_report.alerts),
                "execution_time": health_report.execution_time,
                "recommendations": health_report.recommendations
            }
            
            # Log alerts if any
            if health_report.alerts:
                self.logger.warning(f"Database alerts during ETL {execution_id}: {len(health_report.alerts)} alerts")
                for alert in health_report.alerts:
                    self.logger.warning(f"Alert: {alert.title} - {alert.message}")
            
            return monitoring_result
            
        except Exception as e:
            self.logger.error(f"ETL monitoring failed for {execution_id}: {e}")
            return {
                "execution_id": execution_id,
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
    
    def generate_post_etl_report(self, execution_id: str) -> Dict[str, Any]:
        """
        Generate comprehensive report after ETL execution
        
        Args:
            execution_id: ETL execution ID
            
        Returns:
            Dictionary with post-ETL report
        """
        self.logger.info(f"Generating post-ETL report for execution: {execution_id}")
        
        try:
            # Generate multiple reports
            health_summary = self.db_manager.generate_health_summary(1)  # Last day
            schema_analysis = self.db_manager.analyze_schema(save_report=True)
            
            # Try to generate performance report
            try:
                performance_report = self.db_manager.generate_performance_report(1)
                performance_data = {
                    "status": performance_report.overall_status,
                    "metrics": performance_report.key_metrics,
                    "execution_time": performance_report.execution_time
                }
            except Exception:
                performance_data = {"status": "data_insufficient", "message": "Not enough data for performance analysis"}
            
            post_etl_report = {
                "execution_id": execution_id,
                "timestamp": datetime.now().isoformat(),
                "health_summary": {
                    "status": health_summary.overall_status,
                    "key_metrics": health_summary.key_metrics,
                    "execution_time": health_summary.execution_time
                },
                "schema_analysis": {
                    "status": schema_analysis.overall_status,
                    "missing_tables": len(schema_analysis.missing_tables),
                    "existing_tables": len(schema_analysis.existing_tables),
                    "recommendations": list(schema_analysis.recommendations)
                },
                "performance_analysis": performance_data,
                "overall_assessment": self._assess_etl_impact(health_summary, schema_analysis)
            }
            
            self.logger.info(f"✅ Post-ETL report generated for {execution_id}")
            return post_etl_report
            
        except Exception as e:
            self.logger.error(f"Post-ETL report generation failed for {execution_id}: {e}")
            return {
                "execution_id": execution_id,
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
    
    def _assess_etl_impact(self, health_summary, schema_analysis) -> Dict[str, Any]:
        """
        Assess the impact of ETL execution on the database
        
        Args:
            health_summary: Health summary report
            schema_analysis: Schema analysis report
            
        Returns:
            Assessment of ETL impact
        """
        assessment = {
            "overall_status": "unknown",
            "data_quality": "unknown",
            "schema_integrity": "unknown",
            "recommendations": []
        }
        
        try:
            # Assess overall status
            if (health_summary.overall_status == "excellent" and 
                schema_analysis.overall_status == "complete"):
                assessment["overall_status"] = "excellent"
            elif (health_summary.overall_status in ["good", "excellent"] and
                  schema_analysis.overall_status in ["complete", "minor_issues"]):
                assessment["overall_status"] = "good"
            elif health_summary.overall_status == "fair":
                assessment["overall_status"] = "fair"
            else:
                assessment["overall_status"] = "poor"
            
            # Assess data quality based on health metrics
            uptime = health_summary.key_metrics.get("uptime_percentage", 0)
            if uptime >= 99:
                assessment["data_quality"] = "excellent"
            elif uptime >= 95:
                assessment["data_quality"] = "good"
            elif uptime >= 90:
                assessment["data_quality"] = "fair"
            else:
                assessment["data_quality"] = "poor"
            
            # Assess schema integrity
            if schema_analysis.overall_status == "complete":
                assessment["schema_integrity"] = "complete"
            elif schema_analysis.overall_status == "minor_issues":
                assessment["schema_integrity"] = "minor_issues"
            else:
                assessment["schema_integrity"] = "issues_detected"
            
            # Compile recommendations
            recommendations = []
            if assessment["overall_status"] == "poor":
                recommendations.append("ETL process may have caused issues - investigate immediately")
            
            if len(schema_analysis.missing_tables) > 0:
                recommendations.append(f"Schema incomplete: {len(schema_analysis.missing_tables)} tables missing")
            
            if uptime < 95:
                recommendations.append("Low uptime detected - check for connection issues")
            
            if not recommendations:
                recommendations.append("ETL execution completed successfully with no issues detected")
            
            assessment["recommendations"] = recommendations
            
        except Exception as e:
            assessment["error"] = str(e)
        
        return assessment
    
    def get_database_status_for_api(self) -> Dict[str, Any]:
        """
        Get database status formatted for API responses
        
        Returns:
            Dictionary with API-friendly database status
        """
        try:
            status = quick_health_check()
            
            return {
                "database": {
                    "connected": status["connection"],
                    "schema_complete": status["schema_complete"],
                    "status": status["status"],
                    "last_check": status["timestamp"]
                },
                "ready_for_etl": status["connection"] and status["schema_complete"]
            }
            
        except Exception as e:
            return {
                "database": {
                    "connected": False,
                    "schema_complete": False,
                    "status": "error",
                    "error": str(e),
                    "last_check": datetime.now().isoformat()
                },
                "ready_for_etl": False
            }


# ============================================================================
# CONVENIENCE FUNCTIONS FOR ORCHESTRATOR
# ============================================================================

def check_database_before_etl() -> bool:
    """
    Convenience function: Check if database is ready before ETL
    
    Returns:
        True if database is ready for ETL operations
    """
    integration = OrchestratorDatabaseIntegration()
    result = integration.check_database_readiness()
    return result.get("ready", False)


def monitor_database_during_etl(execution_id: str) -> Dict[str, Any]:
    """
    Convenience function: Monitor database during ETL execution
    
    Args:
        execution_id: ETL execution ID
        
    Returns:
        Monitoring results
    """
    integration = OrchestratorDatabaseIntegration()
    return integration.monitor_etl_execution(execution_id)


def generate_etl_database_report(execution_id: str) -> Dict[str, Any]:
    """
    Convenience function: Generate database report after ETL
    
    Args:
        execution_id: ETL execution ID
        
    Returns:
        Post-ETL database report
    """
    integration = OrchestratorDatabaseIntegration()
    return integration.generate_post_etl_report(execution_id)


def get_api_database_status() -> Dict[str, Any]:
    """
    Convenience function: Get database status for API
    
    Returns:
        API-friendly database status
    """
    integration = OrchestratorDatabaseIntegration()
    return integration.get_database_status_for_api()


if __name__ == "__main__":
    # Test integration
    print("🔗 Testing Orchestrator Database Integration")
    print("=" * 50)
    
    try:
        integration = OrchestratorDatabaseIntegration()
        
        # Test database readiness
        print("\n🔍 Checking database readiness...")
        readiness = integration.check_database_readiness()
        print(f"   Ready: {'✅' if readiness['ready'] else '❌'}")
        print(f"   Connection: {'✅' if readiness['connection'] else '❌'}")
        print(f"   Schema: {'✅' if readiness['schema_complete'] else '❌'}")
        
        # Test API status
        print("\n📊 Getting API status...")
        api_status = integration.get_database_status_for_api()
        print(f"   Connected: {'✅' if api_status['database']['connected'] else '❌'}")
        print(f"   Ready for ETL: {'✅' if api_status['ready_for_etl'] else '❌'}")
        
        if readiness['ready']:
            # Test pre-ETL checks
            print("\n🚀 Running pre-ETL checks...")
            pre_etl = integration.run_pre_etl_checks()
            print(f"   Can proceed: {'✅' if pre_etl['can_proceed'] else '❌'}")
            
            # Test monitoring
            print("\n📈 Testing monitoring...")
            monitoring = integration.monitor_etl_execution("test_execution_001")
            print(f"   Health status: {monitoring.get('health_status', 'unknown').upper()}")
            print(f"   Alerts: {monitoring.get('alerts', 0)}")
        
        print(f"\n🎉 Integration test completed!")
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")