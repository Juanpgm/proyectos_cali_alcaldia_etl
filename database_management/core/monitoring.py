"""
Database Monitoring System using Functional Programming
=======================================================

Pure functional approach to database monitoring with:
- Immutable monitoring state
- Functional composition for checks
- Side-effect isolation
- Event-driven architecture

This module integrates the best monitoring features from gestor_proyectos_db
while maintaining functional programming principles.
"""

import os
import time
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any, NamedTuple, Union
from dataclasses import dataclass, field
from functools import reduce, partial
from enum import Enum

# Import our functional config
from .config import get_database_config, test_connection


# ============================================================================
# IMMUTABLE TYPES
# ============================================================================

class MonitoringStatus(Enum):
    """Estados de monitoreo"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class AlertLevel(Enum):
    """Niveles de alerta"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass(frozen=True)
class MonitoringCheck:
    """Resultado inmutable de una verificación"""
    name: str
    category: str
    status: MonitoringStatus
    message: str
    timestamp: datetime
    execution_time: float
    details: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class Alert:
    """Alerta inmutable"""
    timestamp: datetime
    level: AlertLevel
    title: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    resolved: bool = False
    resolution_timestamp: Optional[datetime] = None


@dataclass(frozen=True)
class MonitoringReport:
    """Reporte inmutable de monitoreo"""
    timestamp: datetime
    overall_status: MonitoringStatus
    checks: List[MonitoringCheck]
    alerts: List[Alert]
    execution_time: float
    recommendations: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario para serialización"""
        return {
            "timestamp": self.timestamp.isoformat(),
            "overall_status": self.overall_status.value,
            "execution_time": self.execution_time,
            "recommendations": self.recommendations,
            "checks": [
                {
                    "name": check.name,
                    "category": check.category,
                    "status": check.status.value,
                    "message": check.message,
                    "timestamp": check.timestamp.isoformat(),
                    "execution_time": check.execution_time,
                    "details": check.details,
                    "recommendations": check.recommendations
                }
                for check in self.checks
            ],
            "alerts": [
                {
                    "timestamp": alert.timestamp.isoformat(),
                    "level": alert.level.value,
                    "title": alert.title,
                    "message": alert.message,
                    "details": alert.details,
                    "resolved": alert.resolved,
                    "resolution_timestamp": alert.resolution_timestamp.isoformat() if alert.resolution_timestamp else None
                }
                for alert in self.alerts
            ]
        }


# ============================================================================
# PURE FUNCTIONS FOR MONITORING CHECKS
# ============================================================================

def check_database_connection(config=None) -> MonitoringCheck:
    """
    Pure function: Verificar conexión a la base de datos
    
    Args:
        config: Configuración de base de datos (opcional)
        
    Returns:
        MonitoringCheck con resultado de la verificación
    """
    start_time = time.time()
    timestamp = datetime.now()
    
    try:
        if config is None:
            config = get_database_config()
        
        if test_connection(config):
            return MonitoringCheck(
                name="database_connection",
                category="connection",
                status=MonitoringStatus.HEALTHY,
                message="Conexión a base de datos exitosa",
                timestamp=timestamp,
                execution_time=time.time() - start_time,
                details={"host": config.host, "port": config.port, "database": config.database}
            )
        else:
            return MonitoringCheck(
                name="database_connection",
                category="connection",
                status=MonitoringStatus.CRITICAL,
                message="No se puede conectar a la base de datos",
                timestamp=timestamp,
                execution_time=time.time() - start_time,
                details={"host": config.host, "port": config.port},
                recommendations=["Verificar que PostgreSQL esté ejecutándose", "Comprobar configuración de red"]
            )
            
    except Exception as e:
        return MonitoringCheck(
            name="database_connection",
            category="connection",
            status=MonitoringStatus.CRITICAL,
            message=f"Error de conexión: {str(e)}",
            timestamp=timestamp,
            execution_time=time.time() - start_time,
            recommendations=["Verificar configuración de .env", "Comprobar credenciales de base de datos"]
        )


def check_connection_latency(config=None) -> MonitoringCheck:
    """
    Pure function: Verificar latencia de conexión
    
    Args:
        config: Configuración de base de datos (opcional)
        
    Returns:
        MonitoringCheck con medición de latencia
    """
    start_time = time.time()
    timestamp = datetime.now()
    
    try:
        if config is None:
            config = get_database_config()
        
        import psycopg2
        
        # Medir tiempo de conexión
        conn_start = time.time()
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        
        latency_ms = (time.time() - conn_start) * 1000
        
        cursor.close()
        conn.close()
        
        details = {"latency_ms": round(latency_ms, 2)}
        
        if latency_ms > 1000:  # > 1 segundo
            status = MonitoringStatus.CRITICAL
            message = f"Latencia crítica: {latency_ms:.1f}ms"
            recommendations = ["Verificar red y conectividad", "Comprobar carga del servidor"]
        elif latency_ms > 200:  # > 200ms
            status = MonitoringStatus.WARNING
            message = f"Latencia alta: {latency_ms:.1f}ms"
            recommendations = ["Monitorear rendimiento de red"]
        else:
            status = MonitoringStatus.HEALTHY
            message = f"Latencia normal: {latency_ms:.1f}ms"
            recommendations = []
        
        return MonitoringCheck(
            name="connection_latency",
            category="performance",
            status=status,
            message=message,
            timestamp=timestamp,
            execution_time=time.time() - start_time,
            details=details,
            recommendations=recommendations
        )
        
    except ImportError:
        return MonitoringCheck(
            name="connection_latency",
            category="performance",
            status=MonitoringStatus.UNKNOWN,
            message="psycopg2 no disponible para medir latencia",
            timestamp=timestamp,
            execution_time=time.time() - start_time,
            recommendations=["Instalar psycopg2-binary para mediciones completas"]
        )
    except Exception as e:
        return MonitoringCheck(
            name="connection_latency",
            category="performance",
            status=MonitoringStatus.CRITICAL,
            message=f"Error midiendo latencia: {str(e)}",
            timestamp=timestamp,
            execution_time=time.time() - start_time
        )


def check_database_size(config=None) -> MonitoringCheck:
    """
    Pure function: Verificar tamaño de la base de datos
    
    Args:
        config: Configuración de base de datos (opcional)
        
    Returns:
        MonitoringCheck con información de tamaño
    """
    start_time = time.time()
    timestamp = datetime.now()
    
    try:
        if config is None:
            config = get_database_config()
        
        import psycopg2
        
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        
        # Obtener tamaño de la base de datos
        cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
        db_size = cursor.fetchone()[0]
        
        # Obtener número de tablas
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        table_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        details = {
            "database_size": db_size,
            "table_count": table_count
        }
        
        return MonitoringCheck(
            name="database_size",
            category="maintenance",
            status=MonitoringStatus.HEALTHY,
            message=f"Base de datos: {db_size}, {table_count} tablas",
            timestamp=timestamp,
            execution_time=time.time() - start_time,
            details=details
        )
        
    except Exception as e:
        return MonitoringCheck(
            name="database_size",
            category="maintenance",
            status=MonitoringStatus.WARNING,
            message=f"Error obteniendo tamaño: {str(e)}",
            timestamp=timestamp,
            execution_time=time.time() - start_time
        )


def check_critical_tables(config=None) -> MonitoringCheck:
    """
    Pure function: Verificar existencia de tablas críticas
    
    Args:
        config: Configuración de base de datos (opcional)
        
    Returns:
        MonitoringCheck con estado de tablas críticas
    """
    start_time = time.time()
    timestamp = datetime.now()
    
    critical_tables = [
        'emp_contratos',
        'emp_seguimiento_procesos_dacp',
        'emp_proyectos',
        'flujo_caja',
        'unidad_proyecto_infraestructura_equipamientos',
        'unidad_proyecto_infraestructura_vial'
    ]
    
    try:
        if config is None:
            config = get_database_config()
        
        import psycopg2
        
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        
        # Verificar existencia de tablas
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        
        existing_tables = {row[0] for row in cursor.fetchall()}
        missing_tables = [table for table in critical_tables if table not in existing_tables]
        
        cursor.close()
        conn.close()
        
        details = {
            "expected_tables": len(critical_tables),
            "existing_tables": len(existing_tables),
            "missing_tables": missing_tables,
            "critical_tables": critical_tables
        }
        
        if missing_tables:
            status = MonitoringStatus.CRITICAL
            message = f"Faltan {len(missing_tables)} tablas críticas"
            recommendations = [
                "Ejecutar scripts de creación de tablas",
                "Verificar proceso de ETL",
                f"Tablas faltantes: {', '.join(missing_tables)}"
            ]
        else:
            status = MonitoringStatus.HEALTHY
            message = f"Todas las {len(critical_tables)} tablas críticas presentes"
            recommendations = []
        
        return MonitoringCheck(
            name="critical_tables",
            category="structure",
            status=status,
            message=message,
            timestamp=timestamp,
            execution_time=time.time() - start_time,
            details=details,
            recommendations=recommendations
        )
        
    except Exception as e:
        return MonitoringCheck(
            name="critical_tables",
            category="structure",
            status=MonitoringStatus.CRITICAL,
            message=f"Error verificando tablas: {str(e)}",
            timestamp=timestamp,
            execution_time=time.time() - start_time
        )


# ============================================================================
# FUNCTIONAL COMPOSITION
# ============================================================================

def run_all_checks(config=None) -> List[MonitoringCheck]:
    """
    Pure function: Ejecutar todas las verificaciones
    
    Args:
        config: Configuración de base de datos (opcional)
        
    Returns:
        Lista inmutable de verificaciones
    """
    check_functions = [
        check_database_connection,
        check_connection_latency,
        check_database_size,
        check_critical_tables
    ]
    
    return [check_func(config) for check_func in check_functions]


def calculate_overall_status(checks: List[MonitoringCheck]) -> MonitoringStatus:
    """
    Pure function: Calcular estado general basado en verificaciones
    
    Args:
        checks: Lista de verificaciones
        
    Returns:
        Estado general del sistema
    """
    if not checks:
        return MonitoringStatus.UNKNOWN
    
    statuses = [check.status for check in checks]
    
    if MonitoringStatus.CRITICAL in statuses:
        return MonitoringStatus.CRITICAL
    elif MonitoringStatus.WARNING in statuses:
        return MonitoringStatus.WARNING
    elif MonitoringStatus.HEALTHY in statuses:
        return MonitoringStatus.HEALTHY
    else:
        return MonitoringStatus.UNKNOWN


def extract_recommendations(checks: List[MonitoringCheck]) -> List[str]:
    """
    Pure function: Extraer todas las recomendaciones únicas
    
    Args:
        checks: Lista de verificaciones
        
    Returns:
        Lista de recomendaciones únicas
    """
    all_recommendations = []
    for check in checks:
        all_recommendations.extend(check.recommendations)
    
    # Remover duplicados manteniendo orden
    seen = set()
    unique_recommendations = []
    for rec in all_recommendations:
        if rec not in seen:
            seen.add(rec)
            unique_recommendations.append(rec)
    
    return unique_recommendations


def generate_alerts_from_checks(checks: List[MonitoringCheck]) -> List[Alert]:
    """
    Pure function: Generar alertas basadas en verificaciones
    
    Args:
        checks: Lista de verificaciones
        
    Returns:
        Lista de alertas generadas
    """
    alerts = []
    timestamp = datetime.now()
    
    for check in checks:
        if check.status == MonitoringStatus.CRITICAL:
            alerts.append(Alert(
                timestamp=timestamp,
                level=AlertLevel.CRITICAL,
                title=f"Error crítico: {check.name}",
                message=check.message,
                details=check.details
            ))
        elif check.status == MonitoringStatus.WARNING:
            alerts.append(Alert(
                timestamp=timestamp,
                level=AlertLevel.WARNING,
                title=f"Advertencia: {check.name}",
                message=check.message,
                details=check.details
            ))
    
    return alerts


# ============================================================================
# MAIN MONITORING FUNCTION
# ============================================================================

def run_monitoring_check(config=None) -> MonitoringReport:
    """
    Pure function: Ejecutar verificación completa de monitoreo
    
    Args:
        config: Configuración de base de datos (opcional)
        
    Returns:
        Reporte inmutable de monitoreo
    """
    start_time = time.time()
    timestamp = datetime.now()
    
    # Ejecutar todas las verificaciones
    checks = run_all_checks(config)
    
    # Calcular estado general
    overall_status = calculate_overall_status(checks)
    
    # Extraer recomendaciones
    recommendations = extract_recommendations(checks)
    
    # Generar alertas
    alerts = generate_alerts_from_checks(checks)
    
    execution_time = time.time() - start_time
    
    return MonitoringReport(
        timestamp=timestamp,
        overall_status=overall_status,
        checks=checks,
        alerts=alerts,
        execution_time=execution_time,
        recommendations=recommendations
    )


# ============================================================================
# SIDE-EFFECT FUNCTIONS (I/O)
# ============================================================================

def save_monitoring_report(report: MonitoringReport, output_dir: Optional[Path] = None) -> Path:
    """
    Side-effect function: Guardar reporte de monitoreo
    
    Args:
        report: Reporte de monitoreo
        output_dir: Directorio de salida (opcional)
        
    Returns:
        Path del archivo guardado
    """
    if output_dir is None:
        output_dir = Path(__file__).parent.parent / "logs" / "monitoring"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = report.timestamp.strftime("%Y%m%d_%H%M%S")
    report_file = output_dir / f"monitoring_report_{timestamp}.json"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)
    
    return report_file


def load_monitoring_history(days: int = 7, logs_dir: Optional[Path] = None) -> List[MonitoringReport]:
    """
    Side-effect function: Cargar historial de reportes
    
    Args:
        days: Número de días de historial
        logs_dir: Directorio de logs (opcional)
        
    Returns:
        Lista de reportes históricos
    """
    if logs_dir is None:
        logs_dir = Path(__file__).parent.parent / "logs" / "monitoring"
    
    if not logs_dir.exists():
        return []
    
    cutoff_date = datetime.now() - timedelta(days=days)
    reports = []
    
    for report_file in logs_dir.glob("monitoring_report_*.json"):
        try:
            file_time = datetime.fromtimestamp(report_file.stat().st_mtime)
            if file_time >= cutoff_date:
                with open(report_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Reconstruir reporte (versión simplificada)
                    # En una implementación completa, tendríamos funciones para deserializar
                    reports.append(data)
        except Exception:
            continue
    
    return reports


def setup_monitoring_logging() -> logging.Logger:
    """
    Side-effect function: Configurar logging para monitoreo
    
    Returns:
        Logger configurado
    """
    logger = logging.getLogger('database_monitoring')
    
    if not logger.handlers:
        # Crear directorio de logs
        log_dir = Path(__file__).parent.parent / "logs" / "monitoring"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Configurar archivo de log
        log_file = log_dir / f"monitoring_{datetime.now().strftime('%Y%m%d')}.log"
        
        handler = logging.FileHandler(log_file)
        handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    return logger


# ============================================================================
# PUBLIC API
# ============================================================================

def monitor_database(config=None, save_report: bool = True) -> MonitoringReport:
    """
    API Principal: Monitorear base de datos
    
    Args:
        config: Configuración de base de datos (opcional)
        save_report: Si guardar el reporte a disco
        
    Returns:
        Reporte de monitoreo
    """
    logger = setup_monitoring_logging()
    logger.info("🔍 Iniciando monitoreo de base de datos")
    
    try:
        # Ejecutar monitoreo (función pura)
        report = run_monitoring_check(config)
        
        # Log resultado
        logger.info(f"✅ Monitoreo completado - Estado: {report.overall_status.value}")
        logger.info(f"   Verificaciones: {len(report.checks)}")
        logger.info(f"   Alertas: {len(report.alerts)}")
        logger.info(f"   Tiempo: {report.execution_time:.2f}s")
        
        # Guardar reporte si se solicita
        if save_report:
            report_path = save_monitoring_report(report)
            logger.info(f"📄 Reporte guardado: {report_path}")
        
        return report
        
    except Exception as e:
        logger.error(f"❌ Error en monitoreo: {e}")
        raise


if __name__ == "__main__":
    # Ejemplo de uso
    print("🎯 Sistema de Monitoreo de Base de Datos")
    print("=" * 50)
    
    try:
        report = monitor_database()
        
        print(f"\n📊 RESULTADO DEL MONITOREO")
        print(f"   Estado General: {report.overall_status.value.upper()}")
        print(f"   Verificaciones: {len(report.checks)}")
        print(f"   Alertas: {len(report.alerts)}")
        print(f"   Tiempo: {report.execution_time:.2f}s")
        
        if report.recommendations:
            print(f"\n💡 RECOMENDACIONES:")
            for i, rec in enumerate(report.recommendations[:5], 1):
                print(f"   {i}. {rec}")
        
        if report.alerts:
            print(f"\n🚨 ALERTAS:")
            for alert in report.alerts:
                print(f"   {alert.level.value.upper()}: {alert.title}")
        
    except Exception as e:
        print(f"❌ Error: {e}")