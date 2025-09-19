"""
Database Reporting System using Functional Programming
======================================================

Pure functional approach to database reporting with:
- Immutable report structures
- Functional data transformations
- Composable analysis functions
- Side-effect isolation for I/O

This module integrates the best reporting features from gestor_proyectos_db
while maintaining functional programming principles.
"""

import json
import statistics
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, NamedTuple, Union
from dataclasses import dataclass, field
from functools import reduce, partial
from enum import Enum

# Import our functional config and monitoring
from .config import get_database_config


# ============================================================================
# IMMUTABLE REPORT TYPES
# ============================================================================

class ReportType(Enum):
    """Tipos de reporte"""
    HEALTH_SUMMARY = "health_summary"
    PERFORMANCE_ANALYSIS = "performance_analysis"
    TREND_ANALYSIS = "trend_analysis"
    EXECUTIVE_SUMMARY = "executive_summary"


@dataclass(frozen=True)
class HealthMetric:
    """Métrica inmutable de salud"""
    name: str
    value: float
    unit: str
    status: str
    timestamp: datetime
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TrendData:
    """Datos inmutables de tendencia"""
    metric_name: str
    period_days: int
    values: Tuple[float, ...]  # Inmutable tuple
    timestamps: Tuple[datetime, ...]  # Inmutable tuple
    
    @property
    def average(self) -> float:
        """Promedio de valores"""
        return statistics.mean(self.values) if self.values else 0.0
    
    @property
    def median(self) -> float:
        """Mediana de valores"""
        return statistics.median(self.values) if self.values else 0.0
    
    @property
    def trend_direction(self) -> str:
        """Dirección de la tendencia"""
        if len(self.values) < 2:
            return "stable"
        
        first_half = self.values[:len(self.values)//2]
        second_half = self.values[len(self.values)//2:]
        
        if not first_half or not second_half:
            return "stable"
        
        avg_first = statistics.mean(first_half)
        avg_second = statistics.mean(second_half)
        
        if abs(avg_second - avg_first) < (avg_first * 0.1):  # < 10% change
            return "stable"
        elif avg_second > avg_first:
            return "increasing"
        else:
            return "decreasing"


@dataclass(frozen=True)
class PerformanceAnalysis:
    """Análisis inmutable de rendimiento"""
    connection_metrics: Dict[str, float]
    query_metrics: Dict[str, float]
    system_metrics: Dict[str, float]
    analysis_timestamp: datetime
    recommendations: Tuple[str, ...]  # Immutable tuple


@dataclass(frozen=True)
class DatabaseReport:
    """Reporte inmutable de base de datos"""
    report_type: ReportType
    timestamp: datetime
    period_start: datetime
    period_end: datetime
    overall_status: str
    key_metrics: Dict[str, Any]
    analysis_data: Dict[str, Any]
    recommendations: Tuple[str, ...]  # Immutable tuple
    execution_time: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario para serialización"""
        return {
            "report_type": self.report_type.value,
            "timestamp": self.timestamp.isoformat(),
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "overall_status": self.overall_status,
            "key_metrics": self.key_metrics,
            "analysis_data": self.analysis_data,
            "recommendations": list(self.recommendations),
            "execution_time": self.execution_time
        }


# ============================================================================
# PURE DATA TRANSFORMATION FUNCTIONS
# ============================================================================

def parse_monitoring_data(monitoring_files: List[Path]) -> List[Dict[str, Any]]:
    """
    Pure function: Parsear datos de monitoreo de archivos
    
    Args:
        monitoring_files: Lista de archivos de monitoreo
        
    Returns:
        Lista de datos parseados
    """
    parsed_data = []
    
    for file_path in monitoring_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                parsed_data.append(data)
        except Exception:
            continue  # Skip problematic files
    
    return parsed_data


def extract_health_metrics(monitoring_data: List[Dict[str, Any]]) -> List[HealthMetric]:
    """
    Pure function: Extraer métricas de salud de datos de monitoreo
    
    Args:
        monitoring_data: Datos de monitoreo
        
    Returns:
        Lista de métricas de salud
    """
    metrics = []
    
    for data in monitoring_data:
        timestamp = datetime.fromisoformat(data.get("timestamp", datetime.now().isoformat()))
        
        # Extraer métricas de checks
        for check in data.get("checks", []):
            if "details" in check and check["details"]:
                details = check["details"]
                
                # Latencia
                if "latency_ms" in details:
                    metrics.append(HealthMetric(
                        name="connection_latency",
                        value=details["latency_ms"],
                        unit="ms",
                        status=check["status"],
                        timestamp=timestamp,
                        details=details
                    ))
                
                # Tamaño de BD
                if "database_size" in details:
                    metrics.append(HealthMetric(
                        name="database_size",
                        value=0.0,  # String value, could be parsed
                        unit="text",
                        status=check["status"],
                        timestamp=timestamp,
                        details=details
                    ))
        
        # Tiempo de ejecución
        if "execution_time" in data:
            metrics.append(HealthMetric(
                name="execution_time",
                value=data["execution_time"],
                unit="seconds",
                status="healthy",
                timestamp=timestamp
            ))
    
    return metrics


def calculate_uptime_percentage(monitoring_data: List[Dict[str, Any]]) -> float:
    """
    Pure function: Calcular porcentaje de uptime
    
    Args:
        monitoring_data: Datos de monitoreo
        
    Returns:
        Porcentaje de uptime
    """
    if not monitoring_data:
        return 0.0
    
    healthy_count = sum(1 for data in monitoring_data 
                       if data.get("overall_status") == "healthy")
    
    return (healthy_count / len(monitoring_data)) * 100.0


def analyze_performance_trends(metrics: List[HealthMetric], metric_name: str, days: int = 7) -> TrendData:
    """
    Pure function: Analizar tendencias de rendimiento
    
    Args:
        metrics: Lista de métricas
        metric_name: Nombre de la métrica a analizar
        days: Número de días a analizar
        
    Returns:
        Datos de tendencia
    """
    # Filtrar métricas por nombre y período
    cutoff_date = datetime.now() - timedelta(days=days)
    filtered_metrics = [
        m for m in metrics 
        if m.name == metric_name and m.timestamp >= cutoff_date
    ]
    
    if not filtered_metrics:
        return TrendData(
            metric_name=metric_name,
            period_days=days,
            values=(),
            timestamps=()
        )
    
    # Ordenar por timestamp
    sorted_metrics = sorted(filtered_metrics, key=lambda m: m.timestamp)
    
    values = tuple(m.value for m in sorted_metrics)
    timestamps = tuple(m.timestamp for m in sorted_metrics)
    
    return TrendData(
        metric_name=metric_name,
        period_days=days,
        values=values,
        timestamps=timestamps
    )


def generate_performance_analysis(metrics: List[HealthMetric]) -> PerformanceAnalysis:
    """
    Pure function: Generar análisis de rendimiento
    
    Args:
        metrics: Lista de métricas
        
    Returns:
        Análisis de rendimiento
    """
    # Filtrar métricas de latencia
    latency_metrics = [m for m in metrics if m.name == "connection_latency"]
    execution_metrics = [m for m in metrics if m.name == "execution_time"]
    
    # Calcular métricas de conexión
    connection_metrics = {}
    if latency_metrics:
        latencies = [m.value for m in latency_metrics]
        connection_metrics = {
            "average_latency_ms": statistics.mean(latencies),
            "median_latency_ms": statistics.median(latencies),
            "max_latency_ms": max(latencies),
            "min_latency_ms": min(latencies),
            "samples": len(latencies)
        }
    
    # Calcular métricas de consultas (usando tiempo de ejecución como proxy)
    query_metrics = {}
    if execution_metrics:
        exec_times = [m.value for m in execution_metrics]
        query_metrics = {
            "average_execution_s": statistics.mean(exec_times),
            "median_execution_s": statistics.median(exec_times),
            "max_execution_s": max(exec_times),
            "samples": len(exec_times)
        }
    
    # Métricas del sistema (placeholder)
    system_metrics = {
        "health_checks_performed": len(metrics),
        "monitoring_period_hours": 24  # Default
    }
    
    # Generar recomendaciones
    recommendations = []
    if connection_metrics.get("average_latency_ms", 0) > 200:
        recommendations.append("Latencia de conexión alta - revisar configuración de red")
    
    if query_metrics.get("average_execution_s", 0) > 5:
        recommendations.append("Tiempo de ejecución alto - considerar optimización")
    
    if not recommendations:
        recommendations.append("Rendimiento dentro de parámetros normales")
    
    return PerformanceAnalysis(
        connection_metrics=connection_metrics,
        query_metrics=query_metrics,
        system_metrics=system_metrics,
        analysis_timestamp=datetime.now(),
        recommendations=tuple(recommendations)
    )


def calculate_health_score(monitoring_data: List[Dict[str, Any]]) -> int:
    """
    Pure function: Calcular puntuación de salud (0-100)
    
    Args:
        monitoring_data: Datos de monitoreo
        
    Returns:
        Puntuación de salud
    """
    if not monitoring_data:
        return 0
    
    # Calcular basado en estados
    healthy_count = sum(1 for data in monitoring_data 
                       if data.get("overall_status") == "healthy")
    warning_count = sum(1 for data in monitoring_data 
                       if data.get("overall_status") == "warning")
    critical_count = sum(1 for data in monitoring_data 
                        if data.get("overall_status") == "critical")
    
    total_count = len(monitoring_data)
    
    # Puntuación base por estado
    base_score = (healthy_count / total_count) * 100
    
    # Penalización por alertas críticas
    critical_penalty = min(critical_count * 10, 30)
    
    # Penalización menor por warnings
    warning_penalty = min(warning_count * 5, 20)
    
    final_score = max(0, base_score - critical_penalty - warning_penalty)
    
    return int(min(final_score, 100))


def determine_overall_status(health_score: int, critical_alerts: int) -> str:
    """
    Pure function: Determinar estado general
    
    Args:
        health_score: Puntuación de salud
        critical_alerts: Número de alertas críticas
        
    Returns:
        Estado general del sistema
    """
    if health_score >= 95 and critical_alerts == 0:
        return "excellent"
    elif health_score >= 85 and critical_alerts <= 1:
        return "good"
    elif health_score >= 70 and critical_alerts <= 3:
        return "fair"
    else:
        return "poor"


# ============================================================================
# MAIN REPORT GENERATION FUNCTIONS
# ============================================================================

def generate_health_summary_report(monitoring_data: List[Dict[str, Any]], 
                                 period_days: int = 7) -> DatabaseReport:
    """
    Pure function: Generar reporte de resumen de salud
    
    Args:
        monitoring_data: Datos de monitoreo
        period_days: Período en días
        
    Returns:
        Reporte de resumen de salud
    """
    timestamp = datetime.now()
    period_start = timestamp - timedelta(days=period_days)
    
    # Filtrar datos por período
    period_data = [
        data for data in monitoring_data
        if datetime.fromisoformat(data.get("timestamp", timestamp.isoformat())) >= period_start
    ]
    
    # Calcular métricas clave
    uptime_percentage = calculate_uptime_percentage(period_data)
    health_score = calculate_health_score(period_data)
    
    # Contar estados
    healthy_count = sum(1 for data in period_data 
                       if data.get("overall_status") == "healthy")
    warning_count = sum(1 for data in period_data 
                       if data.get("overall_status") == "warning")
    critical_count = sum(1 for data in period_data 
                        if data.get("overall_status") == "critical")
    
    # Determinar estado general
    overall_status = determine_overall_status(health_score, critical_count)
    
    key_metrics = {
        "uptime_percentage": round(uptime_percentage, 2),
        "health_score": health_score,
        "total_checks": len(period_data),
        "healthy_checks": healthy_count,
        "warning_checks": warning_count,
        "critical_checks": critical_count
    }
    
    analysis_data = {
        "period_summary": {
            "total_monitoring_points": len(period_data),
            "health_distribution": {
                "healthy": round((healthy_count / len(period_data) * 100), 1) if period_data else 0,
                "warning": round((warning_count / len(period_data) * 100), 1) if period_data else 0,
                "critical": round((critical_count / len(period_data) * 100), 1) if period_data else 0
            }
        }
    }
    
    # Generar recomendaciones
    recommendations = []
    if uptime_percentage < 95:
        recommendations.append("Uptime bajo: investigar causas de inestabilidad")
    if critical_count > 0:
        recommendations.append(f"Resolver {critical_count} alertas críticas")
    if health_score < 80:
        recommendations.append("Puntuación de salud baja: revisión integral requerida")
    
    if not recommendations:
        recommendations.append("Sistema operando dentro de parámetros normales")
    
    return DatabaseReport(
        report_type=ReportType.HEALTH_SUMMARY,
        timestamp=timestamp,
        period_start=period_start,
        period_end=timestamp,
        overall_status=overall_status,
        key_metrics=key_metrics,
        analysis_data=analysis_data,
        recommendations=tuple(recommendations),
        execution_time=0.0  # Set by caller
    )


def generate_performance_report(monitoring_data: List[Dict[str, Any]], 
                              period_days: int = 7) -> DatabaseReport:
    """
    Pure function: Generar reporte de rendimiento
    
    Args:
        monitoring_data: Datos de monitoreo
        period_days: Período en días
        
    Returns:
        Reporte de rendimiento
    """
    timestamp = datetime.now()
    period_start = timestamp - timedelta(days=period_days)
    
    # Extraer métricas
    metrics = extract_health_metrics(monitoring_data)
    
    # Generar análisis de rendimiento
    performance_analysis = generate_performance_analysis(metrics)
    
    # Analizar tendencias
    latency_trend = analyze_performance_trends(metrics, "connection_latency", period_days)
    execution_trend = analyze_performance_trends(metrics, "execution_time", period_days)
    
    key_metrics = {
        "average_latency_ms": performance_analysis.connection_metrics.get("average_latency_ms", 0),
        "average_execution_s": performance_analysis.query_metrics.get("average_execution_s", 0),
        "total_samples": len(metrics),
        "latency_trend": latency_trend.trend_direction,
        "execution_trend": execution_trend.trend_direction
    }
    
    analysis_data = {
        "connection_analysis": performance_analysis.connection_metrics,
        "query_analysis": performance_analysis.query_metrics,
        "system_analysis": performance_analysis.system_metrics,
        "trend_analysis": {
            "latency": {
                "direction": latency_trend.trend_direction,
                "average": latency_trend.average,
                "median": latency_trend.median,
                "samples": len(latency_trend.values)
            },
            "execution": {
                "direction": execution_trend.trend_direction,
                "average": execution_trend.average,
                "median": execution_trend.median,
                "samples": len(execution_trend.values)
            }
        }
    }
    
    # Determinar estado basado en rendimiento
    avg_latency = performance_analysis.connection_metrics.get("average_latency_ms", 0)
    if avg_latency > 1000:
        overall_status = "poor"
    elif avg_latency > 500:
        overall_status = "fair"
    elif avg_latency > 200:
        overall_status = "good"
    else:
        overall_status = "excellent"
    
    return DatabaseReport(
        report_type=ReportType.PERFORMANCE_ANALYSIS,
        timestamp=timestamp,
        period_start=period_start,
        period_end=timestamp,
        overall_status=overall_status,
        key_metrics=key_metrics,
        analysis_data=analysis_data,
        recommendations=performance_analysis.recommendations,
        execution_time=0.0  # Set by caller
    )


def generate_executive_summary(monitoring_data: List[Dict[str, Any]], 
                             period_days: int = 30) -> DatabaseReport:
    """
    Pure function: Generar resumen ejecutivo
    
    Args:
        monitoring_data: Datos de monitoreo
        period_days: Período en días
        
    Returns:
        Resumen ejecutivo
    """
    timestamp = datetime.now()
    period_start = timestamp - timedelta(days=period_days)
    
    # Generar sub-reportes
    health_report = generate_health_summary_report(monitoring_data, period_days)
    performance_report = generate_performance_report(monitoring_data, period_days)
    
    # Métricas clave del resumen ejecutivo
    key_metrics = {
        "overall_health_score": health_report.key_metrics["health_score"],
        "uptime_percentage": health_report.key_metrics["uptime_percentage"],
        "average_latency_ms": performance_report.key_metrics["average_latency_ms"],
        "critical_issues": health_report.key_metrics["critical_checks"],
        "total_monitoring_points": len(monitoring_data)
    }
    
    # Determinar estado ejecutivo
    health_score = key_metrics["overall_health_score"]
    uptime = key_metrics["uptime_percentage"]
    critical_issues = key_metrics["critical_issues"]
    
    if health_score >= 90 and uptime >= 99 and critical_issues == 0:
        overall_status = "excellent"
    elif health_score >= 80 and uptime >= 95 and critical_issues <= 2:
        overall_status = "good"
    elif health_score >= 70 and uptime >= 90 and critical_issues <= 5:
        overall_status = "fair"
    else:
        overall_status = "poor"
    
    # Análisis ejecutivo
    analysis_data = {
        "system_stability": "stable" if uptime >= 95 else "unstable",
        "performance_grade": performance_report.overall_status,
        "health_grade": health_report.overall_status,
        "key_trends": {
            "latency": performance_report.key_metrics["latency_trend"],
            "availability": "stable" if uptime >= 95 else "declining"
        },
        "risk_assessment": "low" if critical_issues == 0 else "high" if critical_issues > 5 else "medium"
    }
    
    # Recomendaciones ejecutivas
    exec_recommendations = []
    if overall_status == "poor":
        exec_recommendations.append("ACCIÓN INMEDIATA: Sistema requiere intervención urgente")
    if critical_issues > 0:
        exec_recommendations.append(f"Resolver {critical_issues} problemas críticos identificados")
    if uptime < 99:
        exec_recommendations.append("Mejorar estabilidad del sistema para alcanzar 99% uptime")
    
    if not exec_recommendations:
        exec_recommendations.append("Sistema estable - mantener monitoreo actual")
    
    return DatabaseReport(
        report_type=ReportType.EXECUTIVE_SUMMARY,
        timestamp=timestamp,
        period_start=period_start,
        period_end=timestamp,
        overall_status=overall_status,
        key_metrics=key_metrics,
        analysis_data=analysis_data,
        recommendations=tuple(exec_recommendations),
        execution_time=0.0  # Set by caller
    )


# ============================================================================
# SIDE-EFFECT FUNCTIONS (I/O)
# ============================================================================

def load_monitoring_files(logs_dir: Path, days: int = 7) -> List[Path]:
    """
    Side-effect function: Cargar archivos de monitoreo
    
    Args:
        logs_dir: Directorio de logs
        days: Días de historial
        
    Returns:
        Lista de archivos de monitoreo
    """
    if not logs_dir.exists():
        return []
    
    cutoff_date = datetime.now() - timedelta(days=days)
    monitoring_files = []
    
    # Buscar archivos de monitoreo
    monitoring_dir = logs_dir / "monitoring"
    if monitoring_dir.exists():
        for file_path in monitoring_dir.glob("monitoring_report_*.json"):
            try:
                file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_time >= cutoff_date:
                    monitoring_files.append(file_path)
            except Exception:
                continue
    
    return sorted(monitoring_files)


def save_report(report: DatabaseReport, output_dir: Path) -> Path:
    """
    Side-effect function: Guardar reporte
    
    Args:
        report: Reporte a guardar
        output_dir: Directorio de salida
        
    Returns:
        Path del archivo guardado
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = report.timestamp.strftime("%Y%m%d_%H%M%S")
    filename = f"{report.report_type.value}_{timestamp}.json"
    file_path = output_dir / filename
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)
    
    return file_path


# ============================================================================
# PUBLIC API
# ============================================================================

def generate_database_report(report_type: ReportType, 
                           period_days: int = 7, 
                           logs_dir: Optional[Path] = None) -> DatabaseReport:
    """
    API Principal: Generar reporte de base de datos
    
    Args:
        report_type: Tipo de reporte a generar
        period_days: Período en días
        logs_dir: Directorio de logs (opcional)
        
    Returns:
        Reporte generado
    """
    import time
    start_time = time.time()
    
    if logs_dir is None:
        logs_dir = Path(__file__).parent.parent / "logs"
    
    # Cargar datos de monitoreo
    monitoring_files = load_monitoring_files(logs_dir, period_days)
    monitoring_data = parse_monitoring_data(monitoring_files)
    
    # Generar reporte según tipo
    if report_type == ReportType.HEALTH_SUMMARY:
        report = generate_health_summary_report(monitoring_data, period_days)
    elif report_type == ReportType.PERFORMANCE_ANALYSIS:
        report = generate_performance_report(monitoring_data, period_days)
    elif report_type == ReportType.EXECUTIVE_SUMMARY:
        report = generate_executive_summary(monitoring_data, period_days)
    else:
        raise ValueError(f"Tipo de reporte no soportado: {report_type}")
    
    # Actualizar tiempo de ejecución
    execution_time = time.time() - start_time
    
    # Crear nuevo reporte con tiempo de ejecución
    report = DatabaseReport(
        report_type=report.report_type,
        timestamp=report.timestamp,
        period_start=report.period_start,
        period_end=report.period_end,
        overall_status=report.overall_status,
        key_metrics=report.key_metrics,
        analysis_data=report.analysis_data,
        recommendations=report.recommendations,
        execution_time=execution_time
    )
    
    return report


if __name__ == "__main__":
    # Ejemplo de uso
    print("📊 Sistema de Reportes de Base de Datos")
    print("=" * 50)
    
    try:
        # Generar resumen ejecutivo
        exec_report = generate_database_report(ReportType.EXECUTIVE_SUMMARY, 30)
        
        print(f"\n📋 RESUMEN EJECUTIVO")
        print(f"   Estado General: {exec_report.overall_status.upper()}")
        print(f"   Puntuación de Salud: {exec_report.key_metrics.get('overall_health_score', 0)}/100")
        print(f"   Uptime: {exec_report.key_metrics.get('uptime_percentage', 0):.1f}%")
        print(f"   Problemas Críticos: {exec_report.key_metrics.get('critical_issues', 0)}")
        
        if exec_report.recommendations:
            print(f"\n💡 PRINCIPALES RECOMENDACIONES:")
            for i, rec in enumerate(exec_report.recommendations[:3], 1):
                print(f"   {i}. {rec}")
        
        print(f"\n⏱️  Reporte generado en {exec_report.execution_time:.2f} segundos")
        
    except Exception as e:
        print(f"❌ Error: {e}")