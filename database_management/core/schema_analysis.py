"""
Database Schema Analysis using Functional Programming
=====================================================

Pure functional approach to database schema analysis with:
- Immutable schema representations
- Functional schema comparisons
- Pure schema validation functions
- Side-effect isolation for database operations

This module integrates the best schema analysis features from gestor_proyectos_db
while maintaining functional programming principles.
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, NamedTuple, Set
from dataclasses import dataclass, field
from functools import reduce, partial
from enum import Enum

# Import our functional config
from .config import get_database_config, test_connection


# ============================================================================
# IMMUTABLE SCHEMA TYPES
# ============================================================================

class TableStatus(Enum):
    """Estados de tabla"""
    EXISTS = "exists"
    MISSING = "missing"
    EXTRA = "extra"
    MODIFIED = "modified"


@dataclass(frozen=True)
class ColumnDefinition:
    """Definición inmutable de columna"""
    name: str
    data_type: str
    nullable: bool
    default: Optional[str] = None
    primary_key: bool = False
    foreign_key: Optional[str] = None


@dataclass(frozen=True)
class TableDefinition:
    """Definición inmutable de tabla"""
    name: str
    columns: Tuple[ColumnDefinition, ...]  # Immutable tuple
    indexes: Tuple[str, ...] = field(default_factory=tuple)  # Immutable tuple
    constraints: Tuple[str, ...] = field(default_factory=tuple)  # Immutable tuple
    
    @property
    def column_names(self) -> Set[str]:
        """Nombres de columnas como set inmutable"""
        return frozenset(col.name for col in self.columns)


@dataclass(frozen=True)
class SchemaDefinition:
    """Definición inmutable de esquema completo"""
    tables: Tuple[TableDefinition, ...]  # Immutable tuple
    version: str = "1.0"
    created_at: datetime = field(default_factory=datetime.now)
    
    @property
    def table_names(self) -> Set[str]:
        """Nombres de tablas como set inmutable"""
        return frozenset(table.name for table in self.tables)
    
    def get_table(self, name: str) -> Optional[TableDefinition]:
        """Obtener definición de tabla por nombre"""
        for table in self.tables:
            if table.name == name:
                return table
        return None


@dataclass(frozen=True)
class TableAnalysis:
    """Análisis inmutable de tabla"""
    name: str
    status: TableStatus
    expected_columns: int
    actual_columns: int
    missing_columns: Tuple[str, ...] = field(default_factory=tuple)
    extra_columns: Tuple[str, ...] = field(default_factory=tuple)
    record_count: int = 0
    indexes_count: int = 0
    recommendations: Tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class SchemaAnalysisReport:
    """Reporte inmutable de análisis de esquema"""
    timestamp: datetime
    database_info: Dict[str, Any]
    expected_tables: Set[str]
    existing_tables: Set[str]
    missing_tables: Tuple[str, ...]  # Immutable tuple
    extra_tables: Tuple[str, ...]    # Immutable tuple
    table_analyses: Tuple[TableAnalysis, ...]  # Immutable tuple
    overall_status: str
    recommendations: Tuple[str, ...]  # Immutable tuple
    execution_time: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario para serialización"""
        return {
            "timestamp": self.timestamp.isoformat(),
            "database_info": self.database_info,
            "expected_tables": list(self.expected_tables),
            "existing_tables": list(self.existing_tables),
            "missing_tables": list(self.missing_tables),
            "extra_tables": list(self.extra_tables),
            "table_analyses": [
                {
                    "name": analysis.name,
                    "status": analysis.status.value,
                    "expected_columns": analysis.expected_columns,
                    "actual_columns": analysis.actual_columns,
                    "missing_columns": list(analysis.missing_columns),
                    "extra_columns": list(analysis.extra_columns),
                    "record_count": analysis.record_count,
                    "indexes_count": analysis.indexes_count,
                    "recommendations": list(analysis.recommendations)
                }
                for analysis in self.table_analyses
            ],
            "overall_status": self.overall_status,
            "recommendations": list(self.recommendations),
            "execution_time": self.execution_time
        }


# ============================================================================
# EXPECTED SCHEMA DEFINITION
# ============================================================================

def get_expected_schema() -> SchemaDefinition:
    """
    Pure function: Obtener definición de esquema esperado
    
    Returns:
        Definición inmutable del esquema esperado
    """
    # Definir columnas para cada tabla
    emp_contratos_columns = (
        ColumnDefinition("id", "integer", False, primary_key=True),
        ColumnDefinition("numero_proceso", "text", False),
        ColumnDefinition("objeto_contratar", "text", True),
        ColumnDefinition("modalidad_seleccion", "text", True),
        ColumnDefinition("valor_contrato", "numeric", True),
        ColumnDefinition("fecha_firma", "date", True),
        ColumnDefinition("plazo_ejecucion", "integer", True),
        ColumnDefinition("contratista", "text", True),
        ColumnDefinition("supervisor", "text", True),
        ColumnDefinition("estado", "text", True),
        ColumnDefinition("created_at", "timestamp", False, "CURRENT_TIMESTAMP"),
        ColumnDefinition("updated_at", "timestamp", False, "CURRENT_TIMESTAMP"),
    )
    
    emp_seguimiento_columns = (
        ColumnDefinition("id", "integer", False, primary_key=True),
        ColumnDefinition("numero_proceso", "text", False),
        ColumnDefinition("fecha_seguimiento", "date", False),
        ColumnDefinition("avance_fisico", "numeric", True),
        ColumnDefinition("avance_financiero", "numeric", True),
        ColumnDefinition("observaciones", "text", True),
        ColumnDefinition("estado", "text", True),
        ColumnDefinition("created_at", "timestamp", False, "CURRENT_TIMESTAMP"),
        ColumnDefinition("updated_at", "timestamp", False, "CURRENT_TIMESTAMP"),
    )
    
    emp_proyectos_columns = (
        ColumnDefinition("id", "integer", False, primary_key=True),
        ColumnDefinition("codigo_proyecto", "text", False),
        ColumnDefinition("nombre_proyecto", "text", False),
        ColumnDefinition("descripcion", "text", True),
        ColumnDefinition("valor_total", "numeric", True),
        ColumnDefinition("fecha_inicio", "date", True),
        ColumnDefinition("fecha_fin", "date", True),
        ColumnDefinition("estado", "text", True),
        ColumnDefinition("responsable", "text", True),
        ColumnDefinition("created_at", "timestamp", False, "CURRENT_TIMESTAMP"),
    )
    
    flujo_caja_columns = (
        ColumnDefinition("id", "integer", False, primary_key=True),
        ColumnDefinition("fecha", "date", False),
        ColumnDefinition("concepto", "text", False),
        ColumnDefinition("valor_ingreso", "numeric", True),
        ColumnDefinition("valor_egreso", "numeric", True),
        ColumnDefinition("saldo", "numeric", True),
        ColumnDefinition("categoria", "text", True),
        ColumnDefinition("created_at", "timestamp", False, "CURRENT_TIMESTAMP"),
    )
    
    infraestructura_equipamientos_columns = (
        ColumnDefinition("id", "integer", False, primary_key=True),
        ColumnDefinition("codigo_unidad", "text", False),
        ColumnDefinition("nombre_unidad", "text", False),
        ColumnDefinition("tipo_equipamiento", "text", True),
        ColumnDefinition("valor_unitario", "numeric", True),
        ColumnDefinition("cantidad", "integer", True),
        ColumnDefinition("valor_total", "numeric", True),
        ColumnDefinition("ubicacion", "text", True),
        ColumnDefinition("estado", "text", True),
        ColumnDefinition("created_at", "timestamp", False, "CURRENT_TIMESTAMP"),
    )
    
    infraestructura_vial_columns = (
        ColumnDefinition("id", "integer", False, primary_key=True),
        ColumnDefinition("codigo_unidad", "text", False),
        ColumnDefinition("nombre_unidad", "text", False),
        ColumnDefinition("tipo_via", "text", True),
        ColumnDefinition("longitud", "numeric", True),
        ColumnDefinition("ancho", "numeric", True),
        ColumnDefinition("valor_unitario", "numeric", True),
        ColumnDefinition("valor_total", "numeric", True),
        ColumnDefinition("ubicacion", "text", True),
        ColumnDefinition("estado", "text", True),
        ColumnDefinition("created_at", "timestamp", False, "CURRENT_TIMESTAMP"),
    )
    
    # Definir tablas
    tables = (
        TableDefinition(
            name="emp_contratos",
            columns=emp_contratos_columns,
            indexes=("idx_emp_contratos_numero_proceso", "idx_emp_contratos_estado")
        ),
        TableDefinition(
            name="emp_seguimiento_procesos_dacp",
            columns=emp_seguimiento_columns,
            indexes=("idx_emp_seguimiento_numero_proceso", "idx_emp_seguimiento_fecha")
        ),
        TableDefinition(
            name="emp_proyectos",
            columns=emp_proyectos_columns,
            indexes=("idx_emp_proyectos_codigo", "idx_emp_proyectos_estado")
        ),
        TableDefinition(
            name="flujo_caja",
            columns=flujo_caja_columns,
            indexes=("idx_flujo_caja_fecha", "idx_flujo_caja_categoria")
        ),
        TableDefinition(
            name="unidad_proyecto_infraestructura_equipamientos",
            columns=infraestructura_equipamientos_columns,
            indexes=("idx_unidad_equipamientos_codigo", "idx_unidad_equipamientos_tipo")
        ),
        TableDefinition(
            name="unidad_proyecto_infraestructura_vial",
            columns=infraestructura_vial_columns,
            indexes=("idx_unidad_vial_codigo", "idx_unidad_vial_tipo")
        ),
    )
    
    return SchemaDefinition(
        tables=tables,
        version="1.0",
        created_at=datetime.now()
    )


# ============================================================================
# PURE SCHEMA ANALYSIS FUNCTIONS
# ============================================================================

def get_existing_tables(config=None) -> Set[str]:
    """
    Pure function: Obtener tablas existentes en la base de datos
    
    Args:
        config: Configuración de base de datos (opcional)
        
    Returns:
        Set inmutable de nombres de tablas existentes
    """
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
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
        """)
        
        tables = frozenset(row[0] for row in cursor.fetchall())
        
        cursor.close()
        conn.close()
        
        return tables
        
    except Exception:
        return frozenset()


def get_table_columns(table_name: str, config=None) -> Tuple[ColumnDefinition, ...]:
    """
    Pure function: Obtener columnas de una tabla específica
    
    Args:
        table_name: Nombre de la tabla
        config: Configuración de base de datos (opcional)
        
    Returns:
        Tuple inmutable de definiciones de columnas
    """
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
        cursor.execute("""
            SELECT 
                column_name, 
                data_type, 
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = tuple(
            ColumnDefinition(
                name=row[0],
                data_type=row[1],
                nullable=row[2] == 'YES',
                default=row[3]
            )
            for row in cursor.fetchall()
        )
        
        cursor.close()
        conn.close()
        
        return columns
        
    except Exception:
        return tuple()


def get_table_record_count(table_name: str, config=None) -> int:
    """
    Pure function: Obtener número de registros en una tabla
    
    Args:
        table_name: Nombre de la tabla
        config: Configuración de base de datos (opcional)
        
    Returns:
        Número de registros
    """
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
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return count
        
    except Exception:
        return 0


def compare_table_schemas(expected: TableDefinition, 
                         actual_columns: Tuple[ColumnDefinition, ...]) -> TableAnalysis:
    """
    Pure function: Comparar esquemas de tabla esperado vs actual
    
    Args:
        expected: Definición esperada de la tabla
        actual_columns: Columnas actuales de la tabla
        
    Returns:
        Análisis de la tabla
    """
    expected_column_names = expected.column_names
    actual_column_names = frozenset(col.name for col in actual_columns)
    
    missing_columns = tuple(expected_column_names - actual_column_names)
    extra_columns = tuple(actual_column_names - expected_column_names)
    
    # Determinar estado
    if missing_columns:
        status = TableStatus.MISSING if len(missing_columns) == len(expected_column_names) else TableStatus.MODIFIED
    elif extra_columns:
        status = TableStatus.MODIFIED
    else:
        status = TableStatus.EXISTS
    
    # Generar recomendaciones
    recommendations = []
    if missing_columns:
        recommendations.append(f"Agregar columnas faltantes: {', '.join(missing_columns)}")
    if extra_columns:
        recommendations.append(f"Revisar columnas adicionales: {', '.join(extra_columns)}")
    if not missing_columns and not extra_columns:
        recommendations.append("Estructura de tabla correcta")
    
    return TableAnalysis(
        name=expected.name,
        status=status,
        expected_columns=len(expected.columns),
        actual_columns=len(actual_columns),
        missing_columns=missing_columns,
        extra_columns=extra_columns,
        recommendations=tuple(recommendations)
    )


def analyze_schema_completeness(expected_schema: SchemaDefinition, 
                              existing_tables: Set[str],
                              config=None) -> Tuple[TableAnalysis, ...]:
    """
    Pure function: Analizar completitud del esquema
    
    Args:
        expected_schema: Esquema esperado
        existing_tables: Tablas existentes
        config: Configuración de base de datos (opcional)
        
    Returns:
        Tuple inmutable de análisis de tablas
    """
    analyses = []
    
    for expected_table in expected_schema.tables:
        if expected_table.name in existing_tables:
            # Tabla existe, analizar estructura
            actual_columns = get_table_columns(expected_table.name, config)
            record_count = get_table_record_count(expected_table.name, config)
            
            analysis = compare_table_schemas(expected_table, actual_columns)
            
            # Actualizar con información adicional
            analysis = TableAnalysis(
                name=analysis.name,
                status=analysis.status,
                expected_columns=analysis.expected_columns,
                actual_columns=analysis.actual_columns,
                missing_columns=analysis.missing_columns,
                extra_columns=analysis.extra_columns,
                record_count=record_count,
                indexes_count=0,  # Could be enhanced
                recommendations=analysis.recommendations
            )
        else:
            # Tabla faltante
            analysis = TableAnalysis(
                name=expected_table.name,
                status=TableStatus.MISSING,
                expected_columns=len(expected_table.columns),
                actual_columns=0,
                missing_columns=tuple(col.name for col in expected_table.columns),
                extra_columns=tuple(),
                record_count=0,
                indexes_count=0,
                recommendations=("Crear tabla completa",)
            )
        
        analyses.append(analysis)
    
    return tuple(analyses)


def get_database_info(config=None) -> Dict[str, Any]:
    """
    Pure function: Obtener información general de la base de datos
    
    Args:
        config: Configuración de base de datos (opcional)
        
    Returns:
        Diccionario con información de la base de datos
    """
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
        
        # Versión de PostgreSQL
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        
        # Tamaño de la base de datos
        cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
        db_size = cursor.fetchone()[0]
        
        # Número total de tablas
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        table_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            "version": version,
            "size": db_size,
            "table_count": table_count,
            "host": config.host,
            "port": config.port,
            "database": config.database
        }
        
    except Exception as e:
        return {"error": str(e)}


def calculate_schema_status(table_analyses: Tuple[TableAnalysis, ...]) -> str:
    """
    Pure function: Calcular estado general del esquema
    
    Args:
        table_analyses: Análisis de tablas
        
    Returns:
        Estado general del esquema
    """
    if not table_analyses:
        return "unknown"
    
    missing_count = sum(1 for analysis in table_analyses 
                       if analysis.status == TableStatus.MISSING)
    modified_count = sum(1 for analysis in table_analyses 
                        if analysis.status == TableStatus.MODIFIED)
    
    total_count = len(table_analyses)
    
    if missing_count == 0 and modified_count == 0:
        return "complete"
    elif missing_count > total_count // 2:
        return "critical"
    elif missing_count > 0 or modified_count > total_count // 2:
        return "incomplete"
    else:
        return "minor_issues"


def generate_schema_recommendations(missing_tables: Tuple[str, ...],
                                  extra_tables: Tuple[str, ...],
                                  table_analyses: Tuple[TableAnalysis, ...]) -> Tuple[str, ...]:
    """
    Pure function: Generar recomendaciones de esquema
    
    Args:
        missing_tables: Tablas faltantes
        extra_tables: Tablas adicionales
        table_analyses: Análisis de tablas
        
    Returns:
        Tuple inmutable de recomendaciones
    """
    recommendations = []
    
    # Recomendaciones por tablas faltantes
    if missing_tables:
        recommendations.append(f"Crear {len(missing_tables)} tablas faltantes: {', '.join(missing_tables)}")
    
    # Recomendaciones por tablas adicionales
    if extra_tables:
        filtered_extra = tuple(t for t in extra_tables if not t.startswith('spatial_ref_sys'))
        if filtered_extra:
            recommendations.append(f"Revisar {len(filtered_extra)} tablas adicionales: {', '.join(filtered_extra)}")
    
    # Recomendaciones por análisis de tablas
    modified_tables = [analysis for analysis in table_analyses 
                      if analysis.status == TableStatus.MODIFIED]
    if modified_tables:
        recommendations.append(f"Revisar estructura de {len(modified_tables)} tablas modificadas")
    
    # Recomendaciones por datos
    empty_tables = [analysis for analysis in table_analyses 
                   if analysis.record_count == 0 and analysis.status == TableStatus.EXISTS]
    if empty_tables:
        recommendations.append(f"Cargar datos en {len(empty_tables)} tablas vacías")
    
    # Recomendación general
    if not recommendations:
        recommendations.append("Esquema de base de datos completo y correcto")
    
    return tuple(recommendations)


# ============================================================================
# MAIN ANALYSIS FUNCTION
# ============================================================================

def analyze_database_schema(config=None) -> SchemaAnalysisReport:
    """
    Pure function: Analizar esquema completo de la base de datos
    
    Args:
        config: Configuración de base de datos (opcional)
        
    Returns:
        Reporte inmutable de análisis de esquema
    """
    start_time = time.time()
    timestamp = datetime.now()
    
    # Obtener esquema esperado
    expected_schema = get_expected_schema()
    expected_tables = expected_schema.table_names
    
    # Obtener información de la base de datos
    database_info = get_database_info(config)
    
    # Obtener tablas existentes
    existing_tables = get_existing_tables(config)
    
    # Calcular diferencias
    missing_tables = tuple(expected_tables - existing_tables)
    extra_tables = tuple(existing_tables - expected_tables)
    
    # Analizar cada tabla esperada
    table_analyses = analyze_schema_completeness(expected_schema, existing_tables, config)
    
    # Calcular estado general
    overall_status = calculate_schema_status(table_analyses)
    
    # Generar recomendaciones
    recommendations = generate_schema_recommendations(
        missing_tables, extra_tables, table_analyses
    )
    
    execution_time = time.time() - start_time
    
    return SchemaAnalysisReport(
        timestamp=timestamp,
        database_info=database_info,
        expected_tables=expected_tables,
        existing_tables=existing_tables,
        missing_tables=missing_tables,
        extra_tables=extra_tables,
        table_analyses=table_analyses,
        overall_status=overall_status,
        recommendations=recommendations,
        execution_time=execution_time
    )


# ============================================================================
# SIDE-EFFECT FUNCTIONS (I/O)
# ============================================================================

def save_schema_analysis(report: SchemaAnalysisReport, output_dir: Optional[Path] = None) -> Path:
    """
    Side-effect function: Guardar análisis de esquema
    
    Args:
        report: Reporte de análisis
        output_dir: Directorio de salida (opcional)
        
    Returns:
        Path del archivo guardado
    """
    if output_dir is None:
        output_dir = Path(__file__).parent.parent / "logs" / "schema_analysis"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = report.timestamp.strftime("%Y%m%d_%H%M%S")
    report_file = output_dir / f"schema_analysis_{timestamp}.json"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        import json
        json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)
    
    return report_file


# ============================================================================
# PUBLIC API
# ============================================================================

def run_schema_analysis(config=None, save_report: bool = True) -> SchemaAnalysisReport:
    """
    API Principal: Analizar esquema de base de datos
    
    Args:
        config: Configuración de base de datos (opcional)
        save_report: Si guardar el reporte a disco
        
    Returns:
        Reporte de análisis de esquema
    """
    print("🔍 Iniciando análisis de esquema de base de datos...")
    
    try:
        # Ejecutar análisis (función pura)
        report = analyze_database_schema(config)
        
        # Mostrar resultados
        print(f"✅ Análisis completado - Estado: {report.overall_status.upper()}")
        print(f"   Tablas esperadas: {len(report.expected_tables)}")
        print(f"   Tablas existentes: {len(report.existing_tables)}")
        print(f"   Tablas faltantes: {len(report.missing_tables)}")
        print(f"   Tiempo: {report.execution_time:.2f}s")
        
        # Guardar reporte si se solicita
        if save_report:
            report_path = save_schema_analysis(report)
            print(f"📄 Reporte guardado: {report_path}")
        
        return report
        
    except Exception as e:
        print(f"❌ Error en análisis: {e}")
        raise


if __name__ == "__main__":
    # Ejemplo de uso
    print("🏗️ Sistema de Análisis de Esquemas de Base de Datos")
    print("=" * 60)
    
    try:
        report = run_schema_analysis()
        
        print(f"\n📊 RESULTADO DEL ANÁLISIS")
        print(f"   Estado del Esquema: {report.overall_status.upper()}")
        print(f"   Tablas Esperadas: {len(report.expected_tables)}")
        print(f"   Tablas Existentes: {len(report.existing_tables)}")
        print(f"   Tablas Faltantes: {len(report.missing_tables)}")
        print(f"   Tablas Adicionales: {len(report.extra_tables)}")
        
        if report.missing_tables:
            print(f"\n⚠️ TABLAS FALTANTES:")
            for table in report.missing_tables:
                print(f"   • {table}")
        
        if report.recommendations:
            print(f"\n💡 RECOMENDACIONES:")
            for i, rec in enumerate(report.recommendations[:5], 1):
                print(f"   {i}. {rec}")
        
        # Análisis detallado de tablas
        modified_tables = [analysis for analysis in report.table_analyses 
                          if analysis.status == TableStatus.MODIFIED]
        if modified_tables:
            print(f"\n🔧 TABLAS CON PROBLEMAS:")
            for analysis in modified_tables:
                print(f"   • {analysis.name}: {len(analysis.missing_columns)} columnas faltantes")
        
    except Exception as e:
        print(f"❌ Error: {e}")