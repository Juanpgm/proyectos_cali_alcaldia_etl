#!/usr/bin/env python3
"""
Analizador de Esquemas de Datos - Alcaldía de Santiago de Cali ETL
================================================================

Este script utiliza programación funcional para analizar automáticamente
todos los archivos JSON en transformation_app/app_outputs y generar
esquemas de datos completos y optimizados.

Características:
- Análisis recursivo de todas las subcarpetas
- Detección automática de tipos de datos
- Inferencia de claves primarias y relaciones
- Generación de metadatos completos
- Soporte para datos geoespaciales
- Manejo de campos nullable inteligente

Autor: Sistema ETL Alcaldía de Cali
Versión: 1.0.0
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass, asdict
from functools import reduce, partial
from itertools import chain
import logging
from datetime import datetime, date
import re

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent))

# Configurar logging funcional
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'schema_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)

logger = logging.getLogger(__name__)

@dataclass
class FieldSchema:
    """Esquema de campo individual"""
    name: str
    data_type: str
    nullable: bool
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    references: Optional[str] = None
    default_value: Optional[Any] = None
    constraints: List[str] = None
    description: Optional[str] = None

    def __post_init__(self):
        if self.constraints is None:
            self.constraints = []

@dataclass
class TableSchema:
    """Esquema de tabla completo"""
    name: str
    fields: List[FieldSchema]
    indexes: List[Dict[str, Any]] = None
    constraints: List[str] = None
    source_file: str = ""
    record_count: int = 0
    description: Optional[str] = None

    def __post_init__(self):
        if self.indexes is None:
            self.indexes = []
        if self.constraints is None:
            self.constraints = []

# ==========================================
# FUNCIONES PURAS DE ANÁLISIS DE TIPOS
# ==========================================

def detect_data_type(value: Any) -> str:
    """Detecta el tipo de dato de un valor específico"""
    if value is None:
        return "unknown"
    
    if isinstance(value, bool):
        return "boolean"
    
    if isinstance(value, int):
        # Verificar si es un timestamp unix
        if 1000000000 <= value <= 9999999999:  # Entre 2001 y 2286
            return "timestamp"
        return "integer"
    
    if isinstance(value, float):
        return "numeric"
    
    if isinstance(value, str):
        # Detectar patrones específicos
        if is_uuid(value):
            return "uuid"
        if is_email(value):
            return "email"
        if is_url(value):
            return "url"
        if is_date(value):
            return "date"
        if is_datetime(value):
            return "datetime"
        if is_geojson(value):
            return "geometry"
        if is_json_string(value):
            return "json"
        if is_numeric_string(value):
            return "numeric"
        
        # Verificar longitud para determinar tipo de texto
        if len(value) > 1000:
            return "text"
        return "varchar"
    
    if isinstance(value, (list, dict)):
        return "json"
    
    return "text"

def is_uuid(value: str) -> bool:
    """Verifica si un string es un UUID"""
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return bool(re.match(uuid_pattern, value.lower()))

def is_email(value: str) -> bool:
    """Verifica si un string es un email"""
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(email_pattern, value))

def is_url(value: str) -> bool:
    """Verifica si un string es una URL"""
    url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    return bool(re.match(url_pattern, value))

def is_date(value: str) -> bool:
    """Verifica si un string es una fecha"""
    date_patterns = [
        r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
        r'^\d{2}/\d{2}/\d{4}$',  # DD/MM/YYYY
        r'^\d{2}-\d{2}-\d{4}$',  # DD-MM-YYYY
    ]
    return any(re.match(pattern, value) for pattern in date_patterns)

def is_datetime(value: str) -> bool:
    """Verifica si un string es datetime"""
    datetime_patterns = [
        r'^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
        r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',   # ISO format
    ]
    return any(re.match(pattern, value) for pattern in datetime_patterns)

def is_geojson(value: str) -> bool:
    """Verifica si un string es GeoJSON"""
    try:
        parsed = json.loads(value)
        return isinstance(parsed, dict) and 'type' in parsed and parsed['type'] in ['Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon']
    except:
        return False

def is_json_string(value: str) -> bool:
    """Verifica si un string es JSON válido"""
    try:
        json.loads(value)
        return True
    except:
        return False

def is_numeric_string(value: str) -> bool:
    """Verifica si un string representa un número"""
    try:
        float(value)
        return True
    except:
        return False

# ==========================================
# FUNCIONES DE ANÁLISIS DE CAMPOS
# ==========================================

def analyze_field_values(field_name: str, values: List[Any]) -> FieldSchema:
    """Analiza los valores de un campo y genera su esquema"""
    # Filtrar valores None para análisis
    non_null_values = [v for v in values if v is not None]
    nullable = len(non_null_values) < len(values)
    
    if not non_null_values:
        return FieldSchema(
            name=field_name,
            data_type="varchar",
            nullable=True,
            max_length=255
        )
    
    # Detectar tipos de todos los valores
    types = [detect_data_type(v) for v in non_null_values]
    
    # Consolidar tipo más común
    type_counts = {}
    for t in types:
        type_counts[t] = type_counts.get(t, 0) + 1
    
    primary_type = max(type_counts.keys(), key=lambda k: type_counts[k])
    
    # Calcular características específicas del tipo
    max_length = None
    precision = None
    scale = None
    
    if primary_type in ["varchar", "text"]:
        string_values = [str(v) for v in non_null_values if isinstance(v, str)]
        if string_values:
            max_length = max(len(v) for v in string_values)
            # Ajustar longitud con margen
            if max_length <= 50:
                max_length = 100
            elif max_length <= 255:
                max_length = 255
            elif max_length <= 500:
                max_length = 500
            else:
                primary_type = "text"
                max_length = None
    
    elif primary_type == "numeric":
        numeric_values = [float(v) for v in non_null_values if isinstance(v, (int, float))]
        if numeric_values:
            max_val = max(abs(v) for v in numeric_values)
            # Determinar precisión basada en el valor máximo
            if max_val < 1000:
                precision = 10
                scale = 2
            elif max_val < 1000000:
                precision = 15
                scale = 2
            else:
                precision = 20
                scale = 2
    
    # Detectar si es clave primaria (heurística)
    is_primary_key = detect_primary_key(field_name, non_null_values)
    
    # Detectar si es clave foránea
    is_foreign_key, references = detect_foreign_key(field_name, non_null_values)
    
    return FieldSchema(
        name=field_name,
        data_type=primary_type,
        nullable=nullable,
        max_length=max_length,
        precision=precision,
        scale=scale,
        is_primary_key=is_primary_key,
        is_foreign_key=is_foreign_key,
        references=references
    )

def detect_primary_key(field_name: str, values: List[Any]) -> bool:
    """Detecta si un campo es clave primaria"""
    # Heurísticas para claves primarias
    pk_patterns = ['id', 'codigo', 'bpin', 'numero', 'proceso_compra', 'referencia']
    
    # Verificar nombre del campo
    field_lower = field_name.lower()
    if any(pattern in field_lower for pattern in pk_patterns):
        # Verificar unicidad
        unique_values = set(values)
        if len(unique_values) == len(values):
            return True
    
    return False

def detect_foreign_key(field_name: str, values: List[Any]) -> Tuple[bool, Optional[str]]:
    """Detecta si un campo es clave foránea"""
    fk_patterns = {
        'entidad_id': 'entidades.id',
        'proveedor_id': 'proveedores.id',
        'contrato_id': 'contratos.id',
        'proyecto_id': 'proyectos.id',
        'proceso_id': 'procesos.id'
    }
    
    field_lower = field_name.lower()
    
    for pattern, reference in fk_patterns.items():
        if pattern in field_lower:
            return True, reference
    
    # Detectar por patrones específicos
    if field_lower.endswith('_id') and field_lower != 'id':
        table_name = field_lower[:-3] + 's'
        return True, f"{table_name}.id"
    
    return False, None

# ==========================================
# FUNCIONES DE PROCESAMIENTO DE ARCHIVOS
# ==========================================

def load_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """Carga un archivo JSON de forma segura"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Asegurar que sea una lista
        if isinstance(data, dict):
            return [data]
        elif isinstance(data, list):
            return data
        else:
            logger.warning(f"Formato inesperado en {file_path}: {type(data)}")
            return []
            
    except json.JSONDecodeError as e:
        logger.error(f"Error JSON en {file_path}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error leyendo {file_path}: {e}")
        return []

def extract_table_name(file_path: Path) -> str:
    """Extrae el nombre de tabla del archivo"""
    # Usar el nombre del archivo sin extensión
    base_name = file_path.stem
    
    # Limpiar el nombre para ser válido en BD
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_name)
    table_name = re.sub(r'_+', '_', table_name)  # Múltiples _ a uno solo
    table_name = table_name.strip('_').lower()
    
    return table_name

def analyze_json_file(file_path: Path) -> Optional[TableSchema]:
    """Analiza un archivo JSON y genera su esquema"""
    logger.info(f"Analizando archivo: {file_path}")
    
    data = load_json_file(file_path)
    if not data:
        return None
    
    table_name = extract_table_name(file_path)
    
    # Extraer todos los campos únicos
    all_fields = set()
    for record in data:
        if isinstance(record, dict):
            all_fields.update(record.keys())
    
    # Analizar cada campo
    field_schemas = []
    for field_name in sorted(all_fields):
        # Extraer valores del campo de todos los registros
        field_values = []
        for record in data:
            if isinstance(record, dict):
                field_values.append(record.get(field_name))
        
        field_schema = analyze_field_values(field_name, field_values)
        field_schemas.append(field_schema)
    
    # Generar índices recomendados
    indexes = generate_recommended_indexes(field_schemas, table_name)
    
    return TableSchema(
        name=table_name,
        fields=field_schemas,
        indexes=indexes,
        source_file=str(file_path),
        record_count=len(data),
        description=f"Tabla generada automáticamente desde {file_path.name}"
    )

def generate_recommended_indexes(fields: List[FieldSchema], table_name: str) -> List[Dict[str, Any]]:
    """Genera índices recomendados basados en los campos"""
    indexes = []
    
    # Índice para clave primaria
    pk_fields = [f for f in fields if f.is_primary_key]
    if pk_fields:
        indexes.append({
            "name": f"idx_{table_name}_pk",
            "fields": [f.name for f in pk_fields],
            "type": "primary_key"
        })
    
    # Índices para claves foráneas
    for field in fields:
        if field.is_foreign_key:
            indexes.append({
                "name": f"idx_{table_name}_{field.name}",
                "fields": [field.name],
                "type": "foreign_key"
            })
    
    # Índices para campos comunes de búsqueda
    search_patterns = ['fecha', 'estado', 'tipo', 'codigo', 'nombre']
    for field in fields:
        field_lower = field.name.lower()
        if any(pattern in field_lower for pattern in search_patterns):
            indexes.append({
                "name": f"idx_{table_name}_{field.name}",
                "fields": [field.name],
                "type": "btree"
            })
    
    return indexes

# ==========================================
# FUNCIONES DE DISCOVERY Y PROCESAMIENTO
# ==========================================

def discover_json_files(base_path: Path) -> List[Path]:
    """Descubre todos los archivos JSON en el directorio base"""
    json_files = []
    
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith('.json'):
                file_path = Path(root) / file
                json_files.append(file_path)
    
    logger.info(f"Encontrados {len(json_files)} archivos JSON")
    return sorted(json_files)

def process_all_files(base_path: Path) -> List[TableSchema]:
    """Procesa todos los archivos JSON encontrados"""
    json_files = discover_json_files(base_path)
    
    # Usar programación funcional para procesar archivos
    table_schemas = []
    for file_path in json_files:
        schema = analyze_json_file(file_path)
        if schema:
            table_schemas.append(schema)
    
    logger.info(f"Generados {len(table_schemas)} esquemas de tabla")
    return table_schemas

# ==========================================
# FUNCIONES DE EXPORTACIÓN
# ==========================================

def export_schemas_to_json(schemas: List[TableSchema], output_path: Path) -> None:
    """Exporta los esquemas a un archivo JSON"""
    schemas_dict = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "total_tables": len(schemas),
            "total_records": sum(s.record_count for s in schemas)
        },
        "schemas": [asdict(schema) for schema in schemas]
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(schemas_dict, f, indent=2, ensure_ascii=False, default=str)
    
    logger.info(f"Esquemas exportados a: {output_path}")

def export_schemas_summary(schemas: List[TableSchema], output_path: Path) -> None:
    """Exporta un resumen de los esquemas generados"""
    summary = {
        "generation_info": {
            "timestamp": datetime.now().isoformat(),
            "total_tables": len(schemas),
            "total_fields": sum(len(s.fields) for s in schemas),
            "total_records": sum(s.record_count for s in schemas)
        },
        "tables_summary": []
    }
    
    for schema in schemas:
        table_summary = {
            "table_name": schema.name,
            "source_file": schema.source_file,
            "record_count": schema.record_count,
            "field_count": len(schema.fields),
            "primary_keys": [f.name for f in schema.fields if f.is_primary_key],
            "foreign_keys": [f.name for f in schema.fields if f.is_foreign_key],
            "nullable_fields": [f.name for f in schema.fields if f.nullable],
            "data_types": list(set(f.data_type for f in schema.fields))
        }
        summary["tables_summary"].append(table_summary)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Resumen exportado a: {output_path}")

# ==========================================
# FUNCIÓN PRINCIPAL
# ==========================================

def main():
    """Función principal del analizador de esquemas"""
    logger.info("Iniciando análisis de esquemas de datos")
    
    # Configurar rutas
    project_root = Path(__file__).parent.parent
    transformation_outputs = project_root / "transformation_app" / "app_outputs"
    output_dir = project_root / "database_management" / "generated_schemas"
    
    # Crear directorio de salida
    output_dir.mkdir(exist_ok=True)
    
    # Verificar que existe el directorio de transformación
    if not transformation_outputs.exists():
        logger.error(f"Directorio no encontrado: {transformation_outputs}")
        return False
    
    try:
        # Procesar todos los archivos
        schemas = process_all_files(transformation_outputs)
        
        if not schemas:
            logger.warning("No se generaron esquemas")
            return False
        
        # Exportar resultados
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        schemas_file = output_dir / f"table_schemas_{timestamp}.json"
        export_schemas_to_json(schemas, schemas_file)
        
        summary_file = output_dir / f"schemas_summary_{timestamp}.json"
        export_schemas_summary(schemas, summary_file)
        
        # También exportar la versión más reciente
        latest_schemas = output_dir / "latest_schemas.json"
        export_schemas_to_json(schemas, latest_schemas)
        
        latest_summary = output_dir / "latest_summary.json"
        export_schemas_summary(schemas, latest_summary)
        
        logger.info("=" * 60)
        logger.info(f"✅ ANÁLISIS COMPLETADO EXITOSAMENTE")
        logger.info(f"📊 Total de tablas: {len(schemas)}")
        logger.info(f"📁 Esquemas en: {schemas_file}")
        logger.info(f"📋 Resumen en: {summary_file}")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"Error durante el análisis: {e}")
        raise

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)