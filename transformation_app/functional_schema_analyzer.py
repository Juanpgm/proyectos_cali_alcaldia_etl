#!/usr/bin/env python3
"""
Analizador Funcional de Esquemas JSON
====================================

Sistema funcional puro para análisis automático de cualquier estructura JSON
y generación de esquemas de base de datos completos.

Características:
- Detección automática de formatos (datos/metadata, arrays, objetos, anidados)
- Análisis recursivo de estructuras complejas
- Mapeo completo de todos los campos
- Funciones puras sin efectos secundarios
- Soporte para múltiples tipos de datos
"""

import json
import re
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Any, Set, Optional, Tuple, Union
from functools import reduce
from itertools import chain


# ============================================================================
# TIPOS DE DATOS FUNCIONALES
# ============================================================================

@dataclass(frozen=True)
class FieldInfo:
    """Información de un campo detectado"""
    name: str
    data_type: str
    max_length: Optional[int]
    is_nullable: bool
    sample_values: Tuple[Any, ...]
    frequency: int
    
@dataclass(frozen=True)
class StructureInfo:
    """Información de estructura detectada"""
    structure_type: str  # 'metadata_datos', 'array', 'object', 'nested'
    data_path: str      # Ruta a los datos reales
    total_records: int
    fields: Tuple[FieldInfo, ...]
    nested_structures: Tuple['StructureInfo', ...] = ()

@dataclass(frozen=True)
class SchemaResult:
    """Resultado del análisis de esquema"""
    file_path: str
    structures: Tuple[StructureInfo, ...]
    total_fields: int
    total_records: int
    analysis_time: float
    generated_at: datetime


# ============================================================================
# FUNCIONES PURAS DE DETECCIÓN DE ESTRUCTURA
# ============================================================================

def detect_json_structure(data: Any) -> str:
    """Función pura: Detectar tipo de estructura JSON"""
    if isinstance(data, dict):
        keys = set(data.keys())
        
        # Formato datos/metadata
        if 'datos' in keys and 'metadata' in keys:
            return 'metadata_datos'
        
        # Objeto con muchas claves (probablemente registro único)
        if len(keys) > 10:
            return 'object'
            
        # Objeto con arrays como valores
        if any(isinstance(v, list) for v in data.values()):
            return 'nested'
            
        return 'object'
    
    elif isinstance(data, list):
        return 'array'
    
    else:
        return 'primitive'

def extract_data_records(data: Any, structure_type: str) -> Tuple[List[Dict], str]:
    """Función pura: Extraer registros de datos según tipo de estructura"""
    
    if structure_type == 'metadata_datos':
        if 'datos' in data and isinstance(data['datos'], list):
            return data['datos'], 'datos'
        return [], 'datos'
    
    elif structure_type == 'array':
        if isinstance(data, list):
            # Filtrar solo diccionarios
            records = [item for item in data if isinstance(item, dict)]
            return records, 'root'
        return [], 'root'
    
    elif structure_type == 'object':
        # Objeto único como registro
        if isinstance(data, dict):
            return [data], 'root'
        return [], 'root'
    
    elif structure_type == 'nested':
        # Buscar arrays anidados
        records = []
        for key, value in data.items():
            if isinstance(value, list):
                nested_records = [item for item in value if isinstance(item, dict)]
                records.extend(nested_records)
        return records, 'nested'
    
    return [], 'unknown'

def analyze_field_values(values: List[Any], field_name: str) -> FieldInfo:
    """Función pura: Analizar valores de un campo"""
    
    # Filtrar valores no nulos
    non_null_values = [v for v in values if v is not None and str(v).strip() != '']
    total_values = len(values)
    non_null_count = len(non_null_values)
    
    # Determinar si es nullable
    is_nullable = non_null_count < total_values
    
    # Si no hay valores, campo de texto nullable
    if non_null_count == 0:
        return FieldInfo(
            name=field_name,
            data_type='VARCHAR(255)',
            max_length=255,
            is_nullable=True,
            sample_values=(),
            frequency=0
        )
    
    # Tomar muestra de valores únicos
    unique_values = list(set(str(v) for v in non_null_values[:100]))
    sample_values = tuple(unique_values[:5])
    
    # Detectar tipo de datos
    data_type, max_length = detect_field_type(non_null_values)
    
    return FieldInfo(
        name=field_name,
        data_type=data_type,
        max_length=max_length,
        is_nullable=is_nullable,
        sample_values=sample_values,
        frequency=non_null_count
    )

def detect_field_type(values: List[Any]) -> Tuple[str, Optional[int]]:
    """Función pura: Detectar tipo SQL para valores de campo"""
    
    if not values:
        return 'VARCHAR(255)', 255
    
    # Convertir todos a string para análisis
    str_values = [str(v) for v in values]
    
    # Detectar números enteros
    int_pattern = re.compile(r'^-?\d+$')
    if all(int_pattern.match(s) for s in str_values):
        max_val = max(abs(int(v)) for v in str_values)
        if max_val < 2147483647:  # INTEGER range
            return 'INTEGER', None
        else:
            return 'BIGINT', None
    
    # Detectar números decimales
    float_pattern = re.compile(r'^-?\d*\.\d+$')
    if all(float_pattern.match(s) or int_pattern.match(s) for s in str_values):
        return 'DECIMAL(15,4)', None
    
    # Detectar booleanos
    bool_values = {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n', 't', 'f'}
    if all(s.lower() in bool_values for s in str_values):
        return 'BOOLEAN', None
    
    # Detectar fechas
    date_patterns = [
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY
        r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
    ]
    
    for pattern in date_patterns:
        if all(re.search(pattern, s) for s in str_values):
            return 'DATE', None
    
    # Texto - calcular longitud máxima
    max_length = max(len(s) for s in str_values)
    
    if max_length <= 255:
        return f'VARCHAR({max(255, max_length)})', max_length
    elif max_length <= 1000:
        return f'VARCHAR(1000)', max_length
    else:
        return 'TEXT', max_length

def sanitize_field_name(name: str) -> str:
    """Función pura: Sanitizar nombre de campo para SQL"""
    # Convertir a minúsculas
    sanitized = str(name).lower()
    
    # Reemplazar caracteres especiales
    sanitized = re.sub(r'[^\w]', '_', sanitized)
    
    # Eliminar guiones bajos múltiples
    sanitized = re.sub(r'_+', '_', sanitized)
    
    # Eliminar guiones bajos al inicio y final
    sanitized = sanitized.strip('_')
    
    # Asegurar que no empiece con número
    if sanitized and sanitized[0].isdigit():
        sanitized = f"field_{sanitized}"
    
    # Evitar palabras reservadas
    sql_reserved = {'user', 'order', 'group', 'table', 'column', 'index', 'primary', 'foreign', 'key'}
    if sanitized in sql_reserved:
        sanitized = f"{sanitized}_field"
    
    # Si queda vacío, usar nombre por defecto
    if not sanitized:
        sanitized = "unnamed_field"
    
    return sanitized

def analyze_records_structure(records: List[Dict]) -> Tuple[FieldInfo, ...]:
    """Función pura: Analizar estructura de registros"""
    
    if not records:
        return ()
    
    # Recopilar todos los campos únicos
    all_fields = set()
    for record in records:
        if isinstance(record, dict):
            all_fields.update(record.keys())
    
    # Analizar cada campo
    field_analyses = []
    
    for field_name in sorted(all_fields):
        # Extraer valores para este campo
        field_values = []
        for record in records:
            if isinstance(record, dict):
                value = record.get(field_name)
                field_values.append(value)
        
        # Sanitizar nombre del campo
        clean_field_name = sanitize_field_name(field_name)
        
        # Analizar campo
        field_info = analyze_field_values(field_values, clean_field_name)
        field_analyses.append(field_info)
    
    return tuple(field_analyses)

def analyze_nested_structures(data: Dict, max_depth: int = 3) -> Tuple[StructureInfo, ...]:
    """Función pura: Analizar estructuras anidadas recursivamente"""
    
    if max_depth <= 0:
        return ()
    
    nested_structures = []
    
    for key, value in data.items():
        if isinstance(value, list) and value:
            # Verificar si es una lista de diccionarios
            dict_items = [item for item in value if isinstance(item, dict)]
            
            if dict_items:
                # Analizar como estructura independiente
                fields = analyze_records_structure(dict_items)
                
                structure = StructureInfo(
                    structure_type='nested_array',
                    data_path=f"nested.{key}",
                    total_records=len(dict_items),
                    fields=fields
                )
                nested_structures.append(structure)
        
        elif isinstance(value, dict):
            # Análisis recursivo de objetos anidados
            sub_nested = analyze_nested_structures(value, max_depth - 1)
            nested_structures.extend(sub_nested)
    
    return tuple(nested_structures)


# ============================================================================
# FUNCIÓN PRINCIPAL DE ANÁLISIS
# ============================================================================

def analyze_json_file_comprehensive(file_path: Path) -> SchemaResult:
    """Función pura: Análisis completo de archivo JSON"""
    
    start_time = datetime.now()
    
    try:
        # Leer archivo
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Detectar estructura principal
        main_structure_type = detect_json_structure(data)
        
        # Extraer registros principales
        main_records, data_path = extract_data_records(data, main_structure_type)
        
        # Analizar campos principales
        main_fields = analyze_records_structure(main_records)
        
        # Crear estructura principal
        main_structure = StructureInfo(
            structure_type=main_structure_type,
            data_path=data_path,
            total_records=len(main_records),
            fields=main_fields
        )
        
        structures = [main_structure]
        
        # Analizar estructuras anidadas si es apropiado
        if isinstance(data, dict) and main_structure_type in ['metadata_datos', 'nested']:
            nested_structures = analyze_nested_structures(data)
            structures.extend(nested_structures)
        
        # Calcular estadísticas totales
        total_fields = sum(len(s.fields) for s in structures)
        total_records = sum(s.total_records for s in structures)
        
        # Tiempo de análisis
        analysis_time = (datetime.now() - start_time).total_seconds()
        
        return SchemaResult(
            file_path=str(file_path),
            structures=tuple(structures),
            total_fields=total_fields,
            total_records=total_records,
            analysis_time=analysis_time,
            generated_at=start_time
        )
    
    except Exception as e:
        # En caso de error, retornar resultado vacío
        analysis_time = (datetime.now() - start_time).total_seconds()
        
        return SchemaResult(
            file_path=str(file_path),
            structures=(),
            total_fields=0,
            total_records=0,
            analysis_time=analysis_time,
            generated_at=start_time
        )


# ============================================================================
# FUNCIONES DE REPORTE
# ============================================================================

def print_schema_analysis(result: SchemaResult) -> None:
    """Función pura: Imprimir análisis de esquema"""
    
    print(f"\n📊 ANÁLISIS COMPLETO: {Path(result.file_path).name}")
    print("=" * 60)
    print(f"📁 Archivo: {result.file_path}")
    print(f"⏱️  Tiempo de análisis: {result.analysis_time:.3f}s")
    print(f"📊 Total de estructuras: {len(result.structures)}")
    print(f"📈 Total de registros: {result.total_records:,}")
    print(f"📏 Total de campos: {result.total_fields}")
    
    for i, structure in enumerate(result.structures, 1):
        print(f"\n🔍 ESTRUCTURA {i}: {structure.structure_type}")
        print("-" * 40)
        print(f"📍 Ruta de datos: {structure.data_path}")
        print(f"📊 Registros: {structure.total_records:,}")
        print(f"📏 Campos: {len(structure.fields)}")
        
        if structure.fields:
            print("\n📋 CAMPOS DETECTADOS:")
            for j, field in enumerate(structure.fields[:20], 1):  # Mostrar primeros 20
                nullable = "NULL" if field.is_nullable else "NOT NULL"
                sample = f" | Ej: {field.sample_values[0]}" if field.sample_values else ""
                print(f"  {j:2d}. {field.name:<25} {field.data_type:<15} {nullable}{sample}")
            
            if len(structure.fields) > 20:
                print(f"  ... y {len(structure.fields) - 20} campos más")


if __name__ == "__main__":
    # Probar con el archivo problemático
    file_path = Path("app_outputs/contratos_dacp/contratacion_dacp.json")
    if file_path.exists():
        result = analyze_json_file_comprehensive(file_path)
        print_schema_analysis(result)
    else:
        print(f"❌ Archivo no encontrado: {file_path}")