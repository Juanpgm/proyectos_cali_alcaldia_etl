#!/usr/bin/env python3
"""
Script Unificador de Análisis de Esquemas - Alcaldía de Santiago de Cali ETL
============================================================================

Este script utiliza programación funcional para unificar todos los análisis
de datos del sistema ETL de proyectos de la Alcaldía de Santiago de Cali.

Consolida la funcionalidad de:
- Análisis de esquemas de datos (analyze_data_schema.py)
- Generación de modelos SQLAlchemy/Pydantic (generate_models.py)
- Generación de scripts SQL de warehouse (generate_sql_warehouse.py)

Características principales:
- Pipeline completo de análisis automático
- Programación funcional con immutability
- Manejo de errores robusto
- Logging comprehensivo
- Configuración centralizada
- Output estructurado y documentado

Autor: Sistema ETL Alcaldía de Cali
Versión: 2.0.0
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, asdict
from datetime import datetime
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor
import shutil

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('unified_schema_analyzer.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ==========================================
# CONFIGURACIÓN UNIFICADA
# ==========================================

@dataclass(frozen=True)
class UnifiedConfig:
    """Configuración inmutable para el analizador unificado"""
    project_root: Path
    data_dir: Path
    output_dir: Path
    models_dir: Path
    sql_dir: Path
    backup_dir: Path
    max_workers: int = 4
    enable_backup: bool = True
    enable_models: bool = True
    enable_sql: bool = True
    log_level: str = "INFO"

def create_unified_config() -> UnifiedConfig:
    """Crea la configuración unificada del sistema"""
    project_root = Path(__file__).parent.parent.parent
    
    return UnifiedConfig(
        project_root=project_root,
        data_dir=project_root / "transformation_app" / "app_outputs",
        output_dir=project_root / "database_management" / "generated_schemas",
        models_dir=project_root / "database_management" / "generated_models",
        sql_dir=project_root / "database_management" / "generated_sql",
        backup_dir=project_root / "database_management" / "backups"
    )

# ==========================================
# FUNCIONES DE ANÁLISIS DE DATOS
# ==========================================

def detect_data_type_unified(value: Any) -> str:
    """Versión unificada de detección de tipos de datos"""
    if value is None:
        return 'unknown'
    
    value_str = str(value).strip()
    
    # Casos especiales
    if not value_str or value_str.lower() in ['null', 'none', 'nan', '']:
        return 'unknown'
    
    # Detección de tipos específicos
    type_patterns = {
        'uuid': lambda x: len(x) == 36 and x.count('-') == 4,
        'email': lambda x: '@' in x and '.' in x.split('@')[-1],
        'url': lambda x: x.startswith(('http://', 'https://', 'www.')),
        'boolean': lambda x: x.lower() in ['true', 'false', '1', '0', 'yes', 'no'],
        'date': lambda x: _is_date(x),
        'datetime': lambda x: _is_datetime(x),
        'timestamp': lambda x: _is_timestamp(x),
        'integer': lambda x: _is_integer(x),
        'numeric': lambda x: _is_numeric(x),
        'json': lambda x: _is_json(x)
    }
    
    for data_type, checker in type_patterns.items():
        try:
            if checker(value_str):
                return data_type
        except:
            continue
    
    # Determinar entre varchar y text
    return 'varchar' if len(value_str) <= 255 else 'text'

def _is_date(value: str) -> bool:
    """Verifica si un valor es una fecha"""
    date_patterns = [
        '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d',
        '%d-%m-%Y', '%m-%d-%Y'
    ]
    
    for pattern in date_patterns:
        try:
            datetime.strptime(value[:10], pattern)
            return True
        except:
            continue
    return False

def _is_datetime(value: str) -> bool:
    """Verifica si un valor es datetime"""
    return ('T' in value or ' ' in value) and any(char.isdigit() for char in value)

def _is_timestamp(value: str) -> bool:
    """Verifica si un valor es timestamp"""
    try:
        float(value)
        return len(value) >= 10 and value.isdigit()
    except:
        return False

def _is_integer(value: str) -> bool:
    """Verifica si un valor es entero"""
    try:
        int(value)
        return '.' not in value
    except:
        return False

def _is_numeric(value: str) -> bool:
    """Verifica si un valor es numérico"""
    try:
        float(value)
        return True
    except:
        return False

def _is_json(value: str) -> bool:
    """Verifica si un valor es JSON válido"""
    try:
        json.loads(value)
        return True
    except:
        return False

def analyze_field_values_unified(values: List[Any]) -> Dict[str, Any]:
    """Versión unificada del análisis de valores de campo"""
    if not values:
        return {
            'data_type': 'unknown',
            'nullable': True,
            'unique_values': 0,
            'null_count': 0,
            'max_length': 0
        }
    
    # Filtrar valores válidos
    valid_values = [v for v in values if v is not None and str(v).strip()]
    null_count = len(values) - len(valid_values)
    
    if not valid_values:
        return {
            'data_type': 'unknown',
            'nullable': True,
            'unique_values': 0,
            'null_count': null_count,
            'max_length': 0
        }
    
    # Análisis de tipos
    type_counts = {}
    for value in valid_values:
        data_type = detect_data_type_unified(value)
        type_counts[data_type] = type_counts.get(data_type, 0) + 1
    
    # Tipo más común
    primary_type = max(type_counts, key=type_counts.get)
    
    # Análisis adicional
    str_values = [str(v) for v in valid_values]
    unique_values = len(set(str_values))
    max_length = max(len(s) for s in str_values) if str_values else 0
    
    # Análisis específico por tipo
    analysis = {
        'data_type': primary_type,
        'nullable': null_count > 0,
        'unique_values': unique_values,
        'null_count': null_count,
        'max_length': max_length,
        'type_distribution': type_counts
    }
    
    # Análisis numérico
    if primary_type in ['integer', 'numeric']:
        numeric_values = []
        for v in valid_values:
            try:
                numeric_values.append(float(v))
            except:
                pass
        
        if numeric_values:
            analysis.update({
                'min_value': min(numeric_values),
                'max_value': max(numeric_values),
                'avg_value': sum(numeric_values) / len(numeric_values)
            })
            
            # Determinar precisión y escala para numeric
            if primary_type == 'numeric':
                decimal_places = []
                for v in str_values:
                    if '.' in v:
                        decimal_places.append(len(v.split('.')[1]))
                
                if decimal_places:
                    analysis['scale'] = max(decimal_places)
                    analysis['precision'] = max_length
    
    return analysis

def process_json_file_unified(file_path: Path) -> Dict[str, Any]:
    """Versión unificada del procesamiento de archivos JSON"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list) or not data:
            return {
                'name': file_path.stem,
                'fields': [],
                'record_count': 0,
                'error': 'No data or invalid format'
            }
        
        # Analizar estructura
        all_fields = {}
        for record in data:
            if isinstance(record, dict):
                for field, value in record.items():
                    if field not in all_fields:
                        all_fields[field] = []
                    all_fields[field].append(value)
        
        # Analizar cada campo
        fields_analysis = []
        for field_name, values in all_fields.items():
            field_analysis = analyze_field_values_unified(values)
            field_analysis['name'] = field_name
            fields_analysis.append(field_analysis)
        
        # Detectar claves primarias potenciales
        for field in fields_analysis:
            if (field['unique_values'] == len(data) and 
                field['null_count'] == 0 and
                field['data_type'] in ['integer', 'uuid', 'varchar']):
                field['is_primary_key'] = True
            else:
                field['is_primary_key'] = False
        
        return {
            'name': file_path.stem,
            'fields': fields_analysis,
            'record_count': len(data),
            'description': f'Tabla generada automáticamente desde {file_path.name}',
            'source_file': str(file_path),
            'analyzed_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error procesando {file_path}: {e}")
        return {
            'name': file_path.stem,
            'fields': [],
            'record_count': 0,
            'error': str(e)
        }

# ==========================================
# FUNCIONES DE GENERACIÓN DE MODELOS
# ==========================================

def generate_sqlalchemy_model_unified(schema: Dict[str, Any]) -> str:
    """Versión unificada de generación de modelos SQLAlchemy"""
    table_name = schema['name']
    fields = schema.get('fields', [])
    
    # Importaciones
    imports = [
        "from sqlalchemy import Column, String, Integer, Boolean, DateTime, Numeric, Text, Date, JSON",
        "from sqlalchemy.dialects.postgresql import UUID, JSONB, GEOMETRY",
        "from sqlalchemy.ext.declarative import declarative_base",
        "from sqlalchemy.sql import func",
        "import uuid"
    ]
    
    # Clase del modelo
    class_lines = [
        f"class {table_name.title().replace('_', '')}(BaseModel):",
        f'    __tablename__ = "{table_name}"',
        "",
        "    # Campos base de auditoria",
        "    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)",
        "    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)",
        "    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)",
        "    version = Column(Integer, default=1, nullable=False)",
        "    is_active = Column(Boolean, default=True, nullable=False)",
        ""
    ]
    
    # Campos del esquema
    type_mapping = {
        'varchar': 'String',
        'text': 'Text',
        'integer': 'Integer',
        'numeric': 'Numeric',
        'boolean': 'Boolean',
        'date': 'Date',
        'datetime': 'DateTime',
        'timestamp': 'DateTime',
        'json': 'JSONB',
        'uuid': 'UUID',
        'geometry': 'GEOMETRY',
        'email': 'String',
        'url': 'Text',
        'unknown': 'Text'
    }
    
    for field in fields:
        if field['name'] in ['id', 'created_at', 'updated_at', 'version', 'is_active']:
            continue
        
        field_name = field['name'].replace('-', '_').replace(' ', '_').lower()
        data_type = field.get('data_type', 'varchar')
        nullable = field.get('nullable', True)
        is_primary_key = field.get('is_primary_key', False)
        max_length = field.get('max_length', 255)
        
        # Mapear tipo
        sql_type = type_mapping.get(data_type, 'Text')
        
        # Construir definición de columna
        if data_type == 'varchar' and max_length:
            column_def = f"Column({sql_type}({max_length})"
        elif data_type == 'numeric':
            precision = field.get('precision', 10)
            scale = field.get('scale', 2)
            column_def = f"Column({sql_type}({precision}, {scale})"
        elif data_type == 'geometry':
            column_def = f"Column(GEOMETRY('POINT', 4326)"
        else:
            column_def = f"Column({sql_type}"
        
        # Agregar restricciones
        if is_primary_key:
            column_def += ", primary_key=True"
        
        if not nullable:
            column_def += ", nullable=False"
        
        column_def += ")"
        
        class_lines.append(f"    {field_name} = {column_def}")
    
    return "\\n".join(imports + ["", ""] + class_lines)

def generate_pydantic_schema_unified(schema: Dict[str, Any]) -> str:
    """Versión unificada de generación de esquemas Pydantic"""
    table_name = schema['name']
    fields = schema.get('fields', [])
    
    # Importaciones
    imports = [
        "from pydantic import BaseModel, Field, ConfigDict",
        "from typing import Optional, Any",
        "from datetime import datetime",
        "from uuid import UUID"
    ]
    
    # Clase del esquema
    class_name = f"{table_name.title().replace('_', '')}Schema"
    class_lines = [
        f"class {class_name}(BaseModel):",
        "    model_config = ConfigDict(from_attributes=True)",
        "",
        "    # Campos base",
        "    id: UUID",
        "    created_at: datetime",
        "    updated_at: datetime", 
        "    version: int = 1",
        "    is_active: bool = True",
        ""
    ]
    
    # Mapeo de tipos Pydantic
    pydantic_types = {
        'varchar': 'str',
        'text': 'str',
        'integer': 'int',
        'numeric': 'float',
        'boolean': 'bool',
        'date': 'datetime',
        'datetime': 'datetime',
        'timestamp': 'datetime',
        'json': 'Any',
        'uuid': 'UUID',
        'geometry': 'Any',
        'email': 'str',
        'url': 'str',
        'unknown': 'Any'
    }
    
    for field in fields:
        if field['name'] in ['id', 'created_at', 'updated_at', 'version', 'is_active']:
            continue
        
        field_name = field['name'].replace('-', '_').replace(' ', '_').lower()
        data_type = field.get('data_type', 'varchar')
        nullable = field.get('nullable', True)
        
        pydantic_type = pydantic_types.get(data_type, 'Any')
        
        if nullable:
            field_def = f"    {field_name}: Optional[{pydantic_type}] = None"
        else:
            field_def = f"    {field_name}: {pydantic_type}"
        
        class_lines.append(field_def)
    
    return "\\n".join(imports + ["", ""] + class_lines)

# ==========================================
# PIPELINE UNIFICADO
# ==========================================

def run_unified_analysis(config: UnifiedConfig) -> Dict[str, Any]:
    """Ejecuta el análisis unificado completo"""
    results = {
        'started_at': datetime.now().isoformat(),
        'config': asdict(config),
        'schemas': [],
        'models_generated': False,
        'sql_generated': False,
        'errors': []
    }
    
    try:
        # 1. Crear directorios necesarios
        for directory in [config.output_dir, config.models_dir, config.sql_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        if config.enable_backup and config.backup_dir:
            config.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 2. Buscar archivos JSON (recursivamente)
        json_files = list(config.data_dir.rglob("*.json"))
        
        if not json_files:
            raise ValueError(f"No se encontraron archivos JSON en {config.data_dir} (búsqueda recursiva)")
        
        logger.info(f"Encontrados {len(json_files)} archivos JSON para procesar")
        
        # 3. Procesar archivos en paralelo
        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            schemas = list(executor.map(process_json_file_unified, json_files))
        
        # Filtrar esquemas exitosos
        valid_schemas = [s for s in schemas if 'error' not in s]
        error_schemas = [s for s in schemas if 'error' in s]
        
        logger.info(f"Procesados exitosamente: {len(valid_schemas)} esquemas")
        if error_schemas:
            logger.warning(f"Errores en: {len(error_schemas)} archivos")
            results['errors'].extend(error_schemas)
        
        results['schemas'] = valid_schemas
        
        # 4. Guardar esquemas
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        schemas_file = config.output_dir / f"unified_schemas_{timestamp}.json"
        with open(schemas_file, 'w', encoding='utf-8') as f:
            json.dump({
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'total_schemas': len(valid_schemas),
                    'total_records': sum(s.get('record_count', 0) for s in valid_schemas),
                    'generator_version': '2.0.0'
                },
                'schemas': valid_schemas
            }, f, indent=2, ensure_ascii=False)
        
        # También guardar como latest
        latest_file = config.output_dir / "latest_schemas.json"
        shutil.copy2(schemas_file, latest_file)
        
        logger.info(f"Esquemas guardados en: {schemas_file}")
        
        # 5. Generar modelos si está habilitado
        if config.enable_models and valid_schemas:
            try:
                models_generated = generate_models_unified(valid_schemas, config)
                results['models_generated'] = models_generated
            except Exception as e:
                logger.error(f"Error generando modelos: {e}")
                results['errors'].append(f"Error en modelos: {e}")
        
        # 6. Generar SQL si está habilitado  
        if config.enable_sql and valid_schemas:
            try:
                sql_generated = generate_sql_unified(valid_schemas, config)
                results['sql_generated'] = sql_generated
            except Exception as e:
                logger.error(f"Error generando SQL: {e}")
                results['errors'].append(f"Error en SQL: {e}")
        
        results['completed_at'] = datetime.now().isoformat()
        results['success'] = True
        
        return results
        
    except Exception as e:
        logger.error(f"Error en análisis unificado: {e}")
        logger.error(traceback.format_exc())
        results['errors'].append(str(e))
        results['success'] = False
        results['completed_at'] = datetime.now().isoformat()
        return results

def generate_models_unified(schemas: List[Dict[str, Any]], config: UnifiedConfig) -> bool:
    """Genera modelos SQLAlchemy y Pydantic unificados"""
    try:
        # Limpiar directorio anterior
        if config.models_dir.exists():
            shutil.rmtree(config.models_dir)
        config.models_dir.mkdir(parents=True)
        
        # Generar archivos
        all_models = []
        all_schemas = []
        
        for schema in schemas:
            try:
                model_code = generate_sqlalchemy_model_unified(schema)
                schema_code = generate_pydantic_schema_unified(schema)
                
                all_models.append(model_code)
                all_schemas.append(schema_code)
                
            except Exception as e:
                logger.error(f"Error generando modelo para {schema.get('name', 'unknown')}: {e}")
                continue
        
        # Crear archivo de modelos
        models_content = [
            "# Modelos SQLAlchemy generados automáticamente",
            "# Alcaldía de Santiago de Cali ETL",
            f"# Generado: {datetime.now().isoformat()}",
            "",
            "from sqlalchemy.ext.declarative import declarative_base",
            "",
            "BaseModel = declarative_base()",
            "",
            *all_models
        ]
        
        models_file = config.models_dir / "models.py"
        with open(models_file, 'w', encoding='utf-8') as f:
            f.write("\\n".join(models_content))
        
        # Crear archivo de esquemas Pydantic
        schemas_content = [
            "# Esquemas Pydantic generados automáticamente", 
            "# Alcaldía de Santiago de Cali ETL",
            f"# Generado: {datetime.now().isoformat()}",
            "",
            *all_schemas
        ]
        
        schemas_file = config.models_dir / "schemas.py"
        with open(schemas_file, 'w', encoding='utf-8') as f:
            f.write("\\n".join(schemas_content))
        
        # Crear __init__.py
        init_content = [
            f'"""',
            f'Modelos y esquemas generados automáticamente',
            f'Alcaldía de Santiago de Cali ETL',
            f'Generado: {datetime.now().isoformat()}',
            f'"""',
            "",
            "from .models import *",
            "from .schemas import *"
        ]
        
        init_file = config.models_dir / "__init__.py"
        with open(init_file, 'w', encoding='utf-8') as f:
            f.write("\\n".join(init_content))
        
        logger.info(f"Modelos generados en: {config.models_dir}")
        return True
        
    except Exception as e:
        logger.error(f"Error en generación de modelos: {e}")
        return False

def generate_sql_unified(schemas: List[Dict[str, Any]], config: UnifiedConfig) -> bool:
    """Genera scripts SQL unificados"""
    try:
        # Importar funciones del generador SQL warehouse
        sql_generator_path = config.project_root / "database_management" / "scripts" / "generate_sql_warehouse.py"
        
        if sql_generator_path.exists():
            # Ejecutar el generador SQL existente
            import subprocess
            result = subprocess.run([
                sys.executable, 
                str(sql_generator_path)
            ], capture_output=True, text=True, cwd=str(config.project_root))
            
            if result.returncode == 0:
                logger.info("Scripts SQL generados exitosamente")
                return True
            else:
                logger.error(f"Error ejecutando generador SQL: {result.stderr}")
                return False
        else:
            logger.error(f"Generador SQL no encontrado: {sql_generator_path}")
            return False
            
    except Exception as e:
        logger.error(f"Error en generación SQL: {e}")
        return False

def print_unified_summary(results: Dict[str, Any]) -> None:
    """Imprime un resumen unificado de los resultados"""
    print("\\n" + "=" * 80)
    print("ANÁLISIS UNIFICADO DE ESQUEMAS - ALCALDÍA DE SANTIAGO DE CALI ETL")
    print("=" * 80)
    
    print(f"Iniciado: {results.get('started_at', 'N/A')}")
    print(f"Completado: {results.get('completed_at', 'N/A')}")
    print(f"Estado: {'✅ EXITOSO' if results.get('success') else '❌ CON ERRORES'}")
    
    print("\\n📊 ESTADÍSTICAS:")
    schemas = results.get('schemas', [])
    total_records = sum(s.get('record_count', 0) for s in schemas)
    total_fields = sum(len(s.get('fields', [])) for s in schemas)
    
    print(f"  • Esquemas procesados: {len(schemas)}")
    print(f"  • Total de registros: {total_records:,}")
    print(f"  • Total de campos: {total_fields:,}")
    
    print("\\n🔧 COMPONENTES GENERADOS:")
    print(f"  • Modelos SQLAlchemy/Pydantic: {'✅' if results.get('models_generated') else '❌'}")
    print(f"  • Scripts SQL Warehouse: {'✅' if results.get('sql_generated') else '❌'}")
    
    if results.get('errors'):
        print(f"\\n⚠️  ERRORES ({len(results['errors'])}):")
        for error in results['errors'][:5]:  # Mostrar solo los primeros 5
            print(f"  • {error}")
        if len(results['errors']) > 5:
            print(f"  ... y {len(results['errors']) - 5} errores más")
    
    # Resumen de tablas
    if schemas:
        print("\\n📋 TABLAS ANALIZADAS:")
        for schema in schemas[:10]:  # Mostrar solo las primeras 10
            name = schema.get('name', 'unknown')
            record_count = schema.get('record_count', 0)
            field_count = len(schema.get('fields', []))
            print(f"  • {name}: {record_count:,} registros, {field_count} campos")
        
        if len(schemas) > 10:
            print(f"  ... y {len(schemas) - 10} tablas más")
    
    print("\\n" + "=" * 80)

def main():
    """Función principal del analizador unificado"""
    logger.info("Iniciando análisis unificado de esquemas")
    
    try:
        # Crear configuración
        config = create_unified_config()
        
        # Ejecutar análisis
        results = run_unified_analysis(config)
        
        # Mostrar resumen
        print_unified_summary(results)
        
        # Guardar resultados (convertir Path a string)
        results_to_save = {
            k: (str(v) if isinstance(v, Path) else v) 
            for k, v in results.items()
        }
        
        # Convertir paths en config también
        if 'config' in results_to_save:
            config_serializable = {}
            for k, v in results_to_save['config'].items():
                if isinstance(v, Path):
                    config_serializable[k] = str(v)
                else:
                    config_serializable[k] = v
            results_to_save['config'] = config_serializable
        
        results_file = config.output_dir / f"unified_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results_to_save, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Resultados guardados en: {results_file}")
        
        return results.get('success', False)
        
    except Exception as e:
        logger.error(f"Error crítico en main: {e}")
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)