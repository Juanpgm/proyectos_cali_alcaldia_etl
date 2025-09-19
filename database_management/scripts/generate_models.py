#!/usr/bin/env python3
"""
Generador de Modelos SQLAlchemy - Alcaldía de Santiago de Cali ETL
================================================================

Este script utiliza programación funcional para generar automáticamente
modelos SQLAlchemy optimizados basados en los esquemas de datos generados
por el analizador de esquemas.

Características:
- Generación automática de modelos SQLAlchemy
- Soporte para relaciones y claves foráneas
- Optimización de índices y constraints
- Integración con PostGIS para datos geoespaciales
- Herencia de BaseModel con timestamps
- Validaciones automáticas
- Código limpio y bien documentado

Autor: Sistema ETL Alcaldía de Cali
Versión: 1.0.0
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import logging
import re
from textwrap import dedent

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# ==========================================
# MAPEO DE TIPOS DE DATOS
# ==========================================

TYPE_MAPPING = {
    'varchar': 'String',
    'text': 'Text',
    'integer': 'Integer',
    'numeric': 'Numeric',
    'boolean': 'Boolean',
    'date': 'Date',
    'datetime': 'DateTime',
    'timestamp': 'DateTime',
    'json': 'JSON',
    'uuid': 'UUID(as_uuid=True)',
    'geometry': 'Geometry',
    'email': 'String',
    'url': 'String',
    'unknown': 'String'
}

IMPORT_MAPPING = {
    'String': 'sqlalchemy',
    'Text': 'sqlalchemy',
    'Integer': 'sqlalchemy',
    'Numeric': 'sqlalchemy',
    'Boolean': 'sqlalchemy',
    'Date': 'sqlalchemy',
    'DateTime': 'sqlalchemy',
    'JSON': 'sqlalchemy',
    'UUID': 'sqlalchemy.dialects.postgresql',
    'Geometry': 'geoalchemy2'
}

# ==========================================
# FUNCIONES UTILITARIAS
# ==========================================

def to_class_name(table_name: str) -> str:
    """Convierte un nombre de tabla a nombre de clase"""
    # Remover sufijos comunes
    table_name = re.sub(r'_(outputs?|data|table)$', '', table_name)
    
    # Convertir a PascalCase
    words = table_name.split('_')
    class_name = ''.join(word.capitalize() for word in words)
    
    # Singularizar (aproximadamente)
    if class_name.endswith('s') and len(class_name) > 3:
        # Casos especiales
        if class_name.endswith('Datos'):
            class_name = class_name[:-5] + 'Dato'
        elif class_name.endswith('Procesos'):
            class_name = class_name[:-8] + 'Proceso'
        elif class_name.endswith('Contratos'):
            class_name = class_name[:-9] + 'Contrato'
        elif class_name.endswith('Proyectos'):
            class_name = class_name[:-9] + 'Proyecto'
        elif class_name.endswith('Indices'):
            class_name = class_name[:-7] + 'Indice'
        elif not class_name.endswith(('ss', 'us')):
            class_name = class_name[:-1]
    
    return class_name

def get_sqlalchemy_type(field_schema: Dict[str, Any]) -> str:
    """Obtiene el tipo SQLAlchemy para un campo"""
    data_type = field_schema.get('data_type', 'varchar')
    max_length = field_schema.get('max_length')
    precision = field_schema.get('precision')
    scale = field_schema.get('scale')
    
    sqlalchemy_type = TYPE_MAPPING.get(data_type, 'String')
    
    # Agregar parámetros según el tipo
    if data_type == 'varchar' and max_length:
        return f"String({max_length})"
    elif data_type == 'numeric' and precision and scale:
        return f"Numeric({precision}, {scale})"
    elif data_type == 'geometry':
        # Asumir Point por defecto, podrías hacer esto más inteligente
        return "Geometry('POINT', srid=4326)"
    
    return sqlalchemy_type

def get_column_definition(field_schema: Dict[str, Any]) -> str:
    """Genera la definición completa de una columna"""
    field_name = field_schema['name']
    sqlalchemy_type = get_sqlalchemy_type(field_schema)
    nullable = field_schema.get('nullable', True)
    is_primary_key = field_schema.get('is_primary_key', False)
    is_foreign_key = field_schema.get('is_foreign_key', False)
    references = field_schema.get('references')
    
    # Construir definición
    definition_parts = [f"Column({sqlalchemy_type}"]
    
    if is_primary_key:
        definition_parts.append("primary_key=True")
    
    if not nullable and not is_primary_key:
        definition_parts.append("nullable=False")
    
    if is_foreign_key and references:
        definition_parts.append(f"ForeignKey('{references}')")
    
    if field_name in ['created_at', 'updated_at']:
        if field_name == 'created_at':
            definition_parts.append("default=datetime.utcnow")
        else:
            definition_parts.append("default=datetime.utcnow")
            definition_parts.append("onupdate=datetime.utcnow")
    
    definition = f"    {field_name} = {', '.join(definition_parts)})"
    
    return definition

def generate_relationships(class_name: str, fields: List[Dict[str, Any]]) -> List[str]:
    """Genera definiciones de relaciones"""
    relationships = []
    
    for field in fields:
        if field.get('is_foreign_key') and field.get('references'):
            references = field['references']
            if '.' in references:
                referenced_table = references.split('.')[0]
                referenced_class = to_class_name(referenced_table)
                
                # Generar nombre de relación
                field_name = field['name']
                if field_name.endswith('_id'):
                    relation_name = field_name[:-3]
                else:
                    relation_name = referenced_table.rstrip('s')
                
                relationship_def = f"    {relation_name} = relationship(\"{referenced_class}\", back_populates=\"{class_name.lower()}s\")"
                relationships.append(relationship_def)
    
    return relationships

def generate_indexes(table_name: str, indexes: List[Dict[str, Any]]) -> List[str]:
    """Genera definiciones de índices"""
    index_definitions = []
    
    for index in indexes:
        index_name = index.get('name', '')
        fields = index.get('fields', [])
        index_type = index.get('type', 'btree')
        
        if index_type == 'primary_key':
            continue  # Se maneja automáticamente
        
        field_list = "', '".join(fields)
        index_def = f"        Index('{index_name}', '{field_list}')"
        
        # Agregar opciones específicas para PostGIS
        if index_type == 'gist':
            index_def += ", postgresql_using='gist'"
        
        index_definitions.append(index_def)
    
    return index_definitions

# ==========================================
# GENERACIÓN DE MODELOS
# ==========================================

def generate_model_class(schema: Dict[str, Any]) -> str:
    """Genera una clase de modelo SQLAlchemy completa"""
    table_name = schema['name']
    class_name = to_class_name(table_name)
    fields = schema.get('fields', [])
    indexes = schema.get('indexes', [])
    description = schema.get('description', '')
    
    # Header del modelo
    model_lines = [
        f"class {class_name}(BaseModel, Base):",
        f'    """Modelo para {description}"""',
        f"    __tablename__ = '{table_name}'",
        ""
    ]
    
    # Generar campos
    for field in fields:
        column_def = get_column_definition(field)
        model_lines.append(column_def)
    
    model_lines.append("")
    
    # Generar relaciones
    relationships = generate_relationships(class_name, fields)
    if relationships:
        model_lines.extend(relationships)
        model_lines.append("")
    
    # Generar índices
    index_definitions = generate_indexes(table_name, indexes)
    if index_definitions:
        model_lines.append("    # Índices")
        model_lines.append("    __table_args__ = (")
        model_lines.extend(index_definitions)
        model_lines.append("    )")
        model_lines.append("")
    
    return "\\n".join(model_lines)

def generate_imports(schemas: List[Dict[str, Any]]) -> List[str]:
    """Genera las importaciones necesarias"""
    imports = [
        '"""',
        'Modelos de datos generados automáticamente',
        'Generado por: database_management/scripts/generate_models.py',
        f'Fecha: {datetime.now().isoformat()}',
        '"""',
        '',
        'from sqlalchemy import (',
        '    Column, Integer, String, Text, DateTime, Numeric, Boolean,',
        '    Date, ForeignKey, Index, CheckConstraint, JSON',
        ')',
        'from sqlalchemy.orm import relationship',
        'from sqlalchemy.dialects.postgresql import UUID',
        'from geoalchemy2 import Geometry',
        'import uuid',
        'from datetime import datetime',
        '',
        'from ..config import Base',
        '',
        '',
        'class BaseModel:',
        '    """Modelo base con campos comunes"""',
        '    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)',
        '    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)',
        '    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)',
        '',
        ''
    ]
    
    return imports

def generate_models_file(schemas: List[Dict[str, Any]]) -> str:
    """Genera el archivo completo de modelos"""
    # Generar importaciones
    imports = generate_imports(schemas)
    
    # Generar modelos
    models = []
    for schema in schemas:
        model_code = generate_model_class(schema)
        models.append(model_code)
    
    # Combinar todo
    full_content = "\\n".join(imports) + "\\n".join(models)
    
    return full_content

# ==========================================
# GENERACIÓN DE SCHEMAS PYDANTIC
# ==========================================

def generate_pydantic_schema(schema: Dict[str, Any]) -> str:
    """Genera esquemas Pydantic para validación"""
    table_name = schema['name']
    class_name = to_class_name(table_name)
    fields = schema.get('fields', [])
    
    # Base schema
    base_lines = [
        f"class {class_name}Base(BaseModel):",
        f'    """Schema base para {table_name}"""',
        "    model_config = ConfigDict(from_attributes=True)",
        ""
    ]
    
    # Agregar campos (excluyendo campos de BaseModel)
    excluded_fields = {'id', 'created_at', 'updated_at'}
    for field in fields:
        if field['name'] not in excluded_fields:
            field_name = field['name']
            python_type = get_python_type(field)
            nullable = field.get('nullable', True)
            
            if nullable:
                field_def = f"    {field_name}: Optional[{python_type}] = None"
            else:
                field_def = f"    {field_name}: {python_type}"
            
            base_lines.append(field_def)
    
    base_lines.extend(["", ""])
    
    # Create schema
    create_lines = [
        f"class {class_name}Create({class_name}Base):",
        f'    """Schema para crear {table_name}"""',
        "    pass",
        "",
        ""
    ]
    
    # Update schema
    update_lines = [
        f"class {class_name}Update(BaseModel):",
        f'    """Schema para actualizar {table_name}"""',
        "    model_config = ConfigDict(from_attributes=True)",
        ""
    ]
    
    # Todos los campos opcionales para update
    for field in fields:
        if field['name'] not in excluded_fields:
            field_name = field['name']
            python_type = get_python_type(field)
            field_def = f"    {field_name}: Optional[{python_type}] = None"
            update_lines.append(field_def)
    
    update_lines.extend(["", ""])
    
    # Response schema
    response_lines = [
        f"class {class_name}Response({class_name}Base):",
        f'    """Schema de respuesta para {table_name}"""',
        "    id: UUID",
        "    created_at: datetime",
        "    updated_at: datetime",
        "",
        ""
    ]
    
    return "\\n".join(base_lines + create_lines + update_lines + response_lines)

def get_python_type(field_schema: Dict[str, Any]) -> str:
    """Convierte tipo de SQLAlchemy a tipo Python"""
    data_type = field_schema.get('data_type', 'varchar')
    
    type_map = {
        'varchar': 'str',
        'text': 'str',
        'integer': 'int',
        'numeric': 'Decimal',
        'boolean': 'bool',
        'date': 'date',
        'datetime': 'datetime',
        'timestamp': 'datetime',
        'json': 'Dict[str, Any]',
        'uuid': 'UUID',
        'geometry': 'str',  # GeoJSON como string
        'email': 'str',
        'url': 'str',
        'unknown': 'str'
    }
    
    return type_map.get(data_type, 'str')

def generate_pydantic_imports() -> List[str]:
    """Genera importaciones para schemas Pydantic"""
    return [
        '"""',
        'Schemas Pydantic generados automáticamente',
        'Generado por: database_management/scripts/generate_models.py',
        f'Fecha: {datetime.now().isoformat()}',
        '"""',
        '',
        'from pydantic import BaseModel, ConfigDict',
        'from typing import Optional, List, Dict, Any',
        'from datetime import datetime, date',
        'from decimal import Decimal',
        'from uuid import UUID',
        '',
        ''
    ]

def generate_pydantic_file(schemas: List[Dict[str, Any]]) -> str:
    """Genera el archivo completo de schemas Pydantic"""
    imports = generate_pydantic_imports()
    
    pydantic_schemas = []
    for schema in schemas:
        pydantic_code = generate_pydantic_schema(schema)
        pydantic_schemas.append(pydantic_code)
    
    return "\\n".join(imports) + "\\n".join(pydantic_schemas)

# ==========================================
# FUNCIÓN PRINCIPAL
# ==========================================

def load_schemas(schemas_file: Path) -> List[Dict[str, Any]]:
    """Carga los esquemas desde el archivo JSON"""
    try:
        with open(schemas_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data.get('schemas', [])
    
    except Exception as e:
        logger.error(f"Error cargando esquemas desde {schemas_file}: {e}")
        return []

def main():
    """Función principal del generador de modelos"""
    logger.info("Iniciando generación de modelos SQLAlchemy")
    
    # Configurar rutas
    project_root = Path(__file__).parent.parent.parent
    schemas_dir = project_root / "database_management" / "generated_schemas"
    output_dir = project_root / "database_management" / "generated_models"
    
    # Crear directorio de salida
    output_dir.mkdir(exist_ok=True)
    
    # Cargar esquemas más recientes
    schemas_file = schemas_dir / "latest_schemas.json"
    
    if not schemas_file.exists():
        logger.error(f"Archivo de esquemas no encontrado: {schemas_file}")
        logger.info("Ejecute primero: python scripts/analyze_data_schema.py")
        return False
    
    try:
        # Cargar esquemas
        schemas = load_schemas(schemas_file)
        
        if not schemas:
            logger.error("No se encontraron esquemas para procesar")
            return False
        
        logger.info(f"Procesando {len(schemas)} esquemas")
        
        # Generar modelos SQLAlchemy
        models_content = generate_models_file(schemas)
        models_file = output_dir / "generated_models.py"
        
        with open(models_file, 'w', encoding='utf-8') as f:
            f.write(models_content)
        
        logger.info(f"Modelos SQLAlchemy generados: {models_file}")
        
        # Generar schemas Pydantic
        pydantic_content = generate_pydantic_file(schemas)
        pydantic_file = output_dir / "generated_schemas.py"
        
        with open(pydantic_file, 'w', encoding='utf-8') as f:
            f.write(pydantic_content)
        
        logger.info(f"Schemas Pydantic generados: {pydantic_file}")
        
        # Generar archivo __init__.py
        init_content = f'''"""
Modelos y schemas generados automáticamente
Generado: {datetime.now().isoformat()}
"""

from .generated_models import *
from .generated_schemas import *

__version__ = "1.0.0"
'''
        
        init_file = output_dir / "__init__.py"
        with open(init_file, 'w', encoding='utf-8') as f:
            f.write(init_content)
        
        logger.info("=" * 60)
        logger.info("GENERACIÓN DE MODELOS COMPLETADA EXITOSAMENTE")
        logger.info(f"Total de modelos: {len(schemas)}")
        logger.info(f"SQLAlchemy models: {models_file}")
        logger.info(f"Pydantic schemas: {pydantic_file}")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"Error durante la generación: {e}")
        raise

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)