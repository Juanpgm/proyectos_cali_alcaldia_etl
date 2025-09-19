"""
Model generator module for automatic SQLAlchemy model creation.

Generates SQLAlchemy models from JSON/GeoJSON data using functional programming.
"""

import logging
from typing import Dict, List, Any, Optional, Type
from pathlib import Path
import json
from dataclasses import dataclass
from functools import reduce

from sqlalchemy import Column, Integer, BigInteger, Numeric, Text, Boolean, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry

from .schema_generator import SchemaGenerator, ColumnInfo, TableSchema

logger = logging.getLogger(__name__)

Base = declarative_base()


@dataclass(frozen=True)
class ModelInfo:
    """Information about a generated model."""
    name: str
    table_name: str
    columns: Dict[str, str]
    is_spatial: bool
    file_path: Optional[Path] = None


class ModelGenerator:
    """Generate SQLAlchemy models from schema information."""
    
    def __init__(self):
        self.type_mapping = self._create_sqlalchemy_type_mapping()
        self.generated_models = {}
    
    @staticmethod
    def _create_sqlalchemy_type_mapping() -> Dict[str, Type]:
        """Create mapping from PostgreSQL types to SQLAlchemy types."""
        return {
            "SERIAL": Integer,
            "INTEGER": Integer,
            "BIGINT": BigInteger,
            "NUMERIC": Numeric,
            "TEXT": Text,
            "BOOLEAN": Boolean,
            "TIMESTAMP": DateTime,
            "DATE": DateTime,
            "JSONB": JSONB,
            "JSON": JSON,
            "GEOMETRY": Geometry,
            "GEOMETRY(POINT, 4326)": Geometry('POINT', srid=4326),
            "GEOMETRY(POLYGON, 4326)": Geometry('POLYGON', srid=4326),
            "GEOMETRY(LINESTRING, 4326)": Geometry('LINESTRING', srid=4326),
            "GEOMETRY(MULTIPOINT, 4326)": Geometry('MULTIPOINT', srid=4326),
            "GEOMETRY(MULTIPOLYGON, 4326)": Geometry('MULTIPOLYGON', srid=4326),
            "GEOMETRY(MULTILINESTRING, 4326)": Geometry('MULTILINESTRING', srid=4326),
        }
    
    def _get_sqlalchemy_type(self, pg_type: str) -> Type:
        """
        Convert PostgreSQL type to SQLAlchemy type.
        
        Args:
            pg_type: PostgreSQL type string
            
        Returns:
            SQLAlchemy type class
        """
        return self.type_mapping.get(pg_type, Text)
    
    def _create_model_class_name(self, table_name: str) -> str:
        """
        Create a proper class name from table name.
        
        Args:
            table_name: Database table name
            
        Returns:
            Pascal case class name
        """
        # Convert to Pascal case
        words = table_name.replace('_', ' ').title().split()
        class_name = ''.join(words)
        
        # Ensure it starts with a letter
        if class_name and not class_name[0].isalpha():
            class_name = f"Table{class_name}"
        
        return class_name or "GeneratedModel"
    
    def generate_model_from_schema(self, schema: TableSchema) -> Type:
        """
        Generate SQLAlchemy model from table schema.
        
        Args:
            schema: Table schema information
            
        Returns:
            Generated SQLAlchemy model class
        """
        class_name = self._create_model_class_name(schema.name)
        
        # Create attributes dictionary
        attrs = {
            "__tablename__": schema.name,
            "__table_args__": {"comment": schema.comment or f"Auto-generated model for {schema.name}"}
        }
        
        # Add columns
        for column_info in schema.columns:
            sqlalchemy_type = self._get_sqlalchemy_type(column_info.sql_type)
            
            column_kwargs = {
                "nullable": column_info.nullable,
                "comment": column_info.comment
            }
            
            if column_info.primary_key:
                column_kwargs["primary_key"] = True
                if column_info.sql_type == "SERIAL":
                    column_kwargs["autoincrement"] = True
            
            if column_info.unique:
                column_kwargs["unique"] = True
            
            if column_info.index:
                column_kwargs["index"] = True
            
            # Handle default values for audit columns
            if column_info.name in ["created_at", "updated_at"]:
                from sqlalchemy import func
                column_kwargs["default"] = func.now()
                if column_info.name == "updated_at":
                    column_kwargs["onupdate"] = func.now()
            
            attrs[column_info.name] = Column(sqlalchemy_type, **column_kwargs)
        
        # Create the model class
        model_class = type(class_name, (Base,), attrs)
        
        # Store the generated model
        self.generated_models[schema.name] = model_class
        
        logger.info(f"Generated model {class_name} for table {schema.name}")
        return model_class
    
    def generate_models_from_directory(self, directory_path: Path) -> Dict[str, Type]:
        """
        Generate models for all JSON/GeoJSON files in a directory.
        
        Args:
            directory_path: Path to directory containing data files
            
        Returns:
            Dictionary mapping table names to model classes
        """
        schema_generator = SchemaGenerator()
        models = {}
        
        try:
            # Find all JSON and GeoJSON files
            json_files = list(directory_path.glob("*.json"))
            geojson_files = list(directory_path.glob("*.geojson"))
            all_files = json_files + geojson_files
            
            for file_path in all_files:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    table_name = file_path.stem
                    
                    # Generate schema
                    is_geojson = (
                        file_path.suffix.lower() == '.geojson' or
                        (isinstance(data, dict) and data.get('type') == 'FeatureCollection')
                    )
                    
                    if is_geojson:
                        field_types = schema_generator.analyze_geojson_structure(data)
                        is_spatial = True
                    else:
                        field_types = schema_generator.analyze_json_structure(data)
                        is_spatial = False
                    
                    schema = schema_generator.generate_table_schema(
                        table_name=table_name,
                        field_types=field_types,
                        is_spatial=is_spatial
                    )
                    
                    # Generate model
                    model_class = self.generate_model_from_schema(schema)
                    models[table_name] = model_class
                    
                except Exception as e:
                    logger.error(f"Failed to generate model for {file_path}: {e}")
                    continue
            
            logger.info(f"Generated {len(models)} models from {directory_path}")
            return models
            
        except Exception as e:
            logger.error(f"Failed to generate models from directory {directory_path}: {e}")
            return {}
    
    def get_model_info(self, table_name: str) -> Optional[ModelInfo]:
        """
        Get information about a generated model.
        
        Args:
            table_name: Name of the table
            
        Returns:
            ModelInfo or None if model not found
        """
        if table_name not in self.generated_models:
            return None
        
        model_class = self.generated_models[table_name]
        
        # Extract column information
        columns = {}
        is_spatial = False
        
        for column_name, column in model_class.__table__.columns.items():
            columns[column_name] = str(column.type)
            if "GEOMETRY" in str(column.type).upper():
                is_spatial = True
        
        return ModelInfo(
            name=model_class.__name__,
            table_name=table_name,
            columns=columns,
            is_spatial=is_spatial
        )
    
    def export_models_to_file(self, output_path: Path) -> bool:
        """
        Export generated models to a Python file.
        
        Args:
            output_path: Path to output Python file
            
        Returns:
            bool: True if successful
        """
        try:
            imports = [
                "from sqlalchemy import Column, Integer, BigInteger, Numeric, Text, Boolean, DateTime, func",
                "from sqlalchemy.ext.declarative import declarative_base",
                "from sqlalchemy.dialects.postgresql import JSONB",
                "from geoalchemy2 import Geometry",
                "",
                "Base = declarative_base()",
                ""
            ]
            
            model_definitions = []
            
            for table_name, model_class in self.generated_models.items():
                class_definition = self._generate_model_class_code(model_class)
                model_definitions.append(class_definition)
            
            # Combine all parts
            content = "\n".join(imports + model_definitions)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.info(f"Exported {len(self.generated_models)} models to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export models to {output_path}: {e}")
            return False
    
    def _generate_model_class_code(self, model_class: Type) -> str:
        """
        Generate Python code for a model class.
        
        Args:
            model_class: SQLAlchemy model class
            
        Returns:
            Python code string
        """
        class_name = model_class.__name__
        table_name = model_class.__tablename__
        
        lines = [
            f"class {class_name}(Base):",
            f'    """Auto-generated model for {table_name} table."""',
            f'    __tablename__ = "{table_name}"',
            ""
        ]
        
        # Generate column definitions
        for column_name, column in model_class.__table__.columns.items():
            column_def = self._generate_column_code(column_name, column)
            lines.append(f"    {column_def}")
        
        lines.extend(["", ""])
        
        return "\n".join(lines)
    
    def _generate_column_code(self, column_name: str, column) -> str:
        """
        Generate Python code for a column definition.
        
        Args:
            column_name: Name of the column
            column: SQLAlchemy column object
            
        Returns:
            Column definition code
        """
        type_str = str(column.type)
        
        # Handle different column types
        if "INTEGER" in type_str:
            type_class = "Integer"
        elif "BIGINT" in type_str:
            type_class = "BigInteger"
        elif "NUMERIC" in type_str:
            type_class = "Numeric"
        elif "TEXT" in type_str:
            type_class = "Text"
        elif "BOOLEAN" in type_str:
            type_class = "Boolean"
        elif "TIMESTAMP" in type_str or "DATETIME" in type_str:
            type_class = "DateTime"
        elif "JSONB" in type_str:
            type_class = "JSONB"
        elif "GEOMETRY" in type_str:
            # Extract geometry details
            if "POINT" in type_str:
                type_class = "Geometry('POINT', srid=4326)"
            elif "POLYGON" in type_str:
                type_class = "Geometry('POLYGON', srid=4326)"
            else:
                type_class = "Geometry"
        else:
            type_class = "Text"
        
        # Build column arguments
        args = []
        
        if column.primary_key:
            args.append("primary_key=True")
        
        if not column.nullable and not column.primary_key:
            args.append("nullable=False")
        
        if column.unique and not column.primary_key:
            args.append("unique=True")
        
        if column.index and not column.primary_key:
            args.append("index=True")
        
        if column.default is not None:
            if column_name in ["created_at", "updated_at"]:
                args.append("default=func.now()")
                if column_name == "updated_at":
                    args.append("onupdate=func.now()")
        
        args_str = ", ".join(args)
        if args_str:
            return f'{column_name} = Column({type_class}, {args_str})'
        else:
            return f'{column_name} = Column({type_class})'


def generate_models_from_json(data_source: Path, output_path: Optional[Path] = None) -> Dict[str, Type]:
    """
    Functional approach to model generation.
    
    Args:
        data_source: Path to JSON/GeoJSON files
        output_path: Optional path to export models to file
        
    Returns:
        Dictionary of generated models
    """
    generator = ModelGenerator()
    
    if data_source.is_dir():
        models = generator.generate_models_from_directory(data_source)
    else:
        # Single file
        schema_gen = SchemaGenerator()
        
        with open(data_source, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        table_name = data_source.stem
        
        is_geojson = (
            data_source.suffix.lower() == '.geojson' or
            (isinstance(data, dict) and data.get('type') == 'FeatureCollection')
        )
        
        if is_geojson:
            field_types = schema_gen.analyze_geojson_structure(data)
            is_spatial = True
        else:
            field_types = schema_gen.analyze_json_structure(data)
            is_spatial = False
        
        schema = schema_gen.generate_table_schema(
            table_name=table_name,
            field_types=field_types,
            is_spatial=is_spatial
        )
        
        model_class = generator.generate_model_from_schema(schema)
        models = {table_name: model_class}
    
    if output_path:
        generator.export_models_to_file(output_path)
    
    return models