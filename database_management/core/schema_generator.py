"""
Schema generator module for automatic database schema creation.

Uses functional programming to analyze JSON/GeoJSON files and generate
optimal PostgreSQL schemas with proper types and indexes.
"""

import json
import logging
from typing import Dict, List, Any, Tuple, Optional, Union
from pathlib import Path
from dataclasses import dataclass, field
from functools import reduce, partial
from datetime import datetime
import re

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ColumnInfo:
    """Immutable column information."""
    name: str
    sql_type: str
    nullable: bool = True
    primary_key: bool = False
    index: bool = False
    unique: bool = False
    comment: Optional[str] = None
    
    def to_sql(self) -> str:
        """Generate SQL column definition."""
        parts = [f'"{self.name}"', self.sql_type]
        
        if self.primary_key:
            parts.append("PRIMARY KEY")
        
        if not self.nullable and not self.primary_key:
            parts.append("NOT NULL")
        
        if self.unique and not self.primary_key:
            parts.append("UNIQUE")
        
        if self.comment:
            parts.append(f"-- {self.comment}")
        
        return " ".join(parts)


@dataclass(frozen=True)
class TableSchema:
    """Immutable table schema information."""
    name: str
    columns: Tuple[ColumnInfo, ...]
    indexes: Tuple[str, ...] = field(default_factory=tuple)
    constraints: Tuple[str, ...] = field(default_factory=tuple)
    comment: Optional[str] = None
    is_spatial: bool = False
    
    def to_create_sql(self) -> str:
        """Generate CREATE TABLE SQL statement."""
        column_defs = [col.to_sql() for col in self.columns]
        
        sql = f'CREATE TABLE IF NOT EXISTS "{self.name}" (\n'
        sql += ",\n".join(f"    {col_def}" for col_def in column_defs)
        
        if self.constraints:
            sql += ",\n" + ",\n".join(f"    {constraint}" for constraint in self.constraints)
        
        sql += "\n);"
        
        if self.comment:
            sql += f"\nCOMMENT ON TABLE \"{self.name}\" IS '{self.comment}';"
        
        return sql
    
    def to_index_sql(self) -> List[str]:
        """Generate index creation SQL statements."""
        index_sqls = []
        
        for col in self.columns:
            if col.index and not col.primary_key:
                index_name = f"idx_{self.name}_{col.name}"
                index_sql = f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{self.name}" ("{col.name}");'
                index_sqls.append(index_sql)
        
        # Add custom indexes
        for index in self.indexes:
            index_sqls.append(index)
        
        return index_sqls


class SchemaGenerator:
    """Functional schema generator for JSON/GeoJSON data."""
    
    def __init__(self):
        self.type_mapping = self._create_type_mapping()
    
    @staticmethod
    def _create_type_mapping() -> Dict[str, str]:
        """Create mapping from Python types to PostgreSQL types."""
        return {
            "str": "TEXT",
            "int": "BIGINT",
            "float": "NUMERIC",
            "bool": "BOOLEAN",
            "datetime": "TIMESTAMP",
            "date": "DATE",
            "list": "JSONB",
            "dict": "JSONB",
            "geometry": "GEOMETRY",
            "point": "GEOMETRY(POINT, 4326)",
            "polygon": "GEOMETRY(POLYGON, 4326)",
            "linestring": "GEOMETRY(LINESTRING, 4326)",
            "multipoint": "GEOMETRY(MULTIPOINT, 4326)",
            "multipolygon": "GEOMETRY(MULTIPOLYGON, 4326)",
            "multilinestring": "GEOMETRY(MULTILINESTRING, 4326)"
        }
    
    def analyze_value(self, value: Any) -> str:
        """
        Analyze a value and determine its PostgreSQL type.
        
        Args:
            value: Value to analyze
            
        Returns:
            str: PostgreSQL type name
        """
        if value is None:
            return "TEXT"  # Default for null values
        
        value_type = type(value).__name__
        
        # Handle special cases
        if value_type == "str":
            # Check if it's a date/datetime string
            if self._is_date_string(value):
                return "TIMESTAMP"
            elif self._is_numeric_string(value):
                return "NUMERIC"
            else:
                return "TEXT"
        
        elif value_type in ["list", "dict"]:
            return "JSONB"
        
        elif value_type == "int":
            # Choose appropriate integer type based on size
            if abs(value) < 2**31:
                return "INTEGER"
            else:
                return "BIGINT"
        
        elif value_type == "float":
            return "NUMERIC"
        
        elif value_type == "bool":
            return "BOOLEAN"
        
        return self.type_mapping.get(value_type, "TEXT")
    
    def _is_date_string(self, value: str) -> bool:
        """Check if string represents a date/datetime."""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO datetime
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # SQL datetime
        ]
        
        return any(re.match(pattern, value) for pattern in date_patterns)
    
    def _is_numeric_string(self, value: str) -> bool:
        """Check if string represents a number."""
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def analyze_json_structure(self, data: Union[List[Dict], Dict]) -> Dict[str, str]:
        """
        Analyze JSON structure and determine field types.
        
        Args:
            data: JSON data (list of dicts or single dict)
            
        Returns:
            Dict[str, str]: Field name to PostgreSQL type mapping
        """
        if isinstance(data, dict):
            data = [data]
        
        if not data:
            return {}
        
        # Collect all fields and their types
        field_types = {}
        
        for record in data:
            if not isinstance(record, dict):
                continue
            
            for field, value in record.items():
                field_type = self.analyze_value(value)
                
                if field in field_types:
                    # Resolve type conflicts (prefer more general types)
                    field_types[field] = self._resolve_type_conflict(
                        field_types[field], field_type
                    )
                else:
                    field_types[field] = field_type
        
        return field_types
    
    def _resolve_type_conflict(self, type1: str, type2: str) -> str:
        """
        Resolve conflicts between two types by choosing the more general one.
        
        Args:
            type1: First type
            type2: Second type
            
        Returns:
            str: Resolved type
        """
        # Type hierarchy (more general types first)
        type_hierarchy = [
            "TEXT",
            "JSONB", 
            "NUMERIC",
            "BIGINT",
            "INTEGER",
            "BOOLEAN",
            "TIMESTAMP",
            "DATE"
        ]
        
        if type1 == type2:
            return type1
        
        # Return the more general type
        for general_type in type_hierarchy:
            if type1 == general_type or type2 == general_type:
                return general_type
        
        return "TEXT"  # Default fallback
    
    def analyze_geojson_structure(self, geojson_data: Dict) -> Dict[str, str]:
        """
        Analyze GeoJSON structure for spatial tables.
        
        Args:
            geojson_data: GeoJSON data
            
        Returns:
            Dict[str, str]: Field name to PostgreSQL type mapping
        """
        field_types = {"geometry": "GEOMETRY"}
        
        if "features" in geojson_data:
            features = geojson_data["features"]
            
            # Analyze properties of all features
            properties_data = []
            for feature in features:
                if "properties" in feature and feature["properties"]:
                    properties_data.append(feature["properties"])
            
            if properties_data:
                properties_types = self.analyze_json_structure(properties_data)
                field_types.update(properties_types)
            
            # Determine specific geometry type
            if features:
                first_geometry = features[0].get("geometry", {})
                geom_type = first_geometry.get("type", "").lower()
                
                if geom_type in self.type_mapping:
                    field_types["geometry"] = self.type_mapping[geom_type]
        
        return field_types
    
    def generate_table_schema(
        self, 
        table_name: str, 
        field_types: Dict[str, str],
        is_spatial: bool = False,
        primary_key: Optional[str] = None
    ) -> TableSchema:
        """
        Generate table schema from field types.
        
        Args:
            table_name: Name of the table
            field_types: Field name to type mapping
            is_spatial: Whether table contains spatial data
            primary_key: Primary key field name
            
        Returns:
            TableSchema: Complete table schema
        """
        columns = []
        
        # Determine primary key
        if not primary_key:
            if "id" in field_types:
                primary_key = "id"  # Use existing id field as primary key
            else:
                # Add auto-generated ID column
                columns.append(ColumnInfo(
                    name="id",
                    sql_type="SERIAL",
                    nullable=False,
                    primary_key=True,
                    comment="Auto-generated primary key"
                ))
        
        # Create columns from field types
        for field_name, sql_type in field_types.items():
            is_pk = field_name == primary_key
            is_indexed = field_name in ["bpin", "codigo", "id", "proceso_compra", "documento_proveedor"]
            
            column = ColumnInfo(
                name=self._clean_column_name(field_name),
                sql_type=sql_type,
                nullable=not is_pk,
                primary_key=is_pk,
                index=is_indexed and not is_pk,
                comment=f"Generated from JSON field: {field_name}"
            )
            columns.append(column)
        
        # Add audit columns
        audit_columns = [
            ColumnInfo(
                name="created_at",
                sql_type="TIMESTAMP",
                nullable=False,
                comment="Record creation timestamp"
            ),
            ColumnInfo(
                name="updated_at", 
                sql_type="TIMESTAMP",
                nullable=False,
                comment="Last update timestamp"
            ),
            ColumnInfo(
                name="data_source",
                sql_type="TEXT",
                nullable=True,
                comment="Source file or system"
            )
        ]
        columns.extend(audit_columns)
        
        # Create indexes for spatial tables
        indexes = []
        if is_spatial:
            indexes.append(
                f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_geometry" '
                f'ON "{table_name}" USING GIST (geometry);'
            )
        
        return TableSchema(
            name=table_name,
            columns=tuple(columns),
            indexes=tuple(indexes),
            comment=f"Auto-generated table for {table_name} data",
            is_spatial=is_spatial
        )
    
    def _clean_column_name(self, name: str) -> str:
        """
        Clean column name for PostgreSQL compatibility.
        
        Args:
            name: Original column name
            
        Returns:
            str: Cleaned column name
        """
        # Replace invalid characters
        cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        
        # Ensure it starts with letter or underscore
        if cleaned and cleaned[0].isdigit():
            cleaned = f"col_{cleaned}"
        
        # Convert to lowercase
        cleaned = cleaned.lower()
        
        # Handle PostgreSQL reserved words
        reserved_words = {
            "user", "table", "index", "select", "insert", "update", "delete",
            "from", "where", "group", "order", "having", "join", "union"
        }
        
        if cleaned in reserved_words:
            cleaned = f"{cleaned}_col"
        
        return cleaned


def generate_schema_from_file(file_path: Path) -> Optional[TableSchema]:
    """
    Generate schema from JSON/GeoJSON file.
    
    Args:
        file_path: Path to JSON/GeoJSON file
        
    Returns:
        TableSchema: Generated schema or None if failed
    """
    try:
        generator = SchemaGenerator()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Determine table name from filename
        table_name = file_path.stem
        
        # Check if it's GeoJSON
        is_geojson = (
            file_path.suffix.lower() == '.geojson' or
            (isinstance(data, dict) and data.get('type') == 'FeatureCollection')
        )
        
        if is_geojson:
            field_types = generator.analyze_geojson_structure(data)
            is_spatial = True
        else:
            field_types = generator.analyze_json_structure(data)
            is_spatial = False
        
        schema = generator.generate_table_schema(
            table_name=table_name,
            field_types=field_types,
            is_spatial=is_spatial
        )
        
        logger.info(f"Generated schema for {table_name}: {len(schema.columns)} columns")
        return schema
        
    except Exception as e:
        logger.error(f"Failed to generate schema from {file_path}: {e}")
        return None


def generate_schema_from_data(data: Any, table_name: str) -> Optional[TableSchema]:
    """
    Generate schema from data object.
    
    Args:
        data: JSON or GeoJSON data
        table_name: Name for the table
        
    Returns:
        TableSchema: Generated schema or None if failed
    """
    try:
        generator = SchemaGenerator()
        
        # Check if it's GeoJSON
        is_geojson = isinstance(data, dict) and data.get('type') == 'FeatureCollection'
        
        if is_geojson:
            field_types = generator.analyze_geojson_structure(data)
            is_spatial = True
        else:
            field_types = generator.analyze_json_structure(data)
            is_spatial = False
        
        schema = generator.generate_table_schema(
            table_name=table_name,
            field_types=field_types,
            is_spatial=is_spatial
        )
        
        logger.info(f"Generated schema for {table_name}: {len(schema.columns)} columns")
        return schema
        
    except Exception as e:
        logger.error(f"Failed to generate schema for {table_name}: {e}")
        return None