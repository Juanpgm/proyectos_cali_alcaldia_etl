"""
Data loader module with functional programming approach.

Provides efficient data loading without API dependencies, with automatic
schema detection and intelligent data transformation.
"""

import json
import logging
from typing import List, Dict, Any, Optional, Union, Iterator, Callable, Tuple
from pathlib import Path
from dataclasses import dataclass
from functools import partial, reduce
from datetime import datetime
import re
from itertools import islice
from sqlalchemy import text

from .database_manager import DatabaseManager
# from .schema_generator import TableSchema, generate_schema_from_data  # Commented out as module doesn't exist

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadResult:
    """Immutable load result information."""
    table_name: str
    records_loaded: int
    records_failed: int
    execution_time: float
    errors: Tuple[str, ...] = ()
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        total = self.records_loaded + self.records_failed
        return (self.records_loaded / total * 100) if total > 0 else 0.0
    
    @property
    def is_successful(self) -> bool:
        """Check if load was successful."""
        return self.records_failed == 0 and self.records_loaded > 0


@dataclass(frozen=True)
class BatchLoadResult:
    """Result of batch loading operation."""
    results: Tuple[LoadResult, ...]
    total_records: int
    total_time: float
    
    @property
    def total_loaded(self) -> int:
        """Total records loaded across all tables."""
        return sum(result.records_loaded for result in self.results)
    
    @property
    def total_failed(self) -> int:
        """Total records failed across all tables."""
        return sum(result.records_failed for result in self.results)
    
    @property
    def overall_success_rate(self) -> float:
        """Overall success rate percentage."""
        total = self.total_loaded + self.total_failed
        return (self.total_loaded / total * 100) if total > 0 else 0.0


class DataLoader:
    """Functional data loader with batch processing and error handling."""
    
    def __init__(self, db_manager: DatabaseManager, batch_size: int = 1000):
        self.db_manager = db_manager
        self.batch_size = batch_size
        self.transformers = self._create_default_transformers()
    
    @staticmethod
    def _create_default_transformers() -> Dict[str, Callable]:
        """Create default data transformers."""
        return {
            "clean_numeric": lambda x: float(x) if isinstance(x, str) and x.replace('.', '').replace('-', '').isdigit() else x,
            "clean_date": lambda x: x if isinstance(x, str) and re.match(r'\d{4}-\d{2}-\d{2}', x) else None,
            "clean_text": lambda x: x.strip() if isinstance(x, str) else x,
            "normalize_boolean": lambda x: x if isinstance(x, bool) else (x.lower() in ['true', '1', 'yes'] if isinstance(x, str) else bool(x))
        }
    
    def add_transformer(self, name: str, transformer: Callable) -> None:
        """Add custom data transformer."""
        self.transformers[name] = transformer
    
    def transform_record(self, record: Dict[str, Any], schema: TableSchema) -> Dict[str, Any]:
        """
        Transform a single record according to schema.
        
        Args:
            record: Original record
            schema: Table schema
            
        Returns:
            Transformed record
        """
        transformed = {}
        
        for column in schema.columns:
            field_name = column.name
            
            # Skip auto-generated fields
            if field_name in ['id', 'created_at', 'updated_at', 'data_source']:
                continue
            
            # Get value from record
            value = record.get(field_name)
            
            if value is not None:
                # Apply transformations based on column type
                if column.sql_type in ['INTEGER', 'BIGINT', 'NUMERIC']:
                    value = self.transformers["clean_numeric"](value)
                elif column.sql_type in ['TIMESTAMP', 'DATE']:
                    value = self.transformers["clean_date"](value)
                elif column.sql_type == 'TEXT':
                    value = self.transformers["clean_text"](value)
                elif column.sql_type == 'BOOLEAN':
                    value = self.transformers["normalize_boolean"](value)
            
            transformed[field_name] = value
        
        # Add audit fields
        transformed['created_at'] = datetime.utcnow()
        transformed['updated_at'] = datetime.utcnow()
        
        return transformed
    
    def prepare_data_for_loading(
        self, 
        data: Union[List[Dict], Dict], 
        schema: TableSchema,
        source_info: str = "unknown"
    ) -> Iterator[Dict[str, Any]]:
        """
        Prepare data for database loading.
        
        Args:
            data: Raw data (list of dicts or single dict)
            schema: Table schema
            source_info: Information about data source
            
        Yields:
            Transformed records ready for loading
        """
        if isinstance(data, dict):
            # Handle GeoJSON
            if data.get('type') == 'FeatureCollection':
                features = data.get('features', [])
                for feature in features:
                    properties = feature.get('properties', {})
                    geometry = feature.get('geometry')
                    
                    # Combine properties with geometry
                    record = properties.copy()
                    if geometry:
                        record['geometry'] = json.dumps(geometry)
                    
                    record['data_source'] = source_info
                    yield self.transform_record(record, schema)
            else:
                # Single record
                data['data_source'] = source_info
                yield self.transform_record(data, schema)
        else:
            # List of records
            for record in data:
                if isinstance(record, dict):
                    record['data_source'] = source_info
                    yield self.transform_record(record, schema)
    
    def load_batch(
        self, 
        records: List[Dict[str, Any]], 
        table_name: str
    ) -> Tuple[int, int, List[str]]:
        """
        Load a batch of records into the database.
        
        Args:
            records: List of records to load
            table_name: Target table name
            
        Returns:
            Tuple of (loaded_count, failed_count, errors)
        """
        if not records:
            return 0, 0, []
        
        loaded_count = 0
        failed_count = 0
        errors = []
        
        try:
            with self.db_manager.get_session() as session:
                for record in records:
                    try:
                        # Build INSERT statement
                        columns = list(record.keys())
                        placeholders = [f":{col}" for col in columns]
                        
                        sql = f"""
                        INSERT INTO "{table_name}" ({', '.join(f'"{col}"' for col in columns)})
                        VALUES ({', '.join(placeholders)})
                        """
                        
                        session.execute(text(sql), record)
                        loaded_count += 1
                        
                    except Exception as e:
                        failed_count += 1
                        error_msg = f"Record failed: {str(e)[:100]}"
                        errors.append(error_msg)
                        logger.warning(error_msg)
                        
                        # Continue with next record
                        continue
                
        except Exception as e:
            error_msg = f"Batch load failed: {e}"
            errors.append(error_msg)
            logger.error(error_msg)
            failed_count = len(records) - loaded_count
        
        return loaded_count, failed_count, errors
    
    def load_data_to_table(
        self, 
        data: Union[List[Dict], Dict], 
        table_name: str,
        schema: Optional[TableSchema] = None,
        source_info: str = "unknown"
    ) -> LoadResult:
        """
        Load data to a specific table.
        
        Args:
            data: Data to load
            table_name: Target table name
            schema: Table schema (will be generated if not provided)
            source_info: Information about data source
            
        Returns:
            LoadResult with loading statistics
        """
        start_time = datetime.utcnow()
        
        try:
            # Generate schema if not provided
            if schema is None:
                schema = generate_schema_from_data(data, table_name)
                if schema is None:
                    raise ValueError(f"Could not generate schema for {table_name}")
            
            # Create table if it doesn't exist
            if not self.db_manager.table_exists(table_name):
                if not self.db_manager.create_table_from_schema(schema):
                    raise ValueError(f"Could not create table {table_name}")
            
            # Prepare data for loading
            prepared_records = list(self.prepare_data_for_loading(data, schema, source_info))
            
            if not prepared_records:
                return LoadResult(
                    table_name=table_name,
                    records_loaded=0,
                    records_failed=0,
                    execution_time=0.0,
                    errors=("No records to load",)
                )
            
            # Load in batches
            total_loaded = 0
            total_failed = 0
            all_errors = []
            
            def chunked(iterable, size):
                """Split iterable into chunks of specified size."""
                iterator = iter(iterable)
                while chunk := list(islice(iterator, size)):
                    yield chunk
            
            for batch in chunked(prepared_records, self.batch_size):
                loaded, failed, errors = self.load_batch(batch, table_name)
                total_loaded += loaded
                total_failed += failed
                all_errors.extend(errors)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info(
                f"Loaded {total_loaded} records to {table_name} "
                f"({total_failed} failed) in {execution_time:.2f}s"
            )
            
            return LoadResult(
                table_name=table_name,
                records_loaded=total_loaded,
                records_failed=total_failed,
                execution_time=execution_time,
                errors=tuple(all_errors)
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            error_msg = f"Failed to load data to {table_name}: {e}"
            logger.error(error_msg)
            
            return LoadResult(
                table_name=table_name,
                records_loaded=0,
                records_failed=0,
                execution_time=execution_time,
                errors=(error_msg,)
            )
    
    def load_from_file(self, file_path: Path) -> LoadResult:
        """
        Load data from JSON/GeoJSON file.
        
        Args:
            file_path: Path to data file
            
        Returns:
            LoadResult with loading statistics
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            table_name = file_path.stem
            source_info = f"File: {file_path.name}"
            
            return self.load_data_to_table(data, table_name, source_info=source_info)
            
        except Exception as e:
            logger.error(f"Failed to load from file {file_path}: {e}")
            return LoadResult(
                table_name=file_path.stem,
                records_loaded=0,
                records_failed=0,
                execution_time=0.0,
                errors=(f"File load failed: {e}",)
            )
    
    def load_from_directory(self, directory_path: Path) -> BatchLoadResult:
        """
        Load all JSON/GeoJSON files from a directory.
        
        Args:
            directory_path: Path to directory containing data files
            
        Returns:
            BatchLoadResult with overall statistics
        """
        start_time = datetime.utcnow()
        results = []
        
        try:
            # Find all JSON and GeoJSON files
            json_files = list(directory_path.glob("*.json"))
            geojson_files = list(directory_path.glob("*.geojson"))
            all_files = json_files + geojson_files
            
            if not all_files:
                logger.warning(f"No JSON/GeoJSON files found in {directory_path}")
                return BatchLoadResult(
                    results=(),
                    total_records=0,
                    total_time=0.0
                )
            
            logger.info(f"Loading {len(all_files)} files from {directory_path}")
            
            for file_path in all_files:
                result = self.load_from_file(file_path)
                results.append(result)
            
            total_time = (datetime.utcnow() - start_time).total_seconds()
            total_records = sum(result.records_loaded + result.records_failed for result in results)
            
            logger.info(
                f"Batch load completed: {len(results)} files, "
                f"{sum(r.records_loaded for r in results)} records loaded "
                f"in {total_time:.2f}s"
            )
            
            return BatchLoadResult(
                results=tuple(results),
                total_records=total_records,
                total_time=total_time
            )
            
        except Exception as e:
            total_time = (datetime.utcnow() - start_time).total_seconds()
            logger.error(f"Batch load failed: {e}")
            
            return BatchLoadResult(
                results=tuple(results),
                total_records=0,
                total_time=total_time
            )


def load_data_functional(
    db_manager: DatabaseManager,
    data_source: Union[Path, Dict, List[Dict]],
    table_name: Optional[str] = None,
    batch_size: int = 1000
) -> Union[LoadResult, BatchLoadResult]:
    """
    Functional approach to data loading.
    
    Args:
        db_manager: Database manager instance
        data_source: Data source (file path, directory, or data object)
        table_name: Table name (required for data objects)
        batch_size: Batch size for loading
        
    Returns:
        LoadResult or BatchLoadResult depending on input type
    """
    loader = DataLoader(db_manager, batch_size)
    
    if isinstance(data_source, Path):
        if data_source.is_dir():
            return loader.load_from_directory(data_source)
        else:
            return loader.load_from_file(data_source)
    elif isinstance(data_source, (dict, list)):
        if table_name is None:
            raise ValueError("table_name is required when loading from data objects")
        return loader.load_data_to_table(data_source, table_name)
    else:
        raise ValueError(f"Unsupported data source type: {type(data_source)}")


def create_data_loader(db_manager: DatabaseManager, **kwargs) -> DataLoader:
    """
    Factory function to create data loader.
    
    Args:
        db_manager: Database manager instance
        **kwargs: Additional configuration options
        
    Returns:
        DataLoader: Configured data loader
    """
    return DataLoader(db_manager, **kwargs)