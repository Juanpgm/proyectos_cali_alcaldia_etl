# -*- coding: utf-8 -*-
"""
Temporary file management system using functional programming patterns.
Handles creation, processing, and cleanup of temporary files for ETL pipeline.
"""

import os
import json
import tempfile
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, TypeVar, Union
from functools import wraps, partial
from contextlib import contextmanager
import pandas as pd

T = TypeVar('T')


class TempFileManager:
    """Functional temporary file manager for ETL operations."""
    
    def __init__(self, base_dir: Optional[str] = None):
        """Initialize with optional base directory."""
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self.temp_dirs: List[Path] = []
        self.temp_files: List[Path] = []
    
    def __enter__(self):
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager with cleanup."""
        self.cleanup_all()
    
    @contextmanager
    def temp_directory(self, prefix: str = "etl_temp_"):
        """Context manager for temporary directories."""
        temp_dir = Path(tempfile.mkdtemp(prefix=prefix))
        self.temp_dirs.append(temp_dir)
        try:
            yield temp_dir
        finally:
            self._cleanup_directory(temp_dir)
    
    def create_temp_file(self, filename: str, content: str) -> Path:
        """Create a temporary file with given content."""
        temp_dir = Path(tempfile.mkdtemp(prefix="etl_temp_"))
        self.temp_dirs.append(temp_dir)
        temp_file = temp_dir / filename
        temp_file.write_text(content, encoding='utf-8')
        self.temp_files.append(temp_file)
        return temp_file
    
    def create_temp_directory(self, name: str = None) -> Path:
        """Create a temporary directory."""
        if name:
            temp_dir = Path(tempfile.mkdtemp(prefix=f"etl_{name}_"))
        else:
            temp_dir = Path(tempfile.mkdtemp(prefix="etl_temp_"))
        self.temp_dirs.append(temp_dir)
        return temp_dir
    
    def _cleanup_directory(self, directory: Path) -> None:
        """Safely remove temporary directory."""
        try:
            if directory.exists():
                shutil.rmtree(directory)
                print(f"🧹 Cleaned up temporary directory: {directory}")
        except Exception as e:
            print(f"⚠️ Warning: Could not clean up {directory}: {e}")
    
    def cleanup_all(self) -> None:
        """Clean up all registered temporary directories."""
        for temp_dir in self.temp_dirs:
            self._cleanup_directory(temp_dir)
        self.temp_dirs.clear()
        self.temp_files.clear()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup_all()
    
    def create_temp_file(self, filename: str, content: str) -> Path:
        """Create a temporary file with given content."""
        with self.temp_directory() as temp_dir:
            file_path = temp_dir / filename
            file_path.write_text(content, encoding='utf-8')
            return file_path
    
    def create_temp_directory(self, dir_name: str) -> Path:
        """Create a temporary directory."""
        temp_dir = Path(tempfile.mkdtemp(prefix=f"{dir_name}_"))
        self.temp_dirs.append(temp_dir)
        return temp_dir


def with_temp_file(file_extension: str = ".json"):
    """Decorator for functions that need temporary files."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as temp_file:
                temp_path = Path(temp_file.name)
                try:
                    result = func(temp_path, *args, **kwargs)
                    return result
                finally:
                    if temp_path.exists():
                        temp_path.unlink()
                        print(f"🧹 Cleaned up temporary file: {temp_path.name}")
        return wrapper
    return decorator


def with_temp_directory(prefix: str = "etl_"):
    """Decorator for functions that need temporary directories."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            temp_dir = Path(tempfile.mkdtemp(prefix=prefix))
            try:
                result = func(temp_dir, *args, **kwargs)
                return result
            finally:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
                    print(f"🧹 Cleaned up temporary directory: {temp_dir}")
        return wrapper
    return decorator


# Functional file operations
def save_json_data(data: Union[Dict, List], file_path: Path) -> bool:
    """Save data to JSON file using functional approach."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"❌ Error saving JSON to {file_path}: {e}")
        return False


def load_json_data(file_path: Path) -> Optional[Union[Dict, List]]:
    """Load data from JSON file using functional approach."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error loading JSON from {file_path}: {e}")
        return None


def save_dataframe_json(df: pd.DataFrame, file_path: Path) -> bool:
    """Save DataFrame to JSON file."""
    try:
        data = df.to_dict('records')
        return save_json_data(data, file_path)
    except Exception as e:
        print(f"❌ Error saving DataFrame to {file_path}: {e}")
        return False


def load_dataframe_json(file_path: Path) -> Optional[pd.DataFrame]:
    """Load DataFrame from JSON file."""
    try:
        data = load_json_data(file_path)
        if data:
            return pd.DataFrame(data)
        return None
    except Exception as e:
        print(f"❌ Error loading DataFrame from {file_path}: {e}")
        return None


# Pipeline processing functions
def process_with_temp_data(
    data: Any,
    processor_func: Callable[[Path, Any], T],
    file_name: str = "temp_data.json"
) -> Optional[T]:
    """Process data using temporary file storage."""
    with tempfile.TemporaryDirectory(prefix="etl_") as temp_dir:
        temp_path = Path(temp_dir) / file_name
        
        # Save data temporarily
        if isinstance(data, pd.DataFrame):
            success = save_dataframe_json(data, temp_path)
        elif isinstance(data, (dict, list)):
            success = save_json_data(data, temp_path)
        else:
            print(f"❌ Unsupported data type: {type(data)}")
            return None
        
        if not success:
            return None
        
        # Process data
        try:
            result = processor_func(temp_path, data)
            print(f"✅ Processed data using temporary file: {temp_path.name}")
            return result
        except Exception as e:
            print(f"❌ Error processing data: {e}")
            return None


def create_temp_input_structure(base_data: Dict[str, Any]) -> Path:
    """Create temporary input directory structure for ETL pipeline."""
    temp_dir = Path(tempfile.mkdtemp(prefix="etl_inputs_"))
    
    # Create subdirectories as needed
    for key, data in base_data.items():
        subdir = temp_dir / f"{key}_input"
        subdir.mkdir(parents=True, exist_ok=True)
        
        # Save data to appropriate file
        if isinstance(data, pd.DataFrame):
            file_path = subdir / f"{key}.json"
            save_dataframe_json(data, file_path)
        elif isinstance(data, (dict, list)):
            file_path = subdir / f"{key}.json"
            save_json_data(data, file_path)
    
    return temp_dir


# In-memory processing functions (no temporary files needed)
def process_in_memory(
    data: Union[pd.DataFrame, Dict, List],
    transformer_func: Callable[[Union[pd.DataFrame, Dict, List]], T]
) -> Optional[T]:
    """Process data entirely in memory without temporary files."""
    try:
        result = transformer_func(data)
        print("✅ Processed data in memory (no temporary files)")
        return result
    except Exception as e:
        print(f"❌ Error processing data in memory: {e}")
        import traceback
        traceback.print_exc()
        return None


# Clean functional pipeline builder
class ETLPipelineBuilder:
    """Builder for creating clean ETL pipelines with temporary file management."""
    
    def __init__(self):
        self.temp_manager = TempFileManager()
        self.steps: List[Callable] = []
    
    def add_step(self, step_func: Callable) -> 'ETLPipelineBuilder':
        """Add a processing step to the pipeline."""
        self.steps.append(step_func)
        return self
    
    def execute(self, initial_data: Any) -> Optional[Any]:
        """Execute the pipeline with automatic cleanup."""
        try:
            result = initial_data
            for step in self.steps:
                result = step(result)
                if result is None:
                    print("❌ Pipeline failed at step")
                    return None
            return result
        finally:
            self.temp_manager.cleanup_all()


# Functional programming utilities
def compose(*functions):
    """Compose functions from right to left."""
    def composed(arg):
        result = arg
        for func in reversed(functions):
            result = func(result)
        return result
    return composed


def pipe(value, *functions):
    """Pipe a value through a series of functions."""
    result = value
    for func in functions:
        result = func(result)
    return result


# In-memory ETL Pipeline
class GenericInMemoryETLPipeline:
    """ETL pipeline that processes data entirely in memory."""
    
    def __init__(self, extraction_func, transformation_func, load_func):
        self.extraction_func = extraction_func
        self.transformation_func = transformation_func
        self.load_func = load_func
        self.temp_manager = TempFileManager()
    
    def execute_pipeline(self) -> bool:
        """Execute the complete ETL pipeline in memory."""
        try:
            # Extract data
            print("📥 Extracting data...")
            raw_data = self.extraction_func()
            if raw_data is None:
                print("❌ Extraction failed")
                return False
            
            # Transform data
            print("🔄 Transforming data...")
            transformed_data = self.transformation_func(raw_data)
            if transformed_data is None:
                print("❌ Transformation failed")
                return False
            
            # Load data
            print("📤 Loading data...")
            load_success = self.load_func(transformed_data)
            if not load_success:
                print("❌ Load failed")
                return False
            
            print("✅ Pipeline completed successfully")
            return True
            
        except Exception as e:
            print(f"❌ Pipeline error: {e}")
            return False
        finally:
            self.temp_manager.cleanup_all()


# Export main functions for use in ETL modules
__all__ = [
    'TempFileManager',
    'GenericInMemoryETLPipeline',
    'compose',
    'pipe',
    'with_temp_file',
    'with_temp_directory', 
    'process_with_temp_data',
    'process_in_memory',
    'create_temp_input_structure',
    'ETLPipelineBuilder',
    'save_json_data',
    'load_json_data',
    'save_dataframe_json',
    'load_dataframe_json'
]