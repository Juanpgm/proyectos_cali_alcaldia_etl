# -*- coding: utf-8 -*-
"""
Bulk data loader module using functional programming principles.
Loads data from various sources directly into the database using SQLAlchemy models.
"""

import json
import os
import sys
from typing import List, Dict, Any, Optional, Callable, Iterator, Tuple
from functools import partial, reduce
from itertools import islice
from pathlib import Path
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add database_management to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'database_management', 'core'))

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker, Session
    from sqlalchemy.exc import IntegrityError, SQLAlchemyError
    
    # Import models and config directly to avoid __init__.py issues
    import models
    import config
    
    Base = models.Base
    UnidadProyecto = models.UnidadProyecto
    get_database_config = config.get_database_config
    test_connection = config.test_connection
    
    print("✅ Database modules imported successfully")
except ImportError as e:
    print(f"❌ Error importing database modules: {e}")
    print("Make sure SQLAlchemy and other dependencies are installed")
    sys.exit(1)


# Functional programming utilities
def compose(*functions):
    """Compose multiple functions into a single function."""
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(data, *functions):
    """Pipe data through multiple functions."""
    return reduce(lambda x, f: f(x), functions, data)


def batch_iterator(iterable: Iterator, batch_size: int) -> Iterator[List]:
    """Create batches from an iterable."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            break
        yield batch


def safe_execute(func: Callable, *args, **kwargs) -> Tuple[bool, Any, Optional[str]]:
    """
    Safely execute a function and return success status, result, and error message.
    
    Returns:
        Tuple of (success: bool, result: Any, error_message: Optional[str])
    """
    try:
        result = func(*args, **kwargs)
        return True, result, None
    except Exception as e:
        return False, None, str(e)


# Data loading functions
def load_geojson_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Load GeoJSON file and extract features.
    
    Args:
        file_path: Path to the GeoJSON file
        
    Returns:
        List of GeoJSON features
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"GeoJSON file not found: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    
    features = geojson_data.get('features', [])
    logger.info(f"📁 Loaded {len(features)} features from {file_path}")
    
    return features


def validate_feature(feature: Dict[str, Any]) -> bool:
    """
    Validate a GeoJSON feature for required fields.
    
    Args:
        feature: GeoJSON feature dictionary
        
    Returns:
        True if feature is valid
    """
    if not isinstance(feature, dict):
        return False
    
    properties = feature.get('properties', {})
    
    # Check for required key field
    if not properties.get('key'):
        logger.warning(f"Feature missing required 'key' field: {properties}")
        return False
    
    return True


def create_model_from_feature(feature: Dict[str, Any]) -> Optional[UnidadProyecto]:
    """
    Create UnidadProyecto model instance from GeoJSON feature.
    
    Args:
        feature: GeoJSON feature dictionary
        
    Returns:
        UnidadProyecto instance or None if creation fails
    """
    try:
        return UnidadProyecto.from_geojson_feature(feature)
    except Exception as e:
        logger.error(f"Error creating model from feature: {e}")
        logger.error(f"Feature data: {feature.get('properties', {}).get('key', 'unknown')}")
        return None


def setup_database(config=None) -> Tuple[bool, Optional[Session], Optional[str]]:
    """
    Setup database connection and create tables if needed.
    
    Args:
        config: Database configuration (optional)
        
    Returns:
        Tuple of (success: bool, session: Optional[Session], error_message: Optional[str])
    """
    try:
        if config is None:
            config = get_database_config()
        
        logger.info("🔧 Setting up database connection...")
        
        # Test connection first
        if not test_connection(config):
            return False, None, "Database connection test failed"
        
        # Create engine
        engine = create_engine(
            config.connection_string,
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            pool_timeout=config.timeout,
            echo=False  # Set to True for SQL debugging
        )
        
        # Create tables
        logger.info("📋 Creating database tables...")
        Base.metadata.create_all(engine)
        
        # Create session
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = SessionLocal()
        
        logger.info("✅ Database setup completed successfully")
        return True, session, None
        
    except Exception as e:
        error_msg = f"Database setup failed: {e}"
        logger.error(error_msg)
        return False, None, error_msg


def bulk_insert_models(session: Session, models: List[UnidadProyecto], batch_size: int = 100) -> Tuple[int, int, List[str]]:
    """
    Bulk insert model instances into database.
    
    Args:
        session: Database session
        models: List of UnidadProyecto instances
        batch_size: Number of records to insert per batch
        
    Returns:
        Tuple of (successful_inserts: int, failed_inserts: int, error_messages: List[str])
    """
    successful_inserts = 0
    failed_inserts = 0
    error_messages = []
    
    logger.info(f"📤 Starting bulk insert of {len(models)} records in batches of {batch_size}")
    
    for i, batch in enumerate(batch_iterator(models, batch_size)):
        try:
            session.add_all(batch)
            session.commit()
            
            batch_count = len(batch)
            successful_inserts += batch_count
            
            logger.info(f"  ✅ Batch {i+1}: Inserted {batch_count} records")
            
        except IntegrityError as e:
            session.rollback()
            error_msg = f"Batch {i+1} integrity error: {e}"
            logger.error(error_msg)
            error_messages.append(error_msg)
            
            # Try inserting one by one to identify problematic records
            individual_success, individual_failed, individual_errors = insert_batch_individually(session, batch)
            successful_inserts += individual_success
            failed_inserts += individual_failed
            error_messages.extend(individual_errors)
            
        except SQLAlchemyError as e:
            session.rollback()
            error_msg = f"Batch {i+1} database error: {e}"
            logger.error(error_msg)
            error_messages.append(error_msg)
            failed_inserts += len(batch)
            
        except Exception as e:
            session.rollback()
            error_msg = f"Batch {i+1} unexpected error: {e}"
            logger.error(error_msg)
            error_messages.append(error_msg)
            failed_inserts += len(batch)
    
    return successful_inserts, failed_inserts, error_messages


def insert_batch_individually(session: Session, models: List[UnidadProyecto]) -> Tuple[int, int, List[str]]:
    """
    Insert models one by one when batch insert fails.
    
    Args:
        session: Database session
        models: List of UnidadProyecto instances
        
    Returns:
        Tuple of (successful_inserts: int, failed_inserts: int, error_messages: List[str])
    """
    successful_inserts = 0
    failed_inserts = 0
    error_messages = []
    
    logger.info(f"🔍 Attempting individual insert for {len(models)} records")
    
    for model in models:
        try:
            session.add(model)
            session.commit()
            successful_inserts += 1
            
        except IntegrityError as e:
            session.rollback()
            error_msg = f"Duplicate key for record {model.key}: {e}"
            logger.warning(error_msg)
            error_messages.append(error_msg)
            failed_inserts += 1
            
        except SQLAlchemyError as e:
            session.rollback()
            error_msg = f"Database error for record {model.key}: {e}"
            logger.error(error_msg)
            error_messages.append(error_msg)
            failed_inserts += 1
            
        except Exception as e:
            session.rollback()
            error_msg = f"Unexpected error for record {model.key}: {e}"
            logger.error(error_msg)
            error_messages.append(error_msg)
            failed_inserts += 1
    
    return successful_inserts, failed_inserts, error_messages


def clear_existing_data(session: Session, model_class=UnidadProyecto) -> Tuple[bool, int, Optional[str]]:
    """
    Clear existing data from table.
    
    Args:
        session: Database session
        model_class: Model class to clear
        
    Returns:
        Tuple of (success: bool, deleted_count: int, error_message: Optional[str])
    """
    try:
        count = session.query(model_class).count()
        if count > 0:
            logger.info(f"🗑️  Clearing {count} existing records from {model_class.__tablename__}")
            deleted = session.query(model_class).delete()
            session.commit()
            logger.info(f"✅ Deleted {deleted} records")
            return True, deleted, None
        else:
            logger.info(f"📋 No existing records to clear in {model_class.__tablename__}")
            return True, 0, None
            
    except Exception as e:
        session.rollback()
        error_msg = f"Error clearing existing data: {e}"
        logger.error(error_msg)
        return False, 0, error_msg


def load_unidades_proyecto(
    geojson_path: str,
    batch_size: int = 100,
    clear_existing: bool = False
) -> Dict[str, Any]:
    """
    Main function to load unidades de proyecto data from GeoJSON to database.
    
    Args:
        geojson_path: Path to the GeoJSON file
        batch_size: Number of records to insert per batch
        clear_existing: Whether to clear existing data first
        
    Returns:
        Dictionary with loading results
    """
    start_time = datetime.now()
    
    logger.info("="*80)
    logger.info("BULK LOADING: UNIDADES DE PROYECTO")
    logger.info("="*80)
    
    # Pipeline using functional programming
    result = {
        'start_time': start_time,
        'success': False,
        'total_features': 0,
        'valid_features': 0,
        'models_created': 0,
        'successful_inserts': 0,
        'failed_inserts': 0,
        'errors': [],
        'duration_seconds': 0
    }
    
    try:
        # Step 1: Setup database
        success, session, error = setup_database()
        if not success:
            result['errors'].append(f"Database setup failed: {error}")
            return result
        
        # Step 2: Clear existing data if requested
        if clear_existing:
            success, deleted_count, error = clear_existing_data(session)
            if not success:
                result['errors'].append(f"Failed to clear existing data: {error}")
                return result
            result['deleted_records'] = deleted_count
        
        # Step 3: Load and process data using functional pipeline
        features = load_geojson_file(geojson_path)
        result['total_features'] = len(features)
        
        # Filter valid features
        valid_features = list(filter(validate_feature, features))
        result['valid_features'] = len(valid_features)
        
        logger.info(f"📊 Valid features: {len(valid_features)}/{len(features)}")
        
        # Create model instances
        models = list(filter(None, map(create_model_from_feature, valid_features)))
        result['models_created'] = len(models)
        
        logger.info(f"📊 Models created: {len(models)}")
        
        # Step 4: Bulk insert
        if models:
            successful, failed, errors = bulk_insert_models(session, models, batch_size)
            result['successful_inserts'] = successful
            result['failed_inserts'] = failed
            result['errors'].extend(errors)
            
            if successful > 0:
                result['success'] = True
        else:
            result['errors'].append("No valid models created for insertion")
        
        # Close session
        session.close()
        
    except Exception as e:
        error_msg = f"Unexpected error during loading: {e}"
        logger.error(error_msg)
        result['errors'].append(error_msg)
        
        if 'session' in locals():
            session.close()
    
    # Calculate duration
    end_time = datetime.now()
    result['end_time'] = end_time
    result['duration_seconds'] = (end_time - start_time).total_seconds()
    
    # Log summary
    logger.info("="*80)
    logger.info("LOADING SUMMARY")
    logger.info("="*80)
    logger.info(f"📊 Total features processed: {result['total_features']}")
    logger.info(f"📊 Valid features: {result['valid_features']}")
    logger.info(f"📊 Models created: {result['models_created']}")
    logger.info(f"📊 Successful inserts: {result['successful_inserts']}")
    logger.info(f"📊 Failed inserts: {result['failed_inserts']}")
    logger.info(f"⏱️  Duration: {result['duration_seconds']:.2f} seconds")
    
    if result['success']:
        logger.info("✅ LOADING COMPLETED SUCCESSFULLY")
    else:
        logger.error("❌ LOADING FAILED")
        for error in result['errors']:
            logger.error(f"   {error}")
    
    logger.info("="*80)
    
    return result


def load_all_available_data(clear_existing: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Load all available data from transformation outputs to database.
    
    Args:
        clear_existing: Whether to clear existing data first
        
    Returns:
        Dictionary with results for each data type
    """
    logger.info("="*80)
    logger.info("BULK LOADING: ALL AVAILABLE DATA")
    logger.info("="*80)
    
    results = {}
    
    # Define data sources and their corresponding load functions
    data_sources = {
        'unidades_proyecto': {
            'path': 'transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto_equipamientos.geojson',
            'loader': load_unidades_proyecto
        }
        # Add more data sources here as they become available
    }
    
    for data_type, config in data_sources.items():
        logger.info(f"\n🔄 Loading {data_type}...")
        
        file_path = os.path.join(os.path.dirname(__file__), '..', config['path'])
        
        if os.path.exists(file_path):
            result = config['loader'](file_path, clear_existing=clear_existing)
            results[data_type] = result
            
            if result['success']:
                logger.info(f"✅ {data_type}: {result['successful_inserts']} records loaded")
            else:
                logger.error(f"❌ {data_type}: Loading failed")
        else:
            logger.warning(f"⚠️  {data_type}: File not found - {file_path}")
            results[data_type] = {
                'success': False,
                'errors': [f"File not found: {file_path}"]
            }
    
    # Overall summary
    logger.info(f"\n📈 OVERALL SUMMARY:")
    total_success = sum(1 for r in results.values() if r.get('success', False))
    total_records = sum(r.get('successful_inserts', 0) for r in results.values())
    
    logger.info(f"  Data types processed: {len(results)}")
    logger.info(f"  Successful loads: {total_success}/{len(results)}")
    logger.info(f"  Total records loaded: {total_records}")
    
    return results


if __name__ == "__main__":
    """
    Script entry point for testing and standalone execution.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Bulk load data to database')
    parser.add_argument('--clear', action='store_true', help='Clear existing data before loading')
    parser.add_argument('--data-type', choices=['unidades_proyecto', 'all'], default='all',
                       help='Type of data to load')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for inserts')
    
    args = parser.parse_args()
    
    if args.data_type == 'unidades_proyecto':
        geojson_path = os.path.join(
            os.path.dirname(__file__), 
            '..',
            'transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto_equipamientos.geojson'
        )
        result = load_unidades_proyecto(geojson_path, args.batch_size, args.clear)
        
        # Exit with appropriate code
        sys.exit(0 if result['success'] else 1)
        
    else:  # args.data_type == 'all'
        results = load_all_available_data(args.clear)
        
        # Exit with appropriate code
        success_count = sum(1 for r in results.values() if r.get('success', False))
        sys.exit(0 if success_count == len(results) else 1)
