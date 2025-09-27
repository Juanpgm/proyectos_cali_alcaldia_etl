# -*- coding: utf-8 -*-
"""
Firebase data loading module for project units (unidades de proyecto) with batch processing.
Implements functional programming patterns for clean, scalable, and efficient Firebase loading.
"""

import os
import json
import sys
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Callable
from functools import reduce, partial, wraps
from datetime import datetime
import time

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.config import get_firestore_client, secure_log
from tqdm import tqdm


# Functional programming utilities
def compose(*functions: Callable) -> Callable:
    """Compose multiple functions into a single function."""
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(value: Any, *functions: Callable) -> Any:
    """Apply a sequence of functions to a value (pipe operator)."""
    return reduce(lambda acc, func: func(acc), functions, value)


def curry(func: Callable) -> Callable:
    """Convert a function to a curried version for partial application."""
    @wraps(func)
    def curried(*args, **kwargs):
        if len(args) + len(kwargs) >= func.__code__.co_argcount:
            return func(*args, **kwargs)
        return lambda *more_args, **more_kwargs: curried(*(args + more_args), **dict(kwargs, **more_kwargs))
    return curried


def safe_execute(func: Callable, fallback_value: Any = None) -> Callable:
    """Safely execute functions with error handling."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Error in {func.__name__}: {e}")
            return fallback_value
    return wrapper


# Firebase data preparation functions
def serialize_for_firebase(value: Any) -> Any:
    """
    Serialize values for Firebase storage, handling lists and complex types.
    
    Args:
        value: Value to serialize
        
    Returns:
        Firebase-compatible value
    """
    if value is None:
        return None
    
    # Handle numpy arrays and lists first
    if isinstance(value, (list, np.ndarray)):
        # Convert to list if numpy array
        if isinstance(value, np.ndarray):
            value = value.tolist()
        # Handle reference lists properly
        return [str(item) for item in value if item is not None and not (isinstance(item, float) and pd.isna(item))]
    
    # Check for pandas NA/NaN for scalar values only
    if hasattr(value, '__len__') and not isinstance(value, (str, bytes)):
        # For other sequence types, convert to string
        return str(value)
    
    # Check for scalar NA values
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        # pd.isna can fail on some types, continue processing
        pass
    
    if isinstance(value, dict):
        # Convert dicts to strings for Firebase
        return str(value)
    elif isinstance(value, (np.int64, np.int32)):
        return int(value)
    elif isinstance(value, (np.float64, np.float32)):
        return float(value)
    elif isinstance(value, bool):
        return bool(value)
    else:
        return str(value)


@safe_execute
def prepare_document_data(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Prepare a single feature for Firebase document storage with robust serialization.
    Converts GeoJSON feature to Firebase-compatible document.
    
    Args:
        feature: GeoJSON feature object
        
    Returns:
        Dict ready for Firebase storage or None if invalid
    """
    if not feature or feature.get('type') != 'Feature':
        return None
    
    # Extract properties and geometry
    properties = feature.get('properties', {})
    geometry = feature.get('geometry')
    
    # Serialize properties for Firebase compatibility
    serialized_properties = {}
    for key, value in properties.items():
        serialized_properties[key] = serialize_for_firebase(value)
    
    # Create document data
    document_data = {
        'type': 'Feature',
        'geometry': geometry,  # Keep full GeoJSON geometry
        'properties': serialized_properties,
        'created_at': datetime.now().isoformat(),
        'updated_at': datetime.now().isoformat()
    }
    
    # Add metadata
    if geometry:
        document_data['geometry_type'] = geometry.get('type')
        document_data['has_geometry'] = True
    else:
        document_data['geometry_type'] = None
        document_data['has_geometry'] = False
    
    return document_data


@safe_execute 
def get_document_id(feature: Dict[str, Any]) -> Optional[str]:
    """
    Extract document ID from feature properties.
    Uses upid as primary key, falls back to index-based ID.
    
    Args:
        feature: GeoJSON feature object
        
    Returns:
        String ID for Firebase document
    """
    properties = feature.get('properties', {})
    
    # Primary: Use upid if available
    upid = properties.get('upid')
    if upid and isinstance(upid, str) and upid.strip():
        return upid.strip()
    
    # Fallback: Use identificador
    identificador = properties.get('identificador')
    if identificador and str(identificador).strip():
        return f"ID-{str(identificador).strip()}"
    
    # Last resort: Use bpin if available
    bpin = properties.get('bpin')
    if bpin and str(bpin).strip():
        return f"BPIN-{str(bpin).strip()}"
    
    return None


@safe_execute
def validate_feature(feature: Dict[str, Any]) -> bool:
    """
    Validate that a feature has required data for Firebase storage.
    
    Args:
        feature: GeoJSON feature to validate
        
    Returns:
        True if feature is valid for storage
    """
    if not feature or feature.get('type') != 'Feature':
        return False
    
    properties = feature.get('properties', {})
    
    # Must have at least one identifying field
    identifying_fields = ['upid', 'identificador', 'bpin']
    has_identifier = any(
        properties.get(field) and str(properties.get(field)).strip() 
        for field in identifying_fields
    )
    
    return has_identifier


# Batch processing functions
def create_batches(features: List[Dict[str, Any]], batch_size: int = 100) -> List[List[Dict[str, Any]]]:
    """
    Split features list into optimized batches for Firebase processing.
    
    Args:
        features: List of GeoJSON features
        batch_size: Number of documents per batch (optimized for Firebase)
        
    Returns:
        List of feature batches
    """
    batches = []
    for i in range(0, len(features), batch_size):
        batch = features[i:i + batch_size]
        batches.append(batch)
    
    print(f"✓ Created {len(batches)} batches with {batch_size} documents each")
    return batches


@curry
def process_batch(collection_name: str, batch_index: int, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Process a single batch of features to Firebase.
    
    Args:
        collection_name: Firebase collection name
        batch_index: Index of current batch
        batch: List of features to process
        
    Returns:
        Dict with batch processing results
    """
    db = get_firestore_client()
    if not db:
        return {'success': False, 'error': 'Failed to get Firestore client', 'processed': 0}
    
    collection_ref = db.collection(collection_name)
    
    successful_writes = 0
    failed_writes = 0
    errors = []
    
    # Use Firestore batch for efficient writes
    firebase_batch = db.batch()
    
    for feature in batch:
        try:
            # Validate feature
            if not validate_feature(feature):
                failed_writes += 1
                errors.append(f"Invalid feature: missing identifier")
                continue
            
            # Prepare document data
            document_data = prepare_document_data(feature)
            if not document_data:
                failed_writes += 1
                errors.append(f"Failed to prepare document data")
                continue
            
            # Get document ID
            doc_id = get_document_id(feature)
            if not doc_id:
                failed_writes += 1
                errors.append(f"Failed to generate document ID")
                continue
            
            # Add to batch
            doc_ref = collection_ref.document(doc_id)
            firebase_batch.set(doc_ref, document_data)
            successful_writes += 1
            
        except Exception as e:
            failed_writes += 1
            errors.append(f"Error processing feature: {str(e)}")
    
    # Commit the batch
    try:
        firebase_batch.commit()
        return {
            'success': True,
            'batch_index': batch_index,
            'processed': successful_writes,
            'failed': failed_writes,
            'errors': errors[:5]  # Limit error list
        }
    except Exception as e:
        return {
            'success': False,
            'batch_index': batch_index,
            'error': f"Batch commit failed: {str(e)}",
            'processed': 0,
            'failed': len(batch)
        }


def load_geojson_file(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Load GeoJSON file from the transformation output.
    
    Args:
        file_path: Path to the GeoJSON file
        
    Returns:
        GeoJSON FeatureCollection or None if failed
    """
    try:
        if not os.path.exists(file_path):
            print(f"✗ File not found: {file_path}")
            return None
        
        with open(file_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        # Validate GeoJSON structure
        if geojson_data.get('type') != 'FeatureCollection':
            print(f"✗ Invalid GeoJSON: not a FeatureCollection")
            return None
        
        features = geojson_data.get('features', [])
        file_size_kb = os.path.getsize(file_path) / 1024
        
        print(f"✓ Loaded GeoJSON file: {os.path.basename(file_path)}")
        print(f"  - Features: {len(features)}")
        print(f"  - File size: {file_size_kb:.1f} KB")
        
        return geojson_data
        
    except Exception as e:
        print(f"✗ Error loading GeoJSON file: {e}")
        return None


@secure_log
def upload_to_firebase(geojson_data: Dict[str, Any], collection_name: str = "unidades_proyecto", batch_size: int = 100) -> Dict[str, Any]:
    """
    Upload GeoJSON features to Firebase Firestore with batch processing and progress tracking.
    
    Args:
        geojson_data: GeoJSON FeatureCollection
        collection_name: Firebase collection name
        batch_size: Number of documents per batch (optimized)
        
    Returns:
        Dict with upload results summary
    """
    print(f"\n" + "="*60)
    print("FIREBASE UPLOAD PROCESS")
    print("="*60)
    print(f"Collection: {collection_name}")
    print(f"Batch size: {batch_size}")
    
    # Extract features
    features = geojson_data.get('features', [])
    if not features:
        return {'success': False, 'error': 'No features found in GeoJSON'}
    
    # Filter valid features
    valid_features = [f for f in features if validate_feature(f)]
    invalid_count = len(features) - len(valid_features)
    
    print(f"Valid features: {len(valid_features)}")
    print(f"Invalid features: {invalid_count}")
    
    if not valid_features:
        return {'success': False, 'error': 'No valid features to upload'}
    
    # Create batches
    batches = create_batches(valid_features, batch_size)
    
    # Initialize results tracking
    results = {
        'total_features': len(features),
        'valid_features': len(valid_features),
        'invalid_features': invalid_count,
        'total_batches': len(batches),
        'successful_batches': 0,
        'failed_batches': 0,
        'total_uploaded': 0,
        'total_failed': 0,
        'start_time': datetime.now(),
        'errors': []
    }
    
    # Create batch processor with collection name
    batch_processor = process_batch(collection_name)
    
    # Process batches with progress bar
    print(f"\nUploading {len(valid_features)} features in {len(batches)} batches...")
    
    with tqdm(total=len(batches), desc="Uploading batches") as pbar:
        for i, batch in enumerate(batches):
            pbar.set_description(f"Batch {i+1}/{len(batches)}")
            
            # Process batch
            batch_result = batch_processor(i, batch)
            
            # Update results
            if batch_result.get('success'):
                results['successful_batches'] += 1
                results['total_uploaded'] += batch_result.get('processed', 0)
                results['total_failed'] += batch_result.get('failed', 0)
            else:
                results['failed_batches'] += 1
                results['total_failed'] += len(batch)
                results['errors'].extend(batch_result.get('errors', []))
            
            # Update progress bar
            pbar.set_postfix({
                'uploaded': results['total_uploaded'],
                'failed': results['total_failed']
            })
            pbar.update(1)
            
            # Small delay to prevent rate limiting
            time.sleep(0.1)
    
    # Calculate final statistics
    results['end_time'] = datetime.now()
    results['duration'] = (results['end_time'] - results['start_time']).total_seconds()
    results['success'] = results['successful_batches'] > 0
    results['upload_rate'] = results['total_uploaded'] / results['duration'] if results['duration'] > 0 else 0
    
    return results


def print_upload_summary(results: Dict[str, Any]):
    """
    Print a detailed summary of the upload results.
    
    Args:
        results: Upload results dictionary
    """
    print(f"\n" + "="*60)
    print("FIREBASE UPLOAD SUMMARY")
    print("="*60)
    
    print(f"📊 Processing Results:")
    print(f"  ✓ Total features processed: {results['total_features']}")
    print(f"  ✓ Valid features: {results['valid_features']}")
    print(f"  ✗ Invalid features: {results['invalid_features']}")
    
    print(f"\n📦 Batch Processing:")
    print(f"  ✓ Total batches: {results['total_batches']}")
    print(f"  ✓ Successful batches: {results['successful_batches']}")
    print(f"  ✗ Failed batches: {results['failed_batches']}")
    
    print(f"\n📤 Upload Results:")
    print(f"  ✓ Successfully uploaded: {results['total_uploaded']}")
    print(f"  ✗ Failed uploads: {results['total_failed']}")
    print(f"  📈 Success rate: {(results['total_uploaded'] / results['valid_features'] * 100):.1f}%")
    
    print(f"\n⏱️ Performance:")
    print(f"  ⏳ Duration: {results['duration']:.2f} seconds")
    print(f"  🚀 Upload rate: {results['upload_rate']:.1f} documents/second")
    
    if results['errors']:
        print(f"\n⚠️ Sample Errors (showing first 5):")
        for i, error in enumerate(results['errors'][:5], 1):
            print(f"  {i}. {error}")
    
    if results['success']:
        print(f"\n✅ Upload completed successfully!")
    else:
        print(f"\n❌ Upload failed or partially completed.")


def load_unidades_proyecto_to_firebase(
    input_file: str = None,
    collection_name: str = "unidades_proyecto",
    batch_size: int = 100
) -> bool:
    """
    Main function to load unidades de proyecto data to Firebase.
    
    Args:
        input_file: Path to GeoJSON file (optional, uses default if None)
        collection_name: Firebase collection name
        batch_size: Batch size for uploads (optimized for Firebase)
        
    Returns:
        True if upload was successful, False otherwise
    """
    try:
        print("="*80)
        print("UNIDADES DE PROYECTO FIREBASE LOADING")
        print("="*80)
        
        # Determine input file path
        if input_file is None:
            # Use default path from transformation output
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            input_file = os.path.join(
                project_root,
                "transformation_app",
                "app_outputs",
                "unidades_proyecto_outputs",
                "unidades_proyecto.geojson"
            )
        
        print(f"Input file: {input_file}")
        print(f"Collection: {collection_name}")
        print(f"Batch size: {batch_size}")
        
        # Load GeoJSON data
        geojson_data = load_geojson_file(input_file)
        if not geojson_data:
            print("✗ Failed to load GeoJSON data")
            return False
        
        # Upload to Firebase using functional pipeline
        upload_results = pipe(
            geojson_data,
            lambda data: upload_to_firebase(data, collection_name, batch_size)
        )
        
        # Display results
        print_upload_summary(upload_results)
        
        return upload_results.get('success', False)
        
    except Exception as e:
        print(f"✗ Error in Firebase loading process: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main function for testing the Firebase loader."""
    return load_unidades_proyecto_to_firebase()


if __name__ == "__main__":
    """
    Main execution block for loading unidades de proyecto to Firebase.
    """
    print("Starting Firebase loading process for unidades de proyecto...")
    
    # Run the complete loading pipeline
    success = main()
    
    if success:
        print("\n" + "="*60)
        print("FIREBASE LOADING COMPLETED SUCCESSFULLY")
        print("="*60)
        print("✅ All unidades de proyecto data uploaded to Firebase")
        
    else:
        print("\n" + "="*60)
        print("FIREBASE LOADING FAILED")
        print("="*60)
        print("❌ Could not upload unidades de proyecto data to Firebase")
