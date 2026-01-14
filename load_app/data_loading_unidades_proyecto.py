# -*- coding: utf-8 -*-
"""
Firebase data loading module for project units (unidades de proyecto) with batch processing.
Implements functional programming patterns for clean, scalable, and efficient Firebase loading.

Mejoras implementadas (Diciembre 2024):
- [OK] Eliminada lógica duplicada de detección de cambios
- [OK] Simplificada función process_batch (el pipeline ya filtra cambios)
- [OK] Mejor performance al no re-verificar cambios en cada documento
- [OK] Preservación correcta de created_at y updated_at timestamps

Nota: Este módulo asume que el pipeline ya ha filtrado los datos que necesitan
actualizarse. Por lo tanto, simplemente escribe todos los documentos recibidos
sin verificar cambios nuevamente, evitando redundancia y mejorando performance.
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
import hashlib

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.config import get_firestore_client, secure_log
from tqdm import tqdm


# Geometry processing utilities
def clean_2d_coordinates(coords: List[Any]) -> List[float]:
    """
    Clean a single coordinate pair to 2D [lon, lat] format.
    
    Args:
        coords: Coordinate array (may have 2D or 3D values)
        
    Returns:
        2D coordinate pair [lon, lat] rounded to 8 decimals
    """
    if len(coords) >= 2:
        return [round(float(coords[0]), 8), round(float(coords[1]), 8)]
    return None


def serialize_geometry_coordinates(geom_type: str, coords: Any) -> Any:
    """
    Serialize geometry coordinates to GeoJSON standard format.
    Handles all geometry types with proper nesting levels.
    Returns native arrays for Firebase compatibility (not JSON strings).
    
    Args:
        geom_type: GeoJSON geometry type
        coords: Raw coordinates from GeoJSON
        
    Returns:
        Cleaned coordinates in standard GeoJSON format
    """
    if not coords:
        return None
    
    try:
        if geom_type == 'Point':
            # Point: [lon, lat]
            return clean_2d_coordinates(coords)
        
        elif geom_type == 'LineString':
            # LineString: [[lon, lat], [lon, lat], ...]
            return [clean_2d_coordinates(c) for c in coords if len(c) >= 2]
        
        elif geom_type == 'Polygon':
            # Polygon: [[[lon, lat], ...], ...] (array of rings)
            return [
                [clean_2d_coordinates(c) for c in ring if len(c) >= 2]
                for ring in coords
            ]
        
        elif geom_type == 'MultiPoint':
            # MultiPoint: [[lon, lat], [lon, lat], ...]
            return [clean_2d_coordinates(c) for c in coords if len(c) >= 2]
        
        elif geom_type == 'MultiLineString':
            # MultiLineString: [[[lon, lat], ...], [[lon, lat], ...], ...]
            return [
                [clean_2d_coordinates(c) for c in line if len(c) >= 2]
                for line in coords
            ]
        
        elif geom_type == 'MultiPolygon':
            # MultiPolygon: [[[[lon, lat], ...], ...], ...]
            return [
                [
                    [clean_2d_coordinates(c) for c in ring if len(c) >= 2]
                    for ring in polygon
                ]
                for polygon in coords
            ]
        
        else:
            # Unknown geometry type - return as-is
            return coords
    
    except Exception as e:
        print(f"Warning: Error serializing {geom_type} coordinates: {e}")
        return None


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
# Campos que SIEMPRE deben guardarse como números decimales (float)
NUMERIC_FLOAT_FIELDS = {
    'avance_obra', 'valor_ejecutado',
    'latitud', 'longitud', 'area', 'longitud_metros', 'ancho'
}

# Campos que SIEMPRE deben guardarse como números enteros (int)
# CRÍTICO: presupuesto_base debe ser int para evitar pérdida de precisión con valores grandes
NUMERIC_INT_FIELDS = {
    'ano', 'cantidad', 'bpin', 'n_intervenciones', 'presupuesto_base', 'valor_contrato'
}


def serialize_for_firebase(value: Any, field_name: str = None) -> Any:
    """
    Serialize values for Firebase storage, handling lists and complex types.
    Preserves data quality from transformation phase.
    
    Args:
        value: Value to serialize
        field_name: Optional field name for context-aware serialization
        
    Returns:
        Firebase-compatible value
    """
    if value is None:
        return None
    
    # Check for scalar NA values first
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        # pd.isna can fail on some types, continue processing
        pass
    
    # CRITICAL: Handle numeric fields BEFORE string conversion
    # Campos que deben ser float (decimal)
    if field_name and field_name.lower() in NUMERIC_FLOAT_FIELDS:
        try:
            # Limpiar y convertir a float
            if isinstance(value, str):
                clean_value = value.strip().replace(',', '.').replace('%', '').replace('(', '').replace(')', '')
                if clean_value.lower() in ['', 'nan', 'none', 'null', 'cero']:
                    return 0.0
                return float(clean_value)
            elif isinstance(value, (int, float, np.int64, np.int32, np.float64, np.float32)):
                return float(value)
        except (ValueError, TypeError):
            return 0.0  # Default para campos numéricos
    
    # Campos que deben ser int (entero)
    if field_name and field_name.lower() in NUMERIC_INT_FIELDS:
        try:
            if isinstance(value, str):
                clean_value = value.strip().replace(',', '').replace('.0', '')
                if clean_value.lower() in ['', 'nan', 'none', 'null']:
                    return 0
                # Intentar convertir a int (puede tener decimales)
                return int(float(clean_value))
            elif isinstance(value, (int, float, np.int64, np.int32, np.float64, np.float32)):
                return int(value)
            elif isinstance(value, bool):
                return int(value)
            else:
                # Intentar convertir cualquier otro tipo a int
                return int(value)
        except (ValueError, TypeError):
            return 0  # Default para campos enteros
    
    # Handle numpy arrays and lists
    if isinstance(value, (list, np.ndarray)):
        # Convert to list if numpy array
        if isinstance(value, np.ndarray):
            value = value.tolist()
        
        # SPECIAL CASE: Preserve intervenciones as array of dicts (not strings)
        if field_name == 'intervenciones':
            result = []
            for item in value:
                if item is None or (isinstance(item, float) and pd.isna(item)):
                    continue
                # Preserve dicts as-is for intervenciones
                if isinstance(item, dict):
                    # Recursively serialize dict contents
                    serialized_item = {}
                    for k, v in item.items():
                        serialized_item[k] = serialize_for_firebase(v, field_name=k)
                    result.append(serialized_item)
                else:
                    result.append(str(item))
            return result
        
        # Handle reference lists properly (convert to strings)
        return [str(item) for item in value if item is not None and not (isinstance(item, float) and pd.isna(item))]
    
    # Check for pandas NA/NaN for scalar values only
    if hasattr(value, '__len__') and not isinstance(value, (str, bytes)):
        # For other sequence types, convert to string
        return str(value)
    
    if isinstance(value, dict):
        # Convert dicts to strings for Firebase
        return str(value)
    elif isinstance(value, (np.int64, np.int32)):
        return int(value)
    elif isinstance(value, (np.float64, np.float32)):
        return float(value)
    elif isinstance(value, bool):
        return bool(value)
    elif isinstance(value, str):
        # CRITICAL: Preserve string values as-is (don't alter normalized states, etc.)
        str_value = value.strip()
        
        # Only try to parse as datetime if field name suggests it's a date
        if field_name and ('fecha' in field_name.lower() or 'date' in field_name.lower()):
            # Check if it's an ISO datetime string and convert to date only
            if 'T' in str_value or ' 00:00:00' in str_value:
                try:
                    # Try to parse as datetime and return only the date part
                    dt = pd.to_datetime(str_value)
                    return dt.strftime('%Y-%m-%d')
                except:
                    pass
        
        return str_value
    else:
        # Convert to string as last resort
        return str(value)


def normalize_estado_value(estado_value: Any, avance_obra: Any = None) -> Optional[str]:
    """
    Normalize estado value to one of the valid states.
    This ensures data quality at the loading stage as a safety net.
    
    Valid estados:
    - "En alistamiento"
    - "En ejecución"
    - "Terminado"
    - "Suspendido" (se preserva sin modificar)
    - "Inaugurado" (se preserva sin modificar)
    
    Args:
        estado_value: Raw estado value from data
        avance_obra: Optional avance_obra value for business rule logic
        
    Returns:
        Normalized estado value or None
    """
    if estado_value is None:
        return None
    
    # Handle pandas NA/NaN
    try:
        if pd.isna(estado_value):
            return None
    except (TypeError, ValueError):
        pass
    
    # Convert to string and normalize
    val_str = str(estado_value).strip()
    val_lower = val_str.lower()
    
    if val_lower == '' or val_lower in ['nan', 'none', 'null']:
        return None
    
    # PRIORIDAD 1: Preservar estados especiales (Suspendido e Inaugurado) - NO MODIFICAR
    if val_lower in {'suspendido', 'inaugurado'}:
        return val_lower.title()  # Retorna "Suspendido" o "Inaugurado"
    
    # REGLA DE NEGOCIO 2: Si avance_obra es cero, establecer "En alistamiento"
    if avance_obra is not None:
        try:
            avance_numeric = float(str(avance_obra).strip().replace(',', '.').replace('cero', '0').replace('(', '').replace(')', ''))
            
            # Si es exactamente 0, es alistamiento
            if avance_numeric == 0.0:
                return 'En alistamiento'
            
            # Si es 100 o más, está terminado
            if avance_numeric >= 100.0:
                return 'Terminado'
                
        except (ValueError, TypeError):
            pass
    
    # Map all variations to standard values
    if 'socializaci' in val_lower or 'alistamiento' in val_lower or 'planeaci' in val_lower or 'preparaci' in val_lower or 'por iniciar' in val_lower:
        return 'En alistamiento'
    elif 'ejecuci' in val_lower or 'proceso' in val_lower or 'construcci' in val_lower or 'desarrollo' in val_lower:
        return 'En ejecución'
    elif 'finalizado' in val_lower or 'terminado' in val_lower or 'completado' in val_lower or 'concluido' in val_lower or 'entregado' in val_lower or 'liquidaci' in val_lower:
        return 'Terminado'
    else:
        # Default to 'En ejecución' for unknown states
        # unless avance suggests otherwise
        try:
            if avance_obra is not None:
                avance_numeric = float(str(avance_obra).strip().replace(',', '.'))
                if avance_numeric >= 100:
                    return 'Terminado'
                elif avance_numeric == 0:
                    return 'En alistamiento'
        except:
            pass
        
        return 'En ejecución'  # Default state


@safe_execute
def prepare_document_data(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Prepare a single feature for Firebase document storage with robust serialization.
    Converts GeoJSON feature to Firebase-compatible document.
    
    Estructura compatible con el estándar de la API:
    - Atributos (properties) a nivel raíz del documento
    - Geometry como campo separado con coordenadas [lat, lon]
    
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
    
    # Convert geometry to GeoJSON standard format (2D coordinates only)
    # Compatible con Firebase y frontend Next.js
    # - Solo coordenadas 2D: [longitude, latitude]
    # - Sin elevación (z-coordinate)
    # - Formato consistente para todos los tipos de geometría
    # - Point usa array nativo, otros tipos usan JSON string (límite de Firebase)
    geometry_field = None
    if geometry:
        geom_type = geometry.get('type')
        coords = geometry.get('coordinates', [])
        
        # Usar helper de serialización para limpiar coordenadas
        clean_coords = serialize_geometry_coordinates(geom_type, coords)
        
        if clean_coords is not None:
            # Firebase solo permite 1 nivel de anidamiento en arrays
            # Point: array directo [lon, lat] - OK
            # LineString/Polygon/Multi*: JSON string (Next.js puede parsear)
            if geom_type == 'Point':
                # Point puede usar array nativo
                geometry_field = {
                    'type': geom_type,
                    'coordinates': clean_coords
                }
            else:
                # Otros tipos: serializar coordinates como JSON string
                geometry_field = {
                    'type': geom_type,
                    'coordinates': json.dumps(clean_coords, separators=(',', ':'))
                }
    
    # CRÍTICO: Si no hay geometry pero hay lat/lon en properties, crear geometry
    if not geometry_field:
        lat = properties.get('lat')
        lon = properties.get('lon')
        
        # Intentar convertir a numérico si son strings
        if lat is not None and lon is not None:
            try:
                if isinstance(lat, str):
                    lat = float(lat.replace(',', '.'))
                else:
                    lat = float(lat)
                
                if isinstance(lon, str):
                    lon = float(lon.replace(',', '.'))
                else:
                    lon = float(lon)
                
                # Validar rangos (Cali y área metropolitana, rangos ampliados)
                if 2.5 <= lat <= 4.5 and -77.5 <= lon <= -75.5:
                    geometry_field = {
                        'type': 'Point',
                        'coordinates': [round(lon, 8), round(lat, 8)]
                    }
                    print(f"      [CREATE] Geometry creada desde lat/lon para documento")
            except (ValueError, TypeError):
                pass  # No se pudo convertir
    
    # Serialize properties for Firebase compatibility (flatten to root level)
    document_data = {}
    for key, value in properties.items():
        # Skip invalid keys (empty strings, None, or whitespace-only strings)
        if not key or not isinstance(key, str) or not key.strip():
            continue
        # Use clean key (remove any whitespace)
        clean_key = key.strip()
        # Pass field name for context-aware serialization
        document_data[clean_key] = serialize_for_firebase(value, field_name=clean_key)
    
    # CRITICAL: Normalize estado value as safety net (even if already normalized in transformation)
    # This ensures data quality regardless of data source
    if 'estado' in document_data:
        original_estado = document_data['estado']
        avance_obra = document_data.get('avance_obra')
        normalized_estado = normalize_estado_value(original_estado, avance_obra)
        
        if normalized_estado != original_estado and original_estado is not None:
            print(f"  ℹ️  Estado normalizado: '{original_estado}' → '{normalized_estado}'")
        
        document_data['estado'] = normalized_estado
    
    # VALIDATION: Verify estado is valid (final check)
    if 'estado' in document_data and document_data['estado'] is not None:
        valid_estados = {'En alistamiento', 'En ejecución', 'Terminado', 'Suspendido', 'Inaugurado'}
        current_estado = document_data['estado']
        if current_estado not in valid_estados:
            print(f"[ERROR] ERROR: Invalid estado after normalization: '{current_estado}'")
            # This should never happen if normalize_estado_value works correctly
    
    # Add geometry as separate field (not nested in properties)
    # Store as GeoJSON object: {type: 'Point', coordinates: [lat, lon]}
    document_data['geometry'] = geometry_field
    
    # Calculate and store hash for incremental change detection
    # This hash is used to detect if the document has changed
    import hashlib
    record_for_hash = {
        'properties': properties,
        'geometry': geometry
    }
    record_str = json.dumps(record_for_hash, sort_keys=True, default=str)
    document_data['_hash'] = hashlib.md5(record_str.encode('utf-8')).hexdigest()
    
    # Add metadata
    document_data['created_at'] = datetime.now().isoformat()
    document_data['updated_at'] = datetime.now().isoformat()
    
    if geometry_field and isinstance(geometry_field, dict):
        document_data['has_geometry'] = True
    else:
        document_data['has_geometry'] = False
    
    return document_data


@safe_execute 
def get_document_id(feature: Dict[str, Any]) -> Optional[str]:
    """
    Extract document ID from feature properties.
    Uses ONLY upid as primary key for consistency across all centros gestores.
    IMPORTANTE: Todas las unidades de proyecto DEBEN tener upid generado.
    
    Args:
        feature: GeoJSON feature object
        
    Returns:
        String ID for Firebase document (upid) or None if not available
    """
    properties = feature.get('properties', {})
    
    # CAMBIO CRÍTICO: SOLO usar upid como identificador
    # Esto garantiza que todas las unidades (incluidas vías de Secretaría de Infraestructura)
    # usen el mismo formato de identificador en Firebase
    upid = properties.get('upid')
    if upid and isinstance(upid, str) and upid.strip():
        return upid.strip()
    
    # Si no tiene upid, es un error de datos - no cargar
    # Esto fuerza a que el pipeline de transformación genere upid siempre
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
    
    print(f"[OK] Created {len(batches)} batches with {batch_size} documents each")
    return batches


@curry
def process_batch(collection_name: str, batch_index: int, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Process a single batch of features to Firebase.
    Assumes the pipeline has already filtered for changes, so this function
    simply writes/updates all received documents without re-checking.
    
    Args:
        collection_name: Firebase collection name
        batch_index: Index of current batch
        batch: List of features to process (already filtered by pipeline)
        
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
    new_records = 0
    updated_records = 0
    
    # Use Firestore batch for efficient writes
    firebase_batch = db.batch()
    
    for feature in batch:
        try:
            # Validate feature
            if not validate_feature(feature):
                failed_writes += 1
                errors.append(f"Invalid feature: missing identifier")
                continue
            
            # Get document ID (upid)
            doc_id = get_document_id(feature)
            if not doc_id:
                failed_writes += 1
                errors.append(f"Failed to generate document ID")
                continue
            
            # Prepare document data
            document_data = prepare_document_data(feature)
            if not document_data:
                failed_writes += 1
                errors.append(f"Failed to prepare document data")
                continue
            
            # Check if document exists (quick check for preserving created_at and geometry)
            doc_ref = collection_ref.document(doc_id)
            existing_doc = doc_ref.get()
            
            if existing_doc.exists:
                # Document exists - update it (pipeline already filtered changes)
                existing_data = existing_doc.to_dict()
                document_data['updated_at'] = datetime.now().isoformat()
                document_data['created_at'] = existing_data.get('created_at', datetime.now().isoformat())
                
                # CRÍTICO: Preservar geometry existente si el nuevo dato no tiene geometry
                geometry_was_preserved = False
                if 'geometry' not in document_data or not document_data.get('geometry'):
                    existing_geometry = existing_data.get('geometry')
                    if existing_geometry:
                        document_data['geometry'] = existing_geometry
                        geometry_was_preserved = True
                        print(f"      [PRESERVE] Geometry preserved for {doc_id}")
                
                # Si se preservó la geometría, recalcular el hash con la geometría restaurada
                if geometry_was_preserved:
                    # Reconstruir el feature para calcular hash correcto
                    feature_for_hash = {
                        'properties': {k: v for k, v in document_data.items() 
                                     if k not in ['geometry', 'created_at', 'updated_at', '_hash', 'has_geometry']},
                        'geometry': document_data.get('geometry')
                    }
                    record_str = json.dumps(feature_for_hash, sort_keys=True, default=str)
                    document_data['_hash'] = hashlib.md5(record_str.encode('utf-8')).hexdigest()
                
                firebase_batch.set(doc_ref, document_data)
                updated_records += 1
                successful_writes += 1
            else:
                # New document
                document_data['created_at'] = datetime.now().isoformat()
                document_data['updated_at'] = datetime.now().isoformat()
                firebase_batch.set(doc_ref, document_data)
                new_records += 1
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
            'new_records': new_records,
            'updated_records': updated_records,
            'errors': errors[:5]  # Limit error list
        }
    except Exception as e:
        return {
            'success': False,
            'batch_index': batch_index,
            'error': f"Batch commit failed: {str(e)}",
            'processed': 0,
            'failed': len(batch),
            'new_records': 0,
            'updated_records': 0
        }


def load_geojson_file(file_path: str, use_s3: bool = True, s3_key: str = None) -> Optional[Dict[str, Any]]:
    """
    Load GeoJSON file from S3 or local filesystem.
    
    Args:
        file_path: Path to the GeoJSON file (used if use_s3=False or S3 fails)
        use_s3: Whether to try loading from S3 first (default: True)
        s3_key: S3 key to use (default: up-geodata/unidades_proyecto_transformed/current/unidades_proyecto_transformed.geojson.gz)
        
    Returns:
        GeoJSON FeatureCollection or None if failed
    """
    geojson_data = None
    
    # Try loading from S3 first if enabled
    if use_s3:
        try:
            # Import S3 downloader
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from utils.s3_downloader import S3Downloader
            
            # Default S3 key if not provided - use CURRENT compressed version (latest)
            if s3_key is None:
                s3_key = "up-geodata/unidades_proyecto_transformed/current/unidades_proyecto_transformed.geojson.gz"
            
            print(f"🌐 Attempting to load from S3...")
            downloader = S3Downloader("aws_credentials.json")
            geojson_data = downloader.read_json_from_s3(s3_key)
            
            if geojson_data:
                # Validate GeoJSON structure
                if geojson_data.get('type') != 'FeatureCollection':
                    print(f"✗ Invalid GeoJSON from S3: not a FeatureCollection")
                    geojson_data = None
                else:
                    features = geojson_data.get('features', [])
                    print(f"[OK] Loaded GeoJSON from S3")
                    print(f"  - Features: {len(features)}")
                    return geojson_data
            else:
                print(f"[WARNING] Could not load from S3, falling back to local file")
                
        except Exception as e:
            print(f"[WARNING] S3 loading failed: {e}")
            print(f"   Falling back to local file")
    
    # Fallback to local file if S3 failed or disabled
    try:
        if not os.path.exists(file_path):
            print(f"✗ File not found: {file_path}")
            return None
        
        print(f"[FILE] Loading from local file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        # Validate GeoJSON structure
        if geojson_data.get('type') != 'FeatureCollection':
            print(f"✗ Invalid GeoJSON: not a FeatureCollection")
            return None
        
        features = geojson_data.get('features', [])
        file_size_kb = os.path.getsize(file_path) / 1024
        
        print(f"[OK] Loaded GeoJSON file: {os.path.basename(file_path)}")
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
    
    # Initialize results tracking with selective update stats
    results = {
        'total_features': len(features),
        'valid_features': len(valid_features),
        'invalid_features': invalid_count,
        'total_batches': len(batches),
        'successful_batches': 0,
        'failed_batches': 0,
        'total_uploaded': 0,
        'total_failed': 0,
        'new_records': 0,
        'updated_records': 0,
        'start_time': datetime.now(),
        'errors': []
    }
    
    # Create batch processor with collection name
    batch_processor = process_batch(collection_name)
    
    # Process batches with progress bar
    print(f"\nUploading {len(valid_features)} features in {len(batches)} batches...")
    print("Using selective field updates based on upid...")
    
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
                results['new_records'] += batch_result.get('new_records', 0)
                results['updated_records'] += batch_result.get('updated_records', 0)
            else:
                results['failed_batches'] += 1
                results['total_failed'] += len(batch)
                results['errors'].extend(batch_result.get('errors', []))
            
            # Update progress bar
            pbar.set_postfix({
                'new': results['new_records'],
                'updated': results['updated_records']
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
    
    print(f"[DATA] Processing Results:")
    print(f"  [OK] Total features processed: {results['total_features']}")
    print(f"  [OK] Valid features: {results['valid_features']}")
    print(f"  ✗ Invalid features: {results['invalid_features']}")
    
    print(f"\n📦 Batch Processing:")
    print(f"  [OK] Total batches: {results['total_batches']}")
    print(f"  [OK] Successful batches: {results['successful_batches']}")
    print(f"  ✗ Failed batches: {results['failed_batches']}")
    
    print(f"\n[OUT] Upload Results:")
    print(f"  [+] New records: {results['new_records']}")
    print(f"  [SYNC] Updated records: {results['updated_records']}")
    print(f"  ✗ Failed uploads: {results['total_failed']}")
    print(f"  [STATS] Success rate: {(results['total_uploaded'] / results['valid_features'] * 100):.1f}%")
    
    print(f"\n[TIME] Performance:")
    print(f"  [WAIT] Duration: {results['duration']:.2f} seconds")
    print(f"  [START] Upload rate: {results['upload_rate']:.1f} documents/second")
    
    if results['errors']:
        print(f"\n[WARNING] Sample Errors (showing first 5):")
        for i, error in enumerate(results['errors'][:5], 1):
            print(f"  {i}. {error}")
    
    if results['success']:
        print(f"\n[OK] Upload completed successfully!")
    else:
        print(f"\n[ERROR] Upload failed or partially completed.")


def load_unidades_proyecto_to_firebase(
    input_file: str = None,
    collection_name: str = "unidades_proyecto",
    batch_size: int = 100,
    use_s3: bool = True,
    s3_key: str = None
) -> bool:
    """
    Main function to load unidades de proyecto data to Firebase.
    
    Args:
        input_file: Path to GeoJSON file (optional, uses default if None)
        collection_name: Firebase collection name
        batch_size: Batch size for uploads (optimized for Firebase)
        use_s3: Whether to load from S3 (default: True)
        s3_key: S3 key if using S3 (default: up-geodata/unidades_proyecto_transformed/current/unidades_proyecto_transformed.geojson.gz)
        
    Returns:
        True if upload was successful, False otherwise
    """
    try:
        print("="*80)
        print("UNIDADES DE PROYECTO FIREBASE LOADING")
        print("="*80)
        
        # Determine input file path (used as fallback if S3 fails)
        if input_file is None:
            # Use default path from transformation output
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            input_file = os.path.join(
                project_root,
                "app_outputs",
                "unidades_proyecto_transformed.geojson"
            )
        
        print(f"Source: {'S3 (with local fallback)' if use_s3 else 'Local file'}")
        if use_s3:
            print(f"S3 Key: {s3_key or 'up-geodata/unidades_proyecto_transformed/current/unidades_proyecto_transformed.geojson.gz'}")
        print(f"Fallback file: {input_file}")
        print(f"Collection: {collection_name}")
        print(f"Batch size: {batch_size}")
        
        # Load GeoJSON data (from S3 or local)
        geojson_data = load_geojson_file(input_file, use_s3=use_s3, s3_key=s3_key)
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
        print("[OK] All unidades de proyecto data uploaded to Firebase")
        
    else:
        print("\n" + "="*60)
        print("FIREBASE LOADING FAILED")
        print("="*60)
        print("[ERROR] Could not upload unidades de proyecto data to Firebase")
