# -*- coding: utf-8 -*-
"""
Firebase data loading module for infrastructure project units (unidades de proyecto - Infraestructura Vial).
Loads data from context/unidades_proyecto.geojson and ensures compatibility with Firebase schema.
Automatically adds tipo_equipamiento='Vias' if not present in the source data.
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
def normalize_estado_value(estado_value: Any, avance_obra: Any = None) -> Optional[str]:
    """
    Normalize estado value to one of the three valid states.
    This ensures data quality at the loading stage as a safety net.
    
    Valid estados:
    - "En alistamiento"
    - "En ejecución"
    - "Terminado"
    
    Args:
        estado_value: Raw estado value from data
        avance_obra: Optional avance_obra value for business rule logic
        
    Returns:
        Normalized estado value or None
    """
    # Handle pandas NA/NaN
    try:
        if pd.isna(estado_value):
            estado_value = None
    except (TypeError, ValueError):
        pass
    
    # Convert to string and normalize
    if estado_value is not None:
        val_str = str(estado_value).strip().lower()
        if val_str == '' or val_str in ['nan', 'none', 'null']:
            estado_value = None
    
    # REGLA DE NEGOCIO: Si estado es None, determinar por avance_obra
    if estado_value is None:
        if avance_obra is None:
            return 'En alistamiento'
        try:
            avance_numeric = float(str(avance_obra).strip().replace(',', '.').replace('cero', '0').replace('(', '').replace(')', ''))
            if avance_numeric == 0.0:
                return 'En alistamiento'
            elif avance_numeric >= 100.0:
                return 'Terminado'
            else:
                return 'En ejecución'
        except (ValueError, TypeError):
            return 'En alistamiento'
    
    val_str = str(estado_value).strip().lower()
    
    # REGLA DE NEGOCIO 1: Si avance_obra es cero, establecer "En alistamiento"
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
    if 'socializaci' in val_str or 'alistamiento' in val_str or 'planeaci' in val_str or 'preparaci' in val_str or 'por iniciar' in val_str:
        return 'En alistamiento'
    elif 'ejecuci' in val_str or 'proceso' in val_str or 'construcci' in val_str or 'desarrollo' in val_str:
        return 'En ejecución'
    elif 'finalizado' in val_str or 'terminado' in val_str or 'completado' in val_str or 'concluido' in val_str or 'entregado' in val_str or 'liquidaci' in val_str:
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


# Campos que SIEMPRE deben guardarse como números decimales (float)
NUMERIC_FLOAT_FIELDS = {
    'avance_obra', 'presupuesto_base', 'valor_contrato', 'valor_ejecutado',
    'latitud', 'longitud', 'area', 'longitud_metros', 'ancho'
}

# Campos que SIEMPRE deben guardarse como números enteros (int)
NUMERIC_INT_FIELDS = {
    'ano', 'cantidad', 'bpin'
}


def serialize_for_firebase(value: Any, field_name: str = None) -> Any:
    """
    Serialize values for Firebase storage, handling lists and complex types.
    
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
        except (ValueError, TypeError):
            return 0  # Default para campos enteros
    
    # Handle numpy arrays and lists
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
        # Convert to string
        str_value = str(value)
        # Check if it's an ISO datetime string and convert to date only
        if 'T' in str_value or ' 00:00:00' in str_value:
            try:
                # Try to parse as datetime and return only the date part
                dt = pd.to_datetime(str_value)
                return dt.strftime('%Y-%m-%d')
            except:
                pass
        return str_value


@safe_execute
def prepare_document_data(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Prepare a single feature for Firebase document storage with robust serialization.
    Converts GeoJSON feature to Firebase-compatible document.
    
    IMPORTANTE: Esta función adapta la estructura para infraestructura vial:
    - Convierte geometrías LineString a formato compatible con Firebase
    - Agrega automáticamente tipo_equipamiento='Vias' si no existe
    - Mantiene compatibilidad con la estructura estándar de la API
    
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
    # CRÍTICO: Compatible con Firebase y frontend NextJS
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
    
    # Serialize properties for Firebase compatibility (flatten to root level)
    document_data = {}
    for key, value in properties.items():
        # Skip invalid keys (empty strings, None, or whitespace-only strings)
        if not key or not isinstance(key, str) or not key.strip():
            continue
        # Use clean key (remove any whitespace)
        clean_key = key.strip()
        # Pass field_name for context-aware serialization (numeric fields)
        document_data[clean_key] = serialize_for_firebase(value, field_name=clean_key)
    
    # ========================================================================
    # COMPATIBILIDAD TOTAL CON FIREBASE Y FRONTEND NEXTJS
    # ========================================================================
    
    # 1. TIPO EQUIPAMIENTO (OBLIGATORIO)
    # CRÍTICO: Siempre "Vias" para infraestructura vial
    document_data['tipo_equipamiento'] = 'Vias'
    
    # 2. GEOMETRY FIELD (FORMATO COMPATIBLE)
    # Geometry siempre en formato GeoJSON con coordinates como string
    document_data['geometry'] = geometry_field
    
    # 3. METADATA DE GEOMETRÍA (REQUERIDO POR FRONTEND)
    if geometry_field and isinstance(geometry_field, dict):
        document_data['has_geometry'] = True
        document_data['geometry_type'] = geometry_field.get('type', 'Unknown')
        
        # Calcular centros de gravedad solo si no existe
        # Para LineStrings, esto se usa en el mapa
        if not document_data.get('centros_gravedad'):
            document_data['centros_gravedad'] = False
    else:
        document_data['has_geometry'] = False
        document_data['geometry_type'] = None
        document_data['centros_gravedad'] = False
    
    # 4. TIMESTAMPS (FORMATO ISO 8601)
    current_time = datetime.now().isoformat()
    document_data['updated_at'] = current_time
    
    # Preservar created_at si existe, sino crear nuevo
    if 'created_at' not in document_data or not document_data['created_at']:
        document_data['created_at'] = current_time
    
    # 5. NORMALIZAR CAMPOS NUMÉRICOS
    # Asegurar que campos numéricos tengan el tipo correcto
    numeric_fields = {
        'presupuesto_base': str,  # Firebase espera string
        'avance_obra': str,       # Firebase espera string
        'cantidad': str,          # Firebase espera string
        'bpin': str,              # Firebase espera string
        'ano': str                # Firebase espera string
    }
    
    for field, target_type in numeric_fields.items():
        if field in document_data and document_data[field] is not None:
            try:
                # Convertir a string si no es None
                if target_type == str:
                    document_data[field] = str(document_data[field])
            except:
                pass
    
    # 6. NORMALIZAR ESTADO (SAFETY NET)
    # CRÍTICO: Asegurar que el campo 'estado' solo contenga valores válidos
    # Estados válidos: "En Alistamiento", "En Ejecución", "Terminado"
    if 'estado' in document_data:
        original_estado = document_data['estado']
        avance_obra = document_data.get('avance_obra')
        normalized_estado = normalize_estado_value(original_estado, avance_obra)
        
        if normalized_estado != original_estado:
            # Log estado normalization for tracking
            print(f"ℹ️  Estado normalizado: '{original_estado}' → '{normalized_estado}'")
        
        document_data['estado'] = normalized_estado
    
    # 7. LIMPIAR VALORES NaN/null QUE NO SEAN COMPATIBLES
    # Firebase no acepta NaN, convertir a None
    for key, value in list(document_data.items()):
        if value is None or (isinstance(value, float) and (str(value).lower() == 'nan')):
            document_data[key] = None
    
    return document_data


@safe_execute 
def get_document_id(feature: Dict[str, Any]) -> Optional[str]:
    """
    Extract document ID from feature properties.
    Uses multiple fallback strategies for infrastructure data.
    
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
    
    # Second: Use bpin combined with direccion for unique ID
    bpin = properties.get('bpin')
    direccion = properties.get('direccion')
    if bpin and direccion:
        # Create composite key from bpin and direccion
        safe_direccion = str(direccion).strip().replace(' ', '_')[:50]
        return f"BPIN-{str(bpin).strip()}-{safe_direccion}"
    
    # Third: Use bpin alone
    if bpin and str(bpin).strip():
        return f"BPIN-{str(bpin).strip()}"
    
    # Fourth: Use identificador
    identificador = properties.get('identificador')
    if identificador and str(identificador).strip():
        return f"ID-{str(identificador).strip()}"
    
    # Last resort: Use nombre_up if available
    nombre_up = properties.get('nombre_up')
    if nombre_up and str(nombre_up).strip():
        safe_name = str(nombre_up).strip().replace(' ', '_')[:50]
        return f"UP-{safe_name}"
    
    return None


@safe_execute
def validate_feature(feature: Dict[str, Any]) -> bool:
    """
    Validate that a feature has required data for Firebase storage.
    Adapted for infrastructure data with more flexible validation.
    
    Args:
        feature: GeoJSON feature to validate
        
    Returns:
        True if feature is valid for storage
    """
    if not feature or feature.get('type') != 'Feature':
        return False
    
    properties = feature.get('properties', {})
    
    # Must have at least one identifying field
    identifying_fields = ['upid', 'bpin', 'identificador', 'nombre_up', 'direccion']
    has_identifier = any(
        properties.get(field) and str(properties.get(field)).strip() 
        for field in identifying_fields
    )
    
    # Should have geometry (but not strictly required)
    geometry = feature.get('geometry')
    has_geometry = geometry is not None and geometry.get('coordinates') is not None
    
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
def process_batch(collection_name: str, batch_index: int, batch: List[Dict[str, Any]], upid_counter: int = 0) -> Dict[str, Any]:
    """
    Process a single batch of features to Firebase with selective field updates.
    Only updates fields that have changed, using document ID as unique identifier.
    Genera UPID automático si no existe (formato: UNP-XXXX).
    
    Args:
        collection_name: Firebase collection name
        batch_index: Index of current batch
        batch: List of features to process
        upid_counter: Starting counter for UPID generation
        
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
    unchanged_records = 0
    current_upid_counter = upid_counter
    
    # Use Firestore batch for efficient writes
    firebase_batch = db.batch()
    
    for feature in batch:
        try:
            # Validate feature
            if not validate_feature(feature):
                failed_writes += 1
                errors.append(f"Invalid feature: missing identifier")
                continue
            
            # Get document ID
            doc_id = get_document_id(feature)
            if not doc_id:
                failed_writes += 1
                errors.append(f"Failed to generate document ID")
                continue
            
            # Prepare new document data
            new_document_data = prepare_document_data(feature)
            if not new_document_data:
                failed_writes += 1
                errors.append(f"Failed to prepare document data")
                continue
            
            # CRÍTICO: Generar UPID si no existe
            if 'upid' not in new_document_data or not new_document_data['upid']:
                current_upid_counter += 1
                new_document_data['upid'] = f"UNP-{current_upid_counter:04d}"
            
            # Check if document exists
            doc_ref = collection_ref.document(doc_id)
            existing_doc = doc_ref.get()
            
            if existing_doc.exists:
                # Document exists - check for changes
                existing_data = existing_doc.to_dict()
                
                # Compare all fields at root level (exclude metadata fields)
                metadata_fields = {'created_at', 'updated_at'}
                data_changed = False
                
                # Check geometry field specifically
                existing_geometry = existing_data.get('geometry')
                new_geometry = new_document_data.get('geometry')
                
                # Detect geometry changes
                geometry_changed = False
                if existing_geometry != new_geometry:
                    if existing_geometry is None and new_geometry is not None:
                        geometry_changed = True
                    elif existing_geometry is not None and new_geometry is None:
                        geometry_changed = True
                    elif existing_geometry != new_geometry:
                        geometry_changed = True
                
                # Check for changes in any field
                all_keys = set(existing_data.keys()) | set(new_document_data.keys())
                for key in all_keys:
                    if key in metadata_fields:
                        continue
                    
                    existing_value = existing_data.get(key)
                    new_value = new_document_data.get(key)
                    
                    if existing_value != new_value:
                        data_changed = True
                        break
                
                # Update if any changes detected (including geometry)
                if data_changed or geometry_changed:
                    # Update document preserving created_at
                    new_document_data['updated_at'] = datetime.now().isoformat()
                    new_document_data['created_at'] = existing_data.get('created_at', datetime.now().isoformat())
                    firebase_batch.set(doc_ref, new_document_data)
                    updated_records += 1
                    successful_writes += 1
                else:
                    # No changes detected
                    unchanged_records += 1
            else:
                # New document
                new_document_data['created_at'] = datetime.now().isoformat()
                firebase_batch.set(doc_ref, new_document_data)
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
            'unchanged_records': unchanged_records,
            'upid_counter': current_upid_counter,
            'errors': errors[:5]  # Limit error list
        }
    except Exception as e:
        print(f"❌ Batch commit error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'batch_index': batch_index,
            'error': f"Batch commit failed: {str(e)}",
            'processed': 0,
            'failed': len(batch),
            'new_records': 0,
            'updated_records': 0,
            'unchanged_records': 0,
            'errors': errors
        }


def load_geojson_file(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Load GeoJSON file from local filesystem.
    Specifically designed for context/unidades_proyecto.geojson
    
    Args:
        file_path: Path to the GeoJSON file
        
    Returns:
        GeoJSON FeatureCollection or None if failed
    """
    try:
        if not os.path.exists(file_path):
            print(f"✗ File not found: {file_path}")
            return None
        
        print(f"📁 Loading from local file: {file_path}")
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
        
        # Analyze geometry types
        geometry_types = {}
        for feature in features:
            geom = feature.get('geometry', {})
            geom_type = geom.get('type', 'Unknown')
            geometry_types[geom_type] = geometry_types.get(geom_type, 0) + 1
        
        print(f"  - Geometry types:")
        for geom_type, count in geometry_types.items():
            print(f"    • {geom_type}: {count}")
        
        return geojson_data
        
    except Exception as e:
        print(f"✗ Error loading GeoJSON file: {e}")
        import traceback
        traceback.print_exc()
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
    print("FIREBASE UPLOAD PROCESS - INFRAESTRUCTURA VIAL")
    print("="*60)
    print(f"Collection: {collection_name}")
    print(f"Batch size: {batch_size}")
    print(f"Tipo equipamiento: Vias (auto-assigned)")
    
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
    
    # Obtener el máximo UPID existente en Firebase
    print("\n🔢 Obteniendo último UPID...")
    db = get_firestore_client()
    max_upid = 0
    try:
        # Buscar UPIDs con formato UNP-XXXX
        upid_docs = db.collection(collection_name).where('upid', '>=', 'UNP-').where('upid', '<', 'UNP-\uf8ff').stream()
        for doc in upid_docs:
            data = doc.to_dict()
            upid = data.get('upid', '')
            if upid.startswith('UNP-'):
                try:
                    num = int(upid.split('-')[1])
                    max_upid = max(max_upid, num)
                except:
                    pass
        print(f"✓ Último UPID encontrado: UNP-{max_upid:04d}")
        print(f"✓ Próximo UPID será: UNP-{max_upid+1:04d}")
    except Exception as e:
        print(f"⚠️  No se pudo obtener último UPID: {e}")
        print(f"   Iniciando contador en 0")
    
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
        'unchanged_records': 0,
        'start_time': datetime.now(),
        'errors': [],
        'upid_counter': max_upid
    }
    
    # Create batch processor with collection name
    batch_processor = process_batch(collection_name)
    
    # Process batches with progress bar
    print(f"\nUploading {len(valid_features)} features in {len(batches)} batches...")
    print("Using selective field updates based on document ID...")
    
    with tqdm(total=len(batches), desc="Uploading batches") as pbar:
        for i, batch in enumerate(batches):
            pbar.set_description(f"Batch {i+1}/{len(batches)}")
            
            # Process batch with current UPID counter
            batch_result = batch_processor(i, batch, results['upid_counter'])
            
            # Update results
            if batch_result.get('success'):
                results['successful_batches'] += 1
                results['total_uploaded'] += batch_result.get('processed', 0)
                results['total_failed'] += batch_result.get('failed', 0)
                results['new_records'] += batch_result.get('new_records', 0)
                results['updated_records'] += batch_result.get('updated_records', 0)
                results['unchanged_records'] += batch_result.get('unchanged_records', 0)
                # Actualizar contador UPID para el siguiente batch
                results['upid_counter'] = batch_result.get('upid_counter', results['upid_counter'])
            else:
                results['failed_batches'] += 1
                results['total_failed'] += len(batch)
                results['errors'].extend(batch_result.get('errors', []))
            
            # Update progress bar
            pbar.set_postfix({
                'new': results['new_records'],
                'updated': results['updated_records'],
                'unchanged': results['unchanged_records']
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
    print("FIREBASE UPLOAD SUMMARY - INFRAESTRUCTURA VIAL")
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
    print(f"  ➕ New records: {results['new_records']}")
    print(f"  🔄 Updated records: {results['updated_records']}")
    print(f"  ✅ Unchanged records: {results['unchanged_records']}")
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


def load_infraestructura_vial_to_firebase(
    input_file: str = None,
    collection_name: str = "unidades_proyecto",
    batch_size: int = 100
) -> bool:
    """
    Main function to load infrastructure data (Vias) to Firebase.
    Loads from context/unidades_proyecto.geojson by default.
    
    Args:
        input_file: Path to GeoJSON file (optional, uses default if None)
        collection_name: Firebase collection name (default: unidades_proyecto)
        batch_size: Batch size for uploads (optimized for Firebase)
        
    Returns:
        True if upload was successful, False otherwise
    """
    try:
        print("="*80)
        print("INFRAESTRUCTURA VIAL FIREBASE LOADING")
        print("="*80)
        
        # Determine input file path
        if input_file is None:
            # Use default path: context/unidades_proyecto.geojson
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            input_file = os.path.join(
                project_root,
                "context",
                "unidades_proyecto.geojson"
            )
        
        print(f"Source: {input_file}")
        print(f"Collection: {collection_name}")
        print(f"Batch size: {batch_size}")
        print(f"Tipo equipamiento: Vias (auto-assigned)")
        
        # Load GeoJSON data from local file
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
    """Main function for testing the Firebase loader for infrastructure data."""
    return load_infraestructura_vial_to_firebase()


if __name__ == "__main__":
    """
    Main execution block for loading infrastructure data (Vias) to Firebase.
    """
    print("Starting Firebase loading process for infrastructure data (Vias)...")
    
    # Run the complete loading pipeline
    success = main()
    
    if success:
        print("\n" + "="*60)
        print("FIREBASE LOADING COMPLETED SUCCESSFULLY")
        print("="*60)
        print("✅ All infrastructure data (Vias) uploaded to Firebase")
        
    else:
        print("\n" + "="*60)
        print("FIREBASE LOADING FAILED")
        print("="*60)
        print("❌ Could not upload infrastructure data to Firebase")
