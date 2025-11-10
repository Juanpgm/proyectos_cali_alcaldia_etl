# -*- coding: utf-8 -*-
"""
Data Transformation Module for RPC Contratos

Transforma y valida datos extraídos de contratos RPC:
- Validación de campos requeridos
- Normalización de datos
- Conversión de tipos
- Enriquecimiento de datos
- Generación de metadata

Implementa programación funcional para transformaciones limpias y componibles.
"""

import os
import sys
import json
import re
from typing import Dict, List, Optional, Any, Callable, Tuple
from functools import wraps, reduce
from datetime import datetime
from pathlib import Path
import pandas as pd

# Add project paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.config import secure_log


# Functional programming utilities
def compose(*functions: Callable) -> Callable:
    """Compose multiple functions into a single function."""
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(value: Any, *functions: Callable) -> Any:
    """Apply a sequence of functions to a value (pipe operator)."""
    return reduce(lambda acc, func: func(acc), functions, value)


def safe_execute(default_value: Any = None) -> Callable:
    """Decorator to safely execute functions with error handling."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"⚠️ Error en {func.__name__}: {e}")
                return default_value
        return wrapper
    return decorator


# Field validation functions
def is_valid_rpc_number(numero_rpc: Optional[str]) -> bool:
    """Validate RPC number format."""
    if not numero_rpc:
        return False
    # RPC numbers typically contain digits
    return bool(re.search(r'\d+', str(numero_rpc)))


def is_valid_document_id(doc_id: Optional[str]) -> bool:
    """Validate document identification number."""
    if not doc_id:
        return False
    # Should be numeric and have reasonable length
    cleaned = re.sub(r'\D', '', str(doc_id))
    return len(cleaned) >= 5 and len(cleaned) <= 15


def is_valid_date(date_str: Optional[str]) -> bool:
    """Validate date format DD/MM/YYYY."""
    if not date_str:
        return False
    
    pattern = r'^\d{2}/\d{2}/\d{4}$'
    if not re.match(pattern, str(date_str)):
        return False
    
    # Try to parse
    try:
        day, month, year = date_str.split('/')
        datetime(int(year), int(month), int(day))
        return True
    except (ValueError, AttributeError):
        return False


def is_valid_amount(amount: Any) -> bool:
    """Validate monetary amount."""
    if amount is None:
        return False
    
    try:
        value = float(amount)
        return value >= 0
    except (ValueError, TypeError):
        return False


# Data normalization functions
@safe_execute(default_value=None)
def normalize_rpc_number(numero_rpc: Optional[str]) -> Optional[str]:
    """Normalize RPC number."""
    if not numero_rpc:
        return None
    
    # Remove extra whitespace
    normalized = ' '.join(str(numero_rpc).split())
    
    return normalized.strip()


@safe_execute(default_value=None)
def normalize_document_id(doc_id: Optional[str]) -> Optional[str]:
    """Normalize document identification number."""
    if not doc_id:
        return None
    
    # Remove all non-numeric characters
    normalized = re.sub(r'\D', '', str(doc_id))
    
    return normalized if normalized else None


@safe_execute(default_value=None)
def normalize_beneficiary_name(name: Optional[str]) -> Optional[str]:
    """Normalize beneficiary name."""
    if not name:
        return None
    
    # Remove extra whitespace
    normalized = ' '.join(str(name).split())
    
    # Title case
    normalized = normalized.title()
    
    return normalized.strip()


@safe_execute(default_value=None)
def normalize_date(date_str: Optional[str]) -> Optional[str]:
    """Normalize date to DD/MM/YYYY format."""
    if not date_str:
        return None
    
    # Already in correct format
    if is_valid_date(date_str):
        return date_str
    
    # Try various formats
    formats = [
        r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # DD/MM/YYYY or DD-MM-YYYY
        r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY/MM/DD or YYYY-MM-DD
    ]
    
    for fmt in formats:
        match = re.search(fmt, str(date_str))
        if match:
            parts = match.groups()
            
            # Check if it's YYYY-MM-DD format
            if len(parts[0]) == 4:
                year, month, day = parts
            else:
                day, month, year = parts
            
            try:
                # Validate date
                datetime(int(year), int(month), int(day))
                return f"{int(day):02d}/{int(month):02d}/{year}"
            except ValueError:
                continue
    
    return None


@safe_execute(default_value=None)
def normalize_amount(amount: Any) -> Optional[float]:
    """Normalize monetary amount to float."""
    if amount is None:
        return None
    
    # If already a number
    if isinstance(amount, (int, float)):
        return float(amount)
    
    # Clean string
    cleaned = re.sub(r'[^\d.]', '', str(amount))
    
    try:
        return float(cleaned)
    except ValueError:
        return None


@safe_execute(default_value=None)
def normalize_bp_code(bp: Optional[str]) -> Optional[str]:
    """Normalize BP code."""
    if not bp:
        return None
    
    # Extract BP pattern (e.g., BP-2600470101/01/02)
    match = re.search(r'BP[- ]?\d+[\d/]*', str(bp), re.IGNORECASE)
    
    if match:
        return match.group(0).upper()
    
    return str(bp).strip().upper() if bp else None


@safe_execute(default_value=[])
def normalize_cdp_list(cdp_data: Any) -> List[str]:
    """Normalize CDP list."""
    if not cdp_data:
        return []
    
    # If already a list
    if isinstance(cdp_data, list):
        cdps = cdp_data
    # If string, split by common separators
    elif isinstance(cdp_data, str):
        cdps = re.split(r'[,;\n]', cdp_data)
    else:
        cdps = [str(cdp_data)]
    
    # Clean each CDP
    normalized = []
    for cdp in cdps:
        cleaned = str(cdp).strip()
        if cleaned:
            normalized.append(cleaned)
    
    return normalized


# Validation report generation
def generate_validation_report(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate validation report for extracted data.
    
    Args:
        data: Extracted RPC data
        
    Returns:
        Dictionary with validation results
    """
    report = {
        'is_valid': True,
        'warnings': [],
        'errors': [],
        'field_status': {}
    }
    
    # Required fields
    required_fields = [
        'numero_rpc',
        'beneficiario',
        'documento_identificacion',
        'valor_rpc'
    ]
    
    # Check required fields
    for field in required_fields:
        value = data.get(field)
        
        if value is None or (isinstance(value, str) and not value.strip()):
            report['errors'].append(f"Campo requerido faltante: {field}")
            report['field_status'][field] = 'missing'
            report['is_valid'] = False
        else:
            report['field_status'][field] = 'present'
    
    # Validate specific fields
    
    # RPC number
    if not is_valid_rpc_number(data.get('numero_rpc')):
        report['warnings'].append("Número RPC con formato inválido")
    
    # Document ID
    if not is_valid_document_id(data.get('documento_identificacion')):
        report['warnings'].append("Documento de identificación con formato inválido")
    
    # Dates
    date_fields = ['fecha_contabilizacion', 'fecha_impresion', 'plazo_contrato']
    for field in date_fields:
        if data.get(field) and not is_valid_date(data.get(field)):
            report['warnings'].append(f"Fecha inválida en campo: {field}")
    
    # Amount
    if not is_valid_amount(data.get('valor_rpc')):
        report['errors'].append("Valor RPC inválido")
        report['is_valid'] = False
    
    return report


# Main transformation function
@safe_execute(default_value=None)
def transform_rpc_data(raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Transform and validate extracted RPC data.
    
    Args:
        raw_data: Raw extracted data from extraction module
        
    Returns:
        Transformed and validated data or None if invalid
    """
    print("🔄 Transformando datos RPC...")
    
    # Create transformed data structure
    transformed = {
        # Core fields
        'numero_rpc': normalize_rpc_number(raw_data.get('numero_rpc')),
        'beneficiario': normalize_beneficiary_name(raw_data.get('beneficiario')),
        'documento_identificacion': normalize_document_id(raw_data.get('documento_identificacion')),
        'contrato_rpc': normalize_rpc_number(raw_data.get('contrato_rpc')),
        
        # Dates
        'fecha_contabilizacion': normalize_date(raw_data.get('fecha_contabilizacion')),
        'fecha_impresion': normalize_date(raw_data.get('fecha_impresion')),
        'plazo_contrato': normalize_date(raw_data.get('plazo_contrato')),
        
        # Status
        'estado_liberacion': raw_data.get('estado_liberacion'),
        
        # Description and amounts
        'descripcion_rpc': raw_data.get('descripcion_rpc'),
        'valor_rpc': normalize_amount(raw_data.get('valor_rpc')),
        
        # Budget codes
        'bp': normalize_bp_code(raw_data.get('bp')),
        'cdp_asociados': normalize_cdp_list(raw_data.get('cdp_asociados')),
        
        # Center
        'nombre_centro_gestor': raw_data.get('nombre_centro_gestor'),
    }
    
    # Generate validation report
    validation = generate_validation_report(transformed)
    
    # Add validation info
    transformed['validation'] = validation
    
    # Add metadata
    transformed['metadata'] = raw_data.get('metadata', {})
    transformed['metadata']['transformed_at'] = datetime.now().isoformat()
    
    # Check if valid
    if not validation['is_valid']:
        print(f"❌ Datos inválidos: {len(validation['errors'])} errores")
        for error in validation['errors']:
            print(f"   - {error}")
        return None
    
    # Show warnings if any
    if validation['warnings']:
        print(f"⚠️ {len(validation['warnings'])} advertencias:")
        for warning in validation['warnings']:
            print(f"   - {warning}")
    
    print("✅ Transformación completada")
    
    return transformed


# Batch transformation
@safe_execute(default_value=[])
def transform_rpc_batch(
    raw_data_list: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Transform batch of RPC data.
    
    Args:
        raw_data_list: List of raw extracted data
        
    Returns:
        Tuple of (transformed_data_list, summary_stats)
    """
    print(f"\n{'='*70}")
    print(f"🔄 Transformando {len(raw_data_list)} registros RPC")
    print("="*70)
    
    transformed_list = []
    stats = {
        'total': len(raw_data_list),
        'successful': 0,
        'failed': 0,
        'with_warnings': 0
    }
    
    for i, raw_data in enumerate(raw_data_list, 1):
        print(f"\n--- Registro {i}/{len(raw_data_list)} ---")
        
        transformed = transform_rpc_data(raw_data)
        
        if transformed:
            transformed_list.append(transformed)
            stats['successful'] += 1
            
            if transformed.get('validation', {}).get('warnings'):
                stats['with_warnings'] += 1
        else:
            stats['failed'] += 1
    
    # Print summary
    print(f"\n{'='*70}")
    print("📊 RESUMEN DE TRANSFORMACIÓN")
    print("="*70)
    print(f"✅ Exitosos: {stats['successful']}")
    print(f"⚠️ Con advertencias: {stats['with_warnings']}")
    print(f"❌ Fallidos: {stats['failed']}")
    print(f"📄 Total: {stats['total']}")
    
    return transformed_list, stats


# Save transformed data
@safe_execute(default_value=False)
def save_transformed_data(
    data: List[Dict[str, Any]],
    output_dir: str,
    output_name: str = "rpc_contratos_transformed"
) -> bool:
    """
    Save transformed data to JSON and CSV.
    
    Args:
        data: List of transformed RPC data
        output_dir: Directory to save outputs
        output_name: Base name for output files
        
    Returns:
        True if saved successfully
    """
    if not data:
        print("⚠️ No hay datos para guardar")
        return False
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Save JSON
    json_file = output_path / f"{output_name}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"💾 JSON guardado: {json_file}")
    
    # Prepare data for CSV (flatten structure)
    try:
        flattened_data = []
        for item in data:
            flat_item = {}
            
            # Core fields
            for key in ['numero_rpc', 'beneficiario', 'documento_identificacion',
                       'contrato_rpc', 'fecha_contabilizacion', 'fecha_impresion',
                       'estado_liberacion', 'plazo_contrato', 'descripcion_rpc',
                       'valor_rpc', 'bp', 'nombre_centro_gestor']:
                flat_item[key] = item.get(key)
            
            # CDP as comma-separated string
            cdp = item.get('cdp_asociados', [])
            flat_item['cdp_asociados'] = ', '.join(cdp) if cdp else None
            
            # Validation status
            validation = item.get('validation', {})
            flat_item['is_valid'] = validation.get('is_valid', False)
            flat_item['validation_warnings'] = len(validation.get('warnings', []))
            flat_item['validation_errors'] = len(validation.get('errors', []))
            
            # Metadata
            metadata = item.get('metadata', {})
            flat_item['source_file'] = metadata.get('source_file')
            flat_item['transformed_at'] = metadata.get('transformed_at')
            
            flattened_data.append(flat_item)
        
        # Save CSV
        df = pd.DataFrame(flattened_data)
        csv_file = output_path / f"{output_name}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        print(f"💾 CSV guardado: {csv_file}")
        print(f"📊 Registros guardados: {len(data)}")
        
    except Exception as e:
        print(f"⚠️ Error guardando CSV: {e}")
    
    return True


if __name__ == "__main__":
    """Prueba del módulo de transformación."""
    print("🧪 Módulo de Transformación RPC Contratos")
    print("="*70)
    
    # Example test data
    test_data = {
        'numero_rpc': 'RPC-12345',
        'beneficiario': 'JUAN PABLO GUZMAN MARTINEZ',
        'documento_identificacion': '4500357611',
        'valor_rpc': '170248807',
        'fecha_contabilizacion': '31/03/2026',
        'bp': 'BP-2600470101/01/02'
    }
    
    print("\n🔬 Probando transformación con datos de ejemplo...")
    transformed = transform_rpc_data(test_data)
    
    if transformed:
        print("\n✅ Transformación exitosa")
        print(f"📊 Datos transformados: {json.dumps(transformed, indent=2, default=str)}")
    else:
        print("\n❌ Transformación falló")
    
    print("\n✅ Módulo cargado correctamente")
