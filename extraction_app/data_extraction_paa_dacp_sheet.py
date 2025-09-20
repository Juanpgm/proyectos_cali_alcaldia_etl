"""
Extracción simple de datos PAA DACP desde Google Sheets usando programación funcional.
"""

import sys
import os
from typing import Dict, List, Any, Tuple
from functools import partial
import json
import logging

# Agregar el directorio actual al path para importar google_sheets_functional
sys.path.append(os.path.dirname(__file__))

from google_sheets_functional import (
    extract_from_sheet_by_id,
    create_column_mapper,
    create_value_cleaner,
    create_row_filter
)

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ID del spreadsheet específico
SPREADSHEET_ID = "1646MbOzi2bsV557g4iT8I_gTSVxjOi9F2lZYMLiCRhU"

def create_paa_dacp_transformers():
    """
    Crear lista de transformadores para datos PAA DACP.
    """
    # Mapeo de columnas basado en la estructura real del PAA 2025
    column_mapping = {
        # Columnas identificadas en el diagnóstico
        'CODIGO ORGANISMO': 'codigo_organismo',
        'ORGANISMO': 'organismo',
        'DESCRIPCION': 'descripcion',
        'VIGENCIA': 'vigencia',
        'FECHA INICIO': 'fecha_inicio',
        
        # Mapeos adicionales comunes
        'codigo': 'codigo',
        'código': 'codigo',
        'descripcion': 'descripcion',
        'descripción': 'descripcion',
        'valor': 'valor_presupuestado',
        'presupuesto': 'valor_presupuestado',
        'monto': 'valor_presupuestado',
        'fecha': 'fecha_programada',
        'fecha_programada': 'fecha_programada',
        'estado': 'estado',
        'modalidad': 'modalidad_contratacion',
        'dependencia': 'dependencia_responsable',
        'fuente': 'fuente_recursos',
        'rubro': 'rubro_presupuestal',
        'observaciones': 'observaciones'
    }
    
    # Limpiador de valores
    def clean_monetary_value(value: str) -> str:
        """Limpiar valores monetarios"""
        if not value:
            return "0"
        
        import re
        cleaned = re.sub(r'[^\d.,\-]', '', str(value).strip())
        
        if ',' in cleaned and '.' in cleaned:
            # Formato: 1.234.567,89 -> 1234567.89
            parts = cleaned.split(',')
            if len(parts) == 2 and len(parts[1]) <= 2:
                integer_part = parts[0].replace('.', '')
                decimal_part = parts[1]
                cleaned = f"{integer_part}.{decimal_part}"
        elif ',' in cleaned:
            parts = cleaned.split(',')
            if len(parts) == 2 and len(parts[1]) <= 2:
                cleaned = cleaned.replace(',', '.')
            else:
                cleaned = cleaned.replace(',', '')
        
        return cleaned if cleaned else "0"
    
    custom_cleaners = {
        'valor_presupuestado': clean_monetary_value
    }
    
    return [
        create_column_mapper(column_mapping),
        create_value_cleaner(
            strip_whitespace=True,
            convert_empty_to_null=True,
            custom_cleaners=custom_cleaners
        ),
        create_row_filter(
            lambda row: any(
                str(value).strip() 
                for key, value in row.items() 
                if not key.startswith('_') and value is not None
            )
        )
    ]

def try_multiple_sheet_names(spreadsheet_id: str) -> Tuple[str, Tuple[Dict[str, Any], ...]]:
    """
    Intentar extraer datos probando múltiples nombres de hoja.
    """
    # Primero intentamos con "PAA" que sabemos que existe
    possible_names = [
        "PAA", "Hoja 1", "Hoja1", "Sheet1", "DACP", "Datos", "Principal", 
        "Consolidado", "Base", "Información", "Data"
    ]
    
    transformers = create_paa_dacp_transformers()
    
    for sheet_name in possible_names:
        try:
            logger.info(f"Intentando extraer de la hoja: '{sheet_name}'")
            
            data = extract_from_sheet_by_id(
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name,
                transformers=transformers
            )
            
            if data and len(data) > 0:
                logger.info(f"✅ Extracción exitosa de '{sheet_name}': {len(data)} registros")
                return sheet_name, data
            else:
                logger.warning(f"Hoja '{sheet_name}' está vacía")
                
        except Exception as e:
            logger.warning(f"Error con hoja '{sheet_name}': {e}")
            continue
    
    raise Exception("No se pudo extraer datos de ninguna hoja")

def extract_paa_dacp_data() -> Tuple[Dict[str, Any], ...]:
    """
    Extraer datos de PAA DACP desde Google Sheets.
    
    Returns:
        Tupla con los datos extraídos y normalizados
    """
    logger.info("Extrayendo datos de Google Sheets...")
    
    # Extraer datos probando diferentes nombres de hoja
    sheet_name, data = try_multiple_sheet_names(SPREADSHEET_ID)
    
    logger.info(f"Datos extraídos de '{sheet_name}': {len(data)} registros")
    return data

def diagnose_spreadsheet():
    """
    Diagnosticar el spreadsheet para ver qué hojas están disponibles.
    """
    from google_sheets_functional import (
        create_auth_config, 
        load_service_account_credentials, 
        create_gspread_client
    )
    
    try:
        logger.info("=== DIAGNÓSTICO DEL SPREADSHEET ===")
        
        # Cargar credenciales
        auth_config = create_auth_config()
        credentials = load_service_account_credentials(auth_config)
        client = create_gspread_client(credentials)
        
        # Abrir spreadsheet
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        logger.info(f"Spreadsheet: '{spreadsheet.title}'")
        
        # Listar hojas disponibles
        logger.info("Hojas disponibles:")
        for i, worksheet in enumerate(spreadsheet.worksheets()):
            logger.info(f"  {i+1}. '{worksheet.title}' (ID: {worksheet.id})")
            
            # Intentar obtener algunas filas para ver si tiene datos
            try:
                values = worksheet.get_all_values()
                logger.info(f"     - Filas: {len(values)}")
                if values and len(values) > 0:
                    logger.info(f"     - Primera fila: {values[0][:5]}...")  # Primeras 5 columnas
            except Exception as e:
                logger.info(f"     - Error leyendo: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error en diagnóstico: {e}")
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--diagnose":
        diagnose_spreadsheet()
    else:
        # Extraer datos para prueba
        data = extract_paa_dacp_data()
        
        # Mostrar preview
        if data:
            print("\n=== PREVIEW DE DATOS ===")
            for i, record in enumerate(data[:3]):
                print(f"Registro {i+1}:")
                for key, value in record.items():
                    if key != '_metadata':
                        print(f"  {key}: {value}")
                print()
