#!/usr/bin/env python
"""
Script para extraer datos de contratación DACP desde Google Sheets.
URL objetivo: https://docs.google.com/spreadsheets/d/1MPHJzY2fkMoINwlIHWadj_OD7Ly12z86/edit?gid=845209346#gid=845209346

Este script utiliza el módulo funcional de Google Sheets para extraer, transformar y 
convertir los datos a DataFrame de pandas.
"""

import os
import sys
import pandas as pd
import logging
from typing import Dict, Any, Tuple, List
from datetime import datetime

# Agregar el directorio actual al path para importar módulos locales
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Importar funciones de Google Sheets desde database.config
from database.config import get_sheets_client, open_spreadsheet_by_url, get_worksheet_data
import gspread

# Configuración de logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extraction_dacp.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# FUNCIONES AUXILIARES SIMPLIFICADAS
# =============================================================================

def create_column_mapper(column_mapping):
    """Función simple para mapear columnas"""
    def mapper(df):
        return df.rename(columns=column_mapping)
    return mapper

def create_value_cleaner(cleaning_rules):
    """Función simple para limpiar valores"""
    def cleaner(df):
        df_cleaned = df.copy()
        for column, cleaning_func in cleaning_rules.items():
            if column in df_cleaned.columns:
                df_cleaned[column] = df_cleaned[column].apply(cleaning_func)
        return df_cleaned
    return cleaner

def create_row_filter(filter_criteria):
    """Función simple para filtrar filas"""
    def filter_func(df):
        df_filtered = df.copy()
        for column, criteria in filter_criteria.items():
            if column in df_filtered.columns:
                if callable(criteria):
                    df_filtered = df_filtered[df_filtered[column].apply(criteria)]
                else:
                    df_filtered = df_filtered[df_filtered[column] == criteria]
        return df_filtered
    return filter_func

def create_auth_config():
    """Configuración básica de autenticación"""
    return {'service_account_file': 'sheets-service-account.json'}

def validate_auth_config(config):
    """Validar configuración de autenticación"""
    if 'service_account_file' in config:
        exists = os.path.exists(config['service_account_file'])
        return exists, "Archivo encontrado" if exists else "Archivo no encontrado"
    return False, "Configuración inválida"

# =============================================================================
# CONFIGURACIÓN DEL SPREADSHEET OBJETIVO
# =============================================================================

# Cargar configuración desde variables de entorno
from dotenv import load_dotenv
load_dotenv()

# Configuración del spreadsheet de índice de procesos
SHEETS_INDICE_PROCESOS_URL = os.getenv('SHEETS_INDICE_PROCESOS_URL')
SHEETS_INDICE_PROCESOS_WORKSHEET = os.getenv('SHEETS_INDICE_PROCESOS_WORKSHEET', 'procesos')

# Extraer ID del spreadsheet de la URL para compatibilidad con funciones existentes
import re
url_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', SHEETS_INDICE_PROCESOS_URL or '')
SPREADSHEET_ID = url_match.group(1) if url_match else "1CqIxNeD4KT1Z3dQACVc1f46OVPIZClcO11cxiWMrpVE"
SHEET_GID = ""  # No hay GID específico en este link

# Configuración de salida
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def get_sheet_name_from_gid(spreadsheet_id: str, gid: str) -> str:
    """
    Obtener el nombre de la hoja usando el GID.
    En caso de error, devuelve nombres comunes para intentar.
    """
    try:
        # Intentar obtener el nombre real de la hoja
        client = get_sheets_client()
        if not client:
            raise Exception("No se pudo obtener cliente de Google Sheets")
        
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        # Buscar la hoja por GID
        for worksheet in spreadsheet.worksheets():
            if str(worksheet.id) == gid:
                logger.info(f"Hoja encontrada por GID {gid}: '{worksheet.title}'")
                return worksheet.title
        
        logger.warning(f"No se encontró hoja con GID {gid}")
        
    except Exception as e:
        logger.warning(f"Error obteniendo nombre de hoja por GID: {e}")
    
    # Nombres comunes para intentar si no funciona el GID
    common_names = [
        "procesos", "Procesos", "PROCESOS", "Hoja1", "Sheet1", "Datos", "Data", "Contratos", "Contratación", 
        "DACP", "Base de datos", "Principal", "Main"
    ]
    
    return common_names

def try_multiple_sheet_names(spreadsheet_id: str, possible_names: List[str]) -> Tuple[str, Any]:
    """
    Intentar extraer datos probando múltiples nombres de hoja.
    """
    for sheet_name in possible_names:
        try:
            logger.info(f"Intentando extraer de la hoja: '{sheet_name}'")
            
            # Usar funciones existentes del módulo database.config
            client = get_sheets_client()
            if not client:
                raise Exception("No se pudo obtener cliente de Google Sheets")
            
            spreadsheet = client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            data = worksheet.get_all_values()
            
            if data and len(data) > 0:
                logger.info(f"✅ Extracción exitosa de '{sheet_name}': {len(data)} filas")
                return sheet_name, data
            else:
                logger.warning(f"Hoja '{sheet_name}' está vacía")
                
        except Exception as e:
            logger.warning(f"Error con hoja '{sheet_name}': {str(e)}")
            logger.debug(f"Detalle del error: {repr(e)}")
            continue
    
    raise Exception("No se pudo extraer datos de ninguna hoja")

# =============================================================================
# TRANSFORMADORES ESPECÍFICOS PARA DATOS DACP
# =============================================================================

def create_dacp_column_normalizer():
    """
    Crear transformador para normalizar nombres de columnas comunes en datos DACP.
    """
    # Mapeo común de columnas encontradas en datos de contratación
    column_mapping = {
        # Columnas de contrato
        'Número de Contrato': 'numero_contrato',
        'No. Contrato': 'numero_contrato',
        'Contrato': 'numero_contrato',
        'Contract Number': 'numero_contrato',
        
        # Columnas de contratista
        'Contratista': 'contratista',
        'Contractor': 'contratista',
        'Proveedor': 'contratista',
        'Razón Social': 'contratista',
        
        # Columnas de valor
        'Valor del Contrato': 'valor_contrato',
        'Valor': 'valor_contrato',
        'Monto': 'valor_contrato',
        'Value': 'valor_contrato',
        'Contract Value': 'valor_contrato',
        
        # Columnas de fecha
        'Fecha de Firma': 'fecha_firma',
        'Fecha Firma': 'fecha_firma',
        'Date': 'fecha_firma',
        'Fecha de Inicio': 'fecha_inicio',
        'Fecha Inicio': 'fecha_inicio',
        'Start Date': 'fecha_inicio',
        'Fecha de Terminación': 'fecha_terminacion',
        'Fecha Fin': 'fecha_terminacion',
        'End Date': 'fecha_terminacion',
        
        # Columnas de objeto/descripción
        'Objeto del Contrato': 'objeto_contrato',
        'Objeto': 'objeto_contrato',
        'Description': 'objeto_contrato',
        'Descripción': 'objeto_contrato',
        
        # Columnas de estado
        'Estado': 'estado',
        'Status': 'estado',
        'Estado del Contrato': 'estado',
        
        # Columnas de entidad
        'Entidad': 'entidad',
        'Entity': 'entidad',
        'Entidad Contratante': 'entidad',
        
        # Columnas de tipo
        'Tipo de Contrato': 'tipo_contrato',
        'Tipo': 'tipo_contrato',
        'Type': 'tipo_contrato',
        
        # Columnas de modalidad
        'Modalidad': 'modalidad',
        'Modalidad de Selección': 'modalidad',
        
        # Columnas de identificación
        'NIT': 'nit_contratista',
        'Identificación': 'identificacion_contratista',
        'ID': 'identificacion_contratista'
    }
    
    return create_column_mapper(column_mapping)

def create_dacp_value_cleaner():
    """
    Crear transformador para limpiar valores específicos de datos DACP.
    """
    def clean_currency_value(value: str) -> str:
        """Limpiar valores monetarios preservando valores numéricos válidos"""
        if not value:
            return None
        
        # Convertir a string y limpiar
        str_value = str(value).strip()
        
        # Verificar si es un valor vacío representado como "$ -" o similar
        if str_value in ['$  -', '$ -', '-', '', 'N/A', 'n/a', 'NULL', 'null', 'Sin información']:
            return None
        
        # Remover símbolos de moneda y espacios extra
        cleaned = str_value.replace('$', '').strip()
        
        # Si después de limpiar queda vacío o solo guiones, retornar None
        if not cleaned or cleaned in ['-', '--']:
            return None
        
        # Manejar formato colombiano: punto como separador de miles, coma como decimal
        # Ejemplos: "5.370.000", "2.392.502.160", "1.033.406.709"
        
        # Si contiene puntos y NO contiene comas, es formato colombiano con separador de miles
        if '.' in cleaned and ',' not in cleaned:
            # Formato colombiano puro: "5.370.000" -> "5370000"
            cleaned = cleaned.replace('.', '')
        elif ',' in cleaned and '.' in cleaned:
            # Formato mixto: determinar cuál es decimal basado en posición
            if cleaned.rfind(',') > cleaned.rfind('.'):
                # Coma es decimal: "1.234.567,89" -> "1234567.89"
                cleaned = cleaned.replace('.', '').replace(',', '.')
            else:
                # Punto es decimal: "1,234,567.89" -> "1234567.89"
                cleaned = cleaned.replace(',', '')
        elif ',' in cleaned:
            # Solo comas - pueden ser separadores de miles o decimales
            comma_count = cleaned.count(',')
            if comma_count == 1 and len(cleaned.split(',')[1]) <= 2:
                # Probablemente decimal: "1234,89" -> "1234.89"
                cleaned = cleaned.replace(',', '.')
            else:
                # Probablemente separadores de miles: "1,234,567" -> "1234567"
                cleaned = cleaned.replace(',', '')
        
        # Intentar convertir a float para validar
        try:
            float_value = float(cleaned)
            # Retornar como entero si es número entero, sino como float
            return str(int(float_value)) if float_value.is_integer() else str(float_value)
        except ValueError:
            logger.warning(f"No se pudo convertir valor monetario: '{value}' -> '{cleaned}'")
            return None
    
    def clean_contract_number(value: str) -> str:
        """Limpiar números de contrato"""
        if not value:
            return value
        
        # Normalizar números de contrato
        return str(value).strip().upper()
    
    custom_cleaners = {
        # Valores monetarios - usar nombres exactos de las columnas del Google Sheets
        'valor_del_proceso': clean_currency_value,
        'valor_del_contrato': clean_currency_value,
        'valor_del_contrato_ejecutado_(sap)': clean_currency_value,
        # También mapear nombres normalizados por si acaso
        'valor_proceso': clean_currency_value,
        'valor_contrato': clean_currency_value,
        'valor_contrato_ejecutado_sap': clean_currency_value,
        # Otros campos
        'numero_contrato': clean_contract_number,
        'nit_contratista': lambda x: str(x).replace('-', '').replace('.', '').strip() if x else x,
        'contratista': lambda x: str(x).title().strip() if x else x
    }
    
    return create_value_cleaner(custom_cleaners)

def create_dacp_date_transformer():
    """
    Crear transformador para normalizar fechas en datos DACP.
    """
    def transform_dates(data: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
        """Transformar campos de fecha a formato ISO"""
        result = []
        
        date_fields = ['fecha_firma', 'fecha_inicio', 'fecha_terminacion']
        
        for row in data:
            new_row = row.copy()
            
            for field in date_fields:
                if field in new_row and new_row[field]:
                    try:
                        date_value = str(new_row[field]).strip()
                        if date_value and date_value.lower() not in ['null', 'none', '', 'n/a']:
                            # Intentar múltiples formatos de fecha
                            date_formats = [
                                '%Y-%m-%d',          # 2025-01-15
                                '%d/%m/%Y',          # 15/01/2025
                                '%m/%d/%Y',          # 01/15/2025
                                '%d-%m-%Y',          # 15-01-2025
                                '%Y/%m/%d',          # 2025/01/15
                                '%d %b %Y',          # 15 Jan 2025
                                '%d de %B de %Y',    # 15 de enero de 2025
                                '%B %d, %Y',         # January 15, 2025
                            ]
                            
                            parsed_date = None
                            for fmt in date_formats:
                                try:
                                    parsed_date = datetime.strptime(date_value, fmt)
                                    break
                                except ValueError:
                                    continue
                            
                            if parsed_date:
                                new_row[field] = parsed_date.strftime('%Y-%m-%d')
                            else:
                                logger.warning(f"No se pudo parsear fecha '{date_value}' en campo '{field}'")
                                new_row[field] = date_value  # Mantener original
                        else:
                            new_row[field] = None
                            
                    except Exception as e:
                        logger.warning(f"Error transformando fecha en campo '{field}': {e}")
                        # Mantener valor original en caso de error
                        pass
            
            result.append(new_row)
        
        return tuple(result)
    
    return transform_dates

def create_dacp_value_converter():
    """
    Crear transformador para convertir tipos de datos numéricos.
    """
    def convert_values(data: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
        """Convertir valores a tipos apropiados"""
        result = []
        
        for row in data:
            new_row = row.copy()
            
            # Convertir valores monetarios
            if 'valor_contrato' in new_row and new_row['valor_contrato']:
                try:
                    valor_str = str(new_row['valor_contrato']).replace(',', '').replace('.', '').strip()
                    if valor_str and valor_str.isdigit():
                        new_row['valor_contrato'] = int(valor_str)
                    else:
                        new_row['valor_contrato'] = 0
                except (ValueError, TypeError):
                    new_row['valor_contrato'] = 0
            
            # Limpiar y validar NIT
            if 'nit_contratista' in new_row and new_row['nit_contratista']:
                try:
                    nit_value = str(new_row['nit_contratista']).replace('-', '').replace('.', '').strip()
                    if nit_value.isdigit():
                        new_row['nit_contratista'] = nit_value
                    else:
                        new_row['nit_contratista'] = None
                except (ValueError, TypeError):
                    new_row['nit_contratista'] = None
            
            result.append(new_row)
        
        return tuple(result)
    
    return convert_values

def create_servicios_profesionales_filter():
    """
    Crear filtro para excluir registros con justificación de modalidad de contratación
    relacionada con "Servicios profesionales y de apoyo a la gestión" y similares.
    """
    def filter_servicios_profesionales(data: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
        """
        Filtrar registros que contengan justificaciones de servicios profesionales.
        Excluye variaciones de:
        - "Servicios profesionales y de apoyo a la gestión"
        - "Servicios profesionales y de apoyo"
        - "Servicios profesionales"
        Y similares
        """
        result = []
        excluded_count = 0
        
        # Patrones a excluir (en minúsculas para comparación)
        exclusion_patterns = [
            "servicios profesionales y de apoyo a la gestión",
            "servicios profesionales y de apoyo a la gestion",
            "servicios profesionales y de apoyo",
            "servicios profesionales",
            "servicio profesional y de apoyo a la gestión",
            "servicio profesional y de apoyo a la gestion", 
            "servicio profesional y de apoyo",
            "servicio profesional",
            "apoyo a la gestión",
            "apoyo a la gestion",
            "consultoria",
            "consultoría"
        ]
        
        # Campos donde buscar las justificaciones (nombres posibles en los datos)
        justification_fields = [
            'justificacion_modalidad_de_contratación',
            'justificación_modalidad_de_contratación',
            'justificacion_modalidad_contratacion',
            'justificacion_modalidad',
            'modalidad_justificacion',
            'justificacion',
            'justification',
            'reason'
        ]
        
        for row in data:
            should_exclude = False
            
            # Buscar en todos los campos posibles de justificación
            for field in justification_fields:
                if field in row and row[field]:
                    justification_value = str(row[field]).lower().strip()
                    
                    # Verificar si contiene algún patrón de exclusión
                    for pattern in exclusion_patterns:
                        if pattern in justification_value:
                            should_exclude = True
                            logger.info(f"🚫 Excluyendo registro por justificación: '{row[field]}'")
                            excluded_count += 1
                            break
                    
                    if should_exclude:
                        break
            
            # Solo agregar el registro si no debe excluirse
            if not should_exclude:
                result.append(row)
        
        if excluded_count > 0:
            logger.info(f"✅ Filtro de servicios profesionales aplicado: {excluded_count} registros excluidos")
            logger.info(f"📊 Registros restantes: {len(result)}")
        else:
            logger.info("ℹ️ No se encontraron registros de servicios profesionales para excluir")
        
        return tuple(result)
    
    return filter_servicios_profesionales

# =============================================================================
# FUNCIÓN PRINCIPAL DE EXTRACCIÓN
# =============================================================================

def extract_dacp_contracting_data(
    spreadsheet_id: str = SPREADSHEET_ID,
    output_format: str = "both"  # "json", "csv", "both"
) -> pd.DataFrame:
    """
    Extraer datos de contratación DACP y convertir a DataFrame.
    
    Args:
        spreadsheet_id: ID del spreadsheet de Google
        output_format: Formato de salida ("json", "csv", "both")
    
    Returns:
        DataFrame de pandas con los datos extraídos
    """
    logger.info("=== INICIANDO EXTRACCIÓN DE DATOS DACP ===")
    logger.info(f"Spreadsheet ID: {spreadsheet_id}")
    
    try:
        # 1. Validar configuración
        auth_config = create_auth_config()
        is_valid, message = validate_auth_config(auth_config)
        
        if not is_valid:
            raise Exception(f"Configuración de autenticación inválida: {message}")
        
        logger.info("✅ Configuración de autenticación válida")
        
        # 2. Intentar obtener el nombre de la hoja
        possible_sheet_names = get_sheet_name_from_gid(spreadsheet_id, SHEET_GID)
        if isinstance(possible_sheet_names, str):
            possible_sheet_names = [possible_sheet_names]
        
        logger.info(f"Intentando con nombres de hoja: {possible_sheet_names}")
        
        # 3. Extraer datos probando diferentes nombres de hoja
        sheet_name, raw_data = try_multiple_sheet_names(spreadsheet_id, possible_sheet_names)
        
        logger.info(f"✅ Datos extraídos exitosamente de la hoja: '{sheet_name}'")
        logger.info(f"Total de filas brutas: {len(raw_data)}")
        
        # 4. Convertir datos a DataFrame para procesamiento básico
        logger.info("Procesando datos...")
        
        # Convertir a DataFrame si hay datos
        if raw_data and len(raw_data) > 1:
            import pandas as pd
            df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
            logger.info(f"DataFrame creado: {len(df)} filas, {len(df.columns)} columnas")
            return df
        else:
            logger.warning("No se encontraron datos válidos")
            import pandas as pd
            return pd.DataFrame()
        
        # 6. Mostrar preview
        print("\n" + "="*60)
        print("PREVIEW DE DATOS EXTRAÍDOS")
        print("="*60)
        print(f"Forma: {df.shape}")
        print(f"Columnas: {list(df.columns)}")
        print("\nPrimeras 5 filas:")
        print(df.head())
        print("\nInformación del DataFrame:")
        print(df.info())
        
        # 7. Guardar archivos de salida
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        files_created = []
        
        if output_format in ["json", "both"]:
            json_path = os.path.join(OUTPUT_DIR, f"dacp_contracting_data_{TIMESTAMP}.json")
            # Convertir tipos numpy a tipos Python nativos para serialización JSON
            df_json = df.copy()
            for col in df_json.select_dtypes(include=['int64', 'float64']).columns:
                df_json[col] = df_json[col].astype(str)
            df_json.to_json(json_path, orient='records', indent=2, force_ascii=False)
            files_created.append(json_path)
            logger.info(f"✅ Archivo JSON guardado: {json_path}")
        
        if output_format in ["csv", "both"]:
            csv_path = os.path.join(OUTPUT_DIR, f"dacp_contracting_data_{TIMESTAMP}.csv")
            df.to_csv(csv_path, index=False, encoding='utf-8')
            files_created.append(csv_path)
            logger.info(f"✅ Archivo CSV guardado: {csv_path}")
        
        # 8. Guardar metadatos
        metadata = {
            'extraction_info': {
                'timestamp': datetime.now().isoformat(),
                'spreadsheet_id': spreadsheet_id,
                'sheet_name': sheet_name,
                'total_rows': int(len(df)),
                'total_columns': int(len(df.columns)),
                'columns': list(df.columns)
            },
            'data_quality': {
                'non_null_rows': int(len(df.dropna())),
                'duplicate_rows': int(df.duplicated().sum()),
                'memory_usage': int(df.memory_usage(deep=True).sum())
            }
        }
        
        metadata_path = os.path.join(OUTPUT_DIR, f"dacp_metadata_{TIMESTAMP}.json")
        with open(metadata_path, 'w', encoding='utf-8') as f:
            import json
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        files_created.append(metadata_path)
        logger.info(f"✅ Metadatos guardados: {metadata_path}")
        
        print(f"\n📁 Archivos creados:")
        for file_path in files_created:
            print(f"  - {file_path}")
        
        logger.info("=== EXTRACCIÓN COMPLETADA EXITOSAMENTE ===")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Error durante la extracción: {e}")
        print(f"\n❌ Error: {e}")
        raise

# =============================================================================
# FUNCIONES DE DIAGNÓSTICO Y CONFIGURACIÓN
# =============================================================================

def diagnose_access_issues(spreadsheet_id: str = SPREADSHEET_ID):
    """
    Diagnosticar problemas de acceso al spreadsheet.
    """
    print("🔍 DIAGNÓSTICO DE ACCESO A GOOGLE SHEETS")
    print("="*50)
    
    try:
        # 1. Verificar archivo de credenciales
        auth_config = create_auth_config()
        print(f"1. Archivo de credenciales: {auth_config.service_account_file}")
        
        if os.path.exists(auth_config.service_account_file):
            print("   ✅ Archivo encontrado")
        else:
            print("   ❌ Archivo NO encontrado")
            return
        
        # 2. Validar credenciales
        is_valid, message = validate_auth_config(auth_config)
        print(f"2. Validación de credenciales: {message}")
        
        if not is_valid:
            print("   ❌ Credenciales inválidas")
            return
        
        # 3. Obtener email del service account
        import json
        with open(auth_config.service_account_file, 'r') as f:
            creds_data = json.load(f)
            service_email = creds_data.get('client_email')
            print(f"3. Email del service account: {service_email}")
        
        # 4. Intentar acceso al spreadsheet
        print("4. Intentando acceso al spreadsheet...")
        
        try:
            client = get_sheets_client()
            if not client:
                raise Exception("No se pudo obtener cliente de Google Sheets")
            
            spreadsheet = client.open_by_key(spreadsheet_id)
            
            print(f"   ✅ Spreadsheet accesible: '{spreadsheet.title}'")
            
            # 5. Listar hojas disponibles
            print("5. Hojas disponibles:")
            for i, worksheet in enumerate(spreadsheet.worksheets()):
                print(f"   {i+1}. '{worksheet.title}' (ID: {worksheet.id})")
                
        except Exception as e:
            print(f"   ❌ Error accediendo al spreadsheet: {e}")
            print("\n🔧 SOLUCIONES POSIBLES:")
            print("   1. Compartir el spreadsheet con el service account:")
            print(f"      - Abrir: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
            print(f"      - Hacer clic en 'Compartir'")
            print(f"      - Agregar email: {service_email}")
            print(f"      - Dar permisos de 'Lector' o 'Editor'")
            print("   2. Verificar que el spreadsheet sea accesible")
            print("   3. Verificar conexión a internet")
            
    except Exception as e:
        print(f"❌ Error en diagnóstico: {e}")

def setup_service_account_guide():
    """
    Mostrar guía para configurar service account.
    """
    print("📋 GUÍA DE CONFIGURACIÓN DE SERVICE ACCOUNT")
    print("="*50)
    
    print("\n1. CREAR PROYECTO EN GOOGLE CLOUD CONSOLE:")
    print("   - Ir a: https://console.cloud.google.com/")
    print("   - Crear nuevo proyecto o seleccionar existente")
    
    print("\n2. HABILITAR APIs:")
    print("   - Google Sheets API")
    print("   - Google Drive API")
    
    print("\n3. CREAR SERVICE ACCOUNT:")
    print("   - Ir a: IAM & Admin > Service Accounts")
    print("   - Hacer clic en 'Create Service Account'")
    print("   - Completar nombre y descripción")
    
    print("\n4. GENERAR CLAVE:")
    print("   - Hacer clic en el service account creado")
    print("   - Ir a 'Keys' > 'Add Key' > 'Create New Key'")
    print("   - Seleccionar formato JSON")
    print("   - Guardar como 'sheet-secrets.json' en extraction_app/")
    
    print("\n5. COMPARTIR SPREADSHEET:")
    print("   - Abrir el Google Sheet")
    print("   - Hacer clic en 'Compartir'")
    print("   - Agregar el email del service account (client_email del JSON)")
    print("   - Dar permisos de 'Lector'")
    
    print("\n6. INSTALAR DEPENDENCIAS:")
    print("   pip install -r extraction_app/requirements_sheets.txt")
    
    print("\n7. PROBAR ACCESO:")
    print("   python data_extraction_contracting_dacp_sheets.py --diagnose")

# =============================================================================
# FUNCIÓN PRINCIPAL Y CLI
# =============================================================================

def main():
    """Función principal con opciones de línea de comandos."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extraer datos de contratación DACP desde Google Sheets')
    parser.add_argument('--diagnose', action='store_true', help='Diagnosticar problemas de acceso')
    parser.add_argument('--setup-guide', action='store_true', help='Mostrar guía de configuración')
    parser.add_argument('--format', choices=['json', 'csv', 'both'], default='both', 
                       help='Formato de salida (default: both)')
    parser.add_argument('--spreadsheet-id', default=SPREADSHEET_ID,
                       help='ID del spreadsheet (default: configurado)')
    
    args = parser.parse_args()
    
    if args.setup_guide:
        setup_service_account_guide()
        return
    
    if args.diagnose:
        diagnose_access_issues(args.spreadsheet_id)
        return
    
    try:
        # Extracción principal
        df = extract_dacp_contracting_data(
            spreadsheet_id=args.spreadsheet_id,
            output_format=args.format
        )
        
        if not df.empty:
            print(f"\n🎉 ¡Extracción exitosa!")
            print(f"DataFrame con {df.shape[0]} filas y {df.shape[1]} columnas")
            return df
        else:
            print("⚠️ No se obtuvieron datos")
            
    except Exception as e:
        print(f"❌ Error durante la extracción: {e}")
        print("\n💡 Sugerencias:")
        print("1. Ejecutar diagnóstico: python script.py --diagnose")
        print("2. Ver guía de configuración: python script.py --setup-guide")
        print("3. Verificar que el spreadsheet esté compartido con el service account")

if __name__ == "__main__":
    main()
