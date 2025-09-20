#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de transformación para datos de contratación DACP.
Aplica estándares mínimos de transformación a los datos extraídos.

Autor: Sistema ETL - Proyectos Cali Alcaldía
Fecha: 2025-09-19
"""

import pandas as pd
import json
import os
import re
import sys
from datetime import datetime
from tqdm import tqdm
import logging
from typing import Dict, List, Any, Optional

# Configuración de encoding UTF-8
import locale
try:
    # Configurar locale para UTF-8
    if sys.platform.startswith('win'):
        locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
    else:
        locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
except:
    # Si falla, usar configuración por defecto
    pass

# Configurar pandas para manejar UTF-8 correctamente
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.unicode.ambiguous_as_wide', True)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transformation_dacp.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configurar encoding para Windows console
import sys
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

# =============================================================================
# CONFIGURACIÓN DE RUTAS Y ARCHIVOS
# =============================================================================

# Rutas de entrada (archivos extraídos)
EXTRACTION_INPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "extraction_app", "outputs")

# Rutas de salida
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "app_outputs", "contratacion_dacp")
OUTPUT_FILE = "procesos_contratacion_dacp.json"

# Crear directorios si no existen
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# FUNCIONES FUNCIONALES PARA FILTRADO Y TRANSFORMACIÓN DE DATOS
# =============================================================================

def filter_by_categoria(records: List[Dict[str, Any]], exclude_categoria: str) -> List[Dict[str, Any]]:
    """
    Función funcional pura para filtrar registros excluyendo una categoría específica
    
    Args:
        records: Lista de diccionarios con los datos
        exclude_categoria: Categoría a excluir
    
    Returns:
        Lista filtrada sin los registros de la categoría especificada
    """
    return [record for record in records if record.get('categoria', '') != exclude_categoria]


def filter_by_plataforma(records: List[Dict[str, Any]], target_plataforma: str) -> List[Dict[str, Any]]:
    """
    Función funcional pura para filtrar registros por plataforma específica
    
    Args:
        records: Lista de diccionarios con los datos
        target_plataforma: Plataforma objetivo a filtrar
    
    Returns:
        Lista filtrada con solo los registros de la plataforma especificada
    """
    return [record for record in records if record.get('plataforma', '') == target_plataforma]


def remove_field_from_record(record: Dict[str, Any], field_name: str) -> Dict[str, Any]:
    """
    Función funcional pura para remover un campo específico de un registro
    
    Args:
        record: Diccionario con el registro
        field_name: Nombre del campo a remover
    
    Returns:
        Nuevo diccionario sin el campo especificado
    """
    return {key: value for key, value in record.items() if key != field_name}


def remove_field_from_records(records: List[Dict[str, Any]], field_name: str) -> List[Dict[str, Any]]:
    """
    Función funcional para remover un campo de todos los registros
    
    Args:
        records: Lista de diccionarios con los datos
        field_name: Nombre del campo a remover
    
    Returns:
        Lista de registros sin el campo especificado
    """
    return [remove_field_from_record(record, field_name) for record in records]


def apply_data_filters_functional(records: List[Dict[str, Any]]) -> tuple:
    """
    Aplica todos los filtros funcionales y divide los datos según los requerimientos
    
    Args:
        records: Lista de registros original
    
    Returns:
        Tupla con (contratos_secop2, ordenes_compra_tvec)
    """
    logger.info("🔄 Aplicando filtros funcionales a los datos...")
    
    # 1. Eliminar registros con categoría "Servicios profesionales y de apoyo"
    logger.info("📋 Filtrando categoría 'Servicios profesionales y de apoyo'...")
    initial_count = len(records)
    filtered_records = filter_by_categoria(records, "Servicios profesionales y de apoyo")
    removed_count = initial_count - len(filtered_records)
    logger.info(f"✅ Eliminados {removed_count} registros de servicios profesionales")
    
    # 2. Remover campo "no" de todos los registros
    logger.info("🗑️ Removiendo campo 'no' de todos los registros...")
    records_without_no = remove_field_from_records(filtered_records, "no")
    logger.info("✅ Campo 'no' removido de todos los registros")
    
    # 3. Separar por plataforma
    logger.info("🔀 Separando datos por plataforma...")
    
    # SECOP II para contratos_dacp.json
    secop2_records = filter_by_plataforma(records_without_no, "SECOP II")
    logger.info(f"📊 SECOP II: {len(secop2_records)} registros")
    
    # TVEC para ordenes_compra_dacp.json
    tvec_records = filter_by_plataforma(records_without_no, "TVEC")
    logger.info(f"📊 TVEC: {len(tvec_records)} registros")
    
    # Verificar otros tipos de plataforma
    other_platforms = set()
    for record in records_without_no:
        platform = record.get('plataforma', '')
        if platform not in ['SECOP II', 'TVEC'] and platform:
            other_platforms.add(platform)
    
    if other_platforms:
        logger.warning(f"⚠️ Encontradas otras plataformas: {list(other_platforms)}")
        for platform in other_platforms:
            count = len(filter_by_plataforma(records_without_no, platform))
            logger.warning(f"   • {platform}: {count} registros")
    
    return secop2_records, tvec_records


def save_json_data(data: List[Dict[str, Any]], output_path: str, file_description: str) -> bool:
    """
    Función funcional para guardar datos en formato JSON con soporte completo para UTF-8
    
    Args:
        data: Lista de registros a guardar
        output_path: Ruta del archivo de salida
        file_description: Descripción del archivo para metadata
    
    Returns:
        True si se guardó exitosamente
    """
    try:
        # Estructura del archivo JSON
        output_data = {
            'metadata': {
                'nombre_archivo': os.path.basename(output_path),
                'descripcion': file_description,
                'fecha_generacion': datetime.now().isoformat(),
                'version': '1.0',
                'fuente': 'DACP - Datos Abiertos Contratación Pública',
                'total_registros': len(data),
                'filtros_aplicados': [
                    'Excluidos registros con categoría "Servicios profesionales y de apoyo"',
                    'Removido campo "no" de todos los registros',
                    'Filtrado por plataforma específica'
                ]
            },
            'datos': data
        }
        
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Guardar archivo JSON con encoding UTF-8 explícito y ensure_ascii=False
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, separators=(',', ': '))
        
        logger.info(f"✅ Archivo guardado: {output_path} ({len(data)} registros)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error guardando archivo {output_path}: {e}")
        return False


# =============================================================================
# FUNCIONES DE LIMPIEZA DE DATOS SEGÚN ESTÁNDARES DEL PROYECTO
# =============================================================================

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia nombres de columnas según las reglas especificadas:
    - Convertir a minúsculas
    - Cambiar espacios por "_"
    - Eliminar conectores: por, para, de, las, los, con, etc.
    """
    logger.info("🔄 Limpiando nombres de columnas...")
    
    # Lista de conectores a eliminar
    conectores = ['de', 'del', 'la', 'las', 'el', 'los', 'con', 'para', 'por', 'en', 'a', 'y', 'o', 'un', 'una']
    
    new_columns = []
    for col in df.columns:
        # Convertir a minúsculas
        new_col = col.lower()
        
        # Reemplazar espacios por guiones bajos
        new_col = new_col.replace(' ', '_')
        
        # Dividir por guiones bajos para procesar palabra por palabra
        words = new_col.split('_')
        
        # Filtrar conectores y palabras vacías
        filtered_words = []
        for word in words:
            word = word.strip()
            if word and word not in conectores:
                filtered_words.append(word)
        
        # Unir palabras filtradas
        new_col = '_'.join(filtered_words)
        
        # Limpiar caracteres especiales (mantener solo letras, números y guiones bajos)
        new_col = re.sub(r'[^\w]', '_', new_col)
        
        # Eliminar guiones bajos múltiples
        new_col = re.sub(r'_+', '_', new_col)
        
        # Eliminar guiones bajos al inicio y final
        new_col = new_col.strip('_')
        
        new_columns.append(new_col)
    
    df.columns = new_columns
    
    # Aplicar renombrados específicos según requerimientos del usuario
    specific_renames = {
        'número_proceso': 'referencia_proceso',
        'numero_proceso': 'referencia_proceso',
        'numero_contrato': 'referencia_contrato',
        'número_contrato': 'referencia_contrato',
        'nombre_organismo': 'nombre_centro_gestor'
    }
    
    # Aplicar renombrados específicos si las columnas existen
    columns_renamed = []
    for old_name, new_name in specific_renames.items():
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})
            columns_renamed.append(f"{old_name} → {new_name}")
    
    if columns_renamed:
        logger.info(f"🔄 Renombrados específicos aplicados: {', '.join(columns_renamed)}")
    
    logger.info(f"✅ Nombres de columnas limpiados: {len(df.columns)} columnas")
    return df

def protect_urls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Protege las URLs en columnas específicas durante el procesamiento
    """
    logger.info("🔄 Protegiendo URLs de corrupción...")
    
    # Identificar columnas que contienen URLs
    url_columns = [col for col in df.columns if 'url' in col.lower() or 'enlace' in col.lower()]
    
    if url_columns:
        logger.info(f"🔗 Columnas con URLs encontradas: {url_columns}")
        
        for col in url_columns:
            # Asegurar que las URLs se mantengan como strings sin procesamiento adicional
            df[col] = df[col].astype(str)
            
            # Reemplazar valores nan/None con string vacío para evitar problemas
            df[col] = df[col].replace(['nan', 'None', 'NaN'], '')
            
            logger.info(f"  ✅ {col}: URLs protegidas")
    else:
        logger.info("ℹ️ No se encontraron columnas con URLs")
    
    return df

def remove_unwanted_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina columnas específicas que no son necesarias para DACP
    """
    logger.info("🔄 Eliminando columnas innecesarias...")
    
    # Lista de columnas a eliminar (patrones comunes en datos administrativos)
    columns_to_remove = [
        'nit_entidad', 
        'departamento', 
        'ciudad', 
        'localización', 
        'orden', 
        'rama', 
        'condiciones_entrega',
        'column_1',  # Columnas autogeneradas
        'column_2',
        'unnamed',   # Columnas sin nombre
        'index'      # Índices
    ]
    
    # Verificar qué columnas existen realmente en el DataFrame
    existing_columns_to_remove = []
    for pattern in columns_to_remove:
        # Buscar columnas que coincidan exactamente o contengan el patrón
        matching_cols = [col for col in df.columns if 
                        col == pattern or 
                        pattern in col.lower() or
                        col.lower().startswith('unnamed')]
        existing_columns_to_remove.extend(matching_cols)
    
    # Remover duplicados
    existing_columns_to_remove = list(set(existing_columns_to_remove))
    
    if existing_columns_to_remove:
        df = df.drop(columns=existing_columns_to_remove)
        logger.info(f"✅ Eliminadas {len(existing_columns_to_remove)} columnas: {existing_columns_to_remove}")
    else:
        logger.info("ℹ️ No se encontraron columnas innecesarias para eliminar")
    
    return df

def standardize_date_formats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza formatos de fecha a YYYY-MM-DD
    """
    logger.info("🔄 Estandarizando formatos de fecha...")
    
    # Identificar columnas de fecha
    date_columns = [col for col in df.columns if any(keyword in col.lower() for keyword in 
                   ['fecha', 'date', 'inicio', 'fin', 'terminacion', 'firma', 'vencimiento'])]
    
    if not date_columns:
        logger.info("ℹ️ No se encontraron columnas de fecha")
        return df
    
    logger.info(f"📅 Columnas de fecha encontradas: {date_columns}")
    
    for col in date_columns:
        logger.info(f"Procesando columna de fecha: {col}")
        
        # Crear una nueva serie para almacenar las fechas procesadas
        processed_dates = []
        
        for idx, value in enumerate(df[col]):
            if pd.isna(value) or str(value).strip() == '' or str(value).lower() in ['none', 'null', 'nan']:
                processed_dates.append(None)
                continue
            
            date_str = str(value).strip()
            parsed_date = None
            
            # Intentar diferentes formatos de fecha
            date_formats = [
                '%Y-%m-%d',          # 2025-01-15
                '%d/%m/%Y',          # 15/01/2025
                '%m/%d/%Y',          # 01/15/2025
                '%d-%m-%Y',          # 15-01-2025
                '%Y/%m/%d',          # 2025/01/15
                '%d %b %Y',          # 15 Jan 2025
                '%d de %B de %Y',    # 15 de enero de 2025
                '%B %d, %Y',         # January 15, 2025
                '%Y-%m-%dT%H:%M:%S', # 2025-01-15T00:00:00
                '%Y-%m-%d %H:%M:%S', # 2025-01-15 00:00:00
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue
            
            if parsed_date:
                processed_dates.append(parsed_date.strftime('%Y-%m-%d'))
            else:
                # Si no se puede parsear, mantener el valor original
                processed_dates.append(date_str)
                if idx < 5:  # Solo mostrar los primeros 5 errores
                    logger.warning(f"No se pudo parsear fecha '{date_str}' en columna '{col}'")
        
        df[col] = processed_dates
        logger.info(f"  ✅ {col}: Fechas estandarizadas")
    
    return df

# Funciones funcionales para procesamiento de valores monetarios

def clean_currency_string(value_str: str) -> str:
    """
    Función pura para limpiar una cadena de valor monetario
    """
    if not value_str or pd.isna(value_str):
        return "0"
    
    # Convertir a string y limpiar
    cleaned = str(value_str).strip()
    
    # Remover símbolos de moneda, espacios y caracteres especiales
    # Mantener solo dígitos, puntos y comas
    cleaned = re.sub(r'[^\d.,]', '', cleaned)
    
    # Si está vacío después de limpiar, retornar 0
    if not cleaned:
        return "0"
    
    return cleaned


def normalize_decimal_separators(value_str: str) -> str:
    """
    Función pura para normalizar separadores decimales
    """
    if not value_str:
        return "0"
    
    # Manejar separadores decimales
    if ',' in value_str and '.' in value_str:
        # Si tiene ambos separadores, el último es decimal
        last_comma = value_str.rfind(',')
        last_dot = value_str.rfind('.')
        
        if last_comma > last_dot:
            # Coma es decimal, punto es miles
            value_str = value_str.replace('.', '').replace(',', '.')
        else:
            # Punto es decimal, coma es miles
            value_str = value_str.replace(',', '')
    elif ',' in value_str:
        # Solo comas
        comma_count = value_str.count(',')
        if comma_count == 1:
            # Si solo hay una coma y después hay 1-2 dígitos, es decimal
            parts = value_str.split(',')
            if len(parts) == 2 and len(parts[1]) <= 2:
                value_str = value_str.replace(',', '.')
            else:
                # Es separador de miles
                value_str = value_str.replace(',', '')
        else:
            # Múltiples comas, son separadores de miles
            value_str = value_str.replace(',', '')
    
    return value_str


def convert_to_integer(value_str: str) -> int:
    """
    Función pura para convertir string limpio a entero
    """
    try:
        if not value_str:
            return 0
        
        # Convertir a float primero para manejar decimales
        float_value = float(value_str)
        
        # Convertir a entero (truncar decimales)
        return int(float_value)
    except (ValueError, TypeError):
        return 0


def process_currency_value(value) -> int:
    """
    Función de composición para procesar un valor monetario completo
    """
    # Componer las funciones puras
    return convert_to_integer(
        normalize_decimal_separators(
            clean_currency_string(value)
        )
    )


def get_valor_columns(df: pd.DataFrame) -> list:
    """
    Función pura para identificar columnas que empiezan por 'valor_'
    """
    return [col for col in df.columns if col.lower().startswith('valor_')]


def apply_currency_transformation(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Función funcional para aplicar transformación de moneda a columnas específicas
    """
    df_copy = df.copy()
    
    for col in columns:
        logger.info(f"Procesando columna monetaria: {col}")
        
        # Aplicar transformación funcional a toda la columna
        df_copy[col] = df_copy[col].apply(process_currency_value)
        
        logger.info(f"  ✅ {col}: Valores convertidos a enteros")
    
    return df_copy


def standardize_currency_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza valores monetarios eliminando símbolos y convirtiendo a enteros
    Enfoque específico para columnas que empiezan por 'valor_'
    """
    logger.info("🔄 Estandarizando valores monetarios...")
    
    # Identificar columnas que empiezan por 'valor_'
    valor_columns = get_valor_columns(df)
    
    if not valor_columns:
        logger.info("ℹ️ No se encontraron columnas que empiecen por 'valor_'")
        return df
    
    logger.info(f"💰 Columnas de valor encontradas: {valor_columns}")
    
    # Aplicar transformación funcional
    df_transformed = apply_currency_transformation(df, valor_columns)
    
    return df_transformed

def clean_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia campos de texto eliminando espacios extra y caracteres especiales
    """
    logger.info("🔄 Limpiando campos de texto...")
    
    # Identificar columnas de texto
    text_columns = df.select_dtypes(include=['object']).columns.tolist()
    
    # Excluir columnas que ya fueron procesadas
    excluded_keywords = ['fecha', 'date', 'valor', 'precio', 'monto', 'url', 'enlace']
    text_columns = [col for col in text_columns if not any(keyword in col.lower() for keyword in excluded_keywords)]
    
    if not text_columns:
        logger.info("ℹ️ No se encontraron columnas de texto para limpiar")
        return df
    
    logger.info(f"📝 Limpiando {len(text_columns)} columnas de texto")
    
    for col in text_columns:
        try:
            # Limpiar espacios extra y caracteres especiales
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True)  # Múltiples espacios a uno
            df[col] = df[col].str.replace(r'[\r\n\t]', ' ', regex=True)  # Saltos de línea y tabs
            
            # Reemplazar valores vacíos con None
            df[col] = df[col].replace(['', 'nan', 'None', 'NaN', 'null'], None)
        except Exception as e:
            logger.warning(f"Error procesando columna {col}: {e}")
            continue
    
    logger.info("✅ Campos de texto limpiados")
    return df

def remove_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas completamente vacías o con datos irrelevantes
    """
    logger.info("🔄 Eliminando filas vacías...")
    
    initial_count = len(df)
    
    # Eliminar filas completamente vacías
    df = df.dropna(how='all')
    
    # Eliminar filas donde todas las columnas de texto están vacías
    text_columns = df.select_dtypes(include=['object']).columns
    if len(text_columns) > 0:
        df = df[~df[text_columns].isin(['', None, 'nan', 'None']).all(axis=1)]
    
    final_count = len(df)
    removed_count = initial_count - final_count
    
    if removed_count > 0:
        logger.info(f"✅ Eliminadas {removed_count} filas vacías ({final_count} filas restantes)")
    else:
        logger.info("ℹ️ No se encontraron filas vacías para eliminar")
    
    return df

def add_metadata(df: pd.DataFrame, source_info: Dict[str, Any]) -> pd.DataFrame:
    """
    Agrega metadatos a cada registro
    """
    logger.info("🔄 Agregando metadatos...")
    
    # Agregar metadatos comunes
    df['_fecha_transformacion'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['_fuente_datos'] = 'DACP - Datos Abiertos Contratación Pública'
    df['_version_transformacion'] = '1.0'
    df['_total_registros'] = len(df)
    
    # Agregar información de la fuente si está disponible
    if source_info:
        for key, value in source_info.items():
            df[f'_fuente_{key}'] = value
    
    logger.info("✅ Metadatos agregados")
    return df

def normalize_centro_gestor(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza los valores de nombre_centro_gestor usando mapeo por palabras clave
    """
    logger.info("🔄 Normalizando nombres de centros gestores...")
    
    # Lista oficial de centros gestores
    centros_oficiales = [
        "Secretaría de Gobierno",
        "Departamento Administrativo de Gestión Jurídica Pública",
        "Departamento Administrativo de Control Interno",
        "Departamento Administrativo de Control Disciplinario Interno de Instrucción",
        "Departamento Administrativo de Hacienda",
        "Departamento Administrativo de Planeación",
        "Departamento Administrativo de Gestión del Medio Ambiente",
        "Departamento Administrativo de Tecnologías de la Información y las Comunicaciones",
        "Departamento Administrativo de Contratación Pública",
        "Departamento Administrativo de Desarrollo e Innovación Institucional",
        "Secretaría de Educación",
        "Secretaría de Salud Pública",
        "Secretaría de Bienestar Social",
        "Secretaría de Vivienda Social y Hábitat",
        "Secretaría de Cultura",
        "Secretaría de Infraestructura",
        "Secretaría de Movilidad",
        "Secretaría de Seguridad y Justicia",
        "Secretaría del Deporte y la Recreación",
        "Secretaría de Gestión del Riesgo de Emergencias y Desastres",
        "Secretaría de Paz y Cultura Ciudadana",
        "Secretaría de Desarrollo Económico",
        "Secretaría de Turismo",
        "Secretaría de Desarrollo Territorial y Participación Ciudadana",
        "Unidad Administrativa Especial de Gestión de Bienes y Servicios",
        "Unidad Administrativa Especial de Servicios Públicos",
        "Unidad Administrativa Especial de Protección Animal"
    ]
    
    # Función para crear palabras clave de un texto
    def extraer_palabras_clave(texto):
        if not isinstance(texto, str):
            return set()
        # Convertir a minúsculas y extraer palabras significativas
        palabras = texto.lower().replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
        # Eliminar conectores comunes
        conectores = {'de', 'del', 'la', 'las', 'el', 'los', 'y', 'para', 'por', 'con', 'en', 'a', 'un', 'una'}
        palabras_filtradas = [p.strip() for p in palabras.split() if p.strip() and p.strip() not in conectores and len(p.strip()) > 2]
        return set(palabras_filtradas)
    
    # Función para calcular similitud entre textos
    def calcular_similitud(texto1, texto2):
        palabras1 = extraer_palabras_clave(texto1)
        palabras2 = extraer_palabras_clave(texto2)
        
        if not palabras1 or not palabras2:
            return 0
        
        interseccion = len(palabras1.intersection(palabras2))
        union = len(palabras1.union(palabras2))
        
        return interseccion / union if union > 0 else 0
    
    # Función para encontrar la mejor coincidencia
    def encontrar_mejor_coincidencia(texto_original):
        if not isinstance(texto_original, str) or not texto_original.strip():
            return texto_original
        
        mejor_coincidencia = texto_original
        mejor_similitud = 0
        
        for centro_oficial in centros_oficiales:
            similitud = calcular_similitud(texto_original, centro_oficial)
            
            # Si encontramos una similitud alta, usar el centro oficial
            if similitud > mejor_similitud and similitud >= 0.3:  # Umbral del 30%
                mejor_similitud = similitud
                mejor_coincidencia = centro_oficial
        
        return mejor_coincidencia
    
    # Aplicar normalización si existe la columna
    if 'nombre_centro_gestor' in df.columns:
        logger.info(f"📊 Normalizando {df['nombre_centro_gestor'].nunique()} valores únicos...")
        
        # Aplicar la normalización
        df['nombre_centro_gestor'] = df['nombre_centro_gestor'].apply(encontrar_mejor_coincidencia)
        
        valores_normalizados = df['nombre_centro_gestor'].nunique()
        logger.info(f"✅ Normalización completada: {valores_normalizados} valores únicos finales")
        
        # Mostrar algunos ejemplos de normalización
        valores_unicos = df['nombre_centro_gestor'].value_counts().head(5)
        logger.info("📋 Principales centros gestores:")
        for centro, count in valores_unicos.items():
            logger.info(f"  • {centro}: {count} registros")
    else:
        logger.warning("⚠️ Columna 'nombre_centro_gestor' no encontrada")
    
    return df

# =============================================================================
# FUNCIÓN PRINCIPAL DE TRANSFORMACIÓN
# =============================================================================

def transform_dacp_data(input_file_path: str, output_path: str) -> bool:
    """
    Aplica todas las transformaciones estándar a los datos DACP con filtrado funcional
    
    Args:
        input_file_path: Ruta del archivo de entrada
        output_path: Ruta del archivo de salida (para contratos SECOP II)
    
    Returns:
        bool: True si la transformación fue exitosa
    """
    try:
        logger.info("="*60)
        logger.info("INICIANDO TRANSFORMACIÓN DE DATOS DACP CON FILTRADO FUNCIONAL")
        logger.info("="*60)
        logger.info(f"Archivo de entrada: {input_file_path}")
        logger.info(f"Archivo de salida principal: {output_path}")
        
        # 1. Cargar datos
        logger.info("📂 Cargando datos de entrada...")
        
        if input_file_path.endswith('.json'):
            # Manejar archivos JSON grandes cargando solo los datos con UTF-8
            with open(input_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extraer la sección de datos si existe estructura con metadata
            if isinstance(data, dict) and 'datos' in data:
                records = data['datos']
            else:
                records = data if isinstance(data, list) else [data]
            
            # Convertir a DataFrame para aplicar transformaciones estándar
            df = pd.DataFrame(records)
            
        elif input_file_path.endswith('.csv'):
            df = pd.read_csv(input_file_path, encoding='utf-8')
        else:
            raise ValueError("Formato de archivo no soportado. Use .json o .csv")
        
        logger.info(f"✅ Datos cargados: {df.shape[0]} filas x {df.shape[1]} columnas")
        
        if df.empty:
            logger.warning("⚠️ El archivo de entrada está vacío")
            return False
        
        # Información de la fuente
        source_info = {
            'archivo_original': os.path.basename(input_file_path),
            'fecha_extraccion': datetime.now().strftime('%Y-%m-%d'),
            'registros_originales': len(df)
        }
        
        # 2. Aplicar transformaciones estándar primero
        logger.info("🔄 Aplicando transformaciones estándar...")
        
        # Pipeline de transformación estándar
        df = clean_column_names(df)
        df = protect_urls(df)
        df = remove_unwanted_columns(df)
        df = standardize_date_formats(df)
        df = standardize_currency_values(df)
        df = normalize_centro_gestor(df)
        df = clean_text_fields(df)
        df = remove_empty_rows(df)
        df = add_metadata(df, source_info)
        
        # 3. Convertir DataFrame a lista de diccionarios para aplicar filtros funcionales
        logger.info("� Convirtiendo datos para aplicar filtros funcionales...")
        df_json = df.fillna('')  # Reemplazar NaN con string vacío para JSON
        records = df_json.to_dict('records')
        
        # 4. Aplicar filtros funcionales y dividir datos
        logger.info("🔄 Aplicando filtros funcionales...")
        secop2_records, tvec_records = apply_data_filters_functional(records)
        
        # 5. Guardar archivo principal de procesos SECOP II (todos los registros)
        logger.info("💾 Guardando archivo de procesos de contratación SECOP II...")
        secop2_success = save_json_data(
            secop2_records, 
            output_path, 
            'Datos de procesos de contratación DACP transformados - Solo SECOP II'
        )
        
        # 6. Guardar archivo de órdenes de compra (TVEC)
        tvec_output_dir = os.path.join(os.path.dirname(__file__), "app_outputs", "ordenes_compra_dacp")
        tvec_output_path = os.path.join(tvec_output_dir, "ordenes_compra_dacp.json")
        
        logger.info("💾 Guardando archivo de órdenes de compra TVEC...")
        tvec_success = save_json_data(
            tvec_records, 
            tvec_output_path, 
            'Datos de órdenes de compra DACP transformados - Solo TVEC'
        )
        
        # 7. Generar estadísticas finales
        logger.info("📊 Generando estadísticas finales...")
        
        total_records = len(secop2_records) + len(tvec_records)
        stats = {
            'registros_procesados_total': total_records,
            'registros_secop2': len(secop2_records),
            'registros_tvec': len(tvec_records),
            'columnas_finales': len(df.columns) if not df.empty else 0,
            'fecha_transformacion': datetime.now().isoformat(),
            'archivos_generados': [
                os.path.basename(output_path),
                'ordenes_compra_dacp.json'
            ]
        }
        
        # 8. Mostrar resumen final
        logger.info("="*60)
        logger.info("TRANSFORMACIÓN COMPLETADA EXITOSAMENTE")
        logger.info("="*60)
        logger.info(f"📈 Total registros procesados: {stats['registros_procesados_total']}")
        logger.info(f"� Contratos SECOP II: {stats['registros_secop2']}")
        logger.info(f"� Órdenes de compra TVEC: {stats['registros_tvec']}")
        logger.info(f"📁 Archivo contratos: {output_path}")
        logger.info(f"📁 Archivo órdenes de compra: {tvec_output_path}")
        logger.info("="*60)
        
        # Verificar que ambos archivos se guardaron exitosamente
        return secop2_success and tvec_success
        
    except Exception as e:
        logger.error(f"❌ Error durante la transformación: {e}")
        return False

def find_latest_extraction_file() -> Optional[str]:
    """
    Busca el archivo de extracción más reciente
    """
    logger.info("🔍 Buscando archivo de extracción más reciente...")
    
    # Buscar en directorio de extracción primero
    possible_patterns = [
        'dacp_contracting_data_*.json',
        'dacp_*_data_*.json', 
        'dacp_*.json',
        'dacp_*.csv'
    ]
    
    latest_file = None
    latest_time = 0
    
    # Buscar en directorio de extracción
    if os.path.exists(EXTRACTION_INPUT_DIR):
        for filename in os.listdir(EXTRACTION_INPUT_DIR):
            # Verificar patrones específicos y excluir archivos transformados
            if (any(pattern.replace('*', '') in filename for pattern in possible_patterns) and
                'contratacion_dacp' not in filename and  # Excluir archivos transformados
                'ordenes_compra_dacp' not in filename):  # Excluir archivos transformados
                
                file_path = os.path.join(EXTRACTION_INPUT_DIR, filename)
                file_time = os.path.getmtime(file_path)
                
                if file_time > latest_time:
                    latest_time = file_time
                    latest_file = file_path
    
    # Si no se encuentra en el directorio de extracción, buscar en otros directorios
    if not latest_file:
        logger.info("🔍 Buscando archivo fuente en directorios alternativos...")
        search_dirs = [
            os.path.dirname(os.path.dirname(__file__)),  # Directorio raíz del proyecto
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "extraction_app"),
        ]
        
        for search_dir in search_dirs:
            if os.path.exists(search_dir):
                for root, dirs, files in os.walk(search_dir):
                    for filename in files:
                        if (filename.startswith('dacp_contracting_data_') and 
                            filename.endswith('.json') and
                            'contratacion_dacp' not in filename and
                            'ordenes_compra_dacp' not in filename):
                            
                            file_path = os.path.join(root, filename)
                            file_time = os.path.getmtime(file_path)
                            
                            if file_time > latest_time:
                                latest_time = file_time
                                latest_file = file_path
    
    if latest_file:
        logger.info(f"✅ Archivo encontrado: {latest_file}")
    else:
        logger.warning("⚠️ No se encontró archivo de extracción")
        logger.info("💡 Ejecuta primero el script de extracción: data_extraction_contracting_dacp_sheets.py")
    
    return latest_file

# =============================================================================
# FUNCIÓN PRINCIPAL Y CLI
# =============================================================================

def main():
    """Función principal con opciones de línea de comandos"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Transformar datos de contratación DACP')
    parser.add_argument('--input', help='Archivo de entrada específico')
    parser.add_argument('--output', default=os.path.join(OUTPUT_DIR, OUTPUT_FILE),
                       help='Archivo de salida (default: app_outputs/contratacion_dacp/procesos_contratacion_dacp.json)')
    parser.add_argument('--auto', action='store_true', 
                       help='Buscar automáticamente el archivo de extracción más reciente')
    
    args = parser.parse_args()
    
    try:
        # Determinar archivo de entrada
        if args.input:
            input_file = args.input
            # Verificar que no sea un archivo transformado
            if 'contratacion_dacp' in input_file or 'ordenes_compra_dacp' in input_file:
                print("⚠️ ADVERTENCIA: El archivo especificado parece ser un archivo transformado.")
                print("💡 Se recomienda usar el archivo fuente original (ej: dacp_contracting_data_*.json)")
                response = input("¿Continuar de todas formas? (s/n): ")
                if response.lower() != 's':
                    return False
        elif args.auto:
            input_file = find_latest_extraction_file()
            if not input_file:
                print("❌ No se encontró archivo de extracción automáticamente")
                print("💡 Ejecuta primero el script de extracción o especifica --input")
                return False
        else:
            # Buscar archivo por defecto
            input_file = find_latest_extraction_file()
            if not input_file:
                print("❌ No se encontró archivo de entrada")
                print("💡 Opciones:")
                print("   1. Ejecutar: python data_extraction_contracting_dacp_sheets.py")
                print("   2. Especificar archivo: --input ruta/al/archivo.json")
                print("   3. Buscar automáticamente: --auto")
                return False
        
        # Verificar que el archivo existe
        if not os.path.exists(input_file):
            print(f"❌ Archivo no encontrado: {input_file}")
            return False
        
        # Validar que es un archivo fuente válido
        if 'contratacion_dacp' in input_file or 'ordenes_compra_dacp' in input_file:
            print("❌ Error: No se puede usar un archivo transformado como entrada")
            print("💡 Use el archivo fuente original (ej: dacp_contracting_data_*.json)")
            return False
        
        # Ejecutar transformación
        success = transform_dacp_data(input_file, args.output)
        
        if success:
            print(f"\n🎉 ¡Transformación completada exitosamente!")
            print(f"📁 Archivo de procesos generado: {args.output}")
            tvec_output_path = os.path.join(os.path.dirname(__file__), "app_outputs", "ordenes_compra_dacp", "ordenes_compra_dacp.json")
            print(f"📁 Archivo de órdenes de compra generado: {tvec_output_path}")
            return True
        else:
            print("❌ Error durante la transformación")
            return False
            
    except Exception as e:
        logger.error(f"Error en función principal: {e}")
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    main()