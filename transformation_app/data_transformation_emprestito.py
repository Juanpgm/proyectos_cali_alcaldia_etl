# -*- coding: utf-8 -*-
"""
Transformación de datos de empréstito
Procesa archivos Excel de empréstito y genera archivos JSON estructurados:
1. Datos foundational del empréstito
2. Seguimiento de publicaciones
3. Publicaciones de contratos
4. Certificados de Disponibilidad Presupuestal (CDP)

Todos los DataFrames siguen los estándares de calidad y normalización establecidos.
"""

import os
import pandas as pd
import numpy as np
import re
import unicodedata
import time
import psutil
import json
from tqdm import tqdm
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime
from dateutil.relativedelta import relativedelta


def normalize_column_names(columns: List[str]) -> List[str]:
    """Normaliza nombres de columnas eliminando espacios, acentos y conectores"""
    normalized_cols = []
    for col in columns:
        # Convert to lowercase
        col = col.lower()
        # Replace spaces with underscores
        col = col.replace(' ', '_')
        # Remove leading/trailing underscores
        col = col.strip('_')
        # Remove accents (tildes)
        col = unicodedata.normalize('NFD', col).encode('ascii', 'ignore').decode('utf-8')
        # Replace 'ñ' with 'n'
        col = col.replace('ñ', 'n')
        normalized_cols.append(col)
    return normalized_cols


def clean_monetary_value(value: Union[str, float, int]) -> int:
    """Limpia valores monetarios removiendo caracteres no numéricos y devuelve entero puro"""
    if pd.isna(value) or value == '':
        return 0
    
    if isinstance(value, (int, float)):
        # Si ya es numérico, convertir a entero
        return int(value) if not pd.isna(value) and not np.isinf(value) else 0
    
    if isinstance(value, str):
        # Remover TODOS los caracteres que no sean dígitos
        # Esto preserva todos los números y elimina puntos, comas, espacios, símbolos, etc.
        cleaned = re.sub(r'[^\d]', '', str(value).strip())
        
        try:
            return int(cleaned) if cleaned else 0
        except ValueError:
            return 0
    
    return 0


def load_bpin_mapping() -> Dict[str, int]:
    """Carga el mapeo de BP a BPIN desde el archivo de datos característicos"""
    try:
        bpin_file = "app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json"
        
        # Si no existe el archivo, usar ruta absoluta
        if not os.path.exists(bpin_file):
            current_dir = os.path.dirname(os.path.abspath(__file__))
            bpin_file = os.path.join(current_dir, "app_outputs", "ejecucion_presupuestal_outputs", "datos_caracteristicos_proyectos.json")
        
        with open(bpin_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Crear mapeo BP -> BPIN
        bp_to_bpin = {}
        for record in data:
            if 'bp' in record and 'bpin' in record:
                bp = record['bp']
                bpin = record['bpin']
                if bp and bpin:
                    bp_to_bpin[str(bp)] = int(bpin)
        
        print(f"Mapeo BP-BPIN cargado con {len(bp_to_bpin)} registros")
        return bp_to_bpin
        
    except Exception as e:
        print(f"Error cargando mapeo BP-BPIN: {e}")
        return {}


def normalize_bp_value(bp_value):
    """Normaliza el valor BP agregando el prefijo 'BP' si no lo tiene"""
    if pd.isna(bp_value) or bp_value == '':
        return None
    
    bp_str = str(bp_value).strip()
    
    # Si ya tiene el formato BP + número, devolverlo tal como está
    if bp_str.upper().startswith('BP') and len(bp_str) > 2:
        numeric_part = bp_str[2:]
        if numeric_part.isdigit():
            return bp_str.upper()  # Asegurar que BP esté en mayúsculas
    
    # Si es solo un número, agregar el prefijo BP
    if bp_str.isdigit():
        return f"BP{bp_str}"
    
    # Intentar extraer cualquier número de la cadena y agregar BP
    numbers = re.findall(r'\d+', bp_str)
    if numbers:
        return f"BP{numbers[0]}"
    
    return None


def add_bpin_column(df: pd.DataFrame, bp_to_bpin: Dict[str, int]) -> pd.DataFrame:
    """Añade la columna BPIN usando el mapeo BP->BPIN"""
    def get_bpin(bp_value):
        if pd.isna(bp_value):
            return None
        
        bp_str = str(bp_value).strip()
        return bp_to_bpin.get(bp_str, None)
    
    df['bpin'] = df['bp'].apply(get_bpin)
    
    # Estadísticas
    total_records = len(df)
    matched_records = df['bpin'].notna().sum()
    print(f"  - BPIN encontrados: {matched_records}/{total_records}")
    
    return df


def load_excel_sheets() -> Dict[str, pd.DataFrame]:
    """Carga todas las hojas de los archivos Excel"""
    print("Cargando archivos Excel de empréstito...")
    
    # Definir rutas de archivos
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(current_dir, "app_inputs", "emprestito_input")
    
    file1 = os.path.join(input_dir, "04-09-25 10-12 AM Base Emprestito - DASHBOARD.xlsx")
    file2 = os.path.join(input_dir, "Flujo de procesos Proyectos-Emprestito.xlsx")
    
    dfs = {}
    
    try:
        # Cargar hojas del primer archivo
        print(f"Cargando {file1}")
        
        # Foundational
        df_foundational = pd.read_excel(file1, sheet_name='foundational')
        dfs['foundational'] = df_foundational
        print(f"  - foundational: {df_foundational.shape}")
        
        # Seguimiento publicaciones
        df_seguimiento = pd.read_excel(file1, sheet_name='seguimiento_publicaciones')
        dfs['seguimiento_publicaciones'] = df_seguimiento
        print(f"  - seguimiento_publicaciones: {df_seguimiento.shape}")
        
        # Publicaciones
        df_publicaciones = pd.read_excel(file1, sheet_name='publicados')
        dfs['publicaciones'] = df_publicaciones
        print(f"  - publicaciones: {df_publicaciones.shape}")
        
        # Cargar CDP del segundo archivo
        print(f"Cargando {file2}")
        df_cdp = pd.read_excel(file2, sheet_name='CDP')
        dfs['cdp'] = df_cdp
        print(f"  - cdp: {df_cdp.shape}")
        
    except Exception as e:
        print(f"Error cargando archivos Excel: {e}")
        raise
    
    return dfs


def normalize_dataframes(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Normaliza nombres de columnas y aplica transformaciones específicas"""
    print("Normalizando DataFrames...")
    
    bp_to_bpin = load_bpin_mapping()
    
    for df_name, df in tqdm(dfs.items(), desc="Normalizando DataFrames"):
        print(f"\nProcesando DataFrame: {df_name}")
        
        # 1. Normalizar nombres de columnas
        df.columns = normalize_column_names(df.columns)
        
        # 2. Reemplazar 'bp_proyecto' por 'bp'
        if 'bp_proyecto' in df.columns:
            df = df.rename(columns={'bp_proyecto': 'bp'})
            print(f"  - Columna 'bp_proyecto' renombrada a 'bp'")
        
        # 3. Normalizar valores BP (agregar prefijo 'BP')
        if 'bp' in df.columns:
            print(f"  - Normalizando valores BP...")
            original_count = df['bp'].notna().sum()
            df['bp'] = df['bp'].apply(normalize_bp_value)
            normalized_count = df['bp'].notna().sum()
            print(f"    Valores BP válidos: {original_count} -> {normalized_count}")
            
            # Mostrar algunos ejemplos
            sample_bp = df['bp'].dropna().head(3).tolist()
            print(f"    Ejemplos: {sample_bp}")
            
            # 4. Añadir columna BPIN
            df = add_bpin_column(df, bp_to_bpin)
        
        # 5. Limpiar valores monetarios
        monetary_columns = [
            'valor_contrato', 'jul_25_desembolso', 'ago_25_desembolso', 'sep_25_desembolso',
            'oct_25_desembolso', 'nov_25_desembolso', 'dic_25_desembolso', 'subtotal_2025',
            'gran_total_2025_2026_2027'
        ]
        
        for col in monetary_columns:
            if col in df.columns:
                original_values = df[col].head(2).tolist()
                df[col] = df[col].apply(clean_monetary_value)
                cleaned_values = df[col].head(2).tolist()
                print(f"  - Limpieza monetaria '{col}': {original_values} -> {cleaned_values}")
        
        # 6. Añadir metadatos operacionales
        df['archivo_origen'] = f"{df_name}_emprestito"
        df['fecha_procesamiento'] = datetime.now().strftime('%Y-%m-%d')
        df['anio'] = 2025  # Año por defecto para empréstito
        
        dfs[df_name] = df
    
    return dfs


def create_unique_data_by_bpin(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    """Estructura los datos para que haya registros únicos por BPIN"""
    print(f"Creando datos únicos por BPIN para {df_name}...")
    
    if 'bpin' not in df.columns:
        print(f"  - No hay columna BPIN en {df_name}, retornando datos originales")
        return df
    
    # Filtrar solo registros con BPIN válido
    df_with_bpin = df[df['bpin'].notna()].copy()
    
    if len(df_with_bpin) == 0:
        print(f"  - No hay registros con BPIN válido en {df_name}")
        return df
    
    # Identificar columnas para agregación
    id_columns = ['bpin', 'bp']
    categorical_columns = [
        'organismo', 'banco', 'descripcion_bp', 'nombre_comercial', 'tipo_de_contratacion',
        'pliego_tipo', 'archivo_origen', 'fecha_procesamiento', 'anio'
    ]
    
    # Encontrar columnas monetarias
    monetary_columns = []
    for col in df_with_bpin.columns:
        if any(keyword in col.lower() for keyword in ['valor', 'desembolso', 'avance', 'total']):
            if df_with_bpin[col].dtype in ['int64', 'float64'] or col in df_with_bpin.select_dtypes(include=[np.number]).columns:
                monetary_columns.append(col)
    
    # Encontrar columnas de fechas
    date_columns = []
    for col in df_with_bpin.columns:
        if 'fecha' in col.lower() or df_with_bpin[col].dtype == 'datetime64[ns]':
            date_columns.append(col)
    
    print(f"  - Columnas monetarias: {monetary_columns[:5]}...")
    print(f"  - Columnas categóricas: {categorical_columns[:5]}...")
    print(f"  - Columnas de fecha: {date_columns}")
    
    # Preparar diccionario de agregación
    agg_dict = {}
    
    # Para columnas categóricas: tomar el primer valor no nulo
    available_categorical = [col for col in categorical_columns if col in df_with_bpin.columns]
    for col in available_categorical:
        agg_dict[col] = 'first'
    
    # Para columnas monetarias: sumar
    available_monetary = [col for col in monetary_columns if col in df_with_bpin.columns]
    for col in available_monetary:
        agg_dict[col] = 'sum'
    
    # Para columnas de fecha: tomar la primera fecha
    available_dates = [col for col in date_columns if col in df_with_bpin.columns]
    for col in available_dates:
        agg_dict[col] = 'first'
    
    # Para otras columnas: tomar el primer valor
    other_columns = [col for col in df_with_bpin.columns 
                    if col not in id_columns + available_categorical + available_monetary + available_dates]
    for col in other_columns:
        agg_dict[col] = 'first'
    
    # Realizar agregación por BPIN
    if agg_dict:
        df_unique = df_with_bpin.groupby('bpin', as_index=False).agg(agg_dict)
        print(f"  - Registros originales: {len(df_with_bpin)} -> Únicos por BPIN: {len(df_unique)}")
    else:
        # Si no hay columnas para agregar, solo eliminar duplicados por BPIN
        df_unique = df_with_bpin.drop_duplicates(subset=['bpin']).reset_index(drop=True)
        print(f"  - Registros originales: {len(df_with_bpin)} -> Únicos por BPIN: {len(df_unique)}")
    
    return df_unique


def separate_dims_and_facts(df: pd.DataFrame, df_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Separa el DataFrame en dimensiones y hechos"""
    print(f"Separando dimensiones y hechos para {df_name}...")
    
    # Definir columnas de dimensiones (características que no cambian frecuentemente)
    dim_columns_candidates = [
        'bpin', 'bp', 'organismo', 'banco', 'descripcion_bp', 'nombre_comercial',
        'tipo_de_contratacion', 'pliego_tipo', 'vig_futura', 'deleg', 'numero_del_contrato',
        'cdp', 'rpc', 'link_secop', 'estado', 'archivo_origen', 'fecha_procesamiento', 'anio'
    ]
    
    # Definir columnas de hechos (valores que cambian o son métricas)
    fact_columns_candidates = [
        'valor_contrato', 'dias', 'dias_habiles_planeados', 'dias_habiles_reales',
        'peso_en_el_proceso', 'peso_en_el_proceso_real'
    ]
    
    # Agregar columnas de desembolsos y avances como hechos
    for col in df.columns:
        if any(keyword in col.lower() for keyword in ['desembolso', 'avance', 'total', 'subtotal']):
            if col not in fact_columns_candidates:
                fact_columns_candidates.append(col)
    
    # Agregar columnas de fechas a dimensiones
    for col in df.columns:
        if 'fecha' in col.lower() or df[col].dtype == 'datetime64[ns]':
            if col not in dim_columns_candidates:
                dim_columns_candidates.append(col)
    
    # Filtrar columnas que realmente existen en el DataFrame
    available_dim_columns = [col for col in dim_columns_candidates if col in df.columns]
    available_fact_columns = [col for col in fact_columns_candidates if col in df.columns]
    
    # Asegurar que BPIN esté en ambos para hacer el join
    if 'bpin' in df.columns:
        if 'bpin' not in available_fact_columns:
            available_fact_columns.insert(0, 'bpin')
    
    # Crear DataFrames de dimensiones y hechos
    if available_dim_columns:
        df_dims = df[available_dim_columns].copy()
        print(f"  - Dimensiones: {len(available_dim_columns)} columnas")
    else:
        df_dims = df.copy()
        print(f"  - Dimensiones: todas las columnas (fallback)")
    
    if available_fact_columns:
        df_facts = df[available_fact_columns].copy()
        print(f"  - Hechos: {len(available_fact_columns)} columnas")
    else:
        df_facts = pd.DataFrame()
        print(f"  - Hechos: sin columnas identificadas")
    
    return df_dims, df_facts


def save_json_files(dataframes: Dict[str, pd.DataFrame], output_dir: str) -> None:
    """Guarda los DataFrames en archivos JSON estructurados"""
    print("Guardando archivos JSON estructurados...")
    
    os.makedirs(output_dir, exist_ok=True)
    
    for df_name, df in tqdm(dataframes.items(), desc="Procesando DataFrames"):
        print(f"\nProcesando {df_name}...")
        
        # 1. Crear datos únicos por BPIN
        df_unique = create_unique_data_by_bpin(df, df_name)
        
        # 2. Separar en dimensiones y hechos
        df_dims, df_facts = separate_dims_and_facts(df_unique, df_name)
        
        # 3. Guardar archivos
        # Guardar dimensiones
        dims_file = os.path.join(output_dir, f"{df_name}_dims.json")
        df_dims.to_json(dims_file, orient='records', indent=2, force_ascii=False)
        dims_size_mb = os.path.getsize(dims_file) / (1024 * 1024)
        print(f"  ✓ Dimensiones guardadas: {len(df_dims)} filas, {dims_size_mb:.2f} MB")
        
        # Guardar hechos (solo si tiene datos)
        if not df_facts.empty:
            facts_file = os.path.join(output_dir, f"{df_name}_facts.json")
            df_facts.to_json(facts_file, orient='records', indent=2, force_ascii=False)
            facts_size_mb = os.path.getsize(facts_file) / (1024 * 1024)
            print(f"  ✓ Hechos guardados: {len(df_facts)} filas, {facts_size_mb:.2f} MB")
        else:
            print(f"  - Sin hechos para guardar en {df_name}")
        
        # Guardar DataFrame completo también (para referencia)
        complete_file = os.path.join(output_dir, f"{df_name}_complete.json")
        df_unique.to_json(complete_file, orient='records', indent=2, force_ascii=False)
        complete_size_mb = os.path.getsize(complete_file) / (1024 * 1024)
        print(f"  ✓ Completo guardado: {len(df_unique)} filas, {complete_size_mb:.2f} MB")


def print_performance_metrics(start_time: float, dataframes: Dict[str, pd.DataFrame]) -> None:
    """Imprime métricas de desempeño"""
    end_time = time.time()
    execution_time = end_time - start_time
    
    process = psutil.Process()
    memory_usage = process.memory_info().rss / (1024 * 1024)  # MB
    
    total_rows = sum(len(df) for df in dataframes.values())
    total_columns = sum(len(df.columns) for df in dataframes.values())
    
    print("\n" + "="*60)
    print("MÉTRICAS DE DESEMPEÑO - EMPRÉSTITO:")
    print("="*60)
    print(f"Tiempo total de ejecución: {execution_time:.2f} segundos")
    print(f"DataFrames procesados: {len(dataframes)}")
    print(f"Total de filas procesadas: {total_rows:,}")
    print(f"Total de columnas: {total_columns}")
    print(f"Memoria utilizada: {memory_usage:.2f} MB")
    
    print("\nDetalle por DataFrame:")
    for name, df in dataframes.items():
        bpin_count = df['bpin'].notna().sum() if 'bpin' in df.columns else 0
        print(f"  - {name}: {len(df)} filas, {len(df.columns)} columnas, {bpin_count} con BPIN")
    
    print("="*60)


def main():
    """Función principal"""
    start_time = time.time()
    
    print("Iniciando transformación de datos de empréstito...")
    
    # Configurar directorios
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, "app_outputs", "emprestito_output")
    
    try:
        # 1. Cargar archivos Excel
        dfs = load_excel_sheets()
        
        # 2. Crear DataFrames específicos según la solicitud
        print("\nCreando DataFrames específicos...")
        
        # df_emp_foundational
        df_emp_foundational = dfs['foundational'].copy()
        print(f"df_emp_foundational creado: {df_emp_foundational.shape}")
        
        # df_emp_seguimiento_publicaciones  
        df_emp_seguimiento_publicaciones = dfs['seguimiento_publicaciones'].copy()
        print(f"df_emp_seguimiento_publicaciones creado: {df_emp_seguimiento_publicaciones.shape}")
        
        # df_emp_publicados
        df_emp_publicados = dfs['publicaciones'].copy()
        print(f"df_emp_publicados creado: {df_emp_publicados.shape}")
        
        # df_emp_cdp
        df_emp_cdp = dfs['cdp'].copy()
        print(f"df_emp_cdp creado: {df_emp_cdp.shape}")
        
        # 3. Organizar DataFrames para procesamiento
        emprestito_dfs = {
            'emp_foundational': df_emp_foundational,
            'emp_seguimiento_publicaciones': df_emp_seguimiento_publicaciones,
            'emp_publicados': df_emp_publicados,
            'emp_cdp': df_emp_cdp
        }
        
        # 4. Normalizar DataFrames (aplicar estándares de calidad)
        emprestito_dfs = normalize_dataframes(emprestito_dfs)
        
        # 5. Guardar archivos JSON estructurados
        save_json_files(emprestito_dfs, output_dir)
        
        # 6. Imprimir métricas
        print_performance_metrics(start_time, emprestito_dfs)
        
        print(f"\n¡Transformación de empréstito completada exitosamente!")
        print(f"Archivos guardados en: {output_dir}")
        print("\nArchivos creados:")
        print("  - emp_foundational_dims.json / emp_foundational_facts.json")
        print("  - emp_seguimiento_publicaciones_dims.json / emp_seguimiento_publicaciones_facts.json")
        print("  - emp_publicados_dims.json / emp_publicados_facts.json")
        print("  - emp_cdp_dims.json / emp_cdp_facts.json")
        
    except Exception as e:
        print(f"Error durante la transformación: {e}")
        raise


if __name__ == "__main__":
    main()
