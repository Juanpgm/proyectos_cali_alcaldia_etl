"""
Módulo de transformación de datos PAA (Plan Anual de Adquisiciones)

Este módulo contiene funciones para procesar y transformar datos del Plan Anual de Adquisiciones
de la Alcaldía de Santiago de Cali.

Funciones principales:
- main(): Ejecuta el procesamiento completo de datos PAA
- generate_emprestito_filtered_json(): Genera archivo filtrado con registros donde emprestito = "SI"
- generate_emprestito_only(): Función independiente para generar solo el archivo de empréstito

Uso:
1. Para ejecutar el procesamiento completo:
   python data_transformation_paa.py

2. Para generar solo el archivo filtrado de empréstito:
   python data_transformation_paa.py --emprestito-only

Archivos generados:
- app_outputs/contratos_paa_output/paa_data.json (archivo principal)
- app_outputs/contratos_paa_output/paa_procesos_emprestito.json (archivo filtrado)
"""

import pandas as pd
import os
import glob
import re
import numpy as np
import json
from datetime import datetime
from pathlib import Path

def read_and_process_paa_data():
    """
    Lee todos los archivos .xlsx del PAA, los unifica en un solo DataFrame 
    y elimina las columnas especificadas.
    """
    # Ruta a la carpeta con los archivos PAA
    paa_folder = Path("app_inputs/contratos_input/PAA")
    
    # Buscar todos los archivos .xlsx en la carpeta
    xlsx_files = list(paa_folder.glob("*.xlsx"))
    
    if not xlsx_files:
        print("No se encontraron archivos .xlsx en la carpeta PAA")
        return None
    
    print(f"Encontrados {len(xlsx_files)} archivos Excel:")
    for file in xlsx_files:
        print(f"  - {file.name}")
    
    # Lista para almacenar todos los DataFrames
    dataframes = []
    
    # Leer cada archivo Excel
    for file_path in xlsx_files:
        try:
            print(f"\nLeyendo archivo: {file_path.name}")
            df = pd.read_excel(file_path)
            print(f"  Filas: {len(df)}, Columnas: {len(df.columns)}")
            dataframes.append(df)
        except Exception as e:
            print(f"Error al leer {file_path.name}: {e}")
    
    if not dataframes:
        print("No se pudieron leer archivos válidos")
        return None
    
    # Unificar todos los DataFrames
    unified_df = pd.concat(dataframes, ignore_index=True)
    print(f"\nDataFrame unificado: {len(unified_df)} filas, {len(unified_df.columns)} columnas")
    
    # Columnas a eliminar (normalizando nombres para comparación)
    columns_to_remove = [
        "CODIGO ORGANISMO",
        "DURACION INTERVALO", 
        "FUENTE RECURSOS-VALOR",
        "ELEMENTO PEP",
        "NOMBRE POSPRE",
        "INCLUSION SOCIAL",
        "FUNCIONAMIENTO REAL ESTIMADO",
        "NOMBRE ABREVIADO",
        "PROYECTO",
        "LLAVE",
        "DELEGACIONES_1",
        "CDP BLOQUEADO",
        "DELEGACIONES_2",
        "POSPRE",
        "NOMBRE POSPRE",
        "ESTADO VIGENCIAS",
        "TIPO DE SOLICITUD",
        "3000",
        "$ 1,423,500",
        "$ 4,270,500,000",
        "Contratación Directa - Prestación de Servicios Profesionales y Apoyo a la Gestión (Persona Natural)",
        "Infraestructura",
        "Modificaciones",
        "1423500",
        "4270500000"
    ]
    
    # También verificar columnas numéricas que pueden aparecer como enteros
    numeric_columns_to_remove = [3000, 1423500, 4270500000]
    
    # Mostrar las columnas actuales antes de eliminar
    print(f"\nColumnas antes de la limpieza ({len(unified_df.columns)}):")
    for i, col in enumerate(unified_df.columns):
        print(f"  {i+1}. {col}")
    
    # Eliminar las columnas especificadas (si existen)
    columns_found = []
    columns_not_found = []
    
    # Primero eliminar por nombre de string
    for col_to_remove in columns_to_remove:
        if col_to_remove in unified_df.columns:
            unified_df = unified_df.drop(columns=[col_to_remove])
            columns_found.append(col_to_remove)
        else:
            columns_not_found.append(col_to_remove)
    
    # Luego eliminar columnas numéricas
    for num_col in numeric_columns_to_remove:
        if num_col in unified_df.columns:
            unified_df = unified_df.drop(columns=[num_col])
            columns_found.append(str(num_col))
        else:
            columns_not_found.append(str(num_col))
    
    print(f"\nColumnas eliminadas ({len(columns_found)}):")
    for col in columns_found:
        print(f"  - {col}")
    
    if columns_not_found:
        print(f"\nColumnas no encontradas en el dataset ({len(columns_not_found)}):")
        for col in columns_not_found:
            print(f"  - {col}")
    
    print(f"\nDataFrame final: {len(unified_df)} filas, {len(unified_df.columns)} columnas")
    print(f"\nColumnas restantes:")
    for i, col in enumerate(unified_df.columns):
        print(f"  {i+1}. {col}")
    
    return unified_df

def clean_monetary_value(value):
    """
    Limpia valores monetarios y los convierte a enteros
    """
    if pd.isna(value) or value == '' or value is None:
        return 0
    
    # Convertir a string si no lo es
    str_value = str(value)
    
    # Remover símbolos de moneda, comas, puntos y espacios
    cleaned = re.sub(r'[$,\.\s]', '', str_value)
    
    # Remover caracteres no numéricos excepto el signo negativo
    cleaned = re.sub(r'[^0-9\-]', '', cleaned)
    
    # Si está vacío después de la limpieza, retornar 0
    if not cleaned or cleaned == '-':
        return 0
    
    try:
        return int(float(cleaned))
    except (ValueError, TypeError):
        return 0

def standardize_data(df):
    """
    Estandariza los datos del DataFrame:
    - Normaliza nombres de columnas (espacios → guiones bajos, elimina stop words)
    - Identifica y convierte columnas monetarias a enteros
    """
    if df is None:
        return None
    
    print("\n=== ESTANDARIZACIÓN DE DATOS ===")
    
    # 1. Normalizar nombres de columnas
    print("1. Normalizando nombres de columnas...")
    df = normalize_column_names(df)
    
    print(f"   Columnas después de normalización: {len(df.columns)}")
    
    # 2. Identificar columnas monetarias por nombre
    monetary_keywords = [
        'valor', 'precio', 'costo', 'monto', 'presupuesto', 'ppto',
        'estimado', 'disponible', 'apropiado', 'total', 'vigencia',
        'futuras', 'actividad', 'inversion', 'real'
    ]
    
    monetary_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in monetary_keywords):
            # Verificar si contiene números o símbolos monetarios
            sample_values = df[col].dropna().astype(str).head(10)
            if any(re.search(r'[\$\d,\.]', str(val)) for val in sample_values):
                monetary_columns.append(col)
    
    print(f"2. Columnas monetarias identificadas ({len(monetary_columns)}):")
    for col in monetary_columns:
        print(f"   - {col}")
    
    # 3. Convertir columnas monetarias a enteros
    if monetary_columns:
        print("3. Convirtiendo valores monetarios a enteros...")
        for col in monetary_columns:
            print(f"   Procesando: {col}")
            original_type = df[col].dtype
            
            # Aplicar limpieza de valores monetarios
            df[col] = df[col].apply(clean_monetary_value)
            
            # Asegurar que sea entero
            df[col] = df[col].astype('int64')
            
            print(f"     Tipo original: {original_type} -> Tipo final: {df[col].dtype}")
    
    # 4. Mostrar muestra de datos estandarizados
    print("\n4. Muestra de datos estandarizados:")
    print("   Primeras 3 filas de columnas monetarias:")
    if monetary_columns:
        sample_monetary = df[monetary_columns[:5]].head(3)  # Primeras 5 columnas monetarias
        print(sample_monetary)
    
    print(f"\n=== RESUMEN ESTANDARIZACIÓN ===")
    print(f"Total columnas procesadas: {len(df.columns)}")
    print(f"Columnas monetarias convertidas: {len(monetary_columns)}")
    print(f"Filas procesadas: {len(df)}")
    
    return df

def clean_nan_values(df):
    """
    Función dedicada a convertir todos los valores NaN a None de manera robusta
    """
    if df is None:
        return None
    
    print("\n=== LIMPIEZA DE VALORES NaN ===")
    
    df_cleaned = df.copy()
    
    # Contador de valores convertidos
    nan_count = 0
    
    # Procesar cada columna individualmente
    for col in df_cleaned.columns:
        column_nan_count = 0
        
        if df_cleaned[col].dtype == 'object':
            # Para columnas de tipo object (strings, mixed types)
            for idx in df_cleaned.index:
                value = df_cleaned.at[idx, col]
                if pd.isna(value) or value == 'nan' or value == 'NaN' or str(value).lower() == 'nan':
                    df_cleaned.at[idx, col] = None
                    column_nan_count += 1
        
        elif df_cleaned[col].dtype in ['float64', 'float32']:
            # Para columnas de tipo float
            mask = pd.isna(df_cleaned[col])
            df_cleaned.loc[mask, col] = None
            column_nan_count = mask.sum()
        
        elif df_cleaned[col].dtype in ['int64', 'int32']:
            # Para columnas de tipo int (aunque normalmente no tienen NaN)
            # Convertir a object primero si hay NaN
            if df_cleaned[col].isna().any():
                df_cleaned[col] = df_cleaned[col].astype('object')
                mask = pd.isna(df_cleaned[col])
                df_cleaned.loc[mask, col] = None
                column_nan_count = mask.sum()
        
        else:
            # Para otros tipos de datos
            mask = pd.isna(df_cleaned[col])
            if mask.any():
                df_cleaned.loc[mask, col] = None
                column_nan_count = mask.sum()
        
        if column_nan_count > 0:
            print(f"   {col}: {column_nan_count} valores NaN → None")
        
        nan_count += column_nan_count
    
    print(f"\nTotal de valores NaN convertidos a None: {nan_count}")
    
    # Aplicar limpieza final más robusta
    print("Aplicando limpieza final robusta...")
    
    # Método principal: usar where con condición global
    df_cleaned = df_cleaned.where(pd.notnull(df_cleaned), None)
    
    # Verificación final
    final_nan_count = 0
    for col in df_cleaned.columns:
        col_nans = pd.isna(df_cleaned[col]).sum()
        final_nan_count += col_nans
        if col_nans > 0:
            print(f"   ⚠️  {col}: aún tiene {col_nans} valores NaN")
    
    print(f"Valores NaN restantes después de limpieza robusta: {final_nan_count}")
    
    if final_nan_count == 0:
        print("✅ Limpieza exitosa: No quedan valores NaN")
    else:
        print("⚠️  Algunos valores NaN persistentes detectados")
        # Si aún quedan NaN, aplicar método fila por fila
        print("Aplicando limpieza manual fila por fila...")
        for idx in df_cleaned.index:
            for col in df_cleaned.columns:
                if pd.isna(df_cleaned.at[idx, col]):
                    df_cleaned.at[idx, col] = None
        
        # Verificación final después de limpieza manual
        final_final_count = 0
        for col in df_cleaned.columns:
            final_final_count += pd.isna(df_cleaned[col]).sum()
        
        print(f"Valores NaN después de limpieza manual: {final_final_count}")
    
    return df_cleaned

def normalize_column_names(df):
    """
    Normaliza los nombres de las columnas:
    - Convierte a minúsculas
    - Reemplaza espacios por guiones bajos
    - Elimina palabras sin significado (stop words)
    - Elimina caracteres especiales
    """
    if df is None:
        return None
    
    print("\n=== NORMALIZACIÓN DE NOMBRES DE COLUMNAS ===")
    
    # Lista de stop words en español para eliminar
    stop_words = {
        'de', 'del', 'la', 'el', 'en', 'a', 'por', 'para', 'con', 'sin', 'sobre', 
        'bajo', 'entre', 'hacia', 'desde', 'hasta', 'durante', 'mediante', 'según',
        'y', 'o', 'u', 'e', 'que', 'como', 'si', 'no', 'se', 'te', 'le', 'lo', 'los',
        'las', 'un', 'una', 'unos', 'unas', 'al', 'es', 'son', 'esta', 'este', 'estos',
        'estas', 'su', 'sus', 'mi', 'mis', 'tu', 'tus', 'nuestra', 'nuestro', 'nuestros',
        'nuestras', 'vuestra', 'vuestro', 'vuestros', 'vuestras'
    }
    
    def clean_column_name(name):
        """Limpia un nombre de columna individual"""
        # Convertir a minúsculas
        clean_name = str(name).lower().strip()
        
        # Reemplazar caracteres especiales por espacios
        import re
        clean_name = re.sub(r'[^\w\s]', ' ', clean_name)
        
        # Dividir en palabras
        words = clean_name.split()
        
        # Filtrar stop words y palabras vacías
        filtered_words = [word for word in words if word and word not in stop_words and len(word) > 1]
        
        # Si no quedan palabras después del filtrado, usar el nombre original procesado
        if not filtered_words:
            # Usar palabras originales pero limpias
            original_words = clean_name.split()
            filtered_words = [word for word in original_words if word and len(word) > 0]
        
        # Unir con guiones bajos
        result = '_'.join(filtered_words)
        
        # Limpiar guiones bajos múltiples
        result = re.sub(r'_+', '_', result)
        
        # Remover guiones bajos al inicio y final
        result = result.strip('_')
        
        return result if result else 'columna_sin_nombre'
    
    # Crear diccionario de mapeo de nombres
    old_names = df.columns.tolist()
    new_names = [clean_column_name(name) for name in old_names]
    
    # Manejar nombres duplicados agregando sufijos
    seen_names = {}
    final_names = []
    
    for new_name in new_names:
        if new_name in seen_names:
            seen_names[new_name] += 1
            final_name = f"{new_name}_{seen_names[new_name]}"
        else:
            seen_names[new_name] = 0
            final_name = new_name
        final_names.append(final_name)
    
    # Aplicar los nuevos nombres
    df.columns = final_names
    
    # Mostrar los cambios
    print("Cambios realizados en nombres de columnas:")
    changes_count = 0
    for old, new in zip(old_names, final_names):
        if old != new:
            print(f"  '{old}' → '{new}'")
            changes_count += 1
    
    print(f"\nTotal columnas renombradas: {changes_count}/{len(old_names)}")
    
    return df

def enrich_with_bpin_data(df):
    """
    Enriquece el DataFrame PAA con datos BPIN desde el archivo de ejecución presupuestal
    usando la columna 'bp' como clave de enlace
    """
    if df is None:
        return None
    
    print("\n=== ENRIQUECIMIENTO CON DATOS BPIN ===")
    
    # Verificar que existe la columna 'bp'
    if 'bp' not in df.columns:
        print("⚠️ Columna 'bp' no encontrada en el DataFrame PAA")
        return df
    
    # Ruta del archivo de datos característicos de proyectos
    ep_data_path = "app_outputs/ejecucion_presupuestal_outputs/datos_caracteristicos_proyectos.json"
    
    if not os.path.exists(ep_data_path):
        print(f"⚠️ Archivo no encontrado: {ep_data_path}")
        return df
    
    print(f"📂 Cargando datos de: {ep_data_path}")
    
    try:
        # Cargar datos de ejecución presupuestal
        with open(ep_data_path, 'r', encoding='utf-8') as f:
            ep_data = json.load(f)
        
        print(f"✅ Datos cargados: {len(ep_data)} registros")
        
        # Crear diccionario de mapeo bp -> bpin
        bp_to_bpin = {}
        for record in ep_data:
            if 'bp' in record and 'bpin' in record:
                bp_value = record['bp']
                bpin_value = record['bpin']
                if bp_value and bpin_value:
                    bp_to_bpin[bp_value] = bpin_value
        
        print(f"📊 Mapeo BP -> BPIN creado: {len(bp_to_bpin)} registros")
        
        # Añadir columna BPIN al DataFrame PAA
        df['bpin'] = df['bp'].map(bp_to_bpin)
        
        # Estadísticas del enriquecimiento
        total_records = len(df)
        enriched_records = df['bpin'].notna().sum()
        not_enriched = total_records - enriched_records
        
        print(f"📈 Estadísticas del enriquecimiento:")
        print(f"   Total registros PAA: {total_records}")
        print(f"   Registros enriquecidos con BPIN: {enriched_records}")
        print(f"   Registros sin BPIN: {not_enriched}")
        print(f"   Porcentaje de éxito: {(enriched_records/total_records)*100:.1f}%")
        
        # Llenar valores nulos con 0 o None según preferencia
        df['bpin'] = df['bpin'].fillna(0).astype('int64')
        
        return df
        
    except Exception as e:
        print(f"❌ Error al cargar datos de ejecución presupuestal: {e}")
        return df

def rename_columns_for_export(df):
    """
    Renombra columnas específicas antes de la exportación a JSON
    con nombres más descriptivos y sin caracteres especiales
    """
    if df is None:
        return None
    
    print("\n=== RENOMBRAMIENTO DE COLUMNAS PARA EXPORTACIÓN ===")
    
    # Mapeo de nombres de columnas específicos
    column_mapping = {
        'organismo': 'centro_gestor',
        'descripcion': 'descripcion_contrato',
        'inversión_real_estimado': 'inversion_estimada',
        'número_modificaciones': 'numero_modificaciones',
        'justificación_vencida': 'justificacion_vencida',
        'elemento_pep': 'bp',
        'empréstito': 'emprestito',
        'link_proceso': 'urlProceso'
    }
    
    # Verificar qué columnas existen y pueden ser renombradas
    existing_columns = df.columns.tolist()
    changes_made = []
    
    for old_name, new_name in column_mapping.items():
        if old_name in existing_columns:
            df = df.rename(columns={old_name: new_name})
            changes_made.append((old_name, new_name))
            print(f"  '{old_name}' → '{new_name}'")
        else:
            print(f"  ⚠️  Columna '{old_name}' no encontrada en el DataFrame")
    
    print(f"\nTotal columnas renombradas: {len(changes_made)}")
    
    return df

def save_processed_data(df, output_path="app_outputs/paa_processed.xlsx"):
    """
    Guarda el DataFrame procesado en un archivo Excel
    """
    if df is not None:
        # Crear la carpeta de salida si no existe
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Guardar el archivo
        df.to_excel(output_path, index=False)
        print(f"\nDatos procesados guardados en: {output_path}")
        return output_path
    else:
        print("No hay datos para guardar")
        return None

def optimize_dataframe_for_json(df):
    """
    Optimiza el DataFrame para exportación JSON reduciendo tamaño
    """
    df_optimized = df.copy()
    
    print("=== OPTIMIZACIÓN PARA JSON ===")
    
    # 1. Aplicar limpieza robusta de NaN primero
    print("1. Aplicando limpieza final de valores NaN...")
    df_optimized = df_optimized.where(pd.notnull(df_optimized), None)
    
    # 2. Optimizar tipos de datos
    print("2. Optimizando tipos de datos...")
    for col in df_optimized.columns:
        if df_optimized[col].dtype == 'object':
            # Convertir strings vacíos a None
            df_optimized[col] = df_optimized[col].replace('', None)
            # Eliminar espacios extra en valores no nulos
            df_optimized[col] = df_optimized[col].apply(
                lambda x: x.strip() if isinstance(x, str) and x is not None else x
            )
            # Convertir 'nan' strings a None
            df_optimized[col] = df_optimized[col].replace('nan', None)
        elif df_optimized[col].dtype in ['int64', 'float64']:
            # Convertir NaN a None para enteros y flotantes
            df_optimized[col] = df_optimized[col].where(pd.notnull(df_optimized[col]), None)
        elif 'datetime' in str(df_optimized[col].dtype):
            # Convertir fechas a strings ISO format
            df_optimized[col] = df_optimized[col].apply(
                lambda x: x.isoformat() if pd.notnull(x) else None
            )
    
    # 3. Comprimir categorías repetitivas
    print("3. Identificando categorías para optimización...")
    categorical_threshold = 0.5  # Si más del 50% de valores se repiten
    for col in df_optimized.select_dtypes(include=['object']).columns:
        if df_optimized[col].nunique() / len(df_optimized) < categorical_threshold:
            print(f"   Optimizando columna categórica: {col}")
            df_optimized[col] = df_optimized[col].astype('category')
    
    return df_optimized

class JSONEncoder(json.JSONEncoder):
    """
    Encoder personalizado para manejar tipos de datos especiales
    """
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            # Verificar si es NaN antes de convertir
            if np.isnan(obj):
                return None
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        # Manejo específico para valores NaN de numpy
        elif str(obj) == 'nan' or str(obj) == 'NaN':
            return None
        return super().default(obj)
    
    def encode(self, obj):
        """
        Override del método encode para manejar casos especiales
        """
        # Convertir el objeto usando el método default primero
        if isinstance(obj, (dict, list)):
            obj = self._clean_nan_recursive(obj)
        return super().encode(obj)
    
    def _clean_nan_recursive(self, obj):
        """
        Limpia recursivamente valores NaN en estructuras anidadas
        """
        if isinstance(obj, dict):
            return {k: self._clean_nan_recursive(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_nan_recursive(item) for item in obj]
        elif pd.isna(obj) or str(obj) == 'nan' or str(obj) == 'NaN':
            return None
        elif isinstance(obj, np.floating) and np.isnan(obj):
            return None
        else:
            return obj

def export_to_optimized_json(df, output_path="app_outputs/contratos_paa_output/paa_data.json"):
    """
    Exporta el DataFrame a JSON optimizado - solo archivo principal
    """
    if df is None:
        print("No hay datos para exportar")
        return None
    
    print("\n=== EXPORTACIÓN JSON OPTIMIZADA ===")
    
    # Crear la carpeta de salida si no existe
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Optimizar DataFrame
    df_optimized = optimize_dataframe_for_json(df)
    
    # Convertir a diccionario optimizado
    print("1. Convirtiendo a estructura JSON optimizada...")
    
    # Estrategia 1: Formato compacto con registros - con limpieza adicional de NaN
    print("   Aplicando limpieza final de NaN antes de serialización...")
    
    def clean_record(record):
        """Limpia un registro individual de valores NaN"""
        cleaned = {}
        for key, value in record.items():
            if pd.isna(value) or str(value) == 'nan' or str(value) == 'NaN':
                cleaned[key] = None
            elif isinstance(value, np.floating) and np.isnan(value):
                cleaned[key] = None
            else:
                cleaned[key] = value
        return cleaned
    
    # Aplicar limpieza a cada registro
    data_records = [clean_record(record) for record in df_optimized.to_dict('records')]
    
    # Estrategia 2: Crear metadata para reducir redundancia
    metadata = {
        "total_records": len(df_optimized),
        "columns": list(df_optimized.columns),
        "data_types": {col: str(df_optimized[col].dtype) for col in df_optimized.columns},
        "export_date": pd.Timestamp.now().isoformat(),
        "source": "PAA Database - Santiago de Cali"
    }
    
    # Estrategia 3: Separar datos categóricos para reducir redundancia
    categorical_data = {}
    
    # Identificar columnas con valores repetitivos
    for col in df_optimized.columns:
        if hasattr(df_optimized[col], 'cat') or (
            df_optimized[col].dtype == 'object' and 
            df_optimized[col].nunique() < len(df_optimized) * 0.3
        ):
            unique_values = df_optimized[col].dropna().unique().tolist()
            if len(unique_values) < 100:  # Solo para categorías pequeñas
                categorical_data[col] = unique_values
    
    print(f"2. Categorías identificadas para optimización: {len(categorical_data)}")
    
    # Crear estructura JSON final optimizada
    json_structure = {
        "metadata": metadata,
        "categorical_mappings": categorical_data,
        "data": data_records
    }
    
    # Exportar JSON principal con formato legible
    print("3. Exportando JSON principal...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_structure, f, ensure_ascii=False, indent=2, cls=JSONEncoder)
    
    # Post-procesamiento para eliminar cualquier NaN restante
    print("4. Post-procesamiento para eliminar NaN restantes...")
    
    def post_process_json_file(file_path):
        """Post-procesa un archivo JSON para eliminar NaN restantes"""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Reemplazar valores NaN con null
        import re
        # Patrón para encontrar ": NaN," o ": NaN}"
        content = re.sub(r':\s*NaN\s*([,}])', r': null\1', content)
        
        # Escribir el contenido corregido
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return content.count('null') - content.count('NaN')
    
    # Aplicar post-procesamiento
    fixed_count = post_process_json_file(output_path)
    print(f"   Valores NaN corregidos: {fixed_count}")
    
    # Mostrar estadísticas del archivo
    file_size = os.path.getsize(output_path)
    
    print(f"\n=== ARCHIVO JSON GENERADO ===")
    print(f"Archivo: {output_path}")
    print(f"Tamaño: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
    print(f"Registros: {len(data_records)}")
    
    return {
        "main_file": output_path,
        "main_size": file_size,
        "records_count": len(data_records)
    }

def generate_emprestito_filtered_json():
    """
    Genera un archivo JSON filtrado que contiene solo los registros 
    donde emprestito = "SI" basado en el archivo paa_data.json existente
    """
    print("\n=== GENERACIÓN DE ARCHIVO FILTRADO EMPRÉSTITO ===")
    
    # Ruta del archivo fuente
    source_file = "app_outputs/contratos_paa_output/paa_data.json"
    # Ruta del archivo de salida
    output_file = "app_outputs/contratos_paa_output/paa_procesos_emprestito.json"
    
    if not os.path.exists(source_file):
        print(f"❌ Archivo fuente no encontrado: {source_file}")
        return None
    
    print(f"📂 Cargando datos desde: {source_file}")
    
    try:
        # Cargar el archivo JSON completo
        with open(source_file, 'r', encoding='utf-8') as f:
            full_data = json.load(f)
        
        # Verificar estructura del JSON
        if 'data' not in full_data:
            print("❌ Estructura JSON inválida: falta clave 'data'")
            return None
        
        total_records = len(full_data['data'])
        print(f"📊 Total de registros en el archivo fuente: {total_records}")
        
        # Filtrar registros donde emprestito = "SI"
        print("🔍 Filtrando registros donde emprestito = 'SI'...")
        
        filtered_records = []
        emprestito_count = 0
        
        for record in full_data['data']:
            if record.get('emprestito') == 'SI':
                filtered_records.append(record)
                emprestito_count += 1
        
        print(f"✅ Registros filtrados: {emprestito_count}")
        print(f"📈 Porcentaje de registros con empréstito: {(emprestito_count/total_records)*100:.2f}%")
        
        if emprestito_count == 0:
            print("⚠️ No se encontraron registros con emprestito = 'SI'")
            return None
        
        # Crear estructura JSON para el archivo filtrado
        filtered_data = {
            "metadata": {
                "title": "Procesos PAA - Empréstito",
                "description": "Registros PAA filtrados donde emprestito = SI",
                "total_records": emprestito_count,
                "filter_applied": "emprestito = 'SI'",
                "source_file": "paa_data.json",
                "source_total_records": total_records,
                "columns": full_data['metadata']['columns'],
                "data_types": full_data['metadata']['data_types'],
                "generation_date": pd.Timestamp.now().isoformat(),
                "generated_by": "data_transformation_paa.py - generate_emprestito_filtered_json()"
            },
            "statistics": {
                "total_source_records": total_records,
                "filtered_records": emprestito_count,
                "filter_percentage": round((emprestito_count/total_records)*100, 2)
            },
            "data": filtered_records
        }
        
        # Crear directorio de salida si no existe
        output_dir = Path(output_file).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Guardar archivo filtrado
        print(f"💾 Guardando archivo filtrado en: {output_file}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=2, cls=JSONEncoder)
        
        # Mostrar estadísticas del archivo generado
        file_size = os.path.getsize(output_file)
        
        print(f"\n=== ARCHIVO EMPRÉSTITO GENERADO ===")
        print(f"📁 Archivo: {output_file}")
        print(f"📏 Tamaño: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
        print(f"📊 Registros: {emprestito_count}")
        print(f"🔢 Porcentaje del total: {(emprestito_count/total_records)*100:.2f}%")
        
        # Mostrar muestra de registros filtrados
        if emprestito_count > 0:
            print(f"\n📋 Muestra de registros filtrados (primeros 3):")
            for i, record in enumerate(filtered_records[:3]):
                print(f"   Registro {i+1}:")
                print(f"     - Centro gestor: {record.get('centro_gestor', 'N/A')}")
                print(f"     - Descripción: {record.get('descripcion_contrato', 'N/A')[:80]}...")
                print(f"     - Empréstito: {record.get('emprestito', 'N/A')}")
                print(f"     - BP: {record.get('bp', 'N/A')}")
                print(f"     - BPIN: {record.get('bpin', 'N/A')}")
        
        return {
            "output_file": output_file,
            "file_size": file_size,
            "total_records": emprestito_count,
            "filter_percentage": (emprestito_count/total_records)*100
        }
        
    except Exception as e:
        print(f"❌ Error al procesar archivo: {e}")
        return None

def generate_emprestito_only():
    """
    Función independiente para generar únicamente el archivo filtrado de empréstito
    sin ejecutar todo el procesamiento PAA
    """
    print("=== GENERACIÓN INDEPENDIENTE - ARCHIVO EMPRÉSTITO ===")
    
    # Cambiar al directorio del script
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # Generar solo el archivo filtrado
    emprestito_result = generate_emprestito_filtered_json()
    
    if emprestito_result:
        print(f"\n✅ Proceso completado exitosamente")
        print(f"📁 Archivo generado: {emprestito_result['output_file']}")
        print(f"📊 Total de registros: {emprestito_result['total_records']}")
        print(f"📏 Tamaño del archivo: {emprestito_result['file_size']/1024:.2f} KB")
        return emprestito_result
    else:
        print(f"\n❌ Error en la generación del archivo")
        return None

def main():
    """
    Función principal para ejecutar el procesamiento de datos PAA
    """
    print("=== PROCESAMIENTO DE DATOS PAA ===")
    
    # Cambiar al directorio del script
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # Procesar los datos
    df_processed = read_and_process_paa_data()
    
    if df_processed is not None:
        # Aplicar estandarización
        df_standardized = standardize_data(df_processed)
        
        if df_standardized is not None:
            # Limpiar valores NaN antes de la exportación
            df_cleaned = clean_nan_values(df_standardized)
            
            if df_cleaned is not None:
                # Renombrar columnas específicas para exportación JSON
                df_for_export = rename_columns_for_export(df_cleaned.copy())
                
                # Enriquecer con datos BPIN
                df_enriched = enrich_with_bpin_data(df_for_export)
                
                # Guardar los datos procesados y estandarizados (comentado para no generar Excel)
                # output_file = save_processed_data(df_enriched)
                
                # Exportar a JSON optimizado
                print(f"\n📤 Exportando datos PAA a JSON...")
                json_files = export_to_optimized_json(df_enriched)
                
                # Generar archivo filtrado de empréstito
                print(f"\n📤 Generando archivo filtrado de empréstito...")
                emprestito_result = generate_emprestito_filtered_json()
                
                # Mostrar resumen final
                print(f"\n=== RESUMEN FINAL ===")
                print(f"Total de registros procesados: {len(df_enriched)}")
                print(f"Total de columnas finales: {len(df_enriched.columns)}")
                if json_files:
                    print(f"Archivo JSON principal: {json_files['main_file']}")
                if emprestito_result:
                    print(f"Archivo filtrado empréstito: {emprestito_result['output_file']}")
                    print(f"Registros empréstito: {emprestito_result['total_records']}")
                
                # Mostrar las primeras filas como muestra
                print(f"\nPrimeras 3 filas del dataset final:")
                print(df_enriched.head(3))
                
                # Mostrar tipos de datos de columnas monetarias
                monetary_cols = [col for col in df_enriched.columns 
                               if any(keyword in col.lower() for keyword in 
                                    ['valor', 'precio', 'costo', 'monto', 'presupuesto', 'ppto'])]
                
                if monetary_cols:
                    print(f"\nTipos de datos de columnas monetarias:")
                    for col in monetary_cols[:10]:  # Mostrar primeras 10
                        print(f"  {col}: {df_enriched[col].dtype}")
                
                return df_enriched
            else:
                print("Error en la limpieza de valores NaN")
                return None
        else:
            print("Error en la estandarización de datos")
            return None
    else:
        print("No se pudieron procesar los datos")
        return None

if __name__ == "__main__":
    import sys
    
    # Verificar si se pasa un argumento específico para generar solo empréstito
    if len(sys.argv) > 1 and sys.argv[1] == "--emprestito-only":
        generate_emprestito_only()
    else:
        main()