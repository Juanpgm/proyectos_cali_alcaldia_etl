# -*- coding: utf-8 -*-
"""
Transformación de datos de empréstito
Procesa archivos Excel del directorio foundational_emprestito y genera:
- emp_proyectos.json con las variables: bp, banco, nombre_comercial

Todos los datos siguen los estándares de calidad y normalización establecidos.
"""

import os
import pandas as pd
import numpy as np
import re
import unicodedata
import json
from typing import Union
from datetime import datetime
from tqdm import tqdm


def load_all_emprestito_files():
    """
    Lee todos los archivos de datos de empréstito desde la carpeta de entrada
    detectando automáticamente las extensiones.
    
    Returns:
        pd.DataFrame: DataFrame con todos los archivos combinados
    """
    
    # Ruta de la carpeta de entrada
    input_folder = "transformation_app/app_inputs/emprestito_input"
    
    # Verificar que la carpeta existe
    if not os.path.exists(input_folder):
        raise FileNotFoundError(f"No se encontró la carpeta: {input_folder}")
    
    # Obtener todos los archivos de la carpeta
    all_files = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]
    
    if not all_files:
        raise FileNotFoundError(f"No se encontraron archivos en la carpeta: {input_folder}")
    
    print(f"🔄 Archivos encontrados en la carpeta: {len(all_files)}")
    for file in all_files:
        file_size = os.path.getsize(os.path.join(input_folder, file)) / (1024 * 1024)  # MB
        print(f"  📄 {file} ({file_size:.1f} MB)")
    
    # Lista para almacenar todos los DataFrames
    all_dataframes = []
    
    # Procesar cada archivo según su extensión
    for file in all_files:
        file_path = os.path.join(input_folder, file)
        file_extension = os.path.splitext(file)[1].lower()
        
        print(f"\n🔄 Procesando archivo: {file}")
        print(f"📋 Extensión detectada: {file_extension}")
        
        try:
            if file_extension == '.csv':
                print("📊 Leyendo archivo CSV...")
                # Intentar diferentes encodings para CSV
                encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
                df_temp = None
                
                for encoding in encodings:
                    try:
                        df_temp = pd.read_csv(file_path, low_memory=False, encoding=encoding)
                        print(f"✅ CSV leído exitosamente con encoding: {encoding}")
                        break
                    except UnicodeDecodeError:
                        continue
                
                if df_temp is None:
                    raise ValueError(f"No se pudo leer el archivo CSV con ningún encoding probado")
                
            elif file_extension in ['.xlsx', '.xls']:
                print("📊 Leyendo archivo Excel...")
                
                # Para archivos Excel, intentar detectar las hojas disponibles
                excel_file = pd.ExcelFile(file_path)
                sheet_names = excel_file.sheet_names
                print(f"📋 Hojas detectadas: {sheet_names}")
                
                # Buscar hoja 'foundational' o usar la primera hoja
                target_sheet = None
                if 'foundational' in sheet_names:
                    target_sheet = 'foundational'
                    print(f"✅ Usando hoja 'foundational'")
                else:
                    target_sheet = sheet_names[0]
                    print(f"✅ Usando primera hoja: '{target_sheet}'")
                
                df_temp = pd.read_excel(file_path, sheet_name=target_sheet)
                
            elif file_extension == '.json':
                print("📊 Leyendo archivo JSON...")
                df_temp = pd.read_json(file_path)
                
            elif file_extension == '.parquet':
                print("📊 Leyendo archivo Parquet...")
                df_temp = pd.read_parquet(file_path)
                
            elif file_extension in ['.txt', '.tsv']:
                print("📊 Leyendo archivo de texto delimitado...")
                # Detectar delimitador
                delimiter = '\t' if file_extension == '.tsv' else ','
                df_temp = pd.read_csv(file_path, delimiter=delimiter, low_memory=False)
                
            else:
                print(f"⚠️ Extensión {file_extension} no soportada. Archivo omitido: {file}")
                print("📋 Extensiones soportadas: .csv, .xlsx, .xls, .json, .parquet, .txt, .tsv")
                continue
            
            print(f"✅ Archivo leído: {len(df_temp):,} registros, {len(df_temp.columns)} columnas")
            
            # Añadir información del archivo origen
            df_temp['archivo_origen'] = file
            all_dataframes.append(df_temp)
            
        except Exception as e:
            print(f"❌ Error leyendo archivo {file}: {str(e)}")
            continue
    
    # Verificar que se leyeron archivos
    if not all_dataframes:
        raise ValueError("No se pudo leer ningún archivo de la carpeta")
    
    # Combinar todos los DataFrames
    print(f"\n🔄 Combinando {len(all_dataframes)} archivos...")
    df = pd.concat(all_dataframes, ignore_index=True, sort=False)
    
    print(f"✅ Datos combinados: {len(df):,} registros totales")
    
    return df


def normalize_column_names(columns):
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


def clean_text_value(value: Union[str, float, int]) -> str:
    """Limpia valores de texto eliminando espacios extra y normalizando"""
    if pd.isna(value) or value == '':
        return ''
    
    text = str(value).strip()
    # Eliminar espacios múltiples
    text = re.sub(r'\s+', ' ', text)
    return text


def load_bpin_mapping():
    """Carga el mapeo de BP a BPIN desde el archivo de datos característicos"""
    print("  - Cargando mapeo BP-BPIN...")
    
    try:
        # Definir ruta del archivo de mapeo
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_file = os.path.join(
            current_dir, 
            "app_outputs", 
            "ejecucion_presupuestal_outputs", 
            "datos_caracteristicos_proyectos.json"
        )
        
        print(f"    Archivo de mapeo: {mapping_file}")
        
        if not os.path.exists(mapping_file):
            print(f"    ⚠️ Archivo de mapeo no encontrado, continuando sin BPIN")
            return {}
        
        # Cargar datos
        with open(mapping_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Crear mapeo BP -> BPIN
        bp_to_bpin = {}
        for record in data:
            if 'bp' in record and 'bpin' in record:
                bp = record['bp']
                bpin = record['bpin']
                if bp and bpin:
                    bp_to_bpin[str(bp)] = int(bpin)
        
        print(f"    ✓ Mapeo cargado: {len(bp_to_bpin)} registros BP-BPIN")
        return bp_to_bpin
        
    except Exception as e:
        print(f"    ⚠️ Error cargando mapeo BP-BPIN: {e}")
        return {}


def add_bpin_to_dataframe(df, bp_to_bpin):
    """Añade la columna BPIN al DataFrame usando el mapeo BP->BPIN"""
    print("  - Añadiendo columna BPIN...")
    
    def get_bpin(bp_value):
        if pd.isna(bp_value) or bp_value == '':
            return None
        
        bp_str = str(bp_value).strip()
        bpin_value = bp_to_bpin.get(bp_str, None)
        
        # Convertir a entero si existe el valor
        if bpin_value is not None:
            return int(bpin_value)
        return None
    
    if bp_to_bpin and 'bp' in df.columns:
        df['bpin'] = df['bp'].apply(get_bpin)
        
        # Estadísticas
        total_records = len(df)
        matched_records = df['bpin'].notna().sum()
        print(f"    ✓ BPIN asignados: {matched_records}/{total_records} registros")
        
        # Mostrar algunos ejemplos
        if matched_records > 0:
            sample_mapping = df[df['bpin'].notna()][['bp', 'bpin']].head(3)
            print(f"    Ejemplos de mapeo:")
            for _, row in sample_mapping.iterrows():
                print(f"      {row['bp']} -> {int(row['bpin'])}")
    else:
        print(f"    ⚠️ No se pudo añadir BPIN (sin mapeo o sin columna BP)")
        # Añadir columna BPIN vacía para mantener consistencia
        df['bpin'] = None
    
    return df


def normalize_bp_value(bp_value):
    """Normaliza el valor BP agregando el prefijo 'BP' si no lo tiene"""
    if pd.isna(bp_value) or bp_value == '':
        return None
    
    bp_str = str(bp_value).strip()
    
    # Si ya tiene el formato BP + número, devolverlo tal como está
    if bp_str.upper().startswith('BP') and len(bp_str) > 2:
        numeric_part = bp_str[2:]
        if numeric_part.isdigit():
            return bp_str.upper()
    
    # Si es solo un número, agregar el prefijo BP
    if bp_str.isdigit():
        return f"BP{bp_str}"
    
    # Intentar extraer cualquier número de la cadena y agregar BP
    numbers = re.findall(r'\d+', bp_str)
    if numbers:
        return f"BP{numbers[0]}"
    
    return None


def load_foundational_emprestito_data() -> pd.DataFrame:
    """Carga los datos de empréstito desde la carpeta de entrada usando carga automática"""
    print("Cargando datos de empréstito desde la carpeta de entrada...")
    
    try:
        # Usar la nueva función de carga automática
        df_foundational = load_all_emprestito_files()
        
        print(f"  - Datos cargados: {df_foundational.shape[0]} filas, {df_foundational.shape[1]} columnas")
        
        # Mapeo automático de columnas
        column_mapping = {
            'bp': ['bp', 'BP Proyecto', 'BP_Proyecto', 'proyecto_bp', 'bp_proyecto'],
            'banco': ['banco', 'Banco', 'entidad_bancaria', 'institucion_financiera'],
            'nombre_comercial': ['nombre_comercial', 'Nombre Comercial', 'nombre_entidad'],
            'bpin': ['bpin', 'BPIN', 'codigo_bpin', 'Codigo BPIN', 'código_bpin']
        }
        
        # Buscar y mapear columnas
        print(f"  - Mapeando columnas automáticamente...")
        mapped_columns = {}
        for target_col, possible_names in column_mapping.items():
            found = False
            for possible_name in possible_names:
                if possible_name in df_foundational.columns:
                    mapped_columns[target_col] = possible_name
                    found = True
                    break
            if not found:
                mapped_columns[target_col] = None
        
        print(f"  - Mapeo de columnas encontrado:")
        for target, source in mapped_columns.items():
            if source:
                print(f"    '{target}' -> '{source}' ✓")
            else:
                print(f"    '{target}' -> No encontrada ❌")
        
        # Renombrar columnas según el mapeo
        rename_dict = {source: target for target, source in mapped_columns.items() if source}
        if rename_dict:
            df_foundational = df_foundational.rename(columns=rename_dict)
            print(f"  ✓ Columnas renombradas correctamente")
        
        # Verificar que tenemos las columnas básicas necesarias
        required_base_columns = ['bp', 'banco']  # Columnas mínimas requeridas
        optional_columns = ['nombre_comercial', 'bpin']  # Columnas opcionales
        available_columns = [col.lower() for col in df_foundational.columns]
        
        print(f"  - Verificando columnas requeridas...")
        missing_required = []
        missing_optional = []
        
        for col in required_base_columns:
            if col not in available_columns:
                missing_required.append(col)
        
        for col in optional_columns:
            if col not in available_columns:
                missing_optional.append(col)
        
        if missing_required:
            print(f"  ❌ Columnas críticas faltantes: {missing_required}")
            print(f"  📋 Columnas disponibles (primeras 15):")
            for i, col in enumerate(df_foundational.columns[:15]):
                print(f"    {i+1:2d}. {col}")
            if len(df_foundational.columns) > 15:
                print(f"    ... y {len(df_foundational.columns) - 15} más")
            raise ValueError(f"Faltan columnas críticas: {missing_required}")
        
        if missing_optional:
            print(f"  ⚠️ Columnas opcionales faltantes: {missing_optional}")
        
        print(f"  ✅ Columnas críticas verificadas correctamente")
        
        return df_foundational
        
    except Exception as e:
        print(f"Error cargando archivos de empréstito: {e}")
        raise


def create_emp_proyectos_json(output_dir: str) -> None:
    """Crea el archivo emp_proyectos.json con solo bp, banco, nombre_comercial desde datos de empréstito"""
    print("Creando emp_proyectos.json desde datos de empréstito...")
    
    try:
        # 1. Cargar datos de empréstito
        df_foundational = load_foundational_emprestito_data()
        
        # 2. Normalizar nombres de columnas
        df_foundational.columns = normalize_column_names(df_foundational.columns)
        print(f"  - Columnas normalizadas")
        
        # 3. Reemplazar 'bp_proyecto' por 'bp' si existe
        if 'bp_proyecto' in df_foundational.columns:
            df_foundational = df_foundational.rename(columns={'bp_proyecto': 'bp'})
            print("  - Columna 'bp_proyecto' renombrada a 'bp'")
        
        # 4. Cargar mapeo BP-BPIN
        bp_to_bpin = load_bpin_mapping()
        
        # 5. Normalizar valores BP
        if 'bp' in df_foundational.columns:
            print("  - Normalizando valores BP...")
            original_count = df_foundational['bp'].notna().sum()
            df_foundational['bp'] = df_foundational['bp'].apply(normalize_bp_value)
            normalized_count = df_foundational['bp'].notna().sum()
            print(f"    Valores BP válidos: {original_count} -> {normalized_count}")
        
        # 6. Añadir columna BPIN usando el mapeo
        df_foundational = add_bpin_to_dataframe(df_foundational, bp_to_bpin)
        
        # 7. Seleccionar solo las columnas que existan (ahora incluyendo bpin)
        desired_columns = ['bp', 'banco', 'nombre_comercial', 'bpin']
        available_columns = [col for col in desired_columns if col in df_foundational.columns]
        missing_columns = [col for col in desired_columns if col not in df_foundational.columns]
        
        if missing_columns:
            print(f"  - Columnas no disponibles: {missing_columns}")
        
        print(f"  - Columnas seleccionadas: {available_columns}")
        
        # 8. Crear DataFrame con solo las columnas requeridas
        df_selected = df_foundational[available_columns].copy()
        
        # 9. Limpiar datos de texto para todas las columnas de texto disponibles
        text_columns = ['banco', 'nombre_comercial']
        for col in text_columns:
            if col in df_selected.columns:
                df_selected[col] = df_selected[col].apply(clean_text_value)
        
        # 10. Eliminar filas con valores nulos en las columnas principales disponibles
        initial_rows = len(df_selected)
        # Usar solo las columnas críticas que están disponibles para verificar filas vacías
        critical_columns = ['bp', 'banco']
        available_critical = [col for col in critical_columns if col in df_selected.columns]
        
        if available_critical:
            df_selected = df_selected.dropna(subset=available_critical, how='all')
            final_rows = len(df_selected)
            
            if initial_rows != final_rows:
                print(f"  - Filas vacías eliminadas: {initial_rows} -> {final_rows}")
        else:
            print(f"  ⚠️ No se pudieron verificar filas vacías (columnas críticas no disponibles)")
        
        # 11. Crear registros únicos por BP
        if 'bp' in df_selected.columns:
            initial_unique = len(df_selected)
            df_selected = df_selected.drop_duplicates(subset=['bp'], keep='first')
            final_unique = len(df_selected)
            print(f"  - Registros únicos por BP: {initial_unique} -> {final_unique}")
        
        # 12. Agregar metadatos (archivo_origen ya fue añadido en load_all_emprestito_files)
        # Solo actualizar fecha de procesamiento
        df_selected['fecha_procesamiento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 13. Resetear índice
        df_selected = df_selected.reset_index(drop=True)
        
        # 14. Convertir BPIN a entero para el JSON (mantener None para valores faltantes)
        if 'bpin' in df_selected.columns:
            # Convertir explícitamente a Int64 (pandas nullable integer)
            df_selected['bpin'] = df_selected['bpin'].astype('Int64')
            print(f"  - BPIN convertido a enteros")
        
        # 15. Guardar archivo JSON
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "emp_proyectos.json")
        
        # Convertir a JSON
        df_selected.to_json(output_file, orient='records', indent=2, force_ascii=False)
        
        # Calcular tamaño del archivo
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        
        print(f"  ✓ Archivo guardado: emp_proyectos.json")
        print(f"  ✓ Registros: {len(df_selected)}")
        print(f"  ✓ Columnas: {len(df_selected.columns)}")
        print(f"  ✓ Tamaño: {file_size_mb:.2f} MB")
        
        # Mostrar resumen de datos
        print(f"\nResumen emp_proyectos.json:")
        for col in available_columns:
            if col in df_selected.columns:
                non_null_count = df_selected[col].notna().sum()
                print(f"  - {col}: {non_null_count}/{len(df_selected)} valores válidos")
        
        return output_file
        
    except Exception as e:
        print(f"Error creando emp_proyectos.json: {e}")
        raise


def main():
    """Función principal"""
    print("Iniciando transformación de datos de empréstito...")
    
    # Configurar directorios
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, "app_outputs", "emprestito_outputs")
    
    try:
        # Crear emp_proyectos.json desde datos foundational
        print("\n" + "="*60)
        print("CREANDO EMP_PROYECTOS.JSON")
        print("="*60)
        create_emp_proyectos_json(output_dir)
        
        print(f"\n¡Transformación de empréstito completada exitosamente!")
        print(f"Archivo guardado en: {output_dir}")
        print("\nArchivo creado:")
        print("  - emp_proyectos.json (bp, banco, nombre_comercial, bpin)")
        
    except Exception as e:
        print(f"Error durante la transformación: {e}")
        raise


if __name__ == "__main__":
    main()
