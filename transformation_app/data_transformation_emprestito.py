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
    """Carga los datos foundational de empréstito desde el directorio foundational_emprestito"""
    print("Cargando datos foundational de empréstito...")
    
    # Definir ruta del archivo foundational
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(
        current_dir, 
        "app_inputs", 
        "emprestito_input", 
        "foundational_emprestito",
        "04-09-25 10-12 AM Base Emprestito - DASHBOARD.xlsx"
    )
    
    print(f"Archivo: {file_path}")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
    
    try:
        # Cargar solo la hoja foundational
        df_foundational = pd.read_excel(file_path, sheet_name='foundational')
        print(f"  - foundational: {df_foundational.shape[0]} filas, {df_foundational.shape[1]} columnas")
        
        return df_foundational
        
    except Exception as e:
        print(f"Error cargando archivo foundational: {e}")
        raise


def create_emp_proyectos_json(output_dir: str) -> None:
    """Crea el archivo emp_proyectos.json con solo bp, banco, nombre_comercial desde datos foundational"""
    print("Creando emp_proyectos.json desde datos foundational...")
    
    try:
        # 1. Cargar datos foundational
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
        
        # 7. Seleccionar solo las columnas requeridas (ahora incluyendo bpin)
        required_columns = ['bp', 'banco', 'nombre_comercial', 'bpin']
        available_columns = [col for col in required_columns if col in df_foundational.columns]
        missing_columns = [col for col in required_columns if col not in df_foundational.columns]
        
        if missing_columns:
            print(f"  - Columnas faltantes: {missing_columns}")
        
        print(f"  - Columnas seleccionadas: {available_columns}")
        
        # 8. Crear DataFrame con solo las columnas requeridas
        df_selected = df_foundational[available_columns].copy()
        
        # 9. Limpiar datos de texto
        for col in ['banco', 'nombre_comercial']:
            if col in df_selected.columns:
                df_selected[col] = df_selected[col].apply(clean_text_value)
        
        # 10. Eliminar filas con valores nulos en todas las columnas seleccionadas principales
        initial_rows = len(df_selected)
        # No incluir bpin en la verificación de filas vacías ya que puede ser None
        main_columns = ['bp', 'banco', 'nombre_comercial']
        available_main = [col for col in main_columns if col in df_selected.columns]
        df_selected = df_selected.dropna(subset=available_main, how='all')
        final_rows = len(df_selected)
        
        if initial_rows != final_rows:
            print(f"  - Filas vacías eliminadas: {initial_rows} -> {final_rows}")
        
        # 11. Crear registros únicos por BP
        if 'bp' in df_selected.columns:
            initial_unique = len(df_selected)
            df_selected = df_selected.drop_duplicates(subset=['bp'], keep='first')
            final_unique = len(df_selected)
            print(f"  - Registros únicos por BP: {initial_unique} -> {final_unique}")
        
        # 12. Agregar metadatos
        df_selected['archivo_origen'] = 'foundational_emprestito'
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
    output_dir = os.path.join(current_dir, "app_outputs", "emprestito_output")
    
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
