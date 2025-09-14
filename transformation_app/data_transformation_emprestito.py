# -*- coding: utf-8 -*-
"""
Transformación de datos de empréstito
Procesa el archivo Excel seguimiento_contratos_emprestito.xlsx y genera:
- emp_contratos_index.json con las variables: banco, id, referencia_proceso, referencia_contrato, fecha_extraccion

Todos los datos siguen los estándares de calidad y normalización establecidos.
"""

import os
import pandas as pd
import json
from datetime import datetime


def clean_text_value(value) -> str:
    """Limpia valores de texto eliminando espacios extra y caracteres escapados"""
    if pd.isna(value) or value == '':
        return ''
    
    text = str(value).strip()
    # Eliminar espacios múltiples
    text = ' '.join(text.split())
    # Corregir barras diagonales escapadas
    text = text.replace('\\/', '/')
    # Corregir comillas escapadas
    text = text.replace('\\"', '"')
    text = text.replace("\\'", "'")
    return text


def create_emp_contratos_index_json(output_dir: str) -> None:
    """
    Crea el archivo emp_contratos_index.json leyendo directamente desde seguimiento_contratos_emprestito.xlsx
    y extrayendo solo las variables: banco, id, referencia_proceso, referencia_contrato, fecha_extraccion
    """
    print("Creando emp_contratos_index.json desde seguimiento_contratos_emprestito.xlsx...")
    
    try:
        # 1. Cargar el archivo Excel directamente
        input_file = "transformation_app/app_inputs/emprestito_input/seguimiento_contratos_emprestito/seguimiento_contratos_emprestito.xlsx"
        
        if not os.path.exists(input_file):
            print(f"  ❌ Archivo no encontrado: {input_file}")
            return
        
        print(f"  - Leyendo archivo: {input_file}")
        df = pd.read_excel(input_file)
        
        print(f"  - Archivo cargado: {len(df)} registros, {len(df.columns)} columnas")
        print(f"  - Columnas disponibles: {list(df.columns)}")
        
        # 2. Mapeo de columnas a las variables requeridas
        column_mapping = {
            'banco': 'BANCO',
            'id': 'ID',  
            'referencia_proceso': 'Nro de Proceso',
            'referencia_contrato': 'referencia_contato',  # Note: hay un typo en el Excel original
            'fecha_extraccion': 'FECHA EXTRACCION'  # Esta columna no existe, se generará automáticamente
        }
        
        # 3. Verificar que las columnas existen y mapear
        mapped_data = {}
        for target_col, excel_col in column_mapping.items():
            if excel_col in df.columns:
                mapped_data[target_col] = df[excel_col]
                print(f"  ✓ Mapeado: {target_col} <- {excel_col}")
            else:
                print(f"  ⚠️ Columna no encontrada: {excel_col}")
                # Buscar columnas similares
                similar_cols = [col for col in df.columns if excel_col.lower() in col.lower() or col.lower() in excel_col.lower()]
                if similar_cols:
                    print(f"    Columnas similares encontradas: {similar_cols}")
                mapped_data[target_col] = None
        
        # 4. Crear DataFrame con solo las columnas requeridas
        result_data = []
        for i in range(len(df)):
            record = {}
            for target_col in column_mapping.keys():
                if mapped_data[target_col] is not None:
                    value = mapped_data[target_col].iloc[i]
                    # Limpiar valores nulos y vacíos
                    if pd.isna(value) or value == '':
                        record[target_col] = '' if target_col != 'id' else None
                    else:
                        # Convertir 'id' a entero
                        if target_col == 'id':
                            try:
                                record[target_col] = int(float(str(value)))
                            except (ValueError, TypeError):
                                record[target_col] = None
                        else:
                            record[target_col] = clean_text_value(str(value))
                else:
                    record[target_col] = '' if target_col != 'id' else None
            result_data.append(record)
        
        # 5. Filtrar registros vacíos (donde todas las columnas críticas están vacías)
        critical_columns = ['banco', 'id', 'referencia_proceso']
        filtered_data = []
        for record in result_data:
            # Verificar que banco y referencia_proceso no estén vacíos, y que id sea un número válido
            if (record.get('banco', '').strip() and 
                record.get('referencia_proceso', '').strip() and 
                record.get('id') is not None and 
                isinstance(record.get('id'), int)):
                filtered_data.append(record)
        
        print(f"  - Registros válidos después de filtrar: {len(filtered_data)}")
        
        # 6. Agregar fecha de extracción si no existe
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for record in filtered_data:
            if not record.get('fecha_extraccion', '').strip():
                record['fecha_extraccion'] = current_timestamp
        
        # 7. Guardar archivo JSON
        if filtered_data:
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, "emp_contratos_index.json")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(filtered_data, f, indent=2, ensure_ascii=False, separators=(',', ':'))
            
            # Calcular tamaño del archivo
            file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
            
            print(f"  ✓ Archivo guardado: emp_contratos_index.json")
            print(f"  ✓ Registros: {len(filtered_data)}")
            print(f"  ✓ Columnas: 5 (banco, id, referencia_proceso, referencia_contrato, fecha_extraccion)")
            print(f"  ✓ Tamaño: {file_size_mb:.3f} MB")
            
            # Mostrar resumen de datos
            print(f"\nResumen emp_contratos_index.json:")
            for col in column_mapping.keys():
                if col == 'id':
                    # Para id (entero), contar valores no nulos
                    non_empty_count = sum(1 for record in filtered_data if record.get(col) is not None)
                else:
                    # Para otros campos (texto), contar valores no vacíos
                    non_empty_count = sum(1 for record in filtered_data if record.get(col, '').strip())
                print(f"  - {col}: {non_empty_count}/{len(filtered_data)} valores válidos")
            
            # Mostrar algunos ejemplos de registros
            if len(filtered_data) > 0:
                print(f"\nEjemplos de registros:")
                for i, record in enumerate(filtered_data[:3]):
                    print(f"  Registro {i+1}: banco='{record.get('banco', '')}', id='{record.get('id', '')}', ref_proceso='{record.get('referencia_proceso', '')}'")
            
            return output_file
        else:
            print("  ⚠️ No se encontraron registros válidos para procesar")
            return None
            
    except Exception as e:
        print(f"Error creando emp_contratos_index.json: {e}")
        import traceback
        traceback.print_exc()
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
        # Crear únicamente emp_contratos_index.json desde seguimiento_contratos_emprestito.xlsx
        print("\n" + "="*60)
        print("CREANDO EMP_CONTRATOS_INDEX.JSON")
        print("="*60)
        create_emp_contratos_index_json(output_dir)
        
        print(f"\n¡Transformación de empréstito completada exitosamente!")
        print(f"Archivo guardado en: {output_dir}")
        print("\nArchivo creado:")
        print("  - emp_contratos_index.json (banco, id, referencia_proceso, referencia_contrato, fecha_extraccion)")
        
    except Exception as e:
        print(f"Error durante la transformación: {e}")
        raise


if __name__ == "__main__":
    main()
