import pandas as pd
import json
import os
import re
import numpy as np
from datetime import datetime
from collections import defaultdict

def clean_column_names(df):
    """
    Limpia nombres de columnas según las reglas especificadas:
    - Convertir a minúsculas
    - Cambiar espacios por "_"
    - Eliminar conectores: por, para, de, las, los, con, etc.
    """
    print("🔄 Limpiando nombres de columnas...")
    
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
    print(f"✅ Nombres de columnas limpiados: {len(new_columns)} columnas")
    
    # Mostrar algunos ejemplos de cambios
    print("📋 Ejemplos de cambios en nombres de columnas:")
    try:
        # Intentar con la ruta desde el directorio de trabajo actual
        csv_path = "transformation_app/app_inputs/procesos_input/SECOP_II_-_Procesos_de_Contratación_20250909.csv"
        if not os.path.exists(csv_path):
            # Intentar con ruta relativa desde el script
            csv_path = "app_inputs/procesos_input/SECOP_II_-_Procesos_de_Contratación_20250909.csv"
        
        if os.path.exists(csv_path):
            original_cols = list(pd.read_csv(csv_path, nrows=0).columns)
            for i in range(min(5, len(original_cols))):
                print(f"   '{original_cols[i]}' → '{new_columns[i]}'")
        else:
            print("   (No se pudo cargar archivo para mostrar ejemplos)")
    except Exception as e:
        print(f"   (Error mostrando ejemplos: {e})")
    
    return df

def clean_monetary_values(df):
    """
    Convierte valores monetarios a enteros, eliminando símbolos y comas
    """
    print("💰 Limpiando valores monetarios...")
    
    # Columnas que típicamente contienen valores monetarios
    monetary_columns = []
    for col in df.columns:
        if any(keyword in col.lower() for keyword in ['precio', 'valor', 'presupuesto', 'costo', 'monto']):
            monetary_columns.append(col)
    
    print(f"📊 Columnas monetarias identificadas: {monetary_columns}")
    
    for col in monetary_columns:
        if col in df.columns:
            print(f"   Procesando columna: {col}")
            # Convertir a string primero para manejar NaN
            df[col] = df[col].astype(str)
            
            # Limpiar valores monetarios
            df[col] = df[col].str.replace('$', '', regex=False)
            df[col] = df[col].str.replace(',', '', regex=False)
            df[col] = df[col].str.replace('.', '', regex=False)
            df[col] = df[col].str.replace(' ', '', regex=False)
            df[col] = df[col].str.replace('nan', '', regex=False)
            df[col] = df[col].str.replace('NaN', '', regex=False)
            
            # Convertir a numérico, NaN para valores inválidos
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convertir a entero donde sea posible, manteniendo NaN como NaN
            # No reemplazar con 0 para mantener los NaN originales
            mask = df[col].notna()
            df.loc[mask, col] = df.loc[mask, col].astype('int64')
    
    print(f"✅ Valores monetarios procesados")
    return df

def clean_date_columns(df):
    """
    Convierte fechas al formato ISO 8601 (YYYY-MM-DD)
    """
    print("📅 Limpiando fechas para cumplir ISO 8601...")
    
    # Identificar columnas de fecha
    date_columns = []
    for col in df.columns:
        if 'fecha' in col.lower():
            date_columns.append(col)
    
    print(f"📊 Columnas de fecha identificadas: {date_columns}")
    
    for col in date_columns:
        if col in df.columns:
            print(f"   Procesando columna de fecha: {col}")
            try:
                # Convertir a datetime con múltiples formatos
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                
                # Convertir a formato ISO 8601 (YYYY-MM-DD)
                df[col] = df[col].dt.strftime('%Y-%m-%d')
                
                # Reemplazar 'NaT' string con None
                df[col] = df[col].replace('NaT', None)
                
            except Exception as e:
                print(f"   ⚠️ Error procesando {col}: {e}")
    
    print(f"✅ Fechas convertidas a formato ISO 8601")
    return df

def clean_nan_values(df):
    """
    Reemplaza valores NaN con None para mejor serialización JSON
    """
    print("🔄 Reemplazando valores NaN con None...")
    
    # Reemplazar diferentes tipos de NaN
    df = df.replace({np.nan: None})
    df = df.replace({pd.NaT: None})
    df = df.replace({'nan': None})
    df = df.replace({'NaN': None})
    df = df.replace({'': None})
    
    print(f"✅ Valores NaN reemplazados con None")
    return df

def create_procesos_proyectos_index(cleaned_data):
    """
    Crea procesos_proyectos_index.json que contiene procesos agrupados por BPIN
    con los campos: bpin, referencia_proceso, proceso_compra, urlproceso
    """
    print("\n🚀 Creando procesos_proyectos_index.json")
    print("=" * 60)
    
    # Ruta del índice de contratos
    contratos_index_path = "transformation_app/app_outputs/contratos_secop_output/contratos_proyectos_index.json"
    output_path = "transformation_app/app_outputs/procesos_secop_output/procesos_proyectos_index.json"
    
    # Verificar que exista el archivo de contratos
    if not os.path.exists(contratos_index_path):
        print(f"❌ No se encontró el archivo: {contratos_index_path}")
        return
    
    try:
        # Cargar el índice de contratos para obtener el mapeo proceso_compra -> bpin
        print("🔄 Cargando índice de contratos...")
        with open(contratos_index_path, 'r', encoding='utf-8') as f:
            contratos_index = json.load(f)
        
        # Crear mapeo proceso_compra -> (bpin, urlproceso)
        proceso_compra_to_info = {}
        
        for bpin_str, bpin_data in contratos_index.items():
            bpin = bpin_data.get('bpin')
            
            for contrato in bpin_data.get('contratos', []):
                proceso_compra = contrato.get('proceso_compra')
                urlproceso = contrato.get('urlproceso')
                if proceso_compra:
                    proceso_compra_to_info[proceso_compra] = {
                        'bpin': bpin,
                        'urlproceso': urlproceso
                    }
        
        print(f"✅ Índice de contratos cargado: {len(proceso_compra_to_info):,} mapeos proceso_compra válidos")
        
        # Agrupar procesos por BPIN
        print("🔄 Agrupando procesos por BPIN...")
        procesos_por_bpin = defaultdict(list)
        procesos_sin_bpin = 0
        
        for proceso in cleaned_data:
            proceso_compra = proceso.get('proceso_compra')
            referencia_proceso = proceso.get('referencia_proceso')
            
            # Verificar si este proceso_compra existe en el índice de contratos
            if proceso_compra in proceso_compra_to_info:
                info = proceso_compra_to_info[proceso_compra]
                bpin = info['bpin']
                
                proceso_info = {
                    'referencia_proceso': referencia_proceso,
                    'proceso_compra': proceso_compra,
                    'urlproceso': info['urlproceso']
                }
                procesos_por_bpin[bpin].append(proceso_info)
            else:
                procesos_sin_bpin += 1
        
        print(f"✅ Agrupación completada:")
        print(f"   - BPINs con procesos: {len(procesos_por_bpin)}")
        print(f"   - Procesos sin BPIN: {procesos_sin_bpin:,}")
        
        # Crear la estructura final agrupada por BPIN
        print("🔄 Creando estructura final agrupada...")
        procesos_proyectos_index = {}
        
        for bpin, procesos_lista in procesos_por_bpin.items():
            procesos_proyectos_index[str(bpin)] = {
                'bpin': bpin,
                'total_procesos': len(procesos_lista),
                'procesos': procesos_lista
            }
        
        # Crear directorio de salida si no existe
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        
        # Guardar el archivo JSON agrupado por BPIN
        print("🔄 Guardando archivo JSON...")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(procesos_proyectos_index, f, ensure_ascii=False, indent=2)
        
        # Información del archivo generado
        file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
        total_procesos = sum(len(data['procesos']) for data in procesos_proyectos_index.values())
        
        print(f"✅ Archivo procesos_proyectos_index.json generado exitosamente:")
        print(f"   📂 Archivo: {output_path}")
        print(f"   📂 Tamaño: {file_size:.1f} MB")
        print(f"   📊 Total BPINs: {len(procesos_proyectos_index):,}")
        print(f"   📊 Total procesos: {total_procesos:,}")
        
        # Mostrar algunos ejemplos
        print(f"\n📋 Ejemplos de la estructura generada:")
        count = 0
        for bpin, data in procesos_proyectos_index.items():
            if count >= 3:
                break
            print(f"   BPIN {bpin}: {data['total_procesos']} procesos")
            if data['procesos']:
                ejemplo = data['procesos'][0]
                print(f"      Ejemplo: {ejemplo['referencia_proceso']} ({ejemplo['proceso_compra']})")
            count += 1
        
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error durante la creación del índice de procesos: {str(e)}")
        raise

def main():
    """
    Convierte datos CSV de procesos SECOP a JSON, 
    excluyendo registros con tipo_contrato = 'Prestación de servicios'
    y filtrando por proceso_compra que existan en contratos_proyectos_index.json
    """
    print("🚀 Iniciando conversión de datos de procesos SECOP a JSON")
    print("=" * 60)
    
    # Ruta del archivo de entrada
    input_path = "transformation_app/app_inputs/procesos_input/SECOP_II_-_Procesos_de_Contratación_20250909.csv"
    contratos_index_path = "transformation_app/app_outputs/contratos_secop_output/contratos_proyectos_index.json"
    
    if not os.path.exists(input_path):
        print(f"❌ No se encontró el archivo: {input_path}")
        return
    
    if not os.path.exists(contratos_index_path):
        print(f"❌ No se encontró el archivo de índice de contratos: {contratos_index_path}")
        return
    
    try:
        # Cargar el índice de contratos para obtener los proceso_compra válidos
        print("🔄 Cargando índice de contratos...")
        with open(contratos_index_path, 'r', encoding='utf-8') as f:
            contratos_index = json.load(f)
        
        # Extraer todos los proceso_compra del índice de contratos
        valid_proceso_compras = set()
        for bpin_data in contratos_index.values():
            for contrato in bpin_data.get('contratos', []):
                if 'proceso_compra' in contrato:
                    valid_proceso_compras.add(contrato['proceso_compra'])
        
        print(f"✅ Índice de contratos cargado: {len(valid_proceso_compras):,} proceso_compra únicos")
        
        # Cargar el CSV
        print("🔄 Cargando datos desde CSV...")
        df = pd.read_csv(input_path, encoding='utf-8')
        print(f"✅ Datos cargados: {len(df):,} registros, {len(df.columns)} columnas")
        
        # Limpiar nombres de columnas
        df = clean_column_names(df)
        
        # Cambiar nombre de columna "id_portafolio" a "proceso_compra"
        if 'id_portafolio' in df.columns:
            df = df.rename(columns={'id_portafolio': 'proceso_compra'})
            print(f"✅ Columna 'id_portafolio' renombrada a 'proceso_compra'")
        else:
            print(f"⚠️ No se encontró la columna 'id_portafolio'")
            print(f"📋 Columnas disponibles: {list(df.columns)}")
        
        # Aplicar transformaciones de calidad de datos
        print(f"\n🔄 Aplicando transformaciones de calidad de datos...")
        df = clean_date_columns(df)
        df = clean_monetary_values(df)
        df = clean_nan_values(df)
        
        # Filtrar por proceso_compra que existan en el índice de contratos
        if 'proceso_compra' in df.columns:
            print(f"\n🔍 Filtrando por proceso_compra válidos...")
            registros_antes_filtro = len(df)
            df = df[df['proceso_compra'].isin(valid_proceso_compras)].copy()
            registros_filtrados = registros_antes_filtro - len(df)
            
            print(f"   - Registros antes del filtro: {registros_antes_filtro:,}")
            print(f"   - Registros filtrados (no en contratos): {registros_filtrados:,}")
            print(f"   - Registros restantes: {len(df):,}")
        else:
            print("⚠️ No se pudo aplicar filtro por proceso_compra (columna no encontrada)")
        
        # Buscar la columna de tipo de contrato (ahora con nombres limpiados)
        tipo_contrato_col = None
        for col in df.columns:
            if 'tipo' in col.lower() and 'contrato' in col.lower():
                tipo_contrato_col = col
                break
        
        if tipo_contrato_col:
            print(f"\n📋 Columna encontrada: '{tipo_contrato_col}'")
            
            # Mostrar tipos de contrato disponibles
            tipos_contrato = df[tipo_contrato_col].value_counts()
            print(f"📊 Tipos de contrato encontrados:")
            for tipo, count in tipos_contrato.items():
                print(f"   - {tipo}: {count:,} registros")
            
            # Filtrar excluyendo "Prestación de servicios"
            registros_antes = len(df)
            df_filtered = df[df[tipo_contrato_col] != "Prestación de servicios"].copy()
            registros_eliminados = registros_antes - len(df_filtered)
            
            print(f"\n🗑️ Eliminando registros de 'Prestación de servicios':")
            print(f"   - Registros eliminados: {registros_eliminados:,}")
            print(f"   - Registros restantes: {len(df_filtered):,}")
            
        else:
            print("\n⚠️ No se encontró columna de tipo de contrato")
            print("📋 Columnas disponibles:")
            for col in df.columns:
                print(f"   - {col}")
            df_filtered = df.copy()
        
        # Eliminar columnas específicas no deseadas
        columns_to_remove = [
            'nit_entidad', 
            'departamento_entidad', 
            'ciudad_entidad', 
            'categorias_adicionales', 
            'codigo_entidad', 
            'ordenentidad'
        ]
        
        print(f"\n🗑️ Eliminando columnas específicas:")
        existing_columns_to_remove = [col for col in columns_to_remove if col in df_filtered.columns]
        
        if existing_columns_to_remove:
            df_filtered = df_filtered.drop(columns=existing_columns_to_remove)
            print(f"   - Columnas eliminadas: {existing_columns_to_remove}")
            print(f"   - Columnas restantes: {len(df_filtered.columns)}")
        else:
            print(f"   - No se encontraron columnas para eliminar")
        
        # Convertir a JSON
        print(f"\n🔄 Convirtiendo a JSON...")
        data_dict = df_filtered.to_dict('records')
        
        # Limpiar datos para serialización JSON
        print(f"🔄 Limpiando datos para serialización JSON...")
        cleaned_data = []
        for record in data_dict:
            cleaned_record = {}
            for key, value in record.items():
                # Manejar diferentes tipos de NaN/None
                if pd.isna(value) or value is pd.NaT or str(value) in ['nan', 'NaN', 'NaT']:
                    cleaned_record[key] = None
                elif isinstance(value, (np.integer)):
                    cleaned_record[key] = int(value)
                elif isinstance(value, (np.floating)):
                    if np.isnan(value):
                        cleaned_record[key] = None
                    else:
                        # Para columnas monetarias, convertir a entero
                        if any(keyword in key.lower() for keyword in ['precio', 'valor', 'presupuesto', 'costo', 'monto']):
                            cleaned_record[key] = int(value)
                        else:
                            cleaned_record[key] = float(value)
                else:
                    cleaned_record[key] = value
            cleaned_data.append(cleaned_record)
        
        # Crear directorio de salida si no existe
        output_dir = "transformation_app/app_outputs/procesos_secop_output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Guardar como procesos_secop.json en el directorio de salida
        output_path = f"{output_dir}/procesos_secop.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, ensure_ascii=False, indent=2, default=str)
        
        # Información del archivo generado
        file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
        print(f"✅ JSON generado exitosamente:")
        print(f"   📂 Archivo: {output_path}")
        print(f"   📂 Tamaño: {file_size:.1f} MB")
        print(f"   📊 Registros: {len(cleaned_data):,}")
        print(f"   📅 Fechas en formato ISO 8601")
        print(f"   💰 Valores monetarios como enteros")
        print(f"   🔄 Valores NaN reemplazados por None")
        
        # Crear el índice de procesos por proyectos
        create_procesos_proyectos_index(cleaned_data)
        
        print("\n🎉 Proceso completado exitosamente")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error durante el proceso: {str(e)}")
        raise

if __name__ == "__main__":
    main()