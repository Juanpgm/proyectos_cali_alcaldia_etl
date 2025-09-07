import pandas as pd
import os
import re
from datetime import datetime
from tqdm import tqdm

def clean_column_names(df):
    """
    Limpia nombres de columnas según las reglas especificadas:
    - Convertir a minúsculas
    - Cambiar espacios por "_"
    - Eliminar conectores: por, para, de, las, los, con, etc.
    """
    print("🔄 Limpiando nombres de columnas...")
    0
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
    print(f"✅ Nombres de columnas limpiados: {len(df.columns)} columnas")
    return df

def protect_urls(df):
    """
    Protege las URLs en columnas específicas durante el procesamiento
    """
    print("🔄 Protegiendo URLs de corrupción...")
    
    # Identificar columnas que contienen URLs
    url_columns = [col for col in df.columns if 'url' in col.lower()]
    
    if url_columns:
        print(f"🔗 Columnas con URLs encontradas: {url_columns}")
        
        for col in url_columns:
            # Asegurar que las URLs se mantengan como strings sin procesamiento adicional
            df[col] = df[col].astype(str)
            
            # Reemplazar valores nan/None con string vacío para evitar problemas
            df[col] = df[col].replace(['nan', 'None', 'NaN'], '')
            
            print(f"  ✅ {col}: URLs protegidas")
    else:
        print("ℹ️ No se encontraron columnas con URLs")
    
    return df

def remove_unwanted_columns(df):
    """
    Elimina columnas específicas que no son necesarias
    """
    print("🔄 Eliminando columnas innecesarias...")
    
    # Lista de columnas a eliminar
    columns_to_remove = [
        'nit_entidad', 
        'departamento', 
        'ciudad', 
        'localización', 
        'orden', 
        'rama', 
        'condiciones_entrega'
    ]
    
    # Verificar qué columnas existen realmente en el DataFrame
    existing_columns_to_remove = [col for col in columns_to_remove if col in df.columns]
    
    if existing_columns_to_remove:
        df = df.drop(columns=existing_columns_to_remove)
        print(f"✅ Eliminadas {len(existing_columns_to_remove)} columnas: {existing_columns_to_remove}")
    else:
        print("ℹ️ No se encontraron columnas para eliminar")
    
    print(f"📊 Columnas restantes: {len(df.columns)}")
    return df

def remove_unwanted_columns(df):
    """
    Elimina columnas específicas que no son necesarias
    """
    print("🔄 Eliminando columnas no deseadas...")
    
    # Lista de columnas a eliminar
    columns_to_remove = [
        'nit_entidad', 
        'departamento', 
        'ciudad', 
        'localización', 
        'orden', 
        'rama', 
        'condiciones_entrega'
    ]
    
    # Filtrar solo las columnas que existen en el DataFrame
    existing_columns_to_remove = [col for col in columns_to_remove if col in df.columns]
    
    if existing_columns_to_remove:
        df = df.drop(columns=existing_columns_to_remove)
        print(f"✅ Eliminadas {len(existing_columns_to_remove)} columnas: {existing_columns_to_remove}")
    else:
        print("ℹ️ No se encontraron columnas para eliminar")
    
    print(f"📊 Columnas restantes: {len(df.columns)}")
    return df

def standardize_dates(df):
    """
    Convierte fechas al estándar ISO 8601 (YYYY-MM-DD)
    """
    print("🔄 Estandarizando fechas a formato ISO 8601...")
    
    # Identificar columnas de fecha
    date_columns = [col for col in df.columns if 'fecha' in col.lower()]
    
    print(f"📅 Columnas de fecha encontradas: {len(date_columns)}")
    
    for col in tqdm(date_columns, desc="Procesando fechas"):
        try:
            # Convertir a datetime y luego a formato ISO
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            
            # Reemplazar NaT con None
            df[col] = df[col].replace('NaT', None)
            
        except Exception as e:
            print(f"⚠️ Error procesando fecha en columna {col}: {e}")
    
    print("✅ Fechas estandarizadas a formato ISO 8601")
    return df

def convert_monetary_to_numeric(df):
    """
    Convierte datos monetarios a valores numéricos enteros
    """
    print("🔄 Convirtiendo valores monetarios a enteros...")
    
    # Identificar columnas monetarias
    monetary_columns = [col for col in df.columns if any(keyword in col.lower() for keyword in 
                       ['valor', 'monto', 'precio', 'costo', 'pago', 'facturado', 'pendiente', 
                        'pagado', 'amortizado', 'saldo', 'cdp', 'vigencia', 'presupuesto'])]
    
    # Agregar columnas específicas que también son monetarias
    specific_monetary_columns = [
        'recursos_propios_alcaldías_gobernaciones_resguardos_indígenas',
        'recursos_propios',
        'presupuesto_general_nacion_pgn',
        'sistema_general_participaciones',
        'sistema_general_regalías',
        'recursos_credito'
    ]
    
    # Agregar columnas específicas que existen en el DataFrame
    for col in specific_monetary_columns:
        if col in df.columns and col not in monetary_columns:
            monetary_columns.append(col)
    
    print(f"💰 Columnas monetarias encontradas: {len(monetary_columns)}")
    
    for col in tqdm(monetary_columns, desc="Procesando valores monetarios"):
        try:
            # Convertir a string primero
            df[col] = df[col].astype(str)
            
            # Limpiar formato monetario
            df[col] = df[col].str.replace(r'[$,\s]', '', regex=True)  # Eliminar $, comas y espacios
            df[col] = df[col].str.replace(r'[^\d.-]', '', regex=True)  # Mantener solo dígitos, punto y guión
            
            # Convertir a numérico
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convertir a entero (rellenar NaN con 0)
            df[col] = df[col].fillna(0).astype('int64')
            
        except Exception as e:
            print(f"⚠️ Error procesando valor monetario en columna {col}: {e}")
    
    print("✅ Valores monetarios convertidos a enteros")
    return df

def clean_nan_values(df):
    """
    Reemplaza valores NaN, 'NaN', 'nan' por None para mejor manejo en JSON
    """
    print("🔄 Limpiando valores NaN...")
    
    # Contar valores NaN antes de la limpieza
    nan_count_before = df.isna().sum().sum()
    
    # Reemplazar diferentes representaciones de NaN
    df = df.replace({
        'NaN': None,
        'nan': None,
        'NaT': None,
        'null': None,
        'NULL': None,
        '': None
    })
    
    # Reemplazar pandas NaN con None
    df = df.where(pd.notnull(df), None)
    
    # Contar None después de la limpieza
    none_count_after = df.isnull().sum().sum()
    
    print(f"✅ Valores NaN limpiados: {nan_count_before} → {none_count_after} valores None")
    
    return df

def rename_and_convert_bpin(df):
    """
    Renombra 'código_bpin' a 'bpin' y convierte a entero
    """
    print("🔄 Procesando columna BPIN...")
    
    if 'código_bpin' in df.columns:
        # Renombrar columna
        df = df.rename(columns={'código_bpin': 'bpin'})
        print("✅ Columna renombrada: 'código_bpin' → 'bpin'")
        
        # Convertir a entero
        try:
            # Limpiar valores no numéricos
            df['bpin'] = df['bpin'].astype(str)
            df['bpin'] = df['bpin'].str.replace(r'[^\d]', '', regex=True)
            
            # Convertir a entero
            df['bpin'] = pd.to_numeric(df['bpin'], errors='coerce')
            df['bpin'] = df['bpin'].fillna(0).astype('int64')
            
            print("✅ Columna BPIN convertida a entero")
        except Exception as e:
            print(f"⚠️ Error convirtiendo BPIN a entero: {e}")
    else:
        print("⚠️ Columna 'código_bpin' no encontrada")
    
    return df

def group_by_bpin_and_optimize(df):
    """
    Agrupa los registros por BPIN conservando TODOS los datos de cada registro
    """
    print("🔄 Agrupando registros por BPIN (conservando todos los datos)...")
    
    if 'bpin' not in df.columns:
        print("❌ Columna 'bpin' no encontrada para agrupar")
        return df
    
    # Agrupar por BPIN
    grouped_data = {}
    
    # Obtener estadísticas generales
    unique_bpins = df[df['bpin'] != 0]['bpin'].unique()  # Excluir valores 0
    
    for bpin_value in tqdm(unique_bpins, desc="Procesando BPINs"):
        bpin_records = df[df['bpin'] == bpin_value]
        
        # Crear estructura con todos los datos
        # Filtrar fechas válidas para estadísticas
        fechas_validas = pd.to_datetime(bpin_records['fecha_firma'], errors='coerce').dropna()
        
        bpin_data = {
            'bpin': int(bpin_value),
            'resumen': {
                'total_contratos': len(bpin_records),
                'valor_total': int(bpin_records['valor_contrato'].sum()),
                'valor_promedio': int(bpin_records['valor_contrato'].mean()),
                'entidades_participantes': list(bpin_records['nombre_entidad'].unique()),
                'tipos_contrato': list(bpin_records['tipo_contrato'].unique()),
                'estados_contrato': list(bpin_records['estado_contrato'].unique()),
                'fecha_primer_contrato': fechas_validas.min().strftime('%Y-%m-%d') if len(fechas_validas) > 0 else None,
                'fecha_ultimo_contrato': fechas_validas.max().strftime('%Y-%m-%d') if len(fechas_validas) > 0 else None
            },
            'contratos': []
        }
        
        # Agregar TODOS los contratos con TODOS sus datos
        for _, record in bpin_records.iterrows():
            # Convertir la fila completa a diccionario, preservando todos los campos
            contract = record.to_dict()
            bpin_data['contratos'].append(contract)
        
        grouped_data[str(bpin_value)] = bpin_data
    
    # Crear estructura final con metadata
    result = {
        'metadata': {
            'fecha_procesamiento': pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_bpins': len(grouped_data),
            'total_contratos': len(df[df['bpin'] != 0]),
            'contratos_por_bpin_promedio': len(df[df['bpin'] != 0]) / len(grouped_data) if len(grouped_data) > 0 else 0,
            'valor_total_todos_contratos': int(df[df['bpin'] != 0]['valor_contrato'].sum())
        },
        'datos_por_bpin': grouped_data
    }
    
    print(f"✅ Agrupación completada:")
    print(f"  📊 Total BPINs únicos: {len(grouped_data)}")
    print(f"  📊 Total contratos: {len(df[df['bpin'] != 0])}")
    print(f"  📊 Promedio contratos por BPIN: {len(df[df['bpin'] != 0])/len(grouped_data):.1f}")
    
    return result

def create_bpin_summary(df):
    """
    Crea un resumen JSON optimizado con solo los campos esenciales:
    bpin, referencia_contrato, proceso_compra, id_contrato, urlproceso
    """
    print("🔄 Creando resumen optimizado por BPIN...")
    
    if 'bpin' not in df.columns:
        print("❌ Columna 'bpin' no encontrada para el resumen")
        return {}
    
    # Filtrar solo registros con BPIN válido (diferente de 0)
    valid_bpin_df = df[df['bpin'] != 0].copy()
    
    # Seleccionar solo las columnas necesarias
    required_columns = ['bpin', 'referencia_contrato', 'proceso_compra', 'id_contrato', 'urlproceso']
    summary_df = valid_bpin_df[required_columns].copy()
    
    # Agrupar por BPIN
    grouped_summary = {}
    unique_bpins = summary_df['bpin'].unique()
    
    for bpin_value in tqdm(unique_bpins, desc="Procesando BPINs para resumen"):
        bpin_records = summary_df[summary_df['bpin'] == bpin_value]
        
        # Crear lista de contratos para este BPIN
        contratos = []
        for _, record in bpin_records.iterrows():
            contrato = {
                'referencia_contrato': record['referencia_contrato'],
                'proceso_compra': record['proceso_compra'],
                'id_contrato': record['id_contrato'],
                'urlproceso': record['urlproceso']
            }
            contratos.append(contrato)
        
        grouped_summary[str(int(bpin_value))] = {
            'bpin': int(bpin_value),
            'total_contratos': len(contratos),
            'contratos': contratos
        }
    
    print(f"✅ Resumen creado para {len(grouped_summary)} BPINs")
    return grouped_summary

def export_optimized_json(grouped_data, output_path):
    """
    Exporta el JSON optimizado agrupado por BPIN
    """
    print(f"🔄 Exportando JSON optimizado agrupado por BPIN...")
    
    # Crear directorio si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Crear estructura final optimizada
    optimized_structure = {
        'metadata': {
            'total_bpins': len(grouped_data),
            'total_contratos': sum(bpin_data['resumen']['total_contratos'] for bpin_data in grouped_data.values()),
            'valor_total_general': sum(bpin_data['resumen']['valor_total'] for bpin_data in grouped_data.values()),
            'fecha_procesamiento': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        'bpins': grouped_data
    }
    
    # Escribir JSON con formato legible (indentación y espacios)
    import json
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(optimized_structure, f, ensure_ascii=False, indent=2, separators=(',', ': '))
    
    # Verificar tamaño del archivo
    file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
    print(f"✅ JSON optimizado exportado: {output_path}")
    print(f"📂 Tamaño del archivo: {file_size:.1f} MB")
    print(f"📊 Estructura: {len(grouped_data)} BPINs con {optimized_structure['metadata']['total_contratos']} contratos")
    
    return optimized_structure

def load_and_filter_contratos_secop():
    """
    Lee los datos de contratos SECOP y aplica filtros específicos.
    
    Elimina todas las filas donde:
    - Tipo de Contrato = "Prestación de servicios" O
    - Código BPIN = "No Definido"
    
    Returns:
        pd.DataFrame: DataFrame filtrado
    """
    
    # Ruta del archivo de datos
    input_path = "app_inputs/contratos_input/SECOP/SECOP_II_-_Contratos_Electrónicos_20250906.csv"
    
    # Verificar que el archivo existe
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"No se encontró el archivo: {input_path}")
    
    print("🔄 Leyendo datos de contratos SECOP...")
    
    # Leer el archivo CSV con barra de progreso
    tqdm.pandas(desc="Cargando CSV")
    df = pd.read_csv(input_path, low_memory=False)
    
    print(f"✅ Datos cargados: {len(df):,} registros")
    
    # Aplicar el filtro con barra de progreso
    print("🔄 Aplicando filtros...")
    
    # Mostrar información antes del filtrado
    prestacion_servicios = df[df['Tipo de Contrato'] == 'Prestación de servicios']
    no_definido_bpin = df[df['Código BPIN'] == 'No Definido']
    
    print(f"📊 Registros con 'Prestación de servicios': {len(prestacion_servicios):,}")
    print(f"📊 Registros con Código BPIN 'No Definido': {len(no_definido_bpin):,}")
    
    # Crear máscara para eliminar registros que cumplan CUALQUIERA de las condiciones
    mask_to_remove = (
        (df['Tipo de Contrato'] == 'Prestación de servicios') |
        (df['Código BPIN'] == 'No Definido')
    )
    
    registros_que_cumplen_filtros = df[mask_to_remove]
    print(f"📊 Registros que serán eliminados (cualquier condición): {len(registros_que_cumplen_filtros):,}")
    
    # Aplicar filtro - eliminar todas las filas que cumplan cualquiera de las condiciones
    df_filtered = df[~mask_to_remove].copy()
    
    registros_eliminados = len(df) - len(df_filtered)
    print(f"✅ Filtrado completo: {registros_eliminados:,} registros eliminados")
    print(f"📊 Registros finales: {len(df_filtered):,}")
    
    return df_filtered

def transform_dataframe(df):
    """
    Aplica todas las transformaciones al DataFrame
    """
    print("🔄 Iniciando transformaciones del DataFrame...")
    
    # 1. Limpiar nombres de columnas
    df = clean_column_names(df)
    
    # 2. Proteger URLs antes de otras transformaciones
    df = protect_urls(df)
    
    # 3. Eliminar columnas innecesarias
    df = remove_unwanted_columns(df)
    
    # 4. Renombrar y convertir BPIN
    df = rename_and_convert_bpin(df)
    
    # 5. Renombrar link_proceso a urlProceso
    if 'link_proceso' in df.columns:
        df = df.rename(columns={'link_proceso': 'urlProceso'})
        print("✅ Columna renombrada: 'link_proceso' → 'urlProceso'")
    
    # 6. Estandarizar fechas
    df = standardize_dates(df)
    
    # 7. Convertir valores monetarios
    df = convert_monetary_to_numeric(df)
    
    # 8. Limpiar valores NaN
    df = clean_nan_values(df)
    
    print("✅ Todas las transformaciones completadas")
    return df

def export_to_json(df, output_path):
    """Exporta el DataFrame a JSON con barra de progreso, preservando URLs sin escape"""
    
    print(f"🔄 Exportando {len(df):,} registros a JSON...")
    
    # Crear directorio si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Convertir DataFrame a diccionarios para control manual del JSON
    records = df.to_dict(orient='records')
    
    # Escribir JSON manualmente para evitar escape de URLs
    import json
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, separators=(',', ': '))
    
    # Verificar tamaño del archivo
    file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
    print(f"✅ Archivo JSON exportado: {output_path}")
    print(f"📂 Tamaño del archivo: {file_size:.1f} MB")
    
    # Verificar que las URLs se preservaron correctamente
    print("🔗 Verificando integridad de URLs...")
    with open(output_path, 'r', encoding='utf-8') as f:
        sample_data = f.read(2000)  # Leer una muestra más grande
        if 'https://community.secop.gov.co' in sample_data:
            print("✅ URLs de SECOP detectadas correctamente sin escape")
        elif 'https:\\/\\/' in sample_data:
            print("⚠️ URLs detectadas pero con escape excesivo")
        else:
            print("⚠️ No se detectaron URLs en la muestra del archivo")

def main():
    """Función principal para ejecutar la transformación"""
    try:
        # Cargar y filtrar los datos
        df_contratos = load_and_filter_contratos_secop()
        
        # Aplicar transformaciones de datos
        df_contratos = transform_dataframe(df_contratos)
        
        # Exportar JSON normal (individual records)
        output_path = "app_outputs/contratos_secop_output/contratos_proyectos.json"
        export_to_json(df_contratos, output_path)
        
        # Crear resumen optimizado por BPIN
        print("\n🔄 Creando resumen optimizado por BPIN...")
        bpin_summary = create_bpin_summary(df_contratos)
        
        # Exportar resumen JSON
        summary_path = "app_outputs/contratos_secop_output/contratos_proyectos_index.json"
        os.makedirs(os.path.dirname(summary_path), exist_ok=True)
        
        import json
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(bpin_summary, f, ensure_ascii=False, indent=2, separators=(',', ': '))
        
        # Verificar tamaño del archivo
        summary_size = os.path.getsize(summary_path) / (1024 * 1024)  # MB
        print(f"✅ Resumen JSON exportado: {summary_path}")
        print(f"📂 Tamaño del archivo: {summary_size:.1f} MB")
        
        print("\n🎉 Proceso completado exitosamente")
        print("📁 Archivos generados:")
        print(f"  1. JSON individual: {output_path}")
        print(f"  2. Resumen por BPIN: {summary_path}")
        
        return df_contratos, bpin_summary
        
    except Exception as e:
        print(f"❌ Error durante la transformación: {str(e)}")
        raise

if __name__ == "__main__":
    df_result = main()
