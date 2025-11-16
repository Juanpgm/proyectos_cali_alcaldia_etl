# -*- coding: utf-8 -*-
"""
Script para analizar valores de la columna 'ano' en unidades_proyecto.json
"""

import json
import os
import pandas as pd
from collections import Counter

# Ruta al archivo
file_path = os.path.join(
    os.path.dirname(__file__),
    'transformation_app',
    'app_inputs',
    'unidades_proyecto_input',
    'unidades_proyecto.json'
)

print("="*80)
print("ANÁLISIS DE VALORES DE LA COLUMNA 'ANO'")
print("="*80)

try:
    # Cargar el archivo JSON
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"\n✓ Archivo cargado: {len(data)} registros\n")
    
    # Crear DataFrame
    df = pd.DataFrame(data)
    
    # Verificar si existe la columna 'ano'
    if 'ano' not in df.columns:
        print("❌ ERROR: La columna 'ano' no existe en el archivo")
        print(f"Columnas disponibles: {list(df.columns)}")
    else:
        print("📊 ANÁLISIS DETALLADO DE LA COLUMNA 'ANO':")
        print("-"*80)
        
        # Estadísticas básicas
        total_records = len(df)
        null_count = df['ano'].isnull().sum()
        not_null_count = df['ano'].notna().sum()
        
        print(f"\n1. ESTADÍSTICAS GENERALES:")
        print(f"   Total de registros: {total_records}")
        print(f"   Valores no nulos: {not_null_count}")
        print(f"   Valores nulos: {null_count}")
        
        # Análisis de tipos de datos
        print(f"\n2. TIPOS DE DATOS ENCONTRADOS:")
        type_counts = df['ano'].apply(lambda x: type(x).__name__ if pd.notna(x) else 'NoneType').value_counts()
        for dtype, count in type_counts.items():
            print(f"   {dtype}: {count} registros")
        
        # Distribución de valores
        print(f"\n3. DISTRIBUCIÓN DE VALORES (Top 20):")
        value_counts = df['ano'].value_counts(dropna=False).head(20)
        for value, count in value_counts.items():
            percentage = (count / total_records) * 100
            print(f"   {str(value):20s}: {count:5d} registros ({percentage:5.2f}%)")
        
        # Análisis de valores únicos
        unique_values = df['ano'].dropna().unique()
        print(f"\n4. VALORES ÚNICOS (sin contar nulos): {len(unique_values)}")
        print(f"   Valores: {sorted([str(x) for x in unique_values])}")
        
        # Identificar valores problemáticos
        print(f"\n5. VALORES PROBLEMÁTICOS:")
        
        # Valores que NO son años válidos (2024-2030)
        valid_anos = {2024, 2025, 2026, 2027, 2028, 2029, 2030}
        
        # Intentar convertir a numérico
        df_temp = df.copy()
        df_temp['ano_numeric'] = pd.to_numeric(df_temp['ano'], errors='coerce')
        
        # Valores que no son numéricos
        non_numeric = df_temp[df_temp['ano_numeric'].isna() & df_temp['ano'].notna()]
        if len(non_numeric) > 0:
            print(f"\n   a) Valores NO NUMÉRICOS: {len(non_numeric)} registros")
            non_numeric_values = non_numeric['ano'].value_counts()
            for value, count in non_numeric_values.items():
                print(f"      '{value}': {count} registros")
        
        # Valores numéricos pero fuera de rango
        df_temp['ano_int'] = df_temp['ano_numeric'].fillna(0).astype(int)
        invalid_numeric = df_temp[
            (df_temp['ano_numeric'].notna()) & 
            (~df_temp['ano_int'].isin(valid_anos)) &
            (df_temp['ano_int'] != 0)
        ]
        if len(invalid_numeric) > 0:
            print(f"\n   b) Valores NUMÉRICOS INVÁLIDOS (fuera de 2024-2030): {len(invalid_numeric)} registros")
            invalid_values = invalid_numeric['ano_int'].value_counts()
            for value, count in invalid_values.items():
                print(f"      {value}: {count} registros")
        
        # Valores cero
        zero_values = df_temp[df_temp['ano_int'] == 0]
        if len(zero_values) > 0:
            print(f"\n   c) Valores CERO: {len(zero_values)} registros")
        
        # Valores vacíos o strings vacíos
        empty_strings = df[(df['ano'] == '') | (df['ano'] == ' ')]
        if len(empty_strings) > 0:
            print(f"\n   d) Strings VACÍOS: {len(empty_strings)} registros")
        
        # Resumen final
        print(f"\n{'='*80}")
        print("6. RESUMEN DE VALIDACIÓN:")
        print("-"*80)
        
        valid_count = df_temp[df_temp['ano_int'].isin(valid_anos)].shape[0]
        invalid_count = total_records - valid_count
        
        print(f"   ✓ Valores VÁLIDOS (2024-2030): {valid_count} ({valid_count/total_records*100:.1f}%)")
        print(f"   ✗ Valores que necesitan REVISIÓN: {invalid_count} ({invalid_count/total_records*100:.1f}%)")
        
        # Breakdown de valores a revisar
        print(f"\n   Breakdown de valores a revisar:")
        print(f"      - Nulos/None: {null_count}")
        print(f"      - No numéricos: {len(non_numeric)}")
        print(f"      - Ceros: {len(zero_values)}")
        print(f"      - Fuera de rango: {len(invalid_numeric)}")
        print(f"      - Strings vacíos: {len(empty_strings)}")
        
        print(f"\n{'='*80}")
        
        # Mostrar algunos ejemplos de registros problemáticos
        if invalid_count > 0:
            print("\n7. EJEMPLOS DE REGISTROS PROBLEMÁTICOS (primeros 10):")
            print("-"*80)
            
            problematic = df_temp[~df_temp['ano_int'].isin(valid_anos)].head(10)
            for idx, row in problematic.iterrows():
                upid = row.get('upid', 'N/A') if 'upid' in row else 'N/A'
                nombre = str(row.get('nombre_up', 'N/A'))[:40] if 'nombre_up' in row else 'N/A'
                ano_original = row.get('ano', 'N/A')
                print(f"   [{idx}] UPID: {upid}")
                print(f"        Nombre: {nombre}")
                print(f"        Año: {repr(ano_original)} (tipo: {type(ano_original).__name__})")
                print()

except FileNotFoundError:
    print(f"❌ ERROR: Archivo no encontrado en {file_path}")
except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
