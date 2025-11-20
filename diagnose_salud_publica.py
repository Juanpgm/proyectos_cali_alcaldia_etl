#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de diagnóstico para verificar el problema con Secretaría de Salud Pública
"""

import json
import pandas as pd

# Cargar el archivo JSON extraído (buscar en múltiples ubicaciones)
import os
possible_paths = [
    'extraction_app/app_inputs/unidades_proyecto_input/unidades_proyecto.json',
    'app_inputs/unidades_proyecto_input/unidades_proyecto.json',
    'cloud_functions/transformation_app/app_inputs/unidades_proyecto_input/unidades_proyecto.json'
]

json_path = None
for path in possible_paths:
    if os.path.exists(path):
        json_path = path
        break

if not json_path:
    print("❌ No se encontró el archivo unidades_proyecto.json")
    exit(1)

with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

df = pd.DataFrame(data)

print("="*80)
print("DIAGNÓSTICO: Secretaría de Salud Pública")
print("="*80)

# Filtrar registros de Secretaría de Salud Pública
if 'nombre_centro_gestor' in df.columns:
    salud_df = df[df['nombre_centro_gestor'] == 'Secretaría de Salud Pública']
    print(f"\nRegistros encontrados: {len(salud_df)}")
else:
    print("\n❌ Campo 'nombre_centro_gestor' NO ENCONTRADO en el DataFrame")
    print(f"Columnas disponibles: {list(df.columns)}")
    
    # Buscar columnas que puedan contener el centro gestor
    possible_cols = [col for col in df.columns if 'centro' in col.lower() or 'secretar' in col.lower() or col == '']
    if possible_cols:
        print(f"\nColumnas relacionadas encontradas: {possible_cols}")
        
        # Verificar si hay columna vacía
        if '' in df.columns:
            print(f"\n⚠️ Hay una columna con nombre VACÍO")
            print(f"Valores únicos en columna vacía: {df[''].unique()[:10]}")
            salud_df = df[df[''] == 'Secretaría de Salud Pública']
        else:
            salud_df = pd.DataFrame()
    else:
        salud_df = pd.DataFrame()

if len(salud_df) > 0:
    print("\n" + "="*80)
    print("ANÁLISIS DE VALORES NUMÉRICOS")
    print("="*80)
    
    # Verificar columnas numéricas
    numeric_cols = ['presupuesto_base', 'ppto_base', 'avance_obra', 'avance_fisico_obra']
    
    for col in numeric_cols:
        if col in salud_df.columns:
            print(f"\n{col}:")
            print(f"  - Valores únicos: {salud_df[col].nunique()}")
            print(f"  - Muestra de valores:")
            for val in salud_df[col].head(5):
                print(f"    {val} (tipo: {type(val).__name__})")
            print(f"  - Min: {salud_df[col].min()}")
            print(f"  - Max: {salud_df[col].max()}")
            print(f"  - Promedio: {salud_df[col].mean():.2f}")
    
    print("\n" + "="*80)
    print("EJEMPLO DE REGISTROS")
    print("="*80)
    
    # Mostrar primer registro completo
    if len(salud_df) > 0:
        first_record = salud_df.iloc[0]
        print("\nPrimer registro de Secretaría de Salud Pública:")
        for col in ['bpin', 'presupuesto_base', 'ppto_base', 'avance_obra', 'avance_fisico_obra', 'nombre_centro_gestor', '']:
            if col in first_record.index:
                print(f"  {col}: {first_record[col]} (tipo: {type(first_record[col]).__name__})")

print("\n" + "="*80)
print("VERIFICACIÓN GENERAL DEL DATAFRAME")
print("="*80)
print(f"Total de registros: {len(df)}")
print(f"Total de columnas: {len(df.columns)}")
print(f"\nColumnas del DataFrame:")
for i, col in enumerate(df.columns, 1):
    col_display = f"'{col}'" if col == '' else col
    print(f"  {i}. {col_display}")
