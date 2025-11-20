#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de diagnóstico completo para Secretaría de Salud Pública
"""

import json
import pandas as pd
import os

# Cargar el archivo JSON extraído (buscar en múltiples ubicaciones)
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
print("DIAGNÓSTICO COMPLETO: Secretaría de Salud Pública")
print("="*80)
print(f"\nArchivo cargado desde: {json_path}")
print(f"Total de registros: {len(df)}")
print(f"Total de columnas: {len(df.columns)}")

# Mostrar valores únicos de nombre_centro_gestor
print("\n" + "="*80)
print("VALORES ÚNICOS DE nombre_centro_gestor")
print("="*80)
if 'nombre_centro_gestor' in df.columns:
    print(f"Total de valores únicos: {df['nombre_centro_gestor'].nunique()}")
    print("\nDistribución de centros gestores:")
    centro_counts = df['nombre_centro_gestor'].value_counts()
    for centro, count in centro_counts.items():
        print(f"  - {centro}: {count} registros")
    
    # Buscar específicamente Salud Pública
    salud_variations = df[df['nombre_centro_gestor'].str.contains('Salud', case=False, na=False)]
    print(f"\n🔍 Registros con 'Salud' en nombre_centro_gestor: {len(salud_variations)}")
    
    if len(salud_variations) > 0:
        salud_df = salud_variations
    else:
        print("❌ No se encontraron registros de Secretaría de Salud Pública")
        salud_df = pd.DataFrame()
else:
    print("❌ Campo 'nombre_centro_gestor' NO ENCONTRADO")
    salud_df = pd.DataFrame()

if len(salud_df) > 0:
    print("\n" + "="*80)
    print("ANÁLISIS DE VALORES NUMÉRICOS - SECRETARÍA DE SALUD")
    print("="*80)
    
    # Verificar columnas numéricas
    numeric_cols = ['presupuesto_base', 'avance_obra']
    
    for col in numeric_cols:
        if col in salud_df.columns:
            print(f"\n📊 {col}:")
            non_null = salud_df[col].dropna()
            if len(non_null) > 0:
                print(f"  - Valores no nulos: {len(non_null)}")
                print(f"  - Tipo de dato: {salud_df[col].dtype}")
                print(f"  - Min: {non_null.min()}")
                print(f"  - Max: {non_null.max()}")
                print(f"  - Promedio: {non_null.mean():.2f}")
                print(f"  - Muestra de valores:")
                for i, val in enumerate(non_null.head(5)):
                    print(f"    {i+1}. {val} (tipo: {type(val).__name__})")
            else:
                print(f"  - ⚠️ Todos los valores son nulos")
    
    print("\n" + "="*80)
    print("EJEMPLO DE REGISTROS COMPLETOS")
    print("="*80)
    
    # Mostrar primer registro completo
    if len(salud_df) > 0:
        first_record = salud_df.iloc[0]
        print("\n✅ Primer registro de Secretaría de Salud:")
        important_cols = ['bpin', 'nombre_up', 'presupuesto_base', 'avance_obra', 'estado', 'nombre_centro_gestor']
        for col in important_cols:
            if col in first_record.index:
                val = first_record[col]
                print(f"  {col}: {val} (tipo: {type(val).__name__})")

# Verificar también en GeoJSON transformado
geojson_path = 'transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto.geojson'
if os.path.exists(geojson_path):
    print("\n" + "="*80)
    print("VERIFICACIÓN EN GEOJSON TRANSFORMADO")
    print("="*80)
    
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geojson = json.load(f)
    
    features = geojson.get('features', [])
    print(f"Total de features en GeoJSON: {len(features)}")
    
    # Buscar registros de Salud
    salud_features = [f for f in features if 'Salud' in str(f.get('properties', {}).get('nombre_centro_gestor', ''))]
    print(f"Features con 'Salud' en nombre_centro_gestor: {len(salud_features)}")
    
    if len(salud_features) > 0:
        print("\n✅ Ejemplo de feature de Salud en GeoJSON:")
        sample = salud_features[0]['properties']
        for key in ['upid', 'nombre_centro_gestor', 'presupuesto_base', 'avance_obra', 'estado']:
            if key in sample:
                print(f"  {key}: {sample[key]}")
