#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Verificación final de datos de Secretaría de Salud Pública
"""

import json
import pandas as pd
import os

# Cargar el archivo JSON RECIÉN EXTRAÍDO
json_path = 'transformation_app/app_inputs/unidades_proyecto_input/unidades_proyecto.json'

if not os.path.exists(json_path):
    print(f"❌ Archivo no encontrado: {json_path}")
    exit(1)

with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

df = pd.DataFrame(data)

print("="*80)
print("VERIFICACIÓN FINAL: Secretaría de Salud Pública")
print("="*80)
print(f"\nArchivo: {json_path}")
print(f"Total de registros: {len(df)}")

# Verificar nombre_centro_gestor
if 'nombre_centro_gestor' in df.columns:
    print(f"\n✅ Campo 'nombre_centro_gestor' encontrado")
    
    # Buscar Salud Pública
    salud_df = df[df['nombre_centro_gestor'].str.contains('Salud', case=False, na=False)]
    
    print(f"\n📊 Registros con 'Salud' en nombre_centro_gestor: {len(salud_df)}")
    
    if len(salud_df) > 0:
        print("\n" + "="*80)
        print("ANÁLISIS DE DATOS DE SALUD PÚBLICA")
        print("="*80)
        
        # Análisis de avance_obra
        if 'avance_obra' in salud_df.columns:
            print("\n📈 avance_obra:")
            avances = salud_df['avance_obra'].dropna()
            print(f"  - Total valores: {len(avances)}")
            print(f"  - Min: {avances.min()}")
            print(f"  - Max: {avances.max()}")
            print(f"  - Promedio: {avances.mean():.2f}")
            print(f"  - Muestra de valores (primeros 10):")
            for i, val in enumerate(avances.head(10), 1):
                print(f"    {i}. {val}")
        
        # Análisis de presupuesto_base
        if 'presupuesto_base' in salud_df.columns:
            print("\n💰 presupuesto_base:")
            presupuestos = salud_df['presupuesto_base'].dropna()
            print(f"  - Total valores: {len(presupuestos)}")
            print(f"  - Min: {presupuestos.min():,.0f}")
            print(f"  - Max: {presupuestos.max():,.0f}")
            print(f"  - Promedio: {presupuestos.mean():,.0f}")
        
        # Estado
        if 'estado' in salud_df.columns:
            print("\n📋 Estados:")
            estados = salud_df['estado'].value_counts()
            for estado, count in estados.items():
                print(f"  - {estado}: {count}")
        
        # Ejemplo completo
        print("\n" + "="*80)
        print("EJEMPLO DE REGISTRO COMPLETO")
        print("="*80)
        first = salud_df.iloc[0]
        cols_importantes = ['bpin', 'nombre_up', 'presupuesto_base', 'avance_obra', 'estado', 'nombre_centro_gestor']
        for col in cols_importantes:
            if col in first.index:
                print(f"  {col}: {first[col]}")
    else:
        print("\n❌ NO SE ENCONTRARON REGISTROS DE SALUD PÚBLICA")
        print("\nCentros gestores disponibles:")
        for centro in df['nombre_centro_gestor'].unique():
            count = len(df[df['nombre_centro_gestor'] == centro])
            print(f"  - {centro}: {count} registros")
else:
    print("\n❌ Campo 'nombre_centro_gestor' NO ENCONTRADO")

# Verificar también el GeoJSON transformado
print("\n" + "="*80)
print("VERIFICACIÓN EN GEOJSON TRANSFORMADO")
print("="*80)

geojson_path = 'transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto.geojson'
if os.path.exists(geojson_path):
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geojson = json.load(f)
    
    features = geojson.get('features', [])
    salud_features = [f for f in features if 'Salud' in str(f.get('properties', {}).get('nombre_centro_gestor', ''))]
    
    print(f"Total features: {len(features)}")
    print(f"Features con 'Salud': {len(salud_features)}")
    
    if len(salud_features) > 0:
        print("\n✅ Ejemplo de feature de Salud:")
        props = salud_features[0]['properties']
        for key in ['upid', 'nombre_centro_gestor', 'presupuesto_base', 'avance_obra', 'estado']:
            if key in props:
                print(f"  {key}: {props[key]}")
    else:
        print("\n❌ NO hay features de Salud en el GeoJSON")
