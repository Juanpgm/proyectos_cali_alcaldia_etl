#!/usr/bin/env python3
"""
Analizar coordenadas en datos crudos de unidades problemáticas específicas
"""
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extraction_app.data_extraction_unidades_proyecto import extract_unidades_proyecto_data

df = extract_unidades_proyecto_data()

# Nombres problemáticos
nombres = [
    "I.E. Luis Fernando Caicedo",
    "Teatro Municipal Enrique Buenaventura",
    "biblioteca pública centro cultural nuevo latir"
]

print("\n🔍 ANÁLISIS DE COORDENADAS EN DATOS CRUDOS\n")

for nombre in nombres:
    mask = df['nombre_up'].str.contains(nombre, case=False, na=False)
    matches = df[mask]
    
    if len(matches) > 0:
        print(f"📍 {nombre}:")
        print(f"   Total registros: {len(matches)}")
        print(f"   Clase UP: {matches['clase_up'].unique()}")
        print(f"   Tipo intervencion: {matches['tipo_intervencion'].unique()[:3]}")
        
        # Analizar coordenadas
        for idx, row in matches.head(3).iterrows():
            lat_raw = row['lat']
            lon_raw = row['lon']
            lat_numeric = pd.to_numeric(lat_raw, errors='coerce')
            lon_numeric = pd.to_numeric(lon_raw, errors='coerce')
            
            print(f"\n   Registro {idx}:")
            print(f"      nombre_up: {row['nombre_up'][:50]}")
            print(f"      lat (raw): '{lat_raw}' (type: {type(lat_raw).__name__})")
            print(f"      lon (raw): '{lon_raw}' (type: {type(lon_raw).__name__})")
            print(f"      lat (numeric): {lat_numeric}")
            print(f"      lon (numeric): {lon_numeric}")
            print(f"      lat notna: {pd.notna(lat_raw)}, lon notna: {pd.notna(lon_raw)}")
        print("\n" + "="*80 + "\n")
