# -*- coding: utf-8 -*-
"""
Prueba directa de la función de inferencia sobre datos ya cargados
"""

import json
import pandas as pd
import geopandas as gpd
from pathlib import Path
import sys
import os

# Agregar path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transformation_app.data_transformation_unidades_proyecto import infer_missing_categorical_values

def test_inferencia_directa():
    """
    Prueba la función de inferencia directamente sobre datos existentes
    """
    print("="*80)
    print("PRUEBA DIRECTA DE FUNCIÓN DE INFERENCIA")
    print("="*80)
    
    # Cargar datos JSON existentes
    output_dir = Path(__file__).parent / 'app_outputs' / 'grouped_structure'
    json_files = sorted(output_dir.glob('unidades_proyecto_*.json'), reverse=True)
    
    if not json_files:
        print("❌ No se encontraron archivos")
        return False
    
    json_file = json_files[0]
    print(f"\n📂 Cargando: {json_file.name}")
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Convertir a DataFrame
    df = pd.DataFrame(data)
    print(f"✓ Total registros: {len(df)}")
    
    # Filtrar Secretaría de Salud Pública ANTES de la inferencia
    salud_antes = df[df['nombre_centro_gestor'] == 'Secretaría de Salud Pública'].copy()
    print(f"✓ Registros Secretaría de Salud Pública: {len(salud_antes)}")
    
    print("\n📊 ANTES DE LA INFERENCIA:")
    print(f"   - con clase_up NaN: {salud_antes['clase_up'].isna().sum()}")
    print(f"   - con tipo_equipamiento NaN: {salud_antes['tipo_equipamiento'].isna().sum()}")
    
    # Crear GeoDataFrame simple (sin geometría real, solo para probar la función)
    gdf = gpd.GeoDataFrame(df)
    
    # Aplicar función de inferencia
    print("\n🔄 Aplicando función de inferencia...")
    gdf_inferido = infer_missing_categorical_values(gdf)
    
    # Filtrar Secretaría de Salud Pública DESPUÉS de la inferencia
    salud_despues = gdf_inferido[gdf_inferido['nombre_centro_gestor'] == 'Secretaría de Salud Pública'].copy()
    
    print("\n📊 DESPUÉS DE LA INFERENCIA:")
    print(f"   - con clase_up NaN: {salud_despues['clase_up'].isna().sum()}")
    print(f"   - con tipo_equipamiento NaN: {salud_despues['tipo_equipamiento'].isna().sum()}")
    print(f"   - con clase_up='Obras equipamientos': {(salud_despues['clase_up'] == 'Obras equipamientos').sum()}")
    print(f"   - con tipo_equipamiento='Centro de salud': {(salud_despues['tipo_equipamiento'] == 'Centro de salud').sum()}")
    
    # Mostrar ejemplos
    print("\n🔍 EJEMPLOS:")
    for idx in salud_despues.head(3).index:
        row = salud_despues.loc[idx]
        print(f"\n   {row['nombre_up']}")
        print(f"      clase_up: {row['clase_up']}")
        print(f"      tipo_equipamiento: {row['tipo_equipamiento']}")
    
    # Validación
    print("\n" + "="*80)
    print("VALIDACIÓN")
    print("="*80)
    
    todos_tienen_clase = (salud_despues['clase_up'] == 'Obras equipamientos').all()
    todos_tienen_tipo = (salud_despues['tipo_equipamiento'] == 'Centro de salud').all()
    
    if todos_tienen_clase and todos_tienen_tipo:
        print("✅ ÉXITO: Todos los registros de Salud Pública tienen valores inferidos correctamente")
        return True
    else:
        if not todos_tienen_clase:
            print(f"❌ FALLO: No todos tienen clase_up='Obras equipamientos'")
        if not todos_tienen_tipo:
            print(f"❌ FALLO: No todos tienen tipo_equipamiento='Centro de salud'")
        return False


if __name__ == '__main__':
    success = test_inferencia_directa()
    exit(0 if success else 1)
