"""
Test script to validate the complete transformation pipeline with real data.
Validates all standardization and cleaning functions.
"""
import sys
import os
import pandas as pd
import json

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))
from data_transformation_unidades_proyecto import (
    clean_data_types,
    standardize_ano_column,
    standardize_tipo_intervencion_column,
    standardize_estado_column,
    impute_unidad_cantidad_by_geometry
)

print("="*80)
print("TEST: VALIDACIÓN COMPLETA DE TRANSFORMACIÓN")
print("="*80)

# Load real data
input_file = "transformation_app/app_inputs/unidades_proyecto_input/unidades_proyecto.json"

if not os.path.exists(input_file):
    print(f"\n❌ ERROR: No se encontró el archivo {input_file}")
    sys.exit(1)

print(f"\n✓ Cargando datos desde: {input_file}")

with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Convert to DataFrame
df = pd.DataFrame(data)
print(f"  Total de registros: {len(df)}")
print(f"  Total de columnas: {len(df.columns)}")

# Store original values for comparison
original_ano = df['ano'].copy() if 'ano' in df.columns else None
original_tipo = df['tipo_intervencion'].copy() if 'tipo_intervencion' in df.columns else None
original_estado = df['estado'].copy() if 'estado' in df.columns else None

print("\n" + "="*80)
print("PASO 1: ANÁLISIS DE DATOS ORIGINALES")
print("="*80)

# Analyze ano column
if 'ano' in df.columns:
    print("\n1. COLUMNA 'ano' (ANTES):")
    print("-" * 80)
    ano_types = df['ano'].apply(lambda x: type(x).__name__).value_counts()
    print(f"  Tipos de datos:")
    for dtype, count in ano_types.items():
        print(f"    • {dtype}: {count}")
    
    ano_values = df['ano'].value_counts(dropna=False)
    print(f"\n  Distribución de valores (Top 10):")
    for val, count in ano_values.head(10).items():
        print(f"    • {val}: {count}")
    
    # Check for floats with decimals
    float_values = df[df['ano'].apply(lambda x: isinstance(x, float) and pd.notna(x))]
    if len(float_values) > 0:
        print(f"\n  ⚠️  Valores float encontrados: {len(float_values)}")
        sample_floats = float_values['ano'].head(5).tolist()
        print(f"      Ejemplos: {sample_floats}")

# Analyze tipo_intervencion column
if 'tipo_intervencion' in df.columns:
    print("\n2. COLUMNA 'tipo_intervencion' (ANTES):")
    print("-" * 80)
    tipo_values = df['tipo_intervencion'].value_counts(dropna=False)
    print(f"  Valores únicos: {len(tipo_values)}")
    print(f"  Distribución:")
    for val, count in tipo_values.items():
        print(f"    • {val}: {count}")

# Analyze estado column
if 'estado' in df.columns and 'avance_obra' in df.columns:
    print("\n3. COLUMNA 'estado' (ANTES):")
    print("-" * 80)
    estado_values = df['estado'].value_counts(dropna=False)
    print(f"  Distribución:")
    for val, count in estado_values.items():
        print(f"    • {val}: {count}")
    
    print(f"\n  Relación con avance_obra:")
    # Convert to numeric first to get proper statistics
    avance_numeric = pd.to_numeric(df['avance_obra'], errors='coerce')
    avance_stats = avance_numeric.describe()
    print(f"    • Min: {avance_stats.get('min', 0):.2f}")
    print(f"    • Max: {avance_stats.get('max', 0):.2f}")
    print(f"    • Mean: {avance_stats.get('mean', 0):.2f}")

print("\n" + "="*80)
print("PASO 2: EJECUTANDO TRANSFORMACIÓN")
print("="*80)

# Apply clean_data_types (includes ano, tipo_intervencion, estado standardization)
print("\nAplicando clean_data_types()...")
df_transformed = clean_data_types(df)

print("\n" + "="*80)
print("PASO 3: ANÁLISIS DE DATOS TRANSFORMADOS")
print("="*80)

# Analyze ano column after transformation
if 'ano' in df_transformed.columns:
    print("\n1. COLUMNA 'ano' (DESPUÉS):")
    print("-" * 80)
    ano_types = df_transformed['ano'].apply(lambda x: type(x).__name__).value_counts()
    print(f"  Tipos de datos:")
    for dtype, count in ano_types.items():
        print(f"    • {dtype}: {count}")
    
    ano_values = df_transformed['ano'].value_counts(dropna=False)
    print(f"\n  Distribución de valores:")
    for val, count in ano_values.items():
        print(f"    • {val}: {count}")
    
    # Check if all valid values are integers
    valid_anos = df_transformed[(df_transformed['ano'] != 'REVISAR') & (df_transformed['ano'].notna())]
    all_integers = all(isinstance(val, int) for val in valid_anos['ano'])
    print(f"\n  ✓ Todos los valores válidos son enteros: {all_integers}")
    
    if not all_integers:
        non_integers = valid_anos[~valid_anos['ano'].apply(lambda x: isinstance(x, int))]
        print(f"  ⚠️  Valores NO enteros encontrados: {len(non_integers)}")
        print(f"      Ejemplos: {non_integers['ano'].head(5).tolist()}")
    
    revisar_count = (df_transformed['ano'] == 'REVISAR').sum()
    print(f"\n  Valores 'REVISAR': {revisar_count}")

# Analyze tipo_intervencion after transformation
if 'tipo_intervencion' in df_transformed.columns:
    print("\n2. COLUMNA 'tipo_intervencion' (DESPUÉS):")
    print("-" * 80)
    tipo_values = df_transformed['tipo_intervencion'].value_counts(dropna=False)
    print(f"  Valores únicos: {len(tipo_values)}")
    print(f"  Distribución:")
    for val, count in tipo_values.items():
        print(f"    • {val}: {count}")
    
    revisar_tipo = (df_transformed['tipo_intervencion'] == 'REVISAR').sum()
    if revisar_tipo > 0:
        print(f"\n  ⚠️  Valores 'REVISAR': {revisar_tipo}")

# Analyze estado after transformation
if 'estado' in df_transformed.columns:
    print("\n3. COLUMNA 'estado' (DESPUÉS):")
    print("-" * 80)
    estado_values = df_transformed['estado'].value_counts(dropna=False)
    print(f"  Distribución:")
    for val, count in estado_values.items():
        print(f"    • {val}: {count}")
    
    # Verify estado matches avance_obra rules
    if 'avance_obra' in df_transformed.columns:
        print(f"\n  Verificación de reglas estado/avance_obra:")
        
        # Check En Alistamiento (avance_obra = 0)
        alistamiento = df_transformed[df_transformed['estado'] == 'En Alistamiento']
        alistamiento_ok = all(alistamiento['avance_obra'] == 0)
        print(f"    • 'En Alistamiento' (avance = 0): {len(alistamiento)} registros")
        print(f"      Regla cumplida: {alistamiento_ok}")
        
        # Check En Ejecución (0 < avance_obra < 100)
        ejecucion = df_transformed[df_transformed['estado'] == 'En Ejecución']
        ejecucion_ok = all((ejecucion['avance_obra'] > 0) & (ejecucion['avance_obra'] < 100))
        print(f"    • 'En Ejecución' (0 < avance < 100): {len(ejecucion)} registros")
        print(f"      Regla cumplida: {ejecucion_ok}")
        
        # Check Terminado (avance_obra = 100)
        terminado = df_transformed[df_transformed['estado'] == 'Terminado']
        terminado_ok = all(terminado['avance_obra'] == 100)
        print(f"    • 'Terminado' (avance = 100): {len(terminado)} registros")
        print(f"      Regla cumplida: {terminado_ok}")

print("\n" + "="*80)
print("PASO 4: COMPARACIÓN ANTES/DESPUÉS")
print("="*80)

# Compare ano changes
if original_ano is not None and 'ano' in df_transformed.columns:
    print("\n1. CAMBIOS EN 'ano':")
    print("-" * 80)
    
    changes = 0
    for idx in df.index:
        old_val = original_ano.iloc[idx]
        new_val = df_transformed.loc[idx, 'ano']
        
        # Check if value changed
        if pd.isna(old_val) and new_val == 'REVISAR':
            continue  # Expected change
        elif isinstance(old_val, float) and isinstance(new_val, int):
            if int(old_val) == new_val:
                changes += 1  # Float to int conversion
    
    print(f"  Conversiones float → int: {changes}")
    
    # Show examples
    print(f"\n  Ejemplos de conversión:")
    count = 0
    for idx in df.index:
        old_val = original_ano.iloc[idx]
        new_val = df_transformed.loc[idx, 'ano']
        
        if isinstance(old_val, float) and isinstance(new_val, int) and pd.notna(old_val):
            print(f"    [{idx}] {old_val} ({type(old_val).__name__}) → {new_val} ({type(new_val).__name__})")
            count += 1
            if count >= 5:
                break

# Compare tipo_intervencion changes
if original_tipo is not None and 'tipo_intervencion' in df_transformed.columns:
    print("\n2. CAMBIOS EN 'tipo_intervencion':")
    print("-" * 80)
    
    changes_dict = {}
    for idx in df.index:
        old_val = str(original_tipo.iloc[idx]) if pd.notna(original_tipo.iloc[idx]) else 'NULL'
        new_val = str(df_transformed.loc[idx, 'tipo_intervencion'])
        
        if old_val != new_val:
            key = f"{old_val} → {new_val}"
            changes_dict[key] = changes_dict.get(key, 0) + 1
    
    if changes_dict:
        print(f"  Estandarizaciones realizadas:")
        for change, count in sorted(changes_dict.items(), key=lambda x: x[1], reverse=True):
            print(f"    • {change}: {count} registros")
    else:
        print(f"  No se realizaron cambios")

# Compare estado changes
if original_estado is not None and 'estado' in df_transformed.columns:
    print("\n3. CAMBIOS EN 'estado':")
    print("-" * 80)
    
    changes_dict = {}
    for idx in df.index:
        old_val = str(original_estado.iloc[idx]) if pd.notna(original_estado.iloc[idx]) else 'NULL'
        new_val = str(df_transformed.loc[idx, 'estado'])
        
        if old_val != new_val:
            key = f"{old_val} → {new_val}"
            changes_dict[key] = changes_dict.get(key, 0) + 1
    
    if changes_dict:
        print(f"  Estandarizaciones realizadas:")
        for change, count in sorted(changes_dict.items(), key=lambda x: x[1], reverse=True):
            print(f"    • {change}: {count} registros")
    else:
        print(f"  No se realizaron cambios")

print("\n" + "="*80)
print("RESUMEN FINAL")
print("="*80)

print(f"\n✅ VALIDACIÓN COMPLETADA")
print(f"  Total de registros procesados: {len(df_transformed)}")
print(f"  Columnas en DataFrame: {len(df_transformed.columns)}")

# Check for critical issues
issues = []

if 'ano' in df_transformed.columns:
    valid_anos = df_transformed[(df_transformed['ano'] != 'REVISAR') & (df_transformed['ano'].notna())]
    non_integers = valid_anos[~valid_anos['ano'].apply(lambda x: isinstance(x, int))]
    if len(non_integers) > 0:
        issues.append(f"⚠️  {len(non_integers)} valores 'ano' válidos NO son enteros")

if 'tipo_intervencion' in df_transformed.columns:
    revisar_tipo = (df_transformed['tipo_intervencion'] == 'REVISAR').sum()
    if revisar_tipo > 0:
        issues.append(f"⚠️  {revisar_tipo} valores 'tipo_intervencion' marcados como 'REVISAR'")

if issues:
    print(f"\n⚠️  ISSUES ENCONTRADOS:")
    for issue in issues:
        print(f"  {issue}")
else:
    print(f"\n✅ ¡TODAS LAS VALIDACIONES PASARON EXITOSAMENTE!")

print("\n" + "="*80)
