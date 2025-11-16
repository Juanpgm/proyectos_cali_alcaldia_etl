"""
Script para verificar las columnas de validación en gdf_geolocalizar.xlsx
"""
import pandas as pd
import os

# Path to the Excel file
excel_path = "app_outputs/unidades_proyecto_outputs/gdf_geolocalizar.xlsx"

if not os.path.exists(excel_path):
    print(f"❌ File not found: {excel_path}")
    exit(1)

# Read Excel
df = pd.read_excel(excel_path)

print("="*80)
print("VERIFICACIÓN DE COLUMNAS DE VALIDACIÓN")
print("="*80)

# List all columns
print(f"\n📋 Total de columnas: {len(df.columns)}")
print(f"Columnas: {', '.join(df.columns.tolist())}")

# Check if validation columns exist
validation_columns = ['validacion_distancias', 'geometry_distancias', 'geometry_val_s2_distancias']

print(f"\n🔍 Verificando columnas de validación:")
for col in validation_columns:
    if col in df.columns:
        non_null_count = df[col].notna().sum()
        null_count = df[col].isna().sum()
        print(f"   ✓ {col}:")
        print(f"     - Valores no nulos: {non_null_count} ({non_null_count/len(df)*100:.1f}%)")
        print(f"     - Valores nulos: {null_count} ({null_count/len(df)*100:.1f}%)")
        
        # Show unique non-null values
        unique_values = df[col].dropna().unique()
        if len(unique_values) > 0:
            if len(unique_values) <= 10:
                print(f"     - Valores únicos: {unique_values}")
            else:
                print(f"     - Valores únicos (primeros 10): {unique_values[:10]}")
        
        # For numeric columns, show statistics
        if col == 'validacion_distancias' and non_null_count > 0:
            numeric_values = pd.to_numeric(df[col], errors='coerce').dropna()
            if len(numeric_values) > 0:
                print(f"     - Estadísticas:")
                print(f"       · Min: {numeric_values.min():.2f}")
                print(f"       · Max: {numeric_values.max():.2f}")
                print(f"       · Mean: {numeric_values.mean():.2f}")
                print(f"       · Median: {numeric_values.median():.2f}")
    else:
        print(f"   ❌ {col}: NO EXISTE")

# Check 'corregir' column distribution
if 'corregir' in df.columns:
    print(f"\n📊 Distribución de 'corregir':")
    corregir_counts = df['corregir'].value_counts()
    for value, count in corregir_counts.items():
        print(f"   - {value}: {count} ({count/len(df)*100:.1f}%)")

print("\n" + "="*80)
