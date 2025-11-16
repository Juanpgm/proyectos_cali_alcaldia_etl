"""
Test script to verify that ano column values are converted to integers (not floats).
"""
import sys
import os
import pandas as pd
import json

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))
from data_transformation_unidades_proyecto import standardize_ano_column

print("="*80)
print("TEST: ANO COLUMN INTEGER CONVERSION")
print("="*80)

# Create test DataFrame with float values (as they appear in the JSON file)
test_data = {
    'ano': [
        2024.0,      # Float value from JSON
        2025.0,      # Float value from JSON
        2026.0,      # Float value from JSON
        '2024',      # String value
        '2025.0',    # String float value
        None,        # Null value
        0,           # Zero value
        2023.0,      # Invalid year (out of range)
        2031.0,      # Invalid year (out of range)
    ],
    'nombre_up': [
        'Proyecto A', 'Proyecto B', 'Proyecto C', 'Proyecto D', 'Proyecto E',
        'Proyecto F', 'Proyecto G', 'Proyecto H', 'Proyecto I'
    ]
}

df_test = pd.DataFrame(test_data)

print("\n✓ Test DataFrame creado con 9 registros")
print("\nVALORES ORIGINALES:")
print("-" * 80)
for idx, row in df_test.iterrows():
    ano_value = row['ano']
    ano_type = type(ano_value).__name__
    print(f"  [{idx}] {row['nombre_up']:15} | ano={ano_value} (type: {ano_type})")

# Apply standardization
df_result = standardize_ano_column(df_test)

print("\n\nVALORES DESPUÉS DE ESTANDARIZACIÓN:")
print("-" * 80)
for idx, row in df_result.iterrows():
    ano_value = row['ano']
    ano_type = type(ano_value).__name__
    print(f"  [{idx}] {row['nombre_up']:15} | ano={ano_value} (type: {ano_type})")

# Verify results
print("\n\n✅ VERIFICACIÓN DE RESULTADOS:")
print("-" * 80)

# Check valid years are integers
valid_years = df_result[(df_result['ano'] != 'REVISAR') & (df_result['ano'].notna())]
print(f"\n1. Valores válidos convertidos a enteros:")
for idx, row in valid_years.iterrows():
    ano_value = row['ano']
    is_integer = isinstance(ano_value, (int, pd.Int64Dtype))
    has_no_decimals = (isinstance(ano_value, int) or (isinstance(ano_value, float) and ano_value == int(ano_value)))
    print(f"   [{idx}] ano={ano_value} | Es entero: {is_integer} | Sin decimales: {has_no_decimals}")

# Check invalid values marked as REVISAR
invalid_years = df_result[df_result['ano'] == 'REVISAR']
print(f"\n2. Valores marcados como 'REVISAR': {len(invalid_years)}")
for idx, row in invalid_years.iterrows():
    original_value = df_test.loc[idx, 'ano']
    print(f"   [{idx}] Original: {original_value} → 'REVISAR'")

# Summary statistics
print("\n\n📊 RESUMEN:")
print("-" * 80)
print(f"  Total registros: {len(df_result)}")
print(f"  Valores válidos (enteros): {len(valid_years)}")
print(f"  Valores marcados 'REVISAR': {len(invalid_years)}")

# Check if all valid values are integers without decimals
all_integers = all(isinstance(val, int) for val in valid_years['ano'])
print(f"\n  ✓ Todos los valores válidos son enteros: {all_integers}")

if all_integers:
    print("\n  🎉 ¡ÉXITO! Los valores válidos se almacenan como enteros sin decimales.")
else:
    print("\n  ⚠️  ADVERTENCIA: Algunos valores válidos no son enteros.")

print("\n" + "="*80)
