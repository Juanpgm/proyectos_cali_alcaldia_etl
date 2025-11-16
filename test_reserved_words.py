"""
Script para verificar que las palabras reservadas se preservan correctamente
"""
import sys
sys.path.append('transformation_app')

from data_transformation_unidades_proyecto import unidades_proyecto_transformer

# Ejecutar transformación
df = unidades_proyecto_transformer()

print('\n' + '='*60)
print('VERIFICACIÓN DE PALABRAS RESERVADAS')
print('='*60)

# Verificar IPS
print('\n--- Ejemplos con IPS ---')
ips_samples = df[df['nombre_up'].str.contains('IPS', na=False, case=False)]['nombre_up'].head(10)
for idx, val in ips_samples.items():
    print(f"  {val}")

# Verificar I.E
print('\n--- Ejemplos con I.E ---')
ie_samples = df[df['nombre_up'].str.contains(r'I\.E', na=False, regex=True)]['nombre_up'].head(10)
for idx, val in ie_samples.items():
    print(f"  {val}")

# Verificar CALI
print('\n--- Ejemplos con CALI ---')
cali_samples = df[df['nombre_up'].str.contains('CALI', na=False, case=False)]['nombre_up'].head(5)
for idx, val in cali_samples.items():
    print(f"  {val}")

# Verificar UTS
print('\n--- Ejemplos con UTS ---')
uts_samples = df[df['nombre_up'].str.contains('UTS', na=False, case=False)]['nombre_up'].head(5)
if len(uts_samples) > 0:
    for idx, val in uts_samples.items():
        print(f"  {val}")
else:
    print("  (No se encontraron ejemplos con UTS)")

print('\n' + '='*60)
print(f'Total de valores únicos: {df["nombre_up"].nunique()}')
print(f'Total de registros: {len(df)}')
print('='*60)
