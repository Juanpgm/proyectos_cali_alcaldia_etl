import pandas as pd

# Cargar Excel
df = pd.read_excel('app_outputs/unidades_proyecto_outputs/unidades_proyecto_simple.xlsx')

print("=" * 80)
print("VERIFICACIÓN DE VALORES 'SOLICITAR' EN EXCEL")
print("=" * 80)

# Buscar SOLICITAR en todas las columnas
print("\nBuscando 'SOLICITAR' en todas las columnas...")
solicitar_count = 0
found_columns = []

for col in df.columns:
    count = (df[col].astype(str) == 'SOLICITAR').sum()
    if count > 0:
        print(f"  ❌ {col}: {count} ocurrencias")
        solicitar_count += count
        found_columns.append(col)

if solicitar_count == 0:
    print("  ✅ No se encontró 'SOLICITAR' en ninguna columna")
else:
    print(f"\n❌ Total ocurrencias de 'SOLICITAR': {solicitar_count}")

# Verificar que se use REVISAR en su lugar
print("\n" + "=" * 80)
print("VERIFICACIÓN DE VALORES 'REVISAR'")
print("=" * 80)

revisar_columns = []
for col in df.columns:
    count = (df[col].astype(str) == 'REVISAR').sum()
    if count > 0:
        print(f"  ✓ {col}: {count} valores 'REVISAR'")
        revisar_columns.append(col)

# Muestra de columnas específicas
print("\n" + "=" * 80)
print("MUESTRA DE VALORES EN COLUMNAS CLAVE")
print("=" * 80)

print("\ndescripcion_intervencion (top 10):")
print(df['descripcion_intervencion'].value_counts().head(10))

print("\nunidad (distribución):")
print(df['unidad'].value_counts())

print("\ntipo_equipamiento (top 5):")
print(df['tipo_equipamiento'].value_counts().head(5))
