import json
import pandas as pd

print("=" * 80)
print("VERIFICACIÓN DE COLUMNAS")
print("=" * 80)

# Verificar GeoJSON
print("\n📄 Verificando GeoJSON...")
with open('app_outputs/unidades_proyecto_outputs/unidades_proyecto.geojson', 'r', encoding='utf-8') as f:
    data = json.load(f)
    
if data['features']:
    geojson_cols = list(data['features'][0]['properties'].keys())
    print(f"Total columnas en GeoJSON: {len(geojson_cols)}")
    print("\nColumnas en GeoJSON:")
    for col in geojson_cols:
        print(f"  - {col}")
    
    # Verificar columnas temporales
    temp_cols = ['geometry_type', 'geometry_bounds', 'processed_timestamp', 
                 'longitude', 'latitude', 'geometry_json', 'microtio', 'dataframe']
    found_temp = [c for c in temp_cols if c in geojson_cols]
    
    print(f"\n{'❌ COLUMNAS TEMPORALES ENCONTRADAS:' if found_temp else '✅ NO HAY COLUMNAS TEMPORALES'}")
    if found_temp:
        for col in found_temp:
            print(f"  ❌ {col}")

# Verificar Excel
print("\n" + "=" * 80)
print("📊 Verificando Excel...")
df = pd.read_excel('app_outputs/unidades_proyecto_outputs/unidades_proyecto_simple.xlsx')
excel_cols = df.columns.tolist()
print(f"Total columnas en Excel: {len(excel_cols)}")
print("\nColumnas en Excel:")
for col in excel_cols:
    print(f"  - {col}")

temp_cols_excel = ['geometry_type', 'geometry_bounds', 'processed_timestamp', 
                   'longitude', 'latitude', 'geometry_json', 'microtio', 'dataframe']
found_temp_excel = [c for c in temp_cols_excel if c in excel_cols]

print(f"\n{'❌ COLUMNAS TEMPORALES ENCONTRADAS:' if found_temp_excel else '✅ NO HAY COLUMNAS TEMPORALES'}")
if found_temp_excel:
    for col in found_temp_excel:
        print(f"  ❌ {col}")

print("\n" + "=" * 80)
