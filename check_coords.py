import pandas as pd
import json

df = pd.read_excel('a:\\programing_workspace\\proyectos_cali_alcaldia_etl\\transformation_app\\app_outputs\\unidades_proyecto_outputs\\gdf_geolocalizar.xlsx')

samples = df[df['geometry_val_s2'] != 'ERROR'].head(5)

print('Verificando formato de geometry_val_s2:')
print('='*80)

for i, (idx, row) in enumerate(samples.iterrows()):
    print(f"\n{i+1}. UPID: {row['upid']}")
    geom_str = row['geometry_val_s2']
    print(f"   Raw: {geom_str}")
    
    geom = json.loads(geom_str)
    coords = geom['coordinates']
    
    print(f"   Coords: [{coords[0]}, {coords[1]}]")
    
    # Check if lat (should be ~3.x) is in position 0
    if 3.0 <= coords[0] <= 4.0:
        print(f"   ✅ CORRECTO: coords[0] = {coords[0]} es LATITUD")
    elif -77.0 <= coords[0] <= -76.0:
        print(f"   ❌ ERROR: coords[0] = {coords[0]} es LONGITUD (debería ser latitud)")
    
    # Check if lon (should be ~-76.x) is in position 1
    if -77.0 <= coords[1] <= -76.0:
        print(f"   ✅ CORRECTO: coords[1] = {coords[1]} es LONGITUD")
    elif 3.0 <= coords[1] <= 4.0:
        print(f"   ❌ ERROR: coords[1] = {coords[1]} es LATITUD (debería ser longitud)")

print('\n' + '='*80)
print('RESUMEN:')
print(f'Total registros con geometry_val_s2: {(df["geometry_val_s2"] != "ERROR").sum()}')
