# -*- coding: utf-8 -*-
import json
import pandas as pd

# Cargar datos
with open('unidades_proyecto.json', encoding='utf-8') as f:
    data = json.load(f)

df = pd.DataFrame(data)

# Analizar coordenadas
print(f"Total registros: {len(df)}")
print(f"Con lat no null: {df['lat'].notna().sum()}")
print(f"Con lon no null: {df['lon'].notna().sum()}")

# Convertir a numérico
df['lat_num'] = pd.to_numeric(df['lat'], errors='coerce')
df['lon_num'] = pd.to_numeric(df['lon'], errors='coerce')

lats_valid = df['lat_num'].dropna()
lons_valid = df['lon_num'].dropna()

print(f"\nCoordenadas numéricas:")
print(f"Lat min: {lats_valid.min()}, max: {lats_valid.max()}")
print(f"Lon min: {lons_valid.min()}, max: {lons_valid.max()}")

# Contar fuera de rango Cali actual (2.5-4.5, -77.5 a -75.5)
fuera_rango_lat = ((lats_valid < 2.5) | (lats_valid > 4.5)).sum()
fuera_rango_lon = ((lons_valid < -77.5) | (lons_valid > -75.5)).sum()

print(f"\nFuera de rango ACTUAL (2.5-4.5, -77.5 a -75.5):")
print(f"Latitudes: {fuera_rango_lat}/{len(lats_valid)} ({fuera_rango_lat*100/len(lats_valid):.1f}%)")
print(f"Longitudes: {fuera_rango_lon}/{len(lons_valid)} ({fuera_rango_lon*100/len(lons_valid):.1f}%)")

# Ver distribución
print(f"\nDistribución de latitudes:")
print(df['lat_num'].describe())
print(f"\nDistribución de longitudes:")
print(df['lon_num'].describe())
