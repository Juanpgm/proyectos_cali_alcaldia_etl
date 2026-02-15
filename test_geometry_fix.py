import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from transformation_app.data_transformation_unidades_proyecto import create_final_geometry

# Crear DataFrame de prueba con coordenadas que estarían fuera del rango antiguo pero válidas
test_data = {
    'upid': ['TEST001', 'TEST002', 'TEST003'],
    'lat': [3.5, 4.2, 2.8],  # Dentro del nuevo rango expandido
    'lon': [-76.5, -76.8, -76.2],  # Dentro del nuevo rango expandido
    'geometry': [None, None, None]  # Sin geometría inicial
}

df = pd.DataFrame(test_data)
gdf = gpd.GeoDataFrame(df, geometry='geometry')

print('DataFrame original:')
print(gdf[['upid', 'lat', 'lon', 'geometry']])

# Aplicar create_final_geometry
result_gdf = create_final_geometry(gdf)

print('\nDespués de create_final_geometry:')
print(result_gdf[['upid', 'lat', 'lon', 'geometry']])

# Verificar que se crearon geometrías
for idx, row in result_gdf.iterrows():
    geom = row['geometry']
    if geom is not None and hasattr(geom, 'x'):
        print(f'UPID {row["upid"]}: Geometry creada - lon={geom.x:.6f}, lat={geom.y:.6f}')
    else:
        print(f'UPID {row["upid"]}: Sin geometría')