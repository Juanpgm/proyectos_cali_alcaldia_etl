import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import os
import json
import re
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def clean_column_name(col_name):
    """Limpia y estandariza nombres de columnas"""
    if pd.isna(col_name) or col_name == '':
        return 'sin_nombre'
    
    # Convertir a string y minúsculas
    col_name = str(col_name).lower().strip()
    
    # Remover caracteres especiales y espacios
    col_name = re.sub(r'[^\w\s]', '', col_name)
    col_name = re.sub(r'\s+', '_', col_name)
    
    # Remover underscores múltiples
    col_name = re.sub(r'_+', '_', col_name)
    col_name = col_name.strip('_')
    
    return col_name

def clean_numeric_value(value):
    """Limpia y convierte valores numéricos"""
    if pd.isna(value) or value == '' or value == 'nan':
        return None
    
    # Convertir a string para procesamiento
    str_value = str(value).strip()
    
    # Si ya es un número, devolverlo
    try:
        return float(str_value)
    except:
        pass
    
    # Limpiar formato de número con separadores de miles y decimales
    str_value = str_value.replace(',', '').replace('$', '').replace('%', '')
    str_value = re.sub(r'[^\d.-]', '', str_value)
    
    try:
        return float(str_value)
    except:
        return str_value.lower() if str_value else None

def clean_text_value(value):
    """Limpia valores de texto"""
    if pd.isna(value) or value == '' or value == 'nan':
        return None
    
    # Convertir a string y limpiar espacios
    clean_value = str(value).strip()
    
    # Para valores que parecen códigos (formato COD0001, etc.), mantener el formato original
    if re.match(r'^[A-Z]{3}\d+$', clean_value):
        return clean_value
    
    # Para valores que parecen códigos numéricos, mantener el formato original
    if re.match(r'^\d+$', clean_value):
        return clean_value
    
    # Para texto normal, convertir a minúsculas
    clean_value = clean_value.lower()
    
    # Reemplazar múltiples espacios con uno solo
    clean_value = re.sub(r'\s+', ' ', clean_value)
    
    return clean_value if clean_value != '' else None

def detect_coordinate_columns(df):
    """Detecta columnas de coordenadas"""
    lon_patterns = ['longitud', 'longitude', 'lon', 'lng', 'x', 'coord_x', 'coordenada_x']
    lat_patterns = ['latitud', 'latitude', 'lat', 'y', 'coord_y', 'coordenada_y']
    
    lon_col = None
    lat_col = None
    
    for col in df.columns:
        col_lower = col.lower()
        if any(pattern in col_lower for pattern in lon_patterns):
            lon_col = col
        if any(pattern in col_lower for pattern in lat_patterns):
            lat_col = col
    
    return lon_col, lat_col

def process_excel_file(file_path):
    """Procesa un archivo Excel y retorna un GeoDataFrame"""
    try:
        # Leer el archivo Excel
        df = pd.read_excel(file_path, engine='openpyxl')
        
        print(f"Procesando archivo: {file_path}")
        print(f"Forma original: {df.shape}")
        print(f"Columnas originales: {list(df.columns)}")
        
        # Limpiar nombres de columnas
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Detectar columnas de coordenadas
        lon_col, lat_col = detect_coordinate_columns(df)
        
        if not lon_col or not lat_col:
            print(f"No se encontraron columnas de coordenadas en {file_path}")
            return None
        
        print(f"Columnas de coordenadas detectadas: {lon_col}, {lat_col}")
        
        # Limpiar datos
        processed_data = []
        
        for idx, row in df.iterrows():
            processed_row = {}
            
            for col in df.columns:
                value = row[col]
                
                # Si es columna de coordenadas, tratar como numérico
                if col in [lon_col, lat_col]:
                    processed_row[col] = clean_numeric_value(value)
                # Si es la columna 'cod', mantener como string
                elif col == 'cod':
                    processed_row[col] = clean_text_value(value)
                else:
                    # Intentar detectar si es numérico
                    if pd.api.types.is_numeric_dtype(df[col]) or (
                        isinstance(value, str) and 
                        any(char.isdigit() for char in str(value)) and
                        len(re.findall(r'\d', str(value))) > len(re.findall(r'[a-zA-Z]', str(value)))
                    ):
                        processed_row[col] = clean_numeric_value(value)
                    else:
                        processed_row[col] = clean_text_value(value)
            
            # Verificar que las coordenadas sean válidas
            if (processed_row.get(lon_col) is not None and 
                processed_row.get(lat_col) is not None):
                try:
                    lon = float(processed_row[lon_col])
                    lat = float(processed_row[lat_col])
                    
                    # Validar rangos de coordenadas para Colombia
                    if -85 <= lon <= -65 and -5 <= lat <= 15:
                        processed_data.append(processed_row)
                    else:
                        print(f"Coordenadas fuera de rango: lon={lon}, lat={lat}")
                except:
                    print(f"Error al convertir coordenadas en fila {idx}")
        
        if not processed_data:
            print(f"No se encontraron datos válidos en {file_path}")
            return None
        
        # Crear DataFrame procesado
        processed_df = pd.DataFrame(processed_data)
        
        # Crear geometría
        geometry = [Point(xy) for xy in zip(processed_df[lon_col], processed_df[lat_col])]
        
        # Crear GeoDataFrame
        gdf = gpd.GeoDataFrame(processed_df, geometry=geometry, crs='EPSG:4326')
        
        print(f"Datos procesados: {len(gdf)} registros válidos")
        return gdf
        
    except Exception as e:
        print(f"Error procesando {file_path}: {str(e)}")
        return None

def main():
    """Función principal"""
    # Definir rutas relativas desde transformation_app
    input_dir = Path("app_inputs/centros_gravedad_input")
    output_dir = Path("app_outputs/centros_gravedad_output")
    
    # Crear directorio de salida si no existe
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Buscar archivos Excel
    excel_files = list(input_dir.glob("*.xlsx")) + list(input_dir.glob("*.xls"))
    
    if not excel_files:
        print("No se encontraron archivos Excel en el directorio de entrada")
        return
    
    print(f"Encontrados {len(excel_files)} archivos Excel")
    
    # Procesar todos los archivos
    gdfs = []
    
    for file_path in excel_files:
        gdf = process_excel_file(file_path)
        if gdf is not None and not gdf.empty:
            gdfs.append(gdf)
    
    if not gdfs:
        print("No se pudo procesar ningún archivo")
        return
    
    # Unificar todos los GeoDataFrames
    print("Unificando datos...")
    
    # Obtener todas las columnas únicas
    all_columns = set()
    for gdf in gdfs:
        all_columns.update(gdf.columns)
    
    # Asegurar que todas las columnas estén presentes en todos los GeoDataFrames
    for gdf in gdfs:
        for col in all_columns:
            if col not in gdf.columns:
                gdf[col] = None
    
    # Concatenar todos los GeoDataFrames
    unified_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
    unified_gdf.crs = 'EPSG:4326'
    
    # Eliminar columnas no deseadas en el GeoJSON final
    columns_to_remove = ['dirección_de_correo_electrónico', 'coordenada_x', 'coordenada_y']
    for col in columns_to_remove:
        if col in unified_gdf.columns:
            unified_gdf = unified_gdf.drop(columns=[col])
    
    print(f"Datos unificados: {len(unified_gdf)} registros totales")
    print(f"Columnas finales: {list(unified_gdf.columns)}")
    
    # Guardar como GeoJSON
    output_file = output_dir / "centros_gravedad_unificado.geojson"
    
    # Configurar opciones de escritura para UTF-8
    unified_gdf.to_file(
        output_file, 
        driver='GeoJSON',
        encoding='utf-8'
    )
    
    print(f"Archivo guardado: {output_file}")
    
    # Generar reporte
    report = {
        "fecha_procesamiento": pd.Timestamp.now().isoformat(),
        "archivos_procesados": [os.path.basename(f) for f in excel_files],
        "total_registros": len(unified_gdf),
        "columnas": list(unified_gdf.columns),
        "bbox": unified_gdf.total_bounds.tolist(),
        "estadisticas": {
            "registros_totales": len(unified_gdf),
            "columnas_incluidas": list(unified_gdf.columns)
        }
    }
    
    # Guardar reporte
    report_file = output_dir / "reporte_procesamiento.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"Reporte guardado: {report_file}")
    print("Procesamiento completado exitosamente")

if __name__ == "__main__":
    main()