# -*- coding: utf-8 -*-
"""
Inspeccionar formato exacto de coordenadas en Excel
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extraction_app.data_extraction_unidades_proyecto import get_excel_files_from_drive, read_excel_file_to_dataframe
import pandas as pd

def inspect_coordinates():
    """Inspecciona coordenadas en archivos Excel originales"""
    
    print("="*80)
    print("INSPECCIÓN DE COORDENADAS EN ARCHIVOS EXCEL")
    print("="*80)
    
    # Obtener archivos de Drive
    print("\n1. Obteniendo lista de archivos...")
    folder_id = "1q7CYewsqxKpz0njlNWjZsFkiAAtOeQDX"
    excel_files = get_excel_files_from_drive(folder_id)
    
    if not excel_files:
        print("[ERROR] No se encontraron archivos")
        return
    
    # Buscar específicamente Vivienda Social (tiene más registros problemáticos)
    vivienda_file = None
    for f in excel_files:
        if 'Vivienda' in f['name']:
            vivienda_file = f
            break
    
    if not vivienda_file:
        print("[ERROR] No se encontró archivo de Vivienda")
        return
    
    print(f"\n2. Analizando: {vivienda_file['name']}")
    
    # Leer archivo directamente
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    import io
    
    # Autenticar
    from config import get_drive_service
    drive_service = get_drive_service()
    
    # Descargar archivo
    request = drive_service.files().get_media(fileId=vivienda_file['id'])
    file_buffer = io.BytesIO()
    
    from googleapiclient.http import MediaIoBaseDownload
    downloader = MediaIoBaseDownload(file_buffer, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    file_buffer.seek(0)
    
    # Leer sin procesar
    df_raw = pd.read_excel(file_buffer, engine='openpyxl')
    
    if df_raw is None or df_raw.empty:
        print("[ERROR] No se pudo leer el archivo")
        return
    
    print(f"\n3. Primeros registros con lat/lon:")
    print(f"   Total registros: {len(df_raw)}")
    
    # Mostrar columnas disponibles
    print(f"\n4. Columnas disponibles:")
    print(f"   {list(df_raw.columns)}")
    
    # Buscar columnas de coordenadas
    coord_cols = [col for col in df_raw.columns if col.lower() in ['lat', 'lon', 'latitud', 'longitud', 'x', 'y']]
    print(f"\n5. Columnas de coordenadas encontradas: {coord_cols}")
    
    if 'lat' in df_raw.columns and 'lon' in df_raw.columns:
        # Mostrar primeros 20 registros con sus tipos de datos
        print(f"\n6. Muestras de coordenadas (primeras 20):")
        print(f"   Tipo de datos - lat: {df_raw['lat'].dtype}, lon: {df_raw['lon'].dtype}")
        
        for idx in range(min(20, len(df_raw))):
            lat_val = df_raw.iloc[idx]['lat']
            lon_val = df_raw.iloc[idx]['lon']
            print(f"\n   [{idx+1}] lat: {lat_val} (tipo: {type(lat_val).__name__})")
            print(f"       lon: {lon_val} (tipo: {type(lon_val).__name__})")
            
            # Mostrar representación exacta
            if pd.notna(lat_val):
                print(f"       lat repr: {repr(lat_val)}")
            if pd.notna(lon_val):
                print(f"       lon repr: {repr(lon_val)}")
        
        # Estadísticas de valores
        print(f"\n7. Estadísticas de valores:")
        print(f"\n   Latitud:")
        print(df_raw['lat'].describe())
        print(f"\n   Longitud:")
        print(df_raw['lon'].describe())


if __name__ == '__main__':
    inspect_coordinates()
