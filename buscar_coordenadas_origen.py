"""
Script para buscar las coordenadas de las UTS de Bienestar Social
en los archivos de entrada (Google Drive).
"""

import sys
import os
from pathlib import Path

# Agregar rutas
sys.path.append(str(Path(__file__).parent))

from database.config import get_drive_service, list_excel_files_in_folder, download_excel_file, DRIVE_FOLDER_ID
import pandas as pd
import io

print("="*80)
print("BÚSQUEDA DE COORDENADAS EN ARCHIVOS DE ENTRADA")
print("="*80)

# Obtener servicio de Google Drive
try:
    service = get_drive_service()
    if not service:
        print("❌ No se pudo conectar a Google Drive")
        exit(1)
    
    # Listar archivos Excel
    files = list_excel_files_in_folder(DRIVE_FOLDER_ID)
    if not files:
        print("❌ No se encontraron archivos Excel en Google Drive")
        exit(1)
    
    print(f"\n📁 Archivos encontrados: {len(files)}")
    
    # Buscar en cada archivo
    uts_found = []
    
    for file_info in files:
        file_name = file_info['name']
        file_id = file_info['id']
        
        print(f"\n🔍 Procesando: {file_name}")
        
        try:
            # Descargar archivo
            file_buffer = download_excel_file(file_id, file_name)
            if not file_buffer:
                print(f"   ⚠️ No se pudo descargar")
                continue
            
            # Leer Excel
            df = pd.read_excel(file_buffer, engine='openpyxl')
            
            # Buscar columnas de coordenadas
            lat_cols = [c for c in df.columns if 'lat' in str(c).lower()]
            lon_cols = [c for c in df.columns if 'lon' in str(c).lower()]
            coord_cols = lat_cols + lon_cols
            
            if not coord_cols:
                print(f"   ⏭️  Sin columnas de coordenadas")
                continue
            
            print(f"   ✅ Columnas de coordenadas: {coord_cols}")
            
            # Buscar registros de Bienestar Social
            bienestar_mask = df.apply(
                lambda row: any('bienestar' in str(val).lower() for val in row if pd.notna(val)),
                axis=1
            )
            
            bienestar_data = df[bienestar_mask]
            
            if len(bienestar_data) > 0:
                print(f"   📍 {len(bienestar_data)} registros de Bienestar Social encontrados")
                
                # Mostrar primeros registros con coordenadas
                for idx, row in bienestar_data.head(5).iterrows():
                    nombre_up = None
                    direccion = None
                    lat = None
                    lon = None
                    
                    # Buscar nombre_up
                    for col in df.columns:
                        if 'nombre' in str(col).lower() and 'up' in str(col).lower():
                            nombre_up = row.get(col)
                            break
                    
                    # Buscar dirección
                    for col in df.columns:
                        if 'direcc' in str(col).lower():
                            direccion = row.get(col)
                            break
                    
                    # Buscar coordenadas
                    if lat_cols:
                        lat = row.get(lat_cols[0])
                    if lon_cols:
                        lon = row.get(lon_cols[0])
                    
                    if pd.notna(lat) and pd.notna(lon):
                        print(f"      • {nombre_up or 'N/A'}")
                        print(f"        Dirección: {direccion or 'N/A'}")
                        print(f"        Coordenadas: lat={lat}, lon={lon}")
                        
                        uts_found.append({
                            'archivo': file_name,
                            'nombre_up': nombre_up,
                            'direccion': direccion,
                            'lat': lat,
                            'lon': lon
                        })
                
        except Exception as e:
            print(f"   ❌ Error procesando: {e}")
    
    # Resumen
    print("\n" + "="*80)
    print("RESUMEN:")
    print(f"  Total archivos procesados: {len(files)}")
    print(f"  UTS con coordenadas encontradas: {len(uts_found)}")
    
    if uts_found:
        print("\n✅ SE ENCONTRARON COORDENADAS en los archivos de entrada:")
        for uts in uts_found[:10]:
            print(f"  • {uts['nombre_up']} → lat={uts['lat']}, lon={uts['lon']}")
            print(f"    Archivo: {uts['archivo']}")
    else:
        print("\n❌ NO SE ENCONTRARON COORDENADAS en los archivos de entrada")
        print("   Las UTS de Bienestar Social NO tienen coordenadas en el origen")
        print("   Solución: Geocodificar las direcciones o actualizar el archivo fuente")

except Exception as e:
    print(f"❌ Error general: {e}")
    import traceback
    traceback.print_exc()
