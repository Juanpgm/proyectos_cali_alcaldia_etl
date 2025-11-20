# -*- coding: utf-8 -*-
"""
Descarga el GeoJSON más reciente desde Google Drive.
"""

import os
import io
import sys
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Configuración
SHAPES_FOLDER_ID = "1BzpTu43s5TyEOwB43fvYO4RwBfDDdX-T"
SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]

# Autenticar
print("🔐 Autenticando con Google Drive...")
creds = service_account.Credentials.from_service_account_file(
    "sheets-service-account.json", 
    scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=creds)

# Buscar archivo
print("🔍 Buscando archivo en Google Drive...")
query = f"name='unidades_proyecto.geojson' and '{SHAPES_FOLDER_ID}' in parents and trashed=false"
results = drive_service.files().list(
    q=query, 
    fields='files(id, name, modifiedTime)',
    orderBy='modifiedTime desc'
).execute()

files = results.get('files', [])

if not files:
    print("❌ Archivo no encontrado")
    sys.exit(1)

file_info = files[0]
file_id = file_info['id']
file_name = file_info['name']
modified = file_info.get('modifiedTime', 'unknown')

print(f"📥 Descargando: {file_name}")
print(f"   ID: {file_id}")
print(f"   Última modificación: {modified}")

# Descargar
request = drive_service.files().get_media(fileId=file_id)
fh = io.BytesIO()
downloader = MediaIoBaseDownload(fh, request)

done = False
while not done:
    status, done = downloader.next_chunk()
    if status:
        print(f"   Progreso: {int(status.progress() * 100)}%", end='\r')

print()  # Nueva línea después del progreso

# Guardar
output_path = "context/unidades_proyecto.geojson"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, 'wb') as f:
    f.write(fh.getvalue())

file_size = len(fh.getvalue()) / 1024
print(f"✅ Archivo descargado: {output_path}")
print(f"   Tamaño: {file_size:.1f} KB")
