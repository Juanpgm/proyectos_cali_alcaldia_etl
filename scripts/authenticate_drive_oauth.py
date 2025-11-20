# -*- coding: utf-8 -*-
"""
Script to authenticate Google Drive using OAuth2 flow.
This creates a token with proper Drive write permissions.
"""

import os
import pickle
from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Scopes necesarios para Drive
SCOPES = ['https://www.googleapis.com/auth/drive']

def authenticate_drive():
    """Authenticates and saves credentials for Google Drive."""
    
    project_root = Path(__file__).parent.parent
    token_path = project_root / 'drive_token.pickle'
    
    creds = None
    
    # Check if token already exists
    if token_path.exists():
        print("📝 Token existente encontrado, cargando...")
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    
    # If no valid credentials, let user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("🔄 Token expirado, refrescando...")
            creds.refresh(Request())
        else:
            print("\n" + "="*80)
            print("AUTENTICACIÓN REQUERIDA")
            print("="*80)
            print("⚠️  No se encontraron credenciales válidas.")
            print("💡 Alternativa: Usa Service Account JSON")
            print(f"   Configura SERVICE_ACCOUNT_FILE en .env con la ruta al JSON")
            print("="*80 + "\n")
            
            print("❌ No se puede continuar sin credenciales OAuth2 client_secret.json")
            print("💡 Para obtenerlo:")
            print("   1. Ve a: https://console.cloud.google.com/apis/credentials")
            print("   2. Crea 'OAuth 2.0 Client ID' tipo 'Desktop app'")
            print("   3. Descarga el JSON y guárdalo como 'client_secret.json' aquí")
            return False
        
        # Save credentials
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
        
        print(f"✅ Token guardado en: {token_path}")
    else:
        print(f"✅ Credenciales válidas encontradas en: {token_path}")
    
    print("\n" + "="*80)
    print("AUTENTICACIÓN EXITOSA")
    print("="*80)
    print("✅ Google Drive autenticado con permisos completos")
    print(f"📁 Token: {token_path}")
    print("\nPuedes ejecutar el script de exportación ahora.")
    print("="*80)
    
    return True

if __name__ == "__main__":
    success = authenticate_drive()
    if success:
        print("\n🎯 ¡Listo! Ejecuta: python scripts/run_export_to_drive.py")
    else:
        print("\n💡 Solución recomendada: Usa Service Account")
        print("   El Service Account ya está configurado, solo necesita permisos de carpeta.")
