#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para obtener el email del Service Account
"""
import json
import os

try:
    sa_file = os.getenv('SERVICE_ACCOUNT_FILE', './sheets-service-account.json')
    
    if os.path.exists(sa_file):
        with open(sa_file, 'r') as f:
            sa_data = json.load(f)
        
        print("=" * 80)
        print("SERVICE ACCOUNT INFORMATION")
        print("=" * 80)
        print(f"\n📧 Email: {sa_data['client_email']}")
        print(f"🆔 Project ID: {sa_data['project_id']}")
        print(f"📝 Client ID: {sa_data['client_id']}")
        
        print("\n" + "=" * 80)
        print("INSTRUCCIONES PARA COMPARTIR LA CARPETA DE GOOGLE DRIVE")
        print("=" * 80)
        print("\n1. Ve a Google Drive: https://drive.google.com/drive/folders/1q7CYewsqxz51NlBzw8YEjNAIUY71aYy6")
        print("\n2. Haz clic derecho en la carpeta > 'Compartir'")
        print(f"\n3. Agrega este email: {sa_data['client_email']}")
        print("\n4. Dale permisos de 'Lector' (Read-only)")
        print("\n5. Haz clic en 'Enviar'")
        print("\n✅ Una vez compartido, el Service Account podrá leer los archivos Excel")
        print("=" * 80)
    else:
        print(f"❌ No se encontró el archivo: {sa_file}")
        print("\n💡 Alternativa: Usar Workload Identity Federation (WIF)")
        print("   El sistema ya está configurado para usar WIF con gcloud auth")
        
except Exception as e:
    print(f"❌ Error: {e}")
