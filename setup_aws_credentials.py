# -*- coding: utf-8 -*-
"""
Script interactivo para configurar credenciales AWS.

Este script ayuda a crear el archivo aws_credentials.json de forma segura.
"""

import json
import os
from pathlib import Path
import getpass


def create_credentials_file():
    """Crea el archivo de credenciales AWS de forma interactiva."""
    
    print("="*80)
    print("CONFIGURACIÓN DE CREDENCIALES AWS PARA S3")
    print("="*80)
    print()
    print("Este asistente te ayudará a configurar las credenciales AWS.")
    print()
    print("⚠️  IMPORTANTE: Tus credenciales se guardarán localmente en:")
    print("   aws_credentials.json (Este archivo NO se subirá a Git)")
    print()
    
    # Check if file already exists
    creds_file = Path("aws_credentials.json")
    if creds_file.exists():
        print("⚠️  El archivo aws_credentials.json ya existe.")
        overwrite = input("¿Deseas sobrescribirlo? (s/N): ").strip().lower()
        if overwrite != 's':
            print("\n❌ Operación cancelada.")
            return
        print()
    
    # Collect credentials
    print("Por favor, ingresa tus credenciales AWS:")
    print()
    
    aws_access_key_id = input("AWS Access Key ID: ").strip()
    if not aws_access_key_id:
        print("\n❌ Error: AWS Access Key ID es requerido.")
        return
    
    # Use getpass for secret key (hides input)
    aws_secret_access_key = getpass.getpass("AWS Secret Access Key: ").strip()
    if not aws_secret_access_key:
        print("\n❌ Error: AWS Secret Access Key es requerido.")
        return
    
    region = input("AWS Region [us-east-1]: ").strip() or "us-east-1"
    bucket_name = input("S3 Bucket Name [unidades-proyecto-documents]: ").strip() or "unidades-proyecto-documents"
    
    # Create credentials dictionary
    credentials = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "region": region,
        "bucket_name": bucket_name
    }
    
    # Save to file
    try:
        with open("aws_credentials.json", 'w', encoding='utf-8') as f:
            json.dump(credentials, f, ensure_ascii=False, indent=4)
        
        print()
        print("="*80)
        print("✅ CREDENCIALES GUARDADAS EXITOSAMENTE")
        print("="*80)
        print()
        print(f"📁 Archivo creado: {creds_file.absolute()}")
        print(f"🪣 Bucket configurado: {bucket_name}")
        print(f"🌍 Región: {region}")
        print()
        print("Próximos pasos:")
        print("1. Verifica que las credenciales funcionan ejecutando:")
        print("   python -c \"from utils.s3_uploader import S3Uploader; S3Uploader()\"")
        print()
        print("2. Ejecuta el pipeline de transformación:")
        print("   python transformation_app/data_transformation_unidades_proyecto.py")
        print()
        print("⚠️  RECORDATORIO: Este archivo está protegido en .gitignore")
        print("   No lo subas manualmente al repositorio.")
        print()
        
    except Exception as e:
        print(f"\n❌ Error al guardar credenciales: {e}")
        return


def main():
    """Función principal."""
    try:
        create_credentials_file()
    except KeyboardInterrupt:
        print("\n\n❌ Operación cancelada por el usuario.")
    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()
