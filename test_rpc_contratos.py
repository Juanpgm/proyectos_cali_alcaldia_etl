# -*- coding: utf-8 -*-
"""
Script de Prueba para Procesamiento de Contratos RPC

Prueba el pipeline completo con los PDFs de ejemplo en la carpeta context/:
- RPC 4500357611 JUAN PABLO GUZMÁN MARTÍNEZ firmado.pdf
- RPC 45003869901 PROYECTOS Y CONSULTORIAS ORION.pdf

Este script demuestra el uso del pipeline y valida la extracción con IA.
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.rpc_contratos_emprestito_pipeline import run_rpc_contratos_pipeline
from extraction_app.data_extraction_rpc_contratos import extract_rpc_from_pdf
from transformation_app.data_transformation_rpc_contratos import transform_rpc_data
from utils.pdf_processing import check_tesseract_installation


def print_header(title: str):
    """Print formatted header."""
    print(f"\n{'='*80}")
    print(f"{title}")
    print("="*80)


def check_prerequisites():
    """Check that all prerequisites are met."""
    print_header("🔍 VERIFICANDO REQUISITOS PREVIOS")
    
    all_ok = True
    
    # Check Tesseract
    print("\n1. Tesseract OCR:")
    if check_tesseract_installation():
        print("   ✅ Instalado y accesible")
    else:
        print("   ❌ No instalado o no accesible")
        print("   💡 Instala Tesseract:")
        print("      Windows: choco install tesseract")
        print("      O descarga: https://github.com/UB-Mannheim/tesseract/wiki")
        all_ok = False
    
    # Check Gemini API Key
    print("\n2. Gemini API Key:")
    gemini_key = os.getenv('GEMINI_API_KEY')
    if gemini_key:
        print(f"   ✅ Configurada (longitud: {len(gemini_key)})")
    else:
        print("   ❌ No configurada")
        print("   💡 Configura la variable de entorno:")
        print("      PowerShell: $env:GEMINI_API_KEY = 'tu_api_key'")
        print("      O agrégala a tu archivo .env")
        print("      Obtén tu key en: https://makersuite.google.com/app/apikey")
        all_ok = False
    
    # Check Firebase credentials
    print("\n3. Firebase Credentials:")
    try:
        from database.config import get_firestore_client
        db = get_firestore_client()
        if db:
            print("   ✅ Configuradas correctamente")
        else:
            print("   ❌ No se pudo conectar a Firebase")
            all_ok = False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        all_ok = False
    
    # Check context directory
    print("\n4. PDFs de Prueba:")
    context_dir = Path(__file__).parent / "context"
    
    if context_dir.exists():
        pdf_files = list(context_dir.glob("RPC*.pdf"))
        if pdf_files:
            print(f"   ✅ Encontrados {len(pdf_files)} PDFs RPC:")
            for pdf in pdf_files:
                print(f"      - {pdf.name}")
        else:
            print("   ⚠️ No se encontraron PDFs RPC en context/")
            all_ok = False
    else:
        print("   ❌ Directorio context/ no encontrado")
        all_ok = False
    
    return all_ok, context_dir if context_dir.exists() else None


def test_single_pdf(pdf_path: Path):
    """Test extraction and transformation on a single PDF."""
    print_header(f"🧪 PRUEBA: {pdf_path.name}")
    
    # Step 1: Extract
    print("\n📥 1. Extracción con Gemini AI...")
    extracted = extract_rpc_from_pdf(str(pdf_path))
    
    if not extracted:
        print("❌ Fallo en extracción")
        return False
    
    print("✅ Extracción completada")
    print("\n📋 Datos extraídos:")
    print(json.dumps(extracted, indent=2, ensure_ascii=False, default=str))
    
    # Step 2: Transform
    print("\n🔄 2. Transformación y validación...")
    transformed = transform_rpc_data(extracted)
    
    if not transformed:
        print("❌ Fallo en transformación")
        return False
    
    print("✅ Transformación completada")
    
    # Show validation
    validation = transformed.get('validation', {})
    print(f"\n📊 Validación:")
    print(f"   Estado: {'✅ Válido' if validation.get('is_valid') else '❌ Inválido'}")
    
    if validation.get('errors'):
        print(f"   Errores: {len(validation['errors'])}")
        for error in validation['errors']:
            print(f"      - {error}")
    
    if validation.get('warnings'):
        print(f"   Advertencias: {len(validation['warnings'])}")
        for warning in validation['warnings']:
            print(f"      - {warning}")
    
    # Show key fields
    print(f"\n🔑 Campos principales:")
    print(f"   Número RPC: {transformed.get('numero_rpc')}")
    print(f"   Beneficiario: {transformed.get('beneficiario')}")
    print(f"   Documento: {transformed.get('documento_identificacion')}")
    print(f"   Valor: ${transformed.get('valor_rpc'):,.2f}" if transformed.get('valor_rpc') else "   Valor: N/A")
    print(f"   BP: {transformed.get('bp')}")
    print(f"   Plazo contrato: {transformed.get('plazo_contrato')}")
    
    return True


def test_full_pipeline(context_dir: Path):
    """Test the full pipeline with all RPC PDFs."""
    print_header("🚀 PRUEBA COMPLETA DEL PIPELINE")
    
    # Find RPC PDFs
    pdf_files = list(context_dir.glob("RPC*.pdf"))
    
    if not pdf_files:
        print("❌ No se encontraron PDFs RPC en context/")
        return False
    
    print(f"\n📄 Se procesarán {len(pdf_files)} PDFs:")
    for pdf in pdf_files:
        print(f"   - {pdf.name}")
    
    # Ask for confirmation
    print("\n⚠️  NOTA: Los datos se cargarán a Firebase Firestore")
    print("   Colección: rpc_contratos_emprestito")
    
    response = input("\n¿Continuar? (s/n): ").strip().lower()
    
    if response != 's':
        print("❌ Cancelado por el usuario")
        return False
    
    # Run pipeline
    print("\n🚀 Ejecutando pipeline completo...")
    
    success = run_rpc_contratos_pipeline(
        pdf_source=str(context_dir),
        collection_name="rpc_contratos_emprestito",
        save_intermediate=True,
        update_existing=True
    )
    
    return success


def main():
    """Main test function."""
    print_header("🧪 SCRIPT DE PRUEBA - CONTRATOS RPC")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check prerequisites
    all_ok, context_dir = check_prerequisites()
    
    if not all_ok:
        print_header("❌ REQUISITOS NO CUMPLIDOS")
        print("\n💡 Por favor, configura todos los requisitos antes de continuar.")
        print("\n📖 Consulta la documentación:")
        print("   - docs/RPC_CONTRATOS_README.md")
        print("   - docs/quick-setup.md")
        return False
    
    print_header("✅ TODOS LOS REQUISITOS CUMPLIDOS")
    
    # Menu
    print("\n📋 Opciones de prueba:")
    print("   1. Probar extracción de un solo PDF (sin cargar a Firebase)")
    print("   2. Ejecutar pipeline completo (carga a Firebase)")
    print("   3. Salir")
    
    choice = input("\nSelecciona una opción (1-3): ").strip()
    
    if choice == "1":
        # Test single PDF
        pdf_files = list(context_dir.glob("RPC*.pdf"))
        
        if not pdf_files:
            print("❌ No se encontraron PDFs RPC")
            return False
        
        print("\n📄 PDFs disponibles:")
        for i, pdf in enumerate(pdf_files, 1):
            print(f"   {i}. {pdf.name}")
        
        pdf_choice = input(f"\nSelecciona un PDF (1-{len(pdf_files)}): ").strip()
        
        try:
            pdf_index = int(pdf_choice) - 1
            if 0 <= pdf_index < len(pdf_files):
                success = test_single_pdf(pdf_files[pdf_index])
            else:
                print("❌ Opción inválida")
                return False
        except ValueError:
            print("❌ Opción inválida")
            return False
    
    elif choice == "2":
        # Test full pipeline
        success = test_full_pipeline(context_dir)
    
    elif choice == "3":
        print("👋 Adiós")
        return True
    
    else:
        print("❌ Opción inválida")
        return False
    
    # Summary
    if success:
        print_header("🎉 PRUEBA COMPLETADA EXITOSAMENTE")
        print("\n✨ El sistema de procesamiento de contratos RPC está funcionando correctamente")
        print("\n💡 Próximos pasos:")
        print("   - Revisa los datos en Firebase Console")
        print("   - Ejecuta el pipeline con más PDFs")
        print("   - Integra con tu aplicación frontend")
    else:
        print_header("❌ PRUEBA FALLIDA")
        print("\n🔧 Revisa los logs arriba para identificar el problema")
    
    return success


if __name__ == "__main__":
    """
    Script de prueba para el módulo de contratos RPC.
    
    Uso:
        python test_rpc_contratos.py
    
    Requisitos previos:
        1. Tesseract OCR instalado
        2. GEMINI_API_KEY configurada
        3. Firebase credentials configuradas
        4. PDFs en la carpeta context/
    """
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n👋 Interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
