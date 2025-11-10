# -*- coding: utf-8 -*-
"""
Script de Pruebas Automatizadas - RPC Contratos
================================================

Ejecuta pruebas automáticas de extracción, transformación y carga
de contratos RPC sin requerir interacción del usuario.

Características:
- Prueba todos los PDFs RPC en context/
- Maneja errores de forma elegante
- Muestra resultados consolidados
- Identifica qué PDFs requieren OCR
- Continúa con los que tienen texto extraíble
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Load environment variables (already handled by database.config import)
# No need to call explicitly - config.py loads on import

# Import extraction, transformation, and loading functions
from extraction_app.data_extraction_rpc_contratos import extract_rpc_from_pdf
from transformation_app.data_transformation_rpc_contratos import transform_rpc_data
from load_app.data_loading_rpc_contratos import load_rpc_to_firebase


def print_header(title: str):
    """Print formatted header."""
    print("\n" + "="*80)
    print(f"{title}")
    print("="*80)


def print_section(title: str):
    """Print formatted section."""
    print(f"\n{'─'*80}")
    print(f"📌 {title}")
    print(f"{'─'*80}")


class TestResult:
    """Container for test results."""
    
    def __init__(self, pdf_name: str):
        self.pdf_name = pdf_name
        self.extraction_success = False
        self.transformation_success = False
        self.load_success = False
        self.extracted_data = None
        self.transformed_data = None
        self.error_message = None
        self.requires_ocr = False


def test_pdf_extraction(pdf_path: Path) -> TestResult:
    """
    Test extraction and transformation on a single PDF.
    
    Args:
        pdf_path: Path to PDF file
        
    Returns:
        TestResult with extraction outcomes
    """
    result = TestResult(pdf_path.name)
    
    try:
        # Step 1: Extract
        print(f"\n📥 Extrayendo datos de: {pdf_path.name}")
        extracted = extract_rpc_from_pdf(str(pdf_path))
        
        if not extracted or not extracted.get('numero_rpc'):
            result.error_message = "No se pudo extraer datos del PDF"
            # Check if it's an OCR issue
            if "OCR" in str(extracted) or not extracted:
                result.requires_ocr = True
                result.error_message = "PDF requiere OCR (probablemente escaneado)"
            return result
        
        result.extraction_success = True
        result.extracted_data = extracted
        print(f"   ✅ Extraídos {len([k for k, v in extracted.items() if v])} campos")
        
        # Step 2: Transform
        print(f"   🔄 Validando y transformando datos...")
        transformed = transform_rpc_data(extracted)
        
        if not transformed:
            result.error_message = "Fallo en transformación"
            return result
        
        result.transformation_success = True
        result.transformed_data = transformed
        
        validation = transformed.get('validation', {})
        if validation.get('is_valid'):
            print(f"   ✅ Validación exitosa")
        else:
            errors = validation.get('errors', [])
            warnings = validation.get('warnings', [])
            print(f"   ⚠️ Validación con observaciones:")
            if errors:
                print(f"      Errores: {len(errors)}")
            if warnings:
                print(f"      Advertencias: {len(warnings)}")
        
    except Exception as e:
        result.error_message = f"Error: {str(e)}"
        print(f"   ❌ {result.error_message}")
    
    return result


def test_all_pdfs(context_dir: Path, load_to_firebase: bool = False) -> List[TestResult]:
    """
    Test all RPC PDFs in context directory.
    
    Args:
        context_dir: Path to context directory
        load_to_firebase: Whether to load successful extractions to Firebase
        
    Returns:
        List of TestResult objects
    """
    pdf_files = list(context_dir.glob("RPC*.pdf"))
    
    if not pdf_files:
        print("❌ No se encontraron PDFs RPC en context/")
        return []
    
    print(f"\n🔍 Encontrados {len(pdf_files)} PDFs para procesar")
    
    results = []
    
    for pdf_path in pdf_files:
        result = test_pdf_extraction(pdf_path)
        results.append(result)
    
    # Load to Firebase if requested and extraction successful
    if load_to_firebase:
        print_section("📤 CARGA A FIREBASE")
        successful_data = [
            r.transformed_data 
            for r in results 
            if r.extraction_success and r.transformation_success
        ]
        
        if successful_data:
            print(f"\n📤 Cargando {len(successful_data)} contratos a Firebase...")
            try:
                load_result = load_rpc_to_firebase(
                    successful_data,
                    collection_name='rpc_contratos_emprestito',
                    batch_size=50
                )
                
                if load_result.get('success'):
                    print(f"✅ Carga exitosa:")
                    print(f"   - Total: {load_result.get('total', 0)}")
                    print(f"   - Exitosos: {load_result.get('successful', 0)}")
                    print(f"   - Fallidos: {load_result.get('failed', 0)}")
                    
                    # Update results with load status
                    for r in results:
                        if r.extraction_success and r.transformation_success:
                            r.load_success = True
                else:
                    print(f"❌ Error en carga: {load_result.get('error')}")
                    
            except Exception as e:
                print(f"❌ Error durante carga: {e}")
        else:
            print("⚠️ No hay datos válidos para cargar")
    
    return results


def print_summary(results: List[TestResult]):
    """Print summary of test results."""
    print_header("📊 RESUMEN DE RESULTADOS")
    
    total = len(results)
    successful_extraction = sum(1 for r in results if r.extraction_success)
    successful_transformation = sum(1 for r in results if r.transformation_success)
    successful_load = sum(1 for r in results if r.load_success)
    requires_ocr = sum(1 for r in results if r.requires_ocr)
    
    print(f"\n📈 Estadísticas:")
    print(f"   Total PDFs procesados: {total}")
    print(f"   ✅ Extracción exitosa: {successful_extraction}/{total}")
    print(f"   ✅ Transformación exitosa: {successful_transformation}/{total}")
    if successful_load > 0:
        print(f"   ✅ Carga a Firebase exitosa: {successful_load}/{total}")
    if requires_ocr > 0:
        print(f"   ⚠️ Requieren OCR: {requires_ocr}/{total}")
    
    # Successful extractions
    if successful_extraction > 0:
        print(f"\n✅ PDFs procesados exitosamente:")
        for r in results:
            if r.extraction_success:
                numero_rpc = r.extracted_data.get('numero_rpc', 'N/A')
                beneficiario = r.extracted_data.get('beneficiario', 'N/A')
                print(f"   • {r.pdf_name}")
                print(f"     RPC: {numero_rpc} | Beneficiario: {beneficiario}")
    
    # Failed extractions
    failed = [r for r in results if not r.extraction_success]
    if failed:
        print(f"\n❌ PDFs con errores:")
        for r in failed:
            print(f"   • {r.pdf_name}")
            print(f"     Motivo: {r.error_message}")
            if r.requires_ocr:
                print(f"     💡 Instala Tesseract y poppler para procesar PDFs escaneados:")
                print(f"        Windows: choco install tesseract poppler")
    
    # Show sample extracted data
    if successful_extraction > 0:
        print(f"\n📋 Muestra de datos extraídos:")
        sample = next((r for r in results if r.extraction_success), None)
        if sample:
            print(json.dumps(sample.extracted_data, indent=2, ensure_ascii=False, default=str))


def check_prerequisites() -> bool:
    """Check basic prerequisites."""
    print_header("🔍 VERIFICANDO REQUISITOS")
    
    all_ok = True
    
    # Check Gemini API Key
    print("\n1. Gemini API Key:")
    gemini_key = os.getenv('GEMINI_API_KEY')
    if gemini_key:
        print(f"   ✅ Configurada")
    else:
        print("   ❌ No configurada")
        print("   💡 Configura GEMINI_API_KEY en .env.local")
        all_ok = False
    
    # Check Firebase
    print("\n2. Firebase Connection:")
    try:
        from database.config import get_firestore_client
        db = get_firestore_client()
        if db:
            print("   ✅ Conectado")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        all_ok = False
    
    # Check context directory
    print("\n3. Context Directory:")
    context_dir = Path(__file__).parent / "context"
    if context_dir.exists():
        pdf_count = len(list(context_dir.glob("RPC*.pdf")))
        print(f"   ✅ Encontrado ({pdf_count} PDFs RPC)")
    else:
        print("   ❌ No encontrado")
        all_ok = False
    
    return all_ok


def main():
    """Main execution function."""
    print_header("🤖 PRUEBAS AUTOMATIZADAS - RPC CONTRATOS")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Requisitos no cumplidos. Abortando.")
        return 1
    
    # Get context directory
    context_dir = Path(__file__).parent / "context"
    
    # Ask if should load to Firebase
    print_section("⚙️ CONFIGURACIÓN DE PRUEBA")
    print("\n¿Deseas cargar los datos exitosos a Firebase?")
    print("   1. Solo probar extracción (no cargar)")
    print("   2. Probar y cargar a Firebase")
    
    try:
        choice = input("\nSelecciona opción (1-2) [1]: ").strip() or "1"
        load_to_firebase = choice == "2"
    except (EOFError, KeyboardInterrupt):
        print("\n\n⚠️ Operación cancelada por el usuario")
        return 0
    
    # Run tests
    print_section("🧪 EJECUTANDO PRUEBAS")
    results = test_all_pdfs(context_dir, load_to_firebase)
    
    # Print summary
    print_summary(results)
    
    # Return exit code
    successful = sum(1 for r in results if r.extraction_success)
    if successful == 0:
        print("\n❌ Ningún PDF procesado exitosamente")
        return 1
    elif successful < len(results):
        print("\n⚠️ Algunos PDFs no pudieron procesarse")
        return 0
    else:
        print("\n✅ Todos los PDFs procesados exitosamente")
        return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️ Proceso interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
