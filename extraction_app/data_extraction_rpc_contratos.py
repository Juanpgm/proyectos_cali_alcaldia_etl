# -*- coding: utf-8 -*-
"""
Data Extraction Module for RPC Contratos from PDF files using Gemini AI

Extrae datos de contratos RPC desde archivos PDF usando:
- OCR para PDFs escaneados
- Google Gemini AI para extracción inteligente de campos
- Programación funcional para código limpio y escalable

Campos extraídos:
- numero_rpc
- beneficiario
- documento_identificacion
- contrato_rpc
- fecha_contabilizacion
- fecha_impresion
- estado_liberacion
- plazo_contrato (fecha de terminación)
- descripcion_rpc
- valor_rpc
- bp (código de presupuesto)
- cdp_asociados
- nombre_centro_gestor
"""

import os
import sys
import json
import re
from typing import Dict, List, Optional, Any, Callable
from functools import wraps, reduce
from pathlib import Path
from datetime import datetime
import pandas as pd

# Google Gemini AI
import google.generativeai as genai

# Add project paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import local modules
from utils.pdf_processing import (
    extract_text_hybrid,
    extract_pdf_metadata,
    check_tesseract_installation
)
from database.config import secure_log


# Functional programming utilities
def compose(*functions: Callable) -> Callable:
    """Compose multiple functions into a single function."""
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(value: Any, *functions: Callable) -> Any:
    """Apply a sequence of functions to a value (pipe operator)."""
    return reduce(lambda acc, func: func(acc), functions, value)


def safe_execute(default_value: Any = None) -> Callable:
    """Decorator to safely execute functions with error handling."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"❌ Error en {func.__name__}: {e}")
                return default_value
        return wrapper
    return decorator


# Gemini AI Configuration
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')

if not GEMINI_API_KEY:
    print("⚠️ GEMINI_API_KEY no configurada")
    print("💡 Configura la variable de entorno:")
    print("   Windows: $env:GEMINI_API_KEY = 'tu_api_key'")
    print("   Linux/Mac: export GEMINI_API_KEY='tu_api_key'")


@safe_execute(default_value=None)
def initialize_gemini() -> Optional[genai.GenerativeModel]:
    """
    Initialize Gemini AI model with API key.
    
    Returns:
        Configured Gemini model or None if fails
    """
    if not GEMINI_API_KEY:
        print("❌ No se puede inicializar Gemini sin API key")
        return None
    
    genai.configure(api_key=GEMINI_API_KEY)
    
    # Try different Gemini models in order of preference
    models_to_try = [
        'gemini-pro',  # Legacy stable model
        'models/gemini-pro',  # With models/ prefix
        'gemini-1.5-flash',  # Newer model
        'gemini-1.0-pro',  # Alternative stable model
    ]
    
    for model_name in models_to_try:
        try:
            model = genai.GenerativeModel(model_name)
            print(f"✅ Gemini AI inicializado (modelo: {model_name})")
            return model
        except Exception as e:
            print(f"⚠️ No se pudo usar {model_name}: {str(e)[:50]}")
            continue
    
    print("❌ No se pudo inicializar ningún modelo Gemini")
    return None


# Prompt engineering for RPC extraction
def create_extraction_prompt(pdf_text: str) -> str:
    """
    Create a structured prompt for Gemini AI to extract RPC contract data.
    
    Args:
        pdf_text: Extracted text from PDF
        
    Returns:
        Structured prompt string
    """
    prompt = f"""
Eres un asistente experto en extraer información de documentos contables colombianos.

Analiza el siguiente texto extraído de un documento RPC (Registro Presupuestal de Compromiso) y extrae EXACTAMENTE los siguientes campos:

**INSTRUCCIONES IMPORTANTES:**

1. **numero_rpc**: El número del RPC, generalmente aparece como "RPC Nro", "RPC No", "Registro No", etc.

2. **beneficiario**: El nombre completo del beneficiario del contrato. Aparece después de "Beneficiario:" y DESPUÉS del número de identificación.

3. **documento_identificacion**: El número de documento de identificación del beneficiario. Aparece inmediatamente después de "Beneficiario:" y ANTES del nombre. Puede ser NIT, CC, CE, etc. Solo el NÚMERO, sin puntos ni guiones.

4. **contrato_rpc**: El número del contrato asociado, puede aparecer como "Contrato No", "Contrato", etc.

5. **fecha_contabilizacion**: Fecha en formato DD/MM/YYYY de la contabilización del RPC.

6. **fecha_impresion**: Fecha de impresión del documento, formato DD/MM/YYYY.

7. **estado_liberacion**: Estado del RPC (Liberado, Pendiente, etc.).

8. **plazo_contrato**: La fecha de TERMINACIÓN del contrato. Busca "Fecha de terminación del contrato", "Plazo del contrato", o similar. Formato DD/MM/YYYY. NO confundir con fecha de inicio.

9. **descripcion_rpc**: Descripción o concepto del RPC. Busca "Concepto", "Descripción", "Observaciones", etc.

10. **valor_rpc**: El valor monetario del RPC. Busca "Valor", "NOV:" (Nuevo Valor), etc. Formato numérico sin símbolos.

11. **bp**: El código BP (Budget Planning/Proyecto). Busca patrones como "BP-XXXX" o "Proyecto: BP-XXXX". En las imágenes aparece en un RECUADRO AZUL.

12. **cdp_asociados**: Lista de CDPs asociados. Busca "CDP Asociados:", "CDP No", etc. Puede ser una lista separada por comas.

13. **nombre_centro_gestor**: El nombre del centro gestor o secretaría responsable. Busca "SECRETARIA DE", "Centro Gestor", etc.

**FORMATO DE SALIDA:**
Devuelve ÚNICAMENTE un objeto JSON válido con esta estructura exacta:

{{
    "numero_rpc": "valor extraído o null",
    "beneficiario": "valor extraído o null",
    "documento_identificacion": "valor extraído o null",
    "contrato_rpc": "valor extraído o null",
    "fecha_contabilizacion": "DD/MM/YYYY o null",
    "fecha_impresion": "DD/MM/YYYY o null",
    "estado_liberacion": "valor extraído o null",
    "plazo_contrato": "DD/MM/YYYY o null",
    "descripcion_rpc": "valor extraído o null",
    "valor_rpc": "valor numérico o null",
    "bp": "valor extraído o null",
    "cdp_asociados": ["lista de CDPs"] o null,
    "nombre_centro_gestor": "valor extraído o null"
}}

**REGLAS IMPORTANTES:**
- Si un campo no se encuentra, usa null (no string "null", sino JSON null)
- Para fechas, usa SIEMPRE formato DD/MM/YYYY
- Para documento_identificacion, extrae SOLO el número, sin espacios ni caracteres especiales
- Para valor_rpc, extrae solo números (sin $ ni puntos ni comas)
- Para cdp_asociados, devuelve un array aunque solo haya uno
- NO incluyas explicaciones adicionales, SOLO el JSON

**TEXTO DEL DOCUMENTO RPC:**

{pdf_text}

**RESPUESTA (SOLO JSON):**
"""
    return prompt


@safe_execute(default_value={})
def extract_data_with_gemini(
    pdf_text: str,
    model: genai.GenerativeModel,
    max_retries: int = 2
) -> Dict[str, Any]:
    """
    Extract structured data from PDF text using Gemini AI.
    
    Args:
        pdf_text: Extracted text from PDF
        model: Initialized Gemini model
        max_retries: Number of retry attempts
        
    Returns:
        Dictionary with extracted fields
    """
    print("🤖 Extrayendo datos con Gemini AI...")
    
    if not model:
        print("❌ Modelo Gemini no inicializado")
        return {}
    
    # Create extraction prompt
    prompt = create_extraction_prompt(pdf_text)
    
    # Try extraction with retries
    for attempt in range(max_retries):
        try:
            print(f"📤 Intento {attempt + 1}/{max_retries}...")
            
            # Generate response
            response = model.generate_content(prompt)
            
            # Extract text from response
            response_text = response.text.strip()
            
            # Clean response (remove markdown code blocks if present)
            response_text = re.sub(r'^```json\s*', '', response_text)
            response_text = re.sub(r'^```\s*', '', response_text)
            response_text = re.sub(r'\s*```$', '', response_text)
            response_text = response_text.strip()
            
            # Parse JSON
            extracted_data = json.loads(response_text)
            
            print("✅ Datos extraídos exitosamente")
            return extracted_data
            
        except json.JSONDecodeError as e:
            print(f"⚠️ Error parseando JSON (intento {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                print("❌ No se pudo parsear respuesta después de múltiples intentos")
                print(f"Respuesta recibida: {response_text[:500]}...")
        
        except Exception as e:
            print(f"⚠️ Error en extracción (intento {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                print("❌ Fallo en extracción después de múltiples intentos")
    
    return {}


# Post-processing and validation
def validate_and_clean_extracted_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and clean extracted data.
    
    Args:
        data: Raw extracted data from Gemini
        
    Returns:
        Cleaned and validated data
    """
    cleaned = {}
    
    # Clean strings
    for key, value in data.items():
        if value is None or value == "" or value == "null":
            cleaned[key] = None
        elif isinstance(value, str):
            cleaned[key] = value.strip()
        else:
            cleaned[key] = value
    
    # Validate and clean specific fields
    
    # documento_identificacion: remove formatting
    if cleaned.get('documento_identificacion'):
        doc = cleaned['documento_identificacion']
        # Remove dots, hyphens, spaces
        doc = re.sub(r'[.\-\s]', '', doc)
        cleaned['documento_identificacion'] = doc
    
    # valor_rpc: ensure numeric
    if cleaned.get('valor_rpc'):
        valor = str(cleaned['valor_rpc'])
        # Remove currency symbols and formatting
        valor = re.sub(r'[$.,\s]', '', valor)
        try:
            cleaned['valor_rpc'] = float(valor)
        except ValueError:
            cleaned['valor_rpc'] = None
    
    # Dates: validate format
    date_fields = ['fecha_contabilizacion', 'fecha_impresion', 'plazo_contrato']
    for field in date_fields:
        if cleaned.get(field):
            # Simple validation: should match DD/MM/YYYY
            if not re.match(r'\d{2}/\d{2}/\d{4}', str(cleaned[field])):
                print(f"⚠️ Formato de fecha inválido en {field}: {cleaned[field]}")
    
    # cdp_asociados: ensure it's a list
    if cleaned.get('cdp_asociados') and not isinstance(cleaned['cdp_asociados'], list):
        cleaned['cdp_asociados'] = [cleaned['cdp_asociados']]
    
    return cleaned


# Main extraction function
@secure_log
@safe_execute(default_value=None)
def extract_rpc_from_pdf(pdf_path: str) -> Optional[Dict[str, Any]]:
    """
    Complete extraction pipeline for a single RPC PDF.
    
    Args:
        pdf_path: Path to PDF file
        
    Returns:
        Dictionary with extracted RPC data or None if fails
    """
    print(f"\n{'='*70}")
    print(f"📄 Procesando: {Path(pdf_path).name}")
    print("="*70)
    
    # Step 1: Extract text from PDF
    print("\n1️⃣ Extrayendo texto del PDF...")
    pdf_text = extract_text_hybrid(pdf_path)
    
    if not pdf_text or len(pdf_text.strip()) < 50:
        print("❌ No se pudo extraer texto del PDF")
        return None
    
    print(f"✅ Texto extraído: {len(pdf_text)} caracteres")
    
    # Step 2: Extract metadata
    print("\n2️⃣ Extrayendo metadata del PDF...")
    metadata = extract_pdf_metadata(pdf_path)
    print(f"✅ Metadata extraída: {metadata.get('num_pages', 0)} páginas")
    
    # Step 3: Initialize Gemini
    print("\n3️⃣ Inicializando Gemini AI...")
    model = initialize_gemini()
    
    if not model:
        print("❌ No se pudo inicializar Gemini")
        return None
    
    # Step 4: Extract data with AI
    print("\n4️⃣ Extrayendo campos con IA...")
    extracted_data = extract_data_with_gemini(pdf_text, model)
    
    if not extracted_data:
        print("❌ No se pudieron extraer datos")
        return None
    
    # Step 5: Validate and clean
    print("\n5️⃣ Validando y limpiando datos...")
    cleaned_data = validate_and_clean_extracted_data(extracted_data)
    
    # Step 6: Add metadata
    cleaned_data['metadata'] = {
        'source_file': Path(pdf_path).name,
        'extraction_date': datetime.now().isoformat(),
        'pdf_pages': metadata.get('num_pages'),
        'text_length': len(pdf_text)
    }
    
    print("\n✅ Extracción completada exitosamente")
    print(f"📊 Campos extraídos: {len([v for v in cleaned_data.values() if v is not None])}")
    
    return cleaned_data


# Batch processing
@safe_execute(default_value=[])
def extract_rpc_from_directory(
    directory_path: str,
    output_format: str = 'json'
) -> List[Dict[str, Any]]:
    """
    Extract RPC data from all PDFs in a directory.
    
    Args:
        directory_path: Path to directory containing PDFs
        output_format: 'json', 'csv', or 'both'
        
    Returns:
        List of extracted RPC data dictionaries
    """
    print(f"\n{'='*70}")
    print(f"📁 Procesando directorio: {directory_path}")
    print("="*70)
    
    # Check prerequisites
    print("\n🔍 Verificando requisitos...")
    if not check_tesseract_installation():
        print("⚠️ Tesseract no disponible, OCR puede fallar")
    
    if not GEMINI_API_KEY:
        print("❌ GEMINI_API_KEY no configurada")
        return []
    
    # Find all PDFs
    pdf_files = list(Path(directory_path).glob("*.pdf"))
    
    if not pdf_files:
        print(f"⚠️ No se encontraron archivos PDF en {directory_path}")
        return []
    
    print(f"✅ Encontrados {len(pdf_files)} archivos PDF")
    
    # Process each PDF
    results = []
    successful = 0
    failed = 0
    
    for i, pdf_file in enumerate(pdf_files, 1):
        print(f"\n{'='*70}")
        print(f"Archivo {i}/{len(pdf_files)}")
        print("="*70)
        
        extracted = extract_rpc_from_pdf(str(pdf_file))
        
        if extracted:
            results.append(extracted)
            successful += 1
        else:
            failed += 1
            print(f"❌ Fallo en: {pdf_file.name}")
    
    # Summary
    print(f"\n{'='*70}")
    print("📊 RESUMEN DE PROCESAMIENTO")
    print("="*70)
    print(f"✅ Exitosos: {successful}")
    print(f"❌ Fallidos: {failed}")
    print(f"📄 Total: {len(pdf_files)}")
    
    return results


# Save results
@safe_execute(default_value=False)
def save_extracted_data(
    data: List[Dict[str, Any]],
    output_dir: str,
    output_name: str = "rpc_contratos_extracted"
) -> bool:
    """
    Save extracted data to JSON and CSV files.
    
    Args:
        data: List of extracted RPC dictionaries
        output_dir: Directory to save outputs
        output_name: Base name for output files
        
    Returns:
        True if saved successfully
    """
    if not data:
        print("⚠️ No hay datos para guardar")
        return False
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Save JSON
    json_file = output_path / f"{output_name}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"💾 JSON guardado: {json_file}")
    
    # Save CSV
    try:
        # Flatten metadata for CSV
        flattened_data = []
        for item in data:
            flat_item = {k: v for k, v in item.items() if k != 'metadata'}
            if 'metadata' in item:
                flat_item['source_file'] = item['metadata'].get('source_file')
                flat_item['extraction_date'] = item['metadata'].get('extraction_date')
            flattened_data.append(flat_item)
        
        df = pd.DataFrame(flattened_data)
        csv_file = output_path / f"{output_name}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        print(f"💾 CSV guardado: {csv_file}")
    except Exception as e:
        print(f"⚠️ No se pudo guardar CSV: {e}")
    
    return True


if __name__ == "__main__":
    """Prueba del módulo de extracción."""
    print("🧪 Módulo de Extracción RPC Contratos")
    print("="*70)
    
    # Check configuration
    print("\n🔍 Verificando configuración...")
    check_tesseract_installation()
    
    if GEMINI_API_KEY:
        print("✅ GEMINI_API_KEY configurada")
    else:
        print("❌ GEMINI_API_KEY no configurada")
    
    print("\n✅ Módulo cargado correctamente")
    print("💡 Uso:")
    print("   from extraction_app.data_extraction_rpc_contratos import extract_rpc_from_pdf")
    print("   data = extract_rpc_from_pdf('path/to/rpc.pdf')")
