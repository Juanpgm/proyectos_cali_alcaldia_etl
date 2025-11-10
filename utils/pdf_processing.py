# -*- coding: utf-8 -*-
"""
PDF Processing Utilities Module

Utilidades funcionales para procesamiento de PDFs:
- Extracción de texto con PyPDF2
- OCR con Tesseract para PDFs escaneados
- Preprocesamiento de imágenes
- Conversión PDF a imágenes

Implementa programación funcional para operaciones limpias y componibles.
"""

import os
import io
import warnings
import logging
from typing import List, Optional, Tuple, Callable, Any, Dict
from functools import wraps, reduce
from pathlib import Path
import re

# Suppress PyPDF2 warnings and logs
warnings.filterwarnings('ignore', category=UserWarning, module='PyPDF2')
warnings.filterwarnings('ignore', category=DeprecationWarning, module='PyPDF2')
logging.getLogger('PyPDF2').setLevel(logging.ERROR)

# PDF processing
import PyPDF2
try:
    from pdf2image import convert_from_path, convert_from_bytes
except ImportError:
    convert_from_path = None
    convert_from_bytes = None

try:
    from PIL import Image, ImageEnhance, ImageFilter
except ImportError:
    Image = None
    ImageEnhance = None
    ImageFilter = None

try:
    import pytesseract
except ImportError:
    pytesseract = None

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
                print(f"⚠️ Error en {func.__name__}: {e}")
                return default_value
        return wrapper
    return decorator


# Image preprocessing functions
@safe_execute(default_value=None)
def enhance_image_contrast(image: Image.Image, factor: float = 2.0) -> Image.Image:
    """Enhance image contrast for better OCR."""
    enhancer = ImageEnhance.Contrast(image)
    return enhancer.enhance(factor)


@safe_execute(default_value=None)
def enhance_image_sharpness(image: Image.Image, factor: float = 2.0) -> Image.Image:
    """Enhance image sharpness for better OCR."""
    enhancer = ImageEnhance.Sharpness(image)
    return enhancer.enhance(factor)


@safe_execute(default_value=None)
def convert_to_grayscale(image: Image.Image) -> Image.Image:
    """Convert image to grayscale."""
    return image.convert('L')


@safe_execute(default_value=None)
def apply_threshold(image: Image.Image, threshold: int = 150) -> Image.Image:
    """Apply binary threshold to image."""
    return image.point(lambda x: 0 if x < threshold else 255, '1')


def preprocess_image_for_ocr(image) -> Optional[Any]:
    """
    Preprocess image for optimal OCR using functional composition.
    
    Args:
        image: PIL Image object
        
    Returns:
        Preprocessed PIL Image or None if PIL not available
    """
    if Image is None or ImageEnhance is None:
        return None
        
    return pipe(
        image,
        convert_to_grayscale,
        lambda img: enhance_image_contrast(img, 1.5),
        lambda img: enhance_image_sharpness(img, 2.0),
        lambda img: apply_threshold(img, 128)
    )


# PDF text extraction
@safe_execute(default_value="")
def extract_text_from_pdf_page(page: PyPDF2.PageObject) -> str:
    """Extract text from a single PDF page."""
    return page.extract_text()


@safe_execute(default_value="")
def extract_text_with_pypdf2(pdf_path: str) -> str:
    """
    Extract text from PDF using PyPDF2 (for text-based PDFs).
    
    Args:
        pdf_path: Path to PDF file
        
    Returns:
        Extracted text as string
    """
    text_parts = []
    
    with open(pdf_path, 'rb') as file:
        # Suppress warnings during PDF reading
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            pdf_reader = PyPDF2.PdfReader(file, strict=False)
        
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text = extract_text_from_pdf_page(page)
            
            if text and text.strip():
                text_parts.append(f"--- Página {page_num + 1} ---\n{text}")
    
    return "\n\n".join(text_parts)


# PDF to images conversion
@safe_execute(default_value=[])
def convert_pdf_to_images(pdf_path: str, dpi: int = 300) -> List[Image.Image]:
    """
    Convert PDF pages to images for OCR processing.
    
    Args:
        pdf_path: Path to PDF file
        dpi: Resolution for image conversion (higher = better quality)
        
    Returns:
        List of PIL Image objects
    """
    print(f"🖼️ Convirtiendo PDF a imágenes (DPI: {dpi})...")
    
    # Convert PDF to images
    images = convert_from_path(pdf_path, dpi=dpi)
    
    print(f"✅ Convertidas {len(images)} páginas a imágenes")
    return images


# OCR text extraction
@safe_execute(default_value="")
def extract_text_from_image(image, lang: str = 'spa') -> str:
    """
    Extract text from image using Tesseract OCR.
    
    Args:
        image: PIL Image object
        lang: Language for OCR (default: Spanish)
        
    Returns:
        Extracted text as string
    """
    if pytesseract is None:
        print("⚠️ Tesseract no disponible, retornando texto vacío")
        return ""
        
    # Preprocess image for better OCR
    processed_image = preprocess_image_for_ocr(image)
    if processed_image is None:
        processed_image = image
    
    # Perform OCR
    custom_config = r'--oem 3 --psm 6'
    text = pytesseract.image_to_string(
        processed_image,
        lang=lang,
        config=custom_config
    )
    
    return text


@safe_execute(default_value="")
def extract_text_with_ocr(pdf_path: str, lang: str = 'spa', dpi: int = 300) -> str:
    """
    Extract text from PDF using OCR (for scanned PDFs).
    
    Args:
        pdf_path: Path to PDF file
        lang: Language for OCR (default: Spanish)
        dpi: Resolution for image conversion
        
    Returns:
        Extracted text as string
    """
    if pytesseract is None or convert_from_path is None:
        print("⚠️ OCR no disponible (Tesseract o pdf2image no instalados)")
        return ""
        
    print(f"📄 Extrayendo texto con OCR de: {Path(pdf_path).name}")
    
    # Convert PDF to images
    images = convert_pdf_to_images(pdf_path, dpi)
    
    if not images:
        print("❌ No se pudieron convertir páginas a imágenes")
        return ""
    
    # Extract text from each image
    text_parts = []
    for i, image in enumerate(images, 1):
        print(f"🔍 Procesando página {i}/{len(images)}...")
        text = extract_text_from_image(image, lang)
        
        if text and text.strip():
            text_parts.append(f"--- Página {i} ---\n{text}")
    
    return "\n\n".join(text_parts)


# Hybrid extraction (try text first, fallback to OCR)
@safe_execute(default_value="")
def extract_text_hybrid(pdf_path: str, min_text_length: int = 100) -> str:
    """
    Hybrid text extraction: tries PyPDF2 first, falls back to OCR if needed.
    
    Args:
        pdf_path: Path to PDF file
        min_text_length: Minimum text length to consider PyPDF2 successful
        
    Returns:
        Extracted text as string
    """
    print(f"📄 Extrayendo texto de: {Path(pdf_path).name}")
    
    # Try text extraction first
    print("1️⃣ Intentando extracción directa de texto...")
    text = extract_text_with_pypdf2(pdf_path)
    
    # Check if extraction was successful
    if text and len(text.strip()) >= min_text_length:
        print(f"✅ Texto extraído exitosamente ({len(text)} caracteres)")
        return text
    
    # Fallback to OCR
    if pytesseract is None or convert_from_path is None:
        print("⚠️ OCR no disponible - PDF probablemente escaneado")
        print("💡 Para procesar PDFs escaneados, instala:")
        print("   Windows: choco install tesseract poppler")
        print("   Linux: sudo apt-get install tesseract-ocr poppler-utils")
        print("   Mac: brew install tesseract poppler")
        return ""
    
    print("2️⃣ Texto insuficiente, usando OCR...")
    text = extract_text_with_ocr(pdf_path)
    
    if text and text.strip():
        print(f"✅ OCR completado ({len(text)} caracteres)")
    else:
        print("❌ No se pudo extraer texto del PDF")
    
    return text


# Text cleaning utilities
def clean_extracted_text(text: str) -> str:
    """
    Clean extracted text using functional transformations.
    
    Args:
        text: Raw extracted text
        
    Returns:
        Cleaned text
    """
    transformations = [
        # Remove excessive whitespace
        lambda t: re.sub(r'\s+', ' ', t),
        # Remove page separators but keep line breaks
        lambda t: t.replace('--- Página', '\n--- Página'),
        # Normalize line breaks
        lambda t: re.sub(r'\n\s*\n\s*\n+', '\n\n', t),
        # Strip leading/trailing whitespace
        str.strip
    ]
    
    return pipe(text, *transformations)


# PDF metadata extraction
@safe_execute(default_value={})
def extract_pdf_metadata(pdf_path: str) -> Dict[str, Any]:
    """
    Extract PDF metadata.
    
    Args:
        pdf_path: Path to PDF file
        
    Returns:
        Dictionary with PDF metadata
    """
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        
        metadata = {
            'num_pages': len(pdf_reader.pages),
            'file_size_mb': os.path.getsize(pdf_path) / (1024 * 1024),
            'file_name': Path(pdf_path).name
        }
        
        # Add PDF info if available
        if pdf_reader.metadata:
            metadata.update({
                'title': pdf_reader.metadata.get('/Title', ''),
                'author': pdf_reader.metadata.get('/Author', ''),
                'subject': pdf_reader.metadata.get('/Subject', ''),
                'creator': pdf_reader.metadata.get('/Creator', ''),
                'producer': pdf_reader.metadata.get('/Producer', ''),
                'creation_date': pdf_reader.metadata.get('/CreationDate', '')
            })
        
        return metadata


# Batch PDF processing
def process_pdf_directory(
    directory_path: str,
    extraction_method: str = 'hybrid'
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Process all PDFs in a directory.
    
    Args:
        directory_path: Path to directory containing PDFs
        extraction_method: 'text', 'ocr', or 'hybrid'
        
    Returns:
        List of tuples (filename, extracted_text, metadata)
    """
    print(f"📁 Procesando PDFs en: {directory_path}")
    
    # Get all PDF files
    pdf_files = list(Path(directory_path).glob("*.pdf"))
    
    if not pdf_files:
        print("⚠️ No se encontraron archivos PDF")
        return []
    
    print(f"📄 Encontrados {len(pdf_files)} archivos PDF")
    
    # Process each PDF
    results = []
    
    for pdf_file in pdf_files:
        print(f"\n{'='*60}")
        print(f"Procesando: {pdf_file.name}")
        print("="*60)
        
        # Extract text based on method
        if extraction_method == 'text':
            text = extract_text_with_pypdf2(str(pdf_file))
        elif extraction_method == 'ocr':
            text = extract_text_with_ocr(str(pdf_file))
        else:  # hybrid
            text = extract_text_hybrid(str(pdf_file))
        
        # Extract metadata
        metadata = extract_pdf_metadata(str(pdf_file))
        
        # Clean text
        cleaned_text = clean_extracted_text(text)
        
        results.append((pdf_file.name, cleaned_text, metadata))
        
        print(f"✅ Completado: {pdf_file.name}")
    
    return results


# Configuration check
def check_tesseract_installation() -> bool:
    """Check if Tesseract OCR is installed and accessible."""
    try:
        version = pytesseract.get_tesseract_version()
        print(f"✅ Tesseract instalado: {version}")
        return True
    except Exception as e:
        print(f"❌ Tesseract no instalado o no accesible: {e}")
        print("💡 Instala Tesseract:")
        print("   Windows: choco install tesseract")
        print("   Linux: sudo apt-get install tesseract-ocr tesseract-ocr-spa")
        print("   Mac: brew install tesseract tesseract-lang")
        return False


if __name__ == "__main__":
    """Pruebas básicas del módulo."""
    print("🧪 Probando módulo de procesamiento de PDFs")
    print("="*60)
    
    # Check Tesseract installation
    check_tesseract_installation()
    
    print("\n✅ Módulo cargado correctamente")
    print("💡 Importa las funciones necesarias:")
    print("   from utils.pdf_processing import extract_text_hybrid")
