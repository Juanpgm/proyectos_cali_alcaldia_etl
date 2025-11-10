# Módulo de Contratos RPC - ETL con IA

## 📋 Descripción General

Sistema ETL completo para procesar **Registros Presupuestales de Compromiso (RPC)** desde archivos PDF usando inteligencia artificial. Implementa extracción inteligente con **Google Gemini AI** y **OCR** para convertir documentos PDF en datos estructurados en Firebase Firestore.

### Características Principales

- ✅ **Extracción Inteligente con IA**: Usa Google Gemini Pro para interpretar el contenido de los PDFs
- ✅ **OCR Automático**: Procesa PDFs escaneados con Tesseract OCR
- ✅ **Validación Robusta**: Normaliza y valida todos los campos extraídos
- ✅ **Carga Batch a Firebase**: Operaciones optimizadas con batch processing
- ✅ **Programación Funcional**: Código limpio, testeable y mantenible
- ✅ **Manejo de Duplicados**: Detecta y actualiza registros existentes
- ✅ **Logging Detallado**: Seguimiento completo del proceso ETL

## 🏗️ Arquitectura

```
context/                              # PDFs de entrada
    └── RPC *.pdf

utils/
    └── pdf_processing.py            # OCR y extracción de texto

extraction_app/
    └── data_extraction_rpc_contratos.py   # Gemini AI extrae campos

transformation_app/
    └── data_transformation_rpc_contratos.py   # Valida y normaliza

load_app/
    └── data_loading_rpc_contratos.py    # Carga a Firebase

pipelines/
    └── rpc_contratos_emprestito_pipeline.py   # Orquesta ETL completo

Firebase Firestore
    └── rpc_contratos_emprestito/    # Colección con datos estructurados
```

## 📦 Esquema de Datos

La colección `rpc_contratos_emprestito` en Firestore contiene documentos con la siguiente estructura:

```javascript
{
  // Campos principales
  "numero_rpc": "RPC-12345",
  "beneficiario": "JUAN PABLO GUZMAN MARTINEZ",
  "documento_identificacion": "4500357611",
  "contrato_rpc": "Contrato-456",
  
  // Fechas (formato DD/MM/YYYY)
  "fecha_contabilizacion": "15/03/2026",
  "fecha_impresion": "16/03/2026",
  "plazo_contrato": "31/03/2026",  // Fecha de terminación
  
  // Estado y descripción
  "estado_liberacion": "Liberado",
  "descripcion_rpc": "Realizar Interventoría a las obras de...",
  
  // Valores monetarios
  "valor_rpc": 170248807.00,
  
  // Códigos presupuestales
  "bp": "BP-2600470101/01/02",
  "cdp_asociados": ["CDP-123", "CDP-456"],
  
  // Centro gestor
  "nombre_centro_gestor": "SECRETARIA DE EDUCACION",
  
  // Metadata
  "metadata": {
    "source_file": "RPC 4500357611.pdf",
    "extraction_date": "2025-11-09T10:30:00",
    "pdf_pages": 2,
    "transformed_at": "2025-11-09T10:30:15"
  },
  
  // Timestamps
  "created_at": "2025-11-09T10:30:20",
  "updated_at": "2025-11-09T10:30:20"
}
```

## 🚀 Instalación y Configuración

### 1. Instalar Dependencias

```powershell
# Instalar paquetes Python
pip install -r requirements.txt
```

Las nuevas dependencias incluidas:

- `google-generativeai`: API de Google Gemini
- `PyPDF2`: Extracción de texto de PDFs
- `pdf2image`: Conversión PDF a imágenes
- `pytesseract`: OCR para PDFs escaneados
- `Pillow`: Procesamiento de imágenes

### 2. Instalar Tesseract OCR

**Windows:**
```powershell
# Con Chocolatey
choco install tesseract

# O descarga el instalador
# https://github.com/UB-Mannheim/tesseract/wiki
```

**Linux:**
```bash
sudo apt-get install tesseract-ocr tesseract-ocr-spa
```

**macOS:**
```bash
brew install tesseract tesseract-lang
```

Verifica la instalación:
```powershell
tesseract --version
```

### 3. Configurar Google Gemini API Key

**Obtener API Key:**
1. Ve a [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Crea una nueva API key
3. Copia la key

**Configurar en tu sistema:**

```powershell
# PowerShell (temporal)
$env:GEMINI_API_KEY = "tu_api_key_aqui"

# Agregar a .env.local (permanente - recomendado)
# Crea o edita: .env.local
GEMINI_API_KEY=tu_api_key_aqui
```

### 4. Configurar Firebase

El proyecto ya usa **Workload Identity Federation**. Solo necesitas:

```powershell
# Autenticarte con Application Default Credentials
gcloud auth application-default login
```

## 📖 Uso

### Opción 1: Script de Prueba Interactivo (Recomendado)

```powershell
python test_rpc_contratos.py
```

Este script:
- ✅ Verifica todos los requisitos
- ✅ Te permite probar con un PDF individual
- ✅ O ejecutar el pipeline completo
- ✅ Muestra resultados detallados

### Opción 2: Pipeline Completo desde CLI

```powershell
# Procesar un solo PDF
python pipelines/rpc_contratos_emprestito_pipeline.py "context/RPC 4500357611 JUAN PABLO GUZMÁN MARTÍNEZ firmado.pdf"

# Procesar todos los PDFs en un directorio
python pipelines/rpc_contratos_emprestito_pipeline.py context/

# Con opciones personalizadas
python pipelines/rpc_contratos_emprestito_pipeline.py context/ --collection rpc_test --no-update
```

**Opciones disponibles:**
- `--collection`: Nombre de colección en Firestore (default: `rpc_contratos_emprestito`)
- `--no-save-intermediate`: No guardar archivos JSON/CSV intermedios
- `--no-update`: No actualizar documentos existentes

### Opción 3: Uso Programático

```python
from pipelines.rpc_contratos_emprestito_pipeline import run_rpc_contratos_pipeline

# Ejecutar pipeline
success = run_rpc_contratos_pipeline(
    pdf_source="context/",
    collection_name="rpc_contratos_emprestito",
    save_intermediate=True,
    update_existing=True
)

if success:
    print("✅ Pipeline completado")
```

### Opción 4: Extracción Individual (sin Firebase)

```python
from extraction_app.data_extraction_rpc_contratos import extract_rpc_from_pdf
from transformation_app.data_transformation_rpc_contratos import transform_rpc_data

# Extraer datos de un PDF
extracted = extract_rpc_from_pdf("path/to/rpc.pdf")

# Transformar y validar
if extracted:
    transformed = transform_rpc_data(extracted)
    
    # Verificar validación
    if transformed['validation']['is_valid']:
        print("✅ Datos válidos")
        print(f"RPC: {transformed['numero_rpc']}")
        print(f"Beneficiario: {transformed['beneficiario']}")
```

## 🔍 Campos Extraídos

### Identificación
- **numero_rpc**: Número del RPC (ej: "RPC-12345")
- **contrato_rpc**: Número del contrato asociado
- **documento_identificacion**: NIT o CC del beneficiario (solo números)

### Beneficiario
- **beneficiario**: Nombre completo del beneficiario

### Fechas (formato DD/MM/YYYY)
- **fecha_contabilizacion**: Fecha de contabilización
- **fecha_impresion**: Fecha de impresión del documento
- **plazo_contrato**: **Fecha de terminación del contrato** (no confundir con fecha de inicio)

### Financiero
- **valor_rpc**: Valor monetario (numérico, sin símbolos)
- **bp**: Código BP (Budget Planning/Proyecto)
- **cdp_asociados**: Lista de CDPs relacionados

### Administrativo
- **estado_liberacion**: Estado del RPC (Liberado, Pendiente, etc.)
- **descripcion_rpc**: Descripción o concepto del RPC
- **nombre_centro_gestor**: Secretaría o centro responsable

## 🎯 Lógica de Extracción Especial

El sistema implementa lógica especial para campos que aparecen en ubicaciones específicas:

### 1. Documento de Identificación
Aparece **después de "Beneficiario:"** y **ANTES del nombre**:
```
Beneficiario: 4500357611 JUAN PABLO GUZMAN MARTINEZ
              ↑↑↑↑↑↑↑↑↑↑
              Este es el documento_identificacion
```

### 2. Plazo del Contrato
Es la **fecha de TERMINACIÓN** (no de inicio). Busca:
- "Fecha de terminación del contrato"
- "Plazo del contrato"
- Generalmente aparece con **flecha roja** en las imágenes

### 3. BP (Budget Planning)
El código BP aparece en un **recuadro azul** en el documento.
Formato típico: `BP-2600470101/01/02`

## 📊 Validación y Normalización

### Validaciones Automáticas

- ✅ **Campos requeridos**: numero_rpc, beneficiario, documento_identificacion, valor_rpc
- ✅ **Formato de fechas**: DD/MM/YYYY
- ✅ **Documento ID**: 5-15 dígitos numéricos
- ✅ **Valores monetarios**: Números positivos
- ✅ **Formato RPC**: Contiene números

### Normalizaciones Aplicadas

- **Nombres**: Title Case, espacios normalizados
- **Documentos**: Solo números, sin puntos ni guiones
- **Fechas**: Convertidas a formato DD/MM/YYYY
- **Valores**: Convertidos a float, sin símbolos
- **BP**: Mayúsculas, formato estandarizado
- **CDPs**: Lista limpia, sin espacios

## 🔧 Troubleshooting

### Error: "Tesseract no instalado"

```powershell
# Windows
choco install tesseract

# Verifica
tesseract --version
```

### Error: "GEMINI_API_KEY no configurada"

```powershell
# Configura temporalmente
$env:GEMINI_API_KEY = "tu_key"

# O agrega a .env.local
echo "GEMINI_API_KEY=tu_key" >> .env.local
```

### Error: "No se pudo conectar a Firebase"

```powershell
# Re-autentica
gcloud auth application-default login

# Verifica proyecto
python database/config.py
```

### La extracción de texto falla

1. **Verifica el PDF**: ¿Es escaneado o tiene texto seleccionable?
2. **Prueba OCR manualmente**:
   ```python
   from utils.pdf_processing import extract_text_hybrid
   text = extract_text_hybrid("path/to/pdf.pdf")
   print(text)
   ```

### Gemini no extrae correctamente

1. **Revisa el prompt** en `extraction_app/data_extraction_rpc_contratos.py`
2. **Aumenta max_retries** si es problema temporal
3. **Verifica el texto extraído** antes de enviarlo a Gemini

## 📈 Monitoreo

### Ver estadísticas de la colección

```python
from load_app.data_loading_rpc_contratos import get_collection_stats

stats = get_collection_stats("rpc_contratos_emprestito")
print(stats)
```

### Consultar contratos

```python
from load_app.data_loading_rpc_contratos import (
    get_all_rpc_contracts,
    get_rpc_contract_by_id,
    query_rpc_by_beneficiary
)

# Todos los contratos
contracts = get_all_rpc_contracts(limit=10)

# Por ID
contract = get_rpc_contract_by_id("RPC-12345")

# Por beneficiario
contracts = query_rpc_by_beneficiary("4500357611")
```

## 🔐 Seguridad

- ✅ **No se almacenan credenciales estáticas**: Usa Workload Identity Federation
- ✅ **API Keys en variables de entorno**: Nunca en código
- ✅ **Archivos .env.local ignorados**: No se commitean a Git
- ✅ **Validación de entrada**: Previene inyección de datos inválidos

## 🎓 Arquitectura Técnica

### Programación Funcional

Todo el código usa patrones funcionales:

```python
# Composición de funciones
result = pipe(
    pdf_path,
    extract_text_hybrid,
    lambda text: extract_data_with_gemini(text, model),
    validate_and_clean_extracted_data
)

# Operaciones seguras con @safe_execute
@safe_execute(default_value=None)
def process_pdf(path):
    return extract_rpc_from_pdf(path)
```

### Flujo de Datos

```
PDF File
   ↓
[PDF Processing] → Extract text (PyPDF2/OCR)
   ↓
[Gemini AI] → Structured data extraction
   ↓
[Transformation] → Validation & normalization
   ↓
[Firebase] → Batch upload to Firestore
```

## 📚 Archivos Principales

```
utils/pdf_processing.py                    # 350 líneas - OCR y procesamiento
extraction_app/data_extraction_rpc_contratos.py   # 450 líneas - Gemini AI
transformation_app/data_transformation_rpc_contratos.py  # 400 líneas - Validación
load_app/data_loading_rpc_contratos.py     # 350 líneas - Firebase
pipelines/rpc_contratos_emprestito_pipeline.py    # 500 líneas - Orquestación
test_rpc_contratos.py                      # 250 líneas - Script de prueba
```

## 🤝 Contribución

Este módulo sigue los mismos patrones que el resto del proyecto:

- **Programación funcional**: `pipe()`, `compose()`, `safe_execute()`
- **Logging decorado**: `@log_step()`, `@secure_log`
- **Manejo de errores**: Valores por defecto, no crashes
- **Documentación inline**: Docstrings en todas las funciones

## 📞 Soporte

Para problemas o preguntas:

1. **Revisa los logs**: El sistema imprime información detallada
2. **Usa el script de prueba**: `test_rpc_contratos.py` diagnostica problemas
3. **Consulta la documentación**: Este README y los docstrings

## 🎉 Próximos Pasos

Después de configurar el módulo:

1. ✅ Ejecuta `test_rpc_contratos.py` para verificar
2. ✅ Procesa los PDFs de ejemplo en `context/`
3. ✅ Revisa los datos en Firebase Console
4. ✅ Integra con tu aplicación frontend
5. ✅ Automatiza el proceso con GitHub Actions (opcional)

---

**¡El sistema está listo para procesar contratos RPC con IA! 🚀**
