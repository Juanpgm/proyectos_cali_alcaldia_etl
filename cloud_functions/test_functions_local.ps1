#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Script de prueba local para Cloud Functions
    
.DESCRIPTION
    Simula la ejecución local de las Cloud Functions sin desplegar a GCP.
    Útil para validar lógica antes del deploy.
    
.EXAMPLE
    .\test_functions_local.ps1
#>

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   TEST LOCAL - CLOUD FUNCTIONS" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Verificar entorno virtual Python
$venvPath = Join-Path $PSScriptRoot "..\env"
if (-not (Test-Path $venvPath)) {
    Write-Host "❌ Error: Entorno virtual Python no encontrado" -ForegroundColor Red
    Write-Host "Ejecuta: python -m venv env" -ForegroundColor Yellow
    exit 1
}

# Activar entorno virtual
Write-Host "[1/4] Activando entorno virtual..." -ForegroundColor Yellow
$activateScript = Join-Path $venvPath "Scripts\Activate.ps1"
& $activateScript
Write-Host "  ✓ Entorno activado" -ForegroundColor Green

# Instalar dependencias de Cloud Functions
Write-Host "`n[2/4] Instalando dependencias..." -ForegroundColor Yellow
$requirementsFile = Join-Path $PSScriptRoot "requirements.txt"
pip install -q -r $requirementsFile
Write-Host "  ✓ Dependencias instaladas" -ForegroundColor Green

# Configurar variables de entorno
Write-Host "`n[3/4] Configurando variables de entorno..." -ForegroundColor Yellow
$env:S3_BUCKET_NAME = "unidades-proyecto-documents"
$env:TESTING_MODE = "true"
$env:USE_LOCAL_CREDENTIALS = "true"
Write-Host "  ✓ Variables configuradas" -ForegroundColor Green

# Ejecutar test
Write-Host "`n[4/4] Ejecutando test local..." -ForegroundColor Yellow
Write-Host "="*50 -ForegroundColor Cyan

$testScript = @"
import sys
import os
from pathlib import Path

# Agregar path de cloud_functions
sys.path.insert(0, r'$PSScriptRoot')

# Mock Flask request para testing local
class MockRequest:
    def get_json(self, silent=False):
        return {'force_reload': False}
    
    @property
    def args(self):
        return {}

try:
    from utils import S3Handler, FirestoreHandler, DataTransformer
    print('✓ Módulos importados correctamente')
    
    # Test S3Handler
    print('\n📦 Testeando S3Handler...')
    s3 = S3Handler('unidades-proyecto-documents')
    print(f'  ✓ S3Handler inicializado para bucket: {s3.bucket_name}')
    
    # Test FirestoreHandler
    print('\n🔥 Testeando FirestoreHandler...')
    try:
        firestore = FirestoreHandler()
        print('  ✓ FirestoreHandler inicializado')
    except Exception as e:
        print(f'  ⚠ FirestoreHandler requiere credenciales GCP: {e}')
    
    # Test DataTransformer
    print('\n🔄 Testeando DataTransformer...')
    transformer = DataTransformer()
    
    # Datos de prueba
    test_geojson = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'properties': {
                    'upid': 'TEST-001',
                    'nombre_proyecto': 'Proyecto Test',
                    'comuna_corregimiento': 'Comuna 1',
                    'comuna_corregimiento_2': 'Comuna 2',
                    'fuera_rango': 'ACEPTABLE',
                    'barrio_vereda': 'Barrio A',
                    'barrio_vereda_2': 'Barrio B',
                    'fecha_inicio_std': '2024-01-01',
                    'fecha_fin_std': '2024-12-31'
                },
                'geometry': {
                    'type': 'Point',
                    'coordinates': [-76.5225, 3.4516]
                }
            }
        ]
    }
    
    docs = transformer.transform_unidades_proyecto(test_geojson)
    print(f'  ✓ Transformados {len(docs)} documentos')
    
    # Verificar transformaciones
    test_doc = docs['TEST-001']
    assert test_doc['comuna_corregimiento'] == 'Comuna 2', 'Error: comuna_corregimiento_2 no aplicado'
    assert test_doc['barrio_vereda'] == 'Barrio B', 'Error: barrio_vereda_2 no aplicado'
    assert test_doc['fecha_inicio'] == '2024-01-01', 'Error: fecha_inicio_std no renombrado'
    assert 'fecha_inicio_std' not in test_doc, 'Error: fecha_inicio_std no eliminado'
    print('  ✓ Mapeo de campos validado correctamente')
    
    print('\n' + '='*50)
    print('✅ TESTS LOCALES EXITOSOS')
    print('='*50)
    print('\n📋 Próximos pasos:')
    print('  1. Ejecutar setup_cloud_functions.ps1 para deploy')
    print('  2. Probar función desplegada con trigger manual')
    
except Exception as e:
    print(f'\n❌ Error en tests: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"@

# Guardar y ejecutar script de test
$testFile = Join-Path $PSScriptRoot "test_local.py"
$testScript | Out-File -FilePath $testFile -Encoding UTF8

python $testFile

# Limpiar
Remove-Item $testFile -ErrorAction SilentlyContinue

Write-Host ""
