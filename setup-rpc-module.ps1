# Setup Script for RPC Contratos Module
# Run with: .\setup-rpc-module.ps1

Write-Host "================================" -ForegroundColor Cyan
Write-Host "🚀 RPC CONTRATOS MODULE SETUP" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# Check Python
Write-Host "1️⃣ Verificando Python..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "   ✅ $pythonVersion" -ForegroundColor Green
}
catch {
    Write-Host "   ❌ Python no encontrado" -ForegroundColor Red
    Write-Host "   💡 Instala Python 3.10+ desde https://python.org" -ForegroundColor Yellow
    exit 1
}

# Install Python dependencies
Write-Host ""
Write-Host "2️⃣ Instalando dependencias Python..." -ForegroundColor Yellow
try {
    pip install -r requirements.txt --quiet
    Write-Host "   ✅ Dependencias instaladas" -ForegroundColor Green
}
catch {
    Write-Host "   ❌ Error instalando dependencias" -ForegroundColor Red
    Write-Host "   💡 Ejecuta manualmente: pip install -r requirements.txt" -ForegroundColor Yellow
    exit 1
}

# Check Tesseract
Write-Host ""
Write-Host "3️⃣ Verificando Tesseract OCR..." -ForegroundColor Yellow
try {
    $tesseractVersion = tesseract --version 2>&1 | Select-String "tesseract"
    Write-Host "   ✅ $tesseractVersion" -ForegroundColor Green
}
catch {
    Write-Host "   ❌ Tesseract no encontrado" -ForegroundColor Red
    Write-Host "   💡 Instala Tesseract:" -ForegroundColor Yellow
    Write-Host "      - Con Chocolatey: choco install tesseract" -ForegroundColor Gray
    Write-Host "      - O descarga: https://github.com/UB-Mannheim/tesseract/wiki" -ForegroundColor Gray
    
    $continue = Read-Host "   ¿Continuar sin Tesseract? (s/n)"
    if ($continue -ne "s") {
        exit 1
    }
}

# Check/Configure Gemini API Key
Write-Host ""
Write-Host "4️⃣ Configurando Gemini API Key..." -ForegroundColor Yellow
$existingKey = $env:GEMINI_API_KEY

if ($existingKey) {
    Write-Host "   ✅ API Key ya configurada (longitud: $($existingKey.Length))" -ForegroundColor Green
    $reconfigure = Read-Host "   ¿Reconfigurar? (s/n)"
    
    if ($reconfigure -eq "s") {
        $existingKey = $null
    }
}

if (-not $existingKey) {
    Write-Host ""
    Write-Host "   💡 Obtén tu API key en: https://makersuite.google.com/app/apikey" -ForegroundColor Cyan
    Write-Host ""
    $apiKey = Read-Host "   Ingresa tu Gemini API Key"
    
    if ($apiKey) {
        # Set for current session
        $env:GEMINI_API_KEY = $apiKey
        
        # Add to .env.local
        $envLocalPath = ".env.local"
        
        if (Test-Path $envLocalPath) {
            # Update existing
            $content = Get-Content $envLocalPath
            $content = $content | Where-Object { $_ -notmatch "^GEMINI_API_KEY=" }
            $content += "GEMINI_API_KEY=$apiKey"
            $content | Set-Content $envLocalPath
        }
        else {
            # Create new
            "# RPC Contratos Module Configuration`nGEMINI_API_KEY=$apiKey" | Set-Content $envLocalPath
        }
        
        Write-Host "   ✅ API Key configurada y guardada en .env.local" -ForegroundColor Green
    }
    else {
        Write-Host "   ⚠️ No se configuró API Key" -ForegroundColor Yellow
        Write-Host "   💡 Configura manualmente: `$env:GEMINI_API_KEY = 'tu_key'" -ForegroundColor Gray
    }
}

# Check Firebase
Write-Host ""
Write-Host "5️⃣ Verificando Firebase..." -ForegroundColor Yellow
try {
    $firebaseTest = python -c "from database.config import get_firestore_client; db = get_firestore_client(); print('OK' if db else 'FAIL')" 2>&1
    
    if ($firebaseTest -match "OK") {
        Write-Host "   ✅ Conexión a Firebase exitosa" -ForegroundColor Green
    }
    else {
        Write-Host "   ❌ No se pudo conectar a Firebase" -ForegroundColor Red
        Write-Host "   💡 Ejecuta: gcloud auth application-default login" -ForegroundColor Yellow
    }
}
catch {
    Write-Host "   ⚠️ No se pudo verificar Firebase" -ForegroundColor Yellow
}

# Check context directory
Write-Host ""
Write-Host "6️⃣ Verificando PDFs de prueba..." -ForegroundColor Yellow
$contextPath = "context"

if (Test-Path $contextPath) {
    $rpcFiles = Get-ChildItem -Path $contextPath -Filter "RPC*.pdf"
    
    if ($rpcFiles.Count -gt 0) {
        Write-Host "   ✅ Encontrados $($rpcFiles.Count) PDFs RPC:" -ForegroundColor Green
        foreach ($file in $rpcFiles) {
            Write-Host "      - $($file.Name)" -ForegroundColor Gray
        }
    }
    else {
        Write-Host "   ⚠️ No se encontraron PDFs RPC en context/" -ForegroundColor Yellow
        Write-Host "   💡 Copia tus PDFs RPC a la carpeta context/" -ForegroundColor Gray
    }
}
else {
    Write-Host "   ⚠️ Carpeta context/ no encontrada" -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $contextPath -Force | Out-Null
    Write-Host "   ✅ Carpeta context/ creada" -ForegroundColor Green
}

# Summary
Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host "📊 RESUMEN DE CONFIGURACIÓN" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

$allGood = $true

Write-Host "✅ Python" -ForegroundColor Green
Write-Host "✅ Dependencias" -ForegroundColor Green

if (Get-Command tesseract -ErrorAction SilentlyContinue) {
    Write-Host "✅ Tesseract OCR" -ForegroundColor Green
}
else {
    Write-Host "⚠️ Tesseract OCR (opcional para OCR)" -ForegroundColor Yellow
}

if ($env:GEMINI_API_KEY) {
    Write-Host "✅ Gemini API Key" -ForegroundColor Green
}
else {
    Write-Host "❌ Gemini API Key (requerido)" -ForegroundColor Red
    $allGood = $false
}

# Next steps
Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host "🎯 PRÓXIMOS PASOS" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

if ($allGood) {
    Write-Host "✅ ¡Configuración completa!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Puedes ejecutar:" -ForegroundColor Cyan
    Write-Host "  1. python test_rpc_contratos.py          # Prueba interactiva" -ForegroundColor White
    Write-Host "  2. python pipelines\rpc_contratos_emprestito_pipeline.py context\  # Pipeline completo" -ForegroundColor White
    Write-Host ""
    Write-Host "📖 Documentación: docs\RPC_CONTRATOS_README.md" -ForegroundColor Gray
}
else {
    Write-Host "⚠️ Configuración incompleta" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Pendientes:" -ForegroundColor Yellow
    
    if (-not $env:GEMINI_API_KEY) {
        Write-Host "  - Configurar GEMINI_API_KEY" -ForegroundColor White
        Write-Host "    https://makersuite.google.com/app/apikey" -ForegroundColor Gray
    }
    
    Write-Host ""
    Write-Host "📖 Ver guía completa: docs\RPC_CONTRATOS_README.md" -ForegroundColor Gray
}

Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
