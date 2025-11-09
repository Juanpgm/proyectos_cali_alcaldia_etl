#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Script de configuración de Workload Identity Federation (ADC) para el proyecto

.DESCRIPTION
    Este script ayuda a configurar Application Default Credentials (ADC) para 
    Firebase y Google Sheets sin necesidad de archivos de credenciales estáticas.

.PARAMETER Environment
    Entorno a configurar: 'dev' o 'prod'

.EXAMPLE
    .\setup-adc.ps1 -Environment dev
    .\setup-adc.ps1 -Environment prod
#>

param(
    [Parameter(Mandatory = $false)]
    [ValidateSet('dev', 'prod')]
    [string]$Environment = 'dev'
)

# Colores para output
function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = 'White'
    )
    Write-Host $Message -ForegroundColor $Color
}

Write-ColorOutput "`n🔐 Configuración de Workload Identity Federation (ADC)`n" -Color Cyan
Write-ColorOutput "=" * 60 -Color Gray

# Determinar proyecto según entorno
$projectId = if ($Environment -eq 'dev') { 
    'calitrack-44403' 
}
else { 
    'dev-test-e778d' 
}

Write-ColorOutput "`n📊 Configuración:" -Color Yellow
Write-ColorOutput "   Entorno: $Environment" -Color White
Write-ColorOutput "   Proyecto: $projectId" -Color White
Write-ColorOutput ""

# Verificar si gcloud está instalado
Write-ColorOutput "🔍 Verificando Google Cloud CLI..." -Color Yellow
try {
    $gcloudVersion = & gcloud --version 2>&1 | Select-Object -First 1
    Write-ColorOutput "   ✅ $gcloudVersion" -Color Green
}
catch {
    Write-ColorOutput "   ❌ Google Cloud CLI no está instalado" -Color Red
    Write-ColorOutput "`n📥 Instala gcloud CLI:" -Color Yellow
    Write-ColorOutput "   Windows: winget install Google.CloudSDK" -Color White
    Write-ColorOutput "   O visita: https://cloud.google.com/sdk/docs/install" -Color White
    exit 1
}

# Configurar proyecto
Write-ColorOutput "`n⚙️  Configurando proyecto..." -Color Yellow
try {
    & gcloud config set project $projectId 2>&1 | Out-Null
    Write-ColorOutput "   ✅ Proyecto configurado: $projectId" -Color Green
}
catch {
    Write-ColorOutput "   ❌ Error configurando proyecto" -Color Red
    Write-ColorOutput "   $_" -Color Red
}

# Verificar proyecto actual
$currentProject = & gcloud config get-value project 2>$null
Write-ColorOutput "   📊 Proyecto activo: $currentProject" -Color Cyan

# Configurar Application Default Credentials
Write-ColorOutput "`n🔑 Configurando Application Default Credentials..." -Color Yellow
Write-ColorOutput "   Se abrirá tu navegador para autenticación..." -Color Gray
Write-ColorOutput ""

try {
    & gcloud auth application-default login --project=$projectId
    
    if ($LASTEXITCODE -eq 0) {
        Write-ColorOutput "`n✅ ADC configurado correctamente!" -Color Green
        
        # Verificar credenciales
        Write-ColorOutput "`n🔍 Verificando credenciales..." -Color Yellow
        $credPath = "$env:APPDATA\gcloud\application_default_credentials.json"
        if (Test-Path $credPath) {
            Write-ColorOutput "   ✅ Archivo de credenciales creado" -Color Green
            Write-ColorOutput "   📁 Ubicación: $credPath" -Color Gray
        }
        
        # Mostrar próximos pasos
        Write-ColorOutput "`n" + "=" * 60 -Color Gray
        Write-ColorOutput "🎯 Próximos pasos:" -Color Cyan
        Write-ColorOutput "`n1. Cambia a la rama correspondiente:" -Color Yellow
        if ($Environment -eq 'dev') {
            Write-ColorOutput "   git checkout dev" -Color White
        }
        else {
            Write-ColorOutput "   git checkout main" -Color White
        }
        
        Write-ColorOutput "`n2. El sistema usará automáticamente ADC" -Color Yellow
        Write-ColorOutput "   - Firebase: $projectId" -Color White
        Write-ColorOutput "   - Google Sheets: Autenticación automática" -Color White
        
        Write-ColorOutput "`n3. Ejecuta tus pipelines:" -Color Yellow
        Write-ColorOutput "   python pipelines/unidades_proyecto_pipeline.py" -Color White
        
        Write-ColorOutput "`n✨ Beneficios de ADC:" -Color Cyan
        Write-ColorOutput "   ✅ Sin archivos de credenciales estáticas" -Color Green
        Write-ColorOutput "   ✅ Rotación automática de tokens" -Color Green
        Write-ColorOutput "   ✅ Mayor seguridad" -Color Green
        Write-ColorOutput "   ✅ Auditoría completa" -Color Green
        
        Write-ColorOutput "`n" + "=" * 60 -Color Gray
        
    }
    else {
        Write-ColorOutput "`n❌ Error configurando ADC" -Color Red
        exit 1
    }
    
}
catch {
    Write-ColorOutput "`n❌ Error durante la configuración: $_" -Color Red
    exit 1
}

# Habilitar APIs necesarias (opcional)
Write-ColorOutput "`n🔧 ¿Deseas habilitar las APIs necesarias? (S/N): " -Color Yellow -NoNewline
$response = Read-Host

if ($response -match '^[Ss]$') {
    Write-ColorOutput "`n📡 Habilitando APIs..." -Color Yellow
    
    $apis = @(
        'firebase.googleapis.com',
        'firestore.googleapis.com',
        'sheets.googleapis.com',
        'drive.googleapis.com'
    )
    
    foreach ($api in $apis) {
        Write-ColorOutput "   Habilitando $api..." -Color Gray
        try {
            & gcloud services enable $api --project=$projectId 2>&1 | Out-Null
            Write-ColorOutput "   ✅ $api habilitada" -Color Green
        }
        catch {
            Write-ColorOutput "   ⚠️  Error habilitando $api" -Color Yellow
        }
    }
}

Write-ColorOutput "`n🎉 ¡Configuración completada!" -Color Green
Write-ColorOutput ""
