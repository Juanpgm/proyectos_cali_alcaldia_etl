<#
.SYNOPSIS
    Configura variables de entorno sensibles para el proyecto ETL Cali

.DESCRIPTION
    Este script ayuda a configurar las variables de entorno sensibles de manera segura.
    Ofrece dos opciones:
    1. Variables de entorno del sistema (permanentes)
    2. Variables de sesión (temporales, solo para la sesión actual)
    3. Archivo .env.local (local, no commiteado)

.PARAMETER Method
    Método de configuración: System, Session, o File
    Default: Session

.PARAMETER SheetsUrl
    URL del Google Sheet de Unidades de Proyecto

.PARAMETER GitHubToken
    Token de GitHub (opcional)

.EXAMPLE
    .\setup-env-vars.ps1 -Method Session -SheetsUrl "https://docs.google.com/..."
    .\setup-env-vars.ps1 -Method System
    .\setup-env-vars.ps1 -Method File

.NOTES
    Autor: ETL Pipeline Cali
    Versión: 1.0
#>

param(
    [Parameter(Mandatory = $false)]
    [ValidateSet('System', 'Session', 'File')]
    [string]$Method = 'Session',
    
    [Parameter(Mandatory = $false)]
    [string]$SheetsUrl = '',
    
    [Parameter(Mandatory = $false)]
    [string]$GitHubToken = ''
)

# Función para escribir con colores
function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = 'White'
    )
    Write-Host $Message -ForegroundColor $Color
}

# Banner
Write-ColorOutput "`n╔════════════════════════════════════════════════════════╗" -Color Cyan
Write-ColorOutput "║     Configuración de Variables de Entorno - ETL Cali  ║" -Color Cyan
Write-ColorOutput "╚════════════════════════════════════════════════════════╝`n" -Color Cyan

# Solicitar URL del Google Sheet si no se proporcionó
if ([string]::IsNullOrWhiteSpace($SheetsUrl)) {
    Write-ColorOutput "📊 Google Sheets Configuration" -Color Yellow
    Write-ColorOutput "Ingresa la URL completa del Google Sheet de Unidades de Proyecto:" -Color White
    Write-ColorOutput "(Ejemplo: https://docs.google.com/spreadsheets/d/1ABC.../edit)" -Color Gray
    $SheetsUrl = Read-Host "URL"
    
    if ([string]::IsNullOrWhiteSpace($SheetsUrl)) {
        Write-ColorOutput "❌ Error: URL del Google Sheet es requerida" -Color Red
        exit 1
    }
}

# Solicitar GitHub Token si no se proporcionó (opcional)
if ([string]::IsNullOrWhiteSpace($GitHubToken)) {
    Write-ColorOutput "`n🔑 GitHub Token (Opcional)" -Color Yellow
    Write-ColorOutput "Ingresa tu GitHub Token (presiona Enter para omitir):" -Color White
    Write-ColorOutput "(Solo necesario para ejecutar workflows desde scripts)" -Color Gray
    $GitHubToken = Read-Host "Token"
}

# Configurar según el método seleccionado
Write-ColorOutput "`n🔧 Método de configuración: $Method" -Color Cyan

switch ($Method) {
    'System' {
        Write-ColorOutput "`n⚠️  IMPORTANTE: Esto modificará las variables de entorno del SISTEMA" -Color Yellow
        Write-ColorOutput "Las variables persistirán después de cerrar esta ventana y reiniciar" -Color Yellow
        $confirm = Read-Host "`n¿Continuar? (s/n)"
        
        if ($confirm -ne 's') {
            Write-ColorOutput "Operación cancelada" -Color Red
            exit 0
        }
        
        # Configurar variables de sistema
        [System.Environment]::SetEnvironmentVariable('SHEETS_UNIDADES_PROYECTO_URL', $SheetsUrl, [System.EnvironmentVariableTarget]::User)
        Write-ColorOutput "✅ SHEETS_UNIDADES_PROYECTO_URL configurada (Sistema)" -Color Green
        
        if (-not [string]::IsNullOrWhiteSpace($GitHubToken)) {
            [System.Environment]::SetEnvironmentVariable('GITHUB_TOKEN', $GitHubToken, [System.EnvironmentVariableTarget]::User)
            Write-ColorOutput "✅ GITHUB_TOKEN configurado (Sistema)" -Color Green
        }
        
        Write-ColorOutput "`n💡 Variables configuradas permanentemente" -Color Cyan
        Write-ColorOutput "Cierra y reabre tu terminal para que surtan efecto" -Color Cyan
    }
    
    'Session' {
        # Configurar variables de sesión
        $env:SHEETS_UNIDADES_PROYECTO_URL = $SheetsUrl
        Write-ColorOutput "✅ SHEETS_UNIDADES_PROYECTO_URL configurada (Sesión)" -Color Green
        
        if (-not [string]::IsNullOrWhiteSpace($GitHubToken)) {
            $env:GITHUB_TOKEN = $GitHubToken
            Write-ColorOutput "✅ GITHUB_TOKEN configurado (Sesión)" -Color Green
        }
        
        Write-ColorOutput "`n💡 Variables configuradas para esta sesión" -Color Cyan
        Write-ColorOutput "Deberás reconfigurarlas si cierras esta terminal" -Color Yellow
        Write-ColorOutput "`nPara hacerlas permanentes, ejecuta:" -Color Cyan
        Write-ColorOutput ".\setup-env-vars.ps1 -Method System -SheetsUrl '$SheetsUrl'" -Color White
    }
    
    'File' {
        # Crear archivo .env.local
        $envLocalPath = Join-Path $PSScriptRoot '.env.local'
        
        if (Test-Path $envLocalPath) {
            Write-ColorOutput "`n⚠️  El archivo .env.local ya existe" -Color Yellow
            $confirm = Read-Host "¿Sobrescribir? (s/n)"
            
            if ($confirm -ne 's') {
                Write-ColorOutput "Operación cancelada" -Color Red
                exit 0
            }
        }
        
        # Crear contenido del archivo
        $content = @"
# .env.local - Variables sensibles locales
# ==========================================
# Este archivo NO debe commitearse a Git
# Generado automáticamente por setup-env-vars.ps1

# Google Sheets Configuration
SHEETS_UNIDADES_PROYECTO_URL=$SheetsUrl
"@
        
        if (-not [string]::IsNullOrWhiteSpace($GitHubToken)) {
            $content += "`n`n# GitHub Token (opcional)`nGITHUB_TOKEN=$GitHubToken"
        }
        
        # Escribir archivo
        $content | Out-File -FilePath $envLocalPath -Encoding utf8 -Force
        Write-ColorOutput "✅ Archivo .env.local creado exitosamente" -Color Green
        Write-ColorOutput "📁 Ubicación: $envLocalPath" -Color Cyan
        
        Write-ColorOutput "`n💡 Variables configuradas en archivo local" -Color Cyan
        Write-ColorOutput "El archivo .env.local está protegido por .gitignore" -Color Green
    }
}

# Verificar configuración
Write-ColorOutput "`n🔍 Verificando configuración..." -Color Cyan

$currentSheetsUrl = $env:SHEETS_UNIDADES_PROYECTO_URL
if ($Method -eq 'File') {
    Write-ColorOutput "📄 Variables guardadas en .env.local" -Color Green
    Write-ColorOutput "Se cargarán automáticamente cuando ejecutes el proyecto" -Color Cyan
}
elseif ([string]::IsNullOrWhiteSpace($currentSheetsUrl)) {
    Write-ColorOutput "⚠️  No se pudo verificar SHEETS_UNIDADES_PROYECTO_URL" -Color Yellow
    Write-ColorOutput "Si usaste 'System', cierra y reabre la terminal" -Color Yellow
}
else {
    Write-ColorOutput "✅ SHEETS_UNIDADES_PROYECTO_URL está configurada" -Color Green
    Write-ColorOutput "   Valor: $($currentSheetsUrl.Substring(0, [Math]::Min(50, $currentSheetsUrl.Length)))..." -Color Gray
}

# Instrucciones finales
Write-ColorOutput "`n📚 Próximos pasos:" -Color Cyan
Write-ColorOutput "1. Configura ADC para Firebase/Sheets:" -Color White
Write-ColorOutput "   .\setup-adc.ps1 -Environment dev" -Color Gray
Write-ColorOutput "2. Ejecuta tu pipeline ETL normalmente" -Color White
Write-ColorOutput "3. Las variables sensibles se cargarán automáticamente" -Color White

Write-ColorOutput "`n✨ Configuración completada exitosamente`n" -Color Green
