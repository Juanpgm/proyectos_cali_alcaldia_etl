<#
.SYNOPSIS
    Configura múltiples cuentas de gcloud para migración entre proyectos Firebase

.DESCRIPTION
    Este script facilita la configuración de diferentes perfiles de gcloud
    para manejar proyectos Firebase en diferentes cuentas.

.PARAMETER Account
    La cuenta a configurar: source o target

.EXAMPLE
    .\setup-firebase-accounts.ps1 -Account source
    .\setup-firebase-accounts.ps1 -Account target

.NOTES
    Autor: ETL Pipeline Cali
    Versión: 1.0
#>

param(
    [Parameter(Mandatory = $false)]
    [ValidateSet('source', 'target', 'both')]
    [string]$Account = 'both'
)

function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = 'White'
    )
    Write-Host $Message -ForegroundColor $Color
}

function Setup-SourceAccount {
    Write-ColorOutput "`n╔════════════════════════════════════════════════════════╗" -Color Cyan
    Write-ColorOutput "║     Configurando Cuenta ORIGEN (source-account)       ║" -Color Cyan
    Write-ColorOutput "╚════════════════════════════════════════════════════════╝" -Color Cyan
    
    Write-ColorOutput "`n📋 Proyecto: unidad-cumplimiento-aa245" -Color Yellow
    Write-ColorOutput "Esta es la cuenta que contiene los datos originales" -Color Gray
    
    # Verificar si la configuración ya existe
    $existingConfigs = gcloud config configurations list --format="value(name)" 2>$null
    if ($existingConfigs -match "source-account") {
        Write-ColorOutput "`n⚠️  La configuración 'source-account' ya existe" -Color Yellow
        $recreate = Read-Host "¿Recrear? (s/n)"
        if ($recreate -eq 's') {
            gcloud config configurations delete source-account --quiet
            Write-ColorOutput "✅ Configuración anterior eliminada" -Color Green
        }
        else {
            Write-ColorOutput "📝 Activando configuración existente..." -Color Cyan
            gcloud config configurations activate source-account
            Write-ColorOutput "✅ Configuración 'source-account' activada" -Color Green
            return
        }
    }
    
    # Crear nueva configuración
    Write-ColorOutput "`n🔧 Creando configuración 'source-account'..." -Color Cyan
    gcloud config configurations create source-account
    
    # Configurar proyecto
    Write-ColorOutput "`n📦 Configurando proyecto..." -Color Cyan
    gcloud config set project unidad-cumplimiento-aa245
    
    # Autenticación
    Write-ColorOutput "`n🔐 Iniciando autenticación..." -Color Yellow
    Write-ColorOutput "Se abrirá tu navegador para autenticar con tu cuenta de Google" -Color Gray
    Write-ColorOutput "Selecciona la cuenta que tiene acceso a 'unidad-cumplimiento-aa245'" -Color Gray
    
    $authChoice = Read-Host "`n¿Proceder con autenticación? (s/n)"
    if ($authChoice -eq 's') {
        gcloud auth login
        Write-ColorOutput "`n✅ Autenticación de usuario completada" -Color Green
        
        Write-ColorOutput "`n🔑 Configurando Application Default Credentials..." -Color Cyan
        gcloud auth application-default login
        Write-ColorOutput "✅ ADC configurado" -Color Green
    }
    else {
        Write-ColorOutput "⏭️  Autenticación omitida" -Color Yellow
    }
    
    Write-ColorOutput "`n✅ Configuración 'source-account' completada" -Color Green
    Write-ColorOutput "📝 Para usar: gcloud config configurations activate source-account" -Color Cyan
}

function Setup-TargetAccount {
    Write-ColorOutput "`n╔════════════════════════════════════════════════════════╗" -Color Cyan
    Write-ColorOutput "║     Configurando Cuenta DESTINO (target-account)      ║" -Color Cyan
    Write-ColorOutput "╚════════════════════════════════════════════════════════╝" -Color Cyan
    
    Write-ColorOutput "`n📋 Proyecto: calitrack-44403" -Color Yellow
    Write-ColorOutput "👤 Cuenta: juanp.gzmz@gmail.com" -Color Yellow
    Write-ColorOutput "Este es el proyecto de desarrollo donde se copiarán los datos" -Color Gray
    
    # Verificar si la configuración ya existe
    $existingConfigs = gcloud config configurations list --format="value(name)" 2>$null
    if ($existingConfigs -match "target-account") {
        Write-ColorOutput "`n⚠️  La configuración 'target-account' ya existe" -Color Yellow
        $recreate = Read-Host "¿Recrear? (s/n)"
        if ($recreate -eq 's') {
            gcloud config configurations delete target-account --quiet
            Write-ColorOutput "✅ Configuración anterior eliminada" -Color Green
        }
        else {
            Write-ColorOutput "📝 Activando configuración existente..." -Color Cyan
            gcloud config configurations activate target-account
            Write-ColorOutput "✅ Configuración 'target-account' activada" -Color Green
            return
        }
    }
    
    # Crear nueva configuración
    Write-ColorOutput "`n🔧 Creando configuración 'target-account'..." -Color Cyan
    gcloud config configurations create target-account
    
    # Configurar proyecto
    Write-ColorOutput "`n📦 Configurando proyecto..." -Color Cyan
    gcloud config set project calitrack-44403
    
    # Autenticación
    Write-ColorOutput "`n🔐 Iniciando autenticación..." -Color Yellow
    Write-ColorOutput "⚠️  IMPORTANTE: Debes autenticarte con: juanp.gzmz@gmail.com" -Color Red
    Write-ColorOutput "Se abrirá tu navegador - ASEGÚRATE de seleccionar juanp.gzmz@gmail.com" -Color Yellow
    
    $authChoice = Read-Host "`n¿Proceder con autenticación? (s/n)"
    if ($authChoice -eq 's') {
        gcloud auth login --force
        Write-ColorOutput "`n✅ Autenticación de usuario completada" -Color Green
        
        Write-ColorOutput "`n🔑 Configurando Application Default Credentials..." -Color Cyan
        Write-ColorOutput "⚠️  Nuevamente, selecciona juanp.gzmz@gmail.com en el navegador" -Color Yellow
        gcloud auth application-default login --no-launch-browser
        Write-ColorOutput "✅ ADC configurado" -Color Green
    }
    else {
        Write-ColorOutput "⏭️  Autenticación omitida" -Color Yellow
    }
    
    Write-ColorOutput "`n✅ Configuración 'target-account' completada" -Color Green
    Write-ColorOutput "📝 Para usar: gcloud config configurations activate target-account" -Color Cyan
}

# Banner principal
Write-ColorOutput "`n╔════════════════════════════════════════════════════════╗" -Color Magenta
Write-ColorOutput "║   Configurador de Cuentas Firebase - Migración ETL    ║" -Color Magenta
Write-ColorOutput "╚════════════════════════════════════════════════════════╝" -Color Magenta

Write-ColorOutput "`n📚 Este script te ayudará a configurar perfiles de gcloud" -Color White
Write-ColorOutput "para manejar proyectos Firebase en diferentes cuentas" -Color Gray

# Verificar gcloud
Write-ColorOutput "`n🔍 Verificando instalación de gcloud..." -Color Cyan
$gcloudVersion = gcloud version --format="value(version)" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-ColorOutput "❌ gcloud CLI no está instalado" -Color Red
    Write-ColorOutput "💡 Instala desde: https://cloud.google.com/sdk/docs/install" -Color Yellow
    exit 1
}
Write-ColorOutput "✅ gcloud CLI instalado (versión: $gcloudVersion)" -Color Green

# Ejecutar configuración según el parámetro
switch ($Account) {
    'source' {
        Setup-SourceAccount
    }
    'target' {
        Setup-TargetAccount
    }
    'both' {
        Setup-SourceAccount
        Write-ColorOutput "`n" + ("=" * 60) -Color Gray
        Setup-TargetAccount
    }
}

# Resumen final
Write-ColorOutput "`n" + ("=" * 60) -Color Magenta
Write-ColorOutput "✨ CONFIGURACIÓN COMPLETADA" -Color Magenta
Write-ColorOutput ("=" * 60) -Color Magenta

Write-ColorOutput "`n📋 Configuraciones disponibles:" -Color Cyan
gcloud config configurations list

Write-ColorOutput "`n💡 Uso de configuraciones:" -Color Yellow
Write-ColorOutput "   Activar origen:  gcloud config configurations activate source-account" -Color White
Write-ColorOutput "   Activar destino: gcloud config configurations activate target-account" -Color White
Write-ColorOutput "   Ver actual:      gcloud config configurations list" -Color White

Write-ColorOutput "`n🚀 Próximos pasos:" -Color Cyan
Write-ColorOutput "1. Verifica que ambas configuraciones estén activas y funcionando" -Color White
Write-ColorOutput "2. Para la migración, el script cambiará automáticamente entre cuentas" -Color White
Write-ColorOutput "3. Ejecuta la migración:" -Color White
Write-ColorOutput "   python migrate_firestore.py --dry-run  # Preview primero" -Color Gray
Write-ColorOutput "   python migrate_firestore.py            # Migración real" -Color Gray

Write-ColorOutput "`n✅ Todo listo para la migración`n" -Color Green
