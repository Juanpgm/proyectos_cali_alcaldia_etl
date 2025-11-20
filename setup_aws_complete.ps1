# Script PowerShell para instalar AWS CLI y configurar credenciales automáticamente
# Autor: AI Assistant
# Fecha: 2025-11-16

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "INSTALACIÓN Y CONFIGURACIÓN DE AWS CLI" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# Función para verificar si AWS CLI está instalado
function Test-AWSCLIInstalled {
    try {
        $null = aws --version 2>&1
        return $true
    } catch {
        return $false
    }
}

# Paso 1: Verificar si AWS CLI está instalado
Write-Host "📋 Verificando instalación de AWS CLI..." -ForegroundColor Yellow
if (Test-AWSCLIInstalled) {
    Write-Host "✅ AWS CLI ya está instalado" -ForegroundColor Green
    aws --version
} else {
    Write-Host "⚠️  AWS CLI no está instalado" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Opciones de instalación:" -ForegroundColor Cyan
    Write-Host "1. Instalar automáticamente usando winget (Windows Package Manager)"
    Write-Host "2. Descargar instalador MSI manualmente"
    Write-Host "3. Instalar usando Chocolatey (si está instalado)"
    Write-Host "4. Salir y configurar manualmente"
    Write-Host ""
    
    $choice = Read-Host "Selecciona una opción (1-4)"
    
    switch ($choice) {
        "1" {
            Write-Host ""
            Write-Host "📦 Instalando AWS CLI con winget..." -ForegroundColor Yellow
            try {
                winget install Amazon.AWSCLI --silent
                Write-Host "✅ AWS CLI instalado correctamente" -ForegroundColor Green
                Write-Host "⚠️  IMPORTANTE: Cierra y vuelve a abrir PowerShell para usar AWS CLI" -ForegroundColor Yellow
                Write-Host ""
                Write-Host "Después de reiniciar PowerShell, ejecuta nuevamente:" -ForegroundColor Cyan
                Write-Host "  .\setup_aws_complete.ps1" -ForegroundColor White
                exit
            } catch {
                Write-Host "❌ Error al instalar con winget: $_" -ForegroundColor Red
                Write-Host "Prueba otra opción de instalación" -ForegroundColor Yellow
                exit 1
            }
        }
        "2" {
            Write-Host ""
            Write-Host "📥 Abriendo página de descarga de AWS CLI..." -ForegroundColor Yellow
            Start-Process "https://awscli.amazonaws.com/AWSCLIV2.msi"
            Write-Host ""
            Write-Host "Pasos siguientes:" -ForegroundColor Cyan
            Write-Host "1. Instala el archivo MSI descargado"
            Write-Host "2. Cierra y vuelve a abrir PowerShell"
            Write-Host "3. Ejecuta nuevamente: .\setup_aws_complete.ps1" -ForegroundColor White
            exit
        }
        "3" {
            Write-Host ""
            Write-Host "📦 Instalando AWS CLI con Chocolatey..." -ForegroundColor Yellow
            try {
                choco install awscli -y
                Write-Host "✅ AWS CLI instalado correctamente" -ForegroundColor Green
                Write-Host "⚠️  IMPORTANTE: Cierra y vuelve a abrir PowerShell para usar AWS CLI" -ForegroundColor Yellow
                Write-Host ""
                Write-Host "Después de reiniciar PowerShell, ejecuta nuevamente:" -ForegroundColor Cyan
                Write-Host "  .\setup_aws_complete.ps1" -ForegroundColor White
                exit
            } catch {
                Write-Host "❌ Error al instalar con Chocolatey: $_" -ForegroundColor Red
                Write-Host "Asegúrate de que Chocolatey esté instalado o prueba otra opción" -ForegroundColor Yellow
                exit 1
            }
        }
        "4" {
            Write-Host ""
            Write-Host "📖 Instalación manual:" -ForegroundColor Cyan
            Write-Host "1. Descarga el instalador desde: https://awscli.amazonaws.com/AWSCLIV2.msi"
            Write-Host "2. Ejecuta el instalador MSI"
            Write-Host "3. Cierra y vuelve a abrir PowerShell"
            Write-Host "4. Ejecuta: .\setup_aws_complete.ps1"
            exit
        }
        default {
            Write-Host "❌ Opción inválida" -ForegroundColor Red
            exit 1
        }
    }
}

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "CONFIGURACIÓN DE CREDENCIALES AWS" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# Paso 2: Verificar si AWS CLI ya está configurado
Write-Host "📋 Verificando configuración existente..." -ForegroundColor Yellow
$awsConfigured = $false

try {
    $identity = aws sts get-caller-identity 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ AWS CLI ya está configurado" -ForegroundColor Green
        $identity | ConvertFrom-Json | Format-List
        $awsConfigured = $true
        
        Write-Host ""
        $reconfigure = Read-Host "¿Deseas reconfigurar las credenciales? (s/N)"
        if ($reconfigure -ne "s") {
            Write-Host "Saltando configuración de credenciales..." -ForegroundColor Yellow
        } else {
            $awsConfigured = $false
        }
    }
} catch {
    Write-Host "⚠️  AWS CLI no está configurado" -ForegroundColor Yellow
}

# Paso 3: Configurar AWS CLI si es necesario
if (-not $awsConfigured) {
    Write-Host ""
    Write-Host "Para configurar AWS CLI necesitas:" -ForegroundColor Cyan
    Write-Host "1. AWS Access Key ID"
    Write-Host "2. AWS Secret Access Key"
    Write-Host ""
    Write-Host "📖 ¿Cómo obtener estas credenciales?" -ForegroundColor Yellow
    Write-Host "1. Ve a: https://console.aws.amazon.com/iam/"
    Write-Host "2. Selecciona 'Users' > Tu usuario"
    Write-Host "3. Pestaña 'Security credentials'"
    Write-Host "4. Haz clic en 'Create access key'"
    Write-Host "5. Selecciona 'Command Line Interface (CLI)'"
    Write-Host "6. Copia el Access Key ID y Secret Access Key"
    Write-Host ""
    
    $continue = Read-Host "¿Tienes tus credenciales listas? (s/N)"
    if ($continue -ne "s") {
        Write-Host ""
        Write-Host "❌ Operación cancelada" -ForegroundColor Red
        Write-Host "Obtén tus credenciales y vuelve a ejecutar este script" -ForegroundColor Yellow
        exit
    }
    
    Write-Host ""
    Write-Host "Configurando AWS CLI..." -ForegroundColor Yellow
    aws configure
    
    # Verificar configuración
    Write-Host ""
    Write-Host "📋 Verificando configuración..." -ForegroundColor Yellow
    try {
        $identity = aws sts get-caller-identity 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ AWS CLI configurado correctamente" -ForegroundColor Green
            $identity | ConvertFrom-Json | Format-List
        } else {
            Write-Host "❌ Error en la configuración" -ForegroundColor Red
            Write-Host $identity
            exit 1
        }
    } catch {
        Write-Host "❌ Error al verificar configuración: $_" -ForegroundColor Red
        exit 1
    }
}

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "CREANDO ARCHIVO aws_credentials.json" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# Paso 4: Crear aws_credentials.json desde la configuración de AWS CLI
Write-Host "📝 Extrayendo credenciales de AWS CLI..." -ForegroundColor Yellow

try {
    # Obtener credenciales del archivo de configuración de AWS
    $awsConfigPath = "$env:USERPROFILE\.aws\credentials"
    $awsConfigRegionPath = "$env:USERPROFILE\.aws\config"
    
    if (-not (Test-Path $awsConfigPath)) {
        Write-Host "❌ No se encontró el archivo de credenciales de AWS CLI" -ForegroundColor Red
        exit 1
    }
    
    # Leer credenciales (sección [default])
    $credentials = Get-Content $awsConfigPath -Raw
    
    # Extraer access key y secret key usando regex
    if ($credentials -match 'aws_access_key_id\s*=\s*(.+)') {
        $accessKeyId = $matches[1].Trim()
    }
    
    if ($credentials -match 'aws_secret_access_key\s*=\s*(.+)') {
        $secretAccessKey = $matches[1].Trim()
    }
    
    # Obtener región
    $region = "us-east-1"  # Default
    if (Test-Path $awsConfigRegionPath) {
        $configContent = Get-Content $awsConfigRegionPath -Raw
        if ($configContent -match 'region\s*=\s*(.+)') {
            $region = $matches[1].Trim()
        }
    }
    
    # Solicitar nombre del bucket
    Write-Host ""
    $bucketName = Read-Host "Nombre del bucket S3 [unidades-proyecto-documents]"
    if ([string]::IsNullOrWhiteSpace($bucketName)) {
        $bucketName = "unidades-proyecto-documents"
    }
    
    # Crear objeto JSON
    $credentialsJson = @{
        aws_access_key_id = $accessKeyId
        aws_secret_access_key = $secretAccessKey
        region = $region
        bucket_name = $bucketName
    }
    
    # Guardar a archivo
    $jsonPath = "aws_credentials.json"
    $credentialsJson | ConvertTo-Json | Set-Content -Path $jsonPath -Encoding UTF8
    
    Write-Host ""
    Write-Host "================================================================================" -ForegroundColor Cyan
    Write-Host "✅ CONFIGURACIÓN COMPLETADA" -ForegroundColor Green
    Write-Host "================================================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "📁 Archivo creado: $(Resolve-Path $jsonPath)" -ForegroundColor Green
    Write-Host "🪣 Bucket configurado: $bucketName" -ForegroundColor Green
    Write-Host "🌍 Región: $region" -ForegroundColor Green
    Write-Host ""
    Write-Host "Próximos pasos:" -ForegroundColor Cyan
    Write-Host "1. Ejecuta el pipeline de transformación:" -ForegroundColor White
    Write-Host "   python transformation_app\data_transformation_unidades_proyecto.py" -ForegroundColor White
    Write-Host ""
    Write-Host "2. Los archivos se subirán automáticamente a S3:" -ForegroundColor White
    Write-Host "   - GeoJSON → /up-geodata/" -ForegroundColor Gray
    Write-Host "   - Logs    → /logs/" -ForegroundColor Gray
    Write-Host "   - Reports → /reports/" -ForegroundColor Gray
    Write-Host ""
    Write-Host "⚠️  RECORDATORIO: aws_credentials.json está protegido en .gitignore" -ForegroundColor Yellow
    Write-Host ""
    
} catch {
    Write-Host "❌ Error al crear aws_credentials.json: $_" -ForegroundColor Red
    exit 1
}
