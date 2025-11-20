# Script simplificado para configurar credenciales AWS
# No requiere reiniciar PowerShell

$awsCli = "C:\Program Files\Amazon\AWSCLIV2\aws.exe"

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "CONFIGURACIÓN RÁPIDA DE CREDENCIALES AWS" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# Verificar si ya existe configuración
$awsConfigPath = "$env:USERPROFILE\.aws\credentials"
$configExists = Test-Path $awsConfigPath

if ($configExists) {
    Write-Host "✅ Configuración de AWS CLI detectada" -ForegroundColor Green
    Write-Host ""
    Write-Host "Verificando credenciales..." -ForegroundColor Yellow
    
    try {
        $identity = & $awsCli sts get-caller-identity 2>&1 | ConvertFrom-Json
        Write-Host "✅ Credenciales válidas:" -ForegroundColor Green
        Write-Host "   Account: $($identity.Account)" -ForegroundColor Gray
        Write-Host "   UserId: $($identity.UserId)" -ForegroundColor Gray
        Write-Host ""
        
        # Extraer credenciales existentes
        $credentials = Get-Content $awsConfigPath -Raw
        $accessKeyId = ""
        $secretAccessKey = ""
        
        if ($credentials -match 'aws_access_key_id\s*=\s*(.+)') {
            $accessKeyId = $matches[1].Trim()
        }
        
        if ($credentials -match 'aws_secret_access_key\s*=\s*(.+)') {
            $secretAccessKey = $matches[1].Trim()
        }
        
        # Obtener región
        $region = "us-east-1"
        $awsConfigRegionPath = "$env:USERPROFILE\.aws\config"
        if (Test-Path $awsConfigRegionPath) {
            $configContent = Get-Content $awsConfigRegionPath -Raw
            if ($configContent -match 'region\s*=\s*(.+)') {
                $region = $matches[1].Trim()
            }
        }
        
        # Crear aws_credentials.json
        Write-Host "📝 Creando archivo aws_credentials.json..." -ForegroundColor Yellow
        
        $bucketName = "unidades-proyecto-documents"
        
        $credentialsJson = @{
            aws_access_key_id = $accessKeyId
            aws_secret_access_key = $secretAccessKey
            region = $region
            bucket_name = $bucketName
        }
        
        $jsonPath = "aws_credentials.json"
        $credentialsJson | ConvertTo-Json | Set-Content -Path $jsonPath -Encoding UTF8
        
        Write-Host ""
        Write-Host "================================================================================" -ForegroundColor Cyan
        Write-Host "✅ CONFIGURACIÓN COMPLETADA" -ForegroundColor Green
        Write-Host "================================================================================" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "📁 Archivo creado: $(Resolve-Path $jsonPath)" -ForegroundColor Green
        Write-Host "🪣 Bucket: $bucketName" -ForegroundColor Green
        Write-Host "🌍 Región: $region" -ForegroundColor Green
        Write-Host ""
        Write-Host "✅ Ahora puedes ejecutar el pipeline de transformación" -ForegroundColor Green
        Write-Host "   python transformation_app\data_transformation_unidades_proyecto.py" -ForegroundColor White
        Write-Host ""
        
    } catch {
        Write-Host "⚠️  Credenciales configuradas pero inválidas o sin acceso a internet" -ForegroundColor Yellow
        Write-Host ""
        
        $reconfigure = Read-Host "¿Deseas reconfigurar? (s/N)"
        if ($reconfigure -eq "s") {
            $configExists = $false
        }
    }
}

if (-not $configExists) {
    Write-Host "⚠️  No se encontró configuración de AWS CLI" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Para obtener credenciales AWS:" -ForegroundColor Cyan
    Write-Host "1. Ve a: https://console.aws.amazon.com/iam/" -ForegroundColor White
    Write-Host "2. Selecciona 'Users' > Tu usuario" -ForegroundColor White
    Write-Host "3. Pestaña 'Security credentials'" -ForegroundColor White
    Write-Host "4. Haz clic en 'Create access key'" -ForegroundColor White
    Write-Host "5. Selecciona 'Command Line Interface (CLI)'" -ForegroundColor White
    Write-Host "6. Copia las credenciales" -ForegroundColor White
    Write-Host ""
    
    $continue = Read-Host "¿Tienes tus credenciales? (s/N)"
    if ($continue -ne "s") {
        Write-Host ""
        Write-Host "❌ Obtén las credenciales y ejecuta nuevamente este script" -ForegroundColor Red
        exit
    }
    
    Write-Host ""
    Write-Host "Configurando AWS CLI..." -ForegroundColor Yellow
    Write-Host ""
    
    & $awsCli configure
    
    Write-Host ""
    Write-Host "Verificando configuración..." -ForegroundColor Yellow
    
    try {
        $identity = & $awsCli sts get-caller-identity 2>&1 | ConvertFrom-Json
        Write-Host "✅ Configuración exitosa" -ForegroundColor Green
        Write-Host ""
        
        # Ahora crear el archivo JSON
        Write-Host "📝 Creando aws_credentials.json..." -ForegroundColor Yellow
        
        $credentials = Get-Content "$env:USERPROFILE\.aws\credentials" -Raw
        $accessKeyId = ""
        $secretAccessKey = ""
        
        if ($credentials -match 'aws_access_key_id\s*=\s*(.+)') {
            $accessKeyId = $matches[1].Trim()
        }
        
        if ($credentials -match 'aws_secret_access_key\s*=\s*(.+)') {
            $secretAccessKey = $matches[1].Trim()
        }
        
        $region = "us-east-1"
        $awsConfigRegionPath = "$env:USERPROFILE\.aws\config"
        if (Test-Path $awsConfigRegionPath) {
            $configContent = Get-Content $awsConfigRegionPath -Raw
            if ($configContent -match 'region\s*=\s*(.+)') {
                $region = $matches[1].Trim()
            }
        }
        
        $bucketName = "unidades-proyecto-documents"
        
        $credentialsJson = @{
            aws_access_key_id = $accessKeyId
            aws_secret_access_key = $secretAccessKey
            region = $region
            bucket_name = $bucketName
        }
        
        $jsonPath = "aws_credentials.json"
        $credentialsJson | ConvertTo-Json | Set-Content -Path $jsonPath -Encoding UTF8
        
        Write-Host ""
        Write-Host "================================================================================" -ForegroundColor Cyan
        Write-Host "✅ CONFIGURACIÓN COMPLETADA" -ForegroundColor Green
        Write-Host "================================================================================" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "📁 Archivo creado: $(Resolve-Path $jsonPath)" -ForegroundColor Green
        Write-Host "🪣 Bucket: $bucketName" -ForegroundColor Green
        Write-Host "🌍 Región: $region" -ForegroundColor Green
        Write-Host ""
        Write-Host "✅ Ahora puedes ejecutar el pipeline de transformación" -ForegroundColor Green
        Write-Host "   python transformation_app\data_transformation_unidades_proyecto.py" -ForegroundColor White
        Write-Host ""
        
    } catch {
        Write-Host "❌ Error en la configuración" -ForegroundColor Red
        Write-Host $_.Exception.Message
        exit 1
    }
}
