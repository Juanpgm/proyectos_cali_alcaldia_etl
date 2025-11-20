#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Configuración de Google Cloud para Cloud Functions con acceso a S3
    
.DESCRIPTION
    Este script configura:
    - Secret Manager con credenciales AWS
    - Service Account para Cloud Functions
    - Permisos necesarios
    - Deploy de Cloud Functions
    
.PARAMETER ProjectId
    ID del proyecto de Google Cloud
    
.PARAMETER Region
    Región donde desplegar las funciones (default: us-central1)
    
.EXAMPLE
    .\setup_cloud_functions.ps1 -ProjectId "mi-proyecto-123" -Region "us-central1"
#>

param(
    [Parameter(Mandatory=$false)]
    [string]$ProjectId,
    
    [Parameter(Mandatory=$false)]
    [string]$Region = "us-central1"
)

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   SETUP CLOUD FUNCTIONS - SERVERLESS ETL" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# 1. Verificar gcloud CLI
Write-Host "[1/8] Verificando gcloud CLI..." -ForegroundColor Yellow
try {
    $gcloudVersion = gcloud version 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "gcloud CLI no encontrado"
    }
    Write-Host "  ✓ gcloud CLI encontrado" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Error: gcloud CLI no instalado" -ForegroundColor Red
    Write-Host "  Instala desde: https://cloud.google.com/sdk/docs/install" -ForegroundColor Yellow
    exit 1
}

# 2. Obtener o solicitar Project ID
Write-Host "`n[2/8] Configurando proyecto GCP..." -ForegroundColor Yellow
if (-not $ProjectId) {
    $currentProject = gcloud config get-value project 2>$null
    if ($currentProject) {
        Write-Host "  Proyecto actual: $currentProject" -ForegroundColor Cyan
        $useCurrentProject = Read-Host "  ¿Usar este proyecto? (s/n)"
        if ($useCurrentProject -eq "s" -or $useCurrentProject -eq "S") {
            $ProjectId = $currentProject
        }
    }
    
    if (-not $ProjectId) {
        $ProjectId = Read-Host "  Ingresa el Project ID de GCP"
    }
}

gcloud config set project $ProjectId
Write-Host "  ✓ Proyecto configurado: $ProjectId" -ForegroundColor Green

# 3. Habilitar APIs necesarias
Write-Host "`n[3/8] Habilitando APIs de GCP..." -ForegroundColor Yellow
$apis = @(
    "cloudfunctions.googleapis.com",
    "cloudbuild.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudscheduler.googleapis.com",
    "firestore.googleapis.com"
)

foreach ($api in $apis) {
    Write-Host "  Habilitando $api..." -ForegroundColor Cyan
    gcloud services enable $api --project=$ProjectId 2>$null
}
Write-Host "  ✓ APIs habilitadas" -ForegroundColor Green

# 4. Crear Secret en Secret Manager con credenciales AWS
Write-Host "`n[4/8] Configurando credenciales AWS en Secret Manager..." -ForegroundColor Yellow
$awsCredsFile = Join-Path $PSScriptRoot "..\aws_credentials.json"

if (-not (Test-Path $awsCredsFile)) {
    Write-Host "  ✗ Error: No se encontró aws_credentials.json" -ForegroundColor Red
    Write-Host "  Ejecuta primero setup_aws_quick.ps1" -ForegroundColor Yellow
    exit 1
}

# Leer credenciales
$awsCreds = Get-Content $awsCredsFile -Raw | ConvertFrom-Json

# Crear JSON para Secret Manager
$secretData = @{
    aws_access_key_id = $awsCreds.aws_access_key_id
    aws_secret_access_key = $awsCreds.aws_secret_access_key
    region = $awsCreds.region
} | ConvertTo-Json -Compress

# Verificar si el secret ya existe
$secretExists = gcloud secrets list --project=$ProjectId --filter="name:aws-credentials" --format="value(name)" 2>$null

if ($secretExists) {
    Write-Host "  Secret 'aws-credentials' ya existe, agregando nueva versión..." -ForegroundColor Cyan
    echo $secretData | gcloud secrets versions add aws-credentials --data-file=- --project=$ProjectId
} else {
    Write-Host "  Creando nuevo secret 'aws-credentials'..." -ForegroundColor Cyan
    echo $secretData | gcloud secrets create aws-credentials --data-file=- --replication-policy="automatic" --project=$ProjectId
}

Write-Host "  ✓ Credenciales AWS almacenadas en Secret Manager" -ForegroundColor Green

# 5. Crear Service Account para Cloud Functions
Write-Host "`n[5/8] Configurando Service Account..." -ForegroundColor Yellow
$serviceAccountName = "cloud-functions-etl"
$serviceAccountEmail = "$serviceAccountName@$ProjectId.iam.gserviceaccount.com"

# Verificar si ya existe
$saExists = gcloud iam service-accounts list --project=$ProjectId --filter="email:$serviceAccountEmail" --format="value(email)" 2>$null

if (-not $saExists) {
    Write-Host "  Creando Service Account..." -ForegroundColor Cyan
    gcloud iam service-accounts create $serviceAccountName `
        --display-name="Cloud Functions ETL Service Account" `
        --project=$ProjectId
} else {
    Write-Host "  Service Account ya existe" -ForegroundColor Cyan
}

# Asignar permisos
Write-Host "  Asignando permisos..." -ForegroundColor Cyan

# Permisos de Secret Manager
gcloud secrets add-iam-policy-binding aws-credentials `
    --member="serviceAccount:$serviceAccountEmail" `
    --role="roles/secretmanager.secretAccessor" `
    --project=$ProjectId 2>$null

# Permisos de Firestore
gcloud projects add-iam-policy-binding $ProjectId `
    --member="serviceAccount:$serviceAccountEmail" `
    --role="roles/datastore.user" 2>$null

Write-Host "  ✓ Service Account configurado" -ForegroundColor Green

# 6. Deploy Cloud Function - Principal
Write-Host "`n[6/8] Desplegando Cloud Function principal..." -ForegroundColor Yellow
Write-Host "  Esto puede tomar varios minutos..." -ForegroundColor Cyan

$cloudFunctionsDir = Join-Path $PSScriptRoot ".."
Push-Location $cloudFunctionsDir\cloud_functions

try {
    gcloud functions deploy load-unidades-proyecto `
        --gen2 `
        --runtime=python311 `
        --region=$Region `
        --source=. `
        --entry-point=load_unidades_proyecto_from_s3 `
        --trigger-http `
        --allow-unauthenticated `
        --service-account=$serviceAccountEmail `
        --set-env-vars="S3_BUCKET_NAME=unidades-proyecto-documents,AWS_CREDENTIALS_SECRET=projects/$ProjectId/secrets/aws-credentials" `
        --memory=512MB `
        --timeout=540s `
        --max-instances=1 `
        --project=$ProjectId
    
    Write-Host "  ✓ Función principal desplegada" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Error en deploy: $_" -ForegroundColor Red
} finally {
    Pop-Location
}

# 7. Deploy Cloud Function - Manual Trigger
Write-Host "`n[7/8] Desplegando función de trigger manual..." -ForegroundColor Yellow

Push-Location $cloudFunctionsDir\cloud_functions

try {
    gcloud functions deploy manual-trigger-unidades-proyecto `
        --gen2 `
        --runtime=python311 `
        --region=$Region `
        --source=. `
        --entry-point=manual_trigger `
        --trigger-http `
        --allow-unauthenticated `
        --service-account=$serviceAccountEmail `
        --set-env-vars="S3_BUCKET_NAME=unidades-proyecto-documents,AWS_CREDENTIALS_SECRET=projects/$ProjectId/secrets/aws-credentials" `
        --memory=512MB `
        --timeout=540s `
        --max-instances=1 `
        --project=$ProjectId
    
    Write-Host "  ✓ Función de trigger manual desplegada" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Error en deploy: $_" -ForegroundColor Red
} finally {
    Pop-Location
}

# 8. Configurar Cloud Scheduler (opcional)
Write-Host "`n[8/8] Configuración de Cloud Scheduler..." -ForegroundColor Yellow
$setupScheduler = Read-Host "  ¿Deseas configurar ejecución automática con Cloud Scheduler? (s/n)"

if ($setupScheduler -eq "s" -or $setupScheduler -eq "S") {
    Write-Host "  Configurando scheduler..." -ForegroundColor Cyan
    
    # Obtener URL de la función
    $functionUrl = gcloud functions describe load-unidades-proyecto `
        --region=$Region `
        --project=$ProjectId `
        --gen2 `
        --format="value(serviceConfig.uri)" 2>$null
    
    if ($functionUrl) {
        # Crear job de Cloud Scheduler (diario a las 2 AM)
        $jobExists = gcloud scheduler jobs list --project=$ProjectId --filter="name:etl-unidades-proyecto-daily" --format="value(name)" 2>$null
        
        if ($jobExists) {
            Write-Host "  Job de scheduler ya existe, actualizando..." -ForegroundColor Cyan
            gcloud scheduler jobs update http etl-unidades-proyecto-daily `
                --location=$Region `
                --schedule="0 2 * * *" `
                --uri=$functionUrl `
                --http-method=POST `
                --project=$ProjectId 2>$null
        } else {
            Write-Host "  Creando nuevo job de scheduler..." -ForegroundColor Cyan
            gcloud scheduler jobs create http etl-unidades-proyecto-daily `
                --location=$Region `
                --schedule="0 2 * * *" `
                --uri=$functionUrl `
                --http-method=POST `
                --project=$ProjectId 2>$null
        }
        
        Write-Host "  ✓ Cloud Scheduler configurado (diario 2:00 AM)" -ForegroundColor Green
    } else {
        Write-Host "  ✗ No se pudo obtener URL de la función" -ForegroundColor Red
    }
} else {
    Write-Host "  Scheduler no configurado" -ForegroundColor Yellow
}

# Resumen final
Write-Host "`n" + "="*50 -ForegroundColor Cyan
Write-Host "   CONFIGURACIÓN COMPLETADA" -ForegroundColor Green
Write-Host "="*50 -ForegroundColor Cyan
Write-Host ""
Write-Host "📋 URLs de las funciones:" -ForegroundColor Cyan

$mainFunctionUrl = gcloud functions describe load-unidades-proyecto `
    --region=$Region `
    --project=$ProjectId `
    --gen2 `
    --format="value(serviceConfig.uri)" 2>$null

$manualFunctionUrl = gcloud functions describe manual-trigger-unidades-proyecto `
    --region=$Region `
    --project=$ProjectId `
    --gen2 `
    --format="value(serviceConfig.uri)" 2>$null

if ($mainFunctionUrl) {
    Write-Host "  Principal: $mainFunctionUrl" -ForegroundColor White
}

if ($manualFunctionUrl) {
    Write-Host "  Manual: $manualFunctionUrl" -ForegroundColor White
}

Write-Host "`n🚀 Para ejecutar manualmente:" -ForegroundColor Cyan
Write-Host "  Invoke-WebRequest -Uri '$manualFunctionUrl' -Method POST" -ForegroundColor White

Write-Host "`n📝 Próximos pasos:" -ForegroundColor Cyan
Write-Host "  1. Probar ejecución manual con el comando de arriba" -ForegroundColor White
Write-Host "  2. Verificar datos en Firestore Console" -ForegroundColor White
Write-Host "  3. Revisar logs en Cloud Functions Console" -ForegroundColor White

Write-Host "`n✅ Setup completado exitosamente!" -ForegroundColor Green
