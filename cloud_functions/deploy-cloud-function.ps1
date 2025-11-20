# PowerShell script para desplegar Cloud Function ETL Pipeline
# Uso: .\deploy-cloud-function.ps1 -ProjectId "YOUR_PROJECT_ID" [-Region "us-central1"] [-ServiceAccountEmail "sa@project.iam.gserviceaccount.com"]

param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectId,
    
    [Parameter(Mandatory = $false)]
    [string]$Region = "us-central1",
    
    [Parameter(Mandatory = $false)]
    [string]$ServiceAccountEmail,
    
    [Parameter(Mandatory = $false)]
    [string]$Memory = "2048MB",
    
    [Parameter(Mandatory = $false)]
    [string]$Timeout = "540s"
)

# Colores para output
function Write-Info {
    param([string]$Message)
    Write-Host "ℹ $Message" -ForegroundColor Blue
}

function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" -ForegroundColor Green
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠ $Message" -ForegroundColor Yellow
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

# Banner
Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Blue
Write-Host "║   Cloud Function Deployment - ETL Pipeline Serverless   ║" -ForegroundColor Blue
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Blue
Write-Host ""

Write-Info "Configuración:"
Write-Host "  - Proyecto: $ProjectId"
Write-Host "  - Región: $Region"
Write-Host "  - Memoria: $Memory"
Write-Host "  - Timeout: $Timeout"

# Verificar gcloud instalado
Write-Info "Verificando gcloud CLI..."
try {
    $gcloudVersion = gcloud --version 2>&1 | Select-String -Pattern "Google Cloud SDK"
    Write-Success "gcloud CLI encontrado: $gcloudVersion"
}
catch {
    Write-Error-Custom "gcloud CLI no está instalado"
    Write-Host "Instala desde: https://cloud.google.com/sdk/docs/install"
    exit 1
}

# Verificar archivos necesarios
Write-Info "Verificando archivos necesarios..."

$requiredFiles = @("main.py", "requirements.txt")
foreach ($file in $requiredFiles) {
    if (Test-Path $file) {
        Write-Success "Encontrado: $file"
    }
    else {
        Write-Error-Custom "Archivo no encontrado: $file"
        exit 1
    }
}

# Construir comando de despliegue
$deployArgs = @(
    "functions", "deploy", "etl-pipeline-hourly",
    "--gen2",
    "--runtime=python311",
    "--region=$Region",
    "--source=.",
    "--entry-point=etl_pipeline_hourly",
    "--trigger-http",
    "--allow-unauthenticated",
    "--memory=$Memory",
    "--timeout=$Timeout",
    "--max-instances=1",
    "--project=$ProjectId"
)

# Agregar service account si se proporcionó
if ($ServiceAccountEmail) {
    $deployArgs += "--service-account=$ServiceAccountEmail"
    Write-Info "Usando Service Account: $ServiceAccountEmail"
}

# Preguntar confirmación
Write-Host ""
$confirmation = Read-Host "¿Desplegar Cloud Function? (y/n)"

if ($confirmation -ne 'y' -and $confirmation -ne 'Y') {
    Write-Warning "Despliegue cancelado"
    exit 0
}

# Desplegar
Write-Info "Desplegando Cloud Function..."
Write-Host ""

try {
    & gcloud $deployArgs
    
    Write-Host ""
    Write-Success "Cloud Function desplegada exitosamente!"
    
    # Obtener URL de la función
    Write-Info "Obteniendo URL de la función..."
    $functionUrl = gcloud functions describe etl-pipeline-hourly `
        --region=$Region `
        --gen2 `
        --format="value(serviceConfig.uri)" `
        --project=$ProjectId 2>$null
    
    if ($functionUrl) {
        Write-Host ""
        Write-Success "URL de la función:"
        Write-Host "  $functionUrl"
        
        Write-Host ""
        Write-Info "Para probar la función:"
        Write-Host "  curl -X POST $functionUrl"
    }
    
    Write-Host ""
    Write-Info "Próximos pasos:"
    Write-Host "  1. Configurar Cloud Scheduler con: .\setup-cloud-scheduler.ps1 -ProjectId $ProjectId"
    Write-Host "  2. Verificar logs con: gcloud functions logs read etl-pipeline-hourly --region=$Region --limit=50"
    Write-Host "  3. Consultar guía completa: SERVERLESS_DEPLOYMENT_GUIDE.md"
    
}
catch {
    Write-Host ""
    Write-Error-Custom "Error en el despliegue: $_"
    exit 1
}
