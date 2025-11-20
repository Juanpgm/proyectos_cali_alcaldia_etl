# PowerShell script para configurar Cloud Scheduler
# Uso: .\setup-cloud-scheduler.ps1 -ProjectId "YOUR_PROJECT_ID" [-Region "us-central1"] [-FunctionUrl "https://..."]

param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectId,
    
    [Parameter(Mandatory = $false)]
    [string]$Region = "us-central1",
    
    [Parameter(Mandatory = $false)]
    [string]$FunctionUrl,
    
    [Parameter(Mandatory = $false)]
    [string]$Schedule = "0 * * * *",
    
    [Parameter(Mandatory = $false)]
    [string]$TimeZone = "America/Bogota"
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
Write-Host "║       Cloud Scheduler Setup - ETL Pipeline Hourly       ║" -ForegroundColor Blue
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Blue
Write-Host ""

# Si no se proporciona URL, intentar obtenerla
if (-not $FunctionUrl) {
    Write-Info "Obteniendo URL de Cloud Function..."
    $FunctionUrl = gcloud functions describe etl-pipeline-hourly `
        --region=$Region `
        --gen2 `
        --format="value(serviceConfig.uri)" `
        --project=$ProjectId 2>$null
    
    if (-not $FunctionUrl) {
        Write-Error-Custom "No se pudo obtener URL de la Cloud Function"
        Write-Info "Asegúrate de que la función esté desplegada o proporciona la URL manualmente"
        exit 1
    }
}

Write-Info "Configuración:"
Write-Host "  - Proyecto: $ProjectId"
Write-Host "  - Región: $Region"
Write-Host "  - Schedule: $Schedule (cada hora desde medianoche)"
Write-Host "  - Timezone: $TimeZone"
Write-Host "  - Function URL: $FunctionUrl"

# Verificar si el job ya existe
Write-Info "Verificando si el job ya existe..."
$jobExists = $false
try {
    $null = gcloud scheduler jobs describe etl-pipeline-hourly-job `
        --location=$Region `
        --project=$ProjectId 2>$null
    $jobExists = $LASTEXITCODE -eq 0
}
catch {
    $jobExists = $false
}

if ($jobExists) {
    Write-Warning "El job 'etl-pipeline-hourly-job' ya existe"
    $confirmation = Read-Host "¿Deseas eliminarlo y crear uno nuevo? (y/n)"
    
    if ($confirmation -eq 'y' -or $confirmation -eq 'Y') {
        Write-Info "Eliminando job existente..."
        gcloud scheduler jobs delete etl-pipeline-hourly-job `
            --location=$Region `
            --project=$ProjectId `
            --quiet
        Write-Success "Job eliminado"
    }
    else {
        Write-Warning "Configuración cancelada"
        exit 0
    }
}

# Preguntar confirmación
Write-Host ""
$confirmation = Read-Host "¿Crear Cloud Scheduler job? (y/n)"

if ($confirmation -ne 'y' -and $confirmation -ne 'Y') {
    Write-Warning "Configuración cancelada"
    exit 0
}

# Crear job
Write-Info "Creando Cloud Scheduler job..."
Write-Host ""

try {
    gcloud scheduler jobs create http etl-pipeline-hourly-job `
        --location=$Region `
        --schedule="$Schedule" `
        --time-zone="$TimeZone" `
        --uri="$FunctionUrl" `
        --http-method=POST `
        --project=$ProjectId
    
    Write-Host ""
    Write-Success "Cloud Scheduler job creado exitosamente!"
    
    Write-Host ""
    Write-Info "Configuración del job:"
    Write-Host "  - Nombre: etl-pipeline-hourly-job"
    Write-Host "  - Schedule: $Schedule"
    $nextHour = (Get-Date).AddHours(1).ToString("yyyy-MM-dd HH:00:00")
    Write-Host "  - Próxima ejecución: $nextHour"
    
    Write-Host ""
    Write-Info "Comandos útiles:"
    Write-Host "  # Ejecutar manualmente (testing):"
    Write-Host "  gcloud scheduler jobs run etl-pipeline-hourly-job --location=$Region --project=$ProjectId"
    Write-Host ""
    Write-Host "  # Ver detalles del job:"
    Write-Host "  gcloud scheduler jobs describe etl-pipeline-hourly-job --location=$Region --project=$ProjectId"
    Write-Host ""
    Write-Host "  # Pausar ejecuciones:"
    Write-Host "  gcloud scheduler jobs pause etl-pipeline-hourly-job --location=$Region --project=$ProjectId"
    Write-Host ""
    Write-Host "  # Reanudar ejecuciones:"
    Write-Host "  gcloud scheduler jobs resume etl-pipeline-hourly-job --location=$Region --project=$ProjectId"
    
    Write-Host ""
    Write-Warning "Nota: El pipeline se ejecutará cada hora automáticamente"
    Write-Info "Monitorea los logs con: gcloud functions logs read etl-pipeline-hourly --region=$Region --limit=50"
    
}
catch {
    Write-Host ""
    Write-Error-Custom "Error al crear Cloud Scheduler job: $_"
    exit 1
}
