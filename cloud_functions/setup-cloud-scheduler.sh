#!/bin/bash
# Script para configurar Cloud Scheduler que ejecuta la Cloud Function cada hora
# Uso: ./setup-cloud-scheduler.sh [PROJECT_ID] [REGION] [FUNCTION_URL]

set -e  # Exit on error

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Valores por defecto
DEFAULT_REGION="us-central1"
DEFAULT_SCHEDULE="0 * * * *"  # Cada hora desde medianoche
DEFAULT_TIMEZONE="America/Bogota"

# Función para imprimir con color
print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Banner
echo -e "${BLUE}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║       Cloud Scheduler Setup - ETL Pipeline Hourly       ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Verificar parámetros
if [ -z "$1" ]; then
    print_error "Falta PROJECT_ID"
    echo "Uso: ./setup-cloud-scheduler.sh PROJECT_ID [REGION] [FUNCTION_URL]"
    exit 1
fi

PROJECT_ID=$1
REGION=${2:-$DEFAULT_REGION}
FUNCTION_URL=$3

# Si no se proporciona URL, intentar obtenerla
if [ -z "$FUNCTION_URL" ]; then
    print_info "Obteniendo URL de Cloud Function..."
    FUNCTION_URL=$(gcloud functions describe etl-pipeline-hourly \
        --region=$REGION \
        --gen2 \
        --format="value(serviceConfig.uri)" \
        --project=$PROJECT_ID 2>/dev/null)
    
    if [ -z "$FUNCTION_URL" ]; then
        print_error "No se pudo obtener URL de la Cloud Function"
        print_info "Asegúrate de que la función esté desplegada o proporciona la URL manualmente"
        exit 1
    fi
fi

print_info "Configuración:"
echo "  - Proyecto: $PROJECT_ID"
echo "  - Región: $REGION"
echo "  - Schedule: $DEFAULT_SCHEDULE (cada hora desde medianoche)"
echo "  - Timezone: $DEFAULT_TIMEZONE"
echo "  - Function URL: $FUNCTION_URL"

# Verificar si el job ya existe
print_info "Verificando si el job ya existe..."
if gcloud scheduler jobs describe etl-pipeline-hourly-job \
    --location=$REGION \
    --project=$PROJECT_ID &>/dev/null; then
    
    print_warning "El job 'etl-pipeline-hourly-job' ya existe"
    read -p "¿Deseas eliminarlo y crear uno nuevo? (y/n): " -n 1 -r
    echo ""
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_info "Eliminando job existente..."
        gcloud scheduler jobs delete etl-pipeline-hourly-job \
            --location=$REGION \
            --project=$PROJECT_ID \
            --quiet
        print_success "Job eliminado"
    else
        print_warning "Configuración cancelada"
        exit 0
    fi
fi

# Preguntar confirmación
echo ""
read -p "¿Crear Cloud Scheduler job? (y/n): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_warning "Configuración cancelada"
    exit 0
fi

# Crear job
print_info "Creando Cloud Scheduler job..."
echo ""

if gcloud scheduler jobs create http etl-pipeline-hourly-job \
    --location=$REGION \
    --schedule="$DEFAULT_SCHEDULE" \
    --time-zone="$DEFAULT_TIMEZONE" \
    --uri="$FUNCTION_URL" \
    --http-method=POST \
    --project=$PROJECT_ID; then
    
    echo ""
    print_success "Cloud Scheduler job creado exitosamente!"
    
    echo ""
    print_info "Configuración del job:"
    echo "  - Nombre: etl-pipeline-hourly-job"
    echo "  - Schedule: $DEFAULT_SCHEDULE"
    echo "  - Próxima ejecución: $(date -d 'next hour' '+%Y-%m-%d %H:00:00')"
    
    echo ""
    print_info "Comandos útiles:"
    echo "  # Ejecutar manualmente (testing):"
    echo "  gcloud scheduler jobs run etl-pipeline-hourly-job --location=$REGION --project=$PROJECT_ID"
    echo ""
    echo "  # Ver detalles del job:"
    echo "  gcloud scheduler jobs describe etl-pipeline-hourly-job --location=$REGION --project=$PROJECT_ID"
    echo ""
    echo "  # Pausar ejecuciones:"
    echo "  gcloud scheduler jobs pause etl-pipeline-hourly-job --location=$REGION --project=$PROJECT_ID"
    echo ""
    echo "  # Reanudar ejecuciones:"
    echo "  gcloud scheduler jobs resume etl-pipeline-hourly-job --location=$REGION --project=$PROJECT_ID"
    
    echo ""
    print_warning "Nota: El pipeline se ejecutará cada hora automáticamente"
    print_info "Monitorea los logs con: gcloud functions logs read etl-pipeline-hourly --region=$REGION --limit=50"
    
else
    echo ""
    print_error "Error al crear Cloud Scheduler job"
    exit 1
fi
