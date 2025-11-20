#!/bin/bash
# Script de despliegue rápido para Cloud Function ETL Pipeline
# Uso: ./deploy-cloud-function.sh [PROJECT_ID] [REGION] [SERVICE_ACCOUNT_EMAIL]

set -e  # Exit on error

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Valores por defecto
DEFAULT_REGION="us-central1"
DEFAULT_MEMORY="2048MB"
DEFAULT_TIMEOUT="540s"

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
echo "║   Cloud Function Deployment - ETL Pipeline Serverless   ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Verificar parámetros
if [ -z "$1" ]; then
    print_error "Falta PROJECT_ID"
    echo "Uso: ./deploy-cloud-function.sh PROJECT_ID [REGION] [SERVICE_ACCOUNT_EMAIL]"
    exit 1
fi

PROJECT_ID=$1
REGION=${2:-$DEFAULT_REGION}
SERVICE_ACCOUNT_EMAIL=$3

print_info "Configuración:"
echo "  - Proyecto: $PROJECT_ID"
echo "  - Región: $REGION"
echo "  - Memoria: $DEFAULT_MEMORY"
echo "  - Timeout: $DEFAULT_TIMEOUT"

# Verificar gcloud instalado
if ! command -v gcloud &> /dev/null; then
    print_error "gcloud CLI no está instalado"
    echo "Instala desde: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

print_success "gcloud CLI encontrado: $(gcloud --version | head -n1)"

# Verificar archivos necesarios
print_info "Verificando archivos necesarios..."

REQUIRED_FILES=("main.py" "requirements.txt")
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        print_error "Archivo no encontrado: $file"
        exit 1
    fi
    print_success "Encontrado: $file"
done

# Construir comando de despliegue
DEPLOY_CMD="gcloud functions deploy etl-pipeline-hourly \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=. \
    --entry-point=etl_pipeline_hourly \
    --trigger-http \
    --allow-unauthenticated \
    --memory=$DEFAULT_MEMORY \
    --timeout=$DEFAULT_TIMEOUT \
    --max-instances=1 \
    --project=$PROJECT_ID"

# Agregar service account si se proporcionó
if [ -n "$SERVICE_ACCOUNT_EMAIL" ]; then
    DEPLOY_CMD="$DEPLOY_CMD --service-account=$SERVICE_ACCOUNT_EMAIL"
    print_info "Usando Service Account: $SERVICE_ACCOUNT_EMAIL"
fi

# Preguntar confirmación
echo ""
read -p "¿Desplegar Cloud Function? (y/n): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_warning "Despliegue cancelado"
    exit 0
fi

# Desplegar
print_info "Desplegando Cloud Function..."
echo ""

if eval $DEPLOY_CMD; then
    echo ""
    print_success "Cloud Function desplegada exitosamente!"
    
    # Obtener URL de la función
    FUNCTION_URL=$(gcloud functions describe etl-pipeline-hourly \
        --region=$REGION \
        --gen2 \
        --format="value(serviceConfig.uri)" \
        --project=$PROJECT_ID 2>/dev/null)
    
    if [ -n "$FUNCTION_URL" ]; then
        echo ""
        print_success "URL de la función:"
        echo "  $FUNCTION_URL"
        
        echo ""
        print_info "Para probar la función:"
        echo "  curl -X POST $FUNCTION_URL"
    fi
    
    echo ""
    print_info "Próximos pasos:"
    echo "  1. Configurar Cloud Scheduler con: setup-cloud-scheduler.sh"
    echo "  2. Verificar logs con: gcloud functions logs read etl-pipeline-hourly --region=$REGION --limit=50"
    echo "  3. Consultar guía completa: SERVERLESS_DEPLOYMENT_GUIDE.md"
    
else
    echo ""
    print_error "Error en el despliegue"
    exit 1
fi
