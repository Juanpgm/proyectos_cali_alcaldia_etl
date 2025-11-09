#!/bin/bash
# Script de configuración de Workload Identity Federation (ADC) para el proyecto

set -e

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
GRAY='\033[0;90m'
NC='\033[0m' # No Color

# Función para imprimir con colores
print_color() {
    local color=$1
    shift
    echo -e "${color}$@${NC}"
}

print_color $CYAN "\n🔐 Configuración de Workload Identity Federation (ADC)\n"
print_color $GRAY "============================================================"

# Determinar entorno
ENVIRONMENT=${1:-dev}
if [ "$ENVIRONMENT" = "dev" ]; then
    PROJECT_ID="calitrack-44403"
elif [ "$ENVIRONMENT" = "prod" ]; then
    PROJECT_ID="dev-test-e778d"
else
    print_color $RED "❌ Entorno inválido. Usa: dev o prod"
    echo "Uso: ./setup-adc.sh [dev|prod]"
    exit 1
fi

print_color $YELLOW "\n📊 Configuración:"
print_color $NC "   Entorno: $ENVIRONMENT"
print_color $NC "   Proyecto: $PROJECT_ID"
echo ""

# Verificar si gcloud está instalado
print_color $YELLOW "🔍 Verificando Google Cloud CLI..."
if ! command -v gcloud &> /dev/null; then
    print_color $RED "   ❌ Google Cloud CLI no está instalado"
    print_color $YELLOW "\n📥 Instala gcloud CLI:"
    print_color $NC "   curl https://sdk.cloud.google.com | bash"
    print_color $NC "   O visita: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

GCLOUD_VERSION=$(gcloud --version | head -n 1)
print_color $GREEN "   ✅ $GCLOUD_VERSION"

# Configurar proyecto
print_color $YELLOW "\n⚙️  Configurando proyecto..."
if gcloud config set project $PROJECT_ID 2>/dev/null; then
    print_color $GREEN "   ✅ Proyecto configurado: $PROJECT_ID"
else
    print_color $RED "   ❌ Error configurando proyecto"
    exit 1
fi

CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)
print_color $CYAN "   📊 Proyecto activo: $CURRENT_PROJECT"

# Configurar Application Default Credentials
print_color $YELLOW "\n🔑 Configurando Application Default Credentials..."
print_color $GRAY "   Se abrirá tu navegador para autenticación..."
echo ""

if gcloud auth application-default login --project=$PROJECT_ID; then
    print_color $GREEN "\n✅ ADC configurado correctamente!"
    
    # Verificar credenciales
    print_color $YELLOW "\n🔍 Verificando credenciales..."
    CRED_PATH="$HOME/.config/gcloud/application_default_credentials.json"
    if [ -f "$CRED_PATH" ]; then
        print_color $GREEN "   ✅ Archivo de credenciales creado"
        print_color $GRAY "   📁 Ubicación: $CRED_PATH"
    fi
    
    # Mostrar próximos pasos
    print_color $GRAY "\n============================================================"
    print_color $CYAN "🎯 Próximos pasos:"
    
    print_color $YELLOW "\n1. Cambia a la rama correspondiente:"
    if [ "$ENVIRONMENT" = "dev" ]; then
        print_color $NC "   git checkout dev"
    else
        print_color $NC "   git checkout main"
    fi
    
    print_color $YELLOW "\n2. El sistema usará automáticamente ADC"
    print_color $NC "   - Firebase: $PROJECT_ID"
    print_color $NC "   - Google Sheets: Autenticación automática"
    
    print_color $YELLOW "\n3. Ejecuta tus pipelines:"
    print_color $NC "   python pipelines/unidades_proyecto_pipeline.py"
    
    print_color $CYAN "\n✨ Beneficios de ADC:"
    print_color $GREEN "   ✅ Sin archivos de credenciales estáticas"
    print_color $GREEN "   ✅ Rotación automática de tokens"
    print_color $GREEN "   ✅ Mayor seguridad"
    print_color $GREEN "   ✅ Auditoría completa"
    
    print_color $GRAY "\n============================================================"
    
else
    print_color $RED "\n❌ Error configurando ADC"
    exit 1
fi

# Habilitar APIs necesarias (opcional)
print_color $YELLOW "\n🔧 ¿Deseas habilitar las APIs necesarias? (s/N): "
read -r response

if [[ "$response" =~ ^[Ss]$ ]]; then
    print_color $YELLOW "\n📡 Habilitando APIs..."
    
    APIS=(
        "firebase.googleapis.com"
        "firestore.googleapis.com"
        "sheets.googleapis.com"
        "drive.googleapis.com"
    )
    
    for api in "${APIS[@]}"; do
        print_color $GRAY "   Habilitando $api..."
        if gcloud services enable $api --project=$PROJECT_ID 2>/dev/null; then
            print_color $GREEN "   ✅ $api habilitada"
        else
            print_color $YELLOW "   ⚠️  Error habilitando $api"
        fi
    done
fi

print_color $GREEN "\n🎉 ¡Configuración completada!"
echo ""
