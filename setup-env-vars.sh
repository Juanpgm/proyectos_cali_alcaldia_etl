#!/bin/bash

# ============================================================
# Script de Configuración de Variables de Entorno - ETL Cali
# ============================================================
# Configura variables de entorno sensibles de manera segura
# Uso: ./setup-env-vars.sh [system|session|file]

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
GRAY='\033[0;90m'
NC='\033[0m' # No Color

# Banner
echo -e "${CYAN}"
echo "╔════════════════════════════════════════════════════════╗"
echo "║     Configuración de Variables de Entorno - ETL Cali  ║"
echo "╚════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Obtener método de configuración
METHOD=${1:-session}

# Validar método
if [[ ! "$METHOD" =~ ^(system|session|file)$ ]]; then
    echo -e "${RED}❌ Método inválido: $METHOD${NC}"
    echo -e "Uso: ./setup-env-vars.sh [system|session|file]"
    exit 1
fi

# Solicitar URL del Google Sheet
echo -e "${YELLOW}📊 Google Sheets Configuration${NC}"
echo -e "Ingresa la URL completa del Google Sheet de Unidades de Proyecto:"
echo -e "${GRAY}(Ejemplo: https://docs.google.com/spreadsheets/d/1ABC.../edit)${NC}"
read -p "URL: " SHEETS_URL

if [[ -z "$SHEETS_URL" ]]; then
    echo -e "${RED}❌ Error: URL del Google Sheet es requerida${NC}"
    exit 1
fi

# Solicitar GitHub Token (opcional)
echo -e "\n${YELLOW}🔑 GitHub Token (Opcional)${NC}"
echo -e "Ingresa tu GitHub Token (presiona Enter para omitir):"
echo -e "${GRAY}(Solo necesario para ejecutar workflows desde scripts)${NC}"
read -p "Token: " GITHUB_TOKEN

# Configurar según el método
echo -e "\n${CYAN}🔧 Método de configuración: $METHOD${NC}"

case $METHOD in
    system)
        echo -e "\n${YELLOW}⚠️  IMPORTANTE: Esto modificará tus archivos de configuración de shell${NC}"
        echo -e "${YELLOW}Las variables persistirán después de cerrar esta terminal${NC}"
        read -p "¿Continuar? (s/n): " CONFIRM
        
        if [[ "$CONFIRM" != "s" ]]; then
            echo -e "${RED}Operación cancelada${NC}"
            exit 0
        fi
        
        # Detectar shell
        SHELL_CONFIG=""
        if [[ -f "$HOME/.bashrc" ]]; then
            SHELL_CONFIG="$HOME/.bashrc"
        elif [[ -f "$HOME/.zshrc" ]]; then
            SHELL_CONFIG="$HOME/.zshrc"
        elif [[ -f "$HOME/.profile" ]]; then
            SHELL_CONFIG="$HOME/.profile"
        else
            echo -e "${RED}❌ No se pudo detectar archivo de configuración de shell${NC}"
            exit 1
        fi
        
        # Agregar variables al archivo de configuración
        echo "" >> "$SHELL_CONFIG"
        echo "# ETL Cali - Variables de entorno sensibles (agregadas por setup-env-vars.sh)" >> "$SHELL_CONFIG"
        echo "export SHEETS_UNIDADES_PROYECTO_URL='$SHEETS_URL'" >> "$SHELL_CONFIG"
        
        if [[ -n "$GITHUB_TOKEN" ]]; then
            echo "export GITHUB_TOKEN='$GITHUB_TOKEN'" >> "$SHELL_CONFIG"
        fi
        
        echo -e "${GREEN}✅ Variables agregadas a $SHELL_CONFIG${NC}"
        echo -e "\n${CYAN}💡 Variables configuradas permanentemente${NC}"
        echo -e "${CYAN}Ejecuta: source $SHELL_CONFIG${NC}"
        echo -e "${CYAN}O cierra y reabre tu terminal${NC}"
        ;;
        
    session)
        # Configurar variables de sesión
        export SHEETS_UNIDADES_PROYECTO_URL="$SHEETS_URL"
        echo -e "${GREEN}✅ SHEETS_UNIDADES_PROYECTO_URL configurada (Sesión)${NC}"
        
        if [[ -n "$GITHUB_TOKEN" ]]; then
            export GITHUB_TOKEN="$GITHUB_TOKEN"
            echo -e "${GREEN}✅ GITHUB_TOKEN configurado (Sesión)${NC}"
        fi
        
        echo -e "\n${CYAN}💡 Variables configuradas para esta sesión${NC}"
        echo -e "${YELLOW}Deberás reconfigurarlas si cierras esta terminal${NC}"
        echo -e "\n${CYAN}Para hacerlas permanentes, ejecuta:${NC}"
        echo -e "${GRAY}./setup-env-vars.sh system${NC}"
        
        # Crear script temporal para exportar en la sesión actual
        TEMP_SCRIPT="/tmp/setup-env-vars-temp-$$.sh"
        echo "export SHEETS_UNIDADES_PROYECTO_URL='$SHEETS_URL'" > "$TEMP_SCRIPT"
        if [[ -n "$GITHUB_TOKEN" ]]; then
            echo "export GITHUB_TOKEN='$GITHUB_TOKEN'" >> "$TEMP_SCRIPT"
        fi
        
        echo -e "\n${CYAN}Para aplicar en esta terminal, ejecuta:${NC}"
        echo -e "${GRAY}source $TEMP_SCRIPT${NC}"
        ;;
        
    file)
        # Crear archivo .env.local
        SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        ENV_LOCAL_PATH="$SCRIPT_DIR/.env.local"
        
        if [[ -f "$ENV_LOCAL_PATH" ]]; then
            echo -e "\n${YELLOW}⚠️  El archivo .env.local ya existe${NC}"
            read -p "¿Sobrescribir? (s/n): " CONFIRM
            
            if [[ "$CONFIRM" != "s" ]]; then
                echo -e "${RED}Operación cancelada${NC}"
                exit 0
            fi
        fi
        
        # Crear contenido del archivo
        cat > "$ENV_LOCAL_PATH" << EOF
# .env.local - Variables sensibles locales
# ==========================================
# Este archivo NO debe commitearse a Git
# Generado automáticamente por setup-env-vars.sh

# Google Sheets Configuration
SHEETS_UNIDADES_PROYECTO_URL=$SHEETS_URL
EOF
        
        if [[ -n "$GITHUB_TOKEN" ]]; then
            cat >> "$ENV_LOCAL_PATH" << EOF

# GitHub Token (opcional)
GITHUB_TOKEN=$GITHUB_TOKEN
EOF
        fi
        
        echo -e "${GREEN}✅ Archivo .env.local creado exitosamente${NC}"
        echo -e "${CYAN}📁 Ubicación: $ENV_LOCAL_PATH${NC}"
        
        echo -e "\n${CYAN}💡 Variables configuradas en archivo local${NC}"
        echo -e "${GREEN}El archivo .env.local está protegido por .gitignore${NC}"
        ;;
esac

# Verificar configuración
echo -e "\n${CYAN}🔍 Verificando configuración...${NC}"

if [[ "$METHOD" == "file" ]]; then
    echo -e "${GREEN}📄 Variables guardadas en .env.local${NC}"
    echo -e "${CYAN}Se cargarán automáticamente cuando ejecutes el proyecto${NC}"
elif [[ -z "$SHEETS_UNIDADES_PROYECTO_URL" ]]; then
    echo -e "${YELLOW}⚠️  No se pudo verificar SHEETS_UNIDADES_PROYECTO_URL${NC}"
    echo -e "${YELLOW}Si usaste 'system', ejecuta: source ~/.bashrc (o tu shell config)${NC}"
else
    echo -e "${GREEN}✅ SHEETS_UNIDADES_PROYECTO_URL está configurada${NC}"
    echo -e "${GRAY}   Valor: ${SHEETS_UNIDADES_PROYECTO_URL:0:50}...${NC}"
fi

# Instrucciones finales
echo -e "\n${CYAN}📚 Próximos pasos:${NC}"
echo "1. Configura ADC para Firebase/Sheets:"
echo -e "   ${GRAY}./setup-adc.sh dev${NC}"
echo "2. Ejecuta tu pipeline ETL normalmente"
echo "3. Las variables sensibles se cargarán automáticamente"

echo -e "\n${GREEN}✨ Configuración completada exitosamente${NC}\n"
