#!/bin/bash
# deploy_to_github.sh
# Script para subir la configuración de GitHub Actions

echo "🚀 DESPLEGANDO CONFIGURACIÓN GITHUB ACTIONS"
echo "============================================="

# Verificar si estamos en un repositorio git
if [ ! -d ".git" ]; then
    echo "❌ No es un repositorio Git. Inicializando..."
    git init
    git remote add origin https://github.com/Juanpgm/proyectos_cali_alcaldia_etl.git
fi

# Verificar status
echo "📋 Estado actual del repositorio:"
git status --short

# Agregar todos los archivos
echo ""
echo "📦 Agregando archivos..."
git add .

# Mostrar qué se va a commitear
echo ""
echo "📝 Archivos a commitear:"
git diff --cached --name-only

# Crear commit
echo ""
echo "💾 Creando commit..."
git commit -m "feat: Configure ETL automation with GitHub Actions for Railway PostgreSQL

✅ Implemented GitHub Actions workflow for automated ETL
✅ Added Railway PostgreSQL support via DATABASE_URL  
✅ Configured daily execution at 2 AM UTC
✅ Added manual trigger with parameters
✅ Included comprehensive logging and verification
✅ Created setup verification script
✅ Added implementation guide

Pipeline stages:
- Health check and database connection test
- Parallel data extraction from multiple sources  
- Data transformation and cleaning
- Loading to Railway PostgreSQL
- Data verification and notifications

The ETL will run automatically daily and load fresh data to Railway PostgreSQL."

# Verificar si el commit fue exitoso
if [ $? -eq 0 ]; then
    echo "✅ Commit creado exitosamente"
    
    # Push to GitHub
    echo ""
    echo "🚀 Subiendo a GitHub..."
    git push origin main
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "🎉 ¡DEPLOYMENT EXITOSO!"
        echo "======================="
        echo ""
        echo "✅ Código subido a GitHub"
        echo "✅ GitHub Actions configurado"
        echo "✅ Workflow listo para ejecución"
        echo ""
        echo "📋 PRÓXIMOS PASOS:"
        echo "1. Ve a https://github.com/Juanpgm/proyectos_cali_alcaldia_etl"
        echo "2. Settings → Secrets and variables → Actions"
        echo "3. Agregar secret: RAILWAY_DATABASE_URL"
        echo "4. Actions → ETL Data Processing Automation → Run workflow"
        echo ""
        echo "🚀 ¡El ETL se ejecutará automáticamente diariamente!"
        
    else
        echo "❌ Error al subir a GitHub"
        echo "Verifica tu conexión y permisos del repositorio"
    fi
else
    echo "❌ Error al crear commit"
fi