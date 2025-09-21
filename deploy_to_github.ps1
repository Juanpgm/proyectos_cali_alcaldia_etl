# deploy_to_github.ps1
# Script PowerShell para subir la configuración de GitHub Actions

Write-Host "🚀 DESPLEGANDO CONFIGURACIÓN GITHUB ACTIONS" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green

# Verificar si estamos en un repositorio git
if (-not (Test-Path ".git")) {
    Write-Host "❌ No es un repositorio Git. Inicializando..." -ForegroundColor Red
    git init
    git remote add origin https://github.com/Juanpgm/proyectos_cali_alcaldia_etl.git
}

# Verificar status
Write-Host ""
Write-Host "📋 Estado actual del repositorio:" -ForegroundColor Cyan
git status --short

# Agregar todos los archivos
Write-Host ""
Write-Host "📦 Agregando archivos..." -ForegroundColor Yellow
git add .

# Mostrar qué se va a commitear
Write-Host ""
Write-Host "📝 Archivos a commitear:" -ForegroundColor Yellow
git diff --cached --name-only

# Crear commit
Write-Host ""
Write-Host "💾 Creando commit..." -ForegroundColor Yellow
$commitMessage = @"
feat: Configure ETL automation with GitHub Actions for Railway PostgreSQL

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

The ETL will run automatically daily and load fresh data to Railway PostgreSQL.
"@

git commit -m $commitMessage

# Verificar si el commit fue exitoso
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Commit creado exitosamente" -ForegroundColor Green
    
    # Push to GitHub
    Write-Host ""
    Write-Host "🚀 Subiendo a GitHub..." -ForegroundColor Yellow
    git push origin main
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "🎉 ¡DEPLOYMENT EXITOSO!" -ForegroundColor Green
        Write-Host "=======================" -ForegroundColor Green
        Write-Host ""
        Write-Host "✅ Código subido a GitHub" -ForegroundColor Green
        Write-Host "✅ GitHub Actions configurado" -ForegroundColor Green
        Write-Host "✅ Workflow listo para ejecución" -ForegroundColor Green
        Write-Host ""
        Write-Host "📋 PRÓXIMOS PASOS:" -ForegroundColor Cyan
        Write-Host "1. Ve a https://github.com/Juanpgm/proyectos_cali_alcaldia_etl" -ForegroundColor White
        Write-Host "2. Settings → Secrets and variables → Actions" -ForegroundColor White
        Write-Host "3. Agregar secret: RAILWAY_DATABASE_URL" -ForegroundColor White
        Write-Host "4. Actions → ETL Data Processing Automation → Run workflow" -ForegroundColor White
        Write-Host ""
        Write-Host "🚀 ¡El ETL se ejecutará automáticamente diariamente!" -ForegroundColor Green
        
    }
    else {
        Write-Host "❌ Error al subir a GitHub" -ForegroundColor Red
        Write-Host "Verifica tu conexión y permisos del repositorio" -ForegroundColor Red
    }
}
else {
    Write-Host "❌ Error al crear commit" -ForegroundColor Red
}