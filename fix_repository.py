#!/usr/bin/env python3
"""
🔧 Script de Configuración de Repositorio
Script para resolver problemas de branch protection y configurar el repositorio correctamente.
"""

import subprocess
import sys
import json
import os
from datetime import datetime
from pathlib import Path


def run_command(command, capture_output=True, check=True):
    """Ejecuta un comando de shell de forma segura."""
    try:
        if isinstance(command, str):
            command = command.split()
        
        result = subprocess.run(
            command, 
            capture_output=capture_output, 
            text=True, 
            check=check
        )
        return result
    except subprocess.CalledProcessError as e:
        print(f"❌ Error ejecutando comando: {' '.join(command)}")
        print(f"   Código de salida: {e.returncode}")
        if e.stdout:
            print(f"   Stdout: {e.stdout}")
        if e.stderr:
            print(f"   Stderr: {e.stderr}")
        return None


def check_git_status():
    """Verifica el estado actual del repositorio Git."""
    print("🔍 Verificando estado del repositorio Git...")
    
    # Verificar si estamos en un repositorio Git
    result = run_command("git rev-parse --is-inside-work-tree", check=False)
    if not result or result.stdout.strip() != "true":
        print("❌ No estamos en un repositorio Git")
        return False
    
    # Obtener información básica
    branch_result = run_command("git branch --show-current", check=False)
    current_branch = branch_result.stdout.strip() if branch_result else "unknown"
    
    remote_result = run_command("git remote -v", check=False)
    
    status_result = run_command("git status --porcelain", check=False)
    has_changes = bool(status_result and status_result.stdout.strip())
    
    print(f"📍 Branch actual: {current_branch}")
    print(f"🔄 Cambios pendientes: {'Sí' if has_changes else 'No'}")
    
    if remote_result:
        print("🌐 Remotos configurados:")
        for line in remote_result.stdout.strip().split('\n'):
            if line:
                print(f"   {line}")
    
    return True


def create_main_branch_if_needed():
    """Crea la branch main si no existe y la configura como default."""
    print("\n🌿 Verificando branch principal...")
    
    # Verificar si main existe
    result = run_command("git show-ref --verify --quiet refs/heads/main", check=False)
    main_exists = result and result.returncode == 0
    
    # Verificar si master existe
    result = run_command("git show-ref --verify --quiet refs/heads/master", check=False)
    master_exists = result and result.returncode == 0
    
    print(f"📍 Branch 'main' existe: {'Sí' if main_exists else 'No'}")
    print(f"📍 Branch 'master' existe: {'Sí' if master_exists else 'No'}")
    
    if not main_exists and master_exists:
        print("🔄 Creando branch 'main' desde 'master'...")
        
        # Cambiar a master
        run_command("git checkout master")
        
        # Crear main desde master
        run_command("git checkout -b main")
        
        # Push main al remoto
        result = run_command("git push -u origin main", check=False)
        if result and result.returncode == 0:
            print("✅ Branch 'main' creada y pushed al remoto")
        else:
            print("⚠️ No se pudo push 'main' al remoto (puede ser normal)")
    
    elif not main_exists and not master_exists:
        print("🆕 Creando branch 'main' inicial...")
        run_command("git checkout -b main")
        
        # Asegurar que tenemos al menos un commit
        if not os.path.exists('.gitignore'):
            with open('.gitignore', 'w') as f:
                f.write("# Python\n__pycache__/\n*.pyc\n*.pyo\n*.pyd\n.Python\nenv/\nvenv/\n")
            run_command("git add .gitignore")
            run_command(['git', 'commit', '-m', 'Initial commit: Add .gitignore'])
        
        result = run_command("git push -u origin main", check=False)
        if result and result.returncode == 0:
            print("✅ Branch 'main' inicial creada")
    
    return main_exists or master_exists


def fix_branch_protection_issues():
    """Intenta resolver problemas de branch protection."""
    print("\n🛡️ Resolviendo problemas de branch protection...")
    
    current_branch_result = run_command("git branch --show-current", check=False)
    current_branch = current_branch_result.stdout.strip() if current_branch_result else "dev"
    
    print(f"📍 Branch actual: {current_branch}")
    
    # Si estamos en dev, intentar diferentes estrategias
    if current_branch == "dev":
        print("🔄 Intentando estrategias para resolver push a 'dev'...")
        
        # Estrategia 1: Pull antes de push
        print("📥 Estrategia 1: Pull antes de push...")
        result = run_command("git pull origin dev", check=False)
        if result and result.returncode == 0:
            print("✅ Pull exitoso")
            
            # Intentar push nuevamente
            result = run_command("git push origin dev", check=False)
            if result and result.returncode == 0:
                print("✅ Push exitoso después del pull")
                return True
            else:
                print("❌ Push falló después del pull")
        
        # Estrategia 2: Cambiar a main y merge
        print("🔄 Estrategia 2: Merge a main...")
        
        # Asegurar que main existe
        create_main_branch_if_needed()
        
        # Cambiar a main
        result = run_command("git checkout main", check=False)
        if result and result.returncode == 0:
            # Pull main
            run_command("git pull origin main", check=False)
            
            # Merge dev en main
            result = run_command("git merge dev", check=False)
            if result and result.returncode == 0:
                # Push main
                result = run_command("git push origin main", check=False)
                if result and result.returncode == 0:
                    print("✅ Cambios pusheados a main exitosamente")
                    
                    # Volver a dev
                    run_command("git checkout dev", check=False)
                    return True
        
        # Estrategia 3: Crear PR via GitHub CLI
        print("🔄 Estrategia 3: Verificar GitHub CLI...")
        gh_result = run_command("gh --version", check=False)
        if gh_result and gh_result.returncode == 0:
            print("✅ GitHub CLI disponible")
            print("💡 Puedes crear un PR con:")
            print("   gh pr create --base main --head dev --title 'Pipeline ETL Updates' --body 'Automated ETL pipeline updates'")
        else:
            print("⚠️ GitHub CLI no disponible")
            print("💡 Instala GitHub CLI: https://cli.github.com/")
    
    return False


def setup_gitignore():
    """Configura un .gitignore apropiado para el proyecto."""
    print("\n📝 Configurando .gitignore...")
    
    gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
pip-wheel-metadata/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# PyInstaller
*.manifest
*.spec

# Installer logs
pip-log.txt
pip-delete-this-directory.txt

# Unit test / coverage reports
htmlcov/
.tox/
.nox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
*.py,cover
.hypothesis/
.pytest_cache/

# Virtual environments
env/
venv/
ENV/
env.bak/
venv.bak/

# Environment variables
.env
.env.local
.env.development.local
.env.test.local
.env.production.local

# IDEs
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Project specific
*.log
logs/
temp/
tmp/
.temp/

# Google credentials (NEVER commit these)
*service-account*.json
*credentials*.json
*key*.json
sheets-service-account.json

# Firebase
.firebase/
firebase-debug.log
firestore-debug.log

# Data files (usually too large or sensitive)
*.csv
*.xlsx
*.xls
data/
datasets/

# Backup files
*.bak
*.backup
*.old

# Output files
transformation_app/app_outputs/**/temp_*
transformation_app/app_outputs/**/*_incremental.geojson
extraction_app/app_inputs/**/temp_*
"""
    
    with open('.gitignore', 'w', encoding='utf-8') as f:
        f.write(gitignore_content)
    
    print("✅ .gitignore configurado")
    
    # Verificar si hay archivos que deberían ser ignorados
    result = run_command("git status --porcelain", check=False)
    if result:
        ignored_patterns = [
            '__pycache__',
            '.pyc',
            'service-account',
            'credentials',
            '.env'
        ]
        
        for line in result.stdout.split('\n'):
            if line.strip():
                for pattern in ignored_patterns:
                    if pattern in line:
                        print(f"⚠️ Archivo detectado que debería ser ignorado: {line.strip()}")
                        print("   Considera removerlo del repositorio si ya está commitado")


def create_pull_request_template():
    """Crea templates para Pull Requests."""
    print("\n📋 Creando template de Pull Request...")
    
    pr_template_dir = Path('.github')
    pr_template_dir.mkdir(exist_ok=True)
    
    pr_template_content = """# 🚀 ETL Pipeline Update

## 📋 Descripción
Describe brevemente los cambios realizados en el pipeline ETL.

## 🔧 Tipo de cambio
- [ ] 🐛 Bug fix (cambio que arregla un problema)
- [ ] ✨ Nueva funcionalidad (cambio que agrega funcionalidad)
- [ ] 💥 Breaking change (cambio que rompe funcionalidad existente)
- [ ] 📚 Documentación (cambios solo en documentación)
- [ ] 🎨 Estilo (formateo, punto y coma faltantes, etc)
- [ ] ♻️ Refactoring (cambio de código que no arregla bug ni agrega funcionalidad)
- [ ] ⚡ Performance (cambio que mejora performance)
- [ ] ✅ Tests (agregar tests faltantes o corregir tests existentes)

## 🧪 Testing
- [ ] Tests locales pasaron
- [ ] Pipeline ETL probado manualmente
- [ ] Conexiones a Firebase probadas
- [ ] Conexiones a Google Sheets probadas

## 📊 Impacto en datos
- [ ] No afecta datos existentes
- [ ] Migración de datos requerida
- [ ] Backup de datos recomendado
- [ ] Cambios en estructura de datos

## 🔍 Checklist
- [ ] Código sigue las convenciones del proyecto
- [ ] Auto-review del código completado
- [ ] Comentarios agregados en partes difíciles de entender
- [ ] Documentación actualizada
- [ ] No hay secrets o credenciales hardcodeadas
- [ ] .gitignore actualizado si es necesario

## 📸 Screenshots (si aplica)
Agrega screenshots si los cambios incluyen UI/outputs visuales.

## 📝 Notas adicionales
Cualquier información adicional que los reviewers deberían saber.
"""
    
    with open('.github/pull_request_template.md', 'w', encoding='utf-8') as f:
        f.write(pr_template_content)
    
    print("✅ Template de Pull Request creado")


def setup_github_configuration():
    """Configura archivos de configuración para GitHub."""
    print("\n⚙️ Configurando archivos de GitHub...")
    
    create_pull_request_template()
    
    # Crear CODEOWNERS si no existe
    codeowners_path = Path('.github/CODEOWNERS')
    if not codeowners_path.exists():
        with open(codeowners_path, 'w') as f:
            f.write("""# Code Owners para ETL Pipeline
# Ver: https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners

# Pipeline principal
/pipelines/ @Juanpgm

# Aplicaciones ETL
/extraction_app/ @Juanpgm
/transformation_app/ @Juanpgm  
/load_app/ @Juanpgm

# Configuración de base de datos
/database/ @Juanpgm

# GitHub Actions workflows
/.github/workflows/ @Juanpgm

# Documentación
*.md @Juanpgm
""")
        print("✅ CODEOWNERS creado")
    
    # Crear issue templates
    issue_templates_dir = Path('.github/ISSUE_TEMPLATE')
    issue_templates_dir.mkdir(exist_ok=True)
    
    bug_template = """---
name: Bug Report
about: Reportar un problema en el pipeline ETL
title: '[BUG] '
labels: bug
assignees: Juanpgm

---

## Descripción del Bug
Descripción clara y concisa del problema.

## Pasos para Reproducir
1. Ve a '...'
2. Ejecuta '....'
3. Ver error

## Comportamiento Esperado
Descripción clara de lo que esperabas que pasara.

## Screenshots
Si aplica, agrega screenshots para ayudar a explicar el problema.

## Ambiente
- OS: [ej. Windows 11]
- Python Version: [ej. 3.12]
- Branch: [ej. dev]

## Logs
```
Pega logs relevantes aquí
```

## Contexto Adicional
Cualquier otra información sobre el problema.
"""
    
    with open(issue_templates_dir / 'bug_report.md', 'w', encoding='utf-8') as f:
        f.write(bug_template)
    
    feature_template = """---
name: Feature Request
about: Sugerir una nueva funcionalidad para el pipeline ETL
title: '[FEATURE] '
labels: enhancement
assignees: Juanpgm

---

## Descripción de la Funcionalidad
Descripción clara y concisa de lo que quieres que se agregue.

## Problema que Resuelve
Describe el problema que esta funcionalidad resolvería.

## Solución Propuesta
Descripción clara de lo que quieres que pase.

## Alternativas Consideradas
Describe alternativas que hayas considerado.

## Criterios de Aceptación
- [ ] Criterio 1
- [ ] Criterio 2
- [ ] Criterio 3

## Contexto Adicional
Cualquier otra información o screenshots sobre la solicitud.
"""
    
    with open(issue_templates_dir / 'feature_request.md', 'w', encoding='utf-8') as f:
        f.write(feature_template)
    
    print("✅ Templates de Issues creados")


def main():
    """Función principal del script."""
    print("🔧 SCRIPT DE CONFIGURACIÓN DE REPOSITORIO")
    print("=" * 60)
    print(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # Verificar estado del repositorio
        if not check_git_status():
            print("❌ No se pudo verificar el estado del repositorio")
            return False
        
        # Configurar .gitignore
        setup_gitignore()
        
        # Configurar archivos de GitHub
        setup_github_configuration()
        
        # Intentar resolver problemas de branch protection
        success = fix_branch_protection_issues()
        
        print("\n" + "=" * 60)
        print("📋 RESUMEN DE CONFIGURACIÓN")
        print("=" * 60)
        
        if success:
            print("✅ Problemas de push resueltos")
        else:
            print("⚠️ Problemas de push no resueltos automáticamente")
            print("\n💡 SOLUCIONES MANUALES:")
            print("1. Crear Pull Request desde dev a main:")
            print("   gh pr create --base main --head dev --title 'ETL Pipeline Updates'")
            print("\n2. O cambiar branch protection rules en GitHub:")
            print("   Settings → Branches → Edit rule for 'dev'")
            print("\n3. O push directamente a main:")
            print("   git checkout main")
            print("   git merge dev")
            print("   git push origin main")
        
        print("✅ .gitignore configurado")
        print("✅ Templates de GitHub creados")
        print("✅ Archivos de configuración listos")
        
        print("\n🚀 PRÓXIMOS PASOS:")
        print("1. Configura secrets en GitHub (ver .github/SECRETS_SETUP.md)")
        print("2. Prueba el workflow manualmente")
        print("3. Configura branch protection rules si es necesario")
        
        return True
        
    except Exception as e:
        print(f"💥 Error inesperado: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)