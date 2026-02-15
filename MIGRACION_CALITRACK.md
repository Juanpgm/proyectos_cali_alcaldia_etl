# 🔄 MIGRACIÓN A CALITRACK-44403

Este documento describe la migración completa del proyecto ETL de la Alcaldía de Cali al nuevo proyecto Firebase `calitrack-44403` con la cuenta `juanp.gzmz@gmail.com`.

## ✅ CAMBIOS REALIZADOS

### 1. Archivos de Configuración Actualizados

- **`.env.dev`**: Ya configurado para `calitrack-44403` ✅
- **`.env.prod`**: Actualizado a `calitrack-44403` ✅
- **`.env.local`**: Actualizado a `calitrack-44403` ✅
- **`.env.example`**: Actualizado con nuevo proyecto ✅

### 2. Configuración de Google Workspace

Se agregó la variable `GOOGLE_WORKSPACE_USER_EMAIL=juanp.gzmz@gmail.com` en:

- `.env.local` ✅
- `.env.example` (como plantilla) ✅

### 3. Código Base Actualizado

- **`database/config.py`**: Proyecto por defecto cambiado a `calitrack-44403` ✅
- **`cloud_functions/utils.py`**: Proyecto por defecto actualizado ✅
- Scripts de análisis actualizados para usar configuración centralizada ✅

### 4. Documentación Actualizada

- **GitHub Secrets Setup**: Referencias actualizadas ✅
- **Documentación de Setup**: Enlaces y comandos actualizados ✅
- **Guías de configuración**: Proyectos actualizados ✅

## 🚀 PASOS PARA COMPLETAR LA MIGRACIÓN

### 1. Autenticación en Google Cloud

```powershell
# Autenticar con la nueva cuenta
gcloud auth login juanp.gzmz@gmail.com

# Configurar proyecto por defecto
gcloud config set project calitrack-44403

# Configurar Application Default Credentials
gcloud auth application-default login --project=calitrack-44403
```

### 2. Ejecutar Script de Verificación

```powershell
# Ejecutar script de configuración
python setup_calitrack_migration.py
```

### 3. Verificar Conexión

```powershell
# Probar conexión a Firebase
python -c "from database.config import test_connection; test_connection()"
```

## 📋 VERIFICACIONES NECESARIAS

### Permisos en Firebase

- [ ] Verificar que `juanp.gzmz@gmail.com` tiene permisos de administrador en `calitrack-44403`
- [ ] Verificar acceso a Firestore
- [ ] Verificar permisos de Google Cloud Project

### Google Drive

- [ ] Compartir carpetas necesarias con `juanp.gzmz@gmail.com`
- [ ] Verificar ID de carpetas en `.env.local`
- [ ] Probar Domain-Wide Delegation (si está configurado)

### GitHub Actions

- [ ] Actualizar secrets de GitHub con credenciales del nuevo proyecto
- [ ] Verificar que workflows usan las variables correctas

## 🔧 CONFIGURACIÓN AVANZADA

### Service Account (Opcional)

Si necesitas usar Service Account en lugar de ADC:

1. Crear Service Account en `calitrack-44403`
2. Descargar JSON de credenciales
3. Guardar como `sheets-service-account.json`
4. Actualizar `SERVICE_ACCOUNT_FILE` en `.env.local`

### Environment Variables por Sistema

#### Windows PowerShell

```powershell
$env:FIREBASE_PROJECT_ID = "calitrack-44403"
$env:GOOGLE_CLOUD_PROJECT = "calitrack-44403"
$env:GOOGLE_WORKSPACE_USER_EMAIL = "juanp.gzmz@gmail.com"
```

#### Linux/Mac

```bash
export FIREBASE_PROJECT_ID="calitrack-44403"
export GOOGLE_CLOUD_PROJECT="calitrack-44403"
export GOOGLE_WORKSPACE_USER_EMAIL="juanp.gzmz@gmail.com"
```

## 📚 RECURSOS ÚTILES

- **Firebase Console**: https://console.firebase.google.com/project/calitrack-44403
- **Google Cloud Console**: https://console.cloud.google.com/firestore?project=calitrack-44403
- **Documentación ADC**: [Google Cloud Auth Guide](https://cloud.google.com/docs/authentication/application-default-credentials)

## ⚠️ NOTAS IMPORTANTES

1. **Archivos `.env*`** están en `.gitignore` - no se commitean
2. **Credenciales**: Usar ADC es más seguro que Service Account files
3. **Rollback**: Si hay problemas, revertir cambios en archivos de configuración
4. **Testing**: Probar cada pipeline antes de ejecutar en producción

## 🐛 TROUBLESHOOTING

### Error: "Project not found"

```powershell
gcloud projects list
gcloud config set project calitrack-44403
```

### Error: "Permission denied"

- Verificar permisos en Firebase Console
- Verificar que el proyecto existe
- Contactar administrador del proyecto

### Error: "ADC not configured"

```powershell
gcloud auth application-default login --project=calitrack-44403
```

---

**✅ Migración completada el**: 3 de febrero de 2026  
**🎯 Proyecto destino**: calitrack-44403  
**👤 Cuenta**: juanp.gzmz@gmail.com
