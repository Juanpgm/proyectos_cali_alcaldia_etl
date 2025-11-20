# Guía Rápida: Despliegue Serverless ETL Pipeline

## 🚀 Inicio Rápido (5 minutos)

### 1. Prerrequisitos

```bash
# Verificar instalaciones
gcloud --version  # Debe estar instalado
python --version  # Python 3.11+

# Configurar proyecto
gcloud config set project YOUR_PROJECT_ID
gcloud auth login
```

### 2. Desplegar Cloud Function

**En Linux/Mac:**

```bash
cd cloud_functions
chmod +x deploy-cloud-function.sh
./deploy-cloud-function.sh YOUR_PROJECT_ID us-central1
```

**En Windows:**

```powershell
cd cloud_functions
.\deploy-cloud-function.ps1 -ProjectId "YOUR_PROJECT_ID" -Region "us-central1"
```

### 3. Configurar Ejecución Automática (cada hora)

**En Linux/Mac:**

```bash
chmod +x setup-cloud-scheduler.sh
./setup-cloud-scheduler.sh YOUR_PROJECT_ID us-central1
```

**En Windows:**

```powershell
.\setup-cloud-scheduler.ps1 -ProjectId "YOUR_PROJECT_ID" -Region "us-central1"
```

## ✅ Verificación

```bash
# Ver función desplegada
gcloud functions list --project=YOUR_PROJECT_ID

# Ver scheduler configurado
gcloud scheduler jobs list --location=us-central1 --project=YOUR_PROJECT_ID

# Probar manualmente
gcloud scheduler jobs run etl-pipeline-hourly-job \
    --location=us-central1 \
    --project=YOUR_PROJECT_ID

# Ver logs
gcloud functions logs read etl-pipeline-hourly \
    --region=us-central1 \
    --limit=50 \
    --project=YOUR_PROJECT_ID
```

## 📊 Qué hace el Pipeline

1. **Cada hora (00:00, 01:00, 02:00, ...):**

   - Extrae datos desde Google Drive
   - Transforma datos (geoespacial, validación, etc.)
   - Carga a Firebase Firestore

2. **Actualizaciones Inteligentes:**

   - Usa `upid` como identificador único
   - Solo actualiza campos que cambiaron
   - No actualiza todo siempre (eficiente)

3. **Sin Credenciales Expuestas:**
   - Usa Service Account
   - Credenciales en Secret Manager
   - No hay archivos de credenciales en el código

## 🔧 Personalización

### Cambiar frecuencia de ejecución

Editar schedule en `setup-cloud-scheduler.sh` o `setup-cloud-scheduler.ps1`:

```bash
# Cada 2 horas
--schedule="0 */2 * * *"

# Solo horario laboral (8 AM - 6 PM)
--schedule="0 8-18 * * *"

# Cada 30 minutos
--schedule="*/30 * * * *"
```

### Ajustar memoria/timeout

Editar en `deploy-cloud-function.sh` o `deploy-cloud-function.ps1`:

```bash
# Más memoria (para datasets grandes)
--memory=4096MB

# Más tiempo (para procesamiento largo)
--timeout=900s  # 15 minutos (máx Cloud Functions)
```

## 📚 Documentación Completa

Ver `SERVERLESS_DEPLOYMENT_GUIDE.md` para:

- Configuración detallada
- Troubleshooting
- Monitoreo avanzado
- Permisos y seguridad

## 🆘 Ayuda Rápida

### Error: "Permission denied"

```bash
# Dar permisos al Service Account
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:SA_EMAIL" \
    --role="roles/firebase.admin"
```

### Error: "Function not found"

```bash
# Re-desplegar función
./deploy-cloud-function.sh YOUR_PROJECT_ID
```

### Pausar ejecuciones automáticas

```bash
gcloud scheduler jobs pause etl-pipeline-hourly-job \
    --location=us-central1 \
    --project=YOUR_PROJECT_ID
```

### Reanudar ejecuciones

```bash
gcloud scheduler jobs resume etl-pipeline-hourly-job \
    --location=us-central1 \
    --project=YOUR_PROJECT_ID
```

## 🎉 ¡Listo!

Tu pipeline ETL serverless está corriendo. Revisa logs cada hora para verificar ejecuciones:

```bash
gcloud functions logs read etl-pipeline-hourly --region=us-central1 --limit=50
```
