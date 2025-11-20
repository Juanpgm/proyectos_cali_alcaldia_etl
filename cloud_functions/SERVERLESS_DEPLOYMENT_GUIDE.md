# Guía de Despliegue Serverless - Pipeline ETL Unidades de Proyecto

## 📋 Tabla de Contenidos

1. [Visión General](#visión-general)
2. [Arquitectura Serverless](#arquitectura-serverless)
3. [Prerrequisitos](#prerrequisitos)
4. [Configuración de Credenciales](#configuración-de-credenciales)
5. [Despliegue de Cloud Function](#despliegue-de-cloud-function)
6. [Configuración de Cloud Scheduler](#configuración-de-cloud-scheduler)
7. [Variables de Entorno](#variables-de-entorno)
8. [Testing y Validación](#testing-y-validación)
9. [Monitoreo y Logs](#monitoreo-y-logs)
10. [Troubleshooting](#troubleshooting)

---

## 🎯 Visión General

Este sistema implementa un pipeline ETL 100% serverless que:

- ✅ Se ejecuta **cada hora** automáticamente desde medianoche (cron: `0 * * * *`)
- ✅ Extrae datos desde **Google Drive**
- ✅ Transforma datos con procesamiento geoespacial
- ✅ Carga a **Firebase Firestore** con actualizaciones selectivas
- ✅ Usa **upid** como identificador único
- ✅ Solo actualiza campos que han cambiado (no todo siempre)
- ✅ No expone credenciales (usa Secret Manager)

---

## 🏗️ Arquitectura Serverless

```
┌─────────────────────────────────────────────────────────────────┐
│                     Cloud Scheduler                              │
│                   Cron: 0 * * * * (cada hora)                   │
└────────────────────────┬────────────────────────────────────────┘
                         │ HTTP Trigger
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Cloud Function: etl_pipeline_hourly                │
│                    (Python 3.11, Gen 2)                         │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  ETAPA 1: Extracción (Google Drive)                      │  │
│  │  - Módulo: transformation_app (incluye extracción)       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                         │                                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  ETAPA 2: Transformación                                 │  │
│  │  - Procesamiento geoespacial                             │  │
│  │  - Validación de coordenadas                             │  │
│  │  - Generación de métricas                                │  │
│  └──────────────────────────────────────────────────────────┘  │
│                         │                                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  ETAPA 3: Carga a Firebase                               │  │
│  │  - Módulo: load_app/data_loading_unidades_proyecto.py   │  │
│  │  - Actualizaciones selectivas por upid                   │  │
│  │  - Comparación de campos (solo actualiza si cambió)     │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Firebase Firestore                            │
│              Colección: unidades_proyecto                       │
│           Identificador único: upid (UNP-XXXX)                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📦 Prerrequisitos

### Software Requerido

```bash
# Google Cloud SDK
gcloud --version  # Mínimo: 450.0.0

# Python
python --version  # Mínimo: 3.11

# Git
git --version
```

### Permisos Necesarios en GCP

El Service Account o usuario debe tener:

- ✅ `Cloud Functions Developer`
- ✅ `Cloud Scheduler Admin`
- ✅ `Secret Manager Secret Accessor`
- ✅ `Firebase Admin`
- ✅ `Service Account User`

---

## 🔐 Configuración de Credenciales

### 1. Service Account para Firebase

```bash
# Crear Service Account
gcloud iam service-accounts create etl-pipeline-sa \
    --display-name="ETL Pipeline Service Account" \
    --project=YOUR_PROJECT_ID

# Asignar roles
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:etl-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/firebase.admin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:etl-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/datastore.user"

# Descargar clave (solo para desarrollo local)
gcloud iam service-accounts keys create firebase-service-account.json \
    --iam-account=etl-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### 2. Configurar Acceso a Google Drive

**Opción A: Usar la misma Service Account**

1. Ir a Google Drive
2. Compartir la carpeta con el email de la Service Account
3. Dar permisos de **Viewer**

**Opción B: Usar credenciales de usuario (OAuth)**

```bash
gcloud auth application-default login \
    --scopes=https://www.googleapis.com/auth/drive.readonly
```

### 3. Guardar Credenciales en Secret Manager (Producción)

```bash
# Crear secret para Firebase
gcloud secrets create firebase-credentials \
    --data-file=firebase-service-account.json \
    --replication-policy="automatic" \
    --project=YOUR_PROJECT_ID

# Dar acceso a la Cloud Function
gcloud secrets add-iam-policy-binding firebase-credentials \
    --member="serviceAccount:etl-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
```

---

## 🚀 Despliegue de Cloud Function

### 1. Preparar el Código

```bash
cd cloud_functions

# Verificar archivos necesarios
ls -la
# Debe contener:
# - main.py
# - requirements.txt
# - utils.py (opcional si usas S3)
```

### 2. Desplegar Cloud Function (Gen 2)

```bash
gcloud functions deploy etl-pipeline-hourly \
    --gen2 \
    --runtime=python311 \
    --region=us-central1 \
    --source=. \
    --entry-point=etl_pipeline_hourly \
    --trigger-http \
    --allow-unauthenticated \
    --memory=2048MB \
    --timeout=540s \
    --max-instances=1 \
    --service-account=etl-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com \
    --set-env-vars=FIREBASE_PROJECT_ID=YOUR_PROJECT_ID,SERVICE_ACCOUNT_FILE=/tmp/firebase-creds.json \
    --project=YOUR_PROJECT_ID
```

**Parámetros Importantes:**

- `--memory=2048MB`: Memoria suficiente para procesamiento geoespacial
- `--timeout=540s`: 9 minutos (máximo para Cloud Functions)
- `--max-instances=1`: Evita ejecuciones paralelas no deseadas
- `--gen2`: Usa Cloud Functions Gen 2 (más potente)

### 3. Verificar Despliegue

```bash
# Listar funciones
gcloud functions list --project=YOUR_PROJECT_ID

# Obtener URL de la función
gcloud functions describe etl-pipeline-hourly \
    --region=us-central1 \
    --gen2 \
    --format="value(serviceConfig.uri)" \
    --project=YOUR_PROJECT_ID
```

---

## ⏰ Configuración de Cloud Scheduler

### 1. Crear Job de Scheduler

```bash
# Crear job que se ejecuta cada hora desde medianoche
gcloud scheduler jobs create http etl-pipeline-hourly-job \
    --location=us-central1 \
    --schedule="0 * * * *" \
    --time-zone="America/Bogota" \
    --uri="https://REGION-PROJECT_ID.cloudfunctions.net/etl-pipeline-hourly" \
    --http-method=POST \
    --oidc-service-account-email=etl-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com \
    --project=YOUR_PROJECT_ID
```

**Cron Schedule Explicado:**

- `0 * * * *` = Cada hora a los 0 minutos (00:00, 01:00, 02:00, ...)
- Comienza desde medianoche (00:00)
- Se ejecuta 24 veces al día

**Otras opciones de schedule:**

```bash
# Cada 2 horas
"0 */2 * * *"

# Solo durante horas laborales (8 AM - 6 PM)
"0 8-18 * * *"

# Cada 30 minutos
"*/30 * * * *"
```

### 2. Verificar Scheduler

```bash
# Listar jobs
gcloud scheduler jobs list --location=us-central1 --project=YOUR_PROJECT_ID

# Ver detalles del job
gcloud scheduler jobs describe etl-pipeline-hourly-job \
    --location=us-central1 \
    --project=YOUR_PROJECT_ID

# Ejecutar manualmente (testing)
gcloud scheduler jobs run etl-pipeline-hourly-job \
    --location=us-central1 \
    --project=YOUR_PROJECT_ID
```

---

## 🔧 Variables de Entorno

### En Cloud Function

Configurar en `gcloud functions deploy`:

```bash
--set-env-vars="\
FIREBASE_PROJECT_ID=YOUR_PROJECT_ID,\
SERVICE_ACCOUNT_FILE=/tmp/firebase-creds.json,\
DRIVE_UNIDADES_PROYECTO_FOLDER_ID=YOUR_DRIVE_FOLDER_ID,\
FIRESTORE_BATCH_SIZE=100,\
FIRESTORE_TIMEOUT=30"
```

### En Desarrollo Local

Crear archivo `.env.local`:

```bash
# Firebase
FIREBASE_PROJECT_ID=your-project-id

# Google Drive
DRIVE_UNIDADES_PROYECTO_FOLDER_ID=your-folder-id
SERVICE_ACCOUNT_FILE=firebase-service-account.json

# Firestore
FIRESTORE_BATCH_SIZE=100
FIRESTORE_TIMEOUT=30
```

---

## ✅ Testing y Validación

### 1. Test Local

```bash
# Activar entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o
venv\Scripts\activate  # Windows

# Instalar dependencias
pip install -r cloud_functions/requirements.txt

# Ejecutar función localmente
cd cloud_functions
functions-framework --target=etl_pipeline_hourly --debug
```

Hacer request HTTP:

```bash
curl -X POST http://localhost:8080
```

### 2. Test en Cloud

```bash
# Invocar Cloud Function directamente
gcloud functions call etl-pipeline-hourly \
    --region=us-central1 \
    --gen2 \
    --project=YOUR_PROJECT_ID

# Ejecutar via Scheduler
gcloud scheduler jobs run etl-pipeline-hourly-job \
    --location=us-central1 \
    --project=YOUR_PROJECT_ID
```

### 3. Validar Carga en Firebase

```bash
# Contar documentos (usando gcloud firestore)
gcloud firestore operations list --project=YOUR_PROJECT_ID

# O desde Python
python -c "
from database.config import get_firestore_client
db = get_firestore_client()
docs = list(db.collection('unidades_proyecto').stream())
print(f'Total documentos: {len(docs)}')
"
```

---

## 📊 Monitoreo y Logs

### Ver Logs de Cloud Function

```bash
# Logs en tiempo real
gcloud functions logs read etl-pipeline-hourly \
    --region=us-central1 \
    --gen2 \
    --limit=50 \
    --project=YOUR_PROJECT_ID

# Logs con filtros
gcloud logging read "resource.type=cloud_function
    AND resource.labels.function_name=etl-pipeline-hourly
    AND severity>=ERROR" \
    --limit=50 \
    --format=json \
    --project=YOUR_PROJECT_ID
```

### Dashboard en GCP Console

1. Ir a **Cloud Functions** → `etl-pipeline-hourly`
2. Tab **Logs**
3. Filtrar por:
   - Severity: Error, Warning
   - Timestamp: Últimas 24 horas

### Métricas Clave

Monitorear:

- ✅ **Invocations**: Debe ser ~24/día (cada hora)
- ✅ **Execution time**: Debe ser < 540s
- ✅ **Errors**: Debe ser 0
- ✅ **Memory usage**: No debe exceder 2GB

---

## 🔧 Troubleshooting

### Error: "Service Account doesn't have permission"

```bash
# Verificar roles
gcloud projects get-iam-policy YOUR_PROJECT_ID \
    --flatten="bindings[].members" \
    --filter="bindings.members:etl-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com"

# Agregar roles faltantes
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:etl-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/cloudfunctions.invoker"
```

### Error: "Module not found" en Cloud Function

**Causa**: Dependencias faltantes en `requirements.txt`

**Solución**:

```bash
# Generar requirements.txt actualizado
pip freeze > requirements.txt

# Re-desplegar
gcloud functions deploy etl-pipeline-hourly ...
```

### Error: "Timeout exceeded"

**Causa**: Procesamiento tarda > 540s

**Soluciones**:

1. **Aumentar memoria** (más CPU):

   ```bash
   --memory=4096MB
   ```

2. **Optimizar código**:

   - Reducir batch size
   - Procesar en paralelo
   - Usar índices en Firebase

3. **Dividir en múltiples funciones**:
   - Función 1: Extracción + Transformación
   - Función 2: Carga a Firebase

### Error: "RESOURCE_EXHAUSTED" en Firestore

**Causa**: Límites de escritura excedidos

**Solución**:

```python
# En load_app/data_loading_unidades_proyecto.py
# Aumentar delay entre batches
time.sleep(0.5)  # Aumentar de 0.1 a 0.5
```

### Scheduler no ejecuta la función

```bash
# Verificar status del job
gcloud scheduler jobs describe etl-pipeline-hourly-job \
    --location=us-central1 \
    --project=YOUR_PROJECT_ID

# Ver últimas ejecuciones
gcloud scheduler jobs list --location=us-central1 --project=YOUR_PROJECT_ID

# Habilitar job si está pausado
gcloud scheduler jobs resume etl-pipeline-hourly-job \
    --location=us-central1 \
    --project=YOUR_PROJECT_ID
```

---

## 📚 Recursos Adicionales

### Documentación Oficial

- [Cloud Functions Gen 2](https://cloud.google.com/functions/docs/2nd-gen/overview)
- [Cloud Scheduler](https://cloud.google.com/scheduler/docs)
- [Firebase Admin SDK](https://firebase.google.com/docs/admin/setup)
- [Secret Manager](https://cloud.google.com/secret-manager/docs)

### Comandos Útiles

```bash
# Pausar scheduler
gcloud scheduler jobs pause etl-pipeline-hourly-job \
    --location=us-central1 \
    --project=YOUR_PROJECT_ID

# Reanudar scheduler
gcloud scheduler jobs resume etl-pipeline-hourly-job \
    --location=us-central1 \
    --project=YOUR_PROJECT_ID

# Eliminar Cloud Function
gcloud functions delete etl-pipeline-hourly \
    --region=us-central1 \
    --gen2 \
    --project=YOUR_PROJECT_ID

# Eliminar Scheduler job
gcloud scheduler jobs delete etl-pipeline-hourly-job \
    --location=us-central1 \
    --project=YOUR_PROJECT_ID
```

---

## 🎉 Conclusión

Has configurado exitosamente un pipeline ETL serverless que:

✅ Se ejecuta automáticamente cada hora  
✅ Procesa datos desde Google Drive  
✅ Actualiza Firebase de forma selectiva  
✅ No expone credenciales  
✅ Es 100% serverless (sin servidores que mantener)

Para soporte o preguntas, consulta los logs o revisa la documentación oficial de GCP.
