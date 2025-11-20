# Cloud Functions - Pipeline ETL Serverless Completo

## 📋 Descripción

Sistema serverless 100% en Google Cloud Platform que ejecuta **cada hora** el pipeline ETL completo:

- ✅ **Extrae** datos desde Google Drive
- ✅ **Transforma** datos (procesamiento geoespacial, validación, normalización)
- ✅ **Carga** a Firebase Firestore con actualizaciones selectivas por `upid`
- ✅ Solo actualiza campos que han cambiado (no todo siempre)
- ✅ Sin credenciales expuestas (usa Secret Manager)

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                     Cloud Scheduler                              │
│                   Cron: 0 * * * * (cada hora)                   │
└────────────────────────┬────────────────────────────────────────┘
                         │ HTTP Trigger
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Cloud Function: etl_pipeline_hourly                │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  ETAPA 1: Extracción (Google Drive)                      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                         │                                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  ETAPA 2: Transformación                                 │  │
│  │  - Procesamiento geoespacial                             │  │
│  │  - Validación de coordenadas                             │  │
│  │  - Normalización de datos                                │  │
│  └──────────────────────────────────────────────────────────┘  │
│                         │                                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  ETAPA 3: Carga a Firebase                               │  │
│  │  - Actualizaciones selectivas por upid                   │  │
│  │  - Solo actualiza si cambió                              │  │
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

## 🚀 Inicio Rápido (5 minutos)

### 1. Desplegar Cloud Function

**Linux/Mac:**

```bash
cd cloud_functions
chmod +x deploy-cloud-function.sh
./deploy-cloud-function.sh YOUR_PROJECT_ID us-central1
```

**Windows:**

```powershell
cd cloud_functions
.\deploy-cloud-function.ps1 -ProjectId "YOUR_PROJECT_ID" -Region "us-central1"
```

### 2. Configurar Ejecución Automática (cada hora)

**Linux/Mac:**

```bash
chmod +x setup-cloud-scheduler.sh
./setup-cloud-scheduler.sh YOUR_PROJECT_ID us-central1
```

**Windows:**

```powershell
.\setup-cloud-scheduler.ps1 -ProjectId "YOUR_PROJECT_ID" -Region "us-central1"
```

### 3. Verificar

```bash
# Ver logs
gcloud functions logs read etl-pipeline-hourly \
    --region=us-central1 \
    --limit=50

# Ejecutar manualmente (testing)
gcloud scheduler jobs run etl-pipeline-hourly-job \
    --location=us-central1
```

## 📚 Documentación Completa

- **[QUICK_START.md](QUICK_START.md)** - Guía de inicio rápido ⚡
- **[SERVERLESS_DEPLOYMENT_GUIDE.md](SERVERLESS_DEPLOYMENT_GUIDE.md)** - Guía completa y detallada 📖

## 📝 Mapeo de Campos

### Unidades de Proyecto

| Campo Origen             | Campo Destino          | Condición                         |
| ------------------------ | ---------------------- | --------------------------------- |
| `comuna_corregimiento_2` | `comuna_corregimiento` | `fuera_rango == 'ACEPTABLE'`      |
| `comuna_corregimiento`   | `comuna_corregimiento` | `fuera_rango == 'FUERA DE RANGO'` |
| `barrio_vereda_2`        | `barrio_vereda`        | Si `barrio_vereda_2` es válido    |
| `barrio_vereda`          | `barrio_vereda`        | Fallback                          |
| `fecha_inicio_std`       | `fecha_inicio`         | Siempre                           |
| `fecha_fin_std`          | `fecha_fin`            | Siempre                           |
| `upid`                   | `upid`                 | Identificador único               |

### Geometría

La geometría GeoJSON se convierte a formato Firestore:

- **Point**: `GeoPoint(longitude, latitude)`
- **Polygon**: `{type: 'polygon', coordinates: [[[...]]]}`
- **MultiPolygon**: `{type: 'multipolygon', coordinates: [[[[...]]]]}`

## 🔧 Uso

### Ejecución Manual

```powershell
# Usando Invoke-WebRequest
$url = "https://REGION-PROJECT_ID.cloudfunctions.net/manual-trigger-unidades-proyecto"
Invoke-WebRequest -Uri $url -Method POST

# Con curl
curl -X POST https://REGION-PROJECT_ID.cloudfunctions.net/manual-trigger-unidades-proyecto
```

### Ejecución desde Pipeline Python

```python
# En pipelines/unidades_proyecto_pipeline.py
import requests

def trigger_cloud_function():
    url = "https://REGION-PROJECT_ID.cloudfunctions.net/load-unidades-proyecto"
    response = requests.post(url)

    if response.status_code == 200:
        result = response.json()
        print(f"Nuevos: {result['stats']['unidades_proyecto']['new']}")
        print(f"Actualizados: {result['stats']['unidades_proyecto']['updated']}")
        print(f"Sin cambios: {result['stats']['unidades_proyecto']['unchanged']}")
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
```

### Ejecución Automática

Cloud Scheduler ejecuta diariamente a las 2:00 AM (si fue configurado):

```powershell
# Ver jobs de scheduler
gcloud scheduler jobs list

# Ejecutar manualmente un job
gcloud scheduler jobs run etl-unidades-proyecto-daily --location=us-central1

# Pausar/reanudar
gcloud scheduler jobs pause etl-unidades-proyecto-daily --location=us-central1
gcloud scheduler jobs resume etl-unidades-proyecto-daily --location=us-central1
```

## 📊 Monitoreo

### Logs

```powershell
# Ver logs en tiempo real
gcloud functions logs read load-unidades-proyecto --region=us-central1 --limit=50

# Filtrar errores
gcloud functions logs read load-unidades-proyecto --region=us-central1 | Select-String "ERROR"
```

### Métricas

Ver en GCP Console:

- **Cloud Functions → load-unidades-proyecto → Metrics**
  - Invocaciones
  - Tiempo de ejecución
  - Errores
  - Uso de memoria

### Firestore

Verificar colecciones:

```powershell
# Con gcloud
gcloud firestore collections list

# Contar documentos (aproximado)
gcloud firestore operations list
```

## 🔐 Seguridad

### Credenciales AWS

Almacenadas en **Secret Manager** (no en código):

- Secret: `aws-credentials`
- Formato: JSON con `aws_access_key_id`, `aws_secret_access_key`, `region`
- Acceso: Solo Service Account de Cloud Functions

### Service Account

Permisos mínimos necesarios:

- `roles/secretmanager.secretAccessor`: Leer credenciales AWS
- `roles/datastore.user`: Escribir a Firestore

### Autenticación HTTP

Actualmente: `--allow-unauthenticated` para facilitar trigger manual.

**Para producción**, cambiar a autenticado:

```powershell
gcloud functions deploy load-unidades-proyecto \
  --no-allow-unauthenticated \
  ...
```

Y usar tokens de autenticación:

```powershell
$token = gcloud auth print-identity-token
Invoke-WebRequest -Uri $url -Method POST -Headers @{"Authorization"="Bearer $token"}
```

## 🐛 Troubleshooting

### Error: "Secret not found"

```powershell
# Verificar secrets
gcloud secrets list

# Re-crear secret
.\setup_cloud_functions.ps1
```

### Error: "Permission denied on Firestore"

```powershell
# Verificar permisos de Service Account
gcloud projects get-iam-policy YOUR_PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:cloud-functions-etl@*"

# Re-asignar permisos
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:cloud-functions-etl@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/datastore.user"
```

### Error: "Cannot connect to S3"

- Verificar que `aws_credentials.json` tenga credenciales válidas
- Verificar que Secret Manager tenga la última versión
- Verificar permisos IAM de usuario AWS en bucket S3

### Timeout en ejecución

Si procesa muchos datos (>1000 registros):

```powershell
# Aumentar timeout a 9 minutos
gcloud functions deploy load-unidades-proyecto \
  --timeout=540s \
  --memory=1GB \
  ...
```

## 📈 Optimizaciones

### Batch Size

Actualmente: 500 documentos por batch.

Para ajustar, editar `cloud_functions/utils.py`:

```python
# FirestoreHandler.batch_upsert()
BATCH_SIZE = 500  # Cambiar según necesidad
```

### Cache de Documentos Existentes

Evita lecturas innecesarias guardando hashes en memoria durante ejecución.

### Comparación MD5

Solo escribe a Firestore si el hash MD5 del documento cambió, ahorrando:

- Write operations (costo)
- Triggers innecesarios
- Bandwidth

## 🔄 Actualización

Para actualizar funciones después de cambios en código:

```powershell
# Re-deploy automático
.\setup_cloud_functions.ps1

# O manual
cd cloud_functions
gcloud functions deploy load-unidades-proyecto \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=. \
  --entry-point=load_unidades_proyecto_from_s3
```

## 📞 Soporte

- **Logs**: Cloud Functions Console → Logs
- **Errores**: Ver sección Troubleshooting
- **Performance**: Metrics tab en Cloud Functions Console

## 🎯 Próximos Pasos

1. ✅ Deploy inicial con `setup_cloud_functions.ps1`
2. ✅ Probar ejecución manual
3. ✅ Verificar datos en Firestore
4. ⬜ Configurar alertas en Cloud Monitoring
5. ⬜ Implementar Cloud Logging para análisis avanzado
6. ⬜ Considerar Cloud Run para workloads más pesados
