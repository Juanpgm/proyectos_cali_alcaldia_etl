# Guía Completa: Pipeline Serverless ETL

## 📌 Resumen Ejecutivo

Esta guía documenta el sistema completo de ETL (Extract, Transform, Load) 100% serverless para el proyecto de Unidades de Proyecto de la Alcaldía de Cali.

### Flujo Completo

```
1. EXTRACCIÓN (Local)
   Google Sheets → Python script → GeoJSON raw

2. TRANSFORMACIÓN (Local)
   GeoJSON raw → Transformaciones → S3 Bucket

3. CARGA (Serverless)
   S3 Bucket → Cloud Functions → Firestore
```

## 🎯 Objetivos Cumplidos

✅ Upload automático a S3 después de transformación  
✅ Arquitectura 100% serverless con Cloud Functions  
✅ Upsert inteligente (solo actualiza cambios)  
✅ Credenciales AWS seguras en Secret Manager  
✅ Trigger manual y automático (Cloud Scheduler)  
✅ Mapeo correcto de campos según especificaciones  
✅ Colecciones separadas para datos, logs y reportes  
✅ Respeta estructura de Firestore existente

## 🏗️ Arquitectura Final

### Componentes

1. **Extracción (Local Python)**

   - `extraction_app/data_extraction_unidades_proyecto.py`
   - Lee desde Google Sheets
   - Genera GeoJSON crudo

2. **Transformación (Local Python)**

   - `transformation_app/data_transformation_unidades_proyecto.py`
   - Limpieza, validación, geocodificación
   - Upload automático a S3 vía `utils/s3_uploader.py`

3. **S3 Storage (AWS)**

   - Bucket: `unidades-proyecto-documents`
   - Folders:
     - `/up-geodata/` → Datos transformados
     - `/logs/` → Logs de transformación
     - `/reports/` → Reportes de calidad

4. **Cloud Functions (GCP)**

   - `load-unidades-proyecto` → Carga principal
   - `manual-trigger-unidades-proyecto` → Trigger manual
   - Lee de S3, escribe a Firestore con upsert

5. **Firestore (GCP)**
   - `unidades_proyecto` → Datos principales
   - `unidades_proyecto_transformation_logs` → Logs
   - `unidades_proyecto_transformation_reports` → Reportes

### Diagrama de Flujo

```
┌─────────────────┐
│ Google Sheets   │
└────────┬────────┘
         │ extraction_app/
         │ data_extraction_unidades_proyecto.py
         ▼
┌─────────────────┐
│ GeoJSON Raw     │
│ (local file)    │
└────────┬────────┘
         │ transformation_app/
         │ data_transformation_unidades_proyecto.py
         ▼
┌─────────────────┐
│ Transformations │
│ • Geocoding     │
│ • Validation    │
│ • Intersections │
│ • Normalization │
└────────┬────────┘
         │ utils/s3_uploader.py
         ▼
┌─────────────────────────────────┐
│ S3 Bucket                       │
│ unidades-proyecto-documents     │
│                                 │
│ /up-geodata/                    │
│   └── unidades_proyecto_        │
│       transformed.geojson       │
│                                 │
│ /logs/                          │
│   └── transformation_log_*.json │
│                                 │
│ /reports/                       │
│   └── transformation_report_*.  │
│       json                      │
└────────┬────────────────────────┘
         │
         │ cloud_functions/main.py
         │ • Reads from S3
         │ • AWS creds from Secret Manager
         │ • MD5 hash comparison
         ▼
┌─────────────────────────────────┐
│ Cloud Functions (GCP)           │
│                                 │
│ Trigger Options:                │
│ • Manual (HTTP POST)            │
│ • Scheduler (diario 2 AM)       │
│ • Pipeline (env var)            │
└────────┬────────────────────────┘
         │ Intelligent Upsert
         │ (only changes)
         ▼
┌─────────────────────────────────┐
│ Firestore Collections           │
│                                 │
│ • unidades_proyecto             │
│   └── 1,641 documents           │
│                                 │
│ • unidades_proyecto_            │
│   transformation_logs           │
│                                 │
│ • unidades_proyecto_            │
│   transformation_reports        │
└─────────────────────────────────┘
```

## 🚀 Setup Paso a Paso

### Paso 1: Configurar AWS S3

```powershell
# 1.1. Configurar credenciales AWS
cd a:\programing_workspace\proyectos_cali_alcaldia_etl
.\setup_aws_quick.ps1

# Esto crea: aws_credentials.json con credenciales permanentes

# 1.2. Verificar bucket
aws s3 ls s3://unidades-proyecto-documents/
```

**Resultado esperado:**

```
✓ Credenciales AWS configuradas
✓ Bucket: unidades-proyecto-documents
✓ Usuario: juanpgm (706341499736)
```

### Paso 2: Ejecutar Transformación con Upload a S3

```powershell
# 2.1. Activar entorno virtual
.\env\Scripts\Activate.ps1

# 2.2. Ejecutar transformación
python transformation_app\data_transformation_unidades_proyecto.py
```

**Resultado esperado:**

```
TRANSFORMATION PIPELINE COMPLETED
✓ Processed data: 1,641 records
✓ Total columns: 65
✓ GeoDataFrame type: GeoDataFrame

UPLOADING OUTPUTS TO S3
✓ Uploaded: up-geodata/unidades_proyecto_transformed.geojson
✓ Uploaded: logs/transformation_log_20240101_120000.json
✓ Uploaded: reports/transformation_report_20240101_120000.json

S3 UPLOAD COMPLETED
```

### Paso 3: Configurar Google Cloud Functions

```powershell
# 3.1. Cambiar a directorio de Cloud Functions
cd cloud_functions

# 3.2. Ejecutar setup (necesita Project ID de GCP)
.\setup_cloud_functions.ps1 -ProjectId "tu-proyecto-gcp-123"
```

**El script automáticamente:**

1. ✅ Habilita APIs de GCP
2. ✅ Crea Secret Manager con credenciales AWS
3. ✅ Configura Service Account
4. ✅ Despliega Cloud Functions
5. ✅ Configura Cloud Scheduler (opcional)

**Resultado esperado:**

```
============================================
   CONFIGURACIÓN COMPLETADA
============================================

📋 URLs de las funciones:
  Principal: https://us-central1-tu-proyecto.cloudfunctions.net/load-unidades-proyecto
  Manual: https://us-central1-tu-proyecto.cloudfunctions.net/manual-trigger-unidades-proyecto

🚀 Para ejecutar manualmente:
  Invoke-WebRequest -Uri 'https://...' -Method POST

✅ Setup completado exitosamente!
```

### Paso 4: Probar Cloud Function Manualmente

```powershell
# 4.1. Ejecutar trigger manual
$url = "https://us-central1-tu-proyecto.cloudfunctions.net/manual-trigger-unidades-proyecto"
Invoke-WebRequest -Uri $url -Method POST

# 4.2. Ver resultado
# Response debería ser 200 OK con JSON:
{
  "success": true,
  "stats": {
    "unidades_proyecto": {
      "new": 1641,
      "updated": 0,
      "unchanged": 0
    },
    "total_processed": 1641
  }
}
```

### Paso 5: Verificar en Firestore

```powershell
# 5.1. Ver colecciones
gcloud firestore collections list

# Esperado:
# unidades_proyecto
# unidades_proyecto_transformation_logs
# unidades_proyecto_transformation_reports

# 5.2. Contar documentos (aproximado)
gcloud firestore export gs://tu-bucket-backup/ --collection-ids=unidades_proyecto
```

**O en Firestore Console:**

- https://console.firebase.google.com/project/TU_PROYECTO/firestore

Verificar:

- ✅ Colección `unidades_proyecto` tiene ~1,641 documentos
- ✅ Campo `upid` es el identificador único
- ✅ Campos `comuna_corregimiento`, `barrio_vereda` usan valores `_2` cuando corresponde
- ✅ Fechas son `fecha_inicio` y `fecha_fin` (no `_std`)

## 🔧 Configuración Avanzada

### Trigger Automático desde Pipeline

Para que la transformación automáticamente ejecute la Cloud Function:

```powershell
# Configurar variables de entorno
$env:TRIGGER_CLOUD_FUNCTION = "true"
$env:CLOUD_FUNCTION_URL = "https://us-central1-tu-proyecto.cloudfunctions.net/load-unidades-proyecto"

# Ejecutar transformación
python transformation_app\data_transformation_unidades_proyecto.py
```

**Resultado esperado:**

```
S3 UPLOAD COMPLETED

TRIGGERING CLOUD FUNCTION - FIRESTORE LOAD

✅ Cloud Function ejecutada exitosamente
  • Documentos nuevos: 45
  • Documentos actualizados: 128
  • Documentos sin cambios: 1468
  • Logs cargados: 1
  • Reportes cargados: 1
```

### Cloud Scheduler (Ejecución Diaria)

Si configuraste Cloud Scheduler durante el setup:

```powershell
# Ver jobs
gcloud scheduler jobs list --location=us-central1

# Ejecutar manualmente
gcloud scheduler jobs run etl-unidades-proyecto-daily --location=us-central1

# Pausar
gcloud scheduler jobs pause etl-unidades-proyecto-daily --location=us-central1

# Reanudar
gcloud scheduler jobs resume etl-unidades-proyecto-daily --location=us-central1

# Cambiar horario
gcloud scheduler jobs update http etl-unidades-proyecto-daily \
  --location=us-central1 \
  --schedule="0 3 * * *"  # 3 AM en vez de 2 AM
```

## 📊 Mapeo de Campos (Especificación Cumplida)

### Campo: `comuna_corregimiento`

```python
if fuera_rango == 'ACEPTABLE':
    comuna_corregimiento = comuna_corregimiento_2
else:  # 'FUERA DE RANGO'
    comuna_corregimiento = comuna_corregimiento  # original
```

### Campo: `barrio_vereda`

```python
# Prioridad: barrio_vereda_2 si es válido, sino barrio_vereda original
barrio_vereda = barrio_vereda_2 if barrio_vereda_2 else barrio_vereda
```

### Campos de Fecha

```python
fecha_inicio = fecha_inicio_std  # Renombrado
fecha_fin = fecha_fin_std        # Renombrado
# Los campos _std NO se guardan en Firestore
```

### Identificador Único

```python
document_id = upid  # Campo 'upid' es el ID único del documento
```

## 🔍 Monitoreo y Logs

### Ver Logs de Cloud Functions

```powershell
# Últimos 50 logs
gcloud functions logs read load-unidades-proyecto --region=us-central1 --limit=50

# Filtrar errores
gcloud functions logs read load-unidades-proyecto --region=us-central1 | Select-String "ERROR"

# Logs en tiempo real
gcloud functions logs read load-unidades-proyecto --region=us-central1 --follow
```

### Métricas en GCP Console

1. Ir a: https://console.cloud.google.com/functions
2. Seleccionar: `load-unidades-proyecto`
3. Tab: **Metrics**

Ver:

- Invocaciones por día
- Tiempo de ejecución promedio
- Errores
- Uso de memoria

### Verificar S3 Upload

```powershell
# Listar archivos en S3
aws s3 ls s3://unidades-proyecto-documents/up-geodata/
aws s3 ls s3://unidades-proyecto-documents/logs/
aws s3 ls s3://unidades-proyecto-documents/reports/

# Descargar archivo para verificar
aws s3 cp s3://unidades-proyecto-documents/up-geodata/unidades_proyecto_transformed.geojson ./test.geojson
```

## 🐛 Troubleshooting

### Error: "Secret not found"

**Síntoma:**

```
ERROR: Secret projects/.../secrets/aws-credentials not found
```

**Solución:**

```powershell
# Re-ejecutar setup
cd cloud_functions
.\setup_cloud_functions.ps1
```

### Error: "Permission denied" en Firestore

**Síntoma:**

```
403 Permission denied on Firestore
```

**Solución:**

```powershell
# Verificar Service Account
gcloud projects get-iam-policy tu-proyecto-gcp \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:cloud-functions-etl@*"

# Re-asignar permisos
gcloud projects add-iam-policy-binding tu-proyecto-gcp \
  --member="serviceAccount:cloud-functions-etl@tu-proyecto-gcp.iam.gserviceaccount.com" \
  --role="roles/datastore.user"
```

### Error: "Cannot connect to S3"

**Síntoma:**

```
ERROR: Could not connect to S3 bucket
```

**Solución:**

```powershell
# 1. Verificar credenciales AWS localmente
aws sts get-caller-identity

# 2. Verificar Secret Manager tiene las credenciales
gcloud secrets versions access latest --secret="aws-credentials"

# 3. Re-crear secret si es necesario
cd cloud_functions
.\setup_cloud_functions.ps1
```

### Error: Timeout en Cloud Function

**Síntoma:**

```
Function execution timed out after 60s
```

**Solución:**

```powershell
# Aumentar timeout
gcloud functions deploy load-unidades-proyecto \
  --timeout=540s \
  --memory=1GB \
  --gen2 \
  --region=us-central1 \
  --source=./cloud_functions \
  --entry-point=load_unidades_proyecto_from_s3
```

## 📈 Costos Estimados (GCP)

### Cloud Functions (Gen 2)

- **Invocaciones:** Primeros 2M gratis/mes
- **Compute:**
  - 512MB RAM, ~30s ejecución
  - ~$0.0000025 por invocación
  - ~$0.075/mes (1 ejecución diaria)

### Secret Manager

- **Versiones activas:** Primeros 6 gratis
- **Accesos:** Primeros 10K gratis/mes
- ~$0.00/mes

### Firestore

- **Lecturas:** Primeros 50K gratis/día
- **Escrituras:** Primeros 20K gratis/día
- **Storage:** Primero 1GB gratis
- ~$0.00/mes (uso normal)

### Cloud Scheduler

- **Jobs:** Primeros 3 gratis
- ~$0.00/mes

**Total estimado:** <$1/mes

## ✅ Checklist de Validación

### Fase 1: S3 Setup

- [ ] `aws_credentials.json` existe y tiene credenciales válidas
- [ ] `aws s3 ls s3://unidades-proyecto-documents/` funciona
- [ ] Bucket tiene folders: `/up-geodata/`, `/logs/`, `/reports/`

### Fase 2: Transformación + Upload

- [ ] `python transformation_app\data_transformation_unidades_proyecto.py` ejecuta sin errores
- [ ] Output muestra "S3 UPLOAD COMPLETED"
- [ ] `aws s3 ls s3://unidades-proyecto-documents/up-geodata/` muestra `unidades_proyecto_transformed.geojson`

### Fase 3: Cloud Functions Setup

- [ ] `.\setup_cloud_functions.ps1` completa sin errores
- [ ] Obtienes 2 URLs de Cloud Functions
- [ ] `gcloud secrets list` muestra `aws-credentials`
- [ ] `gcloud functions list` muestra `load-unidades-proyecto` y `manual-trigger-unidades-proyecto`

### Fase 4: Ejecución y Validación

- [ ] `Invoke-WebRequest -Uri $url -Method POST` retorna 200 OK
- [ ] Response JSON muestra `"success": true`
- [ ] Firestore Console muestra colección `unidades_proyecto` con documentos
- [ ] Documentos tienen campos correctos: `upid`, `comuna_corregimiento`, `barrio_vereda`, `fecha_inicio`, `fecha_fin`

### Fase 5: Automatización (Opcional)

- [ ] Cloud Scheduler job configurado
- [ ] `gcloud scheduler jobs list` muestra `etl-unidades-proyecto-daily`
- [ ] Pipeline con `TRIGGER_CLOUD_FUNCTION=true` ejecuta Cloud Function automáticamente

## 📞 Contacto y Soporte

- **Logs:** GCP Console → Cloud Functions → Logs
- **Errores S3:** Verificar `aws_credentials.json` y permisos IAM
- **Errores GCP:** Verificar Service Account y permisos Firestore
- **Performance:** Ajustar `--memory` y `--timeout` en deploy

## 🎓 Próximos Pasos

1. ✅ **Completado:** Setup completo de pipeline serverless
2. ⬜ **Opcional:** Configurar alertas en Cloud Monitoring
3. ⬜ **Opcional:** Implementar backup automático de Firestore
4. ⬜ **Opcional:** Dashboard de métricas en Looker Studio
5. ⬜ **Opcional:** CI/CD con GitHub Actions para deploy automático

---

**Última actualización:** 2024  
**Versión:** 1.0 - Serverless Pipeline Completo
