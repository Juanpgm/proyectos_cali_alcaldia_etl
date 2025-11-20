# 🏗️ ETL Pipeline - Proyectos Cali Alcaldía

Pipeline automatizado de **Extracción, Transformación y Carga (ETL)** para datos de unidades de proyecto de la Alcaldía de Cali. Implementa programación funcional, arquitectura 100% serverless y upsert inteligente.

## 🎯 Características Principales

- **☁️ 100% Serverless**: Cloud Functions + S3 + Firestore (sin servidores que mantener)
- **⚡ Upsert Inteligente**: Solo actualiza documentos que realmente cambiaron (MD5 hash comparison)
- **🔐 Seguridad**: Credenciales AWS en Secret Manager, Service Accounts con permisos mínimos
- **📊 Monitoreo**: Logs detallados en Cloud Functions, métricas en GCP Console
- **🎮 Trigger Flexible**: Manual (HTTP), automático (Cloud Scheduler), o desde pipeline
- **🏗️ Programación Funcional**: Código limpio, eficiente y reutilizable
- **📦 Storage en S3**: Datos transformados, logs y reportes en AWS S3
- **🔥 Firestore**: 3 colecciones (datos, logs, reportes) con upsert inteligente

## 🚀 Inicio Rápido

### 1. Configuración AWS S3

```powershell
# Configurar credenciales AWS
.\setup_aws_quick.ps1
```

### 2. Desplegar Cloud Functions (GCP)

```powershell
cd cloud_functions
.\setup_cloud_functions.ps1 -ProjectId "tu-proyecto-gcp"
```

### 3. Ejecutar Pipeline Completo

```powershell
# Activar entorno virtual
.\env\Scripts\Activate.ps1

# Opción A: Transformación + Upload S3 + Trigger Manual Firestore
python transformation_app\data_transformation_unidades_proyecto.py
Invoke-WebRequest -Uri "https://REGION-PROJECT.cloudfunctions.net/manual-trigger-unidades-proyecto" -Method POST

# Opción B: Transformación + Upload S3 + Auto-trigger Firestore
$env:TRIGGER_CLOUD_FUNCTION = "true"
$env:CLOUD_FUNCTION_URL = "https://REGION-PROJECT.cloudfunctions.net/load-unidades-proyecto"
python transformation_app\data_transformation_unidades_proyecto.py
```

### 4. Verificar Resultados

```powershell
# Verificar S3
aws s3 ls s3://unidades-proyecto-documents/up-geodata/

# Ver logs Cloud Function
gcloud functions logs read load-unidades-proyecto --region=us-central1 --limit=20

# Verificar Firestore
gcloud firestore collections list
```

**📖 Guía Completa:** [`docs/SERVERLESS_PIPELINE_GUIDE.md`](./docs/SERVERLESS_PIPELINE_GUIDE.md)  
**⚡ Quick Reference:** [`cloud_functions/QUICK_REFERENCE.md`](./cloud_functions/QUICK_REFERENCE.md)

## 📚 Documentación

### [📖 Documentación Completa](./docs/)

- **[🚀 Guía Pipeline Serverless](./docs/SERVERLESS_PIPELINE_GUIDE.md)** ← **NUEVO: Setup completo ETL serverless**
- [⚡ Quick Reference](./cloud_functions/QUICK_REFERENCE.md) - Comandos esenciales
- [☁️ Cloud Functions README](./cloud_functions/README.md) - Detalles técnicos
- [🔐 Configuración Firebase](./docs/firebase-workload-identity-setup.md)
- [📦 Setup AWS S3](./docs/S3_SETUP_GUIDE.md)

## 🏗️ Arquitectura Serverless

```
Google Sheets
    ↓ extraction_app/
    ↓ data_extraction_unidades_proyecto.py
GeoJSON Raw
    ↓ transformation_app/
    ↓ data_transformation_unidades_proyecto.py
    ↓ utils/s3_uploader.py
AWS S3 (unidades-proyecto-documents)
    ├── /up-geodata/
    ├── /logs/
    └── /reports/
        ↓ Cloud Functions (GCP)
        ↓ • Reads from S3
        ↓ • AWS creds from Secret Manager
        ↓ • MD5 hash comparison (upsert)
Firebase Firestore
    ├── unidades_proyecto
    ├── unidades_proyecto_transformation_logs
    └── unidades_proyecto_transformation_reports
```

### Estructura de Código

```
├── cloud_functions/          # ← NUEVO: Cloud Functions serverless
│   ├── main.py              # Entry points (HTTP triggers)
│   ├── utils.py             # S3Handler, FirestoreHandler, DataTransformer
│   ├── requirements.txt     # Dependencies
│   └── setup_cloud_functions.ps1  # Setup automatizado
├── transformation_app/      # Transformación + upload S3
│   └── data_transformation_unidades_proyecto.py
├── extraction_app/          # Extracción Google Sheets
│   └── data_extraction_unidades_proyecto.py
├── utils/                   # Utilidades compartidas
│   └── s3_uploader.py       # Upload a S3 después de transformación
├── docs/                    # Documentación completa
│   └── SERVERLESS_PIPELINE_GUIDE.md  # ← Guía principal
├── aws_credentials.json     # Credenciales AWS (gitignored)
└── requirements.txt         # Dependencias Python
```

## 🔧 Stack Tecnológico

### Backend & Cloud

- **Python 3.11+:** Lenguaje principal
- **Google Cloud Functions (Gen 2):** Serverless compute
- **Firebase Firestore:** NoSQL database con upsert inteligente
- **AWS S3:** Object storage para datos transformados, logs y reportes
- **GCP Secret Manager:** Almacenamiento seguro de credenciales AWS

### Librerías Python

- **geopandas:** Procesamiento geoespacial
- **boto3:** SDK AWS para S3
- **firebase-admin:** SDK Firebase para Firestore
- **pandas:** Manipulación de datos

### Seguridad

- **Secret Manager:** Credenciales AWS (sin archivos locales en Cloud Functions)
- **Service Accounts:** Permisos mínimos necesarios
- **IAM Policies:** Control de acceso granular

## 📊 Estado del Proyecto

### ✅ Completado

- ✅ Extracción desde Google Sheets
- ✅ Transformación completa con validación geoespacial
- ✅ Upload automático a S3 después de transformación
- ✅ Cloud Functions serverless con upsert inteligente
- ✅ Secret Manager para credenciales AWS
- ✅ Trigger manual y automático (Cloud Scheduler)
- ✅ 3 colecciones Firestore (datos, logs, reportes)
- ✅ Mapeo de campos según especificaciones
- ✅ Comparación MD5 para evitar escrituras innecesarias

### 📈 Datos en Producción

- **Unidades de Proyecto:** 1,641 registros geoespaciales
- **Campos:** 65 columnas (upid, comuna_corregimiento, barrio_vereda, fechas, geometría, etc.)
- **Actualización:** Diaria automática (2:00 AM) o manual vía HTTP
- **Storage:** S3 + Firestore

## 💰 Costos Estimados

### Google Cloud Platform

- **Cloud Functions:** <$1/mes (1 ejecución diaria, 512MB, ~30s)
- **Secret Manager:** Gratis (primeros 6 secrets)
- **Firestore:** Gratis (dentro de cuota gratuita)
- **Cloud Scheduler:** Gratis (primeros 3 jobs)

### AWS

- **S3 Storage:** <$0.50/mes (~500MB de datos)
- **S3 Requests:** Gratis (pocas operaciones PUT/GET)

**Total:** <$2/mes

## 🛠️ Mantenimiento

### Logs y Monitoreo

```powershell
# Ver logs Cloud Functions
gcloud functions logs read load-unidades-proyecto --region=us-central1 --limit=50

# Ver métricas en GCP Console
# https://console.cloud.google.com/functions

# Verificar S3
aws s3 ls s3://unidades-proyecto-documents/ --recursive --human-readable
```

### Actualizaciones

```powershell
# Re-deploy después de cambios en código
cd cloud_functions
.\setup_cloud_functions.ps1
```

## 🔐 Seguridad

### Credenciales

- **AWS:** Almacenadas en GCP Secret Manager (no en código)
- **GCP:** Service Account con permisos mínimos (secretAccessor, datastore.user)
- **Firestore:** Reglas de seguridad configuradas

### Best Practices

- ✅ No hay archivos de credenciales en repositorio
- ✅ `.gitignore` incluye `aws_credentials.json`
- ✅ Secret Manager con automatic replication
- ✅ Service Accounts con least privilege principle

## 🆘 Soporte

- **Configuración:** Ver `/docs/`
- **Issues:** Crear issue en GitHub
- **Contacto:** Equipo de desarrollo
