# ⚡ Quick Reference - Serverless ETL Pipeline

## 🚀 Comandos Esenciales

### Setup Inicial (Una Vez)

```powershell
# 1. Configurar AWS
.\setup_aws_quick.ps1

# 2. Desplegar Cloud Functions
cd cloud_functions
.\setup_cloud_functions.ps1 -ProjectId "tu-proyecto-gcp"
```

### Ejecución Manual Completa

```powershell
# Activar entorno
.\env\Scripts\Activate.ps1

# Ejecutar transformación + upload S3
python transformation_app\data_transformation_unidades_proyecto.py

# Trigger manual Cloud Function
$url = "https://REGION-PROJECT.cloudfunctions.net/manual-trigger-unidades-proyecto"
Invoke-WebRequest -Uri $url -Method POST
```

### Ejecución con Auto-trigger

```powershell
# Configurar variables
$env:TRIGGER_CLOUD_FUNCTION = "true"
$env:CLOUD_FUNCTION_URL = "https://REGION-PROJECT.cloudfunctions.net/load-unidades-proyecto"

# Ejecutar (hace todo automáticamente)
python transformation_app\data_transformation_unidades_proyecto.py
```

## 📊 Verificación Rápida

```powershell
# S3
aws s3 ls s3://unidades-proyecto-documents/up-geodata/

# Cloud Functions
gcloud functions list --region=us-central1

# Logs
gcloud functions logs read load-unidades-proyecto --region=us-central1 --limit=20

# Firestore
gcloud firestore collections list
```

## 🎯 URLs Importantes

```powershell
# Obtener URL de función
gcloud functions describe load-unidades-proyecto \
  --region=us-central1 \
  --gen2 \
  --format="value(serviceConfig.uri)"
```

## 🔧 Troubleshooting Express

| Error                | Comando                                                                                                     |
| -------------------- | ----------------------------------------------------------------------------------------------------------- |
| Secret not found     | `cd cloud_functions; .\setup_cloud_functions.ps1`                                                           |
| S3 access denied     | `.\setup_aws_quick.ps1`                                                                                     |
| Firestore permission | `gcloud projects add-iam-policy-binding PROJECT --member="serviceAccount:SA" --role="roles/datastore.user"` |
| Function timeout     | `gcloud functions deploy load-unidades-proyecto --timeout=540s --memory=1GB`                                |

## 📋 Estructura de Archivos

```
aws_credentials.json          # ← Credenciales AWS (gitignored)
cloud_functions/
  ├── main.py                 # ← Cloud Functions entry points
  ├── utils.py                # ← S3/Firestore/Transform handlers
  ├── requirements.txt        # ← Dependencies
  └── setup_cloud_functions.ps1  # ← Setup script
transformation_app/
  └── data_transformation_unidades_proyecto.py  # ← Main ETL
utils/
  └── s3_uploader.py          # ← Upload to S3
```

## 🎬 Flujo Completo en 3 Pasos

1. **Transformación Local** → Genera GeoJSON + sube a S3
2. **Cloud Function** → Lee de S3 + upsert a Firestore
3. **Verificación** → Firestore Console o gcloud commands

## 💡 Tips

- **Test local:** `cloud_functions\test_functions_local.ps1`
- **Ver scheduler:** `gcloud scheduler jobs list --location=us-central1`
- **Ejecutar scheduler:** `gcloud scheduler jobs run etl-unidades-proyecto-daily`
- **Pausar scheduler:** `gcloud scheduler jobs pause etl-unidades-proyecto-daily`
- **Ver secrets:** `gcloud secrets list`

## 📞 Help

- **Full Guide:** `docs\SERVERLESS_PIPELINE_GUIDE.md`
- **Cloud Functions README:** `cloud_functions\README.md`
- **GCP Console:** https://console.cloud.google.com/functions
- **Firestore Console:** https://console.firebase.google.com
