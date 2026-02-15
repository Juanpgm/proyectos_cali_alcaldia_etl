# 🔐 GitHub Actions Secrets Configuration

Este documento describe cómo configurar los secrets necesarios para que los GitHub Actions workflows funcionen correctamente.

## 📋 Secrets Requeridos

### 🔥 Firebase/Google Cloud

1. **`GOOGLE_CLOUD_SERVICE_ACCOUNT_KEY`** (Requerido)
   - **Descripción**: JSON completo de la service account de Google Cloud
   - **Formato**: JSON string completo
   - **Cómo obtenerlo**:

     ```bash
     # 1. Crear service account en Google Cloud Console
     gcloud iam service-accounts create etl-pipeline-sa \
       --display-name="ETL Pipeline Service Account"

     # 2. Asignar roles necesarios
     gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
       --member="serviceAccount:etl-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
       --role="roles/firebase.admin"

     gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
       --member="serviceAccount:etl-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
       --role="roles/datastore.user"

     # 3. Crear y descargar key
     gcloud iam service-accounts keys create key.json \
       --iam-account=etl-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com
     ```

   - **En GitHub**: Copiar todo el contenido del archivo `key.json`

2. **`FIREBASE_PROJECT_ID`** (Requerido)
   - **Descripción**: ID del proyecto Firebase/Google Cloud
   - **Formato**: String simple (ej: `mi-proyecto-id`)
   - **Ejemplo**: `calitrack-44403`

### 📊 Google Sheets

3. **`SHEETS_UNIDADES_PROYECTO_URL`** (Requerido)
   - **Descripción**: URL completa de la hoja de Google Sheets
   - **Formato**: URL completa
   - **Ejemplo**: `https://docs.google.com/spreadsheets/d/1ABC123.../edit`

4. **`SHEETS_UNIDADES_PROYECTO_WORKSHEET`** (Opcional)
   - **Descripción**: Nombre de la hoja específica dentro del documento
   - **Formato**: String simple
   - **Default**: `unidades_proyecto`

## 🛠️ Configuración Paso a Paso

### 1. Configurar Service Account de Google Cloud

```bash
# Instalar gcloud CLI si no está instalado
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud init

# Crear service account
export PROJECT_ID="tu-proyecto-id"
export SA_NAME="etl-pipeline-sa"
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam service-accounts create $SA_NAME \
  --display-name="ETL Pipeline Service Account" \
  --description="Service Account para GitHub Actions ETL Pipeline"

# Asignar roles necesarios
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/firebase.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/datastore.user"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectAdmin"

# Para Google Sheets (opcional, si no usas ADC)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/drive.readonly"

# Crear y descargar key
gcloud iam service-accounts keys create github-actions-key.json \
  --iam-account=$SA_EMAIL

# Mostrar contenido para copiar a GitHub
cat github-actions-key.json
```

### 2. Configurar Secrets en GitHub

1. Ve a tu repositorio en GitHub
2. Click en **Settings** → **Secrets and variables** → **Actions**
3. Click en **New repository secret**
4. Agrega cada secret:

```yaml
Name: GOOGLE_CLOUD_SERVICE_ACCOUNT_KEY
Value: {todo el contenido del archivo github-actions-key.json}

Name: FIREBASE_PROJECT_ID
Value: tu-proyecto-id

Name: SHEETS_UNIDADES_PROYECTO_URL
Value: https://docs.google.com/spreadsheets/d/TU_SHEET_ID/edit

Name: SHEETS_UNIDADES_PROYECTO_WORKSHEET
Value: unidades_proyecto
```

### 3. Configurar Permisos de Google Sheets

Para que la service account pueda acceder a Google Sheets:

1. Abre tu Google Sheet
2. Click en **Share** (Compartir)
3. Agrega el email de la service account: `etl-pipeline-sa@tu-proyecto-id.iam.gserviceaccount.com`
4. Asigna permisos de **Viewer** (solo lectura)

### 4. Habilitar APIs Necesarias

```bash
# Habilitar APIs necesarias en Google Cloud
gcloud services enable firestore.googleapis.com
gcloud services enable sheets.googleapis.com
gcloud services enable drive.googleapis.com
gcloud services enable iam.googleapis.com
```

## 🧪 Probar Configuración

Una vez configurados todos los secrets, puedes probar la configuración:

### Opción 1: Ejecutar Workflow Manual

1. Ve a **Actions** en tu repositorio
2. Selecciona **ETL Manual Control & Monitoring**
3. Click en **Run workflow**
4. Selecciona `test_connections` como acción
5. Click en **Run workflow**

### Opción 2: Usar GitHub CLI

```bash
# Instalar GitHub CLI
gh auth login

# Ejecutar workflow de prueba
gh workflow run "manual-control.yml" \
  --field action=test_connections \
  --field debug_mode=true
```

## 🔒 Mejores Prácticas de Seguridad

### 1. Principio de Menor Privilegio

- Solo asigna los roles mínimos necesarios
- Revisa y audita permisos regularmente
- Usa diferentes service accounts para diferentes ambientes

### 2. Rotación de Keys

```bash
# Crear nueva key
gcloud iam service-accounts keys create new-key.json \
  --iam-account=$SA_EMAIL

# Actualizar secret en GitHub
# Eliminar key antigua
gcloud iam service-accounts keys delete OLD_KEY_ID \
  --iam-account=$SA_EMAIL
```

### 3. Monitoreo

- Habilita logs de audit en Google Cloud
- Monitorea uso de la service account
- Configura alertas para accesos inusuales

### 4. Variables de Ambiente por Ambiente

Para múltiples ambientes (dev, staging, prod):

```yaml
# En tu workflow
env:
  FIREBASE_PROJECT_ID: ${{
    github.ref == 'refs/heads/main' && secrets.PROD_FIREBASE_PROJECT_ID ||
    github.ref == 'refs/heads/staging' && secrets.STAGING_FIREBASE_PROJECT_ID ||
    secrets.DEV_FIREBASE_PROJECT_ID
  }}
```

## 🐛 Troubleshooting

### Error: "Permission denied"

```bash
# Verificar roles asignados
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:$SA_EMAIL"
```

### Error: "Sheet not found"

- Verificar que la service account tiene acceso al sheet
- Verificar que la URL del sheet es correcta
- Verificar que el nombre de la worksheet es correcto

### Error: "Firebase connection failed"

- Verificar que el PROJECT_ID es correcto
- Verificar que las APIs están habilitadas
- Verificar que la service account tiene roles de Firebase

### Debug Mode

Para activar modo debug en workflows:

```yaml
env:
  SECURE_LOGGING: "false" # Activar logs detallados
```

## 📚 Referencias

- [GitHub Actions Secrets](https://docs.github.com/en/actions/reference/encrypted-secrets)
- [Google Cloud Service Accounts](https://cloud.google.com/iam/docs/service-accounts)
- [Firebase Admin SDK](https://firebase.google.com/docs/admin/setup)
- [Google Sheets API](https://developers.google.com/sheets/api)

## 🆘 Soporte

Si tienes problemas con la configuración:

1. Ejecuta el workflow `manual-control.yml` con `action=test_connections` y `debug_mode=true`
2. Revisa los logs del workflow
3. Verifica que todos los secrets estén configurados correctamente
4. Asegúrate de que las APIs necesarias estén habilitadas

---

**⚠️ IMPORTANTE**: Nunca expongas las service account keys en el código fuente. Siempre usa GitHub Secrets.
