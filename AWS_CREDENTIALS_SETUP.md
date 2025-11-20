# Configuración de Credenciales AWS para S3

Este documento explica cómo configurar las credenciales de AWS para habilitar la funcionalidad de carga automática a S3.

## ⚠️ IMPORTANTE - Seguridad

**NUNCA** subas archivos de credenciales al repositorio Git. Los siguientes archivos están protegidos en `.gitignore`:

- `aws_credentials.json`
- Cualquier archivo con `*credentials*.json`
- Directorio `.aws/`

## Opción 1: Archivo de Configuración (RECOMENDADO)

### Paso 1: Crear el archivo de credenciales

Copia el archivo de ejemplo y renómbralo:

```powershell
# En PowerShell
Copy-Item aws_credentials.example.json aws_credentials.json
```

### Paso 2: Editar el archivo

Abre `aws_credentials.json` y completa tus credenciales:

```json
{
  "aws_access_key_id": "TU_ACCESS_KEY_AQUI",
  "aws_secret_access_key": "TU_SECRET_KEY_AQUI",
  "region": "us-east-1",
  "bucket_name": "unidades-proyecto-documents"
}
```

### Paso 3: Obtener credenciales AWS

1. Inicia sesión en la [Consola de AWS](https://console.aws.amazon.com/)
2. Ve a **IAM** (Identity and Access Management)
3. Selecciona **Users** > Tu usuario
4. Ve a la pestaña **Security credentials**
5. Haz clic en **Create access key**
6. Copia el **Access key ID** y **Secret access key**

### Permisos Necesarios

Tu usuario IAM necesita los siguientes permisos para el bucket `unidades-proyecto-documents`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::unidades-proyecto-documents",
        "arn:aws:s3:::unidades-proyecto-documents/*"
      ]
    }
  ]
}
```

## Opción 2: Variables de Entorno

Si prefieres usar variables de entorno en lugar de un archivo:

### PowerShell (Sesión actual)

```powershell
$env:AWS_ACCESS_KEY_ID = "TU_ACCESS_KEY_AQUI"
$env:AWS_SECRET_ACCESS_KEY = "TU_SECRET_KEY_AQUI"
$env:AWS_REGION = "us-east-1"
$env:S3_BUCKET_NAME = "unidades-proyecto-documents"
```

### PowerShell (Permanente - Usuario)

```powershell
[System.Environment]::SetEnvironmentVariable('AWS_ACCESS_KEY_ID', 'TU_ACCESS_KEY_AQUI', 'User')
[System.Environment]::SetEnvironmentVariable('AWS_SECRET_ACCESS_KEY', 'TU_SECRET_KEY_AQUI', 'User')
[System.Environment]::SetEnvironmentVariable('AWS_REGION', 'us-east-1', 'User')
[System.Environment]::SetEnvironmentVariable('S3_BUCKET_NAME', 'unidades-proyecto-documents', 'User')
```

## Estructura del Bucket S3

Los archivos se organizan en el bucket de la siguiente manera:

```
unidades-proyecto-documents/
├── logs/
│   └── transformation_metrics_YYYYMMDD_HHMMSS.json
├── reports/
│   ├── analisis_recomendaciones_YYYYMMDD_HHMMSS.json
│   └── analisis_recomendaciones_YYYYMMDD_HHMMSS.md
└── up-geodata/
    └── unidades_proyecto/
        ├── current/
        │   └── unidades_proyecto_transformed.geojson.gz
        └── archive/
            └── unidades_proyecto_transformed_YYYY-MM-DD_HHMMSS.geojson.gz
```

## Verificar Configuración

Para verificar que las credenciales están correctamente configuradas, ejecuta:

```powershell
python -c "from utils.s3_uploader import S3Uploader; uploader = S3Uploader(); print('✓ Credenciales configuradas correctamente')"
```

## Solución de Problemas

### Error: "AWS credentials not found"

**Causa**: No se encontró el archivo `aws_credentials.json` ni variables de entorno.

**Solución**: Sigue los pasos de la Opción 1 o la Opción 2.

### Error: "AWS credentials not configured"

**Causa**: El archivo `aws_credentials.json` existe pero contiene valores de ejemplo.

**Solución**: Edita el archivo y reemplaza los valores de ejemplo con tus credenciales reales.

### Error: "AccessDenied"

**Causa**: Las credenciales no tienen permisos suficientes.

**Solución**: Asegúrate de que tu usuario IAM tenga los permisos listados en la sección "Permisos Necesarios".

### Error: "NoSuchBucket"

**Causa**: El bucket no existe o el nombre es incorrecto.

**Solución**: Verifica que el bucket `unidades-proyecto-documents` exista en tu cuenta AWS y en la región correcta.

## Rotación de Credenciales

Por seguridad, se recomienda rotar las credenciales periódicamente:

1. Crea una nueva access key en AWS IAM
2. Actualiza `aws_credentials.json` con la nueva key
3. Prueba que funciona correctamente
4. Elimina la access key antigua en AWS IAM

## Recursos Adicionales

- [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
