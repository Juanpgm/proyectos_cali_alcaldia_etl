# 📦 Guía de Configuración de AWS S3 para Pipeline ETL

Esta guía te ayudará a configurar un bucket de AWS S3 para almacenar los resultados de transformación y permitir que Cloud Functions consuman los datos durante la fase de carga.

---

## 📋 Tabla de Contenidos

1. [Prerrequisitos](#prerrequisitos)
2. [Crear Bucket en S3](#crear-bucket-en-s3)
3. [Estructura de Carpetas](#estructura-de-carpetas)
4. [Configurar Políticas de Acceso](#configurar-políticas-de-acceso)
5. [Obtener Credenciales](#obtener-credenciales)
6. [Configurar Lifecycle Rules](#configurar-lifecycle-rules)
7. [Uso del Uploader en Python](#uso-del-uploader-en-python)
8. [Consumir desde Cloud Functions](#consumir-desde-cloud-functions)

---

## 🔧 Prerrequisitos

- Cuenta de AWS activa
- AWS CLI instalado (opcional): https://aws.amazon.com/cli/
- Python 3.8+ con boto3: `pip install boto3`
- Acceso IAM con permisos para crear buckets y políticas

---

## 🪣 Crear Bucket en S3

### Opción A: Console Web de AWS

1. **Ir a S3 Console**:

   - URL: https://console.aws.amazon.com/s3/
   - Click en **"Create bucket"**

2. **Configuración del Bucket**:

   ```
   Bucket name: proyectos-cali-alcaldia-etl
   AWS Region: us-east-1 (o la más cercana)
   ```

   > ⚠️ **El nombre debe ser único globalmente**

3. **Object Ownership**:

   - Seleccionar: **"ACLs disabled (recommended)"**

4. **Block Public Access**:

   - ✅ Mantener TODOS los checks activados
   - Esto previene acceso público no autorizado

5. **Bucket Versioning**:

   - ✅ **Enable** (recomendado)
   - Permite mantener historial de cambios

6. **Default encryption**:

   - ✅ **Enable**
   - Tipo: **Server-side encryption with Amazon S3 managed keys (SSE-S3)**

7. **Tags** (opcional pero recomendado):

   ```
   Key: Project          Value: proyectos-cali-alcaldia
   Key: Environment      Value: production
   Key: ManagedBy        Value: etl-pipeline
   Key: CostCenter       Value: desarrollo
   ```

8. Click **"Create bucket"**

### Opción B: AWS CLI

```bash
# 1. Instalar AWS CLI (si no lo tienes)
# Windows: https://awscli.amazonaws.com/AWSCLIV2.msi
# Mac: brew install awscli
# Linux: sudo apt-get install awscli

# 2. Configurar credenciales
aws configure
# AWS Access Key ID: [tu access key]
# AWS Secret Access Key: [tu secret key]
# Default region name: us-east-1
# Default output format: json

# 3. Crear el bucket
aws s3 mb s3://proyectos-cali-alcaldia-etl --region us-east-1

# 4. Habilitar versionado
aws s3api put-bucket-versioning \
    --bucket proyectos-cali-alcaldia-etl \
    --versioning-configuration Status=Enabled

# 5. Habilitar encriptación
aws s3api put-bucket-encryption \
    --bucket proyectos-cali-alcaldia-etl \
    --server-side-encryption-configuration '{
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "AES256"
            },
            "BucketKeyEnabled": true
        }]
    }'

# 6. Agregar tags
aws s3api put-bucket-tagging \
    --bucket proyectos-cali-alcaldia-etl \
    --tagging 'TagSet=[
        {Key=Project,Value=proyectos-cali-alcaldia},
        {Key=Environment,Value=production},
        {Key=ManagedBy,Value=etl-pipeline}
    ]'

# 7. Verificar configuración
aws s3api get-bucket-versioning --bucket proyectos-cali-alcaldia-etl
aws s3api get-bucket-encryption --bucket proyectos-cali-alcaldia-etl
```

---

## 📁 Estructura de Carpetas

Crea esta estructura en tu bucket:

```
s3://proyectos-cali-alcaldia-etl/
│
├── 📂 transformed/                    # Datos transformados listos para carga
│   ├── 📂 unidades_proyecto/
│   │   ├── 📂 current/                # Versión actual (siempre actualizada)
│   │   │   └── unidades_proyecto_transformed.geojson.gz
│   │   └── 📂 archive/                # Historial con timestamp
│   │       ├── 2025-11-16_025548_unidades_proyecto.geojson.gz
│   │       └── 2025-11-15_143022_unidades_proyecto.geojson.gz
│   │
│   ├── 📂 contratos/
│   │   ├── 📂 current/
│   │   └── 📂 archive/
│   │
│   └── 📂 procesos/
│       ├── 📂 current/
│       └── 📂 archive/
│
├── 📂 logs/                           # Logs de transformación
│   ├── 📂 2025/
│   │   └── 📂 11/
│   │       └── 📂 16/
│   │           └── transformation_metrics_20251116_025548.json.gz
│   └── 📂 archive/
│
├── 📂 reports/                        # Reportes de calidad de datos
│   ├── 📂 current/
│   │   ├── analisis_recomendaciones_latest.json
│   │   └── analisis_recomendaciones_latest.md
│   └── 📂 archive/
│       ├── analisis_recomendaciones_20251116_024112.json
│       └── analisis_recomendaciones_20251116_024112.md
│
└── 📂 temp/                           # Archivos temporales (auto-delete 7 días)
    └── 📂 processing/
```

### Características de la Estructura:

- **`transformed/current/`**: Siempre contiene la última versión de los datos
- **`transformed/archive/`**: Historial completo con timestamps
- **`logs/`**: Organizados por fecha (YYYY/MM/DD) para fácil búsqueda
- **`reports/current/`**: Últimos reportes de calidad
- **`temp/`**: Se limpia automáticamente (lifecycle rule)

---

## 🔐 Configurar Políticas de Acceso

### Paso 1: Crear Política para ETL (Write Access)

1. Ve a **IAM Console** → **Policies** → **Create policy**
2. Selecciona la pestaña **JSON**
3. Pega este contenido:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ETLWriteAccess",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetObjectVersion",
        "s3:ListBucketVersions"
      ],
      "Resource": [
        "arn:aws:s3:::proyectos-cali-alcaldia-etl/*",
        "arn:aws:s3:::proyectos-cali-alcaldia-etl"
      ]
    },
    {
      "Sid": "AllowListAllBuckets",
      "Effect": "Allow",
      "Action": ["s3:ListAllMyBuckets", "s3:GetBucketLocation"],
      "Resource": "*"
    }
  ]
}
```

4. **Nombre de la política**: `ETL-Pipeline-S3-Write-Access`
5. **Descripción**: Permite escritura completa al bucket de ETL
6. Click **Create policy**

### Paso 2: Crear Política para Cloud Functions (Read Access)

1. **IAM** → **Policies** → **Create policy** → **JSON**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudFunctionsReadAccess",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetObjectVersion"],
      "Resource": [
        "arn:aws:s3:::proyectos-cali-alcaldia-etl/transformed/*",
        "arn:aws:s3:::proyectos-cali-alcaldia-etl"
      ]
    }
  ]
}
```

2. **Nombre**: `CloudFunctions-S3-Read-Access`
3. **Descripción**: Permite solo lectura de datos transformados

### Paso 3: Asignar Políticas a Usuarios

#### Para tu Usuario de ETL:

1. **IAM** → **Users** → Selecciona tu usuario
2. **Add permissions** → **Attach policies directly**
3. Busca y selecciona: `ETL-Pipeline-S3-Write-Access`
4. Click **Add permissions**

#### Para Cloud Functions (Service Account):

1. Crea un usuario específico para Cloud Functions
2. Asigna la política: `CloudFunctions-S3-Read-Access`

---

## 🔑 Obtener Credenciales

### Paso 1: Crear Access Keys

1. **IAM Console** → **Users** → Tu usuario
2. Pestaña **Security credentials**
3. Scroll a **Access keys** → Click **Create access key**
4. **Use case**: Selecciona **"Third-party service"** o **"Application running outside AWS"**
5. Click **Next** → **Create access key**
6. **⚠️ MUY IMPORTANTE**:
   - Descarga el archivo `.csv` o copia las credenciales
   - **NO PODRÁS volver a ver el Secret Access Key**

### Paso 2: Configurar Credenciales Localmente

1. Copia el archivo de ejemplo:

```bash
cp aws_credentials.example.json aws_credentials.json
```

2. Edita `aws_credentials.json`:

```json
{
  "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
  "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "region": "us-east-1",
  "bucket_name": "proyectos-cali-alcaldia-etl"
}
```

3. **⚠️ NUNCA subas este archivo a Git** (ya está en `.gitignore`)

### Paso 3: Variables de Entorno (Alternativa)

En lugar de archivo JSON, puedes usar variables de entorno:

**Windows PowerShell**:

```powershell
$env:AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
$env:AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
$env:AWS_REGION="us-east-1"
$env:S3_BUCKET_NAME="proyectos-cali-alcaldia-etl"
```

**Linux/Mac**:

```bash
export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_REGION="us-east-1"
export S3_BUCKET_NAME="proyectos-cali-alcaldia-etl"
```

---

## ⏰ Configurar Lifecycle Rules

Las Lifecycle Rules automatizan la limpieza y archivado de datos antiguos.

### Regla 1: Limpiar Archivos Temporales

```bash
aws s3api put-bucket-lifecycle-configuration \
    --bucket proyectos-cali-alcaldia-etl \
    --lifecycle-configuration '{
        "Rules": [
            {
                "Id": "DeleteTempFilesAfter7Days",
                "Prefix": "temp/",
                "Status": "Enabled",
                "Expiration": {
                    "Days": 7
                }
            }
        ]
    }'
```

### Regla 2: Archivar Logs Antiguos a Glacier

```bash
aws s3api put-bucket-lifecycle-configuration \
    --bucket proyectos-cali-alcaldia-etl \
    --lifecycle-configuration '{
        "Rules": [
            {
                "Id": "ArchiveOldLogs",
                "Prefix": "logs/archive/",
                "Status": "Enabled",
                "Transitions": [
                    {
                        "Days": 90,
                        "StorageClass": "GLACIER_IR"
                    },
                    {
                        "Days": 180,
                        "StorageClass": "DEEP_ARCHIVE"
                    }
                ]
            }
        ]
    }'
```

### Regla 3: Limpiar Versiones Antiguas de Datos

```bash
aws s3api put-bucket-lifecycle-configuration \
    --bucket proyectos-cali-alcaldia-etl \
    --lifecycle-configuration '{
        "Rules": [
            {
                "Id": "DeleteOldArchiveVersions",
                "Prefix": "transformed/",
                "Status": "Enabled",
                "NoncurrentVersionExpiration": {
                    "NoncurrentDays": 30
                }
            }
        ]
    }'
```

### Verificar Lifecycle Rules

```bash
aws s3api get-bucket-lifecycle-configuration \
    --bucket proyectos-cali-alcaldia-etl
```

---

## 🐍 Uso del Uploader en Python

### Instalación de Dependencias

```bash
pip install boto3
```

### Uso Básico

```python
from pathlib import Path
from utils.s3_uploader import S3Uploader

# Inicializar uploader (usa aws_credentials.json)
uploader = S3Uploader()

# Subir todos los outputs del pipeline
output_dir = Path("app_outputs")
results = uploader.upload_all_outputs(output_dir)

# Ver resultados
print(results)
```

### Uso Avanzado

```python
from pathlib import Path
from utils.s3_uploader import S3Uploader

# Inicializar
uploader = S3Uploader("aws_credentials.json")

# 1. Subir solo datos transformados
geojson_path = Path("app_outputs/unidades_proyecto_transformed.geojson")
data_results = uploader.upload_transformed_data(geojson_path, archive=True)

# 2. Subir solo logs
logs_dir = Path("app_outputs/logs")
log_results = uploader.upload_logs(logs_dir)

# 3. Subir solo reportes
reports_dir = Path("app_outputs/reports")
report_results = uploader.upload_reports(reports_dir)

# 4. Subir archivo individual
custom_file = Path("my_data.json")
success = uploader.upload_file(
    custom_file,
    s3_key="custom/path/my_data.json",
    compress=True,
    metadata={
        'custom_field': 'custom_value',
        'timestamp': '2025-11-16'
    }
)
```

### Integrar con Pipeline de Transformación

Edita `transformation_app/data_transformation_unidades_proyecto.py`:

```python
from utils.s3_uploader import S3Uploader

def transform_and_save_unidades_proyecto():
    # ... transformación existente ...

    # Al final, subir a S3
    try:
        uploader = S3Uploader()
        output_dir = Path(__file__).parent.parent / 'app_outputs'
        results = uploader.upload_all_outputs(output_dir)
        print("\n✓ Datos subidos a S3 exitosamente")
    except Exception as e:
        print(f"\n⚠ Error subiendo a S3: {e}")
        # No fallar el pipeline por error de upload

    return gdf_processed
```

---

## ☁️ Consumir desde Cloud Functions

### Para Google Cloud Functions

```python
import boto3
import json
import gzip
from io import BytesIO

def load_data_from_s3(bucket_name, s3_key, aws_credentials):
    """
    Descarga y parsea datos desde S3.

    Args:
        bucket_name: Nombre del bucket
        s3_key: Ruta del objeto en S3
        aws_credentials: Dict con credenciales
    """
    # Crear cliente S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_credentials['aws_access_key_id'],
        aws_secret_access_key=aws_credentials['aws_secret_access_key'],
        region_name=aws_credentials['region']
    )

    # Descargar objeto
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    content = response['Body'].read()

    # Descomprimir si es necesario
    if s3_key.endswith('.gz'):
        content = gzip.decompress(content)

    # Parsear JSON/GeoJSON
    data = json.loads(content.decode('utf-8'))

    return data


def cloud_function_handler(request):
    """Cloud Function que consume datos de S3."""

    # Credenciales desde Secret Manager o variables de entorno
    aws_creds = {
        'aws_access_key_id': os.environ['AWS_ACCESS_KEY_ID'],
        'aws_secret_access_key': os.environ['AWS_SECRET_ACCESS_KEY'],
        'region': os.environ.get('AWS_REGION', 'us-east-1')
    }

    # Cargar datos transformados
    bucket = 'proyectos-cali-alcaldia-etl'
    key = 'transformed/unidades_proyecto/current/unidades_proyecto_transformed.geojson.gz'

    try:
        data = load_data_from_s3(bucket, key, aws_creds)

        # Procesar datos...
        # Tu lógica de carga a base de datos aquí

        return {'status': 'success', 'records_processed': len(data['features'])}

    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500
```

### Para AWS Lambda

```python
import boto3
import json
import gzip

def lambda_handler(event, context):
    """Lambda function que consume datos de S3."""

    # Cliente S3 (usa IAM role de Lambda)
    s3_client = boto3.client('s3')

    bucket = 'proyectos-cali-alcaldia-etl'
    key = 'transformed/unidades_proyecto/current/unidades_proyecto_transformed.geojson.gz'

    try:
        # Descargar
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = gzip.decompress(response['Body'].read())
        data = json.loads(content.decode('utf-8'))

        # Procesar...
        records_processed = len(data['features'])

        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'records': records_processed
            })
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
```

---

## 🔍 Monitoreo y Verificación

### Listar Archivos en S3

```bash
# Listar todo el bucket
aws s3 ls s3://proyectos-cali-alcaldia-etl/ --recursive

# Listar solo transformed/
aws s3 ls s3://proyectos-cali-alcaldia-etl/transformed/ --recursive --human-readable

# Verificar un archivo específico
aws s3 ls s3://proyectos-cali-alcaldia-etl/transformed/unidades_proyecto/current/
```

### Descargar Archivos para Verificación

```bash
# Descargar archivo comprimido
aws s3 cp s3://proyectos-cali-alcaldia-etl/transformed/unidades_proyecto/current/unidades_proyecto_transformed.geojson.gz ./temp/

# Descomprimir
gunzip ./temp/unidades_proyecto_transformed.geojson.gz

# Ver contenido
cat ./temp/unidades_proyecto_transformed.geojson | jq .
```

### Verificar Tamaño y Costos

```bash
# Ver tamaño total del bucket
aws s3 ls s3://proyectos-cali-alcaldia-etl --recursive --summarize

# Ver tamaño por carpeta
aws s3 ls s3://proyectos-cali-alcaldia-etl/transformed/ --recursive --summarize --human-readable
```

---

## 💰 Estimación de Costos

### Storage Costs (us-east-1)

- **Standard Storage**: $0.023 por GB/mes
- **Glacier Instant Retrieval**: $0.004 por GB/mes
- **Deep Archive**: $0.00099 por GB/mes

### Ejemplo para este proyecto:

```
Datos transformados comprimidos: ~1.3 MB × 30 versiones = ~40 MB
Logs: ~1 KB × 365 días = ~365 KB
Reportes: ~10 KB × 365 días = ~3.6 MB

Total Standard Storage: ~44 MB ≈ $0.001/mes
Con Glacier para logs antiguos: < $0.001/mes

Costo mensual estimado: < $0.01 USD/mes
```

### Data Transfer Costs:

- Upload a S3: **GRATIS**
- Download desde mismo region: **GRATIS**
- Download desde internet: Primeros 100 GB gratis/mes

**Costo total estimado: < $1 USD/mes** (prácticamente gratis)

---

## ✅ Checklist de Configuración

- [ ] Bucket S3 creado con nombre único
- [ ] Versionado habilitado
- [ ] Encriptación SSE-S3 activada
- [ ] Block Public Access configurado
- [ ] Estructura de carpetas creada
- [ ] Política IAM para ETL creada y asignada
- [ ] Política IAM para Cloud Functions creada
- [ ] Access Keys generadas y guardadas
- [ ] Archivo `aws_credentials.json` configurado
- [ ] `aws_credentials.json` en `.gitignore`
- [ ] Lifecycle rules configuradas
- [ ] Test de upload exitoso
- [ ] Test de download desde Cloud Function

---

## 🆘 Troubleshooting

### Error: "Access Denied"

**Causa**: Credenciales incorrectas o políticas mal configuradas

**Solución**:

```bash
# Verificar credenciales
aws sts get-caller-identity

# Verificar políticas del usuario
aws iam list-attached-user-policies --user-name TU_USUARIO

# Verificar permisos específicos
aws s3api head-bucket --bucket proyectos-cali-alcaldia-etl
```

### Error: "Bucket already exists"

**Causa**: El nombre del bucket ya está en uso (globalmente)

**Solución**: Cambia el nombre a algo más único, ej:

- `proyectos-cali-alcaldia-etl-2025`
- `tu-organizacion-proyectos-cali-etl`

### Error: "InvalidAccessKeyId"

**Causa**: Access Key incorrecta o expirada

**Solución**:

1. Regenera las Access Keys en IAM Console
2. Actualiza `aws_credentials.json`

### Upload muy lento

**Causa**: Archivos muy grandes sin comprimir

**Solución**:

```python
# Asegúrate de usar compress=True
uploader.upload_file(file_path, s3_key, compress=True)
```

---

## 📚 Recursos Adicionales

- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [S3 Pricing Calculator](https://calculator.aws/)

---

**¿Necesitas ayuda?** Consulta la documentación oficial o revisa los logs en `app_outputs/logs/`.
