# Export Unidades Proyecto to Drive by Centro Gestor

Este módulo exporta datos de la colección Firebase "unidades_proyecto" agrupados por centro gestor, creando archivos Excel individuales y subiéndolos automáticamente a Google Drive.

## 📋 Descripción

El script `export_to_drive_by_centro_gestor.py` realiza las siguientes operaciones:

1. **Lee datos de Firebase**: Obtiene todos los documentos de la colección "unidades_proyecto"
2. **Agrupa por centro gestor**: Organiza los registros según el campo "nombre_centro_gestor"
3. **Crea archivos Excel**: Genera un archivo `.xlsx` para cada centro gestor
4. **Sube a Google Drive**: Carga automáticamente los archivos a la carpeta especificada

## 🔧 Requisitos Previos

### 1. Dependencias Python

Asegúrate de tener instaladas las siguientes librerías:

```bash
pip install pandas openpyxl google-api-python-client google-auth firebase-admin
```

O instala desde `requirements.txt`:

```bash
pip install -r requirements.txt
```

### 2. Configuración de Firebase

El script utiliza las credenciales de Firebase configuradas en el proyecto. Asegúrate de que:

- El archivo de credenciales de Firebase esté configurado correctamente
- La variable de entorno `SERVICE_ACCOUNT_FILE` apunte al archivo de credenciales (opcional)
- O que `Application Default Credentials` esté configurado

### 3. Configuración de Google Drive

El script necesita acceso a Google Drive. Puedes autenticarte de dos formas:

#### Opción A: Service Account (Recomendado para producción)

1. Crea una Service Account en Google Cloud Console
2. Descarga el archivo JSON de credenciales
3. Configura la variable de entorno en `.env`:

```bash
SERVICE_ACCOUNT_FILE=/ruta/a/service-account.json
```

4. Comparte la carpeta de Drive con el email de la Service Account

#### Opción B: Application Default Credentials

```bash
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/drive.file
```

## 📂 Carpeta de Destino en Google Drive

Los archivos se subirán a la siguiente carpeta de Google Drive:

```
https://drive.google.com/drive/folders/1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-
```

**ID de la carpeta**: `1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-`

Asegúrate de tener permisos de escritura en esta carpeta.

## 🚀 Uso

### Ejecución Básica

```bash
python utils/export_to_drive_by_centro_gestor.py
```

### Desde otro script

```python
from utils.export_to_drive_by_centro_gestor import export_and_upload_by_centro_gestor

# Ejecutar exportación
results = export_and_upload_by_centro_gestor(
    collection_name="unidades_proyecto",
    drive_folder_id="1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-",
    temp_dir="app_outputs/excel_by_centro_gestor"  # Opcional: guardar localmente
)

# Verificar resultados
if results['success']:
    print(f"✅ Exportados {results['files_uploaded']} archivos")
else:
    print(f"❌ Error: {results['errors']}")
```

## 📊 Formato de Salida

### Nombre de Archivos

Los archivos Excel se nombran según el centro gestor:

```
nombre_centro_gestor.xlsx
```

Caracteres especiales son reemplazados por guiones bajos (`_`) para compatibilidad.

### Contenido de los Archivos

Cada archivo Excel contiene:

- **Hoja 1**: Nombre del centro gestor (truncado a 31 caracteres)
- **Columnas**: Todas las columnas de la colección, excepto `geometry`
- **Registros**: Todos los documentos correspondientes al centro gestor

### Ejemplo de Archivos Generados

```
Secretaria_de_Salud_Publica.xlsx
Secretaria_de_Infraestructura_y_Valoracion.xlsx
Secretaria_de_Educacion.xlsx
...
```

## 📦 Resultados

Al finalizar, el script mostrará un resumen:

```
================================================================================
EXPORT RESULTS SUMMARY
================================================================================

📊 Data Processing:
   Total records: 1,234
   Centro gestores: 12

📁 File Operations:
   Files created: 12
   Files uploaded: 12

================================================================================
✅ EXPORT COMPLETED SUCCESSFULLY
================================================================================
```

### Estructura del Diccionario de Resultados

```python
{
    'success': True/False,
    'total_records': 1234,
    'total_grupos': 12,
    'files_created': 12,
    'files_uploaded': 12,
    'errors': []
}
```

## 🛠️ Funciones Principales

### `fetch_unidades_proyecto_from_firebase(collection_name)`

Lee todos los documentos de Firebase y los convierte a DataFrame.

### `group_by_centro_gestor(df)`

Agrupa el DataFrame por el campo `nombre_centro_gestor`.

### `dataframe_to_excel_buffer(df, sheet_name)`

Convierte un DataFrame a un archivo Excel en memoria (BytesIO).

### `upload_excel_to_drive(excel_buffer, filename, folder_id)`

Sube un archivo Excel desde memoria a Google Drive.

### `export_and_upload_by_centro_gestor(...)`

Función principal que orquesta todo el proceso.

## 📋 Campos Incluidos en el Excel

Todos los campos de Firebase excepto:

- `geometry` (no se puede serializar a Excel)

Los campos de fecha se convierten automáticamente a formato de texto para compatibilidad.

## 🔒 Seguridad y Privacidad

- Las credenciales sensibles nunca se imprimen en logs
- Se utiliza el modo `SECURE_LOGGING` de la configuración
- Los nombres de archivos se sanitizan para evitar caracteres inválidos
- Las rutas y IDs se muestran truncados en los logs

## 🐛 Solución de Problemas

### Error: "Failed to get Firestore client"

**Solución**: Verifica que las credenciales de Firebase estén configuradas correctamente.

```bash
# Verifica las variables de entorno
echo $SERVICE_ACCOUNT_FILE

# O configura ADC
gcloud auth application-default login
```

### Error: "Failed to get Drive service"

**Solución**: Asegúrate de tener permisos de acceso a Drive.

```bash
# Autenticarse con los scopes correctos
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/drive.file
```

### Error: "Column 'nombre_centro_gestor' not found"

**Solución**: Verifica que los documentos en Firebase tengan el campo `nombre_centro_gestor`.

```python
# Verificar estructura de datos
from database.config import get_firestore_client
db = get_firestore_client()
doc = db.collection('unidades_proyecto').limit(1).get()[0]
print(doc.to_dict().keys())
```

### Archivos no aparecen en Drive

**Solución**:

1. Verifica que tienes permisos de escritura en la carpeta
2. Si usas Service Account, comparte la carpeta con el email de la SA
3. Verifica el `folder_id` en el código

## 📝 Notas Adicionales

- Los archivos se crean en memoria para optimizar el rendimiento
- Opcionalmente, puedes guardar copias locales especificando `temp_dir`
- El script maneja automáticamente centros gestores con nombres nulos o vacíos
- Los archivos duplicados en Drive **no** se sobrescriben automáticamente (se crean nuevos)

## 🔄 Automatización

Para ejecutar automáticamente el script de forma periódica:

### Linux/macOS (cron)

```bash
# Editar crontab
crontab -e

# Ejecutar diariamente a las 2 AM
0 2 * * * cd /ruta/proyecto && python utils/export_to_drive_by_centro_gestor.py >> logs/export.log 2>&1
```

### Windows (Task Scheduler)

1. Abre "Programador de tareas"
2. Crea nueva tarea
3. Trigger: Diariamente a las 2:00 AM
4. Acción: Ejecutar programa
   - Programa: `python`
   - Argumentos: `utils/export_to_drive_by_centro_gestor.py`
   - Iniciar en: `C:\ruta\al\proyecto`

## 📞 Soporte

Para problemas o preguntas, contacta al equipo de desarrollo o revisa los logs en `app_outputs/logs/`.

## 📄 Licencia

Este módulo es parte del proyecto ETL de la Alcaldía de Cali.
