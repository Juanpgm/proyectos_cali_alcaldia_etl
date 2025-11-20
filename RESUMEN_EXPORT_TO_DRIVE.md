# Resumen Ejecutivo: Sistema de Exportación a Drive por Centro Gestor

## 📋 ¿Qué se ha creado?

Se ha implementado un **sistema completo** para exportar datos de Firebase a Google Drive, agrupados automáticamente por centro gestor.

## 🎯 Funcionalidad Principal

El sistema:

1. ✅ Lee datos de la colección Firebase `unidades_proyecto`
2. ✅ Agrupa registros por `nombre_centro_gestor`
3. ✅ Crea archivos Excel (.xlsx) individuales para cada centro gestor
4. ✅ Sube automáticamente los archivos a Google Drive
5. ✅ Opcionalmente guarda copias locales como respaldo

## 📁 Archivos Creados

### 1. Módulo Principal

**📄 `utils/export_to_drive_by_centro_gestor.py`**

- Funcionalidad completa de exportación
- Conexión con Firebase y Google Drive
- Creación y subida de archivos Excel
- Manejo de errores robusto

### 2. Configuración Actualizada

**📄 `database/config.py`** (actualizado)

- Nueva función `upload_file_to_drive()` para subir archivos a Drive
- Integración con el sistema existente de autenticación

### 3. Documentación Completa

**📄 `utils/README_EXPORT_TO_DRIVE.md`**

- Guía completa de uso
- Requisitos y configuración
- Ejemplos de uso
- Solución de problemas

### 4. Script de Pruebas

**📄 `test_export_to_drive.py`**

- Pruebas sin subir a Drive
- Validación de conexiones
- Verificación de datos y agrupación
- Creación de archivos de prueba

### 5. CLI para Ejecución

**📄 `scripts/run_export_to_drive.py`**

- Interfaz de línea de comandos
- Opciones configurables
- Modo dry-run para pruebas
- Manejo de argumentos

## 🚀 Formas de Usar el Sistema

### Opción 1: Ejecución Directa (Simple)

```bash
# Ejecutar con configuración por defecto
python utils/export_to_drive_by_centro_gestor.py
```

### Opción 2: CLI con Opciones (Avanzado)

```bash
# Con todas las opciones
python scripts/run_export_to_drive.py --folder-id 1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU- --temp-dir app_outputs/backup

# Modo de prueba (sin subir a Drive)
python scripts/run_export_to_drive.py --dry-run

# Ver ayuda
python scripts/run_export_to_drive.py --help
```

### Opción 3: Desde Código Python

```python
from utils.export_to_drive_by_centro_gestor import export_and_upload_by_centro_gestor

results = export_and_upload_by_centro_gestor(
    collection_name="unidades_proyecto",
    drive_folder_id="1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-",
    temp_dir="app_outputs/excel_by_centro_gestor"
)

if results['success']:
    print(f"✅ Exportados {results['files_uploaded']} archivos")
```

### Opción 4: Pruebas sin Upload

```bash
# Ejecutar suite de pruebas completa
python test_export_to_drive.py
```

## 📊 Ejemplo de Salida

```
================================================================================
EXPORT UNIDADES PROYECTO BY CENTRO GESTOR TO DRIVE
================================================================================
Collection: unidades_proyecto
Drive Folder ID: 1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-

================================================================================
FETCHING DATA FROM FIREBASE
================================================================================
✅ Fetched 1,234 documents from Firebase
   Columns: 45

================================================================================
GROUPING DATA BY CENTRO GESTOR
================================================================================
✅ Grouped into 12 centro gestores:
   - Secretaria de Infraestructura: 456 registros
   - Secretaria de Salud Publica: 234 registros
   - Secretaria de Educacion: 178 registros
   ...

================================================================================
CREATING AND UPLOADING EXCEL FILES
================================================================================

📊 Processing: Secretaria de Infraestructura
   Records: 456
   Filename: Secretaria_de_Infraestructura.xlsx
   ✅ Uploaded: Secretaria_de_Infraestructura.xlsx
      Link: https://drive.google.com/file/d/...

[... más archivos ...]

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

## 🔧 Requisitos Técnicos

### Dependencias Python (ya instaladas en el proyecto)

- ✅ `pandas` - Manipulación de datos
- ✅ `openpyxl` - Creación de archivos Excel
- ✅ `google-api-python-client` - API de Google Drive
- ✅ `google-auth` - Autenticación Google
- ✅ `firebase-admin` - Conexión con Firebase

### Configuración Necesaria

#### 1. Firebase (ya configurado)

- ✅ Credenciales de Firebase funcionando
- ✅ Acceso a colección `unidades_proyecto`

#### 2. Google Drive (requiere configuración)

Elegir UNA de estas opciones:

**Opción A: Service Account (Recomendada)**

```bash
# 1. Descargar archivo de credenciales JSON de Google Cloud Console
# 2. Configurar en .env:
SERVICE_ACCOUNT_FILE=/ruta/a/service-account.json

# 3. Compartir la carpeta de Drive con el email de la Service Account
```

**Opción B: Application Default Credentials**

```bash
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/drive.file
```

## 🎯 Carpeta de Destino

**URL**: https://drive.google.com/drive/folders/1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-  
**Folder ID**: `1YCSnfvt2vbaDFj8kwooGgVwS9fhOJAU-`

⚠️ **IMPORTANTE**: Asegúrate de tener permisos de escritura en esta carpeta.

## 📝 Formato de Archivos Excel

Cada archivo contiene:

- **Nombre**: `[nombre_centro_gestor].xlsx`
- **Hoja**: Nombre del centro gestor (max 31 caracteres)
- **Columnas**: Todas las columnas de Firebase excepto `geometry`
- **Datos**: Todos los registros del centro gestor correspondiente

### Ejemplo de Archivos Generados

```
Secretaria_de_Infraestructura.xlsx
Secretaria_de_Salud_Publica.xlsx
Secretaria_de_Educacion.xlsx
Departamento_Administrativo_de_Planeacion.xlsx
...
```

## 🧪 Flujo de Pruebas Recomendado

### Paso 1: Verificar Conexiones

```bash
python test_export_to_drive.py
```

### Paso 2: Dry Run (sin subir a Drive)

```bash
python scripts/run_export_to_drive.py --dry-run --temp-dir test_output
```

### Paso 3: Verificar archivos locales

Revisar la carpeta `test_output/` para ver los archivos generados.

### Paso 4: Ejecución Real

```bash
python scripts/run_export_to_drive.py
```

### Paso 5: Verificar en Drive

Ir a la carpeta de Drive y verificar que los archivos se hayan subido correctamente.

## 🔒 Seguridad

- ✅ Las credenciales nunca se imprimen en logs
- ✅ Modo `SECURE_LOGGING` implementado
- ✅ Sanitización de nombres de archivos
- ✅ Validación de datos antes de exportar

## 🔄 Integración con Sistema Existente

El nuevo código se integra perfectamente con:

- ✅ Sistema de autenticación Firebase existente
- ✅ Configuración centralizada en `database/config.py`
- ✅ Estructura de directorios del proyecto
- ✅ Variables de entorno en `.env`

## 📈 Próximos Pasos Sugeridos

1. **Configurar Google Drive**

   - Elegir método de autenticación (Service Account o ADC)
   - Configurar credenciales
   - Compartir carpeta de Drive

2. **Ejecutar Pruebas**

   ```bash
   python test_export_to_drive.py
   ```

3. **Dry Run**

   ```bash
   python scripts/run_export_to_drive.py --dry-run --temp-dir test_output
   ```

4. **Verificar Resultados Locales**

   - Revisar archivos en `test_output/`
   - Verificar estructura y contenido

5. **Ejecución en Producción**

   ```bash
   python utils/export_to_drive_by_centro_gestor.py
   ```

6. **Automatización (Opcional)**
   - Configurar cron job (Linux/Mac)
   - Configurar Task Scheduler (Windows)
   - Ver ejemplos en `utils/README_EXPORT_TO_DRIVE.md`

## 📞 Soporte

Para más información, consulta:

- 📖 `utils/README_EXPORT_TO_DRIVE.md` - Documentación completa
- 🧪 `test_export_to_drive.py` - Script de pruebas
- 🎮 `scripts/run_export_to_drive.py --help` - Ayuda del CLI

## ✅ Checklist de Implementación

- [x] Módulo principal creado
- [x] Función de upload a Drive agregada a config.py
- [x] Documentación completa escrita
- [x] Script de pruebas implementado
- [x] CLI con opciones creado
- [x] Manejo de errores robusto
- [x] Integración con sistema existente
- [ ] Configurar credenciales de Google Drive (usuario)
- [ ] Ejecutar pruebas
- [ ] Ejecutar en producción

---

**Fecha de Creación**: 20 de noviembre de 2025  
**Versión**: 1.0  
**Estado**: ✅ Listo para configuración y pruebas
