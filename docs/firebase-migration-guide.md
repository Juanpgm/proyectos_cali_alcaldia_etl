# Guía de Migración de Firebase entre Cuentas

## 📋 Resumen

Esta guía te ayudará a migrar datos desde el proyecto **unidad-cumplimiento-aa245** hacia **calitrack-44403** (cuenta: juanp.gzmz@gmail.com).

## 🎯 Objetivo

- **Origen**: `unidad-cumplimiento-aa245` (cuenta actual)
- **Destino**: `calitrack-44403` (juanp.gzmz@gmail.com)

---

## 📝 Pasos para la Migración

### Opción 1: Usar el Script Automático (Recomendado)

#### Paso 1: Configurar las cuentas de Firebase

```powershell
# Ejecuta el script de configuración
.\setup-firebase-accounts.ps1 -Account both
```

Este script te guiará para:

1. Crear dos perfiles de gcloud (`source-account` y `target-account`)
2. Autenticarte en cada cuenta
3. Configurar ADC (Application Default Credentials) para cada proyecto

**IMPORTANTE**:

- Para `source-account`: Usa tu cuenta que tiene acceso a `unidad-cumplimiento-aa245`
- Para `target-account`: **DEBES** usar `juanp.gzmz@gmail.com`

#### Paso 2: Verificar las configuraciones

```powershell
# Ver todas las configuraciones
gcloud config configurations list

# Debería mostrar algo como:
# NAME            IS_ACTIVE  ACCOUNT                    PROJECT
# source-account  False      tu-cuenta@example.com      unidad-cumplimiento-aa245
# target-account  True       juanp.gzmz@gmail.com       calitrack-44403
```

#### Paso 3: Ejecutar migración en modo preview (DRY-RUN)

```powershell
# Esto te mostrará qué datos se migrarán SIN hacer cambios reales
python migrate_firestore.py --dry-run
```

Revisa cuidadosamente:

- ✅ Colecciones detectadas
- ✅ Número de documentos
- ✅ Que ambas conexiones funcionen

#### Paso 4: Ejecutar la migración real

```powershell
# Si el dry-run se ve bien, ejecuta la migración real
python migrate_firestore.py
```

El script te pedirá confirmación escribiendo **'SI'** antes de comenzar.

---

### Opción 2: Usar Service Account Keys (Alternativa)

Si tienes problemas con ADC o prefieres usar archivos de credenciales:

#### Paso 1: Descargar Service Account Keys

**Para el proyecto origen (unidad-cumplimiento-aa245):**

1. Ve a [Firebase Console](https://console.firebase.google.com/)
2. Selecciona `unidad-cumplimiento-aa245`
3. Configuración del proyecto → Cuentas de servicio
4. Genera nueva clave privada → Guardar como `source-credentials.json`

**Para el proyecto destino (calitrack-44403):**

1. Inicia sesión con `juanp.gzmz@gmail.com` en Firebase Console
2. Selecciona `calitrack-44403`
3. Configuración del proyecto → Cuentas de servicio
4. Genera nueva clave privada → Guardar como `target-credentials.json`

#### Paso 2: Configurar variables de entorno

```powershell
# Configurar rutas a los archivos de credenciales
$env:SOURCE_CREDENTIALS_FILE = "A:\ruta\a\source-credentials.json"
$env:TARGET_CREDENTIALS_FILE = "A:\ruta\a\target-credentials.json"
```

#### Paso 3: Ejecutar migración

```powershell
# Dry-run primero
python migrate_firestore.py --dry-run

# Si se ve bien, ejecutar migración real
python migrate_firestore.py
```

---

## 🔍 Opciones Adicionales del Script

### Migrar solo colecciones específicas

```powershell
# Migrar solo las colecciones 'usuarios' y 'proyectos'
python migrate_firestore.py --collections usuarios,proyectos
```

### Ver ayuda

```powershell
python migrate_firestore.py --help
```

---

## 📊 Qué hace el script

1. **Conexión**: Se conecta a ambos proyectos Firebase
2. **Listado**: Lista todas las colecciones en el proyecto origen
3. **Backup**: Crea backup JSON de cada documento en `migration_backups/`
4. **Migración**: Copia cada documento preservando:
   - Todos los campos y tipos de datos
   - Timestamps
   - GeoPoints
   - Referencias entre documentos
   - Subcollecciones (recursivamente)
5. **Logging**: Genera log detallado en `migration_backups/migration_log_*.json`

---

## ✅ Verificación Post-Migración

### Verificar datos migrados

```powershell
# Activar la configuración de destino
gcloud config configurations activate target-account

# Verificar colecciones en Firebase Console
# https://console.firebase.google.com/project/calitrack-44403/firestore
```

### Revisar logs de migración

Los logs se guardan en: `migration_backups/migration_log_YYYYMMDD_HHMMSS.json`

Verifica:

- `stats.documents`: Número total de documentos migrados
- `stats.errors`: Debe ser 0
- `log[]`: Lista detallada de cada operación

---

## 🚨 Troubleshooting

### Error: "Permission denied"

**Solución**: Verifica que tu cuenta tenga permisos:

```powershell
# Para proyecto origen
gcloud config configurations activate source-account
gcloud projects get-iam-policy unidad-cumplimiento-aa245

# Para proyecto destino (con juanp.gzmz@gmail.com)
gcloud config configurations activate target-account
gcloud projects get-iam-policy calitrack-44403
```

Necesitas el rol: `Firebase Admin` o `Cloud Datastore Owner`

### Error: "Project not found"

**Solución**: Verifica que el proyecto existe y tienes acceso:

```powershell
gcloud projects list --filter="projectId:calitrack-44403"
```

Si no aparece:

1. Verifica que estés autenticado con `juanp.gzmz@gmail.com`
2. Verifica que el proyecto existe en [Firebase Console](https://console.firebase.google.com/)

### Error: "Default credentials not found"

**Solución**: Configura ADC nuevamente:

```powershell
gcloud auth application-default login
```

O usa la Opción 2 (Service Account Keys)

---

## 📁 Estructura de Archivos

```
proyectos_cali_alcaldia_etl/
├── migrate_firestore.py              # Script principal de migración
├── setup-firebase-accounts.ps1       # Helper para configurar cuentas
├── migration_backups/                # Backups y logs
│   ├── collection_docid.json        # Backup de cada documento
│   └── migration_log_*.json         # Log de migración
└── docs/
    └── firebase-migration-guide.md   # Esta guía
```

---

## ⚠️ IMPORTANTE: Seguridad

1. **NO** commitees archivos `*-credentials.json` a Git
2. Los archivos están protegidos en `.gitignore`
3. Los backups en `migration_backups/` **SÍ** se commitean (no contienen credenciales)
4. Después de la migración, puedes eliminar los archivos de credenciales

---

## 🎉 ¿Todo listo?

Ejecuta estos comandos en orden:

```powershell
# 1. Configurar cuentas (solo primera vez)
.\setup-firebase-accounts.ps1 -Account both

# 2. Preview de la migración
python migrate_firestore.py --dry-run

# 3. Ejecutar migración real (si el preview se ve bien)
python migrate_firestore.py

# 4. Verificar en Firebase Console
# https://console.firebase.google.com/project/calitrack-44403/firestore
```

---

## 📞 Soporte

Si encuentras problemas:

1. Revisa los logs en `migration_backups/migration_log_*.json`
2. Verifica permisos con `gcloud projects get-iam-policy`
3. Revisa que las autenticaciones sean correctas con `gcloud auth list`
