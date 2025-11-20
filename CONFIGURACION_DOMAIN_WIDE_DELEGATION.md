# Configuración de Domain-Wide Delegation para Google Drive

## ¿Qué es Domain-Wide Delegation?

Domain-Wide Delegation permite que un Service Account actúe en nombre de usuarios de tu organización de Google Workspace, permitiendo acceso automático a Google Drive sin necesidad de OAuth2 interactivo.

---

## Requisitos Previos

✅ **NECESITAS:**

- Cuenta de **Google Workspace** (no Gmail personal)
- Acceso de **Super Admin** en Google Workspace
- Service Account ya creado: `sheets-etl-service@dev-test-e778d.iam.gserviceaccount.com`

❌ **NO FUNCIONA CON:**

- Cuentas de Gmail personales (@gmail.com)
- Cuentas sin privilegios de administrador

---

## Paso 1: Habilitar Domain-Wide Delegation en Google Cloud

### 1.1 Ve a Google Cloud Console

```
https://console.cloud.google.com/
```

### 1.2 Selecciona tu proyecto

- Proyecto: `dev-test-e778d`

### 1.3 Ve a IAM & Admin → Service Accounts

```
https://console.cloud.google.com/iam-admin/serviceaccounts?project=dev-test-e778d
```

### 1.4 Encuentra tu Service Account

- Email: `sheets-etl-service@dev-test-e778d.iam.gserviceaccount.com`
- Clic en el email para ver detalles

### 1.5 Habilita Domain-Wide Delegation

1. Ve a la pestaña **"DETAILS"**
2. Busca la sección **"Domain-wide delegation"**
3. Clic en **"ENABLE DOMAIN-WIDE DELEGATION"**
4. **Product name**: `Cali ETL Service`
5. Clic en **"SAVE"**

### 1.6 Copia el Client ID

- Después de habilitar, aparecerá un **"Client ID"** (número largo)
- **COPIA ESTE NÚMERO** (lo necesitarás en el Paso 2)
- Ejemplo: `123456789012345678901`

---

## Paso 2: Autorizar en Google Workspace Admin Console

### 2.1 Ve a Google Workspace Admin Console

```
https://admin.google.com/
```

**⚠️ IMPORTANTE:** Necesitas ser **Super Admin** para acceder.

### 2.2 Navega a Security → API Controls

1. En el menú lateral: **Security** (🔒)
2. Clic en: **API Controls**
3. Clic en: **MANAGE DOMAIN-WIDE DELEGATION**

### 2.3 Agrega un nuevo Client ID

1. Clic en **"Add new"**
2. **Client ID**: Pega el Client ID del Paso 1.6
3. **OAuth Scopes**: Agrega estos scopes (separados por comas):
   ```
   https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/drive.file
   ```

### 2.4 Autorizar

1. Clic en **"AUTHORIZE"**
2. Confirma la autorización

---

## Paso 3: Modificar el Código Python

### 3.1 Instalar biblioteca adicional (si no está instalada)

```powershell
pip install google-auth google-auth-oauthlib google-auth-httplib2
```

### 3.2 Actualizar `database/config.py`

Encuentra la función `get_drive_service()` y modifica para usar delegación:

```python
def get_drive_service(user_email: str = None):
    """
    Obtiene servicio autenticado de Google Drive con Domain-Wide Delegation

    Args:
        user_email: Email del usuario a impersonar (debe ser de tu dominio)
                   Ejemplo: 'admin@tu-dominio.com'
    """
    try:
        # Usar Service Account con Domain-Wide Delegation
        if SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
            from google.oauth2 import service_account

            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE,
                scopes=DRIVE_SCOPES
            )

            # Si se proporciona user_email, delegar al usuario
            if user_email:
                credentials = credentials.with_subject(user_email)
                print(f"✅ Delegando al usuario: {user_email}")

            service = build('drive', 'v3', credentials=credentials)
            print("✅ Google Drive autenticado con Domain-Wide Delegation")
            return service

        else:
            raise Exception("Service Account file not found")

    except Exception as e:
        print(f"❌ Error authenticating Drive: {e}")
        raise
```

### 3.3 Actualizar `.env.prod`

Agrega el email del usuario a impersonar:

```bash
# Email del usuario de Google Workspace para Domain-Wide Delegation
# Debe ser un usuario real de tu organización
GOOGLE_WORKSPACE_USER_EMAIL=admin@tu-dominio.com
```

### 3.4 Actualizar `scripts/run_export_to_drive.py`

```python
import os
from database.config import get_drive_service

# Obtener email del usuario para delegación
user_email = os.getenv('GOOGLE_WORKSPACE_USER_EMAIL')

# Pasar el email al servicio
drive_service = get_drive_service(user_email=user_email)
```

---

## Paso 4: Probar la Configuración

### 4.1 Actualiza `.env.prod` con tu email de Workspace

```bash
GOOGLE_WORKSPACE_USER_EMAIL=tu-email@tu-dominio-workspace.com
```

### 4.2 Ejecuta el script

```powershell
python scripts/run_export_to_drive.py
```

### 4.3 Verifica que funcione

- Deberías ver: `✅ Delegando al usuario: tu-email@tu-dominio-workspace.com`
- Los archivos deberían subirse correctamente
- Aparecerán en tu Google Drive personal como si los hubieras creado tú

---

## Troubleshooting

### Error: "Client is unauthorized to retrieve access tokens"

**Causa:** Domain-Wide Delegation no está habilitado correctamente.

**Solución:**

1. Verifica que el Client ID sea correcto en Admin Console
2. Verifica que los scopes sean exactos (sin espacios extra)
3. Espera 5-10 minutos para que los cambios se propaguen

---

### Error: "User does not have sufficient permissions"

**Causa:** El usuario que intentas impersonar no tiene permisos en Drive.

**Solución:**

- Usa un email de administrador o usuario con permisos completos de Drive
- Verifica que el email esté activo en Google Workspace

---

### Error: "Invalid grant: account not found"

**Causa:** El email no pertenece a tu dominio de Google Workspace.

**Solución:**

- Usa un email del formato `usuario@tu-dominio.com`
- NO uses emails `@gmail.com` o de otros dominios

---

## Alternativa: Seguir usando --dry-run

Si **NO tienes acceso de Admin en Google Workspace**, puedes seguir usando:

```powershell
# Genera archivos localmente
python scripts/run_export_to_drive.py --dry-run --temp-dir app_outputs/excel_by_centro_gestor

# Los archivos quedan en:
# app_outputs/excel_by_centro_gestor/
```

Luego súbelos manualmente a Google Drive.

---

## Resumen de Opciones

| Opción                     | Requiere           | Ventajas                                                | Desventajas                                          |
| -------------------------- | ------------------ | ------------------------------------------------------- | ---------------------------------------------------- |
| **Domain-Wide Delegation** | Admin de Workspace | ✅ Totalmente automático<br>✅ No requiere Shared Drive | ❌ Necesita Super Admin<br>❌ Configuración compleja |
| **Shared Drive**           | Cuenta Workspace   | ✅ Más simple<br>✅ Sin Admin                           | ❌ Necesita Workspace<br>❌ Archivos en Shared Drive |
| **--dry-run + Manual**     | Nada               | ✅ Funciona siempre<br>✅ Sin configuración             | ❌ Manual<br>❌ No automático                        |
| **OAuth2**                 | Gmail personal     | ✅ Funciona con Gmail                                   | ❌ Requiere login cada vez<br>❌ No automático       |

---

## ¿Qué opción elegir?

1. **¿Eres Super Admin de Google Workspace?**

   - ✅ Sí → Usa **Domain-Wide Delegation** (esta guía)
   - ❌ No → Continúa leyendo

2. **¿Tienes Google Workspace (ves "Shared drives")?**

   - ✅ Sí → Usa **Shared Drive** (más simple)
   - ❌ No → Continúa leyendo

3. **¿Solo tienes Gmail personal?**
   - Usa **--dry-run + subida manual**

---

## Contacto

Si tienes dudas sobre cuál opción usar o problemas con la configuración, consulta con el administrador de Google Workspace de tu organización.
