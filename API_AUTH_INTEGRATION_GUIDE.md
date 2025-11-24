# 🔐 Guía de Integración: Sistema de Autenticación en la API

**Proyecto**: Gestor de Proyectos Cali  
**API**: https://gestorproyectoapi-production.up.railway.app  
**Fecha**: 23 de Noviembre 2025

---

## 📋 Resumen Ejecutivo

Este documento describe **exactamente** qué cambios hacer en tu API de FastAPI (desplegada en Railway) para integrar el sistema de autenticación y autorización basado en roles que acabamos de crear.

---

## 🎯 Cambios Necesarios en la API

### **1. Copiar Módulo `auth_system/` a la API**

Copia la carpeta completa `auth_system/` que creamos a tu proyecto de API:

```
tu-api-proyecto/
├── main.py  (o como se llame tu archivo principal)
├── routers/
├── database/
├── auth_system/  ← COPIAR ESTA CARPETA COMPLETA
│   ├── __init__.py
│   ├── constants.py
│   ├── models.py
│   ├── permissions.py
│   ├── decorators.py
│   ├── middleware.py
│   └── utils.py
```

---

### **2. Modificar Archivo Principal de FastAPI (main.py o equivalente)**

En tu archivo principal donde creas la instancia de FastAPI, agrega el middleware:

#### **ANTES:**

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Gestor de Proyectos API",
    description="API para gestión de proyectos",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir routers
app.include_router(proyectos_router)
app.include_router(unidades_proyecto_router)
# etc...
```

#### **DESPUÉS:**

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from auth_system.middleware import AuthorizationMiddleware, AuditLogMiddleware  # ← NUEVO

app = FastAPI(
    title="Gestor de Proyectos API",
    description="API para gestión de proyectos",
    version="1.0.0"
)

# CORS (mantener como está)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ← NUEVO: Middleware de Autenticación
app.add_middleware(
    AuthorizationMiddleware,
    public_paths=[
        "/",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/ping",
        "/health",
        "/cors-test",
        "/test/utf8",
        "/debug/railway",
        "/metrics",
        "/auth/login",
        "/auth/register",
        "/auth/google",
        "/auth/config",
        "/auth/validate-session"
    ]
)

# ← NUEVO (OPCIONAL): Middleware de Auditoría
app.add_middleware(
    AuditLogMiddleware,
    enable_logging=True  # Cambiar a False si no quieres logs automáticos
)

# Incluir routers (mantener como está)
app.include_router(proyectos_router)
app.include_router(unidades_proyecto_router)
# etc...
```

---

### **3. Proteger Endpoints Existentes**

Para CADA endpoint que quieras proteger, agrega el decorador y la dependencia:

#### **Ejemplo: Proteger POST /proyectos-presupuestales/cargar-json**

**ANTES:**

```python
@router.post("/proyectos-presupuestales/cargar-json")
async def cargar_proyectos_presupuestales_json(
    file: UploadFile = File(...),
    update_mode: str = Form("merge")
):
    # código actual
    pass
```

**DESPUÉS:**

```python
from auth_system.decorators import require_permission, get_current_user  # ← NUEVO
from fastapi import Depends  # ← NUEVO (si no lo tienes)

@router.post("/proyectos-presupuestales/cargar-json")
@require_permission("write:proyectos")  # ← NUEVO: Define el permiso requerido
async def cargar_proyectos_presupuestales_json(
    file: UploadFile = File(...),
    update_mode: str = Form("merge"),
    current_user: dict = Depends(get_current_user)  # ← NUEVO: Inyecta usuario autenticado
):
    # código actual (puedes usar current_user['uid'] o current_user['email'])
    print(f"Cargado por: {current_user['email']}")
    pass
```

#### **Ejemplo: Proteger DELETE /unidades-proyecto/delete-by-centro-gestor**

**ANTES:**

```python
@router.delete("/unidades-proyecto/delete-by-centro-gestor")
async def delete_unidades_by_centro_gestor(
    nombre_centro_gestor: str,
    confirm: bool = False
):
    if not confirm:
        return {"message": "Confirmación requerida"}
    # código de eliminación
```

**DESPUÉS:**

```python
from auth_system.decorators import require_permission, get_current_user
from fastapi import Depends

@router.delete("/unidades-proyecto/delete-by-centro-gestor")
@require_permission("delete:proyectos")  # ← NUEVO: Solo usuarios con permiso delete
async def delete_unidades_by_centro_gestor(
    nombre_centro_gestor: str,
    confirm: bool = False,
    current_user: dict = Depends(get_current_user)  # ← NUEVO
):
    if not confirm:
        return {"message": "Confirmación requerida"}

    # Registrar quién realizó la eliminación
    print(f"Eliminación solicitada por: {current_user['email']}")

    # código de eliminación
```

#### **Ejemplo: Proteger GET con filtro por centro gestor (scope)**

Si quieres que un usuario solo vea proyectos de SU centro gestor:

```python
from auth_system.decorators import require_permission, get_current_user
from fastapi import Depends

@router.get("/proyectos-presupuestales/centro-gestor/{nombre_centro_gestor}")
@require_permission("read:proyectos:own_centro")  # ← NUEVO: Permiso con scope
async def get_proyectos_by_centro_gestor(
    nombre_centro_gestor: str,
    current_user: dict = Depends(get_current_user)
):
    # Validar que el usuario solo pueda ver su propio centro
    # (Esto lo valida automáticamente el decorador si el permiso incluye :own_centro)

    # Tu código actual
    pass
```

---

### **4. Crear Nuevos Endpoints de Administración**

Crea un nuevo archivo de router para gestión de usuarios y roles:

**Archivo: `routers/auth.py` (NUEVO)**

```python
"""
Router de Autenticación y Administración de Usuarios
"""

from fastapi import APIRouter, Depends, HTTPException
from auth_system.decorators import require_permission, require_role, get_current_user
from auth_system.models import AssignRolesRequest, GrantTemporaryPermissionRequest
from database.config import get_firestore_client
from datetime import datetime

router = APIRouter(prefix="/auth", tags=["Autenticación y Administración"])


# ========== GESTIÓN DE USUARIOS ==========

@router.get("/users")
@require_permission("manage:users")  # Solo super_admin
async def list_users(current_user: dict = Depends(get_current_user)):
    """
    Listar todos los usuarios del sistema.
    Solo accesible por super_admin.
    """
    db = get_firestore_client()
    users_ref = db.collection('users').stream()

    users = []
    for user_doc in users_ref:
        user_data = user_doc.to_dict()
        user_data['uid'] = user_doc.id
        users.append(user_data)

    return {
        "success": True,
        "count": len(users),
        "data": users
    }


@router.get("/users/{uid}")
@require_permission("manage:users")  # Solo super_admin
async def get_user_details(
    uid: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Obtener detalles de un usuario específico.
    Solo accesible por super_admin.
    """
    db = get_firestore_client()
    user_doc = db.collection('users').document(uid).get()

    if not user_doc.exists:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    user_data = user_doc.to_dict()
    user_data['uid'] = uid

    return {
        "success": True,
        "data": user_data
    }


@router.post("/users/{uid}/roles")
@require_permission("manage:users")  # Solo super_admin
async def assign_roles_to_user(
    uid: str,
    request: AssignRolesRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Asignar roles a un usuario.
    Solo accesible por super_admin.
    """
    db = get_firestore_client()
    user_ref = db.collection('users').document(uid)
    user_doc = user_ref.get()

    if not user_doc.exists:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    # Actualizar roles
    user_ref.update({
        'roles': request.roles,
        'updated_at': datetime.utcnow(),
        'updated_by': current_user['uid']
    })

    # Registrar en audit_logs
    db.collection('audit_logs').add({
        'timestamp': datetime.utcnow(),
        'action': 'assign_roles',
        'user_uid': current_user['uid'],
        'target_user_uid': uid,
        'roles_assigned': request.roles,
        'reason': request.reason
    })

    return {
        "success": True,
        "message": f"Roles asignados exitosamente a {uid}",
        "roles": request.roles
    }


@router.post("/users/{uid}/temporary-permissions")
@require_permission("manage:users")  # Solo super_admin
async def grant_temporary_permission(
    uid: str,
    request: GrantTemporaryPermissionRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Otorgar permiso temporal a un usuario.
    Solo accesible por super_admin.
    """
    db = get_firestore_client()
    user_ref = db.collection('users').document(uid)
    user_doc = user_ref.get()

    if not user_doc.exists:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    user_data = user_doc.to_dict()
    temp_perms = user_data.get('temporary_permissions', [])

    # Agregar nuevo permiso temporal
    temp_perms.append({
        'permission': request.permission,
        'expires_at': request.expires_at,
        'granted_by': current_user['uid'],
        'granted_at': datetime.utcnow(),
        'reason': request.reason
    })

    user_ref.update({
        'temporary_permissions': temp_perms,
        'updated_at': datetime.utcnow()
    })

    return {
        "success": True,
        "message": "Permiso temporal otorgado",
        "permission": request.permission,
        "expires_at": request.expires_at.isoformat()
    }


# ========== GESTIÓN DE ROLES ==========

@router.get("/roles")
@require_permission("manage:roles")  # admin_general o super_admin
async def list_roles(current_user: dict = Depends(get_current_user)):
    """
    Listar todos los roles disponibles.
    Accesible por admin_general y super_admin.
    """
    db = get_firestore_client()
    roles_ref = db.collection('roles').stream()

    roles = []
    for role_doc in roles_ref:
        role_data = role_doc.to_dict()
        role_data['role_id'] = role_doc.id
        roles.append(role_data)

    return {
        "success": True,
        "count": len(roles),
        "data": roles
    }


@router.get("/roles/{role_id}")
@require_permission("manage:roles")
async def get_role_details(
    role_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Obtener detalles de un rol específico.
    """
    db = get_firestore_client()
    role_doc = db.collection('roles').document(role_id).get()

    if not role_doc.exists:
        raise HTTPException(status_code=404, detail="Rol no encontrado")

    role_data = role_doc.to_dict()
    role_data['role_id'] = role_id

    return {
        "success": True,
        "data": role_data
    }


# ========== AUDITORÍA ==========

@router.get("/audit-logs")
@require_permission("view:audit_logs")  # admin_general o super_admin
async def get_audit_logs(
    limit: int = 100,
    user_uid: str = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Obtener logs de auditoría.
    Accesible por admin_general y super_admin.
    """
    db = get_firestore_client()
    query = db.collection('audit_logs').order_by('timestamp', direction='DESCENDING').limit(limit)

    if user_uid:
        query = query.where('user_uid', '==', user_uid)

    logs = []
    for log_doc in query.stream():
        log_data = log_doc.to_dict()
        log_data['log_id'] = log_doc.id
        logs.append(log_data)

    return {
        "success": True,
        "count": len(logs),
        "data": logs
    }
```

**Luego incluye este router en tu main.py:**

```python
from routers import auth as auth_router  # ← NUEVO

app.include_router(auth_router.router)  # ← NUEVO
```

---

## 📊 Tabla de Permisos por Endpoint

| Endpoint                                     | Método | Permiso Requerido  | Roles con Acceso                                                            |
| -------------------------------------------- | ------ | ------------------ | --------------------------------------------------------------------------- |
| `/proyectos-presupuestales/cargar-json`      | POST   | `write:proyectos`  | super_admin, admin_general, admin_centro_gestor, editor_datos               |
| `/proyectos-presupuestales/all`              | GET    | `read:proyectos`   | Todos excepto publico                                                       |
| `/proyectos-presupuestales/bpin/{bpin}`      | GET    | `read:proyectos`   | Todos excepto publico                                                       |
| `/unidades-proyecto/cargar-geojson`          | POST   | `write:unidades`   | super_admin, admin_general, admin_centro_gestor, editor_datos               |
| `/unidades-proyecto/delete-by-centro-gestor` | DELETE | `delete:proyectos` | super_admin, admin_general, admin_centro_gestor                             |
| `/unidades-proyecto/geometry`                | GET    | `read:proyectos`   | Todos excepto publico                                                       |
| `/unidades-proyecto/download-geojson`        | GET    | `download:geojson` | Todos excepto publico, visualizador                                         |
| `/contratos/init_contratos_seguimiento`      | GET    | `read:contratos`   | super_admin, admin_general, admin_centro_gestor, gestor_contratos, analista |
| `/reportes_contratos/`                       | POST   | `write:contratos`  | super_admin, admin_general, admin_centro_gestor, gestor_contratos           |
| `/auth/users`                                | GET    | `manage:users`     | **solo super_admin**                                                        |
| `/auth/users/{uid}/roles`                    | POST   | `manage:users`     | **solo super_admin**                                                        |
| `/auth/roles`                                | GET    | `manage:roles`     | super_admin, admin_general                                                  |
| `/auth/audit-logs`                           | GET    | `view:audit_logs`  | super_admin, admin_general                                                  |

---

## 🚀 Pasos de Implementación

### **Paso 1: Preparar el Código**

1. Copia la carpeta `auth_system/` completa a tu repositorio de la API
2. Crea el archivo `routers/auth.py` con el código proporcionado arriba
3. Modifica tu `main.py` para incluir los middlewares
4. Instala dependencias: `pip install python-dateutil` (si no está en requirements.txt)

### **Paso 2: Proteger Endpoints Críticos Primero**

Empieza protegiendo los endpoints más sensibles:

1. **DELETE endpoints** (eliminación de datos)

   - `/unidades-proyecto/delete-by-centro-gestor`
   - `/unidades-proyecto/delete-by-tipo-equipamiento`

2. **POST endpoints de carga** (modificación masiva)

   - `/proyectos-presupuestales/cargar-json`
   - `/unidades-proyecto/cargar-geojson`

3. **POST endpoints de creación**
   - `/reportes_contratos/`

### **Paso 3: Desplegar en Railway**

1. Commit y push de los cambios:

   ```bash
   git add auth_system/ routers/auth.py main.py
   git commit -m "feat: Add authentication and authorization system"
   git push origin main
   ```

2. Railway desplegará automáticamente

### **Paso 4: Configurar Firebase (si aún no está)**

Asegúrate de que tu API en Railway tenga acceso a Firebase Admin SDK:

- Variable de entorno `GOOGLE_APPLICATION_CREDENTIALS` o
- Service Account Key en Railway

### **Paso 5: Inicializar Datos (Primera Vez)**

Ejecuta el script para inicializar roles y permisos:

```bash
python scripts/init_roles_permissions.py
```

Luego asigna el primer super_admin:

```bash
python scripts/assign_super_admin.py
```

### **Paso 6: Testear desde Frontend**

Una vez desplegado, prueba desde tu app NextJS:

```javascript
// 1. Login del usuario
const loginResponse = await fetch(
  "https://gestorproyectoapi-production.up.railway.app/auth/login",
  {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      email: "admin@cali.gov.co",
      password: "Password123!",
    }),
  }
);

const { id_token } = await loginResponse.json();

// 2. Usar el token en requests protegidos
const response = await fetch(
  "https://gestorproyectoapi-production.up.railway.app/proyectos-presupuestales/cargar-json",
  {
    method: "POST",
    headers: {
      Authorization: `Bearer ${id_token}`,
    },
    body: formData,
  }
);
```

---

## 🔒 Permisos Disponibles

Los permisos siguen la estructura `action:resource[:scope]`:

### **Acciones:**

- `read` - Lectura de datos
- `write` - Creación y actualización
- `delete` - Eliminación de datos
- `manage` - Gestión administrativa
- `upload` - Carga de archivos
- `download` - Descarga de archivos
- `export` - Exportación de datos

### **Recursos:**

- `proyectos` - Proyectos presupuestales
- `unidades` - Unidades de proyecto
- `contratos` - Contratos de empréstito
- `reportes_contratos` - Reportes de seguimiento
- `users` - Usuarios del sistema
- `roles` - Roles y permisos
- `audit_logs` - Logs de auditoría
- `geojson` - Archivos GeoJSON

### **Scopes (opcional):**

- `own_centro` - Solo datos del centro gestor del usuario
- `public` - Solo datos públicos
- `basic` - Solo información básica

---

## 📝 Ejemplos de Uso Completos

### **Ejemplo 1: Endpoint con Filtro por Centro Gestor**

```python
@router.get("/proyectos-presupuestales/centro-gestor/{nombre_centro_gestor}")
@require_permission("read:proyectos:own_centro")
async def get_proyectos_by_centro_gestor(
    nombre_centro_gestor: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Si el usuario tiene read:proyectos:own_centro, solo puede ver proyectos
    de SU centro gestor. El decorador valida esto automáticamente.
    """
    db = get_firestore_client()

    # El decorador ya validó que este usuario puede ver este centro
    query = db.collection('proyectos_presupuestales')\
              .where('nombre_centro_gestor', '==', nombre_centro_gestor)

    proyectos = [doc.to_dict() for doc in query.stream()]

    return {"success": True, "data": proyectos}
```

### **Ejemplo 2: Endpoint Solo para Super Admin**

```python
from auth_system.decorators import require_role

@router.delete("/admin/purge-all-data")
@require_role(["super_admin"])  # Solo super_admin
async def purge_all_data(
    confirm: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Operación crítica solo para super_admin.
    """
    if confirm != "DELETE_EVERYTHING":
        raise HTTPException(400, "Confirmación incorrecta")

    # código de eliminación masiva
    return {"success": True, "message": "Datos eliminados"}
```

### **Ejemplo 3: Endpoint Público (Sin Autenticación)**

```python
# NO agregar decorador ni current_user
@router.get("/public/stats")
async def get_public_stats():
    """
    Endpoint público accesible sin autenticación.
    Asegúrate de agregarlo a public_paths en el middleware.
    """
    return {
        "total_proyectos": 1234,
        "en_ejecucion": 567
    }
```

---

## ⚠️ Consideraciones Importantes

1. **Rutas Públicas**: Todos los endpoints en `public_paths` del middleware NO requieren autenticación
2. **Token Expiration**: Los tokens de Firebase expiran después de 1 hora por defecto
3. **Centro Gestor Scope**: El scope `:own_centro` valida automáticamente que el recurso pertenezca al centro del usuario
4. **Super Admin**: Solo super_admin puede gestionar usuarios (create, update, delete, assign roles)
5. **Auditoría**: El middleware `AuditLogMiddleware` registra automáticamente todas las operaciones POST/PUT/DELETE

---

## 🧪 Testing

Para testear los endpoints protegidos sin frontend:

```bash
# 1. Obtener token (desde Postman o curl)
curl -X POST https://gestorproyectoapi-production.up.railway.app/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@cali.gov.co","password":"Password123!"}'

# Respuesta incluirá: "id_token": "eyJhbGciOiJSUz..."

# 2. Usar el token en requests protegidos
curl -X GET https://gestorproyectoapi-production.up.railway.app/proyectos-presupuestales/all \
  -H "Authorization: Bearer eyJhbGciOiJSUz..."
```

---

## 📚 Recursos Adicionales

- **Especificación completa**: `AUTH_SYSTEM_SPECIFICATION.md`
- **Scripts de utilidad**: Carpeta `scripts/`
- **Código de decoradores**: `auth_system/decorators.py`
- **Modelos Pydantic**: `auth_system/models.py`

---

**Versión:** 1.0  
**Última Actualización:** 23 de Noviembre 2025  
**Autor:** Sistema de Auth para Gestor de Proyectos Cali
