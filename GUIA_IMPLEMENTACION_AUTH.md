# 🚀 Guía de Implementación del Sistema de Autenticación

**Fecha:** 23 de Noviembre 2025  
**Proyecto:** Gestor de Proyectos Cali - Sistema de Roles y Permisos

---

## 📋 Estado Actual

### ✅ Completado

1. **Roles y Permisos Definidos**

   - 8 roles predefinidos (super_admin a publico)
   - 50+ permisos granulares con sintaxis `action:resource[:scope]`
   - Documentación completa en `AUTH_SYSTEM_SPECIFICATION.md`

2. **Base de Datos Inicializada**

   - ✅ Colección `roles` creada con 8 roles
   - ✅ Colección `permissions` creada con 50+ permisos
   - Script: `scripts/init_roles_permissions.py` (ejecutado)

3. **Scripts de Utilidad**
   - ✅ `scripts/init_roles_permissions.py` - Inicializar roles y permisos
   - ✅ `scripts/assign_super_admin.py` - Asignar rol super_admin a usuarios

---

## 🎯 Pasos Siguientes

### **Paso 1: Asignar Super Admin al Primer Usuario** ✅ LISTO PARA EJECUTAR

El script `scripts/assign_super_admin.py` permite:

- Listar usuarios existentes en Firestore
- Asignar rol `super_admin` a un usuario existente
- Crear un nuevo usuario con rol `super_admin` desde cero

**Ejecutar:**

```powershell
python scripts/assign_super_admin.py
```

**Opciones disponibles:**

1. Si tienes usuarios existentes → Selecciona el usuario y le asigna super_admin
2. Si NO tienes usuarios → Crea uno nuevo con email, contraseña y rol super_admin

**Ejemplo de ejecución:**

```
🔐 ASIGNACIÓN DE ROL SUPER_ADMIN
📋 Usuarios encontrados (2):

1. Juan Pérez García
   Email: juan.perez@cali.gov.co
   UID: abc123def456
   Roles actuales: Sin roles

2. María López
   Email: maria.lopez@cali.gov.co
   UID: xyz789uvw012
   Roles actuales: editor_datos

Selecciona una opción:
  1-2: Asignar super_admin a un usuario existente
  0: Crear nuevo usuario super_admin

Ingresa el número de opción: 1

✅ Rol 'super_admin' asignado exitosamente a: juan.perez@cali.gov.co
```

---

### **Paso 2: Crear Más Usuarios y Asignar Roles**

Tienes 2 opciones:

#### **Opción A: Crear usuarios vía API (Recomendado)**

Una vez que tengas un super_admin, usa los endpoints de la API para crear usuarios:

**Endpoint de registro (público):**

```http
POST /auth/register
Content-Type: application/json

{
  "email": "usuario@cali.gov.co",
  "password": "Password123!",
  "full_name": "Nombre Completo",
  "phone_number": "+573001234567"
}
```

**Asignar roles (requiere super_admin):**

```http
POST /auth/users/{uid}/roles
Authorization: Bearer {super_admin_token}
Content-Type: application/json

{
  "roles": ["editor_datos"],
  "reason": "Nuevo miembro del equipo de datos"
}
```

#### **Opción B: Script de creación masiva de usuarios**

Puedes crear un script para cargar usuarios desde un CSV/Excel:

```powershell
# Crear archivo: scripts/create_users_bulk.py
python scripts/create_users_bulk.py --file usuarios.csv
```

**Formato del CSV:**

```csv
email,full_name,password,roles,centro_gestor
ana.garcia@cali.gov.co,Ana García,Pass123!,admin_centro_gestor,Secretaría de Infraestructura
pedro.lopez@cali.gov.co,Pedro López,Pass456!,editor_datos,
maria.ruiz@cali.gov.co,María Ruiz,Pass789!,gestor_contratos,
```

---

### **Paso 3: Implementar Decoradores de Permisos en Endpoints**

Ahora necesitas proteger tus endpoints existentes con los decoradores de permisos.

#### **3.1. Crear el módulo `auth_system/`**

Estructura de carpetas necesaria:

```
a:\programing_workspace\proyectos_cali_alcaldia_etl\
├── auth_system/
│   ├── __init__.py
│   ├── constants.py          # ROLES_PREDEFINIDOS, PERMISSIONS_CATALOG
│   ├── models.py              # Pydantic models
│   ├── permissions.py         # check_permission(), get_user_permissions()
│   ├── decorators.py          # @require_permission, @require_role
│   ├── middleware.py          # AuthorizationMiddleware
│   └── utils.py               # Helper functions
```

#### **3.2. Implementar `auth_system/permissions.py`**

```python
# auth_system/permissions.py
from typing import List, Dict, Optional
from database.config import get_firestore_client


async def get_user_permissions(user_uid: str) -> List[str]:
    """
    Obtiene todos los permisos efectivos de un usuario.

    Args:
        user_uid: UID del usuario

    Returns:
        Lista de permisos (ej: ["read:proyectos", "write:proyectos"])
    """
    try:
        db = get_firestore_client()

        # Obtener usuario de Firestore
        user_doc = db.collection('users').document(user_uid).get()
        if not user_doc.exists:
            return []

        user_data = user_doc.to_dict()
        user_roles = user_data.get('roles', [])
        custom_permissions = user_data.get('custom_permissions', [])

        # Permisos temporales activos
        temporary_permissions = user_data.get('temporary_permissions', [])
        active_temp_perms = []

        from datetime import datetime
        now = datetime.utcnow()

        for temp_perm in temporary_permissions:
            expires_at = temp_perm.get('expires_at')
            if expires_at and expires_at > now:
                active_temp_perms.append(temp_perm['permission'])

        # Obtener permisos de cada rol
        all_permissions = set(custom_permissions + active_temp_perms)

        for role_id in user_roles:
            role_doc = db.collection('roles').document(role_id).get()
            if role_doc.exists:
                role_data = role_doc.to_dict()
                role_permissions = role_data.get('permissions', [])
                all_permissions.update(role_permissions)

        return list(all_permissions)

    except Exception as e:
        print(f"Error obteniendo permisos: {e}")
        return []


async def check_permission(
    user_uid: str,
    required_permission: str,
    resource_data: Optional[Dict] = None
) -> bool:
    """
    Verifica si un usuario tiene un permiso específico.

    Args:
        user_uid: UID del usuario
        required_permission: Permiso requerido (ej: "write:proyectos")
        resource_data: Datos del recurso (para scope validation)

    Returns:
        True si tiene el permiso, False en caso contrario
    """
    permissions = await get_user_permissions(user_uid)

    # Super admin tiene acceso total
    if '*' in permissions:
        return True

    # Verificar permiso exacto
    if required_permission in permissions:
        return True

    # Verificar permiso con wildcard (ej: "read:*" cubre "read:proyectos")
    parts = required_permission.split(':')
    if len(parts) >= 2:
        action, resource = parts[0], parts[1]
        wildcard_perm = f"{action}:*"
        if wildcard_perm in permissions:
            return True

    # Validar scope (own_centro)
    if resource_data and len(parts) == 3:
        action, resource, scope = parts
        if scope == 'own_centro':
            db = get_firestore_client()
            user_doc = db.collection('users').document(user_uid).get()
            if user_doc.exists:
                user_data = user_doc.to_dict()
                user_centro = user_data.get('centro_gestor_assigned')
                resource_centro = resource_data.get('nombre_centro_gestor')

                if user_centro == resource_centro:
                    own_centro_perm = f"{action}:{resource}:own_centro"
                    return own_centro_perm in permissions

    return False
```

#### **3.3. Implementar `auth_system/decorators.py`**

```python
# auth_system/decorators.py
from functools import wraps
from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from firebase_admin import auth
from typing import List, Optional
from .permissions import check_permission, get_user_permissions


security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    """
    Obtiene el usuario actual desde el token de Firebase.

    Returns:
        dict con datos del usuario (uid, email, etc.)
    """
    try:
        # Verificar token de Firebase
        id_token = credentials.credentials
        decoded_token = auth.verify_id_token(id_token)

        uid = decoded_token['uid']
        email = decoded_token.get('email')

        return {
            'uid': uid,
            'email': email,
            'token': decoded_token
        }

    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail=f"Token inválido o expirado: {str(e)}"
        )


def require_permission(permission: str):
    """
    Decorador para requerir un permiso específico.

    Uso:
        @require_permission("write:proyectos")
        async def create_proyecto(...):
            pass
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Obtener current_user de los kwargs
            current_user = kwargs.get('current_user')
            if not current_user:
                raise HTTPException(
                    status_code=401,
                    detail="Usuario no autenticado"
                )

            user_uid = current_user['uid']

            # Verificar permiso
            has_permission = await check_permission(user_uid, permission)

            if not has_permission:
                raise HTTPException(
                    status_code=403,
                    detail=f"Permiso denegado. Requiere: {permission}"
                )

            return await func(*args, **kwargs)

        return wrapper
    return decorator


def require_role(allowed_roles: List[str]):
    """
    Decorador para requerir uno de varios roles.

    Uso:
        @require_role(["super_admin", "admin_general"])
        async def admin_action(...):
            pass
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_user = kwargs.get('current_user')
            if not current_user:
                raise HTTPException(
                    status_code=401,
                    detail="Usuario no autenticado"
                )

            from database.config import get_firestore_client
            db = get_firestore_client()

            user_doc = db.collection('users').document(current_user['uid']).get()
            if not user_doc.exists:
                raise HTTPException(
                    status_code=404,
                    detail="Usuario no encontrado"
                )

            user_data = user_doc.to_dict()
            user_roles = user_data.get('roles', [])

            # Verificar si tiene al menos uno de los roles permitidos
            has_role = any(role in allowed_roles for role in user_roles)

            if not has_role:
                raise HTTPException(
                    status_code=403,
                    detail=f"Rol insuficiente. Requiere uno de: {', '.join(allowed_roles)}"
                )

            return await func(*args, **kwargs)

        return wrapper
    return decorator
```

#### **3.4. Proteger Endpoints Existentes**

Ejemplo de cómo proteger un endpoint:

**ANTES:**

```python
@router.post("/proyectos-presupuestales/cargar-json")
async def cargar_proyectos(file: UploadFile):
    # código sin protección
    pass
```

**DESPUÉS:**

```python
from auth_system.decorators import require_permission, get_current_user
from fastapi import Depends

@router.post("/proyectos-presupuestales/cargar-json")
@require_permission("write:proyectos")
async def cargar_proyectos(
    file: UploadFile,
    current_user: dict = Depends(get_current_user)
):
    # código protegido
    pass
```

---

### **Paso 4: Configurar Middleware de Autorización**

El middleware intercepta TODAS las requests y valida autenticación automáticamente.

#### **4.1. Implementar `auth_system/middleware.py`**

```python
# auth_system/middleware.py
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from firebase_admin import auth
from typing import List
import re


class AuthorizationMiddleware(BaseHTTPMiddleware):
    """
    Middleware que valida autenticación en todas las rutas
    excepto las rutas públicas especificadas.
    """

    def __init__(self, app, public_paths: List[str] = None):
        super().__init__(app)
        self.public_paths = public_paths or [
            "/",
            "/docs",
            "/openapi.json",
            "/redoc",
            "/auth/login",
            "/auth/register",
            "/auth/google",
            "/auth/config",
            "/auth/verify-magic-link",
            "/auth/forgot-password",
            "/auth/reset-password"
        ]

    def is_public_path(self, path: str) -> bool:
        """Verifica si la ruta es pública."""
        for public_path in self.public_paths:
            # Soporte para wildcards
            if public_path.endswith("*"):
                pattern = public_path.replace("*", ".*")
                if re.match(f"^{pattern}", path):
                    return True
            elif path == public_path:
                return True
        return False

    async def dispatch(self, request: Request, call_next):
        """Intercepta cada request."""
        path = request.url.path

        # Si es ruta pública, continuar sin validar
        if self.is_public_path(path):
            return await call_next(request)

        # Obtener token del header Authorization
        auth_header = request.headers.get("Authorization")

        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={
                    "success": False,
                    "message": "Token de autenticación requerido"
                }
            )

        try:
            # Extraer y verificar token
            id_token = auth_header.split("Bearer ")[1]
            decoded_token = auth.verify_id_token(id_token)

            # Agregar usuario al request state
            request.state.user = {
                'uid': decoded_token['uid'],
                'email': decoded_token.get('email'),
                'token': decoded_token
            }

            # Continuar con el request
            response = await call_next(request)
            return response

        except Exception as e:
            return JSONResponse(
                status_code=401,
                content={
                    "success": False,
                    "message": f"Token inválido: {str(e)}"
                }
            )
```

#### **4.2. Registrar Middleware en FastAPI**

En tu archivo principal de FastAPI (ej: `main.py` o `app.py`):

```python
from fastapi import FastAPI
from auth_system.middleware import AuthorizationMiddleware

app = FastAPI()

# Registrar middleware
app.add_middleware(
    AuthorizationMiddleware,
    public_paths=[
        "/",
        "/docs",
        "/openapi.json",
        "/auth/login",
        "/auth/register",
        "/auth/google",
        "/auth/config"
    ]
)
```

---

## 📊 Resumen de Implementación

### Estado de Tareas

| #   | Tarea                             | Estado       | Comando/Acción                             |
| --- | --------------------------------- | ------------ | ------------------------------------------ |
| 1   | Inicializar roles y permisos      | ✅ HECHO     | `python scripts/init_roles_permissions.py` |
| 2   | Asignar super_admin               | 🟡 PENDIENTE | `python scripts/assign_super_admin.py`     |
| 3   | Crear usuarios adicionales        | 🟡 PENDIENTE | Vía API o script bulk                      |
| 4   | Implementar módulo `auth_system/` | ⬜ TODO      | Crear archivos mencionados                 |
| 5   | Proteger endpoints existentes     | ⬜ TODO      | Agregar decoradores                        |
| 6   | Configurar middleware             | ⬜ TODO      | Registrar en FastAPI                       |
| 7   | Testing completo                  | ⬜ TODO      | Pytest + Postman                           |

---

## 🔥 Acción Inmediata

**Lo que debes hacer AHORA:**

1. **Ejecutar script de asignación de super_admin:**

   ```powershell
   python scripts/assign_super_admin.py
   ```

2. **Crear la estructura de carpetas `auth_system/`:**

   ```powershell
   mkdir auth_system
   New-Item -Path auth_system\__init__.py -ItemType File
   New-Item -Path auth_system\permissions.py -ItemType File
   New-Item -Path auth_system\decorators.py -ItemType File
   New-Item -Path auth_system\middleware.py -ItemType File
   ```

3. **Copiar el código proporcionado** en esta guía a los archivos correspondientes

4. **Testear un endpoint protegido:**
   ```python
   # Ejemplo de endpoint protegido
   @router.get("/test-protected")
   @require_permission("read:proyectos")
   async def test_protected(current_user: dict = Depends(get_current_user)):
       return {"message": f"¡Acceso permitido para {current_user['email']}!"}
   ```

---

## 📚 Recursos Adicionales

- **Especificación Completa:** `AUTH_SYSTEM_SPECIFICATION.md`
- **Script de Inicialización:** `scripts/init_roles_permissions.py`
- **Script de Super Admin:** `scripts/assign_super_admin.py`

---

**Versión:** 1.0  
**Última Actualización:** 23 de Noviembre 2025
