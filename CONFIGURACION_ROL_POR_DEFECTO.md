# 🎯 Configuración de Rol Por Defecto para Nuevos Usuarios

**Proyecto**: Gestor de Proyectos Cali  
**Fecha**: 24 de Noviembre 2025

---

## 📋 Resumen

**Regla Principal**: Todos los usuarios nuevos se registran automáticamente con el rol **"visualizador"**, excepto cuando se asigna explícitamente el rol **"super_admin"** por un administrador.

---

## 🔑 Rol Por Defecto

### **Visualizador**

**ID**: `visualizador`  
**Nivel**: 5  
**Permisos**:

- `read:proyectos:basic` - Ver información básica de proyectos
- `read:unidades:basic` - Ver información básica de unidades
- `read:contratos:basic` - Ver información básica de contratos
- `view:dashboard:basic` - Acceso al dashboard básico

**Descripción**: Solo lectura de datos básicos sin capacidad de exportación. Este es el rol más restrictivo (después de "publico") que permite a los usuarios ver información del sistema sin poder modificar nada.

---

## 🚀 Implementación en la API

### **1. Constante Definida**

En `auth_system/constants.py`:

```python
# ROL POR DEFECTO
# Rol asignado automáticamente a todos los usuarios nuevos (excepto super_admin)
DEFAULT_USER_ROLE = "visualizador"
```

### **2. Importar en tu Router de Auth**

En `routers/auth.py`:

```python
from auth_system import DEFAULT_USER_ROLE
from auth_system.permissions import get_user_permissions
```

### **3. Implementar en el Endpoint de Registro**

**Ubicación**: `POST /auth/register`

```python
@router.post("/register")
async def register_user(request: UserRegistrationRequest):
    """
    Registra un nuevo usuario en Firebase Auth y Firestore.
    El usuario recibe automáticamente el rol 'visualizador'.
    """
    try:
        # 1. Crear usuario en Firebase Auth
        user = auth.create_user(
            email=request.email,
            password=request.password,
            email_verified=False,
            disabled=False
        )

        # 2. Preparar datos del usuario para Firestore
        user_data = {
            "uid": user.uid,
            "email": request.email,
            "full_name": request.full_name,
            "phone_number": request.phone_number,
            "centro_gestor_assigned": request.centro_gestor,

            # ✅ ASIGNAR ROL POR DEFECTO
            "roles": [DEFAULT_USER_ROLE],  # ["visualizador"]

            "email_verified": False,
            "phone_verified": False,
            "is_active": True,
            "created_at": datetime.utcnow(),
            "last_login_at": None,
            "created_by": "self_registration",
            "metadata": {
                "registration_source": "web",
                "registration_ip": request.client_ip if hasattr(request, 'client_ip') else None
            }
        }

        # 3. Guardar en Firestore
        db = get_firestore_client()
        db.collection('users').document(user.uid).set(user_data)

        # 4. Enviar email de verificación (opcional)
        # send_verification_email(user.uid)

        return {
            "success": True,
            "message": "Usuario registrado exitosamente con rol 'visualizador'",
            "user": {
                "uid": user.uid,
                "email": user.email,
                "full_name": request.full_name,
                "roles": [DEFAULT_USER_ROLE],
                "email_verified": False
            }
        }

    except auth.EmailAlreadyExistsError:
        raise HTTPException(
            status_code=400,
            detail="El correo electrónico ya está registrado"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

### **4. Validación en el Login**

En el endpoint `POST /auth/login` o `POST /auth/validate-session`:

```python
@router.post("/validate-session")
async def validate_session(request: ValidateSessionRequest):
    """
    Valida el token de Firebase y retorna los datos completos del usuario.
    """
    try:
        # 1. Verificar token de Firebase
        decoded_token = auth.verify_id_token(request.id_token)
        user_uid = decoded_token['uid']

        # 2. Obtener datos del usuario desde Firestore
        db = get_firestore_client()
        user_doc = db.collection('users').document(user_uid).get()

        if not user_doc.exists:
            raise HTTPException(
                status_code=404,
                detail="Usuario no encontrado en Firestore"
            )

        user_data = user_doc.to_dict()

        # 3. Si el usuario no tiene roles asignados, asignar rol por defecto
        if not user_data.get('roles') or len(user_data.get('roles', [])) == 0:
            user_data['roles'] = [DEFAULT_USER_ROLE]

            # Actualizar en Firestore
            db.collection('users').document(user_uid).update({
                'roles': [DEFAULT_USER_ROLE],
                'updated_at': datetime.utcnow()
            })

        # 4. Obtener permisos del usuario basados en sus roles
        permissions = get_user_permissions(user_uid)

        # 5. Actualizar last_login_at
        db.collection('users').document(user_uid).update({
            'last_login_at': datetime.utcnow()
        })

        return {
            "success": True,
            "user": {
                "uid": user_data['uid'],
                "email": user_data['email'],
                "full_name": user_data['full_name'],
                "roles": user_data['roles'],
                "permissions": permissions,
                "centro_gestor_assigned": user_data.get('centro_gestor_assigned'),
                "email_verified": user_data.get('email_verified', False),
                "phone_verified": user_data.get('phone_verified', False),
                "is_active": user_data.get('is_active', True),
                "created_at": user_data.get('created_at'),
                "last_login_at": user_data.get('last_login_at')
            }
        }

    except auth.InvalidIdTokenError:
        raise HTTPException(
            status_code=401,
            detail="Token inválido o expirado"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

---

## 🔐 Asignación de Otros Roles

### **Solo Super Admin puede cambiar roles**

Para cambiar el rol de un usuario de "visualizador" a otro rol, el **super_admin** debe usar el endpoint:

```http
POST /auth/users/{user_uid}/roles
Authorization: Bearer {super_admin_token}

{
  "roles": ["admin_general"]
}
```

**Ejemplo con curl**:

```bash
curl -X POST "https://gestorproyectoapi-production.up.railway.app/auth/users/{user_uid}/roles" \
  -H "Authorization: Bearer {super_admin_token}" \
  -H "Content-Type: application/json" \
  -d '{
    "roles": ["admin_general"]
  }'
```

### **Roles Disponibles para Asignar**

| Rol                   | Nivel | Descripción                                     |
| --------------------- | ----- | ----------------------------------------------- |
| `super_admin`         | 0     | Control total del sistema (gestión de usuarios) |
| `admin_general`       | 1     | Administración de datos y roles (sin usuarios)  |
| `admin_centro_gestor` | 2     | Administración de su centro gestor              |
| `editor_datos`        | 3     | Edición de datos sin eliminación                |
| `gestor_contratos`    | 3     | Gestión exclusiva de contratos                  |
| `analista`            | 4     | Análisis y exportación de datos                 |
| `visualizador`        | 5     | **ROL POR DEFECTO** - Solo lectura básica       |
| `publico`             | 6     | Acceso público muy limitado                     |

---

## 📊 Flujo de Registro y Asignación de Roles

```
┌─────────────────────────────────────────────────────────────┐
│  NUEVO USUARIO SE REGISTRA                                  │
│  POST /auth/register                                        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  1. Crear usuario en Firebase Auth                          │
│  2. Crear documento en Firestore collection 'users'         │
│  3. Asignar automáticamente: roles = ["visualizador"]       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  USUARIO TIENE PERMISOS LIMITADOS:                          │
│  - read:proyectos:basic                                     │
│  - read:unidades:basic                                      │
│  - read:contratos:basic                                     │
│  - view:dashboard:basic                                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  SUPER_ADMIN CAMBIA ROL (si es necesario)                   │
│  POST /auth/users/{uid}/roles                               │
│  Body: { "roles": ["admin_general"] }                       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  USUARIO AHORA TIENE PERMISOS DEL NUEVO ROL                 │
│  (ej: write:proyectos, delete:unidades, etc.)               │
└─────────────────────────────────────────────────────────────┘
```

---

## 🛡️ Validaciones de Seguridad

### **1. Prevenir Auto-Elevación de Privilegios**

```python
@router.post("/users/{user_uid}/roles")
@require_permission("manage:users")  # Solo super_admin
async def assign_roles_to_user(
    user_uid: str,
    request: AssignRolesRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Asigna roles a un usuario.
    Solo super_admin puede hacer esto.
    """

    # ✅ Validar que el usuario no se asigne roles a sí mismo
    if current_user['uid'] == user_uid and "super_admin" in request.roles:
        raise HTTPException(
            status_code=403,
            detail="No puedes asignarte el rol super_admin a ti mismo"
        )

    # ✅ Validar que los roles existen
    db = get_firestore_client()
    for role in request.roles:
        role_doc = db.collection('roles').document(role).get()
        if not role_doc.exists:
            raise HTTPException(
                status_code=400,
                detail=f"El rol '{role}' no existe"
            )

    # ✅ Actualizar roles del usuario
    db.collection('users').document(user_uid).update({
        'roles': request.roles,
        'updated_at': datetime.utcnow(),
        'updated_by': current_user['uid']
    })

    return {
        "success": True,
        "message": f"Roles actualizados para el usuario {user_uid}",
        "roles": request.roles
    }
```

### **2. Validar en cada Login**

Cuando un usuario hace login, verificar que tenga al menos el rol "visualizador":

```python
# En validate_session o login
if not user_data.get('roles') or len(user_data.get('roles', [])) == 0:
    # Si no tiene roles, asignar visualizador por defecto
    user_data['roles'] = [DEFAULT_USER_ROLE]
    db.collection('users').document(user_uid).update({
        'roles': [DEFAULT_USER_ROLE]
    })
```

---

## 🧪 Testing

### **1. Probar Registro de Usuario**

```bash
curl -X POST "http://localhost:8000/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "nuevo.usuario@cali.gov.co",
    "password": "Password123!",
    "full_name": "Nuevo Usuario",
    "phone_number": "+573001234567",
    "centro_gestor": "SECRETARIA DE SALUD"
  }'
```

**Respuesta Esperada**:

```json
{
  "success": true,
  "message": "Usuario registrado exitosamente con rol 'visualizador'",
  "user": {
    "uid": "abc123...",
    "email": "nuevo.usuario@cali.gov.co",
    "full_name": "Nuevo Usuario",
    "roles": ["visualizador"],
    "email_verified": false
  }
}
```

### **2. Verificar Permisos del Usuario**

```bash
curl -X POST "http://localhost:8000/auth/validate-session" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer {user_token}" \
  -d '{
    "id_token": "{firebase_token}"
  }'
```

**Respuesta Esperada**:

```json
{
  "success": true,
  "user": {
    "uid": "abc123...",
    "email": "nuevo.usuario@cali.gov.co",
    "full_name": "Nuevo Usuario",
    "roles": ["visualizador"],
    "permissions": [
      "read:proyectos:basic",
      "read:unidades:basic",
      "read:contratos:basic",
      "view:dashboard:basic"
    ],
    "email_verified": false,
    "is_active": true
  }
}
```

### **3. Probar Restricción de Permisos**

Intentar crear un proyecto (debe fallar):

```bash
curl -X POST "http://localhost:8000/proyectos-presupuestales/cargar-json" \
  -H "Authorization: Bearer {visualizador_token}" \
  -F "file=@proyectos.json"
```

**Respuesta Esperada**:

```json
{
  "detail": "Permiso denegado: Se requiere el permiso 'write:proyectos'"
}
```

---

## 📝 Checklist de Implementación

- [ ] Importar `DEFAULT_USER_ROLE` desde `auth_system`
- [ ] Modificar endpoint `POST /auth/register` para asignar rol "visualizador" por defecto
- [ ] Modificar endpoint `POST /auth/validate-session` para validar/asignar rol por defecto
- [ ] Implementar endpoint `POST /auth/users/{uid}/roles` solo para super_admin
- [ ] Agregar validación para prevenir auto-elevación de privilegios
- [ ] Probar flujo completo de registro → login → intento de acción restringida
- [ ] Probar cambio de rol por super_admin
- [ ] Documentar en frontend cómo manejar permisos limitados

---

## 🎯 Resumen

1. **Registro**: Todos los nuevos usuarios reciben el rol `visualizador` automáticamente
2. **Permisos**: Rol "visualizador" tiene acceso de solo lectura básica (sin exportación)
3. **Cambio de Rol**: Solo `super_admin` puede cambiar roles de usuarios
4. **Seguridad**: Validaciones para prevenir auto-elevación de privilegios
5. **Frontend**: Usuario visualizador verá UI limitada (sin botones de crear/editar/eliminar)

---

**Versión**: 1.0  
**Última Actualización**: 24 de Noviembre 2025  
**Autor**: Sistema de Auth para Gestor de Proyectos Cali
