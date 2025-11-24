# 🔐 Sistema de Autenticación, Roles y Permisos - Especificación Técnica

**Fecha:** 23 de Noviembre 2025  
**Proyecto:** Gestor de Proyectos Cali - API REST  
**Stack:** FastAPI + Firebase Auth + Firestore  
**Autor:** Juan Pablo GM

---

## 📋 TABLA DE CONTENIDOS

1. [Arquitectura General](#arquitectura-general)
2. [Estructura de Datos en Firestore](#estructura-de-datos-en-firestore)
3. [Roles y Permisos Predefinidos](#roles-y-permisos-predefinidos)
4. [Endpoints Existentes](#endpoints-existentes)
5. [Nuevos Endpoints a Implementar](#nuevos-endpoints-a-implementar)
6. [Decoradores y Middleware](#decoradores-y-middleware)
7. [Sistema de Notificaciones](#sistema-de-notificaciones)
8. [Auditoría y Logs](#auditoría-y-logs)
9. [Estructura de Carpetas](#estructura-de-carpetas)
10. [Modelos Pydantic](#modelos-pydantic)

---

## 🏗️ ARQUITECTURA GENERAL

### Flujo de Autenticación y Autorización

```
1. Usuario autentica con Firebase Auth (email/password, Google, magic link)
   ↓
2. Backend valida token con Firebase Admin SDK
   ↓
3. Se obtiene UID del usuario
   ↓
4. Se buscan roles asignados en Firestore (colección 'users')
   ↓
5. Se cargan permisos asociados a esos roles (colección 'roles')
   ↓
6. Se valida si tiene permiso para la acción solicitada
   ↓
7. Se ejecuta la acción y se registra en audit_logs
   ↓
8. Se envían notificaciones si aplica
```

### Componentes Clave

- **Firebase Auth**: Gestión de usuarios y tokens
- **Firestore**: Almacenamiento de roles, permisos y usuarios
- **FastAPI Middleware**: Validación automática de permisos
- **Decoradores**: `@require_permission()`, `@require_role()`
- **Audit Logs**: Registro de todas las acciones sensibles

---

## 📊 ESTRUCTURA DE DATOS EN FIRESTORE

### Colección: `roles`

```javascript
{
  role_id: "admin_proyecto",           // ID único del rol
  name: "Administrador de Proyecto",   // Nombre display
  description: "Gestión completa de proyectos y unidades",
  level: 2,                             // Nivel jerárquico (0=super admin, 6=público)
  permissions: [                        // Array de permisos
    "read:proyectos",
    "write:proyectos",
    "write:unidades",
    "delete:proyectos",
    "upload:geojson",
    "manage:users:same_centro"
  ],
  created_at: timestamp,
  updated_at: timestamp,
  created_by: "admin_uid",
  is_active: true,
  color: "#4ECDC4",                    // Color para UI
  icon: "building"                      // Icono para UI
}
```

### Colección: `permissions`

```javascript
{
  permission_id: "write:proyectos:infraestructura",
  resource: "proyectos_presupuestales",  // Recurso protegido
  action: "write",                       // Acción permitida
  scope: "infraestructura",              // Scope (centro gestor, etc)
  description: "Crear/editar proyectos de Infraestructura",
  category: "data_management",           // Categoría para agrupar
  requires_verification: false,          // Si requiere verificación adicional
  risk_level: "medium"                   // low, medium, high, critical
}
```

### Colección: `users` (Estructura extendida)

```javascript
{
  // Campos básicos existentes
  uid: "user123",
  email: "juan.perez@cali.gov.co",
  full_name: "Juan Pérez García",
  phone_number: "+573001234567",

  // NUEVOS CAMPOS - Roles y Permisos
  roles: ["editor_datos", "gestor_contratos"],
  custom_permissions: ["read:reportes_especiales"],

  // Restricciones por Centro Gestor
  centro_gestor_assigned: "Secretaría de Infraestructura",
  can_access_centros: [
    "Secretaría de Infraestructura",
    "Secretaría de Salud"
  ],

  // Permisos Temporales
  temporary_permissions: [
    {
      permission: "write:contratos",
      expires_at: "2025-12-31T23:59:59Z",
      granted_by: "admin_uid_123",
      granted_by_name: "Admin Sistema",
      reason: "Proyecto especial Q4 2025",
      created_at: "2025-11-01T10:00:00Z"
    }
  ],

  // Verificaciones
  email_verified: true,
  email_verified_at: timestamp,
  phone_verified: false,
  phone_verified_at: null,

  // Seguridad y Auditoría
  last_login_at: timestamp,
  last_login_ip: "192.168.1.100",
  login_count: 45,
  failed_login_attempts: 0,
  last_failed_login_at: null,

  // Estado de la cuenta
  is_active: true,
  account_status: "active",  // active, suspended, pending_verification, locked
  account_locked_until: null,

  // Metadata de roles
  assigned_by: "admin_uid",
  assigned_by_name: "Admin Principal",
  assigned_at: timestamp,
  last_role_change: timestamp,

  // Configuraciones de usuario
  preferences: {
    language: "es",
    timezone: "America/Bogota",
    notifications_email: true,
    notifications_sms: false,
    theme: "light"
  },

  // Timestamps
  created_at: timestamp,
  updated_at: timestamp,

  // Google Sign-In data (opcional)
  google_profile: {
    photo_url: "https://...",
    locale: "es",
    google_id: "123456789"
  }
}
```

### Colección: `audit_logs`

```javascript
{
  log_id: "log_uuid_12345",
  timestamp: "2025-11-23T14:30:45.123Z",

  // Usuario que ejecuta
  user_uid: "user123",
  user_email: "juan.perez@cali.gov.co",
  user_name: "Juan Pérez García",
  user_roles: ["editor_datos"],

  // Acción realizada
  action: "CREATE_PROYECTO",  // CREATE, UPDATE, DELETE, LOGIN, LOGOUT, PERMISSION_CHANGE, etc.
  resource_type: "proyectos_presupuestales",
  resource_id: "proyecto_doc_123",
  resource_name: "Construcción Vía Principal",

  // Permisos
  permission_used: "write:proyectos",
  permission_granted: true,
  permission_source: "role:editor_datos",  // De dónde viene el permiso

  // Contexto HTTP
  ip_address: "192.168.1.100",
  user_agent: "Mozilla/5.0...",
  request_method: "POST",
  request_path: "/proyectos-presupuestales/cargar-json",
  request_id: "req_uuid_456",

  // Datos de cambio
  changes: {
    before: {
      nombre_proyecto: "Vía Principal",
      estado: "planificación"
    },
    after: {
      nombre_proyecto: "Construcción Vía Principal",
      estado: "ejecución"
    }
  },

  // Resultado
  success: true,
  http_status: 201,
  error_message: null,
  execution_time_ms: 245,

  // Metadata adicional
  centro_gestor: "Secretaría de Infraestructura",
  tags: ["alta_prioridad", "proyecto_2025"],

  // Clasificación de riesgo
  risk_level: "medium",  // low, medium, high, critical
  requires_review: false,
  reviewed_by: null,
  reviewed_at: null
}
```

### Colección: `verification_codes`

```javascript
{
  code_id: "code_uuid",
  type: "email_verification",  // email_verification, sms_verification, password_reset, magic_link
  code: "123456",              // Código de 6 dígitos
  user_uid: "user123",
  user_email: "juan.perez@cali.gov.co",

  // Estado
  is_used: false,
  attempts: 0,
  max_attempts: 3,

  // Timestamps
  created_at: timestamp,
  expires_at: timestamp,       // 10 minutos después de created_at
  used_at: null,

  // Metadata
  ip_address: "192.168.1.100",
  user_agent: "Mozilla/5.0..."
}
```

---

## 👥 ROLES Y PERMISOS PREDEFINIDOS

### Roles del Sistema

```python
ROLES_PREDEFINIDOS = {
    "super_admin": {
        "name": "Super Administrador",
        "level": 0,
        "permissions": ["*"],  # Acceso total
        "description": "Control absoluto del sistema incluyendo gestión de usuarios",
        "color": "#FF0000",
        "icon": "shield"
    },

    "admin_general": {
        "name": "Administrador General",
        "level": 1,
        "permissions": [
            "read:*",
            "write:proyectos", "write:unidades", "write:contratos",
            "delete:proyectos", "delete:unidades",
            "manage:roles",  # ✅ PUEDE gestionar roles
            "view:audit_logs",
            "upload:geojson", "download:geojson",
            "export:*"
        ],
        "description": "Administración completa de datos y roles, SIN acceso a gestión de usuarios",
        "color": "#FF6B6B",
        "icon": "user-shield"
    },

    "admin_centro_gestor": {
        "name": "Administrador de Centro Gestor",
        "level": 2,
        "permissions": [
            "read:proyectos:own_centro",
            "write:proyectos:own_centro",
            "write:unidades:own_centro",
            "delete:proyectos:own_centro",
            "read:contratos:own_centro",
            "write:contratos:own_centro",
            "create:reportes_contratos:own_centro",
            "export:proyectos:own_centro",
            "export:unidades:own_centro",
            "export:contratos:own_centro",
            "upload:geojson",
            "download:geojson"
        ],
        "description": "Gestión completa de datos de su centro gestor (sin acceso a usuarios)",
        "color": "#4ECDC4",
        "icon": "building"
    },

    "editor_datos": {
        "name": "Editor de Datos",
        "level": 3,
        "permissions": [
            "read:proyectos", "read:unidades", "read:contratos",
            "write:proyectos", "write:unidades",
            "upload:geojson",
            "export:proyectos", "export:unidades"
        ],
        "description": "Edición de datos sin eliminación",
        "color": "#95E1D3",
        "icon": "edit"
    },

    "gestor_contratos": {
        "name": "Gestor de Contratos",
        "level": 3,
        "permissions": [
            "read:contratos",
            "write:contratos",
            "create:reportes_contratos",
            "read:proyectos:reference",
            "export:contratos"
        ],
        "description": "Gestión exclusiva de contratos",
        "color": "#F38181",
        "icon": "file-contract"
    },

    "analista": {
        "name": "Analista de Datos",
        "level": 4,
        "permissions": [
            "read:proyectos", "read:unidades", "read:contratos",
            "read:reportes_contratos",
            "export:proyectos", "export:unidades", "export:contratos",
            "download:geojson",
            "view:dashboard:advanced"
        ],
        "description": "Análisis y exportación de datos",
        "color": "#A8E6CF",
        "icon": "chart-line"
    },

    "visualizador": {
        "name": "Visualizador",
        "level": 5,
        "permissions": [
            "read:proyectos:basic",
            "read:unidades:basic",
            "read:contratos:basic",
            "view:dashboard:basic"
        ],
        "description": "Solo lectura de datos básicos",
        "color": "#DCEDC8",
        "icon": "eye"
    },

    "publico": {
        "name": "Usuario Público",
        "level": 6,
        "permissions": [
            "read:proyectos:public",
            "read:unidades:public",
            "view:map:public"
        ],
        "description": "Acceso público limitado",
        "color": "#E0E0E0",
        "icon": "globe"
    }
}
```

### Catálogo de Permisos

**Sintaxis:** `action:resource[:scope]`

```python
PERMISSIONS_CATALOG = {
    # Proyectos Presupuestales
    "read:proyectos": "Ver todos los proyectos presupuestales",
    "read:proyectos:own_centro": "Ver proyectos de su centro gestor",
    "read:proyectos:basic": "Ver información básica de proyectos",
    "read:proyectos:public": "Ver proyectos públicos",
    "write:proyectos": "Crear y editar todos los proyectos",
    "write:proyectos:own_centro": "Crear/editar proyectos de su centro",
    "delete:proyectos": "Eliminar cualquier proyecto",
    "delete:proyectos:own_centro": "Eliminar proyectos de su centro",
    "export:proyectos": "Exportar datos de proyectos",

    # Unidades de Proyecto
    "read:unidades": "Ver todas las unidades de proyecto",
    "read:unidades:own_centro": "Ver unidades de su centro",
    "read:unidades:basic": "Ver información básica de unidades",
    "write:unidades": "Crear y editar unidades",
    "write:unidades:own_centro": "Crear/editar unidades de su centro",
    "delete:unidades": "Eliminar unidades",
    "upload:geojson": "Cargar archivos GeoJSON",
    "download:geojson": "Descargar archivos GeoJSON",

    # Contratos y Empréstito
    "read:contratos": "Ver todos los contratos",
    "read:contratos:own_centro": "Ver contratos de su centro",
    "write:contratos": "Crear y editar contratos",
    "delete:contratos": "Eliminar contratos",
    "create:reportes_contratos": "Crear reportes de contratos",
    "approve:reportes_contratos": "Aprobar reportes de contratos",
    "export:contratos": "Exportar datos de contratos",

    # Administración de Usuarios (⚠️ EXCLUSIVAMENTE SUPER_ADMIN)
    "manage:users": "Gestionar todos los usuarios del sistema (SOLO super_admin)",

    # Gestión de Roles (✅ SUPER_ADMIN y ADMIN_GENERAL)
    "manage:roles": "Gestionar roles del sistema",
    "manage:permissions": "Gestionar permisos del sistema",

    # Auditoría
    "view:audit_logs": "Ver logs de auditoría completos",
    "view:audit_logs:own": "Ver sus propios logs",
    "view:audit_logs:own_centro": "Ver logs de su centro",

    # Exportación y Descarga
    "export:*": "Exportar cualquier tipo de dato",

    # Lectura general
    "read:*": "Leer todos los recursos",

    # Acceso total
    "*": "Acceso total al sistema (Super Admin)"
}
```

---

## ✅ ENDPOINTS EXISTENTES

### Autenticación Básica

```
POST /auth/login
  - Body: { email, password }
  - Retorna: { success, user, idToken, refreshToken }

POST /auth/register
  - Body: { email, password, full_name, phone_number }
  - Retorna: { success, user, uid }

POST /auth/validate-session
  - Header: Authorization: Bearer {idToken}
  - Retorna: { success, user, roles, permissions }

POST /auth/change-password
  - Body: { uid, new_password }
  - Requiere: Admin permissions

DELETE /auth/user/{uid}
  - Query: ?soft_delete=true
  - Requiere: Admin permissions
```

### Google Sign-In

```
POST /auth/google
  - Body: { google_token }
  - Retorna: { success, user, is_new_user }
```

### Administración

```
GET /admin/users
  - Query: ?limit=100
  - Retorna: Lista de usuarios

GET /auth/config
  - Público
  - Retorna: Firebase config para frontend

GET /auth/workload-identity/status
  - Debug endpoint
  - Retorna: Estado WIF
```

---

## 🆕 NUEVOS ENDPOINTS A IMPLEMENTAR

### 1. Verificación de Email

```python
POST /auth/send-verification-email
  """
  Envía email de verificación al usuario actual
  """
  Headers:
    - Authorization: Bearer {idToken}

  Response 200:
    {
      "success": true,
      "message": "Email de verificación enviado",
      "email": "user@cali.gov.co",
      "sent_at": "2025-11-23T14:30:00Z"
    }

POST /auth/verify-email
  """
  Verifica email con código OOB de Firebase
  """
  Body:
    {
      "oobCode": "abc123def456"
    }

  Response 200:
    {
      "success": true,
      "message": "Email verificado exitosamente",
      "user": { ...user_data }
    }

POST /auth/resend-verification
  """
  Reenvía email de verificación (rate limited)
  """
  Headers:
    - Authorization: Bearer {idToken}

  Response 200:
    {
      "success": true,
      "message": "Email reenviado",
      "next_allowed_at": "2025-11-23T14:31:00Z"
    }
```

### 2. Verificación con Código (Magic Code)

```python
POST /auth/send-code
  """
  Genera y envía código de 6 dígitos por email
  """
  Body:
    {
      "email": "user@cali.gov.co",
      "purpose": "login"  # login, verification, password_reset
    }

  Response 200:
    {
      "success": true,
      "message": "Código enviado",
      "email": "user@cali.gov.co",
      "expires_in": 600,  # segundos
      "code_id": "code_uuid"
    }

POST /auth/verify-code
  """
  Verifica código y autentica usuario
  """
  Body:
    {
      "email": "user@cali.gov.co",
      "code": "123456"
    }

  Response 200:
    {
      "success": true,
      "message": "Código verificado",
      "user": { ...user_data },
      "idToken": "...",
      "refreshToken": "..."
    }
```

### 3. Magic Links (Passwordless)

```python
POST /auth/send-magic-link
  """
  Genera y envía link mágico de autenticación
  """
  Body:
    {
      "email": "user@cali.gov.co",
      "redirect_url": "https://app.cali.gov.co/dashboard"
    }

  Response 200:
    {
      "success": true,
      "message": "Link mágico enviado",
      "email": "user@cali.gov.co",
      "expires_in": 900
    }

GET /auth/verify-magic-link
  """
  Valida token de magic link y autentica
  """
  Query:
    - token: "magic_link_token_123"
    - redirect: "https://app.cali.gov.co/dashboard"

  Response 302:
    Redirect a: {redirect}?token={idToken}&user={user_uid}
```

### 4. Recuperación de Contraseña

```python
POST /auth/forgot-password
  """
  Inicia proceso de recuperación de contraseña
  """
  Body:
    {
      "email": "user@cali.gov.co"
    }

  Response 200:
    {
      "success": true,
      "message": "Email de recuperación enviado",
      "email": "user@cali.gov.co"
    }

POST /auth/reset-password
  """
  Resetea contraseña con código OOB
  """
  Body:
    {
      "oobCode": "reset_code_123",
      "new_password": "NewSecurePass123!"
    }

  Response 200:
    {
      "success": true,
      "message": "Contraseña actualizada exitosamente"
    }
```

### 5. Verificación SMS (Opcional)

```python
POST /auth/send-sms-code
  """
  Envía código de verificación por SMS
  """
  Body:
    {
      "phone_number": "+573001234567",
      "purpose": "verification"  # verification, login, 2fa
    }

  Response 200:
    {
      "success": true,
      "message": "Código SMS enviado",
      "phone_number": "+57300***4567",
      "expires_in": 600
    }

POST /auth/verify-sms
  """
  Verifica código SMS
  """
  Body:
    {
      "phone_number": "+573001234567",
      "code": "123456"
    }

  Response 200:
    {
      "success": true,
      "message": "Teléfono verificado",
      "user": { ...user_data }
    }
```

### 6. Gestión de Roles de Usuario

```python
GET /auth/users/{uid}/roles
  """
  Obtiene roles de un usuario específico
  Requiere: manage:users (SOLO super_admin)
  """
  Response 200:
    {
      "success": true,
      "user_uid": "user123",
      "roles": ["editor_datos", "gestor_contratos"],
      "effective_permissions": ["read:proyectos", "write:proyectos", ...]
    }

POST /auth/users/{uid}/roles
  """
  Asigna roles a un usuario
  Requiere: manage:users (SOLO super_admin)
  """
  Body:
    {
      "roles": ["editor_datos"],
      "reason": "Nuevo miembro del equipo"
    }

  Headers:
    - Authorization: Bearer {admin_token}

  Response 200:
    {
      "success": true,
      "message": "Roles asignados",
      "user_uid": "user123",
      "roles_added": ["editor_datos"],
      "notification_sent": true
    }

DELETE /auth/users/{uid}/roles/{role_id}
  """
  Remueve un rol específico de un usuario
  Requiere: manage:users (SOLO super_admin)
  """
  Headers:
    - Authorization: Bearer {admin_token}

  Response 200:
    {
      "success": true,
      "message": "Rol removido",
      "role_removed": "editor_datos",
      "remaining_roles": ["visualizador"]
    }
```

### 7. Gestión de Permisos Temporales

```python
POST /auth/users/{uid}/temporary-permissions
  """
  Otorga permisos temporales a un usuario
  Requiere: manage:users (super_admin)
  """
  Body:
    {
      "permission": "write:contratos",
      "expires_at": "2025-12-31T23:59:59Z",
      "reason": "Proyecto especial Q4"
    }

  Headers:
    - Authorization: Bearer {admin_token}

  Response 200:
    {
      "success": true,
      "message": "Permiso temporal otorgado",
      "permission": "write:contratos",
      "expires_at": "2025-12-31T23:59:59Z",
      "notification_sent": true
    }

GET /auth/users/{uid}/temporary-permissions
  """
  Lista permisos temporales activos de un usuario
  """
  Response 200:
    {
      "success": true,
      "user_uid": "user123",
      "temporary_permissions": [
        {
          "permission": "write:contratos",
          "expires_at": "2025-12-31T23:59:59Z",
          "granted_by": "admin_uid",
          "reason": "Proyecto especial Q4",
          "days_remaining": 38
        }
      ]
    }

DELETE /auth/users/{uid}/temporary-permissions/{permission}
  """
  Revoca un permiso temporal
  Requiere: manage:users (super_admin)
  """
  Response 200:
    {
      "success": true,
      "message": "Permiso temporal revocado",
      "permission": "write:contratos"
    }
```

### 8. Gestión de Roles del Sistema

```python
GET /auth/roles
  """
  Lista todos los roles disponibles
  Requiere: manage:roles (super_admin, admin_general)
  """
  Query:
    - active_only: true/false

  Response 200:
    {
      "success": true,
      "roles": [
        {
          "role_id": "editor_datos",
          "name": "Editor de Datos",
          "level": 3,
          "permissions_count": 8,
          "users_count": 15,
          "is_active": true
        }
      ]
    }

POST /auth/roles
  """
  Crea un rol personalizado
  Requiere: manage:roles (super_admin, admin_general)
  """
  Body:
    {
      "role_id": "coordinador_regional",
      "name": "Coordinador Regional",
      "description": "Coordinación de proyectos regionales",
      "permissions": ["read:proyectos", "write:proyectos:own_centro"],
      "level": 3
    }

  Headers:
    - Authorization: Bearer {admin_token}

  Response 201:
    {
      "success": true,
      "message": "Rol creado",
      "role": { ...role_data }
    }

PUT /auth/roles/{role_id}
  """
  Actualiza un rol existente
  Requiere: manage:roles (super_admin, admin_general)
  """
  Body:
    {
      "name": "Editor de Datos Avanzado",
      "permissions": ["read:proyectos", "write:proyectos", "delete:proyectos:own_centro"]
    }

  Response 200:
    {
      "success": true,
      "message": "Rol actualizado",
      "affected_users": 15
    }

DELETE /auth/roles/{role_id}
  """
  Elimina un rol (no predefinido)
  Requiere: manage:roles (super_admin, admin_general)
  """
  Response 200:
    {
      "success": true,
      "message": "Rol eliminado",
      "users_migrated_to": "visualizador"
    }
```

### 9. Verificación de Permisos

```python
POST /auth/check-permission
  """
  Verifica si el usuario actual tiene un permiso
  """
  Body:
    {
      "permission": "write:proyectos",
      "resource_id": "proyecto_123",  # Opcional
      "centro_gestor": "Secretaría de Salud"  # Opcional
    }

  Headers:
    - Authorization: Bearer {idToken}

  Response 200:
    {
      "success": true,
      "has_permission": true,
      "permission": "write:proyectos",
      "granted_by": "role:editor_datos",
      "scope_valid": true
    }

GET /auth/my-permissions
  """
  Obtiene todos los permisos del usuario actual
  """
  Headers:
    - Authorization: Bearer {idToken}

  Response 200:
    {
      "success": true,
      "user_uid": "user123",
      "roles": ["editor_datos", "gestor_contratos"],
      "permissions": {
        "permanent": ["read:proyectos", "write:proyectos", ...],
        "temporary": [
          {
            "permission": "write:contratos",
            "expires_at": "2025-12-31T23:59:59Z"
          }
        ],
        "custom": ["read:reportes_especiales"]
      },
      "centro_gestor": "Secretaría de Infraestructura",
      "can_access_centros": ["Secretaría de Infraestructura"]
    }
```

### 10. Auditoría

```python
GET /auth/audit-logs
  """
  Lista logs de auditoría
  Requiere: view:audit_logs (super_admin, admin_general)
  """
  Query:
    - user_uid: "user123"
    - action: "CREATE_PROYECTO"
    - resource_type: "proyectos_presupuestales"
    - date_from: "2025-11-01"
    - date_to: "2025-11-23"
    - limit: 100
    - offset: 0

  Headers:
    - Authorization: Bearer {admin_token}

  Response 200:
    {
      "success": true,
      "logs": [ ...audit_logs ],
      "total": 1500,
      "page": 1,
      "pages": 15
    }

GET /auth/audit-logs/my
  """
  Obtiene logs del usuario actual
  """
  Headers:
    - Authorization: Bearer {idToken}

  Response 200:
    {
      "success": true,
      "user_uid": "user123",
      "logs": [ ...user_logs ],
      "total": 45
    }

GET /auth/audit-logs/user/{uid}
  """
  Obtiene logs de un usuario específico
  Requiere: view:audit_logs (super_admin, admin_general)
  """
  Response 200:
    {
      "success": true,
      "user_uid": "user123",
      "user_name": "Juan Pérez",
      "logs": [ ...user_logs ],
      "summary": {
        "total_actions": 150,
        "last_login": "2025-11-23T14:30:00Z",
        "most_common_action": "READ_PROYECTO",
        "failed_attempts": 2
      }
    }
```

### 11. Notificaciones

```python
POST /auth/notifications/send
  """
  Envía notificación personalizada a un usuario
  Requiere: manage:users (super_admin)
  """
  Body:
    {
      "user_uid": "user123",
      "type": "email",  # email, sms, both
      "subject": "Cambio de permisos",
      "message": "Se han actualizado tus permisos de acceso",
      "template": "permission_change",  # Opcional
      "data": {  # Variables para template
        "permission": "write:proyectos",
        "granted_by": "Admin Sistema"
      }
    }

  Headers:
    - Authorization: Bearer {admin_token}

  Response 200:
    {
      "success": true,
      "message": "Notificación enviada",
      "notification_id": "notif_123",
      "sent_via": ["email"]
    }

GET /auth/notifications/templates
  """
  Lista templates de notificaciones disponibles
  """
  Response 200:
    {
      "success": true,
      "templates": [
        {
          "id": "welcome",
          "name": "Bienvenida",
          "type": "email",
          "variables": ["user_name", "login_url"]
        },
        {
          "id": "permission_change",
          "name": "Cambio de Permisos",
          "type": "email",
          "variables": ["permission", "granted_by", "reason"]
        }
      ]
    }
```

---

## 🛡️ DECORADORES Y MIDDLEWARE

### Decorador @require_permission

```python
from auth_system.decorators import require_permission

@router.post("/proyectos-presupuestales/cargar-json")
@require_permission("write:proyectos")
async def cargar_proyectos(
    file: UploadFile,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    """
    Endpoint protegido con decorador de permisos
    """
    # El decorador valida automáticamente antes de ejecutar
    # Si no tiene permiso, retorna 403 Forbidden
    pass
```

### Decorador @require_role

```python
from auth_system.decorators import require_role

@router.get("/admin/users")
@require_role(["admin_general", "super_admin"])
async def list_users(current_user: dict = Depends(get_current_user)):
    """
    Solo usuarios con rol admin_general o super_admin
    """
    pass
```

### Decorador @require_centro_gestor_access

```python
from auth_system.decorators import require_centro_gestor_access

@router.get("/proyectos-presupuestales/centro-gestor/{nombre_centro_gestor}")
@require_centro_gestor_access()
async def get_proyectos_by_centro(
    nombre_centro_gestor: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Valida que el usuario tenga acceso al centro gestor solicitado
    """
    pass
```

### Middleware de Autorización

```python
# Middleware automático que se ejecuta en cada request
app.add_middleware(
    AuthorizationMiddleware,
    public_paths=[
        "/",
        "/docs",
        "/openapi.json",
        "/auth/login",
        "/auth/register",
        "/auth/google",
        "/auth/verify-magic-link"
    ],
    admin_only_paths=[
        "/admin/*",
        "/auth/roles",
        "/auth/audit-logs"
    ]
)
```

---

## 📧 SISTEMA DE NOTIFICACIONES

### Eventos que disparan notificaciones

```python
NOTIFICATION_EVENTS = {
    # Cuenta
    "user.created": {
        "template": "welcome",
        "channels": ["email"],
        "required": True
    },
    "user.email_verified": {
        "template": "email_verified",
        "channels": ["email"],
        "required": False
    },
    "user.password_changed": {
        "template": "password_changed",
        "channels": ["email"],
        "required": True
    },

    # Roles y Permisos
    "user.roles_updated": {
        "template": "roles_changed",
        "channels": ["email"],
        "required": True
    },
    "user.permission_granted": {
        "template": "permission_granted",
        "channels": ["email"],
        "required": True
    },
    "user.permission_revoked": {
        "template": "permission_revoked",
        "channels": ["email"],
        "required": True
    },
    "user.temporary_permission_expiring": {
        "template": "temp_permission_expiring",
        "channels": ["email"],
        "required": True,
        "trigger_days_before": 1
    },

    # Seguridad
    "auth.login_failed_multiple": {
        "template": "security_alert",
        "channels": ["email", "sms"],
        "required": True,
        "threshold": 5
    },
    "auth.password_reset_requested": {
        "template": "password_reset",
        "channels": ["email"],
        "required": True
    },

    # Estado de cuenta
    "user.account_disabled": {
        "template": "account_disabled",
        "channels": ["email"],
        "required": True
    },
    "user.account_locked": {
        "template": "account_locked",
        "channels": ["email", "sms"],
        "required": True
    }
}
```

### Templates de Email

```python
EMAIL_TEMPLATES = {
    "welcome": {
        "subject": "Bienvenido a Gestor de Proyectos Cali",
        "variables": ["user_name", "email", "login_url", "support_email"]
    },
    "password_changed": {
        "subject": "Tu contraseña ha sido actualizada",
        "variables": ["user_name", "change_timestamp", "ip_address"]
    },
    "roles_changed": {
        "subject": "Actualización de roles y permisos",
        "variables": ["user_name", "roles_added", "roles_removed", "changed_by"]
    },
    "permission_granted": {
        "subject": "Nuevo permiso otorgado",
        "variables": ["user_name", "permission", "granted_by", "reason", "expires_at"]
    }
}
```

---

## 📁 ESTRUCTURA DE CARPETAS

```
a:\programing_workspace\proyectos_cali_alcaldia_etl\
│
├── auth_system/                      # Módulo principal de autenticación
│   ├── __init__.py
│   ├── constants.py                  # ROLES_PREDEFINIDOS, PERMISSIONS_CATALOG
│   ├── models.py                     # Pydantic models (schemas)
│   ├── permissions.py                # check_permission(), get_user_permissions()
│   ├── roles.py                      # get_role(), assign_role(), remove_role()
│   ├── decorators.py                 # @require_permission, @require_role
│   ├── middleware.py                 # AuthorizationMiddleware
│   └── utils.py                      # Helper functions
│
├── auth_api/                         # Endpoints API
│   ├── __init__.py
│   ├── enhanced_auth.py              # Verificación email/SMS/magic links
│   ├── user_management.py            # CRUD usuarios + roles
│   ├── role_management.py            # CRUD roles del sistema
│   ├── permission_management.py      # Gestión de permisos
│   ├── audit.py                      # Endpoints de auditoría
│   └── notifications.py              # Envío de notificaciones
│
├── database/
│   ├── config.py                     # (existente) Firebase config
│   └── auth_db.py                    # Operaciones Firestore para auth
│
├── services/                         # Servicios externos
│   ├── __init__.py
│   ├── email_service.py              # SendGrid / Firebase Email
│   ├── sms_service.py                # Twilio / Firebase Phone Auth
│   └── notification_service.py       # Orchestrador
│
└── scripts/
    └── init_roles_permissions.py     # ✅ Script de inicialización (CREADO)
```

---

## 📝 MODELOS PYDANTIC

### Request Models

```python
from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional
from datetime import datetime

class UserLoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)

class UserRegistrationRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)
    full_name: str = Field(..., min_length=3)
    phone_number: Optional[str] = None
    centro_gestor: Optional[str] = None

class SendCodeRequest(BaseModel):
    email: EmailStr
    purpose: str = Field(..., regex="^(login|verification|password_reset)$")

class VerifyCodeRequest(BaseModel):
    email: EmailStr
    code: str = Field(..., min_length=6, max_length=6)

class SendMagicLinkRequest(BaseModel):
    email: EmailStr
    redirect_url: Optional[str] = "https://app.cali.gov.co/dashboard"

class AssignRolesRequest(BaseModel):
    roles: List[str]
    reason: Optional[str] = None

class GrantTemporaryPermissionRequest(BaseModel):
    permission: str
    expires_at: datetime
    reason: str

class CreateRoleRequest(BaseModel):
    role_id: str = Field(..., regex="^[a-z_]+$")
    name: str
    description: str
    permissions: List[str]
    level: int = Field(..., ge=0, le=10)
    color: Optional[str] = "#4ECDC4"
    icon: Optional[str] = "user"

class UpdateRoleRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    permissions: Optional[List[str]] = None
    is_active: Optional[bool] = None

class CheckPermissionRequest(BaseModel):
    permission: str
    resource_id: Optional[str] = None
    centro_gestor: Optional[str] = None

class SendNotificationRequest(BaseModel):
    user_uid: str
    type: str = Field(..., regex="^(email|sms|both)$")
    subject: str
    message: str
    template: Optional[str] = None
    data: Optional[dict] = {}
```

### Response Models

```python
class UserResponse(BaseModel):
    uid: str
    email: str
    full_name: str
    phone_number: Optional[str]
    roles: List[str]
    centro_gestor_assigned: Optional[str]
    email_verified: bool
    phone_verified: bool
    is_active: bool
    created_at: datetime
    last_login_at: Optional[datetime]

class RoleResponse(BaseModel):
    role_id: str
    name: str
    description: str
    level: int
    permissions: List[str]
    is_active: bool
    users_count: int
    color: str
    icon: str

class PermissionCheckResponse(BaseModel):
    success: bool
    has_permission: bool
    permission: str
    granted_by: str  # "role:editor_datos" | "custom" | "temporary"
    scope_valid: bool

class AuditLogResponse(BaseModel):
    log_id: str
    timestamp: datetime
    user_uid: str
    user_email: str
    action: str
    resource_type: str
    resource_id: Optional[str]
    success: bool
    ip_address: str
    centro_gestor: Optional[str]
```

---

## 🔒 VALIDACIONES Y REGLAS DE NEGOCIO

### Validación de Permisos con Scope

```python
def check_permission_with_scope(
    user: dict,
    permission: str,
    resource_data: dict = None
) -> bool:
    """
    Valida permiso considerando el scope (centro gestor)

    Ejemplos:
    - "write:proyectos" -> puede escribir en cualquier centro
    - "write:proyectos:own_centro" -> solo en su centro asignado
    """
    # Si el permiso no tiene scope, es global
    if ":" not in permission or permission.count(":") == 1:
        return permission in user['permissions']

    # Extraer scope del permiso
    action, resource, scope = permission.split(":")

    if scope == "own_centro":
        # Validar que el recurso pertenece al centro del usuario
        if resource_data and 'nombre_centro_gestor' in resource_data:
            return (
                resource_data['nombre_centro_gestor'] == user.get('centro_gestor_assigned')
                and f"{action}:{resource}:{scope}" in user['permissions']
            )

    return False
```

### Rate Limiting

```python
RATE_LIMITS = {
    "send_verification_email": {
        "max_attempts": 3,
        "window_minutes": 60
    },
    "send_code": {
        "max_attempts": 5,
        "window_minutes": 15
    },
    "verify_code": {
        "max_attempts": 3,
        "window_minutes": 10
    },
    "login": {
        "max_attempts": 5,
        "lockout_minutes": 30
    }
}
```

### Expiración de Códigos

```python
CODE_EXPIRATION = {
    "email_verification": 600,      # 10 minutos
    "sms_verification": 300,        # 5 minutos
    "password_reset": 900,          # 15 minutos
    "magic_link": 900,              # 15 minutos
    "login_code": 600               # 10 minutos
}
```

---

## 🚀 INTEGRACIÓN CON NEXTJS

### Hook de Autenticación

```typescript
// hooks/useAuth.ts
import { getAuth } from "firebase/auth";

export const useAuth = () => {
  const auth = getAuth();

  const checkPermission = async (permission: string) => {
    const user = auth.currentUser;
    if (!user) return false;

    const idToken = await user.getIdToken();
    const response = await fetch("/auth/check-permission", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${idToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ permission }),
    });

    const data = await response.json();
    return data.has_permission;
  };

  const getMyPermissions = async () => {
    const user = auth.currentUser;
    if (!user) return null;

    const idToken = await user.getIdToken();
    const response = await fetch("/auth/my-permissions", {
      headers: { Authorization: `Bearer ${idToken}` },
    });

    return response.json();
  };

  return { checkPermission, getMyPermissions };
};
```

### Componente Protegido

```typescript
// components/ProtectedButton.tsx
import { useAuth } from "@/hooks/useAuth";
import { useState, useEffect } from "react";

export function ProtectedButton({
  permission,
  children,
  onClick,
}: {
  permission: string;
  children: React.ReactNode;
  onClick: () => void;
}) {
  const { checkPermission } = useAuth();
  const [hasPermission, setHasPermission] = useState(false);

  useEffect(() => {
    checkPermission(permission).then(setHasPermission);
  }, [permission]);

  if (!hasPermission) return null;

  return <button onClick={onClick}>{children}</button>;
}

// Uso
<ProtectedButton permission="write:proyectos" onClick={handleCreate}>
  Crear Proyecto
</ProtectedButton>;
```

---

## ✅ CHECKLIST DE IMPLEMENTACIÓN

### Fase 1: Base de Datos ✅

- [x] Script de inicialización creado: `scripts/init_roles_permissions.py`
- [ ] Ejecutar script para crear colecciones `roles` y `permissions`
- [ ] Actualizar colección `users` con nuevos campos
- [ ] Crear colección `audit_logs`
- [ ] Crear colección `verification_codes`

### Fase 2: Sistema de Permisos

- [ ] Implementar `auth_system/constants.py`
- [ ] Implementar `auth_system/models.py`
- [ ] Implementar `auth_system/permissions.py`
- [ ] Implementar `auth_system/roles.py`
- [ ] Implementar `auth_system/decorators.py`
- [ ] Implementar `auth_system/middleware.py`

### Fase 3: Endpoints de Verificación

- [ ] POST /auth/send-verification-email
- [ ] POST /auth/verify-email
- [ ] POST /auth/send-code
- [ ] POST /auth/verify-code
- [ ] POST /auth/send-magic-link
- [ ] GET /auth/verify-magic-link
- [ ] POST /auth/forgot-password
- [ ] POST /auth/reset-password

### Fase 4: Endpoints de Gestión

- [ ] GET /auth/users/{uid}/roles
- [ ] POST /auth/users/{uid}/roles
- [ ] DELETE /auth/users/{uid}/roles/{role_id}
- [ ] POST /auth/users/{uid}/temporary-permissions
- [ ] GET /auth/users/{uid}/temporary-permissions
- [ ] DELETE /auth/users/{uid}/temporary-permissions/{permission}

### Fase 5: Gestión de Roles

- [ ] GET /auth/roles
- [ ] POST /auth/roles
- [ ] PUT /auth/roles/{role_id}
- [ ] DELETE /auth/roles/{role_id}

### Fase 6: Verificación de Permisos

- [ ] POST /auth/check-permission
- [ ] GET /auth/my-permissions

### Fase 7: Auditoría

- [ ] GET /auth/audit-logs
- [ ] GET /auth/audit-logs/my
- [ ] GET /auth/audit-logs/user/{uid}

### Fase 8: Notificaciones

- [ ] POST /auth/notifications/send
- [ ] GET /auth/notifications/templates
- [ ] Implementar `services/email_service.py`
- [ ] Implementar `services/sms_service.py` (opcional)
- [ ] Implementar `services/notification_service.py`

### Fase 9: Protección de Endpoints Existentes

- [ ] Agregar decoradores a endpoints de proyectos
- [ ] Agregar decoradores a endpoints de unidades
- [ ] Agregar decoradores a endpoints de contratos
- [ ] Agregar decoradores a endpoints de empréstito

### Fase 10: Testing y Documentación

- [ ] Tests unitarios de permisos
- [ ] Tests de integración de endpoints
- [ ] Actualizar OpenAPI/Swagger docs
- [ ] Guía de migración de usuarios existentes

---

## 📚 REFERENCIAS Y RECURSOS

### Firebase Authentication

- [Firebase Auth REST API](https://firebase.google.com/docs/reference/rest/auth)
- [Custom Email Templates](https://firebase.google.com/docs/auth/custom-email-handler)
- [Phone Auth](https://firebase.google.com/docs/auth/web/phone-auth)

### FastAPI

- [Security](https://fastapi.tiangolo.com/tutorial/security/)
- [Middleware](https://fastapi.tiangolo.com/tutorial/middleware/)
- [Dependencies](https://fastapi.tiangolo.com/tutorial/dependencies/)

### Servicios de Email/SMS

- [SendGrid Python](https://github.com/sendgrid/sendgrid-python)
- [Twilio Python](https://www.twilio.com/docs/libraries/python)
- [AWS SNS](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html)

---

## 🎯 PRÓXIMOS PASOS

1. ✅ **Script de inicialización creado** - `scripts/init_roles_permissions.py`
2. **Ejecutar script** para crear roles y permisos en Firestore
3. **Implementar** sistema de permisos (Fase 2)
4. **Crear endpoints** de verificación (Fase 3)
5. **Implementar** gestión de roles (Fases 4-6)
6. **Agregar** auditoría completa (Fase 7)
7. **Configurar** notificaciones (Fase 8)
8. **Proteger** endpoints existentes (Fase 9)
9. **Testing** completo (Fase 10)
10. **Deploy** a producción

---

## ⚠️ CAMBIOS IMPORTANTES

### Restricción de Permisos por Rol

**`super_admin`** - ✅ EXCLUSIVAMENTE con acceso a gestión de usuarios

- **Puede:** TODO incluyendo gestión de usuarios
- Permiso especial: `manage:users`
- **ES EL ÚNICO** que puede crear, editar, eliminar usuarios y asignar/remover roles

**`admin_general`** - ❌ SIN acceso a gestión de usuarios

- **Puede:** Gestionar datos, roles y permisos del sistema
- **NO puede:** Crear, editar o eliminar usuarios
- **NO puede:** Asignar o remover roles de usuarios

**`admin_centro_gestor`** - ❌ SIN acceso a gestión de usuarios

- **Puede:** Gestionar datos de su centro gestor
- **NO puede:** Gestionar usuarios (ni siquiera de su centro)
- Debe solicitar a super_admin para cambios de usuarios

---

**Documento Vivo:** Este archivo debe actualizarse conforme se implementen features y se descubran nuevos requisitos.

**Versión:** 1.1  
**Última Actualización:** 23 de Noviembre 2025 - Restricción de permisos de usuarios para admin_general
