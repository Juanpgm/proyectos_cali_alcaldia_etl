"""
Constantes del Sistema de Autenticación

Definición de roles, permisos y configuraciones del sistema.

Autor: Juan Pablo GM
Fecha: 23 de Noviembre 2025
"""

from typing import Dict, List

# ============================================================================
# ROLES PREDEFINIDOS
# ============================================================================

ROLES_PREDEFINIDOS = {
    "super_admin": {
        "name": "Super Administrador",
        "level": 0,
        "permissions": ["*"],
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
            "manage:roles",
            "view:audit_logs",
            "upload:geojson", "download:geojson",
            "export:*"
        ],
        "description": "Administración completa de datos y roles, sin acceso a gestión de usuarios",
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
        "description": "Solo lectura de datos básicos (ROL POR DEFECTO para nuevos usuarios)",
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

# ============================================================================
# ROL POR DEFECTO
# ============================================================================

# Rol asignado automáticamente a todos los usuarios nuevos (excepto super_admin)
DEFAULT_USER_ROLE = "visualizador"

# ============================================================================
# RATE LIMITS
# ============================================================================

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

# ============================================================================
# EXPIRACIÓN DE CÓDIGOS
# ============================================================================

CODE_EXPIRATION = {
    "email_verification": 600,      # 10 minutos
    "sms_verification": 300,        # 5 minutos
    "password_reset": 900,          # 15 minutos
    "magic_link": 900,              # 15 minutos
    "login_code": 600               # 10 minutos
}

# ============================================================================
# RUTAS PÚBLICAS (No requieren autenticación)
# ============================================================================

PUBLIC_PATHS = [
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
    "/auth/reset-password",
    "/auth/verify-email",
    "/auth/verify-code"
]

# ============================================================================
# RUTAS ADMINISTRATIVAS (Requieren super_admin)
# ============================================================================

ADMIN_ONLY_PATHS = [
    "/admin/users",
    "/auth/users/*/roles",
    "/auth/users/*/temporary-permissions",
    "/auth/change-password",
    "/auth/user/*"
]
