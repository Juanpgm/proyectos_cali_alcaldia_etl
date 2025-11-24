"""
Script de Inicialización de Roles y Permisos en Firestore

Este script crea la estructura inicial de roles y permisos en Firebase Firestore.
Ejecutar una sola vez para configurar el sistema de autenticación.

Autor: Juan Pablo GM
Fecha: 23 de Noviembre 2025
"""

import sys
from pathlib import Path

# Agregar el directorio raíz al path para importar database.config
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.config import get_firestore_client, initialize_firebase
from datetime import datetime
from typing import Dict, List


# ============================================================================
# DEFINICIÓN DE ROLES Y PERMISOS
# ============================================================================

ROLES_PREDEFINIDOS = {
    "super_admin": {
        "name": "Super Administrador",
        "level": 0,
        "permissions": ["*"],  # Acceso total
        "description": "Control absoluto del sistema incluyendo gestión de usuarios",
        "color": "#FF0000",
        "icon": "shield",
        "is_active": True,
        "can_be_deleted": False
    },
    
    "admin_general": {
        "name": "Administrador General",
        "level": 1,
        "permissions": [
            # Lectura total
            "read:*",
            # Proyectos
            "write:proyectos", "delete:proyectos",
            # Unidades
            "write:unidades", "delete:unidades",
            # Contratos
            "write:contratos", "delete:contratos",
            # GeoJSON
            "upload:geojson", "download:geojson",
            # Reportes
            "create:reportes_contratos",
            # Gestión de roles (pero NO usuarios)
            "manage:roles",
            # Auditoría
            "view:audit_logs",
            # Exportación
            "export:*"
        ],
        "description": "Administración completa de datos y roles, sin acceso a gestión de usuarios",
        "color": "#FF6B6B",
        "icon": "user-shield",
        "is_active": True,
        "can_be_deleted": False
    },
    
    "admin_centro_gestor": {
        "name": "Administrador de Centro Gestor",
        "level": 2,
        "permissions": [
            # Lectura limitada a su centro
            "read:proyectos:own_centro",
            "read:unidades:own_centro",
            "read:contratos:own_centro",
            # Escritura limitada a su centro
            "write:proyectos:own_centro",
            "write:unidades:own_centro",
            "write:contratos:own_centro",
            # Eliminación limitada a su centro
            "delete:proyectos:own_centro",
            "delete:unidades:own_centro",
            # Reportes de su centro
            "create:reportes_contratos:own_centro",
            # Exportación de su centro
            "export:proyectos:own_centro",
            "export:unidades:own_centro",
            "export:contratos:own_centro",
            # GeoJSON
            "upload:geojson",
            "download:geojson"
        ],
        "description": "Gestión completa de datos de su centro gestor (sin acceso a usuarios)",
        "color": "#4ECDC4",
        "icon": "building",
        "is_active": True,
        "can_be_deleted": False
    },
    
    "editor_datos": {
        "name": "Editor de Datos",
        "level": 3,
        "permissions": [
            # Lectura amplia
            "read:proyectos", "read:unidades", "read:contratos",
            # Escritura sin eliminación
            "write:proyectos", "write:unidades",
            # GeoJSON
            "upload:geojson",
            # Exportación
            "export:proyectos", "export:unidades"
        ],
        "description": "Edición de datos sin capacidad de eliminación",
        "color": "#95E1D3",
        "icon": "edit",
        "is_active": True,
        "can_be_deleted": False
    },
    
    "gestor_contratos": {
        "name": "Gestor de Contratos",
        "level": 3,
        "permissions": [
            # Contratos
            "read:contratos",
            "write:contratos",
            # Reportes
            "create:reportes_contratos",
            # Lectura de referencia de proyectos
            "read:proyectos:reference",
            # Exportación
            "export:contratos"
        ],
        "description": "Gestión exclusiva de contratos y sus reportes",
        "color": "#F38181",
        "icon": "file-contract",
        "is_active": True,
        "can_be_deleted": False
    },
    
    "analista": {
        "name": "Analista de Datos",
        "level": 4,
        "permissions": [
            # Solo lectura de todo
            "read:proyectos", "read:unidades", "read:contratos",
            "read:reportes_contratos",
            # Exportación completa
            "export:proyectos", "export:unidades", "export:contratos",
            # Descarga de GeoJSON
            "download:geojson",
            # Dashboard avanzado
            "view:dashboard:advanced"
        ],
        "description": "Análisis y exportación de datos sin capacidad de edición",
        "color": "#A8E6CF",
        "icon": "chart-line",
        "is_active": True,
        "can_be_deleted": False
    },
    
    "visualizador": {
        "name": "Visualizador",
        "level": 5,
        "permissions": [
            # Lectura básica
            "read:proyectos:basic",
            "read:unidades:basic",
            "read:contratos:basic",
            # Dashboard básico
            "view:dashboard:basic"
        ],
        "description": "Solo lectura de datos básicos sin capacidad de exportación (ROL POR DEFECTO para nuevos usuarios)",
        "color": "#DCEDC8",
        "icon": "eye",
        "is_active": True,
        "can_be_deleted": False
    },
    
    "publico": {
        "name": "Usuario Público",
        "level": 6,
        "permissions": [
            # Solo datos públicos
            "read:proyectos:public",
            "read:unidades:public",
            "view:map:public"
        ],
        "description": "Acceso público muy limitado a información básica",
        "color": "#E0E0E0",
        "icon": "globe",
        "is_active": True,
        "can_be_deleted": False
    }
}


PERMISSIONS_CATALOG = {
    # ========== Proyectos Presupuestales ==========
    "read:proyectos": {
        "resource": "proyectos_presupuestales",
        "action": "read",
        "scope": "all",
        "description": "Ver todos los proyectos presupuestales",
        "category": "proyectos",
        "risk_level": "low"
    },
    "read:proyectos:own_centro": {
        "resource": "proyectos_presupuestales",
        "action": "read",
        "scope": "own_centro",
        "description": "Ver proyectos de su centro gestor",
        "category": "proyectos",
        "risk_level": "low"
    },
    "read:proyectos:basic": {
        "resource": "proyectos_presupuestales",
        "action": "read",
        "scope": "basic",
        "description": "Ver información básica de proyectos",
        "category": "proyectos",
        "risk_level": "low"
    },
    "read:proyectos:public": {
        "resource": "proyectos_presupuestales",
        "action": "read",
        "scope": "public",
        "description": "Ver proyectos públicos",
        "category": "proyectos",
        "risk_level": "low"
    },
    "read:proyectos:reference": {
        "resource": "proyectos_presupuestales",
        "action": "read",
        "scope": "reference",
        "description": "Ver proyectos solo como referencia",
        "category": "proyectos",
        "risk_level": "low"
    },
    "write:proyectos": {
        "resource": "proyectos_presupuestales",
        "action": "write",
        "scope": "all",
        "description": "Crear y editar todos los proyectos",
        "category": "proyectos",
        "risk_level": "medium"
    },
    "write:proyectos:own_centro": {
        "resource": "proyectos_presupuestales",
        "action": "write",
        "scope": "own_centro",
        "description": "Crear/editar proyectos de su centro",
        "category": "proyectos",
        "risk_level": "medium"
    },
    "delete:proyectos": {
        "resource": "proyectos_presupuestales",
        "action": "delete",
        "scope": "all",
        "description": "Eliminar cualquier proyecto",
        "category": "proyectos",
        "risk_level": "high"
    },
    "delete:proyectos:own_centro": {
        "resource": "proyectos_presupuestales",
        "action": "delete",
        "scope": "own_centro",
        "description": "Eliminar proyectos de su centro",
        "category": "proyectos",
        "risk_level": "high"
    },
    "export:proyectos": {
        "resource": "proyectos_presupuestales",
        "action": "export",
        "scope": "all",
        "description": "Exportar datos de proyectos",
        "category": "proyectos",
        "risk_level": "low"
    },
    "export:proyectos:own_centro": {
        "resource": "proyectos_presupuestales",
        "action": "export",
        "scope": "own_centro",
        "description": "Exportar proyectos de su centro",
        "category": "proyectos",
        "risk_level": "low"
    },
    
    # ========== Unidades de Proyecto ==========
    "read:unidades": {
        "resource": "unidades_proyecto",
        "action": "read",
        "scope": "all",
        "description": "Ver todas las unidades de proyecto",
        "category": "unidades",
        "risk_level": "low"
    },
    "read:unidades:own_centro": {
        "resource": "unidades_proyecto",
        "action": "read",
        "scope": "own_centro",
        "description": "Ver unidades de su centro",
        "category": "unidades",
        "risk_level": "low"
    },
    "read:unidades:basic": {
        "resource": "unidades_proyecto",
        "action": "read",
        "scope": "basic",
        "description": "Ver información básica de unidades",
        "category": "unidades",
        "risk_level": "low"
    },
    "read:unidades:public": {
        "resource": "unidades_proyecto",
        "action": "read",
        "scope": "public",
        "description": "Ver unidades públicas",
        "category": "unidades",
        "risk_level": "low"
    },
    "write:unidades": {
        "resource": "unidades_proyecto",
        "action": "write",
        "scope": "all",
        "description": "Crear y editar unidades",
        "category": "unidades",
        "risk_level": "medium"
    },
    "write:unidades:own_centro": {
        "resource": "unidades_proyecto",
        "action": "write",
        "scope": "own_centro",
        "description": "Crear/editar unidades de su centro",
        "category": "unidades",
        "risk_level": "medium"
    },
    "delete:unidades": {
        "resource": "unidades_proyecto",
        "action": "delete",
        "scope": "all",
        "description": "Eliminar unidades",
        "category": "unidades",
        "risk_level": "high"
    },
    "delete:unidades:own_centro": {
        "resource": "unidades_proyecto",
        "action": "delete",
        "scope": "own_centro",
        "description": "Eliminar unidades de su centro",
        "category": "unidades",
        "risk_level": "high"
    },
    "export:unidades": {
        "resource": "unidades_proyecto",
        "action": "export",
        "scope": "all",
        "description": "Exportar datos de unidades",
        "category": "unidades",
        "risk_level": "low"
    },
    "export:unidades:own_centro": {
        "resource": "unidades_proyecto",
        "action": "export",
        "scope": "own_centro",
        "description": "Exportar unidades de su centro",
        "category": "unidades",
        "risk_level": "low"
    },
    
    # ========== GeoJSON ==========
    "upload:geojson": {
        "resource": "geojson_files",
        "action": "upload",
        "scope": "all",
        "description": "Cargar archivos GeoJSON",
        "category": "geojson",
        "risk_level": "medium"
    },
    "download:geojson": {
        "resource": "geojson_files",
        "action": "download",
        "scope": "all",
        "description": "Descargar archivos GeoJSON",
        "category": "geojson",
        "risk_level": "low"
    },
    
    # ========== Contratos y Empréstito ==========
    "read:contratos": {
        "resource": "contratos_emprestito",
        "action": "read",
        "scope": "all",
        "description": "Ver todos los contratos",
        "category": "contratos",
        "risk_level": "low"
    },
    "read:contratos:own_centro": {
        "resource": "contratos_emprestito",
        "action": "read",
        "scope": "own_centro",
        "description": "Ver contratos de su centro",
        "category": "contratos",
        "risk_level": "low"
    },
    "read:contratos:basic": {
        "resource": "contratos_emprestito",
        "action": "read",
        "scope": "basic",
        "description": "Ver información básica de contratos",
        "category": "contratos",
        "risk_level": "low"
    },
    "write:contratos": {
        "resource": "contratos_emprestito",
        "action": "write",
        "scope": "all",
        "description": "Crear y editar contratos",
        "category": "contratos",
        "risk_level": "medium"
    },
    "write:contratos:own_centro": {
        "resource": "contratos_emprestito",
        "action": "write",
        "scope": "own_centro",
        "description": "Crear/editar contratos de su centro",
        "category": "contratos",
        "risk_level": "medium"
    },
    "delete:contratos": {
        "resource": "contratos_emprestito",
        "action": "delete",
        "scope": "all",
        "description": "Eliminar contratos",
        "category": "contratos",
        "risk_level": "high"
    },
    "create:reportes_contratos": {
        "resource": "reportes_contratos",
        "action": "create",
        "scope": "all",
        "description": "Crear reportes de contratos",
        "category": "contratos",
        "risk_level": "medium"
    },
    "create:reportes_contratos:own_centro": {
        "resource": "reportes_contratos",
        "action": "create",
        "scope": "own_centro",
        "description": "Crear reportes de contratos de su centro",
        "category": "contratos",
        "risk_level": "medium"
    },
    "read:reportes_contratos": {
        "resource": "reportes_contratos",
        "action": "read",
        "scope": "all",
        "description": "Ver reportes de contratos",
        "category": "contratos",
        "risk_level": "low"
    },
    "export:contratos": {
        "resource": "contratos_emprestito",
        "action": "export",
        "scope": "all",
        "description": "Exportar datos de contratos",
        "category": "contratos",
        "risk_level": "low"
    },
    "export:contratos:own_centro": {
        "resource": "contratos_emprestito",
        "action": "export",
        "scope": "own_centro",
        "description": "Exportar contratos de su centro",
        "category": "contratos",
        "risk_level": "low"
    },
    
    # ========== Administración de Usuarios (EXCLUSIVAMENTE SUPER_ADMIN) ==========
    "manage:users": {
        "resource": "users",
        "action": "manage",
        "scope": "all",
        "description": "Gestionar todos los usuarios del sistema (EXCLUSIVO de super_admin)",
        "category": "administration",
        "risk_level": "critical"
    },
    
    # ========== Gestión de Roles (SUPER_ADMIN y ADMIN_GENERAL) ==========
    "manage:roles": {
        "resource": "roles",
        "action": "manage",
        "scope": "all",
        "description": "Gestionar roles del sistema",
        "category": "administration",
        "risk_level": "critical"
    },
    "manage:permissions": {
        "resource": "permissions",
        "action": "manage",
        "scope": "all",
        "description": "Gestionar permisos del sistema",
        "category": "administration",
        "risk_level": "critical"
    },
    
    # ========== Auditoría ==========
    "view:audit_logs": {
        "resource": "audit_logs",
        "action": "view",
        "scope": "all",
        "description": "Ver logs de auditoría completos",
        "category": "audit",
        "risk_level": "low"
    },
    "view:audit_logs:own": {
        "resource": "audit_logs",
        "action": "view",
        "scope": "own",
        "description": "Ver sus propios logs",
        "category": "audit",
        "risk_level": "low"
    },
    "view:audit_logs:own_centro": {
        "resource": "audit_logs",
        "action": "view",
        "scope": "own_centro",
        "description": "Ver logs de su centro",
        "category": "audit",
        "risk_level": "low"
    },
    
    # ========== Dashboard y Visualización ==========
    "view:dashboard:advanced": {
        "resource": "dashboard",
        "action": "view",
        "scope": "advanced",
        "description": "Acceso a dashboard avanzado con análisis",
        "category": "visualization",
        "risk_level": "low"
    },
    "view:dashboard:basic": {
        "resource": "dashboard",
        "action": "view",
        "scope": "basic",
        "description": "Acceso a dashboard básico",
        "category": "visualization",
        "risk_level": "low"
    },
    "view:map:public": {
        "resource": "map",
        "action": "view",
        "scope": "public",
        "description": "Ver mapa público",
        "category": "visualization",
        "risk_level": "low"
    },
    
    # ========== Permisos Globales ==========
    "export:*": {
        "resource": "all",
        "action": "export",
        "scope": "all",
        "description": "Exportar cualquier tipo de dato",
        "category": "global",
        "risk_level": "medium"
    },
    "read:*": {
        "resource": "all",
        "action": "read",
        "scope": "all",
        "description": "Leer todos los recursos",
        "category": "global",
        "risk_level": "low"
    },
    "*": {
        "resource": "all",
        "action": "all",
        "scope": "all",
        "description": "Acceso total al sistema (Super Admin)",
        "category": "global",
        "risk_level": "critical"
    }
}


# ============================================================================
# FUNCIONES DE INICIALIZACIÓN
# ============================================================================

def init_permissions_collection(db) -> bool:
    """
    Inicializa la colección 'permissions' con el catálogo de permisos.
    
    Returns:
        bool: True si se completó exitosamente, False en caso contrario
    """
    try:
        print("\n📋 Inicializando catálogo de permisos...")
        
        permissions_ref = db.collection('permissions')
        
        count = 0
        for permission_id, permission_data in PERMISSIONS_CATALOG.items():
            doc_data = {
                "permission_id": permission_id,
                **permission_data,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "is_active": True
            }
            
            permissions_ref.document(permission_id).set(doc_data)
            count += 1
        
        print(f"✅ {count} permisos creados en Firestore")
        return True
        
    except Exception as e:
        print(f"❌ Error inicializando permisos: {e}")
        return False


def init_roles_collection(db) -> bool:
    """
    Inicializa la colección 'roles' con los roles predefinidos.
    
    Returns:
        bool: True si se completó exitosamente, False en caso contrario
    """
    try:
        print("\n👥 Inicializando roles del sistema...")
        
        roles_ref = db.collection('roles')
        
        count = 0
        for role_id, role_data in ROLES_PREDEFINIDOS.items():
            doc_data = {
                "role_id": role_id,
                **role_data,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "created_by": "system",
                "users_count": 0
            }
            
            roles_ref.document(role_id).set(doc_data)
            count += 1
            
            # Mostrar resumen del rol
            print(f"  ✓ {role_id}: {role_data['name']} (Nivel {role_data['level']}) - {len(role_data['permissions'])} permisos")
        
        print(f"✅ {count} roles creados en Firestore")
        return True
        
    except Exception as e:
        print(f"❌ Error inicializando roles: {e}")
        return False


def verify_collections(db) -> bool:
    """
    Verifica que las colecciones se hayan creado correctamente.
    
    Returns:
        bool: True si las colecciones existen, False en caso contrario
    """
    try:
        print("\n🔍 Verificando colecciones creadas...")
        
        # Verificar colección de permisos
        permissions_count = len(list(db.collection('permissions').limit(1000).stream()))
        print(f"  ✓ Permisos: {permissions_count} documentos")
        
        # Verificar colección de roles
        roles_count = len(list(db.collection('roles').limit(100).stream()))
        print(f"  ✓ Roles: {roles_count} documentos")
        
        if permissions_count > 0 and roles_count > 0:
            print("✅ Verificación exitosa")
            return True
        else:
            print("❌ Algunas colecciones están vacías")
            return False
        
    except Exception as e:
        print(f"❌ Error verificando colecciones: {e}")
        return False


def show_roles_summary():
    """Muestra un resumen de los roles configurados."""
    print("\n" + "=" * 70)
    print("📊 RESUMEN DE ROLES Y PERMISOS")
    print("=" * 70)
    
    for role_id, role_data in ROLES_PREDEFINIDOS.items():
        print(f"\n🔹 {role_data['name']} ({role_id})")
        print(f"   Nivel: {role_data['level']}")
        print(f"   Descripción: {role_data['description']}")
        print(f"   Permisos: {len(role_data['permissions'])}")
        
        # Mostrar primeros 5 permisos
        perms = role_data['permissions'][:5]
        for perm in perms:
            print(f"     - {perm}")
        if len(role_data['permissions']) > 5:
            print(f"     ... y {len(role_data['permissions']) - 5} más")
    
    print("\n" + "=" * 70)
    print(f"📋 Total de roles: {len(ROLES_PREDEFINIDOS)}")
    print(f"📋 Total de permisos únicos: {len(PERMISSIONS_CATALOG)}")
    print("=" * 70)


def main():
    """Función principal del script."""
    print("=" * 70)
    print("🚀 INICIALIZACIÓN DE ROLES Y PERMISOS")
    print("=" * 70)
    
    try:
        # Inicializar Firebase
        print("\n🔧 Conectando a Firebase...")
        initialize_firebase()
        db = get_firestore_client()
        print("✅ Conexión exitosa")
        
        # Mostrar resumen antes de inicializar
        show_roles_summary()
        
        # Solicitar confirmación
        print("\n⚠️  ADVERTENCIA: Este script creará/sobrescribirá las colecciones 'roles' y 'permissions'")
        respuesta = input("¿Deseas continuar? (si/no): ").strip().lower()
        
        if respuesta not in ['si', 's', 'yes', 'y']:
            print("❌ Operación cancelada por el usuario")
            return
        
        # Inicializar permisos
        if not init_permissions_collection(db):
            print("❌ Error inicializando permisos, abortando...")
            return
        
        # Inicializar roles
        if not init_roles_collection(db):
            print("❌ Error inicializando roles, abortando...")
            return
        
        # Verificar
        if not verify_collections(db):
            print("⚠️  Advertencia: Verificación falló")
            return
        
        print("\n" + "=" * 70)
        print("🎉 INICIALIZACIÓN COMPLETADA EXITOSAMENTE")
        print("=" * 70)
        print("\n📝 Próximos pasos:")
        print("  1. Asignar rol 'super_admin' al primer usuario administrador")
        print("  2. Crear usuarios y asignarles roles según corresponda")
        print("  3. Implementar los decoradores de permisos en los endpoints")
        print("  4. Configurar el middleware de autorización")
        print("\n💡 Documentación completa en: AUTH_SYSTEM_SPECIFICATION.md")
        
    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
        

if __name__ == "__main__":
    main()
