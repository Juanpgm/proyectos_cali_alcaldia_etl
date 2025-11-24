"""
Sistema de Gestión de Permisos

Funciones para verificar y obtener permisos de usuarios.

Autor: Juan Pablo GM
Fecha: 23 de Noviembre 2025
"""

from typing import List, Dict, Optional, Set
from datetime import datetime
from database.config import get_firestore_client


async def get_user_permissions(user_uid: str) -> List[str]:
    """
    Obtiene todos los permisos efectivos de un usuario.
    
    Incluye:
    - Permisos de roles asignados
    - Permisos personalizados
    - Permisos temporales activos
    
    Args:
        user_uid: UID del usuario en Firebase
        
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
        
        now = datetime.utcnow()
        
        for temp_perm in temporary_permissions:
            expires_at = temp_perm.get('expires_at')
            if expires_at and isinstance(expires_at, datetime):
                if expires_at > now:
                    active_temp_perms.append(temp_perm['permission'])
            elif expires_at:
                # Si es string, intentar parsear
                try:
                    from dateutil import parser
                    expires_dt = parser.parse(str(expires_at))
                    if expires_dt > now:
                        active_temp_perms.append(temp_perm['permission'])
                except:
                    pass
        
        # Obtener permisos de cada rol
        all_permissions: Set[str] = set(custom_permissions + active_temp_perms)
        
        for role_id in user_roles:
            role_doc = db.collection('roles').document(role_id).get()
            if role_doc.exists:
                role_data = role_doc.to_dict()
                role_permissions = role_data.get('permissions', [])
                all_permissions.update(role_permissions)
        
        return list(all_permissions)
        
    except Exception as e:
        print(f"❌ Error obteniendo permisos para {user_uid}: {e}")
        return []


async def check_permission(
    user_uid: str,
    required_permission: str,
    resource_data: Optional[Dict] = None
) -> bool:
    """
    Verifica si un usuario tiene un permiso específico.
    
    Soporta:
    - Permisos exactos: "write:proyectos"
    - Permisos con wildcard: "read:*" cubre "read:proyectos"
    - Permisos con scope: "write:proyectos:own_centro"
    - Super admin: "*" da acceso total
    
    Args:
        user_uid: UID del usuario
        required_permission: Permiso requerido (ej: "write:proyectos")
        resource_data: Datos del recurso para validación de scope
        
    Returns:
        True si tiene el permiso, False en caso contrario
    """
    try:
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
                    
                    if user_centro and resource_centro and user_centro == resource_centro:
                        own_centro_perm = f"{action}:{resource}:own_centro"
                        return own_centro_perm in permissions
        
        return False
        
    except Exception as e:
        print(f"❌ Error verificando permiso {required_permission} para {user_uid}: {e}")
        return False


async def get_user_roles(user_uid: str) -> List[str]:
    """
    Obtiene los roles asignados a un usuario.
    
    Args:
        user_uid: UID del usuario
        
    Returns:
        Lista de roles (ej: ["editor_datos", "gestor_contratos"])
    """
    try:
        db = get_firestore_client()
        user_doc = db.collection('users').document(user_uid).get()
        
        if not user_doc.exists:
            return []
        
        user_data = user_doc.to_dict()
        return user_data.get('roles', [])
        
    except Exception as e:
        print(f"❌ Error obteniendo roles para {user_uid}: {e}")
        return []


async def has_role(user_uid: str, role: str) -> bool:
    """
    Verifica si un usuario tiene un rol específico.
    
    Args:
        user_uid: UID del usuario
        role: Nombre del rol a verificar
        
    Returns:
        True si tiene el rol, False en caso contrario
    """
    roles = await get_user_roles(user_uid)
    return role in roles


async def has_any_role(user_uid: str, roles: List[str]) -> bool:
    """
    Verifica si un usuario tiene al menos uno de los roles especificados.
    
    Args:
        user_uid: UID del usuario
        roles: Lista de roles a verificar
        
    Returns:
        True si tiene al menos un rol, False en caso contrario
    """
    user_roles = await get_user_roles(user_uid)
    return any(role in roles for role in user_roles)


def get_permission_level(permission: str) -> str:
    """
    Obtiene el nivel de riesgo de un permiso.
    
    Args:
        permission: Permiso a evaluar
        
    Returns:
        "critical", "high", "medium" o "low"
    """
    if permission == "*":
        return "critical"
    
    if "manage:users" in permission or "manage:roles" in permission:
        return "critical"
    
    if "delete:" in permission:
        return "high"
    
    if "write:" in permission or "create:" in permission:
        return "medium"
    
    return "low"
