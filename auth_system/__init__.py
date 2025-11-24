"""
Auth System Module

Sistema de autenticación, autorización y gestión de permisos
para el Gestor de Proyectos de Cali.

Autor: Juan Pablo GM
Fecha: 23 de Noviembre 2025
"""

from .constants import DEFAULT_USER_ROLE
from .permissions import check_permission, get_user_permissions
from .decorators import require_permission, require_role, get_current_user

__all__ = [
    'DEFAULT_USER_ROLE',
    'check_permission',
    'get_user_permissions',
    'require_permission',
    'require_role',
    'get_current_user'
]
