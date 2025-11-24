"""
Decoradores de Autenticación y Autorización

Decoradores para proteger endpoints con autenticación y permisos.

Autor: Juan Pablo GM
Fecha: 23 de Noviembre 2025
"""

from functools import wraps
from typing import List, Optional, Callable
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from firebase_admin import auth

from .permissions import check_permission, get_user_permissions, has_any_role


# Security scheme para FastAPI
security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    """
    Dependency para obtener el usuario actual desde el token de Firebase.
    
    Uso en endpoints:
        async def my_endpoint(current_user: dict = Depends(get_current_user)):
            user_uid = current_user['uid']
            user_email = current_user['email']
    
    Args:
        credentials: Credenciales del header Authorization
        
    Returns:
        dict con datos del usuario (uid, email, etc.)
        
    Raises:
        HTTPException: Si el token es inválido o expirado
    """
    try:
        # Verificar token de Firebase
        id_token = credentials.credentials
        decoded_token = auth.verify_id_token(id_token)
        
        uid = decoded_token['uid']
        email = decoded_token.get('email')
        email_verified = decoded_token.get('email_verified', False)
        
        return {
            'uid': uid,
            'email': email,
            'email_verified': email_verified,
            'token': decoded_token
        }
        
    except auth.InvalidIdTokenError:
        raise HTTPException(
            status_code=401,
            detail="Token inválido o malformado"
        )
    except auth.ExpiredIdTokenError:
        raise HTTPException(
            status_code=401,
            detail="Token expirado. Por favor, inicia sesión nuevamente"
        )
    except auth.RevokedIdTokenError:
        raise HTTPException(
            status_code=401,
            detail="Token revocado. Por favor, inicia sesión nuevamente"
        )
    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail=f"Error de autenticación: {str(e)}"
        )


def require_permission(permission: str, resource_data_key: Optional[str] = None):
    """
    Decorador para requerir un permiso específico en un endpoint.
    
    Uso básico:
        @router.post("/proyectos")
        @require_permission("write:proyectos")
        async def create_proyecto(
            proyecto: ProyectoCreate,
            current_user: dict = Depends(get_current_user)
        ):
            # El decorador valida el permiso antes de ejecutar
            pass
    
    Uso con scope (validación de centro gestor):
        @router.put("/proyectos/{proyecto_id}")
        @require_permission("write:proyectos:own_centro", resource_data_key="proyecto")
        async def update_proyecto(
            proyecto_id: str,
            proyecto: ProyectoUpdate,
            current_user: dict = Depends(get_current_user)
        ):
            # El decorador valida que el proyecto pertenezca al centro del usuario
            # Debe retornar un dict con 'nombre_centro_gestor'
            pass
    
    Args:
        permission: Permiso requerido (ej: "write:proyectos")
        resource_data_key: Key en kwargs que contiene los datos del recurso
                          para validación de scope
    
    Returns:
        Decorador que valida el permiso antes de ejecutar la función
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Obtener current_user de los kwargs
            current_user = kwargs.get('current_user')
            if not current_user:
                raise HTTPException(
                    status_code=401,
                    detail="Usuario no autenticado. Token requerido"
                )
            
            user_uid = current_user['uid']
            
            # Obtener datos del recurso si se especificó
            resource_data = None
            if resource_data_key and resource_data_key in kwargs:
                resource_data = kwargs[resource_data_key]
                # Si es un objeto Pydantic, convertir a dict
                if hasattr(resource_data, 'dict'):
                    resource_data = resource_data.dict()
                elif hasattr(resource_data, 'model_dump'):
                    resource_data = resource_data.model_dump()
            
            # Verificar permiso
            has_permission = await check_permission(user_uid, permission, resource_data)
            
            if not has_permission:
                # Obtener permisos del usuario para mensaje más informativo
                user_permissions = await get_user_permissions(user_uid)
                
                raise HTTPException(
                    status_code=403,
                    detail={
                        "message": "Permiso denegado",
                        "required_permission": permission,
                        "user_email": current_user.get('email'),
                        "hint": "Contacta al administrador para solicitar este permiso"
                    }
                )
            
            # Permiso concedido, ejecutar función
            return await func(*args, **kwargs)
        
        return wrapper
    return decorator


def require_role(allowed_roles: List[str]):
    """
    Decorador para requerir uno o más roles específicos en un endpoint.
    
    Uso:
        @router.get("/admin/dashboard")
        @require_role(["super_admin", "admin_general"])
        async def admin_dashboard(current_user: dict = Depends(get_current_user)):
            # Solo super_admin y admin_general pueden acceder
            pass
    
    Args:
        allowed_roles: Lista de roles permitidos
    
    Returns:
        Decorador que valida el rol antes de ejecutar la función
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_user = kwargs.get('current_user')
            if not current_user:
                raise HTTPException(
                    status_code=401,
                    detail="Usuario no autenticado. Token requerido"
                )
            
            user_uid = current_user['uid']
            
            # Verificar si tiene al menos uno de los roles permitidos
            has_required_role = await has_any_role(user_uid, allowed_roles)
            
            if not has_required_role:
                raise HTTPException(
                    status_code=403,
                    detail={
                        "message": "Rol insuficiente",
                        "required_roles": allowed_roles,
                        "user_email": current_user.get('email'),
                        "hint": "Tu rol actual no tiene acceso a este recurso"
                    }
                )
            
            # Rol válido, ejecutar función
            return await func(*args, **kwargs)
        
        return wrapper
    return decorator


def require_super_admin():
    """
    Decorador para endpoints que requieren exclusivamente super_admin.
    
    Uso:
        @router.post("/admin/users")
        @require_super_admin()
        async def create_user(
            user_data: UserCreate,
            current_user: dict = Depends(get_current_user)
        ):
            # Solo super_admin puede acceder
            pass
    
    Returns:
        Decorador que valida rol super_admin antes de ejecutar
    """
    return require_role(["super_admin"])


def require_email_verified():
    """
    Decorador para requerir que el email del usuario esté verificado.
    
    Uso:
        @router.post("/sensitive-action")
        @require_email_verified()
        async def sensitive_action(current_user: dict = Depends(get_current_user)):
            # Solo usuarios con email verificado pueden acceder
            pass
    
    Returns:
        Decorador que valida email verificado antes de ejecutar
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_user = kwargs.get('current_user')
            if not current_user:
                raise HTTPException(
                    status_code=401,
                    detail="Usuario no autenticado"
                )
            
            email_verified = current_user.get('email_verified', False)
            
            if not email_verified:
                raise HTTPException(
                    status_code=403,
                    detail={
                        "message": "Email no verificado",
                        "user_email": current_user.get('email'),
                        "hint": "Por favor verifica tu email antes de realizar esta acción"
                    }
                )
            
            return await func(*args, **kwargs)
        
        return wrapper
    return decorator


def optional_auth():
    """
    Decorador para endpoints que pueden funcionar con o sin autenticación.
    Si hay token, lo valida y agrega current_user, si no hay token, continúa.
    
    Uso:
        @router.get("/public-data")
        @optional_auth()
        async def get_public_data(current_user: Optional[dict] = None):
            if current_user:
                # Usuario autenticado, mostrar más datos
                pass
            else:
                # Usuario público, mostrar datos limitados
                pass
    
    Returns:
        Decorador que valida token si está presente
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Intentar obtener token del header
            try:
                from fastapi import Request
                request = kwargs.get('request')
                if request and hasattr(request, 'headers'):
                    auth_header = request.headers.get('Authorization')
                    if auth_header and auth_header.startswith('Bearer '):
                        token = auth_header.split('Bearer ')[1]
                        try:
                            decoded_token = auth.verify_id_token(token)
                            kwargs['current_user'] = {
                                'uid': decoded_token['uid'],
                                'email': decoded_token.get('email'),
                                'email_verified': decoded_token.get('email_verified', False),
                                'token': decoded_token
                            }
                        except:
                            # Token inválido, continuar sin usuario
                            pass
            except:
                pass
            
            return await func(*args, **kwargs)
        
        return wrapper
    return decorator
