"""
Middleware de Autenticación y Autorización

Middleware que intercepta requests para validar autenticación automáticamente.

Autor: Juan Pablo GM
Fecha: 23 de Noviembre 2025
"""

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from firebase_admin import auth
from typing import List
import re

from .constants import PUBLIC_PATHS


class AuthorizationMiddleware(BaseHTTPMiddleware):
    """
    Middleware que valida autenticación en todas las rutas
    excepto las rutas públicas especificadas.
    
    Uso en FastAPI:
        from fastapi import FastAPI
        from auth_system.middleware import AuthorizationMiddleware
        
        app = FastAPI()
        
        app.add_middleware(
            AuthorizationMiddleware,
            public_paths=["/", "/docs", "/auth/login", "/auth/register"]
        )
    """
    
    def __init__(
        self, 
        app, 
        public_paths: List[str] = None,
        admin_only_paths: List[str] = None
    ):
        """
        Inicializa el middleware.
        
        Args:
            app: Aplicación FastAPI
            public_paths: Lista de rutas que no requieren autenticación
            admin_only_paths: Lista de rutas que requieren super_admin
        """
        super().__init__(app)
        self.public_paths = public_paths or PUBLIC_PATHS
        self.admin_only_paths = admin_only_paths or []
    
    def is_public_path(self, path: str) -> bool:
        """
        Verifica si una ruta es pública.
        
        Soporta:
        - Rutas exactas: "/auth/login"
        - Wildcards: "/static/*"
        
        Args:
            path: Ruta a verificar
            
        Returns:
            True si es ruta pública, False en caso contrario
        """
        for public_path in self.public_paths:
            # Soporte para wildcards
            if public_path.endswith("*"):
                pattern = public_path.replace("*", ".*")
                if re.match(f"^{pattern}", path):
                    return True
            elif path == public_path:
                return True
        return False
    
    def is_admin_only_path(self, path: str) -> bool:
        """
        Verifica si una ruta requiere super_admin exclusivamente.
        
        Args:
            path: Ruta a verificar
            
        Returns:
            True si requiere super_admin, False en caso contrario
        """
        for admin_path in self.admin_only_paths:
            if admin_path.endswith("*"):
                pattern = admin_path.replace("*", ".*")
                if re.match(f"^{pattern}", path):
                    return True
            elif path == admin_path:
                return True
        return False
    
    async def dispatch(self, request: Request, call_next):
        """
        Intercepta cada request para validar autenticación.
        
        Args:
            request: Request de FastAPI
            call_next: Siguiente handler en la cadena
            
        Returns:
            Response del handler o error de autenticación
        """
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
                    "message": "Token de autenticación requerido",
                    "detail": "Incluye el token en el header: Authorization: Bearer {token}"
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
                'email_verified': decoded_token.get('email_verified', False),
                'token': decoded_token
            }
            
            # Verificar si requiere super_admin
            if self.is_admin_only_path(path):
                # Obtener roles del usuario
                from database.config import get_firestore_client
                db = get_firestore_client()
                user_doc = db.collection('users').document(decoded_token['uid']).get()
                
                if not user_doc.exists:
                    return JSONResponse(
                        status_code=403,
                        content={
                            "success": False,
                            "message": "Usuario no encontrado en el sistema"
                        }
                    )
                
                user_data = user_doc.to_dict()
                user_roles = user_data.get('roles', [])
                
                if 'super_admin' not in user_roles:
                    return JSONResponse(
                        status_code=403,
                        content={
                            "success": False,
                            "message": "Acceso denegado",
                            "detail": "Esta ruta requiere rol de super_admin"
                        }
                    )
            
            # Continuar con el request
            response = await call_next(request)
            return response
            
        except auth.InvalidIdTokenError:
            return JSONResponse(
                status_code=401,
                content={
                    "success": False,
                    "message": "Token inválido o malformado"
                }
            )
        except auth.ExpiredIdTokenError:
            return JSONResponse(
                status_code=401,
                content={
                    "success": False,
                    "message": "Token expirado",
                    "detail": "Por favor inicia sesión nuevamente"
                }
            )
        except auth.RevokedIdTokenError:
            return JSONResponse(
                status_code=401,
                content={
                    "success": False,
                    "message": "Token revocado",
                    "detail": "Tu sesión ha sido cerrada. Inicia sesión nuevamente"
                }
            )
        except Exception as e:
            return JSONResponse(
                status_code=401,
                content={
                    "success": False,
                    "message": "Error de autenticación",
                    "detail": str(e)
                }
            )


class AuditLogMiddleware(BaseHTTPMiddleware):
    """
    Middleware para registrar automáticamente acciones en audit_logs.
    
    Registra:
    - Usuario que ejecuta la acción
    - Endpoint accedido
    - Método HTTP
    - IP del cliente
    - Timestamp
    - Resultado (éxito/error)
    """
    
    def __init__(self, app, enable_logging: bool = True):
        """
        Inicializa el middleware de auditoría.
        
        Args:
            app: Aplicación FastAPI
            enable_logging: Si False, desactiva el logging (útil para testing)
        """
        super().__init__(app)
        self.enable_logging = enable_logging
    
    async def dispatch(self, request: Request, call_next):
        """
        Intercepta request para logging de auditoría.
        
        Args:
            request: Request de FastAPI
            call_next: Siguiente handler
            
        Returns:
            Response del handler
        """
        if not self.enable_logging:
            return await call_next(request)
        
        from datetime import datetime
        import uuid
        
        start_time = datetime.utcnow()
        request_id = str(uuid.uuid4())
        
        # Obtener información del usuario si está autenticado
        user_info = None
        if hasattr(request.state, 'user'):
            user_info = request.state.user
        
        # Ejecutar request
        try:
            response = await call_next(request)
            success = 200 <= response.status_code < 400
            status_code = response.status_code
            error_message = None
        except Exception as e:
            success = False
            status_code = 500
            error_message = str(e)
            raise
        finally:
            # Calcular tiempo de ejecución
            end_time = datetime.utcnow()
            execution_time_ms = int((end_time - start_time).total_seconds() * 1000)
            
            # Registrar en audit_logs si es una acción importante
            if user_info and self._should_log_action(request.method, request.url.path):
                try:
                    await self._log_action(
                        user_info=user_info,
                        request=request,
                        request_id=request_id,
                        success=success,
                        status_code=status_code,
                        error_message=error_message,
                        execution_time_ms=execution_time_ms,
                        timestamp=start_time
                    )
                except Exception as log_error:
                    # No fallar el request si el logging falla
                    print(f"⚠️ Error en audit log: {log_error}")
        
        return response
    
    def _should_log_action(self, method: str, path: str) -> bool:
        """
        Determina si una acción debe ser registrada en audit_logs.
        
        Args:
            method: Método HTTP
            path: Ruta del endpoint
            
        Returns:
            True si debe registrarse, False en caso contrario
        """
        # No loggear GET requests (demasiados)
        if method == "GET":
            return False
        
        # No loggear rutas de autenticación básica
        if path in ["/auth/login", "/auth/validate-session"]:
            return False
        
        # Loggear todo lo demás (POST, PUT, DELETE, PATCH)
        return True
    
    async def _log_action(
        self,
        user_info: dict,
        request: Request,
        request_id: str,
        success: bool,
        status_code: int,
        error_message: str,
        execution_time_ms: int,
        timestamp: datetime
    ):
        """
        Registra la acción en Firestore (colección audit_logs).
        
        Args:
            user_info: Información del usuario
            request: Request de FastAPI
            request_id: UUID del request
            success: Si la acción fue exitosa
            status_code: Código HTTP de respuesta
            error_message: Mensaje de error si aplica
            execution_time_ms: Tiempo de ejecución en ms
            timestamp: Timestamp del inicio del request
        """
        from database.config import get_firestore_client
        import uuid
        
        db = get_firestore_client()
        
        # Obtener información adicional del usuario
        user_doc = db.collection('users').document(user_info['uid']).get()
        user_data = user_doc.to_dict() if user_doc.exists else {}
        
        # Crear log entry
        log_data = {
            'log_id': f"log_{uuid.uuid4()}",
            'timestamp': timestamp,
            'request_id': request_id,
            
            # Usuario
            'user_uid': user_info['uid'],
            'user_email': user_info.get('email'),
            'user_name': user_data.get('full_name'),
            'user_roles': user_data.get('roles', []),
            
            # Request
            'request_method': request.method,
            'request_path': str(request.url.path),
            'ip_address': request.client.host if request.client else 'unknown',
            'user_agent': request.headers.get('User-Agent', 'unknown'),
            
            # Resultado
            'success': success,
            'http_status': status_code,
            'error_message': error_message,
            'execution_time_ms': execution_time_ms,
            
            # Clasificación
            'risk_level': self._classify_risk(request.method, request.url.path),
            'requires_review': not success and status_code >= 500
        }
        
        # Guardar en Firestore
        db.collection('audit_logs').document(log_data['log_id']).set(log_data)
    
    def _classify_risk(self, method: str, path: str) -> str:
        """
        Clasifica el nivel de riesgo de una acción.
        
        Args:
            method: Método HTTP
            path: Ruta del endpoint
            
        Returns:
            "critical", "high", "medium" o "low"
        """
        if "delete" in path.lower() or method == "DELETE":
            return "high"
        
        if "admin" in path or "users" in path or "roles" in path:
            return "critical"
        
        if method in ["POST", "PUT", "PATCH"]:
            return "medium"
        
        return "low"
