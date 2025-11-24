"""
Utilidades del Sistema de Autenticación

Funciones auxiliares para el sistema de autenticación y autorización.

Autor: Juan Pablo GM
Fecha: 23 de Noviembre 2025
"""

from typing import Dict, List, Optional
from datetime import datetime, timedelta
import hashlib
import secrets
import string


def generate_verification_code(length: int = 6) -> str:
    """
    Genera un código de verificación numérico aleatorio.
    
    Args:
        length: Longitud del código (default: 6 dígitos)
        
    Returns:
        Código numérico como string
    """
    return ''.join(secrets.choice(string.digits) for _ in range(length))


def generate_magic_link_token(length: int = 32) -> str:
    """
    Genera un token seguro para magic links.
    
    Args:
        length: Longitud del token en bytes (default: 32)
        
    Returns:
        Token hexadecimal seguro
    """
    return secrets.token_urlsafe(length)


def hash_code(code: str, salt: str = "") -> str:
    """
    Genera un hash SHA-256 de un código.
    
    Args:
        code: Código a hashear
        salt: Salt opcional para mayor seguridad
        
    Returns:
        Hash hexadecimal del código
    """
    return hashlib.sha256(f"{code}{salt}".encode()).hexdigest()


def mask_email(email: str) -> str:
    """
    Enmascara un email para mostrar en logs/notificaciones.
    
    Ejemplo: "juan.perez@cali.gov.co" -> "j***@cali.gov.co"
    
    Args:
        email: Email a enmascarar
        
    Returns:
        Email enmascarado
    """
    if not email or '@' not in email:
        return email
    
    username, domain = email.split('@', 1)
    
    if len(username) <= 1:
        masked_username = username
    elif len(username) <= 3:
        masked_username = username[0] + '*' * (len(username) - 1)
    else:
        masked_username = username[0] + '***'
    
    return f"{masked_username}@{domain}"


def mask_phone(phone: str) -> str:
    """
    Enmascara un número de teléfono.
    
    Ejemplo: "+573001234567" -> "+57300***4567"
    
    Args:
        phone: Número de teléfono a enmascarar
        
    Returns:
        Teléfono enmascarado
    """
    if not phone or len(phone) < 6:
        return phone
    
    return phone[:-4].replace(phone[3:-4], '***') + phone[-4:]


def is_strong_password(password: str) -> tuple[bool, str]:
    """
    Valida si una contraseña es suficientemente fuerte.
    
    Criterios:
    - Al menos 8 caracteres
    - Al menos una mayúscula
    - Al menos una minúscula
    - Al menos un número
    - Al menos un carácter especial (opcional)
    
    Args:
        password: Contraseña a validar
        
    Returns:
        Tuple (es_válida, mensaje)
    """
    if len(password) < 8:
        return False, "La contraseña debe tener al menos 8 caracteres"
    
    if not any(c.isupper() for c in password):
        return False, "La contraseña debe contener al menos una mayúscula"
    
    if not any(c.islower() for c in password):
        return False, "La contraseña debe contener al menos una minúscula"
    
    if not any(c.isdigit() for c in password):
        return False, "La contraseña debe contener al menos un número"
    
    return True, "Contraseña válida"


def calculate_expiration(minutes: int) -> datetime:
    """
    Calcula timestamp de expiración desde ahora.
    
    Args:
        minutes: Minutos hasta la expiración
        
    Returns:
        datetime de expiración
    """
    return datetime.utcnow() + timedelta(minutes=minutes)


def is_expired(expires_at: datetime) -> bool:
    """
    Verifica si un timestamp ya expiró.
    
    Args:
        expires_at: Timestamp de expiración
        
    Returns:
        True si ya expiró, False en caso contrario
    """
    if not expires_at:
        return True
    
    return datetime.utcnow() > expires_at


def format_permission(permission: str) -> Dict[str, str]:
    """
    Parsea un permiso en sus componentes.
    
    Ejemplo: "write:proyectos:own_centro" -> 
             {"action": "write", "resource": "proyectos", "scope": "own_centro"}
    
    Args:
        permission: Permiso a parsear
        
    Returns:
        Dict con componentes del permiso
    """
    parts = permission.split(':')
    
    result = {
        "action": parts[0] if len(parts) > 0 else "",
        "resource": parts[1] if len(parts) > 1 else "",
        "scope": parts[2] if len(parts) > 2 else "all"
    }
    
    return result


def permissions_to_dict(permissions: List[str]) -> Dict[str, List[str]]:
    """
    Agrupa permisos por recurso.
    
    Args:
        permissions: Lista de permisos
        
    Returns:
        Dict agrupado por recurso
    """
    grouped = {}
    
    for permission in permissions:
        parsed = format_permission(permission)
        resource = parsed['resource']
        
        if resource not in grouped:
            grouped[resource] = []
        
        grouped[resource].append(permission)
    
    return grouped


def validate_centro_gestor(user_centro: str, resource_centro: str) -> bool:
    """
    Valida si un usuario tiene acceso a un centro gestor.
    
    Args:
        user_centro: Centro gestor asignado al usuario
        resource_centro: Centro gestor del recurso
        
    Returns:
        True si tiene acceso, False en caso contrario
    """
    if not user_centro or not resource_centro:
        return False
    
    # Normalizar strings (remover espacios, convertir a minúsculas)
    user_centro_norm = user_centro.strip().lower()
    resource_centro_norm = resource_centro.strip().lower()
    
    return user_centro_norm == resource_centro_norm


def get_client_ip(request) -> str:
    """
    Obtiene la IP real del cliente considerando proxies.
    
    Args:
        request: Request de FastAPI
        
    Returns:
        IP del cliente
    """
    # Verificar headers de proxy
    forwarded_for = request.headers.get('X-Forwarded-For')
    if forwarded_for:
        # Puede contener múltiples IPs, tomar la primera
        return forwarded_for.split(',')[0].strip()
    
    real_ip = request.headers.get('X-Real-IP')
    if real_ip:
        return real_ip
    
    # Fallback a la IP directa
    if request.client:
        return request.client.host
    
    return 'unknown'


def sanitize_log_data(data: Dict) -> Dict:
    """
    Sanitiza datos sensibles antes de loggear.
    
    Remueve/enmascara:
    - Contraseñas
    - Tokens
    - Emails (parcialmente)
    - Teléfonos (parcialmente)
    
    Args:
        data: Dict con datos a sanitizar
        
    Returns:
        Dict sanitizado
    """
    sensitive_keys = ['password', 'token', 'secret', 'api_key', 'refresh_token']
    
    sanitized = data.copy()
    
    for key, value in data.items():
        # Remover campos sensibles
        if any(sensitive in key.lower() for sensitive in sensitive_keys):
            sanitized[key] = '***REDACTED***'
        
        # Enmascarar emails
        elif 'email' in key.lower() and isinstance(value, str) and '@' in value:
            sanitized[key] = mask_email(value)
        
        # Enmascarar teléfonos
        elif 'phone' in key.lower() and isinstance(value, str):
            sanitized[key] = mask_phone(value)
    
    return sanitized


def format_role_display(role_id: str, role_data: Dict) -> str:
    """
    Formatea un rol para display en UI.
    
    Args:
        role_id: ID del rol
        role_data: Datos del rol
        
    Returns:
        String formateado para mostrar
    """
    name = role_data.get('name', role_id)
    level = role_data.get('level', '?')
    permissions_count = len(role_data.get('permissions', []))
    
    return f"{name} (Nivel {level}) - {permissions_count} permisos"


def calculate_days_remaining(expires_at: datetime) -> int:
    """
    Calcula días restantes hasta expiración.
    
    Args:
        expires_at: Timestamp de expiración
        
    Returns:
        Número de días (negativo si ya expiró)
    """
    if not expires_at:
        return 0
    
    delta = expires_at - datetime.utcnow()
    return delta.days
