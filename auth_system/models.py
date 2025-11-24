"""
Modelos Pydantic para Autenticación

Schemas de request/response para endpoints de autenticación.

Autor: Juan Pablo GM
Fecha: 23 de Noviembre 2025
"""

from pydantic import BaseModel, EmailStr, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime


# ============================================================================
# REQUEST MODELS
# ============================================================================

class UserLoginRequest(BaseModel):
    """Request para login con email/password."""
    email: EmailStr
    password: str = Field(..., min_length=8)


class UserRegistrationRequest(BaseModel):
    """Request para registro de nuevo usuario."""
    email: EmailStr
    password: str = Field(..., min_length=8)
    full_name: str = Field(..., min_length=3, max_length=100)
    phone_number: Optional[str] = Field(None, max_length=20)
    centro_gestor: Optional[str] = Field(None, max_length=100)
    
    @validator('password')
    def validate_password_strength(cls, v):
        """Valida que la contraseña sea fuerte."""
        if not any(c.isupper() for c in v):
            raise ValueError('La contraseña debe contener al menos una mayúscula')
        if not any(c.islower() for c in v):
            raise ValueError('La contraseña debe contener al menos una minúscula')
        if not any(c.isdigit() for c in v):
            raise ValueError('La contraseña debe contener al menos un número')
        return v


class GoogleAuthRequest(BaseModel):
    """Request para autenticación con Google."""
    google_token: str = Field(..., min_length=10)


class SendCodeRequest(BaseModel):
    """Request para enviar código de verificación."""
    email: EmailStr
    purpose: str = Field(..., regex="^(login|verification|password_reset)$")


class VerifyCodeRequest(BaseModel):
    """Request para verificar código."""
    email: EmailStr
    code: str = Field(..., min_length=6, max_length=6)


class SendMagicLinkRequest(BaseModel):
    """Request para enviar magic link."""
    email: EmailStr
    redirect_url: Optional[str] = "https://app.cali.gov.co/dashboard"


class ForgotPasswordRequest(BaseModel):
    """Request para recuperación de contraseña."""
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    """Request para resetear contraseña con código."""
    oob_code: str = Field(..., min_length=10)
    new_password: str = Field(..., min_length=8)


class ChangePasswordRequest(BaseModel):
    """Request para cambiar contraseña (admin)."""
    uid: str
    new_password: str = Field(..., min_length=8)


class AssignRolesRequest(BaseModel):
    """Request para asignar roles a un usuario."""
    roles: List[str] = Field(..., min_items=1)
    reason: Optional[str] = Field(None, max_length=500)


class GrantTemporaryPermissionRequest(BaseModel):
    """Request para otorgar permiso temporal."""
    permission: str = Field(..., min_length=5)
    expires_at: datetime
    reason: str = Field(..., min_length=10, max_length=500)
    
    @validator('expires_at')
    def validate_expires_at(cls, v):
        """Valida que la fecha de expiración sea futura."""
        if v <= datetime.utcnow():
            raise ValueError('La fecha de expiración debe ser futura')
        return v


class CreateRoleRequest(BaseModel):
    """Request para crear un rol personalizado."""
    role_id: str = Field(..., regex="^[a-z_]+$", min_length=3, max_length=50)
    name: str = Field(..., min_length=3, max_length=100)
    description: str = Field(..., min_length=10, max_length=500)
    permissions: List[str] = Field(..., min_items=1)
    level: int = Field(..., ge=0, le=10)
    color: Optional[str] = Field("#4ECDC4", regex="^#[0-9A-Fa-f]{6}$")
    icon: Optional[str] = Field("user", max_length=50)


class UpdateRoleRequest(BaseModel):
    """Request para actualizar un rol."""
    name: Optional[str] = Field(None, min_length=3, max_length=100)
    description: Optional[str] = Field(None, min_length=10, max_length=500)
    permissions: Optional[List[str]] = Field(None, min_items=1)
    is_active: Optional[bool] = None


class CheckPermissionRequest(BaseModel):
    """Request para verificar un permiso."""
    permission: str = Field(..., min_length=5)
    resource_id: Optional[str] = None
    centro_gestor: Optional[str] = None


class SendNotificationRequest(BaseModel):
    """Request para enviar notificación."""
    user_uid: str
    type: str = Field(..., regex="^(email|sms|both)$")
    subject: str = Field(..., min_length=3, max_length=200)
    message: str = Field(..., min_length=10, max_length=2000)
    template: Optional[str] = None
    data: Optional[Dict[str, Any]] = {}


# ============================================================================
# RESPONSE MODELS
# ============================================================================

class UserResponse(BaseModel):
    """Response con datos de usuario."""
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
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class LoginResponse(BaseModel):
    """Response de login exitoso."""
    success: bool
    message: str
    user: UserResponse
    id_token: str
    refresh_token: str
    expires_in: int = 3600


class RegisterResponse(BaseModel):
    """Response de registro exitoso."""
    success: bool
    message: str
    user: UserResponse
    uid: str


class RoleResponse(BaseModel):
    """Response con datos de un rol."""
    role_id: str
    name: str
    description: str
    level: int
    permissions: List[str]
    is_active: bool
    users_count: int
    color: str
    icon: str
    created_at: datetime
    updated_at: datetime
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class PermissionCheckResponse(BaseModel):
    """Response de verificación de permiso."""
    success: bool
    has_permission: bool
    permission: str
    granted_by: str  # "role:editor_datos" | "custom" | "temporary"
    scope_valid: bool
    message: Optional[str] = None


class UserPermissionsResponse(BaseModel):
    """Response con permisos de un usuario."""
    success: bool
    user_uid: str
    roles: List[str]
    permissions: Dict[str, List[str]]  # {"permanent": [...], "temporary": [...], "custom": [...]}
    centro_gestor: Optional[str]
    can_access_centros: List[str]


class AuditLogResponse(BaseModel):
    """Response con log de auditoría."""
    log_id: str
    timestamp: datetime
    user_uid: str
    user_email: str
    user_name: Optional[str]
    action: str
    resource_type: str
    resource_id: Optional[str]
    success: bool
    http_status: int
    ip_address: str
    centro_gestor: Optional[str]
    execution_time_ms: int
    risk_level: str
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class TemporaryPermissionResponse(BaseModel):
    """Response con permiso temporal."""
    permission: str
    expires_at: datetime
    granted_by: str
    granted_by_name: str
    reason: str
    days_remaining: int
    created_at: datetime
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class BaseResponse(BaseModel):
    """Response base para operaciones exitosas."""
    success: bool
    message: str
    data: Optional[Any] = None


class ErrorResponse(BaseModel):
    """Response para errores."""
    success: bool = False
    message: str
    detail: Optional[str] = None
    error_code: Optional[str] = None
