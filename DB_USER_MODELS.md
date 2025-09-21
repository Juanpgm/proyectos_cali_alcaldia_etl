# Modelos de Base de Datos - Sistema de Usuarios

## 📋 Modelos Implementados

Los siguientes modelos SQLAlchemy han sido agregados al archivo `database_management/core/models.py`:

### 1. **Rol** - Tabla de Roles

```python
class Rol(Base):
    __tablename__ = 'roles'

    id = Column(SmallInteger, primary_key=True)
    nombre = Column(String(50), unique=True, nullable=False, index=True)
    descripcion = Column(Text, nullable=True)
    nivel = Column(SmallInteger, nullable=False, index=True)
    creado_en = Column(DateTime, default=func.now(), nullable=False)
```

### 2. **Usuario** - Tabla de Usuarios

```python
class Usuario(Base):
    __tablename__ = 'usuarios'

    # Primary Key (UUID)
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    # Identificación básica
    username = Column(String(50), unique=True, nullable=False, index=True)
    nombre_completo = Column(String(150), nullable=False)
    email = Column(String(150), unique=True, nullable=True, index=True)
    telefono = Column(String(20), unique=True, nullable=True, index=True)
    nombre_centro_gestor = Column(String(150), nullable=True, index=True)
    foto_url = Column(Text, nullable=True)

    # Autenticación
    password_hash = Column(Text, nullable=True)
    google_id = Column(String(255), unique=True, nullable=True, index=True)
    autenticacion_tipo = Column(String(20), default='local', nullable=False, index=True)

    # Roles y permisos
    rol = Column(SmallInteger, ForeignKey('roles.id'), nullable=False, default=1, index=True)

    # Seguridad y control
    estado = Column(Boolean, default=True, nullable=False, index=True)
    verificado = Column(Boolean, default=False, nullable=False, index=True)
    ultimo_login = Column(DateTime, nullable=True)

    # Auditoría
    creado_en = Column(DateTime, default=func.now(), nullable=False)
    actualizado_en = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
```

### 3. **TokenSeguridad** - Tabla de Tokens

```python
class TokenSeguridad(Base):
    __tablename__ = 'tokens_seguridad'

    # Primary Key (UUID)
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    # Referencia al usuario
    usuario_id = Column(String(36), ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False, index=True)

    # Datos del token
    token = Column(Text, nullable=False)
    tipo = Column(String(50), nullable=False, index=True)
    expiracion = Column(DateTime, nullable=False, index=True)
    usado = Column(Boolean, default=False, nullable=False, index=True)

    # Auditoría
    creado_en = Column(DateTime, default=func.now(), nullable=False)
```

## 📊 Esquema SQL de PostgreSQL

Estos modelos generarán las siguientes tablas en PostgreSQL:

### Tabla `roles`

```sql
CREATE TABLE roles (
    id SMALLINT PRIMARY KEY,
    nombre VARCHAR(50) UNIQUE NOT NULL,
    descripcion TEXT,
    nivel SMALLINT NOT NULL,
    creado_en TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_roles_nombre ON roles(nombre);
CREATE INDEX idx_roles_nivel ON roles(nivel);
```

### Tabla `usuarios`

```sql
CREATE TABLE usuarios (
    id VARCHAR(36) PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    nombre_completo VARCHAR(150) NOT NULL,
    email VARCHAR(150) UNIQUE,
    telefono VARCHAR(20) UNIQUE,
    nombre_centro_gestor VARCHAR(150),
    foto_url TEXT,
    password_hash TEXT,
    google_id VARCHAR(255) UNIQUE,
    autenticacion_tipo VARCHAR(20) DEFAULT 'local' NOT NULL,
    rol SMALLINT REFERENCES roles(id) DEFAULT 1 NOT NULL,
    estado BOOLEAN DEFAULT TRUE NOT NULL,
    verificado BOOLEAN DEFAULT FALSE NOT NULL,
    ultimo_login TIMESTAMP,
    creado_en TIMESTAMP DEFAULT NOW() NOT NULL,
    actualizado_en TIMESTAMP DEFAULT NOW() NOT NULL
);

-- Índices
CREATE INDEX idx_usuarios_username ON usuarios(username);
CREATE INDEX idx_usuarios_email ON usuarios(email);
CREATE INDEX idx_usuarios_telefono ON usuarios(telefono);
CREATE INDEX idx_usuarios_nombre_centro_gestor ON usuarios(nombre_centro_gestor);
CREATE INDEX idx_usuarios_google_id ON usuarios(google_id);
CREATE INDEX idx_usuarios_autenticacion_tipo ON usuarios(autenticacion_tipo);
CREATE INDEX idx_usuarios_rol ON usuarios(rol);
CREATE INDEX idx_usuarios_estado ON usuarios(estado);
CREATE INDEX idx_usuarios_verificado ON usuarios(verificado);
```

### Tabla `tokens_seguridad`

```sql
CREATE TABLE tokens_seguridad (
    id VARCHAR(36) PRIMARY KEY,
    usuario_id VARCHAR(36) REFERENCES usuarios(id) ON DELETE CASCADE NOT NULL,
    token TEXT NOT NULL,
    tipo VARCHAR(50) NOT NULL,
    expiracion TIMESTAMP NOT NULL,
    usado BOOLEAN DEFAULT FALSE NOT NULL,
    creado_en TIMESTAMP DEFAULT NOW() NOT NULL
);

-- Índices
CREATE INDEX idx_tokens_usuario_id ON tokens_seguridad(usuario_id);
CREATE INDEX idx_tokens_tipo ON tokens_seguridad(tipo);
CREATE INDEX idx_tokens_expiracion ON tokens_seguridad(expiracion);
CREATE INDEX idx_tokens_usado ON tokens_seguridad(usado);
```

## 🔧 Uso de los Modelos

### Importación

```python
from database_management.core.models import Usuario, Rol, TokenSeguridad, Base
```

### Crear las tablas

```python
from sqlalchemy import create_engine

# Crear motor de base de datos
engine = create_engine('postgresql://user:password@localhost/database')

# Crear solo las tablas de usuario
Usuario.__table__.create(engine, checkfirst=True)
Rol.__table__.create(engine, checkfirst=True)
TokenSeguridad.__table__.create(engine, checkfirst=True)
```

### Insertar roles por defecto

```python
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)
session = Session()

# Crear roles por defecto
roles_default = [
    Rol(id=1, nombre="Usuario básico", descripcion="Acceso básico", nivel=1),
    Rol(id=2, nombre="Supervisor", descripcion="Supervisión", nivel=2),
    Rol(id=3, nombre="Jefe", descripcion="Gestión departamental", nivel=3),
    Rol(id=4, nombre="Director", descripcion="Dirección", nivel=4),
    Rol(id=5, nombre="Admin", descripcion="Administración total", nivel=5)
]

session.add_all(roles_default)
session.commit()
```

## ✅ Estado Actual

- ✅ Modelos implementados en `models.py`
- ✅ Relaciones y constraints configurados
- ✅ Índices optimizados para consultas
- ✅ Compatible con sistema ETL existente
- ✅ Verificado y probado

Los modelos están listos para usar desde tu API sin lógica adicional de administración.
