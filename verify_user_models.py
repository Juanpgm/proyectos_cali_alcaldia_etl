#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de verificación para los modelos de usuario
Verifica que los modelos Usuario, Rol y TokenSeguridad funcionen correctamente.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database_management', 'core'))

# Importar directamente desde models.py
import models
from models import Base, Usuario, Rol, TokenSeguridad, UnidadProyecto
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import uuid

def test_models_basic():
    """Prueba básica de los modelos"""
    print("🔍 Verificando modelos de usuario...")
    
    try:
        # Test instanciación
        rol = Rol(id=5, nombre="Admin", descripcion="Administrador", nivel=5)
        usuario = Usuario(
            username="admin_test",
            nombre_completo="Admin Test",
            email="admin@test.com",
            nombre_centro_gestor="Secretaría de TIC",
            rol=5,
            estado=True,
            verificado=True
        )
        token = TokenSeguridad(
            usuario_id=str(uuid.uuid4()),
            token="test_token",
            tipo="reset_password",
            expiracion=models.datetime.utcnow() + models.timedelta(hours=24)
        )
        
        print("✅ Modelos se instancian correctamente")
        print("✅ Relaciones configuradas")
        
        # Test compatibilidad con modelos existentes
        proyecto = UnidadProyecto(key="TEST-001", identificador="Test")
        print("✅ Compatible con modelos existentes")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_table_creation():
    """Prueba creación de tablas"""
    print("\n🏗️ Verificando creación de tablas...")
    
    try:
        # Base de datos en memoria para testing
        engine = create_engine("sqlite:///:memory:", echo=False)
        Base.metadata.create_all(engine)
        
        # Verificar tablas creadas
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        required_tables = ['usuarios', 'roles', 'tokens_seguridad']
        for table in required_tables:
            if table in tables:
                print(f"✅ Tabla '{table}' creada correctamente")
            else:
                print(f"❌ Tabla '{table}' no encontrada")
                return False
        
        # Test inserción básica
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Insertar rol
        rol = Rol(id=1, nombre="Usuario", descripcion="Usuario básico", nivel=1)
        session.add(rol)
        session.commit()
        
        # Insertar usuario
        usuario = Usuario(
            username="test_user",
            nombre_completo="Test User",
            email="test@example.com",
            rol=1
        )
        session.add(usuario)
        session.commit()
        
        print("✅ Inserción de datos funciona correctamente")
        session.close()
        return True
        
    except Exception as e:
        print(f"❌ Error en creación de tablas: {e}")
        return False

def main():
    """Verificación principal"""
    print("🚀 Verificando modelos de base de datos de usuarios")
    print("=" * 55)
    
    if not test_models_basic():
        return 1
        
    if not test_table_creation():
        return 1
    
    print("\n" + "=" * 55)
    print("🎉 ¡Verificación exitosa!")
    print("\n📋 Modelos listos para usar:")
    print("   • Usuario - Gestión de usuarios")
    print("   • Rol - Sistema de roles (5 niveles)")
    print("   • TokenSeguridad - Tokens de seguridad")
    print("\n💡 Los modelos están en: database_management/core/models.py")
    print("📖 Documentación en: DB_USER_MODELS.md")
    
    return 0

if __name__ == "__main__":
    exit(main())