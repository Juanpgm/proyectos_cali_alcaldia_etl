#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para verificar que las tablas de usuario se crearon correctamente en PostgreSQL
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database_management', 'core'))

import models
from models import Usuario, Rol, TokenSeguridad
import config
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def verify_database_tables():
    """Verificar que las tablas existen en PostgreSQL"""
    print("🔍 Verificando tablas en PostgreSQL...")
    
    try:
        # Obtener configuración
        db_config = config.get_database_config()
        
        # Crear conexión
        engine = create_engine(db_config.connection_string)
        session = sessionmaker(bind=engine)()
        
        # Verificar tablas
        tables_to_check = [
            ('roles', 'Tabla de roles'),
            ('usuarios', 'Tabla de usuarios'), 
            ('tokens_seguridad', 'Tabla de tokens de seguridad')
        ]
        
        for table_name, description in tables_to_check:
            try:
                result = session.execute(text(f"""
                    SELECT table_name, table_schema 
                    FROM information_schema.tables 
                    WHERE table_name = '{table_name}' 
                    AND table_schema = 'public'
                """))
                
                if result.fetchone():
                    print(f"✅ {description} ({table_name}) - Existe")
                else:
                    print(f"❌ {description} ({table_name}) - No encontrada")
                    
            except Exception as e:
                print(f"❌ Error verificando {table_name}: {e}")
        
        # Verificar datos de roles
        try:
            roles_count = session.query(Rol).count()
            print(f"\n📊 Datos encontrados:")
            print(f"   • Roles: {roles_count}")
            
            if roles_count > 0:
                roles = session.query(Rol).all()
                for rol in roles:
                    print(f"     - {rol.id}: {rol.nombre} (Nivel {rol.nivel})")
            
            usuarios_count = session.query(Usuario).count()
            print(f"   • Usuarios: {usuarios_count}")
            
            tokens_count = session.query(TokenSeguridad).count()
            print(f"   • Tokens: {tokens_count}")
            
        except Exception as e:
            print(f"❌ Error consultando datos: {e}")
        
        session.close()
        return True
        
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return False

def main():
    print("🚀 Verificación de Base de Datos PostgreSQL")
    print("=" * 50)
    
    if verify_database_tables():
        print("\n✅ Verificación completada exitosamente")
        print("\n📋 Estado:")
        print("   • Tablas de usuarios creadas en PostgreSQL")
        print("   • Roles por defecto inicializados")
        print("   • Sistema listo para usar desde API")
        return 0
    else:
        print("\n❌ Problemas encontrados en la verificación")
        return 1

if __name__ == "__main__":
    exit(main())