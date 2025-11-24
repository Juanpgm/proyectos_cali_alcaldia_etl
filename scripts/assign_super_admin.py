"""
Script para asignar el rol 'super_admin' a un usuario existente

Este script permite asignar el rol de super_admin al primer administrador
o a cualquier usuario específico por su email.

Uso:
    python scripts/assign_super_admin.py

Autor: Juan Pablo GM
Fecha: 23 de Noviembre 2025
"""

import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.config import get_firestore_client, initialize_firebase
from datetime import datetime


def list_users(db):
    """Lista todos los usuarios existentes en Firestore."""
    try:
        users_ref = db.collection('users')
        users = list(users_ref.stream())
        
        if not users:
            print("❌ No hay usuarios en la base de datos")
            return []
        
        print(f"\n📋 Usuarios encontrados ({len(users)}):\n")
        user_list = []
        
        for idx, user_doc in enumerate(users, 1):
            user_data = user_doc.to_dict()
            uid = user_doc.id
            email = user_data.get('email', 'N/A')
            full_name = user_data.get('full_name', 'N/A')
            current_roles = user_data.get('roles', [])
            
            user_list.append({
                'uid': uid,
                'email': email,
                'full_name': full_name,
                'roles': current_roles
            })
            
            roles_display = ', '.join(current_roles) if current_roles else "Sin roles"
            print(f"{idx}. {full_name}")
            print(f"   Email: {email}")
            print(f"   UID: {uid}")
            print(f"   Roles actuales: {roles_display}")
            print()
        
        return user_list
        
    except Exception as e:
        print(f"❌ Error listando usuarios: {e}")
        return []


def assign_super_admin_role(db, user_uid: str, user_email: str = None):
    """
    Asigna el rol de super_admin a un usuario.
    
    Args:
        db: Cliente de Firestore
        user_uid: UID del usuario
        user_email: Email del usuario (opcional, para logging)
    """
    try:
        user_ref = db.collection('users').document(user_uid)
        user_doc = user_ref.get()
        
        if not user_doc.exists:
            print(f"❌ Usuario con UID '{user_uid}' no existe")
            return False
        
        user_data = user_doc.to_dict()
        current_roles = user_data.get('roles', [])
        
        # Verificar si ya tiene el rol
        if 'super_admin' in current_roles:
            print(f"⚠️  El usuario ya tiene el rol 'super_admin'")
            return True
        
        # Agregar super_admin a los roles
        new_roles = list(set(current_roles + ['super_admin']))
        
        # Actualizar documento
        update_data = {
            'roles': new_roles,
            'assigned_by': 'system',
            'assigned_by_name': 'Script de Inicialización',
            'assigned_at': datetime.utcnow(),
            'last_role_change': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        
        user_ref.update(update_data)
        
        email_display = user_email or user_data.get('email', user_uid)
        print(f"✅ Rol 'super_admin' asignado exitosamente a: {email_display}")
        print(f"   Roles anteriores: {', '.join(current_roles) if current_roles else 'Ninguno'}")
        print(f"   Roles actuales: {', '.join(new_roles)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error asignando rol super_admin: {e}")
        import traceback
        traceback.print_exc()
        return False


def create_super_admin_user(db, email: str, password: str, full_name: str):
    """
    Crea un nuevo usuario super_admin en Firebase Auth y Firestore.
    
    Args:
        db: Cliente de Firestore
        email: Email del usuario
        password: Contraseña del usuario
        full_name: Nombre completo del usuario
    """
    try:
        from firebase_admin import auth
        
        # Crear usuario en Firebase Auth
        print(f"\n🔧 Creando usuario en Firebase Auth...")
        user_record = auth.create_user(
            email=email,
            password=password,
            email_verified=True,  # Pre-verificado para super_admin
            display_name=full_name
        )
        
        print(f"✅ Usuario creado en Firebase Auth")
        print(f"   UID: {user_record.uid}")
        print(f"   Email: {user_record.email}")
        
        # Crear documento en Firestore
        print(f"\n🔧 Creando documento en Firestore...")
        user_data = {
            'uid': user_record.uid,
            'email': email,
            'full_name': full_name,
            'roles': ['super_admin'],
            'email_verified': True,
            'phone_verified': False,
            'is_active': True,
            'account_status': 'active',
            'assigned_by': 'system',
            'assigned_by_name': 'Script de Inicialización',
            'assigned_at': datetime.utcnow(),
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
            'preferences': {
                'language': 'es',
                'timezone': 'America/Bogota',
                'notifications_email': True,
                'notifications_sms': False,
                'theme': 'light'
            }
        }
        
        db.collection('users').document(user_record.uid).set(user_data)
        
        print(f"✅ Usuario super_admin creado exitosamente")
        print(f"   Email: {email}")
        print(f"   Rol: super_admin")
        print(f"\n⚠️  IMPORTANTE: Guarda estas credenciales de forma segura")
        print(f"   Email: {email}")
        print(f"   Contraseña: {password}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creando usuario super_admin: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Función principal del script."""
    print("=" * 70)
    print("🔐 ASIGNACIÓN DE ROL SUPER_ADMIN")
    print("=" * 70)
    
    try:
        # Inicializar Firebase
        print("\n🔧 Conectando a Firebase...")
        initialize_firebase()
        db = get_firestore_client()
        print("✅ Conexión exitosa")
        
        # Verificar que existan los roles
        roles_ref = db.collection('roles').document('super_admin')
        if not roles_ref.get().exists:
            print("\n❌ Error: Los roles no han sido inicializados")
            print("💡 Ejecuta primero: python scripts/init_roles_permissions.py")
            return
        
        # Listar usuarios existentes
        users = list_users(db)
        
        if not users:
            print("\n❓ ¿Deseas crear un nuevo usuario super_admin? (si/no): ", end="")
            respuesta = input().strip().lower()
            
            if respuesta in ['si', 's', 'yes', 'y']:
                print("\n📝 Ingresa los datos del nuevo super_admin:")
                email = input("Email: ").strip()
                password = input("Contraseña (mínimo 8 caracteres): ").strip()
                full_name = input("Nombre completo: ").strip()
                
                if len(password) < 8:
                    print("❌ La contraseña debe tener al menos 8 caracteres")
                    return
                
                create_super_admin_user(db, email, password, full_name)
            else:
                print("❌ Operación cancelada")
            return
        
        # Seleccionar usuario
        print("=" * 70)
        print("Selecciona una opción:")
        print("  1-{}: Asignar super_admin a un usuario existente".format(len(users)))
        print("  0: Crear nuevo usuario super_admin")
        print("=" * 70)
        
        seleccion = input("\nIngresa el número de opción: ").strip()
        
        if seleccion == '0':
            print("\n📝 Ingresa los datos del nuevo super_admin:")
            email = input("Email: ").strip()
            password = input("Contraseña (mínimo 8 caracteres): ").strip()
            full_name = input("Nombre completo: ").strip()
            
            if len(password) < 8:
                print("❌ La contraseña debe tener al menos 8 caracteres")
                return
            
            create_super_admin_user(db, email, password, full_name)
            
        elif seleccion.isdigit() and 1 <= int(seleccion) <= len(users):
            idx = int(seleccion) - 1
            selected_user = users[idx]
            
            print(f"\n⚠️  ¿Confirmas asignar rol 'super_admin' a:")
            print(f"   Nombre: {selected_user['full_name']}")
            print(f"   Email: {selected_user['email']}")
            print(f"   UID: {selected_user['uid']}")
            
            confirmacion = input("\n¿Continuar? (si/no): ").strip().lower()
            
            if confirmacion in ['si', 's', 'yes', 'y']:
                assign_super_admin_role(
                    db, 
                    selected_user['uid'],
                    selected_user['email']
                )
            else:
                print("❌ Operación cancelada")
        else:
            print("❌ Opción inválida")
            return
        
        print("\n" + "=" * 70)
        print("✅ PROCESO COMPLETADO")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
