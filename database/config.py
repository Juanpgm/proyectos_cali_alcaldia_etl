"""
Firebase Database Configuration Module

Configuración simple usando Workload Identity Federation (sin archivos de claves).
Compatible con Windows y Linux.
"""

import os
import firebase_admin
from firebase_admin import credentials, firestore
from typing import Optional, List
from pathlib import Path

# Cargar variables de entorno desde .env
try:
    from dotenv import load_dotenv
    # Buscar .env en el directorio raíz del proyecto
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)
except ImportError:
    print("⚠️  python-dotenv no instalado, usando variables de entorno del sistema")

# Variables globales
_app = None
_client = None

# Configuración desde variables de entorno
PROJECT_ID = os.getenv('FIREBASE_PROJECT_ID', 'dev-test-e778d')
BATCH_SIZE = int(os.getenv('FIRESTORE_BATCH_SIZE', '500'))
TIMEOUT = int(os.getenv('FIRESTORE_TIMEOUT', '30'))


def initialize_firebase() -> firebase_admin.App:
    """Inicializa Firebase usando Application Default Credentials."""
    global _app
    
    if _app:
        return _app
    
    try:
        _app = firebase_admin.get_app()
        return _app
    except ValueError:
        pass
    
    try:
        # Usar Application Default Credentials (método seguro recomendado)
        cred = credentials.ApplicationDefault()
        _app = firebase_admin.initialize_app(cred, {'projectId': PROJECT_ID})
        print(f"✅ Firebase inicializado: {PROJECT_ID}")
        return _app
    except Exception as e:
        print(f"❌ Error inicializando Firebase: {e}")
        print("� Ejecuta: gcloud auth application-default login")
        raise


def get_firestore_client():
    """Obtiene cliente de Firestore."""
    global _client
    
    if not _client:
        initialize_firebase()
        _client = firestore.client()
        print("✅ Cliente Firestore listo")
    
    return _client


def test_connection() -> bool:
    """Prueba la conexión a Firestore."""
    try:
        client = get_firestore_client()
        list(client.collections())
        print(f"🔗 Conexión exitosa a {PROJECT_ID}")
        return True
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return False


def list_collections() -> List[str]:
    """Lista todas las colecciones en Firestore."""
    try:
        client = get_firestore_client()
        collections = [col.id for col in client.collections()]
        print(f"📚 Colecciones: {collections}")
        return collections
    except Exception as e:
        print(f"❌ Error listando colecciones: {e}")
        return []


def get_collection_count(collection_name: str) -> int:
    """Obtiene el número de documentos en una colección."""
    try:
        client = get_firestore_client()
        docs = client.collection(collection_name).limit(1).get()
        # Para conteo real usar aggregate queries en producción
        collection_ref = client.collection(collection_name)
        docs = list(collection_ref.stream())
        return len(docs)
    except Exception as e:
        print(f"❌ Error contando documentos en {collection_name}: {e}")
        return 0


def create_collection_if_not_exists(collection_name: str) -> bool:
    """Crea una colección si no existe."""
    try:
        client = get_firestore_client()
        # Firestore crea colecciones automáticamente al agregar documentos
        # Solo verificamos que el cliente funcione
        collections = [col.id for col in client.collections()]
        if collection_name not in collections:
            print(f"📝 Colección '{collection_name}' se creará al insertar datos")
        else:
            print(f"✅ Colección '{collection_name}' ya existe")
        return True
    except Exception as e:
        print(f"❌ Error verificando colección {collection_name}: {e}")
        return False


def test_data_operations() -> bool:
    """Prueba operaciones básicas de datos (crear, leer)."""
    try:
        client = get_firestore_client()
        test_collection = "test_connection"
        
        # Crear documento de prueba
        doc_ref = client.collection(test_collection).document("test_doc")
        doc_ref.set({"test": True, "timestamp": firestore.SERVER_TIMESTAMP})
        print("✅ Escritura de prueba exitosa")
        
        # Leer documento de prueba
        doc = doc_ref.get()
        if doc.exists:
            print("✅ Lectura de prueba exitosa")
            
            # Limpiar documento de prueba
            doc_ref.delete()
            print("✅ Eliminación de prueba exitosa")
            return True
        else:
            print("❌ No se pudo leer el documento de prueba")
            return False
            
    except Exception as e:
        print(f"❌ Error en operaciones de prueba: {e}")
        return False


def setup_firebase() -> bool:
    """Configuración completa de Firebase con verificación de carga de datos."""
    try:
        print("🚀 Configurando Firebase...")
        print(f"🔧 Proyecto: {PROJECT_ID}")
        print(f"⚙️  Batch size: {BATCH_SIZE}")
        print(f"⏱️  Timeout: {TIMEOUT}s")
        
        if not test_connection():
            return False
            
        print("\n📊 Probando operaciones de datos...")
        if not test_data_operations():
            print("⚠️  Operaciones de datos fallaron, pero conexión básica funciona")
            return False
            
        collections = list_collections()
        
        if not collections:
            print("📝 Base de datos vacía, lista para recibir datos")
        else:
            print(f"📚 {len(collections)} colecciones existentes")
        
        print("✅ Firebase listo para ETL")
        return True
        
    except Exception as e:
        print(f"❌ Error en configuración: {e}")
        return False


def show_system_info():
    """Muestra información del sistema y configuración."""
    import platform
    print(f"💻 OS: {platform.system()} {platform.release()}")
    print(f"🐍 Python: {platform.python_version()}")
    print(f"📁 Directorio: {Path.cwd()}")
    print(f"🔧 Project ID: {PROJECT_ID}")
    
    # Verificar si gcloud está instalado
    import subprocess
    try:
        result = subprocess.run(['gcloud', '--version'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            version_line = result.stdout.split('\n')[0]
            print(f"☁️  {version_line}")
        else:
            print("⚠️  gcloud CLI no encontrado")
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("⚠️  gcloud CLI no instalado o no accesible")


# Ejecutar si se llama directamente
if __name__ == "__main__":
    print("=" * 60)
    show_system_info()
    print("=" * 60)
    
    success = setup_firebase()
    if success:
        print("\n🎯 Configuración completada exitosamente")
        print("💾 Sistema listo para cargar datos")
    else:
        print("\n💥 Configuración fallida")
        print("🔧 Instala gcloud CLI y ejecuta: gcloud auth application-default login")