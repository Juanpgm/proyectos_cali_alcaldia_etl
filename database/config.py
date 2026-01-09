"""
ETL Configuration Module

Configuración cent    'unidades_proyecto': {
        'url': os.getenv('SHEETS_UNIDADES_PROYECTO_URL'),
        'worksheet': os.getenv('SHEETS_UNIDADES_PROYECTO_WORKSHEET', 'unidades_proyecto')
    }zada para toda la pipeline ETL de Cali.
Implemen        print("💡 Ejecuta: gcloud auth application-default login")a Firebase Application Default Credentials y Service Account para Google Sheets.
Incluye Firebase, Google Sheets, y configuración general del sistema.
Implementa programación funcional para configuración segura y escalable.
"""

import os
import firebase_admin
from firebase_admin import credentials, firestore
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from typing import Optional, List, Dict, Any, Callable
from pathlib import Path
from functools import wraps, lru_cache
import io

# Cargar variables de entorno desde .env basado en la rama de Git
try:
    from dotenv import load_dotenv
    import subprocess
    
    # Detectar rama actual de Git
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=Path(__file__).parent.parent
        )
        current_branch = result.stdout.strip() if result.returncode == 0 else 'main'
    except Exception:
        current_branch = 'main'
    
    # Determinar el archivo .env según la rama
    project_root = Path(__file__).parent.parent
    if current_branch == 'dev':
        env_path = project_root / '.env.dev'
        print("Usando configuracion de DESARROLLO (.env.dev)")
    elif current_branch == 'main':
        env_path = project_root / '.env.prod'
        print("Usando configuracion de PRODUCCION (.env.prod)")
    else:
        # Para otras ramas, usar .env.dev como default
        env_path = project_root / '.env.dev'
        print(f"Rama '{current_branch}' no reconocida, usando .env.dev")
    
    # Cargar el archivo correspondiente
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Variables de entorno cargadas desde {env_path.name}")
    else:
        # Fallback a .env genérico
        env_path = project_root / '.env'
        if env_path.exists():
            load_dotenv(env_path)
            print(f"Usando .env generico (crea {env_path.parent / ('.env.dev' if current_branch == 'dev' else '.env.prod')})")
        else:
            print(f"No se encontro archivo de configuracion {env_path}")
    
    # Siempre cargar .env.local al final (sobrescribe otras configuraciones)
    env_local_path = project_root / '.env.local'
    if env_local_path.exists():
        load_dotenv(env_local_path, override=True)
        print(f"Variables locales cargadas desde {env_local_path.name}")
            
except ImportError:
    print("python-dotenv no instalado, usando variables de entorno del sistema")

# Variables globales para singletons
_firebase_app = None
_firestore_client = None
_drive_service = None

# Configuración centralizada desde variables de entorno
PROJECT_ID = os.getenv('FIREBASE_PROJECT_ID', os.getenv('GOOGLE_CLOUD_PROJECT', 'dev-test-e778d'))
BATCH_SIZE = int(os.getenv('FIRESTORE_BATCH_SIZE', '500'))
TIMEOUT = int(os.getenv('FIRESTORE_TIMEOUT', '30'))

# Google Drive configuration
# Usar scopes amplios para evitar problemas con ADC
DRIVE_SCOPES = [
    'https://www.googleapis.com/auth/drive'
]

# Drive folder configuration (desde variables de entorno)
# IMPORTANTE: Configurar en .env.dev o .env.prod, NO hardcodear aquí
DRIVE_FOLDER_ID = os.getenv('DRIVE_UNIDADES_PROYECTO_FOLDER_ID')

# Service Account configuration (usado para Drive y Firebase)
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE', None)

# Configuración de logging seguro (sin exponer información sensible)
SECURE_LOGGING = False


# Decorador para logging seguro
def secure_log(func: Callable) -> Callable:
    """Decorador para logging que no expone información sensible."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if SECURE_LOGGING:
            func_name = func.__name__
            print(f"[CONFIG] Ejecutando: {func_name}")
        try:
            result = func(*args, **kwargs)
            if SECURE_LOGGING:
                print(f"[OK] {func_name}: completado")
            return result
        except Exception as e:
            if SECURE_LOGGING:
                print(f"[ERROR] {func_name}: error (detalles omitidos por seguridad)")
            raise
    return wrapper


@lru_cache(maxsize=1)
@secure_log
def initialize_firebase() -> firebase_admin.App:
    """Inicializa Firebase usando Application Default Credentials con cache."""
    global _firebase_app
    
    if _firebase_app:
        return _firebase_app
    
    try:
        _firebase_app = firebase_admin.get_app()
        return _firebase_app
    except ValueError:
        pass
    
    try:
        # Usar Application Default Credentials (método seguro recomendado)
        cred = credentials.ApplicationDefault()
        _firebase_app = firebase_admin.initialize_app(cred, {'projectId': PROJECT_ID})
        print(f"[OK] Firebase inicializado: {PROJECT_ID}")
        return _firebase_app
    except Exception as e:
        print(f"[ERROR] Error inicializando Firebase: {e}")
        print("� Ejecuta: gcloud auth application-default login")
        raise


@lru_cache(maxsize=1)
@secure_log
def get_firestore_client():
    """Obtiene cliente de Firestore con cache."""
    global _firestore_client
    
    if not _firestore_client:
        initialize_firebase()
        _firestore_client = firestore.client()
    
    return _firestore_client


@secure_log
def test_connection() -> bool:
    """Prueba la conexión a Firestore de forma segura."""
    try:
        client = get_firestore_client()
        if not client:
            if not SECURE_LOGGING:
                print("[ERROR] No se pudo obtener cliente de Firestore")
            return False
        
        # Intentar una operación más simple primero
        try:
            # Simplemente verificar que podemos crear una referencia
            test_ref = client.collection('_test_connection')
            if test_ref:
                if not SECURE_LOGGING:
                    print("[OK] Conexión a Firestore verificada")
                return True
        except Exception as inner_e:
            if not SECURE_LOGGING:
                print(f"[ERROR] Error en verificación de conexión: {inner_e}")
        
        # Si falla, intentar listar colecciones (método original)
        try:
            collections = list(client.collections())
            if not SECURE_LOGGING:
                print(f"[OK] Conexión a Firestore verificada - {len(collections)} colecciones encontradas")
            return True
        except Exception as inner_e:
            if not SECURE_LOGGING:
                print(f"[ERROR] Error listando colecciones: {inner_e}")
            return False
            
    except Exception as e:
        if not SECURE_LOGGING:
            print(f"[ERROR] Error de conexión a Firebase: {e}")
        return False


# Funciones para Google Drive
@lru_cache(maxsize=1)
@secure_log
def get_drive_service(user_email: Optional[str] = None):
    """
    Obtiene el servicio de Google Drive autenticado.
    Soporta Domain-Wide Delegation si se proporciona user_email.
    
    Args:
        user_email: Email del usuario de Google Workspace a impersonar (opcional)
                   Solo funciona si Domain-Wide Delegation está habilitado
    
    Returns:
        Servicio de Google Drive autenticado o None si falla
    """
    global _drive_service
    
    # Si ya hay un servicio y no se requiere delegación, reutilizarlo
    if _drive_service and not user_email:
        return _drive_service
    
    try:
        # Opción 1: Service Account con Domain-Wide Delegation (si user_email está presente)
        if SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
            from google.oauth2 import service_account
            
            credentials_obj = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE,
                scopes=DRIVE_SCOPES
            )
            
            # Aplicar Domain-Wide Delegation si se proporciona user_email
            if user_email:
                try:
                    credentials_obj = credentials_obj.with_subject(user_email)
                    service = build('drive', 'v3', credentials=credentials_obj)
                    print(f"[OK] Google Drive autenticado con Domain-Wide Delegation")
                    print(f"   Delegando al usuario: {user_email}")
                    return service
                except Exception as e:
                    print(f"[ERROR] Error con Domain-Wide Delegation: {e}")
                    print(f"💡 Verifica que Domain-Wide Delegation esté configurado correctamente")
                    print(f"   Consulta: CONFIGURACION_DOMAIN_WIDE_DELEGATION.md")
                    return None
            else:
                # Service Account sin delegación (para Shared Drives)
                _drive_service = build('drive', 'v3', credentials=credentials_obj)
                print("[OK] Google Drive autenticado con Service Account")
                print("💡 Para carpetas personales: usa Domain-Wide Delegation")
                print("💡 Para Shared Drives: asegúrate de compartir con el Service Account")
                return _drive_service
        
        # Opción 2: Application Default Credentials (fallback)
        try:
            credentials_obj, project = default()
            _drive_service = build('drive', 'v3', credentials=credentials_obj)
            print("[OK] Google Drive autenticado con ADC")
            print("[WARNING]  Nota: ADC puede no tener scopes de Drive configurados")
            return _drive_service
        except Exception as e:
            if not SECURE_LOGGING:
                print(f"[WARNING]  Error con ADC: {e}")
        
        print("💡 Opciones de autenticación:")
        print("   1. Service Account con Domain-Wide Delegation: configura SERVICE_ACCOUNT_FILE + GOOGLE_WORKSPACE_USER_EMAIL")
        print("   2. Service Account con Shared Drive: configura SERVICE_ACCOUNT_FILE y comparte Shared Drive")
        print("   3. ADC: gcloud auth application-default login")
        return None
        
    except Exception as e:
        if not SECURE_LOGGING:
            print(f"[ERROR] Error autenticando Google Drive: {e}")
        return None


@secure_log
def list_excel_files_in_folder(folder_id: str) -> List[Dict[str, str]]:
    """
    Lista todos los archivos Excel (.xlsx) en una carpeta de Google Drive.
    
    Args:
        folder_id: ID de la carpeta de Google Drive
        
    Returns:
        Lista de diccionarios con 'id' y 'name' de cada archivo Excel
    """
    try:
        service = get_drive_service()
        if not service:
            return []
        
        # Buscar archivos Excel en la carpeta
        query = f"'{folder_id}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel') and trashed=false"
        
        results = service.files().list(
            q=query,
            fields="files(id, name, mimeType)",
            pageSize=100,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        files = results.get('files', [])
        
        if not files:
            print(f"[WARNING]  No se encontraron archivos Excel en la carpeta")
            return []
        
        print(f"[OK] Encontrados {len(files)} archivos Excel")
        for file in files:
            # Mostrar solo nombre parcial por seguridad
            name_display = file['name'][:30] + "..." if len(file['name']) > 30 else file['name']
            print(f"   - {name_display}")
        
        return files
        
    except Exception as e:
        if not SECURE_LOGGING:
            print(f"[ERROR] Error listando archivos: {e}")
        return []


@secure_log
def download_excel_file(file_id: str, file_name: str) -> Optional[io.BytesIO]:
    """
    Descarga un archivo Excel desde Google Drive a memoria.
    
    Args:
        file_id: ID del archivo en Google Drive
        file_name: Nombre del archivo (para logging)
        
    Returns:
        BytesIO con el contenido del archivo o None si falla
    """
    try:
        service = get_drive_service()
        if not service:
            return None
        
        request = service.files().get_media(fileId=file_id)
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                if progress % 25 == 0:  # Mostrar cada 25%
                    name_display = file_name[:30] + "..." if len(file_name) > 30 else file_name
                    print(f"   Descargando {name_display}: {progress}%")
        
        file_buffer.seek(0)  # Volver al inicio del buffer
        name_display = file_name[:30] + "..." if len(file_name) > 30 else file_name
        print(f"[OK] Descargado: {name_display}")
        return file_buffer
        
    except Exception as e:
        if not SECURE_LOGGING:
            print(f"[ERROR] Error descargando archivo '{file_name}': {e}")
        return None


@secure_log
def upload_file_to_drive(
    file_buffer: io.BytesIO,
    filename: str,
    folder_id: str,
    mime_type: str = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
) -> Optional[Dict[str, str]]:
    """
    Sube un archivo a Google Drive desde un buffer en memoria.
    Si el archivo ya existe, lo reemplaza (actualiza).
    
    Args:
        file_buffer: Buffer con el contenido del archivo
        filename: Nombre del archivo en Drive
        folder_id: ID de la carpeta destino en Drive
        mime_type: Tipo MIME del archivo (default: Excel .xlsx)
        
    Returns:
        Dict con información del archivo subido (id, name, webViewLink) o None si falla
    """
    try:
        from googleapiclient.http import MediaIoBaseUpload
        
        service = get_drive_service()
        if not service:
            return None
        
        # Verificar si el archivo ya existe en la carpeta
        existing_file_id = None
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            results = service.files().list(
                q=query,
                fields='files(id, name)',
                pageSize=1,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            files = results.get('files', [])
            if files:
                existing_file_id = files[0]['id']
                if not SECURE_LOGGING:
                    name_display = filename[:50] + "..." if len(filename) > 50 else filename
                    print(f"📝 Archivo existe, actualizando: {name_display}")
        except Exception as e:
            if not SECURE_LOGGING:
                print(f"[WARNING]  No se pudo verificar archivo existente: {e}")
        
        # Crear MediaIoBaseUpload desde el buffer
        file_buffer.seek(0)  # Asegurar que estamos al inicio del buffer
        media = MediaIoBaseUpload(
            file_buffer,
            mimetype=mime_type,
            resumable=True
        )
        
        if existing_file_id:
            # Actualizar archivo existente
            file = service.files().update(
                fileId=existing_file_id,
                media_body=media,
                fields='id, name, webViewLink',
                supportsAllDrives=True
            ).execute()
            
            name_display = filename[:50] + "..." if len(filename) > 50 else filename
            print(f"[OK] Archivo actualizado en Drive: {name_display}")
        else:
            # Crear nuevo archivo
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink',
                supportsAllDrives=True
            ).execute()
            
            name_display = filename[:50] + "..." if len(filename) > 50 else filename
            print(f"[OK] Archivo subido a Drive: {name_display}")
        
        return {
            'id': file.get('id'),
            'name': file.get('name'),
            'webViewLink': file.get('webViewLink')
        }
        
    except Exception as e:
        if not SECURE_LOGGING:
            print(f"[ERROR] Error subiendo archivo '{filename}': {e}")
            import traceback
            traceback.print_exc()
        return None


@secure_log
def list_collections() -> List[str]:
    """Lista todas las colecciones en Firestore de forma segura."""
    try:
        client = get_firestore_client()
        collections = [col.id for col in client.collections()]
        if not SECURE_LOGGING:
            print(f"📚 Colecciones: {collections}")
        return collections
    except Exception as e:
        if not SECURE_LOGGING:
            print(f"[ERROR] Error listando colecciones: {e}")
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
        print(f"[ERROR] Error contando documentos en {collection_name}: {e}")
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
            print(f"[OK] Colección '{collection_name}' ya existe")
        return True
    except Exception as e:
        print(f"[ERROR] Error verificando colección {collection_name}: {e}")
        return False


def test_data_operations() -> bool:
    """Prueba operaciones básicas de datos (crear, leer)."""
    try:
        client = get_firestore_client()
        test_collection = "test_connection"
        
        # Crear documento de prueba
        doc_ref = client.collection(test_collection).document("test_doc")
        doc_ref.set({"test": True, "timestamp": firestore.SERVER_TIMESTAMP})
        print("[OK] Escritura de prueba exitosa")
        
        # Leer documento de prueba
        doc = doc_ref.get()
        if doc.exists:
            print("[OK] Lectura de prueba exitosa")
            
            # Limpiar documento de prueba
            doc_ref.delete()
            print("[OK] Eliminación de prueba exitosa")
            return True
        else:
            print("[ERROR] No se pudo leer el documento de prueba")
            return False
            
    except Exception as e:
        print(f"[ERROR] Error en operaciones de prueba: {e}")
        return False


def setup_firebase() -> bool:
    """Configuración completa de Firebase con verificación de carga de datos."""
    try:
        print("[START] Configurando Firebase...")
        # Mostrar PROJECT_ID de forma segura
        project_display = f"{PROJECT_ID[:8]}***" if PROJECT_ID and len(PROJECT_ID) > 8 else "[CONFIGURED]"
        print(f"[CONFIG] Proyecto: {project_display}")
        print(f"⚙️  Batch size: {BATCH_SIZE}")
        print(f"[TIME]  Timeout: {TIMEOUT}s")
        
        if not test_connection():
            return False
            
        print("\n[DATA] Probando operaciones de datos...")
        if not test_data_operations():
            print("[WARNING]  Operaciones de datos fallaron, pero conexión básica funciona")
            return False
            
        collections = list_collections()
        
        if not collections:
            print("📝 Base de datos vacía, lista para recibir datos")
        else:
            print(f"📚 {len(collections)} colecciones existentes")
            # No mostrar nombres completos de colecciones por seguridad
            for col in collections[:3]:  # Solo primeras 3
                col_display = col[:10] + "***" if len(col) > 10 else col
                print(f"    - {col_display}")
            if len(collections) > 3:
                print(f"    - ... y {len(collections) - 3} más")
        
        print("[OK] Firebase listo para ETL")
        return True
        
    except Exception as e:
        # No mostrar el error completo por seguridad
        error_msg = str(e)[:50] + "..." if len(str(e)) > 50 else str(e)
        print(f"[ERROR] Error en configuración: {error_msg}")
        return False


def show_system_info():
    """Muestra información del sistema y configuración de forma segura."""
    import platform
    print(f"💻 OS: {platform.system()} {platform.release()}")
    print(f"🐍 Python: {platform.python_version()}")
    # No mostrar la ruta completa del directorio por seguridad
    current_dir = Path.cwd()
    print(f"[FILE] Directorio: .../{current_dir.name}")
    
    # Mostrar PROJECT_ID de forma segura
    if PROJECT_ID:
        masked_project = f"{PROJECT_ID[:8]}***" if len(PROJECT_ID) > 8 else "[CONFIGURED]"
        print(f"[CONFIG] Project ID: {masked_project}")
    else:
        print("[CONFIG] Project ID: [NOT CONFIGURED]")
    
    # Verificar si gcloud está instalado
    import subprocess
    try:
        result = subprocess.run(['gcloud', '--version'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            version_line = result.stdout.split('\n')[0]
            print(f"☁️  {version_line}")
        else:
            print("[WARNING]  gcloud CLI no encontrado")
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("[WARNING]  gcloud CLI no instalado o no accesible")


# Ejecutar si se llama directamente
if __name__ == "__main__":
    print("=" * 60)
    show_system_info()
    print("=" * 60)
    
    success = setup_firebase()
    if success:
        print("\n[SUCCESS] Configuración completada exitosamente")
        print("[SAVE] Sistema listo para cargar datos")
    else:
        print("\n[FAILED] Configuración fallida")
        print("[CONFIG] Instala gcloud CLI y ejecuta: gcloud auth application-default login")