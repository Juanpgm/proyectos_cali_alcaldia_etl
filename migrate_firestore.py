"""
Script de Migración de Firebase Firestore
==========================================
Migra datos entre proyectos Firebase, incluso si están en diferentes cuentas

Características:
- Soporta proyectos en diferentes cuentas de Firebase
- Exporta todas las colecciones y documentos
- Maneja subcollecciones recursivamente
- Preserva tipos de datos especiales (timestamps, geopoints, referencias)
- Logging detallado del progreso
- Verificación antes de ejecutar
- Modo dry-run para previsualización

Configuración de Cuentas:
    1. Cuenta origen (unidad-cumplimiento-aa245): 
       gcloud config configurations create source-account
       gcloud auth login  # Inicia sesión con la cuenta de origen
       gcloud auth application-default login
       gcloud config set project unidad-cumplimiento-aa245
    
    2. Cuenta destino (calitrack-44403 - juanp.gzmz@gmail.com):
       gcloud config configurations create target-account
       gcloud auth login  # Inicia sesión con juanp.gzmz@gmail.com
       gcloud auth application-default login
       gcloud config set project calitrack-44403

Uso:
    python migrate_firestore.py --dry-run  # Previsualizar sin ejecutar
    python migrate_firestore.py            # Ejecutar migración
    python migrate_firestore.py --collections usuarios,proyectos  # Migrar colecciones específicas
"""

import os
import sys
import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_document import DocumentSnapshot
from google.cloud.firestore_v1 import DocumentReference, GeoPoint
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.oauth2 import service_account
from google.auth import default as google_auth_default

# Agregar el directorio raíz al path para importar módulos locales
sys.path.insert(0, str(Path(__file__).parent))

# Configuración de proyectos
SOURCE_PROJECT_ID = "unidad-cumplimiento-aa245"  # Base de datos origen
TARGET_PROJECT_ID = "calitrack-44403"  # Desarrollo (juanp.gzmz@gmail.com)

# Configuración de credenciales (opcional - deja None para usar ADC)
SOURCE_CREDENTIALS_FILE = os.getenv('SOURCE_CREDENTIALS_FILE', None)
TARGET_CREDENTIALS_FILE = os.getenv('TARGET_CREDENTIALS_FILE', None)

# Colecciones a excluir de la migración (si las hay)
EXCLUDED_COLLECTIONS = set()

# Directorio para backups
BACKUP_DIR = Path(__file__).parent / "migration_backups"
BACKUP_DIR.mkdir(exist_ok=True)


class FirestoreMigration:
    """Clase para manejar la migración de Firestore"""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.source_app = None
        self.target_app = None
        self.source_db = None
        self.target_db = None
        self.stats = {
            'collections': 0,
            'documents': 0,
            'subcollections': 0,
            'errors': 0,
            'skipped': 0
        }
        self.migration_log = []
        
    def get_credentials_for_project(self, project_id: str, creds_file: Optional[str]):
        """Obtiene credenciales para un proyecto específico"""
        if creds_file and os.path.exists(creds_file):
            return credentials.Certificate(creds_file)
        
        # Usar ADC - el usuario debe tener la configuración correcta activada
        return None  # Firebase Admin SDK usará ADC automáticamente
    
    def initialize_apps(self):
        """Inicializa las conexiones a ambos proyectos Firebase"""
        print("\n🔌 Inicializando conexiones a Firebase...")
        print("=" * 60)
        
        try:
            # Inicializar app de origen
            print(f"\n📥 PROYECTO ORIGEN: {SOURCE_PROJECT_ID}")
            source_cred = self.get_credentials_for_project(SOURCE_PROJECT_ID, SOURCE_CREDENTIALS_FILE)
            
            if source_cred:
                print(f"   🔑 Usando credenciales desde: {SOURCE_CREDENTIALS_FILE}")
                self.source_app = firebase_admin.initialize_app(
                    source_cred,
                    options={'projectId': SOURCE_PROJECT_ID},
                    name='source'
                )
            else:
                print(f"   🔐 Usando Application Default Credentials (ADC)")
                print(f"   💡 Asegúrate de tener acceso al proyecto")
                self.source_app = firebase_admin.initialize_app(
                    options={'projectId': SOURCE_PROJECT_ID},
                    name='source'
                )
            
            self.source_db = firestore.client(app=self.source_app)
            print(f"   ✅ Conexión establecida")
            
            # Inicializar app de destino
            print(f"\n📤 PROYECTO DESTINO: {TARGET_PROJECT_ID}")
            print(f"   👤 Cuenta: juanp.gzmz@gmail.com")
            target_cred = self.get_credentials_for_project(TARGET_PROJECT_ID, TARGET_CREDENTIALS_FILE)
            
            if target_cred:
                print(f"   🔑 Usando credenciales desde: {TARGET_CREDENTIALS_FILE}")
                self.target_app = firebase_admin.initialize_app(
                    target_cred,
                    options={'projectId': TARGET_PROJECT_ID},
                    name='target'
                )
            else:
                print(f"   🔐 Usando Application Default Credentials (ADC)")
                print(f"   💡 Asegúrate de tener acceso con juanp.gzmz@gmail.com")
                self.target_app = firebase_admin.initialize_app(
                    options={'projectId': TARGET_PROJECT_ID},
                    name='target'
                )
            
            self.target_db = firestore.client(app=self.target_app)
            print(f"   ✅ Conexión establecida")
            
            print("\n" + "=" * 60)
            print("✅ Ambas conexiones establecidas correctamente")
            print("=" * 60)
            
        except Exception as e:
            print(f"\n❌ Error al inicializar Firebase: {e}")
            print("\n" + "=" * 60)
            print("💡 SOLUCIÓN: Configurar acceso a diferentes cuentas")
            print("=" * 60)
            print("\nOPCIÓN 1: Usar configuraciones de gcloud separadas")
            print("   Para el proyecto origen:")
            print("   $ gcloud config configurations create source-account")
            print("   $ gcloud config configurations activate source-account")
            print("   $ gcloud auth login")
            print("   $ gcloud auth application-default login")
            print(f"   $ gcloud config set project {SOURCE_PROJECT_ID}")
            print("\n   Para el proyecto destino (juanp.gzmz@gmail.com):")
            print("   $ gcloud config configurations create target-account")
            print("   $ gcloud config configurations activate target-account")
            print("   $ gcloud auth login  # Inicia sesión con juanp.gzmz@gmail.com")
            print("   $ gcloud auth application-default login")
            print(f"   $ gcloud config set project {TARGET_PROJECT_ID}")
            print("\nOPCIÓN 2: Usar archivos de credenciales de Service Account")
            print("   $ set SOURCE_CREDENTIALS_FILE=path/to/source-credentials.json")
            print("   $ set TARGET_CREDENTIALS_FILE=path/to/target-credentials.json")
            print("\nOPCIÓN 3: Usar ADC con cuenta que tenga acceso a ambos proyectos")
            print("   $ gcloud auth application-default login")
            print("   (Inicia sesión con una cuenta que tenga permisos en ambos proyectos)")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def serialize_value(self, value: Any) -> Any:
        """Convierte valores de Firestore a formato serializable"""
        if isinstance(value, DatetimeWithNanoseconds):
            return value
        elif isinstance(value, datetime):
            return value
        elif isinstance(value, DocumentReference):
            # Preservar referencias
            return value
        elif isinstance(value, GeoPoint):
            return value
        elif isinstance(value, dict):
            return {k: self.serialize_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self.serialize_value(item) for item in value]
        else:
            return value
    
    def get_document_data(self, doc: DocumentSnapshot) -> Dict[str, Any]:
        """Obtiene los datos de un documento preservando tipos especiales"""
        return {k: self.serialize_value(v) for k, v in doc.to_dict().items()}
    
    def backup_document(self, collection_path: str, doc_id: str, data: Dict[str, Any]):
        """Guarda un backup del documento en formato JSON"""
        backup_file = BACKUP_DIR / f"{collection_path.replace('/', '_')}_{doc_id}.json"
        
        # Convertir tipos no serializables para el backup
        def convert_for_json(obj):
            if isinstance(obj, (datetime, DatetimeWithNanoseconds)):
                return obj.isoformat()
            elif isinstance(obj, DocumentReference):
                return {'__type__': 'reference', 'path': obj.path}
            elif isinstance(obj, GeoPoint):
                return {'__type__': 'geopoint', 'latitude': obj.latitude, 'longitude': obj.longitude}
            elif isinstance(obj, dict):
                return {k: convert_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_for_json(item) for item in obj]
            return obj
        
        backup_data = {
            'collection_path': collection_path,
            'doc_id': doc_id,
            'data': convert_for_json(data),
            'timestamp': datetime.now().isoformat()
        }
        
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, indent=2, ensure_ascii=False)
    
    def document_needs_migration(self, source_doc_ref, target_doc_ref) -> tuple[bool, str]:
        """
        Verifica si un documento necesita ser migrado
        Retorna: (necesita_migración, razón)
        """
        try:
            # Obtener documento de destino
            target_doc = target_doc_ref.get()
            
            if not target_doc.exists:
                return (True, "nuevo")
            
            # Obtener documento de origen
            source_doc = source_doc_ref.get()
            if not source_doc.exists:
                return (False, "origen_no_existe")
            
            # Comparar timestamps de actualización
            source_data = source_doc.to_dict()
            target_data = target_doc.to_dict()
            
            # Si tienen timestamps, comparar
            source_update = source_doc.update_time
            target_update = target_doc.update_time
            
            if source_update and target_update:
                if source_update > target_update:
                    return (True, "actualizado")
            
            # Comparar número de campos como fallback
            if len(source_data) != len(target_data):
                return (True, "campos_diferentes")
            
            return (False, "actualizado")
            
        except Exception as e:
            # Si hay error, migrar por seguridad
            return (True, f"error_verificacion:{str(e)}")
    
    def migrate_document(self, source_doc_ref, target_collection_ref, doc_id: str, depth: int = 0):
        """Migra un documento específico incluyendo sus subcollecciones (solo si es necesario)"""
        indent = "  " * depth
        
        try:
            # Obtener documento de origen
            doc = source_doc_ref.get()
            
            if not doc.exists:
                print(f"{indent}⚠️  Documento {doc_id} no existe, saltando...")
                self.stats['skipped'] += 1
                return
            
            # Verificar si necesita migración
            target_doc_ref = target_collection_ref.document(doc_id)
            needs_migration, reason = self.document_needs_migration(source_doc_ref, target_doc_ref)
            
            if not needs_migration:
                print(f"{indent}⏭️  Documento ya actualizado: {doc_id}")
                self.stats['skipped'] += 1
                self.migration_log.append({
                    'type': 'skipped',
                    'path': source_doc_ref.path,
                    'doc_id': doc_id,
                    'reason': reason
                })
                
                # Aún así, revisar subcollecciones
                subcollections = source_doc_ref.collections()
                for subcollection in subcollections:
                    self.migrate_collection(subcollection, target_doc_ref, depth + 1)
                return
            
            # Obtener datos
            data = self.get_document_data(doc)
            
            # Crear backup
            collection_path = '/'.join(source_doc_ref.path.split('/')[:-1])
            self.backup_document(collection_path, doc_id, data)
            
            if not self.dry_run:
                # Escribir documento en destino
                target_doc_ref.set(data)
                print(f"{indent}✅ Documento migrado: {doc_id} ({reason})")
            else:
                print(f"{indent}🔍 [DRY-RUN] Documento a migrar: {doc_id} ({len(data)} campos, {reason})")
            
            self.stats['documents'] += 1
            self.migration_log.append({
                'type': 'document',
                'path': source_doc_ref.path,
                'doc_id': doc_id,
                'fields_count': len(data),
                'reason': reason,
                'status': 'migrated' if not self.dry_run else 'dry_run'
            })
            
            # Migrar subcollecciones
            subcollections = source_doc_ref.collections()
            for subcollection in subcollections:
                self.migrate_collection(subcollection, target_doc_ref, depth + 1)
                
        except Exception as e:
            print(f"{indent}❌ Error migrando documento {doc_id}: {e}")
            self.stats['errors'] += 1
            self.migration_log.append({
                'type': 'error',
                'path': source_doc_ref.path,
                'doc_id': doc_id,
                'error': str(e)
            })
    
    def migrate_collection(self, source_collection, target_parent, depth: int = 0):
        """Migra una colección completa incluyendo todos sus documentos"""
        collection_id = source_collection.id
        indent = "  " * depth
        
        if collection_id in EXCLUDED_COLLECTIONS:
            print(f"{indent}⏭️  Colección '{collection_id}' excluida, saltando...")
            return
        
        print(f"{indent}📁 {'Sub-' if depth > 0 else ''}Colección: {collection_id}")
        
        # Obtener referencia a la colección de destino
        if isinstance(target_parent, firestore.firestore.DocumentReference):
            target_collection = target_parent.collection(collection_id)
        else:
            target_collection = target_parent.collection(collection_id)
        
        self.stats['collections' if depth == 0 else 'subcollections'] += 1
        
        # Obtener documentos en lotes para evitar timeouts
        batch_size = 500
        last_doc = None
        doc_count = 0
        
        while True:
            # Crear query con límite y orden
            query = source_collection.order_by('__name__').limit(batch_size)
            
            # Si hay un último documento, empezar después de él
            if last_doc:
                query = query.start_after({'__name__': last_doc})
            
            # Obtener documentos del lote actual
            try:
                docs = list(query.stream())
            except Exception as e:
                print(f"{indent}⚠️ Error al obtener lote de documentos: {e}")
                self.stats['errors'] += 1
                break
            
            # Si no hay más documentos, terminar
            if not docs:
                break
            
            # Procesar cada documento del lote
            for doc in docs:
                doc_count += 1
                self.migrate_document(doc.reference, target_collection, doc.id, depth + 1)
                last_doc = doc.id
        
        print(f"{indent}✅ Colección '{collection_id}' procesada: {doc_count} documentos")
    
    def list_collections(self) -> List[str]:
        """Lista todas las colecciones principales en el proyecto de origen"""
        print("\n📋 Listando colecciones en proyecto de origen...")
        collections = self.source_db.collections()
        collection_names = [col.id for col in collections]
        
        print(f"\nEncontradas {len(collection_names)} colecciones:")
        for name in collection_names:
            # Contar documentos en cada colección
            doc_count = len(list(self.source_db.collection(name).limit(1000).stream()))
            print(f"  • {name} ({doc_count}+ documentos)")
        
        return collection_names
    
    def migrate_all(self, specific_collections: Optional[List[str]] = None):
        """Ejecuta la migración completa"""
        print("\n" + "="*60)
        print("🚀 INICIANDO MIGRACIÓN DE FIRESTORE")
        print("="*60)
        print(f"Origen:  {SOURCE_PROJECT_ID}")
        print(f"Destino: {TARGET_PROJECT_ID}")
        print(f"Modo:    {'DRY-RUN (sin cambios)' if self.dry_run else 'EJECUCIÓN REAL'}")
        print("="*60)
        
        # Inicializar conexiones
        self.initialize_apps()
        
        # Listar colecciones
        all_collections = self.list_collections()
        
        # Determinar qué colecciones migrar
        if specific_collections:
            collections_to_migrate = [col for col in specific_collections if col in all_collections]
            if len(collections_to_migrate) != len(specific_collections):
                missing = set(specific_collections) - set(all_collections)
                print(f"\n⚠️  Colecciones no encontradas: {', '.join(missing)}")
        else:
            collections_to_migrate = all_collections
        
        # Confirmar migración
        if not self.dry_run:
            print(f"\n⚠️  ADVERTENCIA: Esto migrará {len(collections_to_migrate)} colecciones")
            print(f"   desde {SOURCE_PROJECT_ID} hacia {TARGET_PROJECT_ID}")
            confirm = input("\n¿Continuar? (escribe 'SI' para confirmar): ")
            if confirm != 'SI':
                print("❌ Migración cancelada")
                return
        
        # Ejecutar migración
        start_time = datetime.now()
        
        for collection_name in collections_to_migrate:
            source_collection = self.source_db.collection(collection_name)
            self.migrate_collection(source_collection, self.target_db, depth=0)
        
        # Resumen final
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*60)
        print("📊 RESUMEN DE MIGRACIÓN")
        print("="*60)
        print(f"✅ Colecciones principales: {self.stats['collections']}")
        print(f"✅ Subcollecciones: {self.stats['subcollections']}")
        print(f"✅ Documentos migrados: {self.stats['documents']}")
        print(f"⏭️  Documentos saltados (ya actualizados): {self.stats['skipped']}")
        print(f"❌ Errores: {self.stats['errors']}")
        print(f"⏱️  Duración: {duration:.2f} segundos")
        
        # Calcular eficiencia
        total_docs = self.stats['documents'] + self.stats['skipped']
        if total_docs > 0:
            efficiency = (self.stats['skipped'] / total_docs) * 100
            print(f"📈 Eficiencia: {efficiency:.1f}% de documentos ya actualizados")
        
        print(f"📁 Backups guardados en: {BACKUP_DIR}")
        print("="*60)
        
        # Guardar log de migración
        log_file = BACKUP_DIR / f"migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump({
                'source_project': SOURCE_PROJECT_ID,
                'target_project': TARGET_PROJECT_ID,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'dry_run': self.dry_run,
                'stats': self.stats,
                'log': self.migration_log
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n📝 Log detallado guardado en: {log_file}")
        
        if self.dry_run:
            print("\n💡 Esto fue un DRY-RUN. Para ejecutar la migración real:")
            print("   python migrate_firestore.py")
        else:
            print("\n✨ Migración completada exitosamente!")
    
    def cleanup(self):
        """Limpia las conexiones"""
        if self.source_app:
            firebase_admin.delete_app(self.source_app)
        if self.target_app:
            firebase_admin.delete_app(self.target_app)


def main():
    parser = argparse.ArgumentParser(
        description='Migra datos de Firestore entre proyectos Firebase'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Ejecutar en modo preview sin hacer cambios reales'
    )
    parser.add_argument(
        '--collections',
        type=str,
        help='Colecciones específicas a migrar (separadas por coma)'
    )
    
    args = parser.parse_args()
    
    # Parsear colecciones específicas si se proporcionaron
    specific_collections = None
    if args.collections:
        specific_collections = [col.strip() for col in args.collections.split(',')]
    
    # Crear instancia de migración
    migration = FirestoreMigration(dry_run=args.dry_run)
    
    try:
        migration.migrate_all(specific_collections=specific_collections)
    except KeyboardInterrupt:
        print("\n\n⚠️  Migración interrumpida por el usuario")
    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
    finally:
        migration.cleanup()


if __name__ == "__main__":
    main()
