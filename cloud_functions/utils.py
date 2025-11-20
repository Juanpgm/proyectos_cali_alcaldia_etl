"""
Utilidades compartidas para Cloud Functions
Manejo de S3, Firebase, y transformación de datos
"""

import os
import json
import io
from typing import Dict, List, Any, Optional
import boto3
from botocore.exceptions import ClientError
from google.cloud import secretmanager
from google.cloud import firestore
import hashlib
from datetime import datetime


class S3Handler:
    """Maneja operaciones con AWS S3."""
    
    def __init__(self, bucket_name: str = "unidades-proyecto-documents"):
        """
        Inicializa el handler de S3.
        
        Args:
            bucket_name: Nombre del bucket S3
        """
        self.bucket_name = bucket_name
        self.s3_client = None
        
    def _get_aws_credentials(self) -> Dict[str, str]:
        """Obtiene credenciales AWS desde Secret Manager."""
        try:
            project_id = os.environ.get('GCP_PROJECT', 'dev-test-e778d')
            client = secretmanager.SecretManagerServiceClient()
            
            # Obtener credenciales desde Secret Manager
            secret_name = f"projects/{project_id}/secrets/aws-credentials/versions/latest"
            response = client.access_secret_version(request={"name": secret_name})
            credentials_json = response.payload.data.decode('UTF-8')
            credentials = json.loads(credentials_json)
            
            return credentials
            
        except Exception as e:
            print(f"❌ Error obteniendo credenciales AWS: {e}")
            raise
    
    def _initialize_s3_client(self):
        """Inicializa el cliente S3 con credenciales."""
        if self.s3_client is None:
            credentials = self._get_aws_credentials()
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=credentials['aws_access_key_id'],
                aws_secret_access_key=credentials['aws_secret_access_key'],
                region_name=credentials.get('region', 'us-east-1')
            )
    
    def read_json_from_s3(self, key: str) -> Optional[Dict]:
        """
        Lee un archivo JSON desde S3.
        
        Args:
            key: Clave del archivo en S3
            
        Returns:
            Diccionario con los datos JSON o None si falla
        """
        try:
            self._initialize_s3_client()
            
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            
            print(f"✓ Leído desde S3: s3://{self.bucket_name}/{key}")
            return data
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"⚠ Archivo no encontrado: {key}")
            else:
                print(f"❌ Error leyendo de S3: {e}")
            return None
        except Exception as e:
            print(f"❌ Error procesando archivo: {e}")
            return None
    
    def list_files(self, prefix: str) -> List[str]:
        """
        Lista archivos en un prefijo de S3.
        
        Args:
            prefix: Prefijo/carpeta en S3
            
        Returns:
            Lista de claves de archivos
        """
        try:
            self._initialize_s3_client()
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            files = [obj['Key'] for obj in response['Contents']]
            print(f"✓ Encontrados {len(files)} archivos en {prefix}")
            return files
            
        except Exception as e:
            print(f"❌ Error listando archivos: {e}")
            return []


class FirestoreHandler:
    """Maneja operaciones con Firestore."""
    
    def __init__(self):
        """Inicializa el handler de Firestore."""
        self.db = firestore.Client()
    
    def get_existing_documents(self, collection_name: str) -> Dict[str, Dict]:
        """
        Obtiene documentos existentes de una colección.
        
        Args:
            collection_name: Nombre de la colección
            
        Returns:
            Diccionario con {doc_id: {hash, data}}
        """
        try:
            existing = {}
            docs = self.db.collection(collection_name).stream()
            
            for doc in docs:
                doc_data = doc.to_dict()
                doc_hash = self._calculate_hash(doc_data)
                existing[doc.id] = {
                    'hash': doc_hash,
                    'data': doc_data
                }
            
            print(f"✓ Obtenidos {len(existing)} documentos de {collection_name}")
            return existing
            
        except Exception as e:
            print(f"❌ Error obteniendo documentos: {e}")
            return {}
    
    def upsert_document(self, collection_name: str, doc_id: str, data: Dict, 
                       existing_hash: Optional[str] = None) -> bool:
        """
        Inserta o actualiza un documento solo si hay cambios.
        
        Args:
            collection_name: Nombre de la colección
            doc_id: ID del documento
            data: Datos a guardar
            existing_hash: Hash del documento existente (para comparación)
            
        Returns:
            True si se actualizó, False si no hubo cambios
        """
        try:
            new_hash = self._calculate_hash(data)
            
            # Solo actualizar si hay cambios
            if existing_hash and new_hash == existing_hash:
                return False
            
            # Agregar metadata
            if existing_hash:
                data['updated_at'] = firestore.SERVER_TIMESTAMP
            else:
                data['created_at'] = firestore.SERVER_TIMESTAMP
                data['updated_at'] = firestore.SERVER_TIMESTAMP
            
            # Upsert
            doc_ref = self.db.collection(collection_name).document(doc_id)
            doc_ref.set(data, merge=True)
            
            return True
            
        except Exception as e:
            print(f"❌ Error en upsert de {doc_id}: {e}")
            return False
    
    def batch_upsert(self, collection_name: str, documents: Dict[str, Dict], 
                    existing_docs: Dict[str, Dict]) -> Dict[str, int]:
        """
        Upsert de documentos en lotes.
        
        Args:
            collection_name: Nombre de la colección
            documents: Diccionario {doc_id: data}
            existing_docs: Documentos existentes con hashes
            
        Returns:
            Estadísticas de la operación
        """
        stats = {'new': 0, 'updated': 0, 'unchanged': 0, 'failed': 0}
        
        try:
            batch = self.db.batch()
            batch_count = 0
            
            for doc_id, data in documents.items():
                try:
                    new_hash = self._calculate_hash(data)
                    existing = existing_docs.get(doc_id, {})
                    existing_hash = existing.get('hash')
                    
                    # Verificar si hay cambios
                    if existing_hash and new_hash == existing_hash:
                        stats['unchanged'] += 1
                        continue
                    
                    # Preparar documento
                    doc_data = data.copy()
                    if existing_hash:
                        doc_data['updated_at'] = firestore.SERVER_TIMESTAMP
                        stats['updated'] += 1
                    else:
                        doc_data['created_at'] = firestore.SERVER_TIMESTAMP
                        doc_data['updated_at'] = firestore.SERVER_TIMESTAMP
                        stats['new'] += 1
                    
                    # Agregar al batch
                    doc_ref = self.db.collection(collection_name).document(doc_id)
                    batch.set(doc_ref, doc_data, merge=True)
                    batch_count += 1
                    
                    # Commit cada 500 operaciones (límite de Firestore)
                    if batch_count >= 500:
                        batch.commit()
                        batch = self.db.batch()
                        batch_count = 0
                        
                except Exception as e:
                    print(f"⚠ Error procesando {doc_id}: {e}")
                    stats['failed'] += 1
            
            # Commit final
            if batch_count > 0:
                batch.commit()
            
            print(f"✓ Batch completado: +{stats['new']} nuevos, ~{stats['updated']} actualizados, ={stats['unchanged']} sin cambios")
            return stats
            
        except Exception as e:
            print(f"❌ Error en batch upsert: {e}")
            stats['failed'] += len(documents)
            return stats
    
    @staticmethod
    def _calculate_hash(data: Dict) -> str:
        """Calcula hash MD5 de un documento."""
        # Excluir campos de metadata
        clean_data = {
            k: v for k, v in data.items() 
            if k not in ['created_at', 'updated_at', 'processed_timestamp']
        }
        data_str = json.dumps(clean_data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()


class DataTransformer:
    """Transforma datos para carga en Firestore."""
    
    @staticmethod
    def transform_unidades_proyecto(geojson_data: Dict) -> Dict[str, Dict]:
        """
        Transforma features de GeoJSON a documentos de Firestore.
        
        Args:
            geojson_data: GeoJSON con features
            
        Returns:
            Diccionario {upid: document_data}
        """
        documents = {}
        features = geojson_data.get('features', [])
        
        for feature in features:
            try:
                properties = feature.get('properties', {})
                geometry = feature.get('geometry')
                
                # Obtener upid como ID único
                upid = properties.get('upid')
                if not upid:
                    continue
                
                # Mapear campos según especificaciones
                fuera_rango = properties.get('fuera_rango')
                
                # Lógica de mapeo de comuna_corregimiento
                if fuera_rango == 'ACEPTABLE':
                    comuna = properties.get('comuna_corregimiento_2')
                else:
                    comuna = properties.get('comuna_corregimiento')
                
                # Lógica de mapeo de barrio_vereda
                barrio_2 = properties.get('barrio_vereda_2')
                barrio = properties.get('barrio_vereda')
                barrio_final = barrio_2 if barrio_2 and str(barrio_2).strip() else barrio
                
                # Construir documento
                document = {
                    'type': 'Feature',
                    'geometry': geometry,
                    'properties': {
                        'upid': upid,
                        'nombre_up': properties.get('nombre_up'),
                        'nombre_up_detalle': properties.get('nombre_up_detalle'),
                        'direccion': properties.get('direccion'),
                        'comuna_corregimiento': comuna,
                        'barrio_vereda': barrio_final,
                        'tipo_intervencion': properties.get('tipo_intervencion'),
                        'tipo_equipamiento': properties.get('tipo_equipamiento'),
                        'estado': properties.get('estado'),
                        'presupuesto_base': properties.get('presupuesto_base'),
                        'avance_obra': properties.get('avance_obra'),
                        'avance_fisico_obra': properties.get('avance_fisico_obra'),
                        'fecha_inicio': properties.get('fecha_inicio_std'),
                        'fecha_fin': properties.get('fecha_fin_std'),
                        'referencia_proceso': properties.get('referencia_proceso'),
                        'referencia_contrato': properties.get('referencia_contrato'),
                        'url_proceso': properties.get('url_proceso'),
                        'identificador': properties.get('identificador'),
                        'bpin': properties.get('bpin'),
                        'nickname': properties.get('nickname'),
                        'nickname_detalle': properties.get('nickname_detalle'),
                        'descripcion_intervencion': properties.get('descripcion_intervencion'),
                        'fuera_rango': fuera_rango,
                        'processed_timestamp': properties.get('processed_timestamp')
                    }
                }
                
                # Limpiar None values
                document['properties'] = {
                    k: v for k, v in document['properties'].items() 
                    if v is not None
                }
                
                documents[upid] = document
                
            except Exception as e:
                print(f"⚠ Error transformando feature: {e}")
                continue
        
        print(f"✓ Transformados {len(documents)} documentos")
        return documents
    
    @staticmethod
    def prepare_log_document(log_data: Dict) -> Dict:
        """Prepara documento de log para Firestore."""
        return {
            'timestamp': log_data.get('execution_timestamp'),
            'process_name': log_data.get('process_name'),
            'version': log_data.get('version'),
            'data_loading': log_data.get('data_loading', {}),
            'data_transformation': log_data.get('data_transformation', {}),
            'geospatial_processing': log_data.get('geospatial_processing', {}),
            'validation': log_data.get('validation', {}),
            'date_processing': log_data.get('date_processing', {}),
            'summary': log_data.get('summary', {}),
            'created_at': firestore.SERVER_TIMESTAMP
        }
    
    @staticmethod
    def prepare_report_document(report_data: Dict) -> Dict:
        """Prepara documento de reporte para Firestore."""
        return {
            'metadata': report_data.get('metadata', {}),
            'resumen_ejecutivo': report_data.get('resumen_ejecutivo', {}),
            'analisis_detallado': report_data.get('analisis_detallado', {}),
            'recomendaciones': report_data.get('recomendaciones', []),
            'acciones_prioritarias': report_data.get('acciones_prioritarias', []),
            'metricas_calidad': report_data.get('metricas_calidad', {}),
            'created_at': firestore.SERVER_TIMESTAMP
        }
