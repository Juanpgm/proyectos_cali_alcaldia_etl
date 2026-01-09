"""
Script para descargar attachments (fotografías) del Dashboard de Cali
Los dashboards de ArcGIS pueden tener archivos adjuntos (fotos, PDFs, etc.)
"""

import requests
import os
import json
from typing import List, Dict
from pathlib import Path
from datetime import datetime
import time


class DashboardAttachmentDownloader:
    """Descarga attachments (fotos) de los servicios de ArcGIS"""
    
    def __init__(self, base_url: str = "https://geoportal.cali.gov.co/agserver"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        })
        self.download_folder = "dashboard_attachments"
        
    def create_download_folder(self, folder_name: str = None) -> Path:
        """Crea la carpeta de descargas"""
        if folder_name:
            self.download_folder = folder_name
        
        folder = Path(self.download_folder)
        folder.mkdir(exist_ok=True)
        return folder
    
    def get_feature_attachments_info(self, layer_url: str, object_id: int) -> List[Dict]:
        """
        Obtiene información de los attachments de un feature
        
        Args:
            layer_url: URL de la capa (ej: .../FeatureServer/0)
            object_id: OBJECTID del feature
        
        Returns:
            Lista de attachments con su info
        """
        attachments_url = f"{layer_url}/{object_id}/attachments"
        
        try:
            response = self.session.get(
                attachments_url,
                params={'f': 'json'},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            if 'error' in data:
                return []
            
            return data.get('attachmentInfos', [])
            
        except Exception as e:
            print(f"❌ Error obteniendo attachments para OBJECTID {object_id}: {e}")
            return []
    
    def download_attachment(self, layer_url: str, object_id: int, 
                          attachment_id: int, filename: str,
                          subfolder: str = None) -> bool:
        """
        Descarga un attachment específico
        
        Args:
            layer_url: URL de la capa
            object_id: OBJECTID del feature
            attachment_id: ID del attachment
            filename: Nombre del archivo a guardar
            subfolder: Subcarpeta opcional
        
        Returns:
            True si se descargó correctamente
        """
        attachment_url = f"{layer_url}/{object_id}/attachments/{attachment_id}"
        
        try:
            response = self.session.get(attachment_url, timeout=30, stream=True)
            response.raise_for_status()
            
            # Determinar ruta de guardado
            if subfolder:
                save_folder = Path(self.download_folder) / subfolder
                save_folder.mkdir(exist_ok=True)
            else:
                save_folder = Path(self.download_folder)
            
            file_path = save_folder / filename
            
            # Guardar archivo
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size = os.path.getsize(file_path) / 1024  # KB
            print(f"   ✅ Descargado: {filename} ({file_size:.1f} KB)")
            return True
            
        except Exception as e:
            print(f"   ❌ Error descargando {filename}: {e}")
            return False
    
    def get_all_features_with_attachments(self, layer_url: str) -> List[Dict]:
        """
        Obtiene todos los features que tienen attachments
        
        Returns:
            Lista de features con attachments
        """
        query_url = f"{layer_url}/query"
        
        params = {
            'where': '1=1',
            'outFields': 'OBJECTID',
            'returnGeometry': 'false',
            'f': 'json',
            'returnIdsOnly': 'false'
        }
        
        try:
            print(f"🔍 Buscando features con attachments...")
            response = self.session.get(query_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            features = data.get('features', [])
            print(f"📊 {len(features)} features encontrados")
            
            # Filtrar solo los que tienen attachments
            features_with_attachments = []
            
            for feature in features:
                object_id = feature['attributes']['OBJECTID']
                
                # Verificar si tiene attachments
                attachments = self.get_feature_attachments_info(layer_url, object_id)
                
                if attachments:
                    features_with_attachments.append({
                        'object_id': object_id,
                        'attachments': attachments
                    })
                
                time.sleep(0.1)  # Pequeña pausa
            
            print(f"📎 {len(features_with_attachments)} features tienen attachments")
            return features_with_attachments
            
        except Exception as e:
            print(f"❌ Error consultando features: {e}")
            return []
    
    def download_all_attachments_from_layer(self, layer_url: str, 
                                           layer_name: str = None) -> Dict:
        """
        Descarga todos los attachments de una capa
        
        Returns:
            Resumen de la descarga
        """
        if not layer_name:
            layer_name = layer_url.split('/')[-2]
        
        print(f"\n{'='*60}")
        print(f"📥 Descargando attachments de: {layer_name}")
        print(f"{'='*60}")
        
        # Crear subcarpeta para esta capa
        layer_folder = layer_name.replace(' ', '_').replace('/', '_')
        
        # Obtener features con attachments
        features = self.get_all_features_with_attachments(layer_url)
        
        if not features:
            print("⚠️ No hay attachments para descargar")
            return {
                'layer_name': layer_name,
                'total_features': 0,
                'total_attachments': 0,
                'downloaded': 0,
                'failed': 0
            }
        
        # Descargar attachments
        total_attachments = sum(len(f['attachments']) for f in features)
        downloaded = 0
        failed = 0
        
        print(f"\n📦 Descargando {total_attachments} archivos...\n")
        
        for feature in features:
            object_id = feature['object_id']
            attachments = feature['attachments']
            
            print(f"📎 Feature OBJECTID {object_id}: {len(attachments)} attachments")
            
            for attachment in attachments:
                attachment_id = attachment['id']
                original_name = attachment['name']
                content_type = attachment.get('contentType', 'unknown')
                size = attachment.get('size', 0)
                
                # Crear nombre de archivo único
                filename = f"objectid_{object_id}_{attachment_id}_{original_name}"
                
                # Descargar
                success = self.download_attachment(
                    layer_url,
                    object_id,
                    attachment_id,
                    filename,
                    subfolder=layer_folder
                )
                
                if success:
                    downloaded += 1
                else:
                    failed += 1
                
                time.sleep(0.2)  # Pausa entre descargas
        
        summary = {
            'layer_name': layer_name,
            'total_features': len(features),
            'total_attachments': total_attachments,
            'downloaded': downloaded,
            'failed': failed
        }
        
        print(f"\n✅ Descarga completada para {layer_name}")
        print(f"   📊 {downloaded} archivos descargados, {failed} fallidos")
        
        return summary
    
    def download_attachments_from_service(self, service_url: str) -> List[Dict]:
        """
        Descarga attachments de todas las capas de un servicio
        """
        print(f"\n{'='*60}")
        print(f"🚀 Procesando servicio: {service_url}")
        print(f"{'='*60}")
        
        # Obtener metadata del servicio
        try:
            response = self.session.get(f"{service_url}?f=json", timeout=10)
            response.raise_for_status()
            metadata = response.json()
        except Exception as e:
            print(f"❌ Error obteniendo metadata: {e}")
            return []
        
        # Obtener capas
        layers = metadata.get('layers', []) + metadata.get('tables', [])
        
        if not layers:
            print("⚠️ No hay capas en este servicio")
            return []
        
        summaries = []
        
        for layer_info in layers:
            layer_id = layer_info['id']
            layer_name = layer_info['name']
            layer_url = f"{service_url}/{layer_id}"
            
            # Verificar si la capa soporta attachments
            # (esto requeriría consultar la metadata de la capa individualmente)
            
            summary = self.download_all_attachments_from_layer(layer_url, layer_name)
            summaries.append(summary)
            
            time.sleep(1)  # Pausa entre capas
        
        return summaries
    
    def save_download_summary(self, summaries: List[Dict], filename: str = None):
        """Guarda un resumen de las descargas"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"download_summary_{timestamp}.json"
        
        summary_data = {
            'download_date': datetime.now().isoformat(),
            'total_layers': len(summaries),
            'layers': summaries,
            'totals': {
                'features': sum(s['total_features'] for s in summaries),
                'attachments': sum(s['total_attachments'] for s in summaries),
                'downloaded': sum(s['downloaded'] for s in summaries),
                'failed': sum(s['failed'] for s in summaries)
            }
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Resumen guardado en: {filename}")
        return summary_data


def main():
    """Función principal - descargar attachments del dashboard"""
    
    print("="*70)
    print("  DESCARGADOR DE ATTACHMENTS - Dashboard Cali")
    print("="*70)
    
    # Servicio conocido que tiene fotos (survey123)
    service_url = "https://geoportal.cali.gov.co/agserver/rest/services/Hosted/survey123_9f77b14314db40cca29f48bbe746263d_form/FeatureServer"
    
    # Crear descargador
    downloader = DashboardAttachmentDownloader()
    
    # Crear carpeta de descargas
    downloader.create_download_folder("fotos_dashboard_cali")
    
    print(f"\n📁 Carpeta de descargas: {downloader.download_folder}")
    
    # Descargar attachments
    summaries = downloader.download_attachments_from_service(service_url)
    
    # Guardar resumen
    summary_data = downloader.save_download_summary(summaries)
    
    # Mostrar resumen final
    print("\n" + "="*70)
    print("✅ DESCARGA COMPLETADA")
    print("="*70)
    print(f"\n📊 Resumen:")
    print(f"   Capas procesadas: {summary_data['totals']['features']}")
    print(f"   Features con attachments: {summary_data['totals']['features']}")
    print(f"   Archivos descargados: {summary_data['totals']['downloaded']}")
    print(f"   Archivos fallidos: {summary_data['totals']['failed']}")
    print(f"\n📁 Archivos guardados en: {downloader.download_folder}/")


if __name__ == "__main__":
    main()
