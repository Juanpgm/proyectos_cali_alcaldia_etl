"""
Script para extraer datos del Dashboard de ArcGIS de Cali
URL: https://geoportal.cali.gov.co/arcgis/apps/opsdashboard/index.html#/fb87e184c255488fb4d10183f816d0a6

Los dashboards de ArcGIS Operations Dashboard consumen datos de servicios REST de ArcGIS.
Este script intenta identificar y extraer esos datos.
"""

import requests
import json
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs
import time
from datetime import datetime
import pandas as pd


class ArcGISDashboardScraper:
    """Scraper para dashboards de ArcGIS Operations Dashboard"""
    
    def __init__(self, dashboard_url: str):
        self.dashboard_url = dashboard_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'es-ES,es;q=0.9',
        })
        self.base_url = "https://geoportal.cali.gov.co"
        self.arcgis_server = f"{self.base_url}/agserver"
        self.discovered_services = []
        
    def extract_dashboard_id(self) -> Optional[str]:
        """Extrae el ID del dashboard de la URL"""
        match = re.search(r'#/([a-f0-9]+)', self.dashboard_url)
        return match.group(1) if match else None
    
    def discover_feature_services(self) -> List[str]:
        """
        Intenta descubrir los servicios de features que usa el dashboard.
        Los dashboards de ArcGIS típicamente usan servicios FeatureServer.
        """
        print("🔍 Buscando servicios de ArcGIS...")
        
        # Servicios comunes encontrados en el HTML del dashboard
        common_services = [
            "/rest/services/Hosted/survey123_9f77b14314db40cca29f48bbe746263d_form/FeatureServer",
            "/rest/services/Hosted/Presupuesto_Participativo_2025/FeatureServer",
            "/rest/services/Hosted/Emprestito_2025/FeatureServer",
            "/rest/services/Hosted/Intervenciones_2025/FeatureServer",
            "/rest/services/Hosted/Lotes_2025/FeatureServer",
        ]
        
        services = []
        for service_path in common_services:
            service_url = f"{self.arcgis_server}{service_path}"
            if self.check_service_exists(service_url):
                services.append(service_url)
                print(f"✅ Servicio encontrado: {service_path}")
        
        return services
    
    def check_service_exists(self, service_url: str) -> bool:
        """Verifica si un servicio existe"""
        try:
            response = self.session.get(f"{service_url}?f=json", timeout=10)
            if response.status_code == 200:
                data = response.json()
                return 'error' not in data
        except Exception as e:
            print(f"❌ Error verificando servicio: {e}")
        return False
    
    def get_service_metadata(self, service_url: str) -> Dict:
        """Obtiene metadata de un servicio de ArcGIS"""
        try:
            response = self.session.get(f"{service_url}?f=json", timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Error obteniendo metadata: {e}")
            return {}
    
    def get_layer_data(self, layer_url: str, where: str = "1=1", 
                       out_fields: str = "*", return_geometry: bool = True) -> Dict:
        """
        Extrae datos de una capa de ArcGIS FeatureServer
        
        Args:
            layer_url: URL de la capa (ej: .../FeatureServer/0)
            where: Cláusula WHERE para filtrar datos
            out_fields: Campos a retornar (* para todos)
            return_geometry: Si retornar geometría
        """
        query_url = f"{layer_url}/query"
        
        params = {
            'where': where,
            'outFields': out_fields,
            'returnGeometry': str(return_geometry).lower(),
            'f': 'json',
            'outSR': '4326',  # WGS84
        }
        
        try:
            print(f"📥 Consultando: {layer_url}")
            response = self.session.get(query_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'error' in data:
                print(f"❌ Error en la consulta: {data['error']}")
                return {}
            
            feature_count = len(data.get('features', []))
            print(f"✅ {feature_count} registros obtenidos")
            
            return data
            
        except Exception as e:
            print(f"❌ Error consultando capa: {e}")
            return {}
    
    def extract_all_layers_from_service(self, service_url: str) -> Dict[str, any]:
        """Extrae todas las capas de un servicio"""
        print(f"\n{'='*60}")
        print(f"📊 Procesando servicio: {service_url}")
        print(f"{'='*60}")
        
        metadata = self.get_service_metadata(service_url)
        
        if not metadata:
            return {}
        
        service_info = {
            'service_url': service_url,
            'service_name': metadata.get('name'),
            'description': metadata.get('serviceDescription'),
            'layers': []
        }
        
        # Obtener información de capas
        layers = metadata.get('layers', [])
        tables = metadata.get('tables', [])
        
        all_layers = layers + tables
        
        for layer_info in all_layers:
            layer_id = layer_info['id']
            layer_name = layer_info['name']
            layer_url = f"{service_url}/{layer_id}"
            
            print(f"\n📋 Capa {layer_id}: {layer_name}")
            
            # Obtener datos de la capa
            layer_data = self.get_layer_data(layer_url)
            
            if layer_data:
                service_info['layers'].append({
                    'id': layer_id,
                    'name': layer_name,
                    'url': layer_url,
                    'data': layer_data
                })
        
        return service_info
    
    def scrape_all_data(self) -> Dict:
        """Método principal para extraer todos los datos del dashboard"""
        print("🚀 Iniciando extracción de datos del Dashboard de Cali")
        print(f"🌐 URL: {self.dashboard_url}\n")
        
        dashboard_id = self.extract_dashboard_id()
        print(f"🆔 Dashboard ID: {dashboard_id}\n")
        
        # Descubrir servicios
        services = self.discover_feature_services()
        
        if not services:
            print("⚠️ No se encontraron servicios automáticamente.")
            print("💡 Intentando servicios conocidos...")
        
        # Extraer datos de todos los servicios
        all_data = {
            'extraction_date': datetime.now().isoformat(),
            'dashboard_url': self.dashboard_url,
            'dashboard_id': dashboard_id,
            'services': []
        }
        
        for service_url in services:
            try:
                service_data = self.extract_all_layers_from_service(service_url)
                if service_data:
                    all_data['services'].append(service_data)
                time.sleep(1)  # Pausa entre servicios
            except Exception as e:
                print(f"❌ Error procesando servicio {service_url}: {e}")
        
        return all_data
    
    def save_to_json(self, data: Dict, filename: str = None):
        """Guarda los datos en formato JSON"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"dashboard_cali_data_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Datos guardados en: {filename}")
        return filename
    
    def convert_to_dataframes(self, data: Dict) -> Dict[str, pd.DataFrame]:
        """Convierte los datos a DataFrames de pandas"""
        dataframes = {}
        
        for service in data.get('services', []):
            service_name = service.get('service_name', 'Unknown')
            
            for layer in service.get('layers', []):
                layer_name = layer['name']
                layer_data = layer.get('data', {})
                features = layer_data.get('features', [])
                
                if features:
                    # Extraer atributos y geometría
                    records = []
                    for feature in features:
                        record = feature.get('attributes', {})
                        
                        # Agregar geometría si existe
                        geometry = feature.get('geometry')
                        if geometry:
                            if 'x' in geometry and 'y' in geometry:
                                record['longitude'] = geometry['x']
                                record['latitude'] = geometry['y']
                            elif 'rings' in geometry:
                                record['geometry_type'] = 'polygon'
                                record['geometry'] = json.dumps(geometry)
                            elif 'paths' in geometry:
                                record['geometry_type'] = 'polyline'
                                record['geometry'] = json.dumps(geometry)
                        
                        records.append(record)
                    
                    df = pd.DataFrame(records)
                    df_name = f"{service_name}_{layer_name}"
                    dataframes[df_name] = df
                    
                    print(f"📊 DataFrame '{df_name}': {len(df)} filas, {len(df.columns)} columnas")
        
        return dataframes
    
    def export_to_excel(self, data: Dict, filename: str = None):
        """Exporta los datos a Excel con múltiples hojas"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"dashboard_cali_data_{timestamp}.xlsx"
        
        dataframes = self.convert_to_dataframes(data)
        
        if not dataframes:
            print("⚠️ No hay datos para exportar")
            return None
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for sheet_name, df in dataframes.items():
                # Excel limita nombres de hojas a 31 caracteres
                safe_sheet_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
        
        print(f"\n📊 Datos exportados a Excel: {filename}")
        return filename


def main():
    """Función principal"""
    dashboard_url = "https://geoportal.cali.gov.co/arcgis/apps/opsdashboard/index.html#/fb87e184c255488fb4d10183f816d0a6"
    
    # Crear scraper
    scraper = ArcGISDashboardScraper(dashboard_url)
    
    # Extraer todos los datos
    data = scraper.scrape_all_data()
    
    # Guardar en JSON
    json_file = scraper.save_to_json(data)
    
    # Exportar a Excel
    excel_file = scraper.export_to_excel(data)
    
    # Mostrar resumen
    print("\n" + "="*60)
    print("✅ EXTRACCIÓN COMPLETADA")
    print("="*60)
    print(f"📁 Archivos generados:")
    print(f"   - JSON: {json_file}")
    if excel_file:
        print(f"   - Excel: {excel_file}")
    
    print(f"\n📈 Resumen de datos extraídos:")
    for service in data.get('services', []):
        print(f"\n  Servicio: {service.get('service_name')}")
        for layer in service.get('layers', []):
            feature_count = len(layer.get('data', {}).get('features', []))
            print(f"    - {layer['name']}: {feature_count} registros")


if __name__ == "__main__":
    main()
