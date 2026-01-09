"""
Script Avanzado para extraer datos del Dashboard de ArcGIS de Cali
Incluye descubrimiento automático de servicios analizando el código JavaScript
"""

import requests
import json
import re
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
import time
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os


class AdvancedArcGISScraper:
    """
    Scraper avanzado que analiza el código JavaScript del dashboard
    para descubrir automáticamente todos los servicios REST de ArcGIS
    """
    
    def __init__(self, dashboard_url: str, use_selenium: bool = False):
        self.dashboard_url = dashboard_url
        self.use_selenium = use_selenium
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'es-ES,es;q=0.9',
            'Referer': dashboard_url,
        })
        self.base_url = "https://geoportal.cali.gov.co"
        self.discovered_services = set()
        self.driver = None
    
    def setup_selenium(self):
        """Configura Selenium para análisis dinámico"""
        if self.driver:
            return
        
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument(f'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            print("✅ Selenium configurado correctamente")
        except Exception as e:
            print(f"⚠️ No se pudo configurar Selenium: {e}")
            print("💡 Continuando sin Selenium...")
            self.use_selenium = False
    
    def close_selenium(self):
        """Cierra el navegador Selenium"""
        if self.driver:
            self.driver.quit()
            self.driver = None
    
    def analyze_dashboard_with_selenium(self) -> Set[str]:
        """Analiza el dashboard usando Selenium para capturar llamadas dinámicas"""
        print("🔍 Analizando dashboard con Selenium...")
        
        if not self.use_selenium:
            return set()
        
        self.setup_selenium()
        if not self.driver:
            return set()
        
        services = set()
        
        try:
            # Habilitar captura de logs de red
            self.driver.get(self.dashboard_url)
            
            # Esperar a que cargue el dashboard
            print("⏳ Esperando que cargue el dashboard...")
            time.sleep(10)  # Dar tiempo para que carguen los servicios
            
            # Capturar todas las requests
            logs = self.driver.get_log('performance')
            
            for log in logs:
                message = json.loads(log['message'])
                method = message.get('message', {}).get('method', '')
                
                if method == 'Network.requestWillBeSent':
                    url = message['message']['params']['request']['url']
                    
                    # Buscar URLs de FeatureServer
                    if 'FeatureServer' in url or 'MapServer' in url:
                        # Extraer la URL base del servicio
                        match = re.search(r'(.*?/FeatureServer|.*?/MapServer)', url)
                        if match:
                            service_url = match.group(1)
                            services.add(service_url)
                            print(f"🎯 Servicio detectado: {service_url}")
            
            # También buscar en el código JavaScript cargado
            scripts = self.driver.find_elements(By.TAG_NAME, 'script')
            for script in scripts:
                script_content = script.get_attribute('innerHTML')
                if script_content:
                    found_services = self.extract_services_from_text(script_content)
                    services.update(found_services)
            
        except Exception as e:
            print(f"❌ Error en análisis con Selenium: {e}")
        
        return services
    
    def extract_services_from_text(self, text: str) -> Set[str]:
        """Extrae URLs de servicios de ArcGIS de un texto"""
        services = set()
        
        # Patrones para encontrar servicios de ArcGIS
        patterns = [
            r'https?://[^"\'\s]+/FeatureServer(?:/\d+)?',
            r'https?://[^"\'\s]+/MapServer(?:/\d+)?',
            r'/rest/services/[^"\'\s]+/FeatureServer',
            r'/rest/services/[^"\'\s]+/MapServer',
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                url = match.group(0)
                
                # Limpiar la URL
                url = url.rstrip('",\'\\')
                
                # Normalizar URL relativa
                if url.startswith('/'):
                    url = f"{self.base_url}{url}"
                
                # Remover el número de capa si existe
                base_service = re.sub(r'/\d+$', '', url)
                services.add(base_service)
        
        return services
    
    def fetch_dashboard_source(self) -> str:
        """Descarga el HTML del dashboard"""
        try:
            print("📥 Descargando código fuente del dashboard...")
            response = self.session.get(self.dashboard_url, timeout=30)
            response.raise_for_status()
            print("✅ Código fuente descargado")
            return response.text
        except Exception as e:
            print(f"❌ Error descargando dashboard: {e}")
            return ""
    
    def parse_dashboard_html(self, html: str) -> Set[str]:
        """Analiza el HTML del dashboard para encontrar servicios"""
        print("🔎 Analizando HTML del dashboard...")
        
        services = self.extract_services_from_text(html)
        
        # Parsear con BeautifulSoup para encontrar scripts externos
        soup = BeautifulSoup(html, 'html.parser')
        
        # Buscar en todos los scripts
        for script in soup.find_all('script'):
            if script.string:
                found_services = self.extract_services_from_text(script.string)
                services.update(found_services)
            
            # Si tiene src, descargar el script externo
            if script.get('src'):
                script_url = script['src']
                if script_url.startswith('/'):
                    script_url = urljoin(self.base_url, script_url)
                elif not script_url.startswith('http'):
                    script_url = urljoin(self.dashboard_url, script_url)
                
                # Descargar script externo
                try:
                    script_content = self.fetch_external_script(script_url)
                    if script_content:
                        found_services = self.extract_services_from_text(script_content)
                        services.update(found_services)
                except Exception as e:
                    pass  # Ignorar errores en scripts externos
        
        return services
    
    def fetch_external_script(self, script_url: str) -> str:
        """Descarga un script externo"""
        try:
            response = self.session.get(script_url, timeout=10)
            response.raise_for_status()
            return response.text
        except:
            return ""
    
    def discover_all_services(self) -> Set[str]:
        """Descubre todos los servicios posibles"""
        print("\n" + "="*60)
        print("🔍 DESCUBRIENDO SERVICIOS")
        print("="*60)
        
        all_services = set()
        
        # Método 1: Analizar HTML estático
        html = self.fetch_dashboard_source()
        if html:
            services_from_html = self.parse_dashboard_html(html)
            all_services.update(services_from_html)
            print(f"✅ Encontrados {len(services_from_html)} servicios desde HTML")
        
        # Método 2: Usar Selenium si está disponible
        if self.use_selenium:
            services_from_selenium = self.analyze_dashboard_with_selenium()
            all_services.update(services_from_selenium)
            print(f"✅ Encontrados {len(services_from_selenium)} servicios adicionales con Selenium")
        
        # Método 3: Servicios conocidos comunes
        known_services = self.get_known_services()
        all_services.update(known_services)
        
        # Método 4: Explorar directorio de servicios
        catalog_services = self.explore_services_catalog()
        all_services.update(catalog_services)
        
        print(f"\n📊 Total de servicios descubiertos: {len(all_services)}")
        return all_services
    
    def get_known_services(self) -> Set[str]:
        """Retorna servicios conocidos basados en el contenido del dashboard"""
        print("📚 Agregando servicios conocidos...")
        
        known = {
            f"{self.base_url}/agserver/rest/services/Hosted/survey123_9f77b14314db40cca29f48bbe746263d_form/FeatureServer",
            f"{self.base_url}/agserver/rest/services/Hosted/Presupuesto_Participativo_2025/FeatureServer",
            f"{self.base_url}/agserver/rest/services/Hosted/Emprestito_2025/FeatureServer",
            f"{self.base_url}/agserver/rest/services/Hosted/Intervenciones_Contratistas_2025/FeatureServer",
        }
        
        # Verificar cuáles existen
        verified = set()
        for service_url in known:
            if self.verify_service(service_url):
                verified.add(service_url)
        
        print(f"✅ {len(verified)} servicios conocidos verificados")
        return verified
    
    def explore_services_catalog(self) -> Set[str]:
        """Explora el catálogo de servicios de ArcGIS"""
        print("🗂️ Explorando catálogo de servicios...")
        
        catalog_url = f"{self.base_url}/agserver/rest/services"
        services = set()
        
        try:
            response = self.session.get(f"{catalog_url}?f=json", timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                # Buscar en folders
                for folder in data.get('folders', []):
                    folder_url = f"{catalog_url}/{folder}"
                    folder_services = self.explore_folder(folder_url)
                    services.update(folder_services)
                
                # Buscar servicios en el root
                for service_info in data.get('services', []):
                    service_name = service_info['name']
                    service_type = service_info['type']
                    service_url = f"{catalog_url}/{service_name}/{service_type}"
                    if 'FeatureServer' in service_type or 'MapServer' in service_type:
                        services.add(service_url)
        
        except Exception as e:
            print(f"⚠️ No se pudo explorar catálogo: {e}")
        
        if services:
            print(f"✅ {len(services)} servicios encontrados en catálogo")
        
        return services
    
    def explore_folder(self, folder_url: str) -> Set[str]:
        """Explora un folder del catálogo"""
        services = set()
        
        try:
            response = self.session.get(f"{folder_url}?f=json", timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                for service_info in data.get('services', []):
                    service_name = service_info['name']
                    service_type = service_info['type']
                    # El nombre ya incluye el folder
                    service_url = f"{self.base_url}/agserver/rest/services/{service_name}/{service_type}"
                    if 'FeatureServer' in service_type or 'MapServer' in service_type:
                        services.add(service_url)
        
        except:
            pass
        
        return services
    
    def verify_service(self, service_url: str) -> bool:
        """Verifica si un servicio existe y es accesible"""
        try:
            response = self.session.get(f"{service_url}?f=json", timeout=10)
            if response.status_code == 200:
                data = response.json()
                return 'error' not in data
        except:
            pass
        return False
    
    def get_service_metadata(self, service_url: str) -> Dict:
        """Obtiene metadata completa de un servicio"""
        try:
            response = self.session.get(f"{service_url}?f=json", timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {'error': str(e)}
    
    def query_layer(self, layer_url: str, where: str = "1=1", 
                    out_fields: str = "*", return_geometry: bool = True,
                    result_offset: int = 0, result_record_count: int = 1000) -> Dict:
        """
        Consulta una capa con paginación
        """
        query_url = f"{layer_url}/query"
        
        params = {
            'where': where,
            'outFields': out_fields,
            'returnGeometry': str(return_geometry).lower(),
            'f': 'json',
            'outSR': '4326',
            'resultOffset': result_offset,
            'resultRecordCount': result_record_count,
        }
        
        try:
            response = self.session.get(query_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {'error': str(e)}
    
    def extract_all_features(self, layer_url: str, batch_size: int = 1000) -> List[Dict]:
        """Extrae todos los features de una capa usando paginación"""
        all_features = []
        offset = 0
        
        while True:
            result = self.query_layer(
                layer_url,
                result_offset=offset,
                result_record_count=batch_size
            )
            
            if 'error' in result:
                print(f"❌ Error: {result['error']}")
                break
            
            features = result.get('features', [])
            if not features:
                break
            
            all_features.extend(features)
            offset += len(features)
            
            print(f"   📦 Descargados {len(all_features)} registros...", end='\r')
            
            # Si obtuvimos menos registros que el batch_size, terminamos
            if len(features) < batch_size:
                break
            
            time.sleep(0.5)  # Pausa entre requests
        
        print(f"   ✅ Total: {len(all_features)} registros descargados")
        return all_features
    
    def scrape_complete_dashboard(self) -> Dict:
        """Extrae completamente todos los datos del dashboard"""
        print("🚀 INICIANDO EXTRACCIÓN COMPLETA")
        print("="*60)
        
        # Descubrir servicios
        services = self.discover_all_services()
        
        if not services:
            print("⚠️ No se encontraron servicios")
            return {}
        
        # Extraer datos
        result = {
            'extraction_date': datetime.now().isoformat(),
            'dashboard_url': self.dashboard_url,
            'total_services': len(services),
            'services': []
        }
        
        for service_url in sorted(services):
            print(f"\n{'='*60}")
            print(f"📊 Procesando: {service_url}")
            print(f"{'='*60}")
            
            metadata = self.get_service_metadata(service_url)
            
            if 'error' in metadata:
                print(f"❌ Error accediendo al servicio: {metadata['error']}")
                continue
            
            service_data = {
                'url': service_url,
                'name': metadata.get('serviceDescription') or metadata.get('name'),
                'layers': []
            }
            
            # Procesar cada capa
            layers = metadata.get('layers', []) + metadata.get('tables', [])
            
            for layer_info in layers:
                layer_id = layer_info['id']
                layer_name = layer_info['name']
                layer_url = f"{service_url}/{layer_id}"
                
                print(f"\n📋 Capa {layer_id}: {layer_name}")
                
                # Extraer todos los features
                features = self.extract_all_features(layer_url)
                
                if features:
                    service_data['layers'].append({
                        'id': layer_id,
                        'name': layer_name,
                        'url': layer_url,
                        'feature_count': len(features),
                        'features': features
                    })
            
            result['services'].append(service_data)
            time.sleep(1)
        
        return result
    
    def save_results(self, data: Dict, base_filename: str = "dashboard_complete"):
        """Guarda los resultados en múltiples formatos"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # JSON
        json_file = f"{base_filename}_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\n💾 JSON guardado: {json_file}")
        
        # Excel
        try:
            excel_file = f"{base_filename}_{timestamp}.xlsx"
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                for service in data.get('services', []):
                    service_name = service.get('name', 'Unknown')[:20]
                    
                    for layer in service.get('layers', []):
                        layer_name = layer['name']
                        features = layer.get('features', [])
                        
                        if features:
                            records = []
                            for feature in features:
                                record = feature.get('attributes', {})
                                
                                # Agregar geometría
                                geometry = feature.get('geometry')
                                if geometry:
                                    if 'x' in geometry and 'y' in geometry:
                                        record['longitude'] = geometry['x']
                                        record['latitude'] = geometry['y']
                                
                                records.append(record)
                            
                            if records:
                                df = pd.DataFrame(records)
                                sheet_name = f"{service_name}_{layer_name}"[:31]
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"📊 Excel guardado: {excel_file}")
        except Exception as e:
            print(f"⚠️ No se pudo crear Excel: {e}")
        
        return json_file


def main():
    """Función principal"""
    dashboard_url = "https://geoportal.cali.gov.co/arcgis/apps/opsdashboard/index.html#/fb87e184c255488fb4d10183f816d0a6"
    
    # Opción 1: Sin Selenium (más rápido, menos completo)
    print("🔧 Modo: Análisis estático (sin Selenium)")
    scraper = AdvancedArcGISScraper(dashboard_url, use_selenium=False)
    
    # Opción 2: Con Selenium (más completo, requiere ChromeDriver)
    # print("🔧 Modo: Análisis dinámico (con Selenium)")
    # scraper = AdvancedArcGISScraper(dashboard_url, use_selenium=True)
    
    try:
        # Extraer datos
        data = scraper.scrape_complete_dashboard()
        
        # Guardar resultados
        scraper.save_results(data)
        
        # Resumen
        print("\n" + "="*60)
        print("✅ EXTRACCIÓN COMPLETADA")
        print("="*60)
        
        total_features = sum(
            layer['feature_count'] 
            for service in data.get('services', [])
            for layer in service.get('layers', [])
        )
        
        print(f"📊 Servicios procesados: {len(data.get('services', []))}")
        print(f"📦 Total de registros: {total_features}")
        
    finally:
        scraper.close_selenium()


if __name__ == "__main__":
    main()
