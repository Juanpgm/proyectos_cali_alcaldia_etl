"""
Script de prueba rápida para verificar que el scraper funciona correctamente
Ejecuta este script primero para validar la configuración
"""

import sys

def test_imports():
    """Verifica que todas las dependencias estén instaladas"""
    print("🔍 Verificando dependencias...")
    
    required_packages = {
        'requests': 'requests',
        'pandas': 'pandas',
        'openpyxl': 'openpyxl',
    }
    
    optional_packages = {
        'beautifulsoup4': 'bs4',
        'selenium': 'selenium',
    }
    
    missing_required = []
    missing_optional = []
    
    # Verificar requeridas
    for package_name, import_name in required_packages.items():
        try:
            __import__(import_name)
            print(f"   ✅ {package_name}")
        except ImportError:
            print(f"   ❌ {package_name} - NO INSTALADO")
            missing_required.append(package_name)
    
    # Verificar opcionales
    for package_name, import_name in optional_packages.items():
        try:
            __import__(import_name)
            print(f"   ✅ {package_name} (opcional)")
        except ImportError:
            print(f"   ⚠️  {package_name} (opcional) - No instalado")
            missing_optional.append(package_name)
    
    if missing_required:
        print(f"\n❌ Faltan dependencias requeridas: {', '.join(missing_required)}")
        print(f"💡 Instalar con: pip install {' '.join(missing_required)}")
        return False
    
    if missing_optional:
        print(f"\n⚠️  Dependencias opcionales no instaladas: {', '.join(missing_optional)}")
        print(f"💡 Para funcionalidad completa: pip install {' '.join(missing_optional)}")
    
    print("\n✅ Todas las dependencias requeridas están instaladas")
    return True


def test_connectivity():
    """Verifica conectividad con el servidor de ArcGIS"""
    print("\n🌐 Verificando conectividad...")
    
    try:
        import requests
        
        # Test 1: Servidor base
        base_url = "https://geoportal.cali.gov.co"
        response = requests.get(base_url, timeout=10)
        
        if response.status_code == 200:
            print(f"   ✅ Conexión a {base_url}")
        else:
            print(f"   ⚠️  Respuesta {response.status_code} de {base_url}")
        
        # Test 2: Servidor ArcGIS
        arcgis_url = "https://geoportal.cali.gov.co/agserver/rest/services"
        response = requests.get(f"{arcgis_url}?f=json", timeout=10)
        
        if response.status_code == 200:
            print(f"   ✅ Servidor ArcGIS accesible")
            data = response.json()
            if 'folders' in data or 'services' in data:
                print(f"   ✅ API REST respondiendo correctamente")
            else:
                print(f"   ⚠️  Respuesta inesperada del API")
        else:
            print(f"   ❌ No se puede acceder al servidor ArcGIS")
            return False
        
        # Test 3: Servicio específico
        service_url = "https://geoportal.cali.gov.co/agserver/rest/services/Hosted/survey123_9f77b14314db40cca29f48bbe746263d_form/FeatureServer"
        response = requests.get(f"{service_url}?f=json", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'error' not in data:
                print(f"   ✅ Servicio de ejemplo accesible")
            else:
                print(f"   ⚠️  Servicio retorna error: {data.get('error')}")
        else:
            print(f"   ⚠️  Servicio de ejemplo no accesible")
        
        print("\n✅ Conectividad verificada")
        return True
        
    except Exception as e:
        print(f"\n❌ Error de conectividad: {e}")
        return False


def test_basic_extraction():
    """Realiza una extracción de prueba simple"""
    print("\n🧪 Realizando extracción de prueba...")
    
    try:
        from scraper_dashboard_cali import ArcGISDashboardScraper
        
        dashboard_url = "https://geoportal.cali.gov.co/arcgis/apps/opsdashboard/index.html#/fb87e184c255488fb4d10183f816d0a6"
        scraper = ArcGISDashboardScraper(dashboard_url)
        
        # Test: Extraer metadata de un servicio
        service_url = f"{scraper.arcgis_server}/rest/services/Hosted/survey123_9f77b14314db40cca29f48bbe746263d_form/FeatureServer"
        
        print(f"   📊 Consultando servicio de prueba...")
        metadata = scraper.get_service_metadata(service_url)
        
        if metadata and 'layers' in metadata:
            print(f"   ✅ Metadata obtenida correctamente")
            print(f"   📋 Capas encontradas: {len(metadata.get('layers', []))}")
            
            # Test: Extraer datos de primera capa
            if metadata.get('layers'):
                layer_id = metadata['layers'][0]['id']
                layer_url = f"{service_url}/{layer_id}"
                
                print(f"   📥 Extrayendo datos de capa {layer_id}...")
                layer_data = scraper.get_layer_data(layer_url)
                
                if layer_data and 'features' in layer_data:
                    feature_count = len(layer_data['features'])
                    print(f"   ✅ Datos extraídos: {feature_count} registros")
                    
                    if feature_count > 0:
                        print(f"   ✅ Estructura de datos verificada")
                        return True
                else:
                    print(f"   ⚠️  No se encontraron features")
                    return False
        else:
            print(f"   ❌ Error obteniendo metadata")
            return False
        
    except ImportError as e:
        print(f"   ❌ Error importando scraper: {e}")
        print(f"   💡 Asegúrate de que scraper_dashboard_cali.py esté en el directorio")
        return False
    except Exception as e:
        print(f"   ❌ Error en extracción: {e}")
        return False


def test_file_creation():
    """Verifica que se puedan crear archivos"""
    print("\n📁 Verificando permisos de escritura...")
    
    try:
        import json
        from datetime import datetime
        
        # Crear archivo de prueba
        test_file = f"test_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        test_data = {
            'test': True,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(test_data, f)
        
        print(f"   ✅ Archivo de prueba creado: {test_file}")
        
        # Eliminar archivo de prueba
        import os
        os.remove(test_file)
        print(f"   ✅ Archivo de prueba eliminado")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error creando archivos: {e}")
        return False


def run_all_tests():
    """Ejecuta todos los tests"""
    print("="*70)
    print("  PRUEBA DE CONFIGURACIÓN - Scraper Dashboard Cali")
    print("="*70)
    
    results = {
        'imports': test_imports(),
        'connectivity': test_connectivity(),
        'extraction': test_basic_extraction(),
        'file_creation': test_file_creation()
    }
    
    print("\n" + "="*70)
    print("  RESUMEN DE PRUEBAS")
    print("="*70)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status} - {test_name.replace('_', ' ').title()}")
    
    all_passed = all(results.values())
    
    if all_passed:
        print("\n🎉 ¡Todas las pruebas pasaron! El scraper está listo para usar.")
        print("\n💡 Siguiente paso:")
        print("   python scraper_dashboard_cali.py")
    else:
        print("\n⚠️  Algunas pruebas fallaron. Revisa los errores arriba.")
        
        failed_tests = [name for name, result in results.items() if not result]
        
        if 'imports' in failed_tests:
            print("\n💡 Para solucionar problemas de dependencias:")
            print("   pip install -r requirements_scraper.txt")
        
        if 'connectivity' in failed_tests:
            print("\n💡 Para solucionar problemas de conectividad:")
            print("   - Verifica tu conexión a internet")
            print("   - Verifica que el firewall permita conexiones")
            print("   - Intenta acceder al dashboard en el navegador")
    
    return all_passed


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
