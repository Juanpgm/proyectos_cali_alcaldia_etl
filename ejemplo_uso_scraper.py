"""
Ejemplo de uso simple del scraper de dashboard de Cali
Ejecuta este script para una extracción rápida
"""

from scraper_dashboard_cali import ArcGISDashboardScraper
import json

def ejemplo_basico():
    """Ejemplo más simple - extracción completa"""
    print("=" * 70)
    print("  EJEMPLO BÁSICO - Extracción completa del Dashboard")
    print("=" * 70)
    
    # URL del dashboard
    dashboard_url = "https://geoportal.cali.gov.co/arcgis/apps/opsdashboard/index.html#/fb87e184c255488fb4d10183f816d0a6"
    
    # Crear scraper
    scraper = ArcGISDashboardScraper(dashboard_url)
    
    # Extraer todos los datos
    data = scraper.scrape_all_data()
    
    # Guardar en JSON
    json_file = scraper.save_to_json(data)
    
    # Exportar a Excel
    excel_file = scraper.export_to_excel(data)
    
    print("\n✅ Extracción completada exitosamente!")
    print(f"📁 Archivos generados:")
    print(f"   - {json_file}")
    print(f"   - {excel_file}")


def ejemplo_servicio_especifico():
    """Ejemplo: Extraer solo un servicio específico"""
    print("\n" + "=" * 70)
    print("  EJEMPLO 2 - Extracción de servicio específico")
    print("=" * 70)
    
    dashboard_url = "https://geoportal.cali.gov.co/arcgis/apps/opsdashboard/index.html#/fb87e184c255488fb4d10183f816d0a6"
    scraper = ArcGISDashboardScraper(dashboard_url)
    
    # Servicio de Presupuesto Participativo
    service_url = f"{scraper.arcgis_server}/rest/services/Hosted/survey123_9f77b14314db40cca29f48bbe746263d_form/FeatureServer"
    
    print(f"\n📊 Extrayendo: Presupuesto Participativo 2025")
    
    # Extraer datos de este servicio
    service_data = scraper.extract_all_layers_from_service(service_url)
    
    if service_data:
        # Guardar solo este servicio
        output_data = {
            'extraction_date': service_data.get('extraction_date'),
            'service': service_data
        }
        
        with open('presupuesto_participativo.json', 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print("\n✅ Datos guardados en: presupuesto_participativo.json")


def ejemplo_capa_especifica():
    """Ejemplo: Extraer solo una capa específica"""
    print("\n" + "=" * 70)
    print("  EJEMPLO 3 - Extracción de capa específica")
    print("=" * 70)
    
    dashboard_url = "https://geoportal.cali.gov.co/arcgis/apps/opsdashboard/index.html#/fb87e184c255488fb4d10183f816d0a6"
    scraper = ArcGISDashboardScraper(dashboard_url)
    
    # Capa 0 del servicio de survey123
    layer_url = f"{scraper.arcgis_server}/rest/services/Hosted/survey123_9f77b14314db40cca29f48bbe746263d_form/FeatureServer/0"
    
    print(f"\n📋 Extrayendo capa específica...")
    
    # Extraer datos de esta capa
    layer_data = scraper.get_layer_data(layer_url)
    
    if layer_data and 'features' in layer_data:
        features = layer_data['features']
        print(f"✅ {len(features)} registros extraídos")
        
        # Mostrar primer registro como ejemplo
        if features:
            print("\n📝 Ejemplo de registro:")
            print(json.dumps(features[0], indent=2, ensure_ascii=False)[:500] + "...")
        
        # Guardar
        with open('capa_especifica.json', 'w', encoding='utf-8') as f:
            json.dump(layer_data, f, ensure_ascii=False, indent=2)
        
        print("\n✅ Datos guardados en: capa_especifica.json")


def ejemplo_filtrado():
    """Ejemplo: Extraer datos filtrados"""
    print("\n" + "=" * 70)
    print("  EJEMPLO 4 - Extracción con filtros")
    print("=" * 70)
    
    dashboard_url = "https://geoportal.cali.gov.co/arcgis/apps/opsdashboard/index.html#/fb87e184c255488fb4d10183f816d0a6"
    scraper = ArcGISDashboardScraper(dashboard_url)
    
    layer_url = f"{scraper.arcgis_server}/rest/services/Hosted/survey123_9f77b14314db40cca29f48bbe746263d_form/FeatureServer/0"
    
    # Ejemplo: Filtrar por lote
    # Nota: Ajusta el nombre del campo según la estructura real
    print(f"\n🔍 Extrayendo registros del Lote 1...")
    
    # Intentar diferentes campos comunes
    filtros_posibles = [
        "lote = 'Lote 1'",
        "Lote = 'Lote 1'",
        "LOTE = 'Lote 1'",
        "lote = 1",
    ]
    
    for filtro in filtros_posibles:
        print(f"\n   Probando filtro: {filtro}")
        layer_data = scraper.get_layer_data(
            layer_url,
            where=filtro,
            out_fields="*"
        )
        
        if layer_data and 'features' in layer_data:
            features = layer_data['features']
            if features:
                print(f"   ✅ {len(features)} registros encontrados con este filtro")
                
                with open('lote_1_filtrado.json', 'w', encoding='utf-8') as f:
                    json.dump(layer_data, f, ensure_ascii=False, indent=2)
                
                print("   💾 Guardado en: lote_1_filtrado.json")
                break
        else:
            print(f"   ⚠️ Sin resultados con este filtro")


def ejemplo_analisis_rapido():
    """Ejemplo: Análisis rápido de la estructura"""
    print("\n" + "=" * 70)
    print("  EJEMPLO 5 - Análisis de estructura")
    print("=" * 70)
    
    dashboard_url = "https://geoportal.cali.gov.co/arcgis/apps/opsdashboard/index.html#/fb87e184c255488fb4d10183f816d0a6"
    scraper = ArcGISDashboardScraper(dashboard_url)
    
    # Descubrir servicios
    services = scraper.discover_feature_services()
    
    print(f"\n📊 Análisis de servicios descubiertos:\n")
    
    for service_url in services:
        metadata = scraper.get_service_metadata(service_url)
        
        if metadata and 'name' in metadata:
            print(f"🔷 Servicio: {metadata.get('name', 'Sin nombre')}")
            print(f"   URL: {service_url}")
            print(f"   Descripción: {metadata.get('serviceDescription', 'N/A')}")
            
            # Capas
            layers = metadata.get('layers', [])
            tables = metadata.get('tables', [])
            
            print(f"   Capas: {len(layers)}")
            for layer in layers:
                print(f"      - [{layer['id']}] {layer['name']}")
            
            if tables:
                print(f"   Tablas: {len(tables)}")
                for table in tables:
                    print(f"      - [{table['id']}] {table['name']}")
            
            print()


def menu():
    """Menú interactivo"""
    print("\n" + "=" * 70)
    print("  SCRAPER DASHBOARD CALI - Menú de Ejemplos")
    print("=" * 70)
    print("\n Selecciona una opción:")
    print("\n  1. Extracción completa (JSON + Excel)")
    print("  2. Extraer servicio específico")
    print("  3. Extraer capa específica")
    print("  4. Extraer con filtros")
    print("  5. Analizar estructura de servicios")
    print("  0. Salir")
    print("\n" + "=" * 70)
    
    opcion = input("\n👉 Ingresa el número de opción: ").strip()
    
    if opcion == "1":
        ejemplo_basico()
    elif opcion == "2":
        ejemplo_servicio_especifico()
    elif opcion == "3":
        ejemplo_capa_especifica()
    elif opcion == "4":
        ejemplo_filtrado()
    elif opcion == "5":
        ejemplo_analisis_rapido()
    elif opcion == "0":
        print("\n👋 ¡Hasta luego!")
        return
    else:
        print("\n❌ Opción inválida")
    
    # Preguntar si quiere ejecutar otro
    continuar = input("\n¿Ejecutar otro ejemplo? (s/n): ").strip().lower()
    if continuar == 's':
        menu()


if __name__ == "__main__":
    # Ejecutar menú interactivo
    menu()
    
    # O descomentar para ejecutar directamente un ejemplo:
    # ejemplo_basico()
    # ejemplo_servicio_especifico()
    # ejemplo_capa_especifica()
    # ejemplo_filtrado()
    # ejemplo_analisis_rapido()
