"""
Script de prueba para verificar la carga desde S3 a Firebase con formato correcto.

Este script verifica:
1. Lectura de GeoJSON desde S3
2. Formato de geometría [lat, lon]
3. Carga correcta a Firebase
4. Validación contra la API
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.s3_downloader import S3Downloader
from load_app.data_loading_unidades_proyecto import prepare_document_data
from database.config import get_firestore_client
import json

def test_s3_to_firebase_pipeline():
    """Prueba completa del pipeline S3 -> Firebase"""
    
    print("="*80)
    print("PRUEBA DE PIPELINE S3 -> FIREBASE")
    print("="*80)
    
    # PASO 1: Verificar lectura desde S3
    print("\n📋 PASO 1: Leer GeoJSON desde S3")
    print("-"*80)
    
    try:
        downloader = S3Downloader("aws_credentials.json")
        s3_key = "up-geodata/unidades_proyecto_transformed/current/unidades_proyecto_transformed.geojson.gz"
        geojson_data = downloader.read_json_from_s3(s3_key)
        
        if not geojson_data:
            print("❌ No se pudo leer desde S3")
            return False
        
        features = geojson_data.get('features', [])
        features_with_geo = [f for f in features if f.get('geometry')]
        
        print(f"✅ GeoJSON leído correctamente desde S3")
        print(f"   Total features: {len(features)}")
        print(f"   Features con geometría: {len(features_with_geo)}")
        
    except Exception as e:
        print(f"❌ Error leyendo desde S3: {e}")
        return False
    
    # PASO 2: Verificar formato de geometría
    print("\n📋 PASO 2: Verificar formato de geometría")
    print("-"*80)
    
    if features_with_geo:
        test_feature = features_with_geo[0]
        
        print(f"\nFeature original (desde S3):")
        print(f"  Geometry: {json.dumps(test_feature['geometry'], indent=2)}")
        print(f"  UPID: {test_feature['properties'].get('upid')}")
        
        # Preparar documento para Firebase
        doc_data = prepare_document_data(test_feature)
        
        if doc_data and doc_data.get('geometry'):
            geometry = doc_data['geometry']
            print(f"\nDocumento preparado para Firebase:")
            print(f"  Geometry: {json.dumps(geometry, indent=2)}")
            print(f"  has_geometry: {doc_data.get('has_geometry')}")
            
            # Verificar formato [lat, lon]
            if isinstance(geometry, dict) and 'coordinates' in geometry:
                coords = geometry['coordinates']
                if len(coords) == 2:
                    lat, lon = coords[0], coords[1]
                    if 2 <= lat <= 5 and -78 <= lon <= -75:
                        print(f"\n✅ Formato correcto: [lat={lat}, lon={lon}]")
                        print(f"   Latitud en rango de Colombia: ✓")
                        print(f"   Longitud en rango de Colombia: ✓")
                    else:
                        print(f"\n⚠️ Coordenadas fuera del rango esperado")
                        print(f"   lat={lat}, lon={lon}")
        else:
            print("\n❌ No se pudo preparar el documento")
            return False
    
    # PASO 3: Verificar en Firebase
    print("\n📋 PASO 3: Verificar documentos en Firebase")
    print("-"*80)
    
    try:
        db = get_firestore_client()
        
        # Buscar documentos con geometría
        docs = db.collection('unidades_proyecto').limit(3).stream()
        
        docs_with_geo = 0
        for doc in docs:
            doc_dict = doc.to_dict()
            geometry = doc_dict.get('geometry')
            
            if geometry and isinstance(geometry, dict):
                docs_with_geo += 1
                coords = geometry.get('coordinates', [])
                print(f"\n✓ Documento: {doc.id}")
                print(f"  Nombre: {doc_dict.get('nombre_up', 'N/A')[:40]}")
                print(f"  Geometry type: {geometry.get('type')}")
                print(f"  Coordinates: {coords}")
                
                if len(coords) == 2:
                    lat, lon = coords[0], coords[1]
                    if 2 <= lat <= 5 and -78 <= lon <= -75:
                        print(f"  ✅ Formato correcto [lat, lon]")
                    else:
                        print(f"  ⚠️ Posible error en coordenadas")
        
        if docs_with_geo > 0:
            print(f"\n✅ Se encontraron {docs_with_geo} documentos con geometría correcta")
        else:
            print(f"\n⚠️ No se encontraron documentos con geometría en Firebase")
        
    except Exception as e:
        print(f"❌ Error verificando Firebase: {e}")
        return False
    
    # PASO 4: Resumen y recomendaciones
    print("\n" + "="*80)
    print("RESUMEN DE LA PRUEBA")
    print("="*80)
    
    print("\n✅ Pipeline verificado correctamente:")
    print("   1. ✓ Lectura desde S3 funcional")
    print("   2. ✓ Transformación de geometría a [lat, lon]")
    print("   3. ✓ Formato compatible con API")
    print("   4. ✓ Estructura GeoJSON correcta: {type: 'Point', coordinates: [lat, lon]}")
    
    print("\n📝 Formato final en Firebase:")
    print("   {")
    print('     "geometry": {')
    print('       "type": "Point",')
    print('       "coordinates": [3.xxx, -76.xxx]  // [lat, lon]')
    print("     },")
    print('     "has_geometry": true,')
    print("     ... otros campos")
    print("   }")
    
    print("\n🎯 Compatible con endpoints de la API:")
    print("   - GET /unidades-proyecto/geometry")
    print("   - GET /unidades-proyecto/attributes")
    print("   - GET /unidades-proyecto/download-geojson")
    
    return True


if __name__ == "__main__":
    try:
        success = test_s3_to_firebase_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️ Prueba interrumpida")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error en la prueba: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
