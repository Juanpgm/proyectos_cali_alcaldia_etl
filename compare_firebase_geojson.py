"""
Script para comparar datos en Firebase vs GeoJSON
"""
import json
from database.config import get_firestore_client

def compare_firebase_geojson():
    """Comparar geometrías entre Firebase y GeoJSON"""
    
    # Cargar GeoJSON
    print("📂 Cargando GeoJSON...")
    with open('app_outputs/unidades_proyecto_transformed.geojson', 'r', encoding='utf-8') as f:
        geojson = json.load(f)
    
    # Crear diccionario upid -> geometry
    geojson_geometries = {}
    for feature in geojson['features']:
        upid = feature['properties'].get('upid')
        geometry = feature.get('geometry')
        if upid:
            geojson_geometries[upid] = geometry
    
    print(f"✓ GeoJSON cargado: {len(geojson_geometries)} features")
    
    # Conectar a Firebase
    print("\n🔗 Conectando a Firebase...")
    db = get_firestore_client()
    
    # Obtener documentos sin geometría
    print("📊 Obteniendo documentos sin geometría de Firebase...")
    docs = db.collection('unidades_proyecto').where('has_geometry', '==', False).limit(10).stream()
    
    print("\n" + "="*80)
    print("COMPARACIÓN: Firebase vs GeoJSON")
    print("="*80)
    
    count = 0
    for doc in docs:
        count += 1
        doc_dict = doc.to_dict()
        upid = doc_dict.get('upid')
        
        print(f"\n📄 Documento {count}: {upid}")
        print(f"   Firebase geometry: {doc_dict.get('geometry')}")
        print(f"   Firebase has_geometry: {doc_dict.get('has_geometry')}")
        
        if upid in geojson_geometries:
            geojson_geom = geojson_geometries[upid]
            print(f"   GeoJSON geometry type: {geojson_geom.get('type')}")
            print(f"   GeoJSON coordinates: {geojson_geom.get('coordinates')}")
            
            if geojson_geom.get('coordinates'):
                print(f"   ⚠️  DISCREPANCIA: Firebase tiene None, GeoJSON tiene coordenadas válidas!")
            else:
                print(f"   ✓ Consistente: Ambos sin coordenadas")
        else:
            print(f"   ✗ UPID no encontrado en GeoJSON")
    
    if count == 0:
        print("\n✅ No se encontraron documentos sin geometría en Firebase")

if __name__ == "__main__":
    compare_firebase_geojson()
