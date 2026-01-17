"""
Inspeccionar estructura detallada de geometrías LineString en Firebase
"""
import firebase_admin
from firebase_admin import credentials, firestore
import json

# Inicializar Firebase
if not firebase_admin._apps:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)

db = firestore.client()

print("🔍 Inspeccionando estructura de LineString...\n")

# Obtener UN documento con LineString
docs = list(
    db.collection('unidades_proyecto')
    .where('geometry', '!=', None)
    .limit(3)
    .stream()
)

for i, doc in enumerate(docs, 1):
    data = doc.to_dict()
    upid = data.get('upid')
    geometry = data.get('geometry')
    
    print(f"\n{'='*80}")
    print(f"DOCUMENTO {i}: {upid}")
    print(f"{'='*80}")
    
    print(f"\nTipo de geometry: {type(geometry)}")
    print(f"Claves en geometry: {list(geometry.keys()) if isinstance(geometry, dict) else 'N/A'}")
    
    if isinstance(geometry, dict):
        tipo = geometry.get('type')
        coords = geometry.get('coordinates')
        
        print(f"\ntype: {tipo}")
        print(f"coordinates type: {type(coords)}")
        print(f"coordinates length: {len(coords) if hasattr(coords, '__len__') else 'N/A'}")
        
        if coords:
            print(f"\nPrimeros 3 puntos:")
            if isinstance(coords, list):
                for j, punto in enumerate(coords[:3], 1):
                    print(f"  Punto {j}: {punto} (type: {type(punto)})")
            
            # Mostrar estructura completa de coordenadas
            print(f"\nEstructura completa de coordinates:")
            print(json.dumps(coords, indent=2, default=str))
