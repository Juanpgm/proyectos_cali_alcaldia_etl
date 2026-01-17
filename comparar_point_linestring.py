"""
Inspeccionar estructura de geometrías Point vs LineString
"""
import firebase_admin
from firebase_admin import credentials, firestore
import json

# Inicializar Firebase
if not firebase_admin._apps:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)

db = firestore.client()

print("🔍 Comparando Point vs LineString...\n")

# Buscar documentos que empiecen con UNP- (deberían ser Point)
all_docs = list(
    db.collection('unidades_proyecto')
    .where('geometry', '!=', None)
    .limit(50)
    .stream()
)

# Filtrar manualmente por prefijo UNP-
docs_point = [doc for doc in all_docs if doc.to_dict().get('upid', '').startswith('UNP-')][:3]
docs_line = [doc for doc in all_docs if doc.to_dict().get('upid', '').startswith('INF-')][:3]

print("=" * 80)
print("DOCUMENTOS TIPO POINT (UNP-*)")
print("=" * 80)

for doc in docs_point:
    data = doc.to_dict()
    upid = data.get('upid')
    geometry = data.get('geometry')
    
    if geometry:
        tipo = geometry.get('type')
        coords = geometry.get('coordinates')
        
        print(f"\n{upid}")
        print(f"  type: {tipo}")
        print(f"  coordinates type: {type(coords)}")
        print(f"  coordinates: {coords}")
        
        # Verificar si es válido
        if tipo == 'Point' and isinstance(coords, list) and len(coords) == 2:
            print(f"  ✅ VÁLIDO")
        else:
            print(f"  ❌ INVÁLIDO")

# docs_line ya está filtrada arriba

print("\n" + "=" * 80)
print("DOCUMENTOS TIPO LINESTRING (INF-*)")
print("=" * 80)

for doc in docs_line:
    data = doc.to_dict()
    upid = data.get('upid')
    geometry = data.get('geometry')
    
    if geometry:
        tipo = geometry.get('type')
        coords = geometry.get('coordinates')
        
        print(f"\n{upid}")
        print(f"  type: {tipo}")
        print(f"  coordinates type: {type(coords)}")
        
        if isinstance(coords, str):
            print(f"  ❌ PROBLEMA: coordinates es STRING, debería ser lista")
            print(f"  Primeros 100 caracteres: {coords[:100]}")
        elif isinstance(coords, list):
            print(f"  coordinates length: {len(coords)}")
            print(f"  Primer punto: {coords[0] if coords else 'N/A'}")
            print(f"  ✅ VÁLIDO")
