"""
Verificar tipos de geometry en Firebase y su validez
"""
import firebase_admin
from firebase_admin import credentials, firestore

# Inicializar Firebase
if not firebase_admin._apps:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)

db = firestore.client()

print("🔍 Analizando tipos de geometry en Firebase...\n")
print("=" * 80)

# Obtener todos los documentos con geometry
docs = list(
    db.collection('unidades_proyecto')
    .where('geometry', '!=', None)
    .limit(20)
    .stream()
)

# Contadores por tipo
tipos_geometry = {}
geometry_validos = 0
geometry_invalidos = 0

for doc in docs[:20]:
    data = doc.to_dict()
    upid = data.get('upid', 'N/A')
    geometry = data.get('geometry')
    
    if geometry and isinstance(geometry, dict):
        tipo = geometry.get('type', 'UNKNOWN')
        tipos_geometry[tipo] = tipos_geometry.get(tipo, 0) + 1
        
        # Validar estructura según tipo
        coords = geometry.get('coordinates')
        valido = False
        
        if tipo == 'Point':
            if isinstance(coords, list) and len(coords) == 2:
                valido = True
                geometry_validos += 1
            else:
                geometry_invalidos += 1
        elif tipo == 'LineString':
            if isinstance(coords, list) and len(coords) >= 2 and all(isinstance(p, list) and len(p) >= 2 for p in coords):
                valido = True
                geometry_validos += 1
            else:
                geometry_invalidos += 1
        elif tipo == 'Polygon':
            if isinstance(coords, list) and len(coords) > 0:
                valido = True
                geometry_validos += 1
            else:
                geometry_invalidos += 1
        else:
            geometry_invalidos += 1
        
        # Mostrar detalles
        status = "✅" if valido else "❌"
        print(f"{status} {upid}: {tipo}")
        if tipo == 'Point' and valido:
            print(f"   Coordenadas: [{coords[0]:.6f}, {coords[1]:.6f}]")
        elif tipo == 'LineString' and valido:
            print(f"   Puntos: {len(coords)}")
    else:
        geometry_invalidos += 1
        print(f"❌ {upid}: Geometry inválido")

# Resumen por tipo
print("\n" + "=" * 80)
print("📊 RESUMEN POR TIPO DE GEOMETRY")
print("=" * 80)
for tipo, count in sorted(tipos_geometry.items()):
    print(f"  {tipo}: {count}")

print(f"\n✅ Válidos: {geometry_validos}")
print(f"❌ Inválidos: {geometry_invalidos}")

# Contar totales en toda la colección
print("\n" + "=" * 80)
print("📊 ESTADÍSTICAS GLOBALES")
print("=" * 80)
total_docs = db.collection('unidades_proyecto').count().get()[0][0].value
docs_con_geo = db.collection('unidades_proyecto').where('geometry', '!=', None).count().get()[0][0].value

print(f"Total documentos: {total_docs}")
print(f"Con geometry: {docs_con_geo} ({docs_con_geo/total_docs*100:.1f}%)")
print(f"Sin geometry: {total_docs - docs_con_geo} ({(total_docs - docs_con_geo)/total_docs*100:.1f}%)")

print("\n✅ Análisis completado")
