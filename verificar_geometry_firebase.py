"""
Verificar que los documentos en Firebase NO tengan lat/lon como campos separados
y que las coordenadas estén SOLO en el campo geometry
"""
import firebase_admin
from firebase_admin import credentials, firestore
import random

# Inicializar Firebase
if not firebase_admin._apps:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)

db = firestore.client()

# Obtener muestra de documentos
print("🔍 Verificando estructura de documentos en Firebase...\n")
print("=" * 80)

# Obtener 10 documentos aleatorios con geometry
docs_con_geometry = list(
    db.collection('unidades_proyecto')
    .where('geometry', '!=', None)
    .limit(5)
    .stream()
)

# Obtener 5 documentos sin geometry
docs_sin_geometry = list(
    db.collection('unidades_proyecto')
    .where('geometry', '==', None)
    .limit(5)
    .stream()
)

print(f"📊 Analizando {len(docs_con_geometry)} documentos CON geometry")
print(f"📊 Analizando {len(docs_sin_geometry)} documentos SIN geometry")
print("=" * 80)

# Verificar documentos CON geometry
print("\n✅ DOCUMENTOS CON GEOMETRY:")
print("-" * 80)
for doc in docs_con_geometry:
    data = doc.to_dict()
    upid = data.get('upid', 'N/A')
    nombre = data.get('nombre_up', 'N/A')[:40]
    
    # Verificar que NO existan campos lat/lon separados
    tiene_lat_separado = 'lat' in data
    tiene_lon_separado = 'lon' in data
    tiene_latitud_separado = 'latitud' in data
    tiene_longitud_separado = 'longitud' in data
    
    # Verificar geometry
    geometry = data.get('geometry')
    tiene_geometry = geometry is not None
    geometry_valido = False
    
    if tiene_geometry:
        geometry_valido = (
            isinstance(geometry, dict) and
            geometry.get('type') == 'Point' and
            'coordinates' in geometry and
            isinstance(geometry['coordinates'], list) and
            len(geometry['coordinates']) == 2
        )
    
    print(f"\n📍 {upid} - {nombre}")
    print(f"   Geometry: {'✅ VÁLIDO' if geometry_valido else '❌ INVÁLIDO'}")
    if geometry_valido:
        coords = geometry['coordinates']
        print(f"   Coordenadas: [{coords[0]:.6f}, {coords[1]:.6f}]")
    
    # CRÍTICO: Verificar que NO haya campos lat/lon separados
    tiene_campos_separados = (
        tiene_lat_separado or tiene_lon_separado or
        tiene_latitud_separado or tiene_longitud_separado
    )
    
    if tiene_campos_separados:
        print(f"   ❌ ERROR: Tiene campos de coordenadas separados:")
        if tiene_lat_separado:
            print(f"      - lat: {data.get('lat')}")
        if tiene_lon_separado:
            print(f"      - lon: {data.get('lon')}")
        if tiene_latitud_separado:
            print(f"      - latitud: {data.get('latitud')}")
        if tiene_longitud_separado:
            print(f"      - longitud: {data.get('longitud')}")
    else:
        print(f"   ✅ OK: NO tiene campos lat/lon separados")

# Verificar documentos SIN geometry
print("\n\n❌ DOCUMENTOS SIN GEOMETRY:")
print("-" * 80)
for doc in docs_sin_geometry:
    data = doc.to_dict()
    upid = data.get('upid', 'N/A')
    nombre = data.get('nombre_up', 'N/A')[:40]
    
    # Verificar que NO existan campos lat/lon separados
    tiene_lat_separado = 'lat' in data
    tiene_lon_separado = 'lon' in data
    tiene_latitud_separado = 'latitud' in data
    tiene_longitud_separado = 'longitud' in data
    
    print(f"\n📍 {upid} - {nombre}")
    print(f"   Geometry: ❌ NULL")
    
    # CRÍTICO: Verificar que NO haya campos lat/lon separados
    tiene_campos_separados = (
        tiene_lat_separado or tiene_lon_separado or
        tiene_latitud_separado or tiene_longitud_separado
    )
    
    if tiene_campos_separados:
        print(f"   ❌ ERROR: Tiene campos de coordenadas separados:")
        if tiene_lat_separado:
            print(f"      - lat: {data.get('lat')}")
        if tiene_lon_separado:
            print(f"      - lon: {data.get('lon')}")
        if tiene_latitud_separado:
            print(f"      - latitud: {data.get('latitud')}")
        if tiene_longitud_separado:
            print(f"      - longitud: {data.get('longitud')}")
    else:
        print(f"   ✅ OK: NO tiene campos lat/lon separados")

# Resumen
print("\n\n" + "=" * 80)
print("📊 RESUMEN DE VERIFICACIÓN")
print("=" * 80)

# Contar totales
total_docs = db.collection('unidades_proyecto').count().get()[0][0].value
docs_con_geo_count = db.collection('unidades_proyecto').where('geometry', '!=', None).count().get()[0][0].value
docs_sin_geo_count = total_docs - docs_con_geo_count

print(f"\n📈 Total de documentos: {total_docs}")
print(f"   ✅ Con geometry: {docs_con_geo_count} ({docs_con_geo_count/total_docs*100:.1f}%)")
print(f"   ❌ Sin geometry: {docs_sin_geo_count} ({docs_sin_geo_count/total_docs*100:.1f}%)")

print("\n✅ VERIFICACIÓN COMPLETADA")
print("   Las coordenadas deben estar SOLO en el campo 'geometry'")
print("   NO debe haber campos 'lat', 'lon', 'latitud' o 'longitud' separados")
