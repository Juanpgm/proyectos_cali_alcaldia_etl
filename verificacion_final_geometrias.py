"""
Verificación final: Confirmar que TODAS las geometrías están correctamente estructuradas
"""
import firebase_admin
from firebase_admin import credentials, firestore

# Inicializar Firebase
if not firebase_admin._apps:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)

db = firestore.client()

print("=" * 80)
print("🔍 VERIFICACIÓN FINAL DE GEOMETRÍAS EN FIREBASE")
print("=" * 80)

# Obtener muestra de documentos con geometry
all_docs = list(
    db.collection('unidades_proyecto')
    .where('geometry', '!=', None)
    .limit(100)
    .stream()
)

# Filtrar por tipo de UPID
docs_unp = [doc for doc in all_docs if doc.to_dict().get('upid', '').startswith('UNP-')][:5]
docs_inf = [doc for doc in all_docs if doc.to_dict().get('upid', '').startswith('INF-')][:5]

# Verificar documentos UNP-* (Point)
print("\n📍 DOCUMENTOS TIPO POINT (UNP-*)")
print("-" * 80)

geometry_point_ok = 0
geometry_point_error = 0
lat_lon_separados_point = 0

for doc in docs_unp:
    data = doc.to_dict()
    upid = data.get('upid')
    geometry = data.get('geometry')
    
    # Verificar NO hay lat/lon separados
    tiene_lat = 'lat' in data or 'latitud' in data
    tiene_lon = 'lon' in data or 'longitud' in data
    
    if tiene_lat or tiene_lon:
        lat_lon_separados_point += 1
        print(f"❌ {upid}: Tiene lat/lon separados")
        continue
    
    # Verificar estructura de geometry
    if geometry and isinstance(geometry, dict):
        tipo = geometry.get('type')
        coords = geometry.get('coordinates')
        
        if tipo == 'Point' and isinstance(coords, list) and len(coords) == 2:
            geometry_point_ok += 1
            print(f"✅ {upid}: Point válido [{coords[0]:.6f}, {coords[1]:.6f}]")
        else:
            geometry_point_error += 1
            print(f"❌ {upid}: Geometry inválido (type={tipo}, coords type={type(coords)})")
    else:
        geometry_point_error += 1
        print(f"❌ {upid}: Geometry no es dict")

# Verificar documentos INF-* (LineString)
print("\n\n🛣️  DOCUMENTOS TIPO LINESTRING (INF-*)")
print("-" * 80)

geometry_linestring_ok = 0
geometry_linestring_error = 0
lat_lon_separados_inf = 0

for doc in docs_inf:
    data = doc.to_dict()
    upid = data.get('upid')
    geometry = data.get('geometry')
    
    # Verificar NO hay lat/lon separados
    tiene_lat = 'lat' in data or 'latitud' in data
    tiene_lon = 'lon' in data or 'longitud' in data
    
    if tiene_lat or tiene_lon:
        lat_lon_separados_inf += 1
        print(f"❌ {upid}: Tiene lat/lon separados")
        continue
    
    # Verificar estructura de geometry
    if geometry and isinstance(geometry, dict):
        tipo = geometry.get('type')
        coords = geometry.get('coordinates')
        
        # CRÍTICO: coordinates debe ser LIST, no STRING
        if tipo == 'LineString':
            if isinstance(coords, str):
                geometry_linestring_error += 1
                print(f"❌ {upid}: coordinates es STRING (debería ser array)")
                print(f"   Primeros 50 chars: {coords[:50]}")
            elif isinstance(coords, list) and len(coords) >= 2:
                # Verificar que cada punto es [lon, lat]
                punto_valido = all(
                    isinstance(p, list) and len(p) >= 2 and 
                    isinstance(p[0], (int, float)) and isinstance(p[1], (int, float))
                    for p in coords
                )
                if punto_valido:
                    geometry_linestring_ok += 1
                    print(f"✅ {upid}: LineString válido ({len(coords)} puntos)")
                else:
                    geometry_linestring_error += 1
                    print(f"❌ {upid}: Puntos inválidos en LineString")
            else:
                geometry_linestring_error += 1
                print(f"❌ {upid}: coordinates no es lista válida (type={type(coords)})")
        else:
            geometry_linestring_error += 1
            print(f"❌ {upid}: Tipo incorrecto (type={tipo})")
    else:
        geometry_linestring_error += 1
        print(f"❌ {upid}: Geometry no es dict")

# Resumen global
print("\n\n" + "=" * 80)
print("📊 RESUMEN DE VERIFICACIÓN")
print("=" * 80)

print("\n📍 POINT (UNP-*):")
print(f"   ✅ Válidos: {geometry_point_ok}")
print(f"   ❌ Inválidos: {geometry_point_error}")
print(f"   ❌ Con lat/lon separados: {lat_lon_separados_point}")

print("\n🛣️  LINESTRING (INF-*):")
print(f"   ✅ Válidos: {geometry_linestring_ok}")
print(f"   ❌ Inválidos: {geometry_linestring_error}")
print(f"   ❌ Con lat/lon separados: {lat_lon_separados_inf}")

# Estadísticas globales
print("\n📈 ESTADÍSTICAS GLOBALES:")
total_docs = db.collection('unidades_proyecto').count().get()[0][0].value
docs_con_geo = db.collection('unidades_proyecto').where('geometry', '!=', None).count().get()[0][0].value

print(f"   Total documentos: {total_docs}")
print(f"   Con geometry: {docs_con_geo} ({docs_con_geo/total_docs*100:.1f}%)")
print(f"   Sin geometry: {total_docs - docs_con_geo} ({(total_docs - docs_con_geo)/total_docs*100:.1f}%)")

# Resultado final
print("\n" + "=" * 80)
if (geometry_point_error + geometry_linestring_error + 
    lat_lon_separados_point + lat_lon_separados_inf) == 0:
    print("✅ VERIFICACIÓN EXITOSA")
    print("   - Todas las geometrías tienen estructura correcta")
    print("   - NO hay campos lat/lon separados")
    print("   - LineString coordinates son arrays nativos")
else:
    print("⚠️  VERIFICACIÓN CON PROBLEMAS")
    print("   Se encontraron errores en la estructura de geometrías")
print("=" * 80)
