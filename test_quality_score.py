# Test del nuevo cálculo de quality score
import sys
import json
import tempfile
sys.path.insert(0, 'a:/programing_workspace/proyectos_cali_alcaldia_etl')
from utils.quality_control import validate_geojson

# Cargar datos de Firebase para probar
from database.config import get_firestore_client

print("Conectando a Firebase...")
db = get_firestore_client()
collection = db.collection('unidades_proyecto')
docs = collection.stream()

print("Cargando registros...")
features = []
for doc in docs:
    data = doc.to_dict()
    features.append({
        'type': 'Feature',
        'properties': data,
        'geometry': data.get('geometry')
    })

print(f"Registros cargados: {len(features)}")

# Crear GeoJSON temporal
geojson_data = {
    'type': 'FeatureCollection',
    'features': features
}

with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False, encoding='utf-8') as tmp:
    json.dump(geojson_data, tmp, ensure_ascii=False)
    tmp_path = tmp.name

# Ejecutar validación
print("\nEjecutando validaciones...")
result = validate_geojson(tmp_path, verbose=False)

stats = result.get('statistics', {})
print("\n" + "="*60)
print("RESULTADO DEL CONTROL DE CALIDAD")
print("="*60)
print(f"Quality Score: {stats.get('quality_score', 0)}%")
print(f"Quality Rating: {stats.get('quality_rating', 'N/A')}")
print(f"Records Affected: {stats.get('records_affected', 0)}")
print(f"Records Affected %: {stats.get('records_affected_percentage', 0):.2f}%")
print(f"Total Issues: {result.get('total_issues', 0)}")
print("\nPor severidad:")
for sev, count in stats.get('by_severity', {}).items():
    print(f"  {sev}: {count}")
