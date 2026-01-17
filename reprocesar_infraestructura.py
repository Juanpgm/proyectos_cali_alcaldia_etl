"""
Reprocesar datos de infraestructura para corregir serialización de geometrías LineString
"""
import sys
import json

# Agregar paths necesarios
sys.path.insert(0, 'load_app')
sys.path.insert(0, 'database')

from data_loading_unidades_proyecto_infraestructura import upload_to_firebase

# Ruta al GeoJSON preparado
GEOJSON_FILE = r"A:\programing_workspace\proyectos_cali_alcaldia_etl\app_outputs\unidades_proyecto_infraestructura_outputs\unidades_proyecto_infraestructura_2024_2025_prepared.geojson"
COLLECTION_NAME = "unidades_proyecto"
BATCH_SIZE = 100

print("=" * 80)
print("REPROCESANDO DATOS DE INFRAESTRUCTURA VIAL")
print("=" * 80)
print(f"\n📂 Archivo: {GEOJSON_FILE}")
print(f"📦 Colección: {COLLECTION_NAME}")
print(f"🔢 Batch size: {BATCH_SIZE}\n")

try:
    # Cargar GeoJSON
    print("📖 Cargando GeoJSON...")
    with open(GEOJSON_FILE, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    print(f"✅ Cargado: {len(geojson_data.get('features', []))} features")
    
    # Subir a Firebase
    print("\n📤 Subiendo a Firebase...")
    result = upload_to_firebase(
        geojson_data=geojson_data,
        collection_name=COLLECTION_NAME,
        batch_size=BATCH_SIZE
    )
    
    print("\n" + "=" * 80)
    print("✅ REPROCESAMIENTO COMPLETADO")
    print("=" * 80)
    print(f"\nResultado: {result}")
    
except Exception as e:
    print("\n" + "=" * 80)
    print("❌ ERROR EN REPROCESAMIENTO")
    print("=" * 80)
    print(f"\n{type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
