"""Inspeccionar estructura de documentos existentes en Firebase."""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.config import get_firestore_client

db = get_firestore_client()

# 1. Estructura del primer documento
print("="*60)
print("ESTRUCTURA DE DOCUMENTOS EXISTENTES")
print("="*60)

docs = list(db.collection("unidades_proyecto").limit(5).stream())
for i, doc in enumerate(docs):
    data = doc.to_dict() or {}
    print(f"\n--- Doc {i+1}: {doc.id} ---")
    for k, v in sorted(data.items()):
        val_repr = repr(v)[:100]
        print(f"  {k}: {val_repr}")

# 2. Verificar si tienen nombre_up, direccion, bpin, geometry
print(f"\n\n--- Campos clave para fingerprint (10 muestras) ---")
for doc in db.collection("unidades_proyecto").limit(10).stream():
    data = doc.to_dict() or {}
    print(f"  {doc.id}: nombre_up={data.get('nombre_up', '<MISSING>')!r:.40s} | "
          f"direccion={data.get('direccion', '<MISSING>')!r:.40s} | "
          f"bpin={data.get('bpin', '<MISSING>')!r:.30s} | "
          f"geometry={'YES' if data.get('geometry') else 'NO'}")

# 3. Check intervenciones structure
print(f"\n\n--- Intervenciones (5 muestras) ---")
for doc in db.collection("intervenciones_unidades_proyecto").limit(5).stream():
    data = doc.to_dict() or {}
    print(f"\n--- INT: {doc.id} ---")
    for k, v in sorted(data.items()):
        val_repr = repr(v)[:100]
        print(f"  {k}: {val_repr}")
