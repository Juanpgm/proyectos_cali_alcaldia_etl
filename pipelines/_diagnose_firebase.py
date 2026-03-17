"""Diagnóstico: verificar qué registros de infraestructura existen actualmente en Firebase.
Revisar nombre_centro_gestor, upid, y estructura."""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.config import get_firestore_client

db = get_firestore_client()

# 1. Contar total de docs en unidades_proyecto
print("="*60)
print("DIAGNÓSTICO DE REGISTROS EXISTENTES EN FIREBASE")
print("="*60)

# 2. Buscar con filtro nombre_centro_gestor == "Secretaría de Infraestructura"
print("\n[1] Query con filtro nombre_centro_gestor == 'Secretaría de Infraestructura':")
query = db.collection("unidades_proyecto").where(
    "nombre_centro_gestor", "==", "Secretaría de Infraestructura"
)
infra_docs = list(query.stream())
print(f"  → {len(infra_docs)} documentos encontrados")

if infra_docs:
    sample = infra_docs[0].to_dict()
    print(f"  Ejemplo doc ID: {infra_docs[0].id}")
    print(f"  Campos: {list(sample.keys())}")
    print(f"  nombre_centro_gestor: {sample.get('nombre_centro_gestor')!r}")
    print(f"  upid: {sample.get('upid')!r}")

# 3. Muestrear docs con distintos nombre_centro_gestor
print(f"\n[2] Muestreo de nombre_centro_gestor existentes:")
gestor_values = {}
total_up = 0
for doc in db.collection("unidades_proyecto").stream():
    data = doc.to_dict() or {}
    total_up += 1
    ncg = data.get("nombre_centro_gestor", "<NO FIELD>")
    if ncg not in gestor_values:
        gestor_values[ncg] = {"count": 0, "sample_id": doc.id, "sample_upid": data.get("upid")}
    gestor_values[ncg]["count"] += 1

print(f"  → Total docs en 'unidades_proyecto': {total_up}")
for val, info in sorted(gestor_values.items(), key=lambda x: -x[1]["count"]):
    print(f"    '{val}': {info['count']} docs  (ej: {info['sample_id']}, upid={info['sample_upid']})")

# 4. Verificar intervenciones
print(f"\n[3] Muestreo de 'intervenciones_unidades_proyecto':")
int_count = 0
int_upids = set()
for doc in db.collection("intervenciones_unidades_proyecto").stream():
    data = doc.to_dict() or {}
    int_count += 1
    int_upids.add(data.get("upid"))

print(f"  → Total docs: {int_count}")
print(f"  → UPIDs únicos: {len(int_upids)}")
if int_upids:
    sorted_upids = sorted(int_upids, key=lambda x: int(x.split("-")[-1]) if x and "-" in x else 0)
    print(f"  → Rango: {sorted_upids[0]} a {sorted_upids[-1]}")

# 5. Buscar UPIDs del rango del notebook (UNP-1 a UNP-2230)
print(f"\n[4] UPIDs del notebook (UNP-1 a UNP-2230) en UP:")
notebook_count = 0
notebook_infra = 0
for doc in db.collection("unidades_proyecto").stream():
    data = doc.to_dict() or {}
    upid = data.get("upid", doc.id)
    if upid and "UNP-" in str(upid):
        try:
            num = int(str(upid).split("-")[-1])
            if 1 <= num <= 2230:
                notebook_count += 1
                if data.get("nombre_centro_gestor") == "Secretaría de Infraestructura":
                    notebook_infra += 1
        except ValueError:
            pass

print(f"  → {notebook_count} docs con UPID en rango UNP-1..UNP-2230")
print(f"  → {notebook_infra} de ellos con nombre_centro_gestor == 'Secretaría de Infraestructura'")
