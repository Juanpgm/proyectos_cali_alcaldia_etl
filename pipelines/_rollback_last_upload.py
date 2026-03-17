"""Elimina los registros subidos en el último batch (UNP-2231 a UNP-3438)
de unidades_proyecto e intervenciones_unidades_proyecto.

Uso: python pipelines/_rollback_last_upload.py
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.config import get_firestore_client

COLLECTION_UP = "unidades_proyecto"
COLLECTION_INT = "intervenciones_unidades_proyecto"

# Rango de UPIDs a eliminar
UPID_START = 2231
UPID_END = 3438


def main():
    db = get_firestore_client()

    upids_to_delete = {f"UNP-{i}" for i in range(UPID_START, UPID_END + 1)}
    print(f"Eliminando {len(upids_to_delete)} UPIDs: UNP-{UPID_START} a UNP-{UPID_END}")

    # 1. Eliminar de unidades_proyecto (directamente por ID, sin verificar existencia)
    print(f"\n[1/2] Eliminando de '{COLLECTION_UP}'...")
    up_deleted = 0
    batch = db.batch()
    batch_count = 0

    for upid in sorted(upids_to_delete):
        doc_ref = db.collection(COLLECTION_UP).document(upid)
        batch.delete(doc_ref)
        up_deleted += 1
        batch_count += 1

        if batch_count >= 500:
            batch.commit()
            print(f"  ↑ {up_deleted} eliminados...")
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()
    print(f"  ✓ {up_deleted} documentos eliminados de '{COLLECTION_UP}'")

    # 2. Eliminar de intervenciones_unidades_proyecto
    print(f"\n[2/2] Eliminando de '{COLLECTION_INT}'...")
    int_deleted = 0
    batch = db.batch()
    batch_count = 0

    # Escanear la colección y eliminar los que tengan upid en el rango
    for doc in db.collection(COLLECTION_INT).stream():
        data = doc.to_dict() or {}
        if data.get("upid") in upids_to_delete:
            batch.delete(doc.reference)
            int_deleted += 1
            batch_count += 1

            if batch_count >= 500:
                batch.commit()
                print(f"  ↑ {int_deleted} eliminados...")
                batch = db.batch()
                batch_count = 0

    if batch_count > 0:
        batch.commit()
    print(f"  ✓ {int_deleted} documentos eliminados de '{COLLECTION_INT}'")

    print(f"\n{'='*50}")
    print(f"ROLLBACK COMPLETADO")
    print(f"  UP eliminados: {up_deleted}")
    print(f"  INT eliminados: {int_deleted}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
