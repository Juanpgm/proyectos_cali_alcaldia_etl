# -*- coding: utf-8 -*-
"""
Verificar si lat/lon están en Firebase
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client

db = get_firestore_client()

# Obtener un registro sin geometry
docs = list(db.collection('unidades_proyecto').where('has_geometry', '==', False).limit(5).stream())

print(f"[INFO] Registros sin geometry encontrados: {len(docs)}\n")

for doc in docs:
    data = doc.to_dict()
    print(f"UPID: {doc.id}")
    print(f"  Has 'lat' field: {'lat' in data}")
    print(f"  Has 'lon' field: {'lon' in data}")
    print(f"  lat value: {data.get('lat')}")
    print(f"  lon value: {data.get('lon')}")
    print(f"  nombre_corto: {data.get('nombre_corto', 'N/A')}")
    print(f"  centro_gestor: {data.get('nombre_centro_gestor', 'N/A')}")
    print()
