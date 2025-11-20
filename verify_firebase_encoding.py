# -*- coding: utf-8 -*-
"""Verificar encoding en Firebase"""

from load_app.data_loading_unidades_proyecto import get_firestore_client

db = get_firestore_client()

# Buscar documentos con barrio_vereda_2 que tengan acentos
print("Buscando documentos con barrio_vereda_2 != None...")
docs = db.collection('unidades_proyecto').where('barrio_vereda_2', '!=', None).limit(10).stream()

count = 0
with_accents = []
for doc in docs:
    count += 1
    data = doc.to_dict()
    barrio = data.get('barrio_vereda_2', '')
    if barrio and any(c in barrio for c in 'áéíóúñÁÉÍÓÚÑ'):
        with_accents.append(barrio)
        print(f"UPID: {data.get('upid')}")
        print(f"Barrio vereda 2: {barrio}")
        print(f"Nombre: {data.get('nombre_up')}")
        print()

print(f"\nTotal documentos revisados: {count}")
print(f"Total con acentos en barrio_vereda_2: {len(with_accents)}")
if with_accents:
    print("\nBarrios con acentos encontrados:")
    for b in set(with_accents):
        print(f"  - {b}")
