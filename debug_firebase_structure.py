# -*- coding: utf-8 -*-
"""Script para debuggear la estructura de Firebase."""

from database.config import get_firestore_client
import json

db = get_firestore_client()
collection_ref = db.collection("unidades_proyecto")

# Obtener un documento de ejemplo
docs = list(collection_ref.limit(1).stream())

if docs:
    doc = docs[0]
    data = doc.to_dict()
    
    print("=" * 80)
    print(f"Documento ID: {doc.id}")
    print("=" * 80)
    print("\nEstructura completa:")
    print(json.dumps(data, indent=2, default=str, ensure_ascii=False)[:3000])
    
    print("\n" + "=" * 80)
    print("Claves principales:")
    print(list(data.keys()))
    
    if 'properties' in data:
        print("\nClaves en properties:")
        print(list(data['properties'].keys())[:30])
        
        if 'intervenciones' in data['properties']:
            intervenciones = data['properties']['intervenciones']
            print(f"\n¿Tiene array de intervenciones? {isinstance(intervenciones, list)}")
            if isinstance(intervenciones, list):
                print(f"Número de intervenciones: {len(intervenciones)}")
                if len(intervenciones) > 0:
                    print("\nPrimera intervención:")
                    print(json.dumps(intervenciones[0], indent=2, default=str, ensure_ascii=False))
