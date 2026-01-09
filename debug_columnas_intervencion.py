# -*- coding: utf-8 -*-
"""Script para verificar columnas de intervención."""

import pandas as pd
from database.config import get_firestore_client

db = get_firestore_client()
collection_ref = db.collection("unidades_proyecto")

# Obtener algunos documentos con intervenciones
docs = list(collection_ref.limit(5).stream())

print("=" * 80)
print("VERIFICACIÓN DE COLUMNAS DE INTERVENCIÓN")
print("=" * 80)

intervenciones_list = []

for doc in docs:
    data = doc.to_dict()
    intervenciones = data.get('intervenciones', [])
    
    print(f"\nDocumento: {doc.id}")
    print(f"  UPID: {data.get('upid')}")
    print(f"  Número de intervenciones: {len(intervenciones)}")
    
    for i, interv in enumerate(intervenciones):
        print(f"\n  Intervención {i+1}:")
        print(f"    presupuesto_base: {interv.get('presupuesto_base')} (tipo: {type(interv.get('presupuesto_base')).__name__})")
        print(f"    avance_obra: {interv.get('avance_obra')} (tipo: {type(interv.get('avance_obra')).__name__})")
        print(f"    estado: {interv.get('estado')}")
        print(f"    tipo_intervencion: {interv.get('tipo_intervencion')}")
        
        # Agregar a lista para análisis
        intervenciones_list.append({
            'upid': data.get('upid'),
            'presupuesto_base': interv.get('presupuesto_base'),
            'avance_obra': interv.get('avance_obra'),
            'estado': interv.get('estado')
        })

# Crear DataFrame para análisis
df = pd.DataFrame(intervenciones_list)

print("\n" + "=" * 80)
print("ANÁLISIS DEL DATAFRAME")
print("=" * 80)
print(f"\nColumnas: {list(df.columns)}")
print(f"\nTipos de datos:")
print(df.dtypes)

print(f"\n\nPrimeras filas:")
print(df.head())

print(f"\n\nValores nulos:")
print(df.isnull().sum())

print(f"\n\nValores no nulos en presupuesto_base: {df['presupuesto_base'].notna().sum()}")
print(f"Valores no nulos en avance_obra: {df['avance_obra'].notna().sum()}")
