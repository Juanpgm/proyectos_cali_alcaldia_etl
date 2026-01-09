"""
Script de verificación para montos de empréstito por centro gestor en Firebase
"""

from database.config import get_firestore_client
import pandas as pd

print("=" * 80)
print("VERIFICACIÓN DE MONTOS EMPRÉSTITO POR CENTRO GESTOR EN FIREBASE")
print("=" * 80)

db = get_firestore_client()
collection_name = "montos_emprestito_asignados_centro_gestor"

# Obtener todos los documentos
print(f"\n🔍 Consultando colección: {collection_name}")
docs = db.collection(collection_name).stream()

data = []
for doc in docs:
    doc_data = doc.to_dict()
    doc_data['id'] = doc.id
    data.append(doc_data)

print(f"✅ Total de registros en Firebase: {len(data)}")

if data:
    df = pd.DataFrame(data)
    
    print("\n📊 Resumen de datos:")
    print(f"   Bancos únicos: {df['banco'].nunique()}")
    print(f"   Centros gestores únicos: {df['nombre_centro_gestor'].nunique()}")
    print(f"   Años: {sorted(df['anio'].unique())}")
    print(f"   Monto total programado: ${df['monto_programado'].sum():,.2f}")
    
    print("\n🏦 Distribución por banco:")
    banco_stats = df.groupby('banco').agg({
        'monto_programado': ['sum', 'count']
    }).round(2)
    print(banco_stats)
    
    print("\n📋 Primeros 5 registros:")
    print(df[['banco', 'nombre_centro_gestor', 'bp', 'anio', 'monto_programado']].head())
    
    print("\n✅ Verificación completada exitosamente")
else:
    print("⚠️ No se encontraron registros en la colección")

print("=" * 80)
