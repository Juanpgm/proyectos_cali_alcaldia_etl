from database.config import get_firestore_client

db = get_firestore_client()

# Contar contratos
contratos = list(db.collection('contratos_emprestito').limit(5).stream())
print(f'📄 Contratos en Firestore: {len(contratos)} (muestra de 5)')
for i, c in enumerate(contratos):
    data = c.to_dict()
    print(f'{i+1}. {c.id}: {data.get("referencia_contrato", "N/A")} - {data.get("banco", "N/A")}')

# Contar órdenes de compra
ordenes = list(db.collection('ordenes_compra_emprestito').limit(5).stream())
print(f'\n📋 Órdenes de compra en Firestore: {len(ordenes)} (muestra de 5)')
for i, o in enumerate(ordenes):
    data = o.to_dict()
    print(f'{i+1}. {o.id}:')
    print(f'   numero_orden: {data.get("numero_orden", "N/A")}')
    print(f'   banco: {data.get("nombre_banco", "N/A")}')
    print(f'   valor: {data.get("valor_orden", "N/A")}')
