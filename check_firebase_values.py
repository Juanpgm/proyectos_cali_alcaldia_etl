# Check Firebase data for standardized values
from database.config import get_firestore_client

db = get_firestore_client()
docs = db.collection('unidades_proyecto').where('tipo_equipamiento', '!=', 'Vias').limit(50).stream()

estados = {}
tipos = {}

for doc in docs:
    d = doc.to_dict()
    e = d.get('estado')
    t = d.get('tipo_intervencion')
    if e:
        estados[e] = estados.get(e, 0) + 1
    if t:
        tipos[t] = tipos.get(t, 0) + 1

print('=== ESTADOS EN FIREBASE ===')
for k, v in sorted(estados.items()):
    print(f'{k}: {v}')

print('\n=== TIPOS INTERVENCIÓN EN FIREBASE ===')
for k, v in sorted(tipos.items()):
    print(f'{k}: {v}')

# Check for non-standardized values
print('\n=== VERIFICACIÓN DE VALORES NO ESTANDARIZADOS ===')
if 'Socialización' in estados:
    print(f'❌ PROBLEMA: "Socialización" encontrado ({estados["Socialización"]} docs) - debería ser "En alistamiento"')
if 'Adecuaciones' in tipos:
    print(f'❌ PROBLEMA: "Adecuaciones" encontrado ({tipos["Adecuaciones"]} docs) - debería ser "Adecuaciones y Mantenimientos"')
if 'Mantenimiento' in tipos:
    print(f'❌ PROBLEMA: "Mantenimiento" encontrado ({tipos["Mantenimiento"]} docs) - debería ser "Adecuaciones y Mantenimientos"')
if 'Rehabilitación / Reforzamiento' in tipos:
    print(f'❌ PROBLEMA: "Rehabilitación / Reforzamiento" encontrado - debería ser "Rehabilitación - Reforzamiento"')

if not any(x in estados for x in ['Socialización']) and not any(x in tipos for x in ['Adecuaciones', 'Mantenimiento', 'Rehabilitación / Reforzamiento']):
    print('✅ Todos los valores están correctamente estandarizados')
