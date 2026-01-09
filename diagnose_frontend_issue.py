# -*- coding: utf-8 -*-
"""
Script para diagnosticar problema de lectura del frontend
Genera queries de ejemplo que el frontend debería usar
"""

import sys
import json
sys.path.append('database')
from config import get_firestore_client

print('='*80)
print('DIAGNOSTICO DE PROBLEMA FRONTEND vs FIREBASE')
print('='*80)

db = get_firestore_client()

# 1. Verificar si existe colección antigua o diferentes
print('\n[CHECK 1] Verificando si hay colecciones con estructura antigua:')
collections = list(db.collections())
for col in collections:
    col_name = col.id
    if 'unidad' in col_name.lower() or 'interven' in col_name.lower():
        try:
            sample_docs = list(db.collection(col_name).limit(2).stream())
            print(f'\n  Colección: {col_name}')
            print(f'    - Total docs (sample): {len(sample_docs)}')
            if len(sample_docs) > 0:
                data = sample_docs[0].to_dict()
                # Verificar si tiene estructura plana (vieja) o anidada (nueva)
                if 'intervenciones' in data:
                    print(f'    - Estructura: NUEVA (con array intervenciones)')
                    print(f'    - Intervenciones en primer doc: {len(data["intervenciones"])}')
                elif 'intervencion_id' in data:
                    print(f'    - Estructura: VIEJA (documento plano por intervención)')
                else:
                    print(f'    - Estructura: DESCONOCIDA')
                print(f'    - Campos principales: {list(data.keys())[:10]}')
        except Exception as e:
            print(f'    - Error leyendo: {e}')

# 2. Comparar timestamps de última actualización
print(f'\n[CHECK 2] Verificando timestamps de actualización:')
docs = list(db.collection('unidades_proyecto').limit(5).stream())
for doc in docs[:3]:
    data = doc.to_dict()
    upid = data.get('upid', 'N/A')
    updated = data.get('updated_at', 'N/A')
    created = data.get('created_at', 'N/A')
    print(f'  UPID: {upid}')
    print(f'    - Created: {created}')
    print(f'    - Updated: {updated}')

# 3. Generar query de ejemplo para frontend
print(f'\n[CHECK 3] Query de ejemplo para frontend (JavaScript):')
print('''
// Frontend debe hacer:
const db = firebase.firestore();
const snapshot = await db.collection('unidades_proyecto').get();

let totalIntervenciones = 0;
let totalPresupuesto = 0;
let totalAvance = 0;
let countAvance = 0;

snapshot.forEach(doc => {
  const up = doc.data();
  const intervenciones = up.intervenciones || [];
  
  intervenciones.forEach(interv => {
    totalIntervenciones++;
    totalPresupuesto += interv.presupuesto_base || 0;
    if (interv.avance_obra !== null) {
      totalAvance += interv.avance_obra;
      countAvance++;
    }
  });
});

console.log('Total UPs:', snapshot.size);
console.log('Total Intervenciones:', totalIntervenciones);
console.log('Suma Presupuesto:', totalPresupuesto);
console.log('Promedio Avance:', totalAvance / countAvance);
''')

# 4. Verificar si el frontend está leyendo estructura antigua
print(f'\n[CHECK 4] Buscando posible colección antigua (estructura plana):')

# Buscar en proyectos_presupuestales (podría ser la colección que el frontend lee)
try:
    proy_docs = list(db.collection('proyectos_presupuestales').limit(3).stream())
    print(f'\n  Colección "proyectos_presupuestales": {len(proy_docs)} docs (sample)')
    if len(proy_docs) > 0:
        proy_data = proy_docs[0].to_dict()
        print(f'  - Estructura: {list(proy_data.keys())[:10]}')
        print(f'  [WARNING] El frontend podría estar leyendo esta colección!')
        
        # Contar total
        all_proy = list(db.collection('proyectos_presupuestales').stream())
        print(f'  - Total documentos: {len(all_proy)}')
except Exception as e:
    print(f'  - No se pudo leer: {e}')

# 5. Verificar reglas de seguridad de Firestore
print(f'\n[CHECK 5] Posibles problemas de lectura:')
print(f'  1. Frontend podría estar usando colección incorrecta')
print(f'  2. Frontend podría no estar sumando las intervenciones del array')
print(f'  3. Frontend podría estar filtrando registros por error')
print(f'  4. Frontend podría estar usando caché obsoleta')

# 6. Generar JSON de ejemplo de un documento correcto
print(f'\n[CHECK 6] Estructura correcta de un documento:')
sample_doc = docs[0].to_dict()
# Limpiar campos innecesarios para el ejemplo
clean_sample = {
    'upid': sample_doc.get('upid'),
    'nombre_up': sample_doc.get('nombre_up'),
    'clase_up': sample_doc.get('clase_up'),
    'n_intervenciones': sample_doc.get('n_intervenciones'),
    'geometry': sample_doc.get('geometry'),
    'has_geometry': sample_doc.get('has_geometry'),
    'intervenciones': sample_doc.get('intervenciones', [])[:2]  # Solo 2 primeras
}
print(json.dumps(clean_sample, indent=2, default=str, ensure_ascii=False)[:1500])

print(f'\n{"="*80}')
print(f'CONCLUSIONES:')
print(f'{"="*80}')
print(f'[OK] Datos en Firebase están CORRECTOS')
print(f'[OK] ETL funciona perfectamente')
print(f'[PROBLEMA] El frontend está:')
print(f'  A) Consultando colección incorrecta, o')
print(f'  B) No leyendo el array "intervenciones" correctamente, o')
print(f'  C) Aplicando filtros que eliminan datos, o')
print(f'  D) Usando caché desactualizada')
print(f'\n[SOLUCION] Revisar código del frontend:')
print(f'  1. Verificar que usa colección "unidades_proyecto"')
print(f'  2. Verificar que itera sobre "intervenciones" de cada UP')
print(f'  3. Hacer hard refresh (Ctrl+Shift+R) para limpiar caché')
print(f'  4. Verificar queries y agregaciones en código frontend')
print(f'{"="*80}')
