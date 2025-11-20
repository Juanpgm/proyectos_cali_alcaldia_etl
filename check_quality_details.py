"""
Script para verificar detalles de control de calidad desde Firebase
"""
import firebase_admin
from firebase_admin import credentials, firestore
from collections import Counter

# Inicializar Firebase
if not firebase_admin._apps:
    cred = credentials.Certificate('target-credentials.json')
    firebase_admin.initialize_app(cred)

db = firestore.client()

# Contar documentos en cada colección
print('📊 COLECCIONES DE CONTROL DE CALIDAD')
print('='*70)

collections = [
    'unidades_proyecto_quality_control_records',
    'unidades_proyecto_quality_control_by_centro_gestor',
    'unidades_proyecto_quality_control_summary',
    'unidades_proyecto_quality_control_metadata',
    'unidades_proyecto_quality_control_changelog'
]

for coll_name in collections:
    docs = list(db.collection(coll_name).limit(5).stream())
    print(f"\n📁 {coll_name}")
    print(f"   Documentos: {len(docs)}")
    
    if docs:
        first_doc = docs[0].to_dict()
        print(f"   Campos: {list(first_doc.keys())[:10]}")

# Obtener sample de records para ver problemas
print('\n\n📋 SAMPLE DE PROBLEMAS (primeros 3 registros)')
print('='*70)

records_ref = db.collection('unidades_proyecto_quality_control_records').limit(3)
for doc in records_ref.stream():
    data = doc.to_dict()
    upid = data.get('upid', 'N/A')
    issues = data.get('issues', [])
    quality_score = data.get('quality_score', 0)
    
    print(f"\n🔍 {upid}")
    print(f"   Score: {quality_score:.1f}/100")
    print(f"   Problemas: {len(issues)}")
    
    if issues:
        # Contar por severidad
        by_severity = Counter(issue.get('severity') for issue in issues)
        print(f"   Por severidad: {dict(by_severity)}")
        
        # Mostrar primer problema
        first_issue = issues[0]
        print(f"   Ejemplo: {first_issue.get('rule_id')} - {first_issue.get('message')[:80]}...")
