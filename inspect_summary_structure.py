"""
Script para ver la estructura real del summary en Firebase
"""
from database.config import get_firestore_client
import json

print('='*80)
print('🔍 ESTRUCTURA REAL DEL SUMMARY EN FIREBASE')
print('='*80)

# Obtener cliente de Firebase usando configuración centralizada
db = get_firestore_client()

# Obtener TODOS los documentos de summary
summary_docs = list(db.collection('unidades_proyecto_quality_control_summary').stream())

print(f"\n📊 Total documentos en summary: {len(summary_docs)}")

for i, doc in enumerate(summary_docs, 1):
    print(f"\n{'='*80}")
    print(f"DOCUMENTO {i}: {doc.id}")
    print('='*80)
    
    data = doc.to_dict()
    
    # Mostrar estructura completa
    print(json.dumps(data, indent=2, default=str))

# Verificar también algunos records
print('\n\n' + '='*80)
print('🔍 ESTRUCTURA DE ALGUNOS RECORDS')
print('='*80)

records = list(db.collection('unidades_proyecto_quality_control_records').limit(3).stream())

for i, doc in enumerate(records, 1):
    print(f"\n{'='*80}")
    print(f"RECORD {i}: {doc.id}")
    print('='*80)
    
    data = doc.to_dict()
    
    # Mostrar info básica
    print(f"UPID: {data.get('upid')}")
    print(f"Quality Score: {data.get('quality_score')}")
    print(f"Timestamp: {data.get('timestamp')}")
    
    # Mostrar issues
    issues = data.get('issues', [])
    print(f"\nIssues ({len(issues)}):")
    
    for j, issue in enumerate(issues[:3], 1):  # Primeros 3
        print(f"\n  Issue {j}:")
        print(f"    Rule: {issue.get('rule_id')}")
        print(f"    Severity: {issue.get('severity')}")
        print(f"    Type: {issue.get('issue_type')}")
        print(f"    Message: {issue.get('message')[:100]}...")
        print(f"    Field: {issue.get('field')}")

print('\n' + '='*80)
