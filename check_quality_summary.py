"""
Script para verificar el resumen de control de calidad desde Firebase
"""
import firebase_admin
from firebase_admin import credentials, firestore
from collections import Counter
import json

# Inicializar Firebase
if not firebase_admin._apps:
    cred = credentials.Certificate('target-credentials.json')
    firebase_admin.initialize_app(cred)

db = firestore.client()

# Obtener summary
summary_ref = db.collection('unidades_proyecto_quality_control_summary').limit(1)
summary_docs = list(summary_ref.stream())

if not summary_docs:
    print("❌ No se encontró resumen de calidad")
    exit(1)

summary = summary_docs[0].to_dict()

print('📊 RESUMEN DE CONTROL DE CALIDAD')
print('='*70)
print(f"✅ Registros analizados: {summary.get('total_records', 0)}")
print(f"⚠️  Problemas detectados: {summary.get('total_issues', 0)}")
print(f"📉 Quality Score: {summary.get('quality_score', 0):.2f}/100")
print()

print('📋 PROBLEMAS POR TIPO:')
print('-'*70)
by_type = summary.get('issues_by_type', {})
for issue_type, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True)[:15]:
    print(f"  {issue_type}: {count}")
print()

print('📊 PROBLEMAS POR SEVERIDAD:')
print('-'*70)
by_severity = summary.get('issues_by_severity', {})
for severity in ['CRITICO', 'MAYOR', 'MEDIO', 'MENOR']:
    if severity in by_severity:
        print(f"  {severity}: {by_severity[severity]}")
print()

print('🎯 TOP 5 REGLAS MÁS FRECUENTES:')
print('-'*70)
by_rule = summary.get('issues_by_rule', {})
for rule_id, count in sorted(by_rule.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f"  {rule_id}: {count}")
