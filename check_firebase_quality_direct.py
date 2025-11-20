"""
Script para verificar control de calidad directamente desde Firebase
"""
import firebase_admin
from firebase_admin import credentials, firestore
from collections import Counter

# Inicializar Firebase
if not firebase_admin._apps:
    cred = credentials.Certificate('target-credentials.json')
    firebase_admin.initialize_app(cred)

db = firestore.client()

print('='*80)
print('🔍 VERIFICACIÓN DIRECTA DE FIREBASE - CONTROL DE CALIDAD')
print('='*80)

# 1. Verificar colecciones existen
print('\n📁 VERIFICANDO COLECCIONES:')
print('-'*80)

collections_to_check = [
    'unidades_proyecto_quality_control_records',
    'unidades_proyecto_quality_control_by_centro_gestor', 
    'unidades_proyecto_quality_control_summary',
    'unidades_proyecto_quality_control_metadata',
    'unidades_proyecto_quality_control_changelog'
]

collection_counts = {}
for coll_name in collections_to_check:
    try:
        # Contar documentos (sample de 10 para verificar)
        docs = list(db.collection(coll_name).limit(10).stream())
        collection_counts[coll_name] = len(docs)
        status = '✅' if docs else '⚠️ '
        print(f'{status} {coll_name}: {len(docs)} docs (sample)')
    except Exception as e:
        print(f'❌ {coll_name}: Error - {e}')
        collection_counts[coll_name] = 0

# 2. Obtener Summary
print('\n\n📊 RESUMEN GLOBAL (summary):')
print('-'*80)

try:
    summary_docs = list(db.collection('unidades_proyecto_quality_control_summary').stream())
    if summary_docs:
        summary = summary_docs[0].to_dict()
        print(f"✅ Registros analizados: {summary.get('total_records', 0):,}")
        print(f"⚠️  Problemas detectados: {summary.get('total_issues', 0):,}")
        print(f"📉 Quality Score: {summary.get('quality_score', 0):.2f}/100")
        print(f"📅 Timestamp: {summary.get('timestamp', 'N/A')}")
        print(f"🔖 Report ID: {summary.get('report_id', 'N/A')}")
        
        # Problemas por severidad
        by_severity = summary.get('issues_by_severity', {})
        if by_severity:
            print(f"\n📊 Por severidad:")
            for sev in ['CRITICO', 'MAYOR', 'MEDIO', 'MENOR']:
                if sev in by_severity:
                    print(f"   {sev}: {by_severity[sev]:,}")
        
        # Top 10 tipos de problemas
        by_type = summary.get('issues_by_type', {})
        if by_type:
            print(f"\n📋 Top 10 tipos de problemas:")
            for issue_type, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"   {issue_type}: {count:,}")
        
        # Top 5 reglas
        by_rule = summary.get('issues_by_rule', {})
        if by_rule:
            print(f"\n🎯 Top 5 reglas:")
            for rule_id, count in sorted(by_rule.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   {rule_id}: {count:,}")
    else:
        print('⚠️  No hay documentos en summary')
except Exception as e:
    print(f'❌ Error obteniendo summary: {e}')

# 3. Sample de records
print('\n\n📋 SAMPLE DE REGISTROS (primeros 5):')
print('-'*80)

try:
    records = list(db.collection('unidades_proyecto_quality_control_records').limit(5).stream())
    for i, doc in enumerate(records, 1):
        data = doc.to_dict()
        upid = data.get('upid', 'N/A')
        issues = data.get('issues', [])
        quality_score = data.get('quality_score', 0)
        
        print(f"\n{i}. 🔍 {upid}")
        print(f"   Score: {quality_score:.1f}/100")
        print(f"   Problemas: {len(issues)}")
        
        if issues:
            # Contar por severidad
            by_sev = Counter(issue.get('severity') for issue in issues)
            print(f"   Severidad: {dict(by_sev)}")
            
            # Primer problema como ejemplo
            first = issues[0]
            print(f"   Ejemplo: [{first.get('severity')}] {first.get('rule_id')} - {first.get('message')[:70]}...")
except Exception as e:
    print(f'❌ Error obteniendo records: {e}')

# 4. Metadata categórica
print('\n\n📦 METADATA CATEGÓRICA:')
print('-'*80)

try:
    metadata_docs = list(db.collection('unidades_proyecto_quality_control_metadata').stream())
    if metadata_docs:
        metadata = metadata_docs[0].to_dict()
        print(f"✅ Metadata disponible")
        print(f"   Secciones: {list(metadata.keys())}")
        
        # Verificar estructura para Next.js
        if 'filters' in metadata:
            print(f"   Filters: {len(metadata['filters'])} disponibles")
        if 'tabs' in metadata:
            print(f"   Tabs: {len(metadata['tabs'])} disponibles")
        if 'tables' in metadata:
            print(f"   Tables: {len(metadata['tables'])} disponibles")
    else:
        print('⚠️  No hay metadata disponible')
except Exception as e:
    print(f'❌ Error obteniendo metadata: {e}')

# 5. Changelog
print('\n\n📝 CHANGELOG (últimos 5):')
print('-'*80)

try:
    changelog_docs = list(db.collection('unidades_proyecto_quality_control_changelog')
                         .order_by('timestamp', direction=firestore.Query.DESCENDING)
                         .limit(5)
                         .stream())
    if changelog_docs:
        for i, doc in enumerate(changelog_docs, 1):
            data = doc.to_dict()
            print(f"\n{i}. {data.get('action', 'N/A')}: {data.get('upid', 'N/A')}")
            print(f"   Collection: {data.get('collection', 'N/A')}")
            print(f"   Timestamp: {data.get('timestamp', 'N/A')}")
            if 'changes' in data:
                print(f"   Cambios: {len(data['changes'])} campos")
    else:
        print('⚠️  No hay entradas en changelog')
except Exception as e:
    print(f'❌ Error obteniendo changelog: {e}')

print('\n' + '='*80)
print('✅ VERIFICACIÓN COMPLETADA')
print('='*80)
