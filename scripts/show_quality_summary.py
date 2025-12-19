"""
Script para mostrar resumen de métricas de calidad
"""
import json
import webbrowser
from pathlib import Path

# Cargar último reporte
report_file = Path("app_outputs/quality_reports/quality_report_20251218_035807.json")
with open(report_file, 'r', encoding='utf-8') as f:
    r = json.load(f)

print("\n" + "="*80)
print("📊 MÉTRICAS DE CALIDAD - RESUMEN FINAL")
print("="*80)

print(f"\n🎯 Puntuación Global: {r['statistics']['quality_score']}/100")
print(f"   Rating: {r['statistics']['quality_rating']}")

print(f"\n📋 Estructura del Dataset:")
print(f"   • {r['total_unidades']:,} unidades de proyecto")
print(f"   • {r['total_intervenciones']:,} intervenciones totales")
print(f"   • Promedio: {r['total_intervenciones']/r['total_unidades']:.2f} intervenciones por unidad")

print(f"\n⚠️  Problemas Detectados:")
print(f"   • Total de issues: {r['total_issues']:,}")
print(f"   • Unidades afectadas: {r['unidades_with_issues']:,} ({r['unidades_with_issues']/r['total_unidades']*100:.1f}%)")
print(f"   • Intervenciones afectadas: {r['intervenciones_with_issues']:,} ({r['intervenciones_with_issues']/r['total_intervenciones']*100:.1f}%)")

print(f"\n🔴 Por Severidad:")
for severity, count in r['statistics']['by_severity'].items():
    emoji = {'CRITICAL': '🔴', 'HIGH': '🟠', 'MEDIUM': '🟡', 'LOW': '🔵', 'INFO': '⚪'}.get(severity, '')
    print(f"   {emoji} {severity}: {count:,}")

print(f"\n📌 Top 5 Problemas Más Frecuentes:")
for i, (k, v) in enumerate(list(r['statistics']['top_issues'].items())[:5], 1):
    print(f"   {i}. {k}: {v['count']:,} - {v['name']}")
    print(f"      Severidad: {v['severity']}, Dimensión: {v['dimension']}")

print(f"\n📊 Dimensiones ISO 19157:")
for dimension, count in r['statistics']['by_dimension'].items():
    print(f"   • {dimension}: {count:,} problemas")

print(f"\n💡 Recomendaciones Principales:")
print(f"   1. Corregir {r['statistics']['top_issues']['PA002']['count']:,} coordenadas invertidas (CRÍTICO)")
print(f"   2. Completar {r['statistics']['top_issues']['CO001']['count']} campos obligatorios faltantes")
print(f"   3. Revisar {r['statistics']['top_issues']['LC001']['count']} presupuestos inválidos")
if r['statistics']['top_issues'].get('CO002'):
    print(f"   4. Geolocalizar {r['statistics']['top_issues']['CO002']['count']} unidades sin geometría")

print(f"\n📁 Archivos Generados:")
print(f"   • JSON: {report_file}")
print(f"   • HTML: {report_file.with_suffix('.html')}")

print("\n" + "="*80)

# Abrir HTML
html_file = report_file.with_suffix('.html')
webbrowser.open(html_file.absolute().as_uri())
print("🌐 Reporte HTML abierto en el navegador")
