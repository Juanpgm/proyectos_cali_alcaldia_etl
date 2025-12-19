#!/usr/bin/env python3
"""
Verificar estructura del GeoJSON generado
"""
import json

# Cargar GeoJSON
with open('app_outputs/unidades_proyecto_transformed.geojson', 'r', encoding='utf-8') as f:
    geojson = json.load(f)

print(f"\n📊 ESTRUCTURA DEL GEOJSON\n")
print(f"Total features: {len(geojson['features'])}")

# Analizar primera feature
feature = geojson['features'][0]
print(f"\n🔍 Primera feature (unidad de proyecto):")
print(f"   • UPID: {feature['properties'].get('upid')}")
print(f"   • Nombre: {feature['properties'].get('nombre_up', 'N/A')[:60]}")
print(f"   • Geometry a nivel de unidad: {'✅ Presente' if feature.get('geometry') else '❌ Ausente'}")
print(f"   • N° intervenciones: {len(feature['properties'].get('intervenciones', []))}")

# Verificar si geometry está en intervenciones (no debería)
if feature['properties'].get('intervenciones'):
    interv = feature['properties']['intervenciones'][0]
    print(f"\n   📋 Primera intervención:")
    print(f"      - ID: {interv.get('intervencion_id')}")
    print(f"      - Estado: {interv.get('estado')}")
    print(f"      - Geometry en intervención: {'❌ PRESENTE (ERROR)' if 'geometry' in interv else '✅ Ausente (correcto)'}")

# Buscar unidad con múltiples intervenciones
print(f"\n🔗 Buscando unidades con múltiples intervenciones...")
multi_intervention_units = [
    f for f in geojson['features'] 
    if len(f['properties'].get('intervenciones', [])) > 1
]

print(f"   • Unidades con >1 intervención: {len(multi_intervention_units)}")

if multi_intervention_units:
    # Ordenar por número de intervenciones
    multi_intervention_units.sort(
        key=lambda f: len(f['properties'].get('intervenciones', [])), 
        reverse=True
    )
    
    print(f"\n🏆 Top 5 unidades más agrupadas:")
    for i, f in enumerate(multi_intervention_units[:5], 1):
        upid = f['properties'].get('upid')
        nombre = f['properties'].get('nombre_up', 'N/A')[:50]
        n_interv = len(f['properties'].get('intervenciones', []))
        has_geom = '✅' if f.get('geometry') else '❌'
        print(f"   {i}. {upid}: {nombre} - {n_interv} intervenciones {has_geom}")

# Verificar unidades sin geometry
sin_geometry = [f for f in geojson['features'] if not f.get('geometry')]
print(f"\n⚠️  Unidades sin geometry: {len(sin_geometry)}")

# Verificar unidades agrupadas sin geometry
agrupadas_sin_geom = [
    f for f in multi_intervention_units 
    if not f.get('geometry')
]
print(f"   • Unidades agrupadas sin geometry: {len(agrupadas_sin_geom)}")

if agrupadas_sin_geom:
    print(f"\n   Unidades agrupadas SIN geometry:")
    for f in agrupadas_sin_geom[:5]:
        upid = f['properties'].get('upid')
        nombre = f['properties'].get('nombre_up') or 'N/A'
        nombre_display = nombre[:50] if nombre != 'N/A' else 'N/A'
        n_interv = len(f['properties'].get('intervenciones', []))
        print(f"      - {upid}: {nombre_display} ({n_interv} intervenciones)")

print(f"\n{'='*80}")
if len(agrupadas_sin_geom) == 0:
    print("✅ VALIDACIÓN EXITOSA")
    print(f"   • Todas las unidades agrupadas tienen geometry")
    print(f"   • Geometry está a nivel de unidad de proyecto")
    print(f"   • Intervenciones NO tienen geometry")
else:
    print("⚠️  VALIDACIÓN PARCIAL")
    print(f"   • {len(agrupadas_sin_geom)} unidades agrupadas sin geometry")
print(f"{'='*80}\n")
