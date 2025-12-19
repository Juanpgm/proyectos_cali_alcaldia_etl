"""
Script para verificar que el GeoJSON generado incluye todas las variables
y la estructura correcta con frente_activo
"""

import json
import sys
from pathlib import Path

def verify_output():
    """Verifica la estructura completa del GeoJSON de salida"""
    
    geojson_path = Path("app_outputs/unidades_proyecto_transformed.geojson")
    
    if not geojson_path.exists():
        print(f"❌ No se encontró el archivo: {geojson_path}")
        return False
    
    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print("\n" + "="*80)
    print("🔍 VERIFICACIÓN DEL GEOJSON GENERADO")
    print("="*80)
    
    # Verificar estructura general
    print(f"\n✅ Total features (unidades): {len(data['features'])}")
    print(f"✅ Type: {data['type']}")
    
    if not data['features']:
        print("❌ No hay features en el GeoJSON")
        return False
    
    # Analizar primera unidad
    first_feature = data['features'][0]
    props = first_feature['properties']
    
    print("\n" + "-"*80)
    print("📋 CAMPOS A NIVEL DE UNIDAD DE PROYECTO")
    print("-"*80)
    
    unit_fields = [k for k in sorted(props.keys()) if k != 'intervenciones']
    for field in unit_fields:
        print(f"  ✓ {field}")
    
    print(f"\n✅ Total campos de unidad: {len(unit_fields)}")
    
    # Analizar intervenciones
    print("\n" + "-"*80)
    print("📋 CAMPOS EN INTERVENCIONES (array)")
    print("-"*80)
    
    if 'intervenciones' not in props:
        print("❌ No se encontró el campo 'intervenciones'")
        return False
    
    intervenciones = props['intervenciones']
    print(f"\n✅ Número de intervenciones en primera unidad: {len(intervenciones)}")
    
    if not intervenciones:
        print("⚠️  Primera unidad no tiene intervenciones")
        # Buscar una unidad con intervenciones
        for feat in data['features'][:10]:
            if feat['properties']['intervenciones']:
                intervenciones = feat['properties']['intervenciones']
                print(f"✓ Usando unidad {feat['properties']['upid']} que tiene {len(intervenciones)} intervenciones")
                break
    
    if intervenciones:
        interv_fields = sorted(intervenciones[0].keys())
        for field in interv_fields:
            print(f"  ✓ {field}")
        
        print(f"\n✅ Total campos de intervención: {len(interv_fields)}")
        
        # Verificar específicamente frente_activo
        if 'frente_activo' in interv_fields:
            print("\n✅ Campo 'frente_activo' PRESENTE en intervenciones")
        else:
            print("\n❌ Campo 'frente_activo' NO ENCONTRADO en intervenciones")
    
    # Verificar geometry
    print("\n" + "-"*80)
    print("🌍 VERIFICACIÓN DE GEOMETRY")
    print("-"*80)
    
    if 'geometry' in first_feature:
        geom = first_feature['geometry']
        if geom is not None:
            print(f"✅ Geometry presente: Type={geom.get('type', 'N/A')}")
            if geom['type'] == 'Point' and 'coordinates' in geom:
                print(f"  Coordinates: {geom['coordinates']}")
        else:
            print("⚠️  Geometry es null en la primera unidad")
    else:
        print("❌ Campo 'geometry' no encontrado")
    
    # Contar unidades con geometría
    units_with_geom = sum(1 for f in data['features'] if f.get('geometry') is not None)
    geom_coverage = (units_with_geom / len(data['features'])) * 100
    print(f"✅ Unidades con geometry: {units_with_geom}/{len(data['features'])} ({geom_coverage:.1f}%)")
    
    # Ejemplos de frente_activo
    print("\n" + "-"*80)
    print("📊 EJEMPLOS DE frente_activo POR ESTADO")
    print("-"*80)
    
    ejemplos_por_estado = {}
    for feature in data['features'][:50]:
        for interv in feature['properties']['intervenciones']:
            estado = interv.get('estado', 'N/A')
            frente = interv.get('frente_activo', 'N/A')
            
            if estado not in ejemplos_por_estado and estado != 'N/A':
                ejemplos_por_estado[estado] = frente
    
    for estado, frente in sorted(ejemplos_por_estado.items()):
        print(f"  Estado: {estado:30s} → frente_activo: {frente}")
    
    # Unidades con múltiples intervenciones
    print("\n" + "-"*80)
    print("🔢 UNIDADES CON MÚLTIPLES INTERVENCIONES")
    print("-"*80)
    
    multi_interv = [(f['properties']['upid'], 
                     f['properties']['nombre_up'], 
                     len(f['properties']['intervenciones']))
                    for f in data['features'] 
                    if len(f['properties']['intervenciones']) > 1]
    
    multi_interv.sort(key=lambda x: x[2], reverse=True)
    
    print(f"\n✅ Total unidades con >1 intervención: {len(multi_interv)}")
    print("\nTop 5:")
    for upid, nombre, n_interv in multi_interv[:5]:
        print(f"  • {upid}: {nombre} ({n_interv} intervenciones)")
    
    print("\n" + "="*80)
    print("✅ VERIFICACIÓN COMPLETA")
    print("="*80)
    
    return True

if __name__ == "__main__":
    try:
        success = verify_output()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Error durante la verificación: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
