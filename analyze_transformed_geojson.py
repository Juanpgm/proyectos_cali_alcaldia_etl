# -*- coding: utf-8 -*-
import json

# Cargar GeoJSON transformado
with open('app_outputs/unidades_proyecto_transformed.geojson', encoding='utf-8') as f:
    geojson = json.load(f)

features = geojson['features']

print(f"Total features (unidades): {len(features)}")

# Analizar geometry
con_geom = [f for f in features if f.get('geometry') is not None]
sin_geom = [f for f in features if f.get('geometry') is None]

print(f"Con geometry: {len(con_geom)} ({len(con_geom)*100/len(features):.1f}%)")
print(f"Sin geometry: {len(sin_geom)} ({len(sin_geom)*100/len(features):.1f}%)")

# Analizar lat/lon en properties
con_lat_lon = [f for f in features if f['properties'].get('lat') is not None and f['properties'].get('lon') is not None]
sin_lat_lon = [f for f in features if f['properties'].get('lat') is None or f['properties'].get('lon') is None]

print(f"\nCon lat/lon en properties: {len(con_lat_lon)} ({len(con_lat_lon)*100/len(features):.1f}%)")
print(f"Sin lat/lon en properties: {len(sin_lat_lon)} ({len(sin_lat_lon)*100/len(features):.1f}%)")

# Analizar por tipo
print(f"\nAnálisis detallado de registros SIN geometry:")
print(f"Total sin geometry: {len(sin_geom)}")

# Analizar cuántos tienen intervenciones
sin_geom_1_interv = [f for f in sin_geom if len(f['properties'].get('intervenciones', [])) == 1]
sin_geom_multi = [f for f in sin_geom if len(f['properties'].get('intervenciones', [])) > 1]

print(f"  - Con 1 intervención: {len(sin_geom_1_interv)}")
print(f"  - Con múltiples intervenciones: {len(sin_geom_multi)}")

# Ver ejemplos de tipos de equipamiento sin geometry
tipos = {}
for f in sin_geom[:100]:
    tipo = f['properties'].get('tipo_equipamiento', 'N/A')
    tipos[tipo] = tipos.get(tipo, 0) + 1

print(f"\nTipos de equipamiento más comunes sin geometry:")
for tipo, count in sorted(tipos.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {tipo}: {count}")

# Analizar cuántos deberían tener coordenadas por tener intervenciones
print(f"\n[CRITICAL] Registros que DEBERÍAN tener geometry pero no la tienen:")
print(f"  Múltiples intervenciones sin geometry: {len(sin_geom_multi)}")

if len(sin_geom_multi) > 0:
    print(f"\n  Ejemplos:")
    for f in sin_geom_multi[:3]:
        props = f['properties']
        print(f"    UPID: {props.get('upid')}")
        print(f"    Nombre: {props.get('nombre_up')}")
        print(f"    Intervenciones: {len(props.get('intervenciones', []))}")
        print(f"    lat: {props.get('lat')}, lon: {props.get('lon')}")
        print()
