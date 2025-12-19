#!/usr/bin/env python3
"""
Script para verificar que las unidades agrupadas tengan geometry válida y consistente
"""
import json
import sys

def main():
    # Cargar GeoJSON
    with open('app_outputs/unidades_proyecto_transformed.geojson', 'r', encoding='utf-8') as f:
        geojson = json.load(f)

    features = geojson['features']
    print(f'📊 Total de features: {len(features)}\n')

    # Agrupar por UPID
    upid_data = {}
    for feature in features:
        props = feature['properties']
        upid = props.get('upid')
        geom = feature.get('geometry')
        
        if upid not in upid_data:
            upid_data[upid] = {
                'count': 0,
                'geometries': [],
                'nombre': props.get('nombre_up', 'N/A')
            }
        
        upid_data[upid]['count'] += 1
        upid_data[upid]['geometries'].append(geom)

    # Filtrar unidades agrupadas (más de 1 intervención)
    grouped = {k: v for k, v in upid_data.items() if v['count'] > 1}
    print(f'🔗 Unidades agrupadas (>1 intervención): {len(grouped)}')

    # Verificar geometry null en unidades agrupadas
    null_geometry = []
    inconsistent_geometry = []

    for upid, data in grouped.items():
        geoms = data['geometries']
        
        # Verificar si alguna geometry es null
        null_count = sum(1 for g in geoms if g is None)
        if null_count > 0:
            null_geometry.append((upid, data['nombre'], data['count'], null_count))
        
        # Verificar consistencia (todas las geometries deben ser iguales)
        unique_geoms = set(json.dumps(g, sort_keys=True) if g else 'null' for g in geoms)
        if len(unique_geoms) > 1:
            inconsistent_geometry.append((upid, data['nombre'], data['count'], len(unique_geoms)))

    print(f'\n✅ Verificación de geometry en unidades agrupadas:')
    print(f'   • Total unidades agrupadas: {len(grouped)}')
    print(f'   • Con geometry null: {len(null_geometry)}')
    print(f'   • Con geometries inconsistentes: {len(inconsistent_geometry)}')

    if null_geometry:
        print(f'\n❌ Unidades agrupadas con geometry NULL:')
        for upid, nombre, total, nulls in null_geometry[:15]:
            nombre_safe = nombre[:60] if nombre else 'N/A'
            print(f'   • {upid}: {nombre_safe} ({nulls}/{total} null)')
    else:
        print(f'\n✅ ¡Todas las unidades agrupadas tienen geometry válida!')

    if inconsistent_geometry:
        print(f'\n⚠️  Unidades con geometries INCONSISTENTES:')
        for upid, nombre, total, unique in inconsistent_geometry[:15]:
            print(f'   • {upid}: {nombre[:60]} ({unique} geometries diferentes en {total} intervenciones)')
    else:
        print(f'\n✅ ¡Todas las unidades agrupadas tienen geometry consistente!')

    # Top 10 unidades más agrupadas
    top_grouped = sorted(grouped.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
    print(f'\n🏆 Top 10 unidades más agrupadas:')
    for upid, data in top_grouped:
        geom_status = '✅ con geometry' if data['geometries'][0] else '❌ sin geometry'
        nombre = data['nombre'][:60]
        print(f'   {upid}: {nombre} - {data["count"]} intervenciones {geom_status}')

    # Resumen final
    print(f'\n' + '='*80)
    if null_geometry or inconsistent_geometry:
        print('❌ VALIDACIÓN FALLIDA')
        if null_geometry:
            print(f'   • {len(null_geometry)} unidades agrupadas tienen geometry null')
        if inconsistent_geometry:
            print(f'   • {len(inconsistent_geometry)} unidades tienen geometries inconsistentes')
        sys.exit(1)
    else:
        print('✅ VALIDACIÓN EXITOSA')
        print(f'   • Todas las {len(grouped)} unidades agrupadas tienen geometry válida y consistente')
        sys.exit(0)

if __name__ == '__main__':
    main()
