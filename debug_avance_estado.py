#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de debugging para verificar el procesamiento de avance_obra y estado
"""

import json
import os

# Cargar GeoJSON transformado
geojson_path = 'transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto.geojson'

if os.path.exists(geojson_path):
    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    features = data.get('features', [])
    print(f"Total de features: {len(features)}")
    print("\n" + "="*80)
    print("ANÁLISIS DE AVANCE_OBRA Y ESTADO")
    print("="*80)
    
    # Buscar casos donde avance_obra es cero
    casos_cero = []
    estados_encontrados = set()
    avances_encontrados = set()
    
    for i, feature in enumerate(features):
        props = feature.get('properties', {})
        avance_obra = props.get('avance_obra')
        estado = props.get('estado')
        upid = props.get('upid')
        nickname = props.get('nickname', 'N/A')
        
        estados_encontrados.add(str(estado))
        avances_encontrados.add(str(avance_obra))
        
        # Detectar casos donde avance_obra es cero en diferentes formatos
        avance_str = str(avance_obra).strip().lower() if avance_obra is not None else ''
        
        if avance_str in ['cero', '(cero)', '(0)', '0', '0.0', '0,0']:
            casos_cero.append({
                'index': i,
                'upid': upid,
                'nickname': nickname,
                'avance_obra': avance_obra,
                'estado': estado
            })
    
    print(f"\n📊 RESUMEN:")
    print(f"  - Total de registros: {len(features)}")
    print(f"  - Casos con avance_obra = 0: {len(casos_cero)}")
    
    print(f"\n📈 Estados encontrados ({len(estados_encontrados)}):")
    for estado in sorted(estados_encontrados):
        count = sum(1 for f in features if str(f['properties'].get('estado')) == estado)
        print(f"  - {estado}: {count}")
    
    print(f"\n📉 Avances encontrados (únicos: {len(avances_encontrados)}):")
    # Mostrar solo los primeros 20 valores únicos
    for avance in sorted(list(avances_encontrados))[:20]:
        count = sum(1 for f in features if str(f['properties'].get('avance_obra')) == avance)
        print(f"  - {avance}: {count}")
    
    if len(casos_cero) > 0:
        print(f"\n🔍 CASOS CON AVANCE_OBRA = 0 (primeros 10):")
        print("="*80)
        for caso in casos_cero[:10]:
            print(f"\n  UPID: {caso['upid']}")
            print(f"  Proyecto: {caso['nickname']}")
            print(f"  Avance obra: '{caso['avance_obra']}' (tipo: {type(caso['avance_obra']).__name__})")
            print(f"  Estado: '{caso['estado']}'")
            print(f"  ¿Debería ser 'En alistamiento'?: {caso['estado'] != 'En alistamiento'}")
            print("-"*80)
    else:
        print("\n✅ No se encontraron casos con avance_obra = 0")
    
    # Verificar casos problemáticos específicos
    print(f"\n🔍 VERIFICACIÓN DE CASOS PROBLEMÁTICOS:")
    print("="*80)
    
    # Casos donde avance_obra es 0 pero estado NO es "En alistamiento"
    problematicos = [caso for caso in casos_cero if caso['estado'] != 'En alistamiento']
    
    if problematicos:
        print(f"\n❌ ENCONTRADOS {len(problematicos)} CASOS PROBLEMÁTICOS:")
        print("   (avance_obra = 0 pero estado != 'En alistamiento')")
        for caso in problematicos[:5]:
            print(f"\n  - UPID: {caso['upid']}")
            print(f"    Avance: {caso['avance_obra']}")
            print(f"    Estado: {caso['estado']}")
    else:
        print("\n✅ No hay casos problemáticos (todos los avance_obra=0 tienen estado='En alistamiento')")
    
else:
    print(f"❌ Archivo no encontrado: {geojson_path}")
