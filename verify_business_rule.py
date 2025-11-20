#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de verificación final para validar que la regla de negocio se aplicó correctamente
"""

import json
import os

# Cargar GeoJSON transformado
geojson_path = 'transformation_app/app_outputs/unidades_proyecto_outputs/unidades_proyecto.geojson'

if os.path.exists(geojson_path):
    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    features = data.get('features', [])
    print("="*80)
    print("VERIFICACIÓN FINAL DE LA REGLA DE NEGOCIO")
    print("="*80)
    print(f"\nTotal de registros: {len(features)}")
    
    # Estadísticas por estado
    estados = {}
    avances_por_estado = {}
    
    for feature in features:
        props = feature.get('properties', {})
        estado = props.get('estado', 'Sin estado')
        avance_obra = props.get('avance_obra')
        
        # Contar estados
        estados[estado] = estados.get(estado, 0) + 1
        
        # Agrupar avances por estado
        if estado not in avances_por_estado:
            avances_por_estado[estado] = []
        avances_por_estado[estado].append(avance_obra)
    
    print("\n" + "="*80)
    print("DISTRIBUCIÓN DE ESTADOS")
    print("="*80)
    for estado, count in sorted(estados.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(features)) * 100
        print(f"  {estado}: {count} ({percentage:.1f}%)")
    
    # Verificar la regla de negocio
    print("\n" + "="*80)
    print("VERIFICACIÓN DE REGLA DE NEGOCIO")
    print("="*80)
    print("Regla: Cuando avance_obra = 0, estado debe ser 'En Alistamiento'")
    
    casos_correctos = 0
    casos_incorrectos = []
    
    for feature in features:
        props = feature.get('properties', {})
        avance_obra = props.get('avance_obra')
        estado = props.get('estado')
        upid = props.get('upid')
        
        # Verificar si avance_obra es 0
        if avance_obra is not None and avance_obra == 0.0:
            if estado == 'En Alistamiento':
                casos_correctos += 1
            else:
                casos_incorrectos.append({
                    'upid': upid,
                    'avance_obra': avance_obra,
                    'estado': estado
                })
    
    print(f"\n✅ Casos correctos (avance_obra=0 y estado='En Alistamiento'): {casos_correctos}")
    print(f"❌ Casos incorrectos: {len(casos_incorrectos)}")
    
    if len(casos_incorrectos) > 0:
        print("\n⚠️ CASOS INCORRECTOS ENCONTRADOS:")
        for i, caso in enumerate(casos_incorrectos[:10], 1):
            print(f"  {i}. UPID: {caso['upid']}, Avance: {caso['avance_obra']}, Estado: '{caso['estado']}'")
    else:
        print("\n🎉 ¡PERFECTO! Todos los registros con avance_obra=0 tienen estado='En Alistamiento'")
    
    # Análisis de avances por estado
    print("\n" + "="*80)
    print("ANÁLISIS DE AVANCES POR ESTADO")
    print("="*80)
    
    for estado in sorted(avances_por_estado.keys()):
        avances = avances_por_estado[estado]
        min_avance = min(avances) if avances else None
        max_avance = max(avances) if avances else None
        avg_avance = sum([a for a in avances if a is not None]) / len([a for a in avances if a is not None]) if avances else 0
        
        print(f"\n{estado}:")
        print(f"  - Cantidad: {len(avances)}")
        print(f"  - Avance mínimo: {min_avance}")
        print(f"  - Avance máximo: {max_avance}")
        print(f"  - Avance promedio: {avg_avance:.2f}")
    
    # Verificar coherencia: proyectos en ejecución no deberían tener avance 0
    print("\n" + "="*80)
    print("VERIFICACIÓN DE COHERENCIA")
    print("="*80)
    
    incoherencias = []
    for feature in features:
        props = feature.get('properties', {})
        avance_obra = props.get('avance_obra')
        estado = props.get('estado')
        upid = props.get('upid')
        
        # Caso incoherente: En Ejecución con avance 0
        if estado == 'En Ejecución' and avance_obra == 0.0:
            incoherencias.append({
                'upid': upid,
                'problema': 'En Ejecución con avance 0',
                'avance_obra': avance_obra,
                'estado': estado
            })
        
        # Caso incoherente: Terminado sin avance 100
        if estado == 'Terminado' and avance_obra != 100.0:
            incoherencias.append({
                'upid': upid,
                'problema': 'Terminado pero avance != 100',
                'avance_obra': avance_obra,
                'estado': estado
            })
    
    if len(incoherencias) > 0:
        print(f"\n⚠️ Se encontraron {len(incoherencias)} posibles incoherencias:")
        for i, caso in enumerate(incoherencias[:10], 1):
            print(f"  {i}. UPID: {caso['upid']}")
            print(f"     Problema: {caso['problema']}")
            print(f"     Avance: {caso['avance_obra']}, Estado: {caso['estado']}")
    else:
        print("\n✅ No se encontraron incoherencias obvias")
    
    print("\n" + "="*80)
    print("RESUMEN FINAL")
    print("="*80)
    print(f"✓ Total de registros procesados: {len(features)}")
    print(f"✓ Regla de negocio aplicada correctamente: {casos_correctos} casos")
    print(f"✓ Estados normalizados: {len(estados)} tipos diferentes")
    print(f"✓ Calidad de datos: {'EXCELENTE' if len(casos_incorrectos) == 0 else 'NECESITA REVISIÓN'}")
    print("="*80)
    
else:
    print(f"❌ Archivo no encontrado: {geojson_path}")
