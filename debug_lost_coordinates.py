# -*- coding: utf-8 -*-
"""
Script para identificar dónde se pierden las coordenadas
"""
import sys
import os
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_extraction_data():
    """Verifica las coordenadas en el archivo extraído"""
    extraction_file = "unidades_proyecto.json"
    
    if not os.path.exists(extraction_file):
        print(f"[ERROR] No existe {extraction_file}")
        return
    
    with open(extraction_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    total = len(data)
    with_lat = sum(1 for r in data if r.get('lat') is not None and r.get('lat') != '')
    with_lon = sum(1 for r in data if r.get('lon') is not None and r.get('lon') != '')
    
    print(f"\n[EXTRACTION] unidades_proyecto.json:")
    print(f"  Total registros: {total}")
    print(f"  Con lat: {with_lat} ({with_lat*100/total:.1f}%)")
    print(f"  Con lon: {with_lon} ({with_lon*100/total:.1f}%)")
    
    # Ejemplos sin coordenadas
    without_coords = [r for r in data if not r.get('lat') or not r.get('lon')]
    if without_coords:
        print(f"\n  [DETAIL] Ejemplos sin coordenadas ({len(without_coords)} registros):")
        for r in without_coords[:5]:
            print(f"    - {r.get('nombre_corto', 'N/A')} (Centro: {r.get('nombre_centro_gestor', 'N/A')})")
            print(f"      lat: {r.get('lat')}, lon: {r.get('lon')}")

def check_transformed_data():
    """Verifica las coordenadas en el archivo transformado"""
    transformed_file = "app_outputs/unidades_proyecto_transformed.geojson"
    
    if not os.path.exists(transformed_file):
        print(f"[WARNING] No existe {transformed_file} (fue eliminado por el pipeline)")
        return
    
    with open(transformed_file, 'r', encoding='utf-8') as f:
        geojson = json.load(f)
    
    features = geojson.get('features', [])
    total = len(features)
    with_geom = sum(1 for f in features if f.get('geometry') is not None)
    
    print(f"\n[TRANSFORMATION] unidades_proyecto_transformed.geojson:")
    print(f"  Total features: {total}")
    print(f"  Con geometry: {with_geom} ({with_geom*100/total:.1f}%)")
    print(f"  Sin geometry: {total - with_geom}")
    
    # Ejemplos sin geometry
    without_geom = [f for f in features if f.get('geometry') is None]
    if without_geom:
        print(f"\n  [DETAIL] Ejemplos sin geometry ({len(without_geom)} registros):")
        for f in without_geom[:5]:
            props = f.get('properties', {})
            print(f"    - UPID: {props.get('upid', 'N/A')}")
            print(f"      Nombre: {props.get('nombre_corto', 'N/A')}")
            print(f"      Centro: {props.get('nombre_centro_gestor', 'N/A')}")
            print(f"      lat: {props.get('lat')}, lon: {props.get('lon')}")

if __name__ == "__main__":
    print("="*80)
    print("DIAGNÓSTICO DE COORDENADAS PERDIDAS")
    print("="*80)
    
    check_extraction_data()
    check_transformed_data()
    
    print("\n" + "="*80)
