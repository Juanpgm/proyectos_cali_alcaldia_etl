#!/usr/bin/env python3
"""
Debug: Verificar coordenadas en DataFrame después del clustering
"""
import pandas as pd
import sys
import os

# Agregar directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extraction_app.data_extraction_unidades_proyecto import extract_unidades_proyecto_data
from transformation_app.geospatial_clustering import agrupar_datos_geoespacial

def main():
    print("\n🔍 DEBUG: Coordenadas después del clustering\n")
    
    # Extraer datos
    print("Extrayendo datos...")
    df_raw = extract_unidades_proyecto_data()
    print(f"Total registros extraídos: {len(df_raw)}\n")
    
    # Ejecutar clustering
    print("Ejecutando clustering...")
    unidades_dict = agrupar_datos_geoespacial(df_raw)
    
    # Analizar unidades problemáticas
    problematic_upids = ['UNP-1', 'UNP-7', 'UNP-11', 'UNP-13', 'UNP-27']
    
    print(f"\n{'='*80}")
    print("🔍 ANÁLISIS DE UNIDADES PROBLEMÁTICAS")
    print(f"{'='*80}\n")
    
    for upid in problematic_upids:
        if upid in unidades_dict:
            unidad = unidades_dict[upid]
            lat = unidad.get('lat')
            lon = unidad.get('lon')
            n_intervenciones = unidad.get('n_intervenciones', 0)
            nombre = unidad.get('nombre_up', 'N/A')[:60]
            
            print(f"{upid}: {nombre}")
            print(f"   • Intervenciones: {n_intervenciones}")
            print(f"   • Coordenadas: lat={lat}, lon={lon}")
            
            # Verificar intervenciones individuales
            intervenciones = unidad.get('intervenciones', [])
            if len(intervenciones) > 0:
                print(f"   • Muestra primera intervención:")
                primera = intervenciones[0]
                print(f"      - intervencion_id: {primera.get('intervencion_id', 'N/A')}")
                print(f"      - estado: {primera.get('estado', 'N/A')}")
            print()
    
    # Estadísticas generales
    print(f"{'='*80}")
    print("📊 ESTADÍSTICAS GENERALES")
    print(f"{'='*80}\n")
    
    total_unidades = len(unidades_dict)
    con_coords = sum(1 for u in unidades_dict.values() if u.get('lat') is not None and u.get('lon') is not None)
    sin_coords = total_unidades - con_coords
    
    print(f"Total unidades: {total_unidades}")
    print(f"Con coordenadas: {con_coords} ({con_coords/total_unidades*100:.1f}%)")
    print(f"Sin coordenadas: {sin_coords} ({sin_coords/total_unidades*100:.1f}%)")
    
    # Contar unidades agrupadas
    agrupadas = sum(1 for u in unidades_dict.values() if u.get('n_intervenciones', 1) > 1)
    agrupadas_sin_coords = sum(
        1 for u in unidades_dict.values()
        if u.get('n_intervenciones', 1) > 1 and (u.get('lat') is None or u.get('lon') is None)
    )
    
    print(f"\nUnidades agrupadas (>1 intervención): {agrupadas}")
    print(f"Agrupadas sin coordenadas: {agrupadas_sin_coords}")

if __name__ == '__main__':
    main()
