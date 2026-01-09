# -*- coding: utf-8 -*-
"""
Diagnóstico para entender por qué tipo_equipamiento aparece como NaN
en los registros de Secretaría de Salud Pública
"""

import json
import sys
import os
from pathlib import Path

# Agregar path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def diagnosticar():
    """Diagnostica los datos de Salud Pública en diferentes etapas"""
    print("="*80)
    print("DIAGNÓSTICO: tipo_equipamiento en Secretaría de Salud Pública")
    print("="*80)
    
    # 1. Revisar el archivo JSON de unidades_proyecto (no agrupado)
    output_dir = Path(__file__).parent / 'app_outputs'
    
    # Buscar archivo unidades_proyecto.geojson más reciente
    geojson_files = sorted(output_dir.glob('**/unidades_proyecto*.geojson'), reverse=True)
    
    if geojson_files:
        geojson_file = geojson_files[0]
        print(f"\n📂 Analizando GeoJSON: {geojson_file.name}")
        
        with open(geojson_file, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        features = geojson_data.get('features', [])
        
        # Filtrar Secretaría de Salud Pública
        salud_features = [
            f for f in features
            if f.get('properties', {}).get('nombre_centro_gestor') == 'Secretaría de Salud Pública'
        ]
        
        print(f"\n📊 Registros Secretaría de Salud Pública en GeoJSON: {len(salud_features)}")
        
        if salud_features:
            print("\n🔍 Primeros 3 ejemplos:")
            for i, feature in enumerate(salud_features[:3], 1):
                props = feature.get('properties', {})
                print(f"\n   Ejemplo {i}:")
                print(f"      nombre_up: {props.get('nombre_up', 'N/A')}")
                print(f"      clase_up: {props.get('clase_up', 'N/A')}")
                print(f"      tipo_equipamiento: {props.get('tipo_equipamiento', 'N/A')}")
                print(f"      estado: {props.get('estado', 'N/A')}")
                print(f"      tipo_intervencion: {props.get('tipo_intervencion', 'N/A')}")
                print(f"      frente_activo: {props.get('frente_activo', 'N/A')}")
            
            # Análisis estadístico
            tipos_equip = {}
            clases = {}
            for f in salud_features:
                props = f.get('properties', {})
                tipo = props.get('tipo_equipamiento', 'N/A')
                clase = props.get('clase_up', 'N/A')
                
                tipos_equip[tipo] = tipos_equip.get(tipo, 0) + 1
                clases[clase] = clases.get(clase, 0) + 1
            
            print("\n📊 Distribución tipo_equipamiento:")
            for tipo, count in sorted(tipos_equip.items()):
                print(f"   - {tipo}: {count}")
            
            print("\n📊 Distribución clase_up:")
            for clase, count in sorted(clases.items()):
                print(f"   - {clase}: {count}")
    
    # 2. Revisar archivo grouped_structure (agrupado)
    grouped_files = sorted((output_dir / 'grouped_structure').glob('unidades_proyecto_*.json'), reverse=True)
    
    if grouped_files:
        grouped_file = grouped_files[0]
        print(f"\n\n📂 Analizando Grouped Structure: {grouped_file.name}")
        
        with open(grouped_file, 'r', encoding='utf-8') as f:
            grouped_data = json.load(f)
        
        salud_grouped = [
            d for d in grouped_data
            if d.get('nombre_centro_gestor') == 'Secretaría de Salud Pública'
        ]
        
        print(f"\n📊 Registros Secretaría de Salud Pública en Grouped: {len(salud_grouped)}")
        
        if salud_grouped:
            # Análisis estadístico
            tipos_equip = {}
            clases = {}
            estados = {}
            frentes = {}
            
            for d in salud_grouped:
                tipo = d.get('tipo_equipamiento', 'N/A')
                clase = d.get('clase_up', 'N/A')
                estado = d.get('estado', 'N/A')
                frente = d.get('frente_activo', 'N/A')
                
                tipos_equip[str(tipo)] = tipos_equip.get(str(tipo), 0) + 1
                clases[str(clase)] = clases.get(str(clase), 0) + 1
                estados[str(estado)] = estados.get(str(estado), 0) + 1
                frentes[str(frente)] = frentes.get(str(frente), 0) + 1
            
            print("\n📊 Distribución tipo_equipamiento:")
            for tipo, count in sorted(tipos_equip.items()):
                print(f"   - {tipo}: {count}")
            
            print("\n📊 Distribución clase_up:")
            for clase, count in sorted(clases.items()):
                print(f"   - {clase}: {count}")
            
            print("\n📊 Distribución estado:")
            for estado, count in sorted(estados.items()):
                print(f"   - {estado}: {count}")
            
            print("\n📊 Distribución frente_activo:")
            for frente, count in sorted(frentes.items()):
                print(f"   - {frente}: {count}")
    
    print("\n" + "="*80)
    print("CONCLUSIÓN")
    print("="*80)
    print("""
Si tipo_equipamiento aparece como 'N/A' o 'nan' en ambos archivos, significa que:
1. Los datos de origen (Google Sheets) NO contienen ese valor
2. O la columna tiene un nombre diferente en el Excel
3. O el valor está vacío en la fuente original

Si aparece como 'IPS' en el GeoJSON pero 'nan' en grouped_structure,
significa que se pierde durante la agrupación geoespacial.
    """)


if __name__ == '__main__':
    diagnosticar()
