# -*- coding: utf-8 -*-
"""
Script para diagnosticar y corregir el problema de geometría inválida en UPIDs.
"""

import os
import sys
import json
from database.config import get_firestore_client

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def diagnose_upid_issue():
    """Diagnostica el problema con un UPID específico."""
    
    db = get_firestore_client()
    if not db:
        print("❌ No se pudo conectar a Firebase")
        return
    
    # Verificar un UPID específico
    upid = 'UNP-11'
    doc_ref = db.collection('unidades_proyecto').document(upid)
    doc = doc_ref.get()
    
    if not doc.exists:
        print(f"❌ {upid} no existe en Firebase")
        return
    
    data = doc.to_dict()
    
    print(f"📄 Diagnóstico de {upid}:\n")
    print(f"Has geometry: {data.get('has_geometry')}")
    print(f"Geometry type: {data.get('geometry_type')}")
    print(f"Geometry: {data.get('geometry')}")
    print(f"\nCampos a nivel raíz (primeros 10):")
    root_fields = [k for k in data.keys() if k not in ['geometry', 'properties', '_hash', 'has_geometry', 'geometry_type']]
    for field in root_fields[:10]:
        print(f"  - {field}: {data[field]}")
    
    print(f"\n¿Tiene campo 'properties'? {('properties' in data)}")
    if 'properties' in data:
        props = data.get('properties', {})
        print(f"Contenido de 'properties': {props}")
        print(f"Tipo: {type(props)}")
        print(f"Es dict vacío: {props == {}}")
    
    print(f"\n¿Tiene campo 'nombre_up'? {('nombre_up' in data)}")
    if 'nombre_up' in data:
        print(f"Valor de nombre_up: {data['nombre_up']}")
    
    print(f"\n¿Tiene campo 'identificador'? {('identificador' in data)}")
    if 'identificador' in data:
        print(f"Valor de identificador: {data['identificador']}")
    
    return data

def check_source_geojson():
    """Verifica el archivo GeoJSON fuente."""
    
    geojson_path = "context/unidades_proyecto.geojson"
    
    if not os.path.exists(geojson_path):
        print(f"❌ No existe el archivo: {geojson_path}")
        return
    
    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    features = data.get('features', [])
    print(f"\n📁 Archivo GeoJSON fuente:")
    print(f"Total features: {len(features)}")
    
    # Buscar UNP-11 en el GeoJSON
    upid_to_find = 'UNP-11'
    found = False
    
    for feature in features:
        props = feature.get('properties', {})
        
        # Buscar por diferentes campos de identificación
        for id_field in ['upid', 'identificador', 'nombre_up']:
            if id_field in props and upid_to_find in str(props.get(id_field, '')):
                found = True
                print(f"\n✓ Encontrado {upid_to_find} en el GeoJSON:")
                print(f"  Campo de búsqueda: {id_field}")
                print(f"  Valor: {props.get(id_field)}")
                print(f"  Has geometry: {feature.get('geometry') is not None}")
                if feature.get('geometry'):
                    print(f"  Geometry type: {feature['geometry'].get('type')}")
                print(f"  Propiedades (primeras 5):")
                for i, (k, v) in enumerate(list(props.items())[:5]):
                    print(f"    - {k}: {v}")
                break
        
        if found:
            break
    
    if not found:
        print(f"\n❌ No se encontró {upid_to_find} en el GeoJSON")
        
        # Mostrar muestra de identificadores
        print(f"\nMuestra de identificadores en el GeoJSON:")
        sample_ids = []
        for feature in features[:20]:
            props = feature.get('properties', {})
            for id_field in ['upid', 'identificador']:
                if id_field in props:
                    sample_ids.append(f"{id_field}: {props[id_field]}")
                    break
        for sid in sample_ids[:10]:
            print(f"  - {sid}")

if __name__ == "__main__":
    print("="*60)
    print("DIAGNÓSTICO DE PROBLEMA DE GEOMETRÍA")
    print("="*60)
    
    firebase_data = diagnose_upid_issue()
    check_source_geojson()
    
    print("\n" + "="*60)
    print("CONCLUSIÓN")
    print("="*60)
    
    if firebase_data:
        has_properties_field = 'properties' in firebase_data
        properties_empty = firebase_data.get('properties', {}) == {}
        has_root_fields = any(k not in ['geometry', 'properties', '_hash', 'has_geometry', 'geometry_type', 'created_at', 'updated_at'] 
                              for k in firebase_data.keys())
        
        print(f"\nEstructura en Firebase:")
        print(f"  ✓ Tiene campo 'properties': {has_properties_field}")
        if has_properties_field:
            print(f"  ✓ Campo 'properties' está vacío: {properties_empty}")
        print(f"  ✓ Tiene campos en la raíz: {has_root_fields}")
        
        if has_properties_field and properties_empty:
            print(f"\n⚠️  PROBLEMA IDENTIFICADO:")
            print(f"     Los documentos tienen un campo 'properties' vacío {{}}.")
            print(f"     Esto puede estar causando problemas con el frontend/queries.")
            print(f"     Solución: Eliminar el campo 'properties' vacío o llenarlo correctamente.")
