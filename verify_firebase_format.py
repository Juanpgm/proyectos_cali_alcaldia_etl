#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Verificar formato de fechas y geometrías en Firebase
"""

import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client

def verify_firebase_format():
    """Verificar formato de fechas y geometrías en Firebase"""
    
    print("🔍 Conectando a Firebase...")
    db = get_firestore_client()
    
    if not db:
        print("❌ No se pudo conectar a Firebase")
        return
    
    print("✅ Conectado a Firebase")
    print("\n📊 Verificando formato de datos...\n")
    
    # Obtener algunos documentos de ejemplo
    docs_ref = db.collection('unidades_proyecto').limit(5)
    docs = docs_ref.stream()
    
    count = 0
    for doc in docs:
        count += 1
        data = doc.to_dict()
        
        print(f"{'='*80}")
        print(f"Documento #{count}: {doc.id}")
        print(f"{'='*80}")
        
        # Verificar fechas
        print("\n📅 FECHAS:")
        fecha_inicio = data.get('fecha_inicio_std')
        fecha_fin = data.get('fecha_fin_std')
        
        if fecha_inicio:
            print(f"  ✓ fecha_inicio_std: {fecha_inicio}")
            print(f"    Tipo: {type(fecha_inicio)}")
            print(f"    Formato correcto: {'✅ SÍ' if 'T' not in str(fecha_inicio) and ' 00:00:00' not in str(fecha_inicio) else '❌ NO (contiene timestamp)'}")
        else:
            print(f"  ⚠️ fecha_inicio_std: None")
        
        if fecha_fin:
            print(f"  ✓ fecha_fin_std: {fecha_fin}")
            print(f"    Tipo: {type(fecha_fin)}")
            print(f"    Formato correcto: {'✅ SÍ' if 'T' not in str(fecha_fin) and ' 00:00:00' not in str(fecha_fin) else '❌ NO (contiene timestamp)'}")
        else:
            print(f"  ⚠️ fecha_fin_std: None")
        
        # Verificar geometría
        print("\n📍 GEOMETRÍA:")
        geometry = data.get('geometry')
        
        if geometry:
            geom_type = geometry.get('type') if isinstance(geometry, dict) else None
            coordinates = geometry.get('coordinates') if isinstance(geometry, dict) else None
            
            print(f"  ✓ Tipo: {geom_type}")
            print(f"  ✓ Coordenadas: {coordinates}")
            
            if coordinates and len(coordinates) == 2:
                lon, lat = coordinates
                print(f"    - Longitud (lon): {lon}")
                print(f"    - Latitud (lat): {lat}")
                
                # Validar que sean coordenadas de Cali
                # Cali está en: lat ~3.4°N, lon ~76.5°W (negativo)
                if -78 < lon < -75 and 2 < lat < 5:
                    print(f"    ✅ Formato correcto: [lon, lat] - GeoJSON estándar")
                elif 2 < lon < 5 and -78 < lat < -75:
                    print(f"    ❌ Formato incorrecto: [lat, lon] - Debería ser [lon, lat]")
                else:
                    print(f"    ⚠️ Coordenadas fuera del rango de Cali")
            else:
                print(f"    ⚠️ Coordenadas inválidas o ausentes")
        else:
            print(f"  ⚠️ geometry: None")
        
        # Verificar ubicación
        print("\n🗺️ UBICACIÓN:")
        print(f"  Comuna: {data.get('comuna_corregimiento')}")
        print(f"  Barrio: {data.get('barrio_vereda')}")
        
        print()
    
    if count == 0:
        print("⚠️ No se encontraron documentos en la colección")
    else:
        print(f"✅ Verificación completada para {count} documentos")

if __name__ == "__main__":
    verify_firebase_format()
