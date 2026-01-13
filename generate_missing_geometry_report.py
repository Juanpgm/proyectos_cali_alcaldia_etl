# -*- coding: utf-8 -*-
"""
Script para generar reporte detallado de UNPs sin geometry.

Este script genera un reporte en Excel con todos los UNPs sin geometry,
incluyendo direcciones y otra información que pueda ayudar a geocodificar.
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client


def generate_missing_geometry_report(collection_name: str = "unidades_proyecto"):
    """
    Genera un reporte Excel de UNPs sin geometry.
    
    Args:
        collection_name: Nombre de la colección en Firebase
    """
    print(f"\n{'='*80}")
    print(f"GENERANDO REPORTE DE UNPs SIN GEOMETRY")
    print(f"{'='*80}\n")
    
    try:
        db = get_firestore_client()
        if not db:
            print("[ERROR] No se pudo conectar a Firebase")
            return
        
        collection_ref = db.collection(collection_name)
        
        # Obtener todos los documentos
        print("📥 Obteniendo documentos de Firebase...")
        docs = list(collection_ref.stream())
        print(f"   Total documentos: {len(docs)}\n")
        
        # Recopilar información de registros sin geometry
        registros_sin_geometry = []
        
        for doc in docs:
            data = doc.to_dict()
            
            geometry = data.get('geometry')
            lat = data.get('lat')
            lon = data.get('lon')
            
            # Verificar si tiene geometry válida
            has_valid_geometry = (
                geometry is not None and 
                isinstance(geometry, dict) and
                geometry.get('type') == 'Point' and
                geometry.get('coordinates') is not None and
                len(geometry.get('coordinates', [])) == 2
            )
            
            # Si no tiene geometry válida, agregar al reporte
            if not has_valid_geometry:
                registro = {
                    'upid': data.get('upid'),
                    'nombre_up': data.get('nombre_up'),
                    'nombre_up_detalle': data.get('nombre_up_detalle'),
                    'direccion': data.get('direccion'),
                    'barrio_vereda': data.get('barrio_vereda'),
                    'comuna_corregimiento': data.get('comuna_corregimiento'),
                    'tipo_equipamiento': data.get('tipo_equipamiento'),
                    'clase_up': data.get('clase_up'),
                    'nombre_centro_gestor': data.get('nombre_centro_gestor'),
                    'n_intervenciones': data.get('n_intervenciones', 0),
                    'presupuesto_base': data.get('presupuesto_base', 0),
                    'avance_obra': data.get('avance_obra', 0),
                    'lat': lat,
                    'lon': lon,
                    'has_lat_lon': 'Sí' if (lat is not None and lon is not None) else 'No',
                    'tiene_direccion': 'Sí' if data.get('direccion') else 'No'
                }
                
                # Agregar información de primera intervención
                intervenciones = data.get('intervenciones', [])
                if intervenciones and len(intervenciones) > 0:
                    primera_interv = intervenciones[0]
                    registro['estado'] = primera_interv.get('estado')
                    registro['tipo_intervencion'] = primera_interv.get('tipo_intervencion')
                    registro['ano'] = primera_interv.get('ano')
                    registro['referencia_proceso'] = str(primera_interv.get('referencia_proceso', ''))
                else:
                    registro['estado'] = None
                    registro['tipo_intervencion'] = None
                    registro['ano'] = None
                    registro['referencia_proceso'] = None
                
                registros_sin_geometry.append(registro)
        
        # Crear DataFrame
        df = pd.DataFrame(registros_sin_geometry)
        
        # Ordenar por UPID
        df = df.sort_values('upid')
        
        # Generar reporte
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'reporte_unidades_sin_geometry_{timestamp}.xlsx'
        
        print(f"💾 Generando reporte Excel...")
        
        # Crear Excel con formato
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='UNPs sin Geometry', index=False)
            
            # Ajustar ancho de columnas
            worksheet = writer.sheets['UNPs sin Geometry']
            for idx, col in enumerate(df.columns, 1):
                max_length = max(
                    df[col].astype(str).apply(len).max(),
                    len(col)
                ) + 2
                worksheet.column_dimensions[chr(64 + idx)].width = min(max_length, 50)
        
        print(f"   ✅ Reporte generado: {output_file}")
        print(f"\n📊 RESUMEN:")
        print(f"   Total UNPs sin geometry: {len(df)}")
        print(f"   Con dirección: {df['tiene_direccion'].value_counts().get('Sí', 0)}")
        print(f"   Sin dirección: {df['tiene_direccion'].value_counts().get('No', 0)}")
        print(f"\n   Por Centro Gestor:")
        
        if 'nombre_centro_gestor' in df.columns:
            centro_counts = df['nombre_centro_gestor'].value_counts().head(10)
            for centro, count in centro_counts.items():
                print(f"      - {centro}: {count}")
        
        print(f"\n{'='*80}")
        
        return output_file
        
    except Exception as e:
        print(f"[ERROR] Error generando reporte: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    output_file = generate_missing_geometry_report()
    
    if output_file:
        print(f"\n✅ Reporte generado exitosamente: {output_file}")
        print(f"\nPuedes abrir el archivo para revisar los UNPs sin coordenadas.")
        print(f"Considera geocodificar las direcciones disponibles.")
