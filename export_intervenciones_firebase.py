# -*- coding: utf-8 -*-
"""
Script para exportar intervenciones de Firebase a Excel con columnas específicas.

Descarga los datos de unidades_proyecto desde Firebase, expande las intervenciones
y exporta a Excel con las columnas requeridas.
"""

import pandas as pd
from database.config import get_firestore_client
from datetime import datetime
import sys


def descargar_intervenciones_firebase(collection_name="unidades_proyecto"):
    """
    Descarga intervenciones de Firebase y las expande en un DataFrame.
    
    Returns:
        DataFrame con una fila por intervención
    """
    print(f"🔄 Conectando a Firebase colección '{collection_name}'...")
    
    try:
        db = get_firestore_client()
        if not db:
            print("❌ No se pudo conectar a Firebase")
            return None
        
        collection_ref = db.collection(collection_name)
        docs = list(collection_ref.stream())
        
        print(f"✅ Descargados {len(docs)} documentos de Firebase")
        
        if not docs:
            print("⚠️ No se encontraron documentos en la colección")
            return None
        
        # Expandir intervenciones
        intervenciones_list = []
        docs_con_array = 0
        docs_sin_array = 0
        total_intervenciones = 0
        
        for doc in docs:
            data = doc.to_dict()
            
            # Extraer coordenadas con manejo de None
            lat = None
            lon = None
            geometry = data.get('geometry', {})
            if geometry and isinstance(geometry, dict):
                coordinates = geometry.get('coordinates', [None, None])
                if coordinates and len(coordinates) >= 2:
                    # Asumir formato [lat, lon] como está configurado en el sistema
                    lat = coordinates[0]
                    lon = coordinates[1]
            
            # Datos a nivel de unidad (aplican a todas las intervenciones)
            unidad_data = {
                'upid': data.get('upid'),
                'nombre_up': data.get('nombre_up'),
                'nombre_up_detalle': data.get('nombre_up_detalle'),
                'direccion': data.get('direccion'),
                'comuna_corregimiento': data.get('comuna_corregimiento'),
                'barrio_vereda': data.get('barrio_vereda'),
                'identificador': data.get('identificador'),
                'tipo_equipamiento': data.get('tipo_equipamiento'),
                'clase_up': data.get('clase_up'),
                'nombre_centro_gestor': data.get('nombre_centro_gestor'),
                'lat': lat,
                'lon': lon
            }
            
            # Expandir intervenciones del array
            intervenciones = data.get('intervenciones', [])
            
            # Verificar si hay array de intervenciones
            if intervenciones and isinstance(intervenciones, list) and len(intervenciones) > 0:
                # CASO 1: Hay array de intervenciones - expandir cada una
                docs_con_array += 1
                for interv in intervenciones:
                    if not isinstance(interv, dict):
                        continue
                    
                    total_intervenciones += 1
                    interv_data = unidad_data.copy()
                    
                    # Agregar TODAS las variables de la intervención
                    for key, value in interv.items():
                        # Manejar el campo 'ano' que se exporta como 'año'
                        if key == 'ano':
                            interv_data['año'] = value
                        else:
                            interv_data[key] = value
                    
                    intervenciones_list.append(interv_data)
            else:
                # CASO 2: No hay array de intervenciones - usar datos directos (estructura antigua)
                docs_sin_array += 1
                interv_data = unidad_data.copy()
                interv_data.update({
                    'referencia_proceso': data.get('referencia_proceso'),
                    'referencia_contrato': data.get('referencia_contrato'),
                    'bpin': data.get('bpin'),
                    'fuente_financiacion': data.get('fuente_financiacion'),
                    'tipo_intervencion': data.get('tipo_intervencion'),
                    'estado': data.get('estado'),
                    'presupuesto_base': data.get('presupuesto_base'),
                    'avance_obra': data.get('avance_obra'),
                    'año': data.get('ano') or data.get('año'),
                    'fecha_inicio': data.get('fecha_inicio'),
                    'fecha_fin': data.get('fecha_fin'),
                    'url_proceso': data.get('url_proceso'),
                    'descripcion_intervencion': data.get('descripcion_intervencion'),
                    'unidad': data.get('unidad'),
                    'cantidad': data.get('cantidad'),
                    'plataforma': data.get('plataforma')
                })
                intervenciones_list.append(interv_data)
        
        df = pd.DataFrame(intervenciones_list)
        
        print(f"✅ Estructura de datos analizada:")
        print(f"   • Documentos con array de intervenciones: {docs_con_array}")
        print(f"   • Documentos sin array (estructura directa): {docs_sin_array}")
        print(f"   • Total intervenciones expandidas: {len(df)}")
        if docs_con_array > 0:
            print(f"   • Promedio intervenciones/unidad: {total_intervenciones/docs_con_array:.2f}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error descargando datos de Firebase: {e}")
        import traceback
        traceback.print_exc()
        return None


def exportar_intervenciones(df, output_path="intervenciones_export.xlsx"):
    """
    Exporta DataFrame con TODAS las columnas a Excel.
    
    Args:
        df: DataFrame con datos de intervenciones
        output_path: Ruta del archivo de salida
    """
    # Verificar todas las columnas disponibles
    print(f"\n📋 Columnas disponibles en los datos: {len(df.columns)}")
    print(f"   {', '.join(sorted(df.columns))}")
    
    # Crear DataFrame de exportación con todas las columnas
    df_export = df.copy()
    
    # Convertir listas a strings (para campos como referencia_proceso que pueden ser arrays)
    for col in df_export.columns:
        if df_export[col].dtype == 'object':
            df_export[col] = df_export[col].apply(
                lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x
            )
    
    # Exportar a Excel
    try:
        df_export.to_excel(output_path, index=False, engine='openpyxl')
        print(f"\n✅ Datos exportados exitosamente:")
        print(f"   📄 Archivo: {output_path}")
        print(f"   📊 Filas: {len(df_export)}")
        print(f"   📋 Columnas: {len(df_export.columns)}")
        
        # Mostrar resumen estadístico
        print(f"\n📈 Resumen:")
        print(f"   • Total intervenciones: {len(df_export)}")
        print(f"   • Intervenciones con geometría: {df_export[['lat', 'lon']].notna().all(axis=1).sum()}")
        if 'estado' in df_export.columns:
            print(f"\n   Estados:")
            estados_count = df_export['estado'].value_counts()
            for estado, count in estados_count.items():
                print(f"      - {estado}: {count}")
        
        # Mostrar columnas exportadas
        print(f"\n📋 Columnas exportadas ({len(df_export.columns)}):")
        for i, col in enumerate(sorted(df_export.columns), 1):
            print(f"   {i:2d}. {col}")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Error exportando a Excel: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """Función principal."""
    print("=" * 80)
    print("🚀 EXPORTACIÓN DE INTERVENCIONES DESDE FIREBASE")
    print("=" * 80)
    print()
    
    # Descargar datos de Firebase
    df = descargar_intervenciones_firebase()
    
    if df is None or df.empty:
        print("❌ No se pudieron descargar los datos")
        return False
    
    # Generar nombre de archivo con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"intervenciones_export_{timestamp}.xlsx"
    
    # Exportar a Excel
    result = exportar_intervenciones(df, output_path)
    
    if result:
        print(f"\n🎉 Exportación completada exitosamente!")
        print(f"📂 Archivo generado: {result}")
        return True
    else:
        print(f"\n❌ La exportación falló")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
