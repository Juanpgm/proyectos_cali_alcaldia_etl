# -*- coding: utf-8 -*-
"""
Script para analizar y verificar la lógica de agrupación de unidades de proyecto e intervenciones.
Detecta problemas en la generación de UPIDs, consolidación de datos y agrupación.
"""

import os
import sys
import pandas as pd

# Agregar rutas necesarias al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client


def analyze_grouping_logic(collection_name: str = "unidades_proyecto"):
    """
    Analiza la lógica de agrupación de unidades e intervenciones desde Firebase.
    
    Args:
        collection_name: Nombre de la colección en Firebase
    """
    try:
        print(f"\n{'='*80}")
        print("[DATA] ANÁLISIS DE AGRUPACIÓN DE UNIDADES E INTERVENCIONES")
        print("="*80)
        
        db = get_firestore_client()
        if not db:
            print("[ERROR] No se pudo conectar a Firebase")
            return
        
        # Obtener todos los documentos
        print("\n[SYNC] Obteniendo documentos desde Firebase...")
        docs = list(db.collection(collection_name).stream())
        
        if not docs:
            print("[WARNING] No hay documentos en Firebase")
            return
        
        print(f"[OK] Obtenidos {len(docs)} documentos (unidades de proyecto)")
        
        # Estructuras para análisis
        unidades_data = []
        intervenciones_data = []
        problemas = []
        
        print("\n[SYNC] Analizando estructura de agrupación...")
        
        # Analizar cada unidad
        for doc in docs:
            upid = doc.id
            data = doc.to_dict()
            
            # Verificar campos críticos de unidad
            unidad_info = {
                'upid': upid,
                'nombre_up': data.get('nombre_up'),
                'nombre_centro_gestor': data.get('nombre_centro_gestor'),
                'clase_up': data.get('clase_up'),
                'tipo_equipamiento': data.get('tipo_equipamiento'),
                'n_intervenciones': data.get('n_intervenciones'),
                'has_geometry': data.get('has_geometry'),
                'geometry': data.get('geometry'),
                'lat': data.get('lat'),
                'lon': data.get('lon')
            }
            
            # Verificar intervenciones
            intervenciones = data.get('intervenciones', [])
            
            # Problema 1: n_intervenciones no coincide con array real
            if isinstance(intervenciones, list):
                n_real = len(intervenciones)
                n_declarado = data.get('n_intervenciones', 0)
                
                if n_real != n_declarado:
                    problemas.append({
                        'tipo': 'DESINCRONIZACIÓN',
                        'upid': upid,
                        'descripcion': f"n_intervenciones={n_declarado} pero array tiene {n_real} elementos",
                        'severidad': 'ALTA'
                    })
                
                unidad_info['n_intervenciones_real'] = n_real
                
                # Analizar cada intervención
                for idx, interv in enumerate(intervenciones, 1):
                    if not isinstance(interv, dict):
                        problemas.append({
                            'tipo': 'ESTRUCTURA_INVÁLIDA',
                            'upid': upid,
                            'descripcion': f"Intervención {idx} no es un diccionario",
                            'severidad': 'ALTA'
                        })
                        continue
                    
                    interv_info = {
                        'upid': upid,
                        'intervencion_id': interv.get('intervencion_id'),
                        'estado': interv.get('estado'),
                        'tipo_intervencion': interv.get('tipo_intervencion'),
                        'presupuesto_base': interv.get('presupuesto_base'),
                        'avance_obra': interv.get('avance_obra'),
                        'referencia_proceso': interv.get('referencia_proceso'),
                        'ano': interv.get('ano')
                    }
                    intervenciones_data.append(interv_info)
                    
                    # Problema 2: Intervención sin intervencion_id
                    if not interv.get('intervencion_id'):
                        problemas.append({
                            'tipo': 'INTERVENCION_SIN_ID',
                            'upid': upid,
                            'descripcion': f"Intervención {idx} sin intervencion_id",
                            'severidad': 'MEDIA'
                        })
                    
                    # Problema 3: Intervención sin presupuesto ni avance
                    if not interv.get('presupuesto_base') and not interv.get('avance_obra'):
                        problemas.append({
                            'tipo': 'INTERVENCION_VACÍA',
                            'upid': upid,
                            'descripcion': f"Intervención {interv.get('intervencion_id', idx)} sin datos relevantes",
                            'severidad': 'BAJA'
                        })
            else:
                # Problema 4: intervenciones no es una lista
                problemas.append({
                    'tipo': 'ESTRUCTURA_INCORRECTA',
                    'upid': upid,
                    'descripcion': f"Campo 'intervenciones' no es una lista: {type(intervenciones)}",
                    'severidad': 'CRÍTICA'
                })
                unidad_info['n_intervenciones_real'] = 0
            
            # Problema 5: Unidad sin geometría ni coordenadas
            if not data.get('has_geometry') and not data.get('lat') and not data.get('lon'):
                problemas.append({
                    'tipo': 'SIN_UBICACIÓN',
                    'upid': upid,
                    'descripcion': "Unidad sin geometry, lat ni lon",
                    'severidad': 'ALTA'
                })
            
            unidades_data.append(unidad_info)
        
        # Convertir a DataFrames
        df_unidades = pd.DataFrame(unidades_data)
        df_intervenciones = pd.DataFrame(intervenciones_data)
        df_problemas = pd.DataFrame(problemas)
        
        # ANÁLISIS Y REPORTES
        print(f"\n{'='*80}")
        print("[DATA] RESULTADOS DEL ANÁLISIS")
        print("="*80)
        
        # 1. Estadísticas generales
        print(f"\n1. ESTADÍSTICAS GENERALES:")
        print(f"   • Total unidades: {len(df_unidades)}")
        print(f"   • Total intervenciones: {len(df_intervenciones)}")
        print(f"   • Promedio intervenciones/unidad: {len(df_intervenciones)/len(df_unidades):.2f}")
        
        # 2. Distribución de intervenciones por unidad
        print(f"\n2. DISTRIBUCIÓN DE INTERVENCIONES:")
        dist_intervenciones = df_unidades['n_intervenciones_real'].value_counts().sort_index()
        for n_interv, count in dist_intervenciones.head(10).items():
            print(f"   • {n_interv} intervención(es): {count} unidades ({count/len(df_unidades)*100:.1f}%)")
        
        if len(dist_intervenciones) > 10:
            print(f"   • ... y más")
        
        # 3. Unidades con más intervenciones
        print(f"\n3. TOP 10 UNIDADES CON MÁS INTERVENCIONES:")
        top_unidades = df_unidades.nlargest(10, 'n_intervenciones_real')[['upid', 'nombre_up', 'n_intervenciones_real', 'clase_up']]
        for idx, row in top_unidades.iterrows():
            print(f"   • {row['upid']}: {row['n_intervenciones_real']} intervenciones - {row['nombre_up']} ({row['clase_up']})")
        
        # 4. Análisis por clase_up
        print(f"\n4. AGRUPACIÓN POR CLASE_UP:")
        clase_stats = df_unidades.groupby('clase_up').agg({
            'upid': 'count',
            'n_intervenciones_real': ['sum', 'mean']
        }).round(2)
        clase_stats.columns = ['N_Unidades', 'Total_Intervenciones', 'Promedio_Intervenciones']
        print(clase_stats.to_string())
        
        # 5. Análisis de coordenadas
        print(f"\n5. ANÁLISIS DE COORDENADAS:")
        unidades_con_geom = df_unidades['has_geometry'].sum()
        unidades_con_lat = df_unidades['lat'].notna().sum()
        unidades_con_lon = df_unidades['lon'].notna().sum()
        unidades_con_coords = (df_unidades['lat'].notna() & df_unidades['lon'].notna()).sum()
        
        print(f"   • Unidades con has_geometry=True: {unidades_con_geom} ({unidades_con_geom/len(df_unidades)*100:.1f}%)")
        print(f"   • Unidades con lat válida: {unidades_con_lat} ({unidades_con_lat/len(df_unidades)*100:.1f}%)")
        print(f"   • Unidades con lon válida: {unidades_con_lon} ({unidades_con_lon/len(df_unidades)*100:.1f}%)")
        print(f"   • Unidades con lat Y lon: {unidades_con_coords} ({unidades_con_coords/len(df_unidades)*100:.1f}%)")
        
        # 6. PROBLEMAS DETECTADOS
        if len(df_problemas) > 0:
            print(f"\n6. PROBLEMAS DETECTADOS ({len(df_problemas)} total):")
            
            # Agrupar por tipo y severidad
            problemas_por_tipo = df_problemas.groupby(['severidad', 'tipo']).size().sort_values(ascending=False)
            
            for (severidad, tipo), count in problemas_por_tipo.items():
                print(f"\n   [{severidad}] {tipo}: {count} ocurrencias")
                
                # Mostrar algunos ejemplos
                ejemplos = df_problemas[(df_problemas['severidad'] == severidad) & (df_problemas['tipo'] == tipo)].head(3)
                for _, problema in ejemplos.iterrows():
                    print(f"      • {problema['upid']}: {problema['descripcion']}")
        else:
            print(f"\n6. PROBLEMAS DETECTADOS:")
            print("   ✅ No se detectaron problemas de estructura")
        
        # 7. Verificación de integridad de IDs
        print(f"\n7. INTEGRIDAD DE IDs:")
        upids_esperados = set(df_unidades['upid'])
        upids_en_intervenciones = set(df_intervenciones['upid'])
        upids_huerfanos = upids_en_intervenciones - upids_esperados
        
        if upids_huerfanos:
            print(f"   ⚠️  {len(upids_huerfanos)} UPIDs huérfanos en intervenciones (sin unidad padre)")
        else:
            print(f"   ✅ Todos los UPIDs en intervenciones tienen su unidad correspondiente")
        
        # 8. Recomendaciones
        print(f"\n{'='*80}")
        print("[DATA] RECOMENDACIONES")
        print("="*80)
        
        recomendaciones = []
        
        # Recomendación 1: Unidades con muchas intervenciones
        max_intervenciones = df_unidades['n_intervenciones_real'].max()
        if max_intervenciones > 10:
            unidades_sospechosas = (df_unidades['n_intervenciones_real'] > 10).sum()
            recomendaciones.append(f"⚠️  {unidades_sospechosas} unidades con más de 10 intervenciones. Verificar si la agrupación es correcta.")
        
        # Recomendación 2: Sin coordenadas
        sin_coords = len(df_unidades) - unidades_con_coords
        if sin_coords > len(df_unidades) * 0.1:
            recomendaciones.append(f"⚠️  {sin_coords} unidades ({sin_coords/len(df_unidades)*100:.1f}%) sin coordenadas. Ejecutar pipeline con correcciones aplicadas.")
        
        # Recomendación 3: Problemas críticos
        if len(df_problemas[df_problemas['severidad'] == 'CRÍTICA']) > 0:
            recomendaciones.append("🚨 Se detectaron problemas CRÍTICOS de estructura. Revisar urgentemente.")
        
        # Recomendación 4: Desincronización
        if 'DESINCRONIZACIÓN' in df_problemas['tipo'].values:
            count_desinc = (df_problemas['tipo'] == 'DESINCRONIZACIÓN').sum()
            recomendaciones.append(f"⚠️  {count_desinc} unidades con n_intervenciones desincronizado. Re-ejecutar pipeline.")
        
        if recomendaciones:
            for rec in recomendaciones:
                print(f"\n{rec}")
        else:
            print("\n✅ La agrupación está correcta. No se requieren acciones.")
        
        print(f"\n{'='*80}")
        
    except Exception as e:
        print(f"[ERROR] Error en análisis: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    """
    Ejecutar análisis de agrupación.
    """
    print("[START] Iniciando análisis de agrupación...")
    analyze_grouping_logic()
    print("\n[DONE] Análisis completado.")
