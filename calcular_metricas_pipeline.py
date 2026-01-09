# -*- coding: utf-8 -*-
"""
Script para calcular métricas totales desde Firebase después del pipeline.
Muestra estadísticas sobre presupuesto_base, avances_obra, n_intervenciones y unidades_proyecto.
"""

import os
import sys

# Agregar rutas necesarias al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client


def calculate_comprehensive_metrics(collection_name: str = "unidades_proyecto"):
    """
    Calcula métricas completas desde Firebase después del pipeline.
    
    Args:
        collection_name: Nombre de la colección en Firebase
    """
    try:
        print(f"\n{'='*80}")
        print("[DATA] MÉTRICAS TOTALES (DESDE FIREBASE)")
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
        
        print(f"[OK] Obtenidos {len(docs)} documentos")
        
        # Inicializar contadores
        total_unidades = len(docs)
        total_intervenciones = 0
        
        # Listas para valores numéricos
        presupuestos = []
        avances = []
        
        # Contadores adicionales
        unidades_con_geometry = 0
        unidades_sin_geometry = 0
        unidades_con_intervenciones = 0
        intervenciones_con_presupuesto = 0
        intervenciones_con_avance = 0
        
        print("\n[SYNC] Procesando documentos...")
        
        # Procesar cada documento
        for doc in docs:
            data = doc.to_dict()
            
            # Verificar geometry
            geometry = data.get('geometry')
            if geometry and isinstance(geometry, dict):
                coords = geometry.get('coordinates')
                if coords and isinstance(coords, list) and len(coords) >= 2:
                    unidades_con_geometry += 1
                else:
                    unidades_sin_geometry += 1
            else:
                unidades_sin_geometry += 1
            
            # Obtener intervenciones
            intervenciones = data.get('intervenciones', [])
            
            if not isinstance(intervenciones, list):
                # Si no es lista, intentar convertir
                continue
            
            n_interv = len(intervenciones)
            if n_interv > 0:
                total_intervenciones += n_interv
                unidades_con_intervenciones += 1
            
            # Procesar cada intervención
            for interv in intervenciones:
                if not isinstance(interv, dict):
                    continue
                
                # Recoger presupuestos
                presupuesto = interv.get('presupuesto_base')
                if presupuesto is not None:
                    try:
                        if isinstance(presupuesto, str):
                            # Limpiar string
                            presupuesto_clean = presupuesto.replace('$', '').replace(',', '').replace('.', '').strip()
                            if presupuesto_clean and presupuesto_clean.lower() not in ['nan', 'none', 'null']:
                                presupuesto_val = float(presupuesto_clean)
                                if presupuesto_val > 0:
                                    presupuestos.append(presupuesto_val)
                                    intervenciones_con_presupuesto += 1
                        elif isinstance(presupuesto, (int, float)):
                            presupuesto_val = float(presupuesto)
                            if presupuesto_val > 0:
                                presupuestos.append(presupuesto_val)
                                intervenciones_con_presupuesto += 1
                    except (ValueError, TypeError) as e:
                        pass
                
                # Recoger avances
                avance = interv.get('avance_obra')
                if avance is not None:
                    try:
                        if isinstance(avance, str):
                            avance_clean = avance.replace('%', '').replace(',', '.').strip()
                            if avance_clean and avance_clean.lower() not in ['nan', 'none', 'null']:
                                avance_val = float(avance_clean)
                                avances.append(avance_val)
                                intervenciones_con_avance += 1
                        elif isinstance(avance, (int, float)):
                            avance_val = float(avance)
                            avances.append(avance_val)
                            intervenciones_con_avance += 1
                    except (ValueError, TypeError) as e:
                        pass
        
        # Mostrar resultados
        print(f"\n{'='*80}")
        print("[DATA] RESULTADOS DE MÉTRICAS")
        print("="*80)
        
        print(f"\n1. UNIDADES DE PROYECTO:")
        print(f"   • Total de unidades (UPIDs únicos): {total_unidades:,}")
        print(f"   • Unidades con geometry válida: {unidades_con_geometry:,} ({unidades_con_geometry/total_unidades*100:.1f}%)")
        print(f"   • Unidades sin geometry: {unidades_sin_geometry:,} ({unidades_sin_geometry/total_unidades*100:.1f}%)")
        print(f"   • Unidades con intervenciones: {unidades_con_intervenciones:,} ({unidades_con_intervenciones/total_unidades*100:.1f}%)")
        
        print(f"\n2. INTERVENCIONES:")
        print(f"   • Total de intervenciones: {total_intervenciones:,}")
        print(f"   • Promedio intervenciones por UP: {total_intervenciones/total_unidades:.2f}")
        
        print(f"\n3. PRESUPUESTO BASE:")
        if presupuestos:
            suma_presupuesto = sum(presupuestos)
            promedio_presupuesto = suma_presupuesto / len(presupuestos)
            print(f"   • Sumatoria total: ${suma_presupuesto:,.0f}")
            print(f"   • Promedio por intervención: ${promedio_presupuesto:,.0f}")
            print(f"   • Intervenciones con presupuesto válido: {intervenciones_con_presupuesto:,} ({intervenciones_con_presupuesto/total_intervenciones*100:.1f}%)")
            print(f"   • Intervenciones sin presupuesto: {total_intervenciones - intervenciones_con_presupuesto:,}")
        else:
            print("   • No hay datos válidos de presupuesto")
        
        print(f"\n4. AVANCE DE OBRA:")
        if avances:
            suma_avance = sum(avances)
            promedio_avance = suma_avance / len(avances)
            print(f"   • Promedio general: {promedio_avance:.2f}%")
            print(f"   • Sumatoria de avances: {suma_avance:,.2f}%")
            print(f"   • Intervenciones con avance válido: {intervenciones_con_avance:,} ({intervenciones_con_avance/total_intervenciones*100:.1f}%)")
            print(f"   • Intervenciones sin avance: {total_intervenciones - intervenciones_con_avance:,}")
            
            # Estadísticas adicionales
            avances_por_estado = {
                'En alistamiento (0%)': sum(1 for a in avances if a == 0),
                'En ejecución (1-99%)': sum(1 for a in avances if 0 < a < 100),
                'Terminado (100%)': sum(1 for a in avances if a >= 100)
            }
            print(f"\n   Distribución por estado (según avance):")
            for estado, count in avances_por_estado.items():
                print(f"   • {estado}: {count:,} ({count/len(avances)*100:.1f}%)")
        else:
            print("   • No hay datos válidos de avance")
        
        print(f"\n{'='*80}")
        print("[OK] ANÁLISIS COMPLETADO")
        print("="*80)
        
        # Alerta sobre posible pérdida de datos
        if unidades_sin_geometry > total_unidades * 0.1:
            print(f"\n⚠️  ALERTA: {unidades_sin_geometry} unidades ({unidades_sin_geometry/total_unidades*100:.1f}%) sin geometry")
            print("   Esto puede indicar pérdida de datos durante el pipeline.")
        
        if intervenciones_con_presupuesto < total_intervenciones * 0.5:
            print(f"\n⚠️  ALERTA: Solo {intervenciones_con_presupuesto/total_intervenciones*100:.1f}% de intervenciones tienen presupuesto")
            print("   Revisar calidad de datos de presupuesto_base.")
        
    except Exception as e:
        print(f"[ERROR] Error calculando métricas: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    """
    Ejecutar cálculo de métricas.
    """
    print("[START] Iniciando cálculo de métricas...")
    calculate_comprehensive_metrics()
    print("\n[DONE] Cálculo completado.")
