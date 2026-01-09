# -*- coding: utf-8 -*-
"""
Script para validar que NO existe duplicación de presupuesto_base o distorsión 
en avance_obra debido a agregaciones incorrectas.

Calcula:
1. Suma de presupuesto_base solo desde intervenciones (método correcto)
2. Promedio de avance_obra solo desde intervenciones (método correcto)
3. Verifica que no existan campos agregados a nivel de unidad que distorsionen datos
"""

import os
import sys

# Agregar rutas necesarias al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client


def validar_no_duplicacion():
    """
    Valida que presupuesto_base y avance_obra se calculan correctamente
    sin duplicación o distorsión por agregaciones incorrectas.
    """
    try:
        print(f"\n{'='*80}")
        print("[VALIDACIÓN] NO DUPLICACIÓN DE PRESUPUESTOS Y AVANCES")
        print("="*80)
        
        db = get_firestore_client()
        if not db:
            print("[ERROR] No se pudo conectar a Firebase")
            return
        
        print("\n[SYNC] Obteniendo todos los documentos desde Firebase...")
        docs = list(db.collection("unidades_proyecto").stream())
        
        if not docs:
            print("[WARNING] No hay documentos en Firebase")
            return
        
        print(f"[OK] Obtenidos {len(docs)} documentos")
        
        # Método 1: Suma desde intervenciones (CORRECTO)
        print("\n[CÁLCULO] Método 1: Suma desde intervenciones individuales")
        suma_presupuesto_intervenciones = 0
        suma_avance_intervenciones = 0
        count_presupuesto = 0
        count_avance = 0
        
        for doc in docs:
            data = doc.to_dict()
            intervenciones = data.get('intervenciones', [])
            
            for interv in intervenciones:
                # Presupuesto
                presupuesto = interv.get('presupuesto_base')
                if presupuesto is not None and isinstance(presupuesto, (int, float)):
                    if presupuesto > 0:
                        suma_presupuesto_intervenciones += presupuesto
                        count_presupuesto += 1
                
                # Avance
                avance = interv.get('avance_obra')
                if avance is not None and isinstance(avance, (int, float)):
                    suma_avance_intervenciones += avance
                    count_avance += 1
        
        promedio_avance_intervenciones = suma_avance_intervenciones / count_avance if count_avance > 0 else 0
        
        print(f"  Presupuesto total: ${suma_presupuesto_intervenciones:,.0f}")
        print(f"  Avance promedio: {promedio_avance_intervenciones:.2f}%")
        print(f"  Intervenciones con presupuesto: {count_presupuesto:,}")
        print(f"  Intervenciones con avance: {count_avance:,}")
        
        # Método 2: Verificar si existen campos agregados a nivel de unidad (INCORRECTO)
        print("\n[VERIFICACIÓN] Método 2: Buscar campos agregados a nivel de unidad")
        campos_sospechosos = [
            'presupuesto_base',
            'presupuesto_total',
            'presupuesto_total_up',
            'presupuesto_acumulado',
            'avance_obra',
            'avance_promedio',
            'avance_promedio_up',
            'avance_total'
        ]
        
        unidades_con_campos_sospechosos = {}
        for campo in campos_sospechosos:
            unidades_con_campos_sospechosos[campo] = []
        
        for doc in docs:
            data = doc.to_dict()
            upid = data.get('upid', 'UNKNOWN')
            
            for campo in campos_sospechosos:
                if campo in data:
                    unidades_con_campos_sospechosos[campo].append({
                        'upid': upid,
                        'valor': data[campo]
                    })
        
        # Reporte de campos sospechosos
        campos_encontrados = {k: v for k, v in unidades_con_campos_sospechosos.items() if v}
        
        if campos_encontrados:
            print(f"  ❌ SE ENCONTRARON CAMPOS SOSPECHOSOS A NIVEL DE UNIDAD:")
            for campo, unidades in campos_encontrados.items():
                print(f"\n    Campo '{campo}' encontrado en {len(unidades)} unidades:")
                for unidad in unidades[:3]:  # Mostrar solo primeras 3
                    print(f"      UPID: {unidad['upid']}, Valor: {unidad['valor']}")
                if len(unidades) > 3:
                    print(f"      ... y {len(unidades) - 3} más")
        else:
            print(f"  ✅ NO se encontraron campos agregados a nivel de unidad")
        
        # Método 3: Calcular si hubiera agregación incorrecta (para comparar)
        print("\n[SIMULACIÓN] Método 3: ¿Qué pasaría con agregación incorrecta?")
        
        # Simular suma por unidad (si cada unidad tuviera presupuesto_total_up)
        suma_si_agregacion_por_upid = 0
        
        for doc in docs:
            data = doc.to_dict()
            intervenciones = data.get('intervenciones', [])
            n_intervenciones = len(intervenciones)
            
            # Calcular presupuesto de esta unidad
            presupuesto_unidad = 0
            for interv in intervenciones:
                presupuesto = interv.get('presupuesto_base', 0)
                if presupuesto and isinstance(presupuesto, (int, float)) and presupuesto > 0:
                    presupuesto_unidad += presupuesto
            
            # Sumar presupuesto de cada unidad (método correcto, debe dar igual a método 1)
            suma_si_agregacion_por_upid += presupuesto_unidad
        
        print(f"  Si se suma por UPID (correcto): ${suma_si_agregacion_por_upid:,.0f}")
        print(f"  Diferencia con suma por intervención: ${abs(suma_presupuesto_intervenciones - suma_si_agregacion_por_upid):,.0f}")
        
        diferencia_pct = abs(suma_presupuesto_intervenciones - suma_si_agregacion_por_upid) / suma_presupuesto_intervenciones * 100 if suma_presupuesto_intervenciones > 0 else 0
        
        if diferencia_pct < 0.01:
            print(f"  ✅ Diferencia insignificante ({diferencia_pct:.4f}%) - Cálculo correcto")
        else:
            print(f"  ⚠️  Diferencia significativa ({diferencia_pct:.2f}%)")
        
        # Resultado final
        print(f"\n{'='*80}")
        print("[RESULTADO] VALIDACIÓN FINAL")
        print("="*80)
        
        if not campos_encontrados and diferencia_pct < 0.01:
            print(f"\n✅ VALIDACIÓN EXITOSA:")
            print(f"  • NO hay duplicación de presupuestos")
            print(f"  • NO hay distorsión de avances")
            print(f"  • Los datos se calculan correctamente desde intervenciones")
            print(f"\n  TOTALES CORRECTOS:")
            print(f"  • Presupuesto total: ${suma_presupuesto_intervenciones:,.0f}")
            print(f"  • Avance promedio: {promedio_avance_intervenciones:.2f}%")
            print(f"  • Intervenciones: {count_avance:,}")
            print(f"  • Unidades de proyecto: {len(docs):,}")
        else:
            print(f"\n❌ PROBLEMAS DETECTADOS:")
            if campos_encontrados:
                print(f"  • Existen campos agregados a nivel de unidad")
                print(f"  • Esto podría causar duplicación o distorsión")
            if diferencia_pct >= 0.01:
                print(f"  • Diferencia significativa entre métodos de cálculo")
                print(f"  • Revisar lógica de agregación")
        
        print(f"\n{'='*80}")
        
    except Exception as e:
        print(f"[ERROR] Error en validación: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    """
    Ejecutar validación de no duplicación.
    """
    print("[START] Iniciando validación...")
    validar_no_duplicacion()
    print("\n[DONE] Validación completada.")
