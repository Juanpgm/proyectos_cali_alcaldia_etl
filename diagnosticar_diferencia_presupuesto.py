"""
Diagnosticar por qué el presupuesto total difiere entre ETL y Frontend
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client

def analizar_presupuesto():
    print("=" * 80)
    print("DIAGNÓSTICO: DIFERENCIA DE PRESUPUESTO ETL vs FRONTEND")
    print("=" * 80)
    print()
    
    db = get_firestore_client()
    collection = db.collection('unidades_proyecto')
    
    print("📥 Obteniendo datos desde Firebase...")
    results = collection.stream()
    
    # Métricas ETL (suma por intervenciones)
    suma_por_intervenciones = 0
    intervenciones_con_presupuesto = 0
    total_intervenciones = 0
    
    # Métricas Frontend (suma por unidades)
    suma_por_unidades = 0
    unidades_con_presupuesto = 0
    total_unidades = 0
    
    registros = []
    
    for doc in results:
        data = doc.to_dict()
        total_unidades += 1
        
        # Presupuesto de la unidad (agregado)
        presupuesto_unidad = data.get('presupuesto_base', 0)
        if presupuesto_unidad and presupuesto_unidad > 0:
            suma_por_unidades += presupuesto_unidad
            unidades_con_presupuesto += 1
        
        # Intervenciones de la unidad
        intervenciones = data.get('intervenciones', [])
        if intervenciones:
            for interv in intervenciones:
                total_intervenciones += 1
                presupuesto_interv = interv.get('presupuesto_base', 0)
                if presupuesto_interv and presupuesto_interv > 0:
                    suma_por_intervenciones += presupuesto_interv
                    intervenciones_con_presupuesto += 1
        
        registros.append({
            'upid': data.get('upid'),
            'nombre_up': data.get('nombre_up'),
            'presupuesto_unidad': presupuesto_unidad,
            'num_intervenciones': len(intervenciones),
            'presupuestos_intervenciones': [i.get('presupuesto_base', 0) for i in intervenciones]
        })
    
    print(f"✅ Analizados {total_unidades} unidades de proyecto\n")
    
    print("=" * 80)
    print("CÁLCULO POR INTERVENCIONES (como hace el ETL):")
    print("=" * 80)
    print(f"Total intervenciones: {total_intervenciones}")
    print(f"Intervenciones con presupuesto: {intervenciones_con_presupuesto}")
    print(f"Suma total: ${suma_por_intervenciones:,.2f}")
    print()
    
    print("=" * 80)
    print("CÁLCULO POR UNIDADES (como hace el Frontend):")
    print("=" * 80)
    print(f"Total unidades: {total_unidades}")
    print(f"Unidades con presupuesto: {unidades_con_presupuesto}")
    print(f"Suma total: ${suma_por_unidades:,.2f}")
    print()
    
    diferencia = suma_por_intervenciones - suma_por_unidades
    print("=" * 80)
    print("DIFERENCIA:")
    print("=" * 80)
    print(f"Diferencia absoluta: ${diferencia:,.2f}")
    print(f"Porcentaje: {(diferencia / suma_por_intervenciones * 100):.2f}%")
    print()
    
    # Buscar unidades con múltiples intervenciones
    print("=" * 80)
    print("UNIDADES CON MÚLTIPLES INTERVENCIONES:")
    print("=" * 80)
    
    multiples = [r for r in registros if r['num_intervenciones'] > 1]
    print(f"Unidades con múltiples intervenciones: {len(multiples)}\n")
    
    if multiples:
        print("Ejemplos (primeras 5):")
        for i, record in enumerate(multiples[:5], 1):
            print(f"\n{i}. {record['upid']} - {record['nombre_up'][:50]}")
            print(f"   Presupuesto unidad: ${record['presupuesto_unidad']:,.2f}")
            print(f"   Intervenciones: {record['num_intervenciones']}")
            print(f"   Presupuestos intervenciones:")
            for j, p in enumerate(record['presupuestos_intervenciones'], 1):
                print(f"      {j}. ${p:,.2f}")
            suma_intervs = sum(record['presupuestos_intervenciones'])
            print(f"   Suma intervenciones: ${suma_intervs:,.2f}")
            if suma_intervs != record['presupuesto_unidad']:
                print(f"   ⚠️ DIFERENCIA: ${abs(suma_intervs - record['presupuesto_unidad']):,.2f}")
    
    print("\n" + "=" * 80)
    print("CONCLUSIÓN:")
    print("=" * 80)
    
    if abs(diferencia) < 1000:
        print("✅ Los cálculos son consistentes (diferencia despreciable)")
    else:
        print("⚠️ Hay una diferencia significativa:")
        print()
        print("POSIBLES CAUSAS:")
        print("1. ETL suma por intervenciones individuales")
        print("2. Frontend suma por unidades agregadas (presupuesto_base)")
        print("3. Cuando una unidad tiene múltiples intervenciones:")
        print("   - ETL suma CADA presupuesto_base de intervención")
        print("   - Frontend usa presupuesto_base agregado de la unidad")
        print()
        print("RECOMENDACIÓN:")
        print("El presupuesto_base en la unidad debería ser:")
        print("- MAX de presupuestos si las intervenciones son para el mismo proyecto")
        print("- SUM de presupuestos si las intervenciones son independientes")

if __name__ == "__main__":
    analizar_presupuesto()
