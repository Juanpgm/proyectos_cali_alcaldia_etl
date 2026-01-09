"""
Script para verificar la suma de presupuesto_base en Firebase
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.config import get_firestore_client

def verificar_presupuesto():
    """Verifica la suma de presupuestos en Firebase"""
    
    print("="*80)
    print("VERIFICACIÓN DE PRESUPUESTO EN FIREBASE")
    print("="*80)
    
    db = get_firestore_client()
    if not db:
        print("[ERROR] No se pudo conectar a Firebase")
        return
    
    collection_ref = db.collection('unidades_proyecto')
    docs = collection_ref.stream()
    
    total_upids = 0
    total_intervenciones = 0
    presupuestos = []
    presupuestos_por_tipo = {}  # Para debug
    
    print("\n📊 Analizando datos...")
    
    for doc in docs:
        total_upids += 1
        data = doc.to_dict()
        
        # Las intervenciones están en un array dentro de cada UP
        intervenciones = data.get('intervenciones', [])
        total_intervenciones += len(intervenciones)
        
        # Procesar cada intervención
        for idx, interv in enumerate(intervenciones):
            presupuesto = interv.get('presupuesto_base')
            
            if presupuesto is not None:
                # Debug: ver tipo de dato
                tipo = type(presupuesto).__name__
                if tipo not in presupuestos_por_tipo:
                    presupuestos_por_tipo[tipo] = []
                presupuestos_por_tipo[tipo].append(presupuesto)
                
                try:
                    if isinstance(presupuesto, str):
                        # Si es string, limpiar
                        presupuesto_clean = presupuesto.replace('$', '').replace(',', '').replace('.', '').strip()
                        presupuesto_val = float(presupuesto_clean)
                    else:
                        # Si es numérico, usar directamente
                        presupuesto_val = float(presupuesto)
                    
                    if presupuesto_val > 0:
                        presupuestos.append(presupuesto_val)
                        
                        # Debug: mostrar algunos valores grandes
                        if presupuesto_val > 1000000000:  # Mayor a mil millones
                            print(f"   💰 Presupuesto grande encontrado: ${presupuesto_val:,.0f} (tipo: {tipo})")
                            if idx < 3:  # Solo los primeros 3 por documento
                                print(f"      UPID: {doc.id}, Intervención: {interv.get('descripcion_intervencion', 'N/A')[:50]}")
                            
                except (ValueError, TypeError) as e:
                    print(f"   ⚠️ Error procesando presupuesto: {presupuesto} (tipo: {tipo}, error: {e})")
                    pass
    
    # Mostrar análisis de tipos de datos
    print(f"\n📋 Tipos de datos encontrados:")
    for tipo, valores in presupuestos_por_tipo.items():
        print(f"   - {tipo}: {len(valores)} valores")
        if len(valores) > 0:
            ejemplo = valores[0]
            print(f"      Ejemplo: {ejemplo} (${float(ejemplo) if isinstance(ejemplo, (int, float)) else 'no numérico'})")
    
    # Calcular suma
    print(f"\n📊 RESULTADOS:")
    print(f"1. Número total de unidades de proyecto (UPIDs únicos): {total_upids}")
    print(f"2. Número total de intervenciones: {total_intervenciones}")
    
    if presupuestos:
        suma_presupuesto = sum(presupuestos)
        print(f"3. Sumatoria de presupuesto_base: ${suma_presupuesto:,.2f}")
        print(f"   (Basado en {len(presupuestos)} intervenciones con presupuesto válido)")
        
        # Estadísticas adicionales
        print(f"\n📈 Estadísticas adicionales:")
        print(f"   - Presupuesto mínimo: ${min(presupuestos):,.2f}")
        print(f"   - Presupuesto máximo: ${max(presupuestos):,.2f}")
        print(f"   - Presupuesto promedio: ${sum(presupuestos)/len(presupuestos):,.2f}")
        
        # Top 5 presupuestos más grandes
        top_5 = sorted(presupuestos, reverse=True)[:5]
        print(f"\n🔝 Top 5 presupuestos más grandes:")
        for i, p in enumerate(top_5, 1):
            print(f"   {i}. ${p:,.2f}")
    else:
        print("3. Sumatoria de presupuesto_base: No hay datos válidos")
    
    print("="*80)

if __name__ == "__main__":
    verificar_presupuesto()
