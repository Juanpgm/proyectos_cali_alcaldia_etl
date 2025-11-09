"""
Script de Carga Inicial de Colecciones Analíticas
Estrategia: INCREMENTAL - Solo añade datos nuevos, skip existentes

Este script ejecuta las agregaciones analíticas iniciales para el Data Warehouse
usando arquitectura Constellation Schema.

Características:
- Carga incremental automática (skip documentos existentes)
- Agregaciones semanales (no mensuales)
- Optimizado para consultas del frontend
- Preserva nombres de variables existentes

Uso:
    python load_initial_analytics.py
    python load_initial_analytics.py --force-rebuild

Autor: Sistema ETL Alcaldía de Cali
Fecha: Noviembre 2025
"""

import sys
import os
from datetime import datetime

# Agregar el directorio actual al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from cloud_functions.analytics_aggregations import (
    aggregate_contratos_weekly,
    aggregate_flujo_caja_banco,
    calculate_global_kpis
)
from cloud_functions.emprestito_analytics import (
    aggregate_analysis_by_banco,
    aggregate_analysis_by_centro_gestor,
    aggregate_resumen_anual,
    aggregate_series_temporales_diarias
)
from database.config import secure_log

# =============================================================================
# FUNCIONES DE VERIFICACIÓN INCREMENTAL
# =============================================================================

def check_collection_status(collection_name: str):
    """
    Verifica el estado actual de una colección analítica.
    
    Args:
        collection_name: Nombre de la colección a verificar
        
    Returns:
        Dict con información de la colección
    """
    from database.config import get_firestore_client
    
    db = get_firestore_client()
    collection_ref = db.collection(collection_name)
    
    try:
        # Contar documentos existentes
        docs = list(collection_ref.limit(1000).stream())
        count = len(docs)
        
        # Obtener último timestamp de actualización
        ultimo_doc = None
        if count > 0:
            # Ordenar por timestamp descendente
            for doc in docs:
                data = doc.to_dict()
                if 'timestamp' in data:
                    if not ultimo_doc or data['timestamp'] > ultimo_doc:
                        ultimo_doc = data['timestamp']
        
        return {
            'exists': count > 0,
            'count': count,
            'ultimo_update': ultimo_doc.strftime('%Y-%m-%d %H:%M:%S') if ultimo_doc else None
        }
    except Exception as e:
        return {
            'exists': False,
            'count': 0,
            'error': str(e)
        }


# =============================================================================
# FUNCIÓN PRINCIPAL DE CARGA INICIAL
# =============================================================================

@secure_log
def load_initial_analytics(force_rebuild: bool = False):
    """
    Ejecuta carga inicial de colecciones analíticas.
    
    Args:
        force_rebuild: Si True, regenera todas las agregaciones.
                       Si False (default), solo añade datos nuevos.
    """
    print("\n" + "="*80)
    print("📊 CARGA INICIAL DE COLECCIONES ANALÍTICAS")
    print("="*80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Modo: {'FORCE REBUILD' if force_rebuild else 'INCREMENTAL (skip existentes)'}")
    print("="*80 + "\n")
    
    resultados = {}
    
    # =========================================================================
    # 1. AGREGACIÓN SEMANAL DE CONTRATOS
    # =========================================================================
    collection_name = 'analytics_contratos_weekly'
    print(f"\n[1/3] Verificando colección '{collection_name}'...")
    
    # Si force_rebuild, no verificar estado (evita lecturas innecesarias)
    if force_rebuild:
        print(f"🔄 FORCE REBUILD - Regenerando colección...")
        resultado = aggregate_contratos_weekly()
        resultados['contratos_weekly'] = resultado
    else:
        status = check_collection_status(collection_name)
        
        if status['exists']:
            print(f"✅ Colección ya existe con {status['count']} documentos")
            print(f"   Última actualización: {status['ultimo_update']}")
            print(f"   ⏭️  SKIP - Modo incremental")
            resultados['contratos_weekly'] = {
                'success': True,
                'skipped': True,
                'existing_docs': status['count']
            }
        else:
            print(f"🆕 Colección vacía - Generando agregaciones iniciales...")
            resultado = aggregate_contratos_weekly()
            resultados['contratos_weekly'] = resultado
    
    # =========================================================================
    # 2. AGREGACIÓN DE FLUJO DE CAJA POR BANCO
    # =========================================================================
    collection_name = 'analytics_flujo_caja_banco'
    print(f"\n[2/3] Verificando colección '{collection_name}'...")
    
    # Si force_rebuild, no verificar estado (evita lecturas innecesarias)
    if force_rebuild:
        print(f"🔄 FORCE REBUILD - Regenerando colección...")
        resultado = aggregate_flujo_caja_banco()
        resultados['flujo_caja_banco'] = resultado
    else:
        status = check_collection_status(collection_name)
        
        if status['exists']:
            print(f"✅ Colección ya existe con {status['count']} documentos")
            print(f"   Última actualización: {status['ultimo_update']}")
            print(f"   ⏭️  SKIP - Modo incremental")
            resultados['flujo_caja_banco'] = {
                'success': True,
                'skipped': True,
                'existing_docs': status['count']
            }
        else:
            print(f"🆕 Colección vacía - Generando agregaciones iniciales...")
            resultado = aggregate_flujo_caja_banco()
            resultados['flujo_caja_banco'] = resultado
    
    # =========================================================================
    # 3. KPIs GLOBALES DEL DASHBOARD
    # =========================================================================
    collection_name = 'analytics_kpi_dashboard'
    print(f"\n[3/7] Verificando colección '{collection_name}'...")
    
    # Los KPIs siempre se recalculan para tener datos actualizados
    print(f"🔄 Recalculando KPIs globales (siempre actualizado)...")
    resultado = calculate_global_kpis()
    resultados['kpis_globales'] = resultado
    
    # =========================================================================
    # 4. ANÁLISIS POR BANCO (EMPRÉSTITO)
    # =========================================================================
    collection_name = 'analytics_emprestito_por_banco'
    print(f"\n[4/7] Verificando colección '{collection_name}'...")
    
    # Skip verification when force_rebuild
    if force_rebuild:
        print(f"🔄 FORCE REBUILD - Regenerando colección...")
        resultado = aggregate_analysis_by_banco()
        resultados['por_banco'] = resultado
    else:
        status = check_collection_status(collection_name)
        
        if status['exists']:
            print(f"✅ Colección ya existe con {status['count']} documentos")
            print(f"   Última actualización: {status['ultimo_update']}")
            print(f"   ⏭️  SKIP - Modo incremental")
            resultados['por_banco'] = {
                'success': True,
                'skipped': True,
                'existing_docs': status['count']
            }
        else:
            print(f"🆕 Colección vacía - Generando análisis por banco...")
            resultado = aggregate_analysis_by_banco()
            resultados['por_banco'] = resultado
    
    # =========================================================================
    # 5. ANÁLISIS POR CENTRO GESTOR (EMPRÉSTITO)
    # =========================================================================
    collection_name = 'analytics_emprestito_por_centro_gestor'
    print(f"\n[5/7] Verificando colección '{collection_name}'...")
    
    # Skip verification when force_rebuild
    if force_rebuild:
        print(f"🔄 FORCE REBUILD - Regenerando colección...")
        resultado = aggregate_analysis_by_centro_gestor()
        resultados['por_centro_gestor'] = resultado
    else:
        status = check_collection_status(collection_name)
        
        if status['exists']:
            print(f"✅ Colección ya existe con {status['count']} documentos")
            print(f"   Última actualización: {status['ultimo_update']}")
            print(f"   ⏭️  SKIP - Modo incremental")
            resultados['por_centro_gestor'] = {
                'success': True,
                'skipped': True,
                'existing_docs': status['count']
            }
        else:
            print(f"🆕 Colección vacía - Generando análisis por centro gestor...")
            resultado = aggregate_analysis_by_centro_gestor()
            resultados['por_centro_gestor'] = resultado
    
    # =========================================================================
    # 6. RESUMEN ANUAL (EMPRÉSTITO)
    # =========================================================================
    collection_name = 'analytics_emprestito_resumen_anual'
    print(f"\n[6/7] Verificando colección '{collection_name}'...")
    
    # Skip verification when force_rebuild
    if force_rebuild:
        print(f"🔄 FORCE REBUILD - Regenerando colección...")
        resultado = aggregate_resumen_anual()
        resultados['resumen_anual'] = resultado
    else:
        status = check_collection_status(collection_name)
        
        if status['exists']:
            print(f"✅ Colección ya existe con {status['count']} documentos")
            print(f"   Última actualización: {status['ultimo_update']}")
            print(f"   ⏭️  SKIP - Modo incremental")
            resultados['resumen_anual'] = {
                'success': True,
                'skipped': True,
                'existing_docs': status['count']
            }
        else:
            print(f"🆕 Colección vacía - Generando resumen anual...")
            resultado = aggregate_resumen_anual()
            resultados['resumen_anual'] = resultado
    
    # =========================================================================
    # 7. SERIES TEMPORALES DIARIAS (EMPRÉSTITO)
    # =========================================================================
    collection_name = 'analytics_emprestito_series_temporales_diarias'
    print(f"\n[7/7] Verificando colección '{collection_name}'...")
    
    # Skip verification when force_rebuild
    if force_rebuild:
        print(f"🔄 FORCE REBUILD - Regenerando colección...")
        resultado = aggregate_series_temporales_diarias()
        resultados['series_temporales'] = resultado
    else:
        status = check_collection_status(collection_name)
        
        if status['exists']:
            print(f"✅ Colección ya existe con {status['count']} documentos")
            print(f"   Última actualización: {status['ultimo_update']}")
            print(f"   ⏭️  SKIP - Modo incremental")
            resultados['series_temporales'] = {
                'success': True,
                'skipped': True,
                'existing_docs': status['count']
            }
        else:
            print(f"🆕 Colección vacía - Generando series temporales diarias...")
            resultado = aggregate_series_temporales_diarias()
            resultados['series_temporales'] = resultado
    
    # =========================================================================
    # RESUMEN FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("📊 RESUMEN DE CARGA INICIAL")
    print("="*80)
    
    total_success = 0
    total_skipped = 0
    total_error = 0
    
    for nombre, resultado in resultados.items():
        if resultado.get('skipped'):
            status_icon = "⏭️"
            status_text = f"SKIPPED ({resultado.get('existing_docs', 0)} docs existentes)"
            total_skipped += 1
        elif resultado.get('success'):
            status_icon = "✅"
            status_text = "SUCCESS"
            total_success += 1
        else:
            status_icon = "❌"
            status_text = f"ERROR: {resultado.get('error', 'Unknown')}"
            total_error += 1
        
        print(f"{status_icon} {nombre}: {status_text}")
    
    print("\n" + "-"*80)
    print(f"Total: {total_success} exitosas | {total_skipped} omitidas | {total_error} errores")
    print("="*80 + "\n")
    
    # =========================================================================
    # ÍNDICES RECOMENDADOS
    # =========================================================================
    print("\n" + "="*80)
    print("� ÍNDICES RECOMENDADOS PARA FIRESTORE")
    print("="*80)
    
    indices_recomendados = [
        {
            'collection': 'analytics_contratos_weekly',
            'fields': ['anio', 'semana', 'banco', 'nombre_centro_gestor'],
            'reason': 'Consultas por período y filtros del dashboard'
        },
        {
            'collection': 'analytics_flujo_caja_banco',
            'fields': ['anio', 'trimestre', 'banco'],
            'reason': 'Consultas por banco y período'
        },
        {
            'collection': 'contratos_emprestito',
            'fields': ['estado_contrato', 'banco', 'fecha_inicio_contrato'],
            'reason': 'Filtros comunes en el frontend'
        },
        {
            'collection': 'reportes_contratos',
            'fields': ['referencia_contrato', 'fecha_reporte'],
            'reason': 'Join con contratos por referencia'
        }
    ]
    
    print("\nAgregar estos índices a firestore.indexes.json:\n")
    
    for idx in indices_recomendados:
        print(f"Collection: {idx['collection']}")
        print(f"  Fields: {', '.join(idx['fields'])}")
        print(f"  Reason: {idx['reason']}\n")
    
    print("="*80 + "\n")
    
    return resultados


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Carga inicial de colecciones analíticas con estrategia incremental'
    )
    parser.add_argument(
        '--force-rebuild',
        action='store_true',
        help='Forzar regeneración de todas las colecciones (ignora existentes)'
    )
    
    args = parser.parse_args()
    
    # Ejecutar carga
    resultados = load_initial_analytics(force_rebuild=args.force_rebuild)
    
    # Exit code basado en resultados
    total_errores = sum(1 for r in resultados.values() if not r.get('success') and not r.get('skipped'))
    sys.exit(0 if total_errores == 0 else 1)
