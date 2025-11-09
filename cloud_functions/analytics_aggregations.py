"""
Cloud Functions para Agregaciones Analíticas - Data Warehouse
Arquitectura: Constellation Schema con agregaciones semanales

Este módulo implementa Cloud Functions que generan colecciones analíticas
pre-calculadas optimizadas para las consultas del frontend.

Características:
- Agregaciones SEMANALES (no mensuales)
- Cálculos incrementales
- Dimensiones conformadas compartidas
- Optimizado para consultas del dashboard

Autor: Sistema ETL Alcaldía de Cali
Fecha: Noviembre 2025
"""

import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict
import sys
import os

# Agregar el directorio padre al path para importar database.config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.config import get_firestore_client, secure_log

# =============================================================================
# UTILIDADES PARA CÁLCULOS TEMPORALES
# =============================================================================

def get_week_number_and_dates(date: datetime) -> Dict[str, Any]:
    """
    Obtiene el número de semana ISO y fechas inicio/fin de la semana.
    
    Args:
        date: Fecha para calcular la semana
        
    Returns:
        Dict con: anio, semana, fecha_inicio_semana, fecha_fin_semana, semana_id
    """
    # Semana ISO: semana comienza el lunes
    iso_calendar = date.isocalendar()
    year = iso_calendar[0]
    week = iso_calendar[1]
    
    # Calcular el lunes de esa semana (inicio)
    weekday = date.weekday()  # 0 = Monday
    monday = date - timedelta(days=weekday)
    sunday = monday + timedelta(days=6)
    
    return {
        'anio': year,
        'semana': week,
        'fecha_inicio_semana': monday.strftime('%Y-%m-%d'),
        'fecha_fin_semana': sunday.strftime('%Y-%m-%d'),
        'semana_id': f"{year}-W{week:02d}"  # Formato: 2025-W45
    }


def parse_firebase_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parsea una fecha de Firebase a datetime.
    
    Args:
        date_str: String de fecha en formato ISO o personalizado
        
    Returns:
        datetime object o None si no es válida
    """
    if not date_str:
        return None
    
    try:
        # Intentar formato ISO con timezone
        if 'T' in date_str:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        # Formato solo fecha
        return datetime.strptime(date_str.split('T')[0], '%Y-%m-%d')
    except:
        return None


# =============================================================================
# FUNCIÓN 1: AGREGACIÓN SEMANAL DE CONTRATOS
# =============================================================================

@secure_log
def aggregate_contratos_weekly():
    """
    Genera agregaciones semanales de contratos de empréstito.
    
    Colección destino: analytics_contratos_weekly
    Granularidad: Semanal por BP + Centro Gestor + Banco
    
    Estructura:
    {
        id: "2025-W45_BP26005260_DATIC_Bancolombia",
        anio: 2025,
        semana: 45,
        fecha_inicio_semana: "2025-11-03",
        fecha_fin_semana: "2025-11-09",
        bp: "BP26005260",
        nombre_centro_gestor: "DATIC",
        banco: "Bancolombia",
        
        metricas_contratos: {
            cantidad_contratos: 3,
            valor_total_contratos: 1500000000,
            valor_pagado_total: 800000000,
            porcentaje_ejecucion: 53.33
        },
        
        metricas_reportes: {
            cantidad_reportes: 12,
            avance_fisico_promedio: 45.5,
            avance_financiero_promedio: 53.33,
            ultimo_reporte: "2025-11-09"
        },
        
        alertas: {
            contratos_retrasados: 0,
            sin_reportes_semana: false
        },
        
        timestamp: Timestamp,
        ultima_actualizacion: Timestamp
    }
    """
    print("\n" + "="*80)
    print("📊 AGREGACIÓN SEMANAL DE CONTRATOS")
    print("="*80)
    
    try:
        db = get_firestore_client()
        
        # 1. Obtener contratos desde endpoint unificado (incluye órdenes de compra)
        print("📥 Obteniendo contratos de empréstito...")
        import requests
        try:
            print("📡 Consultando endpoint /contratos_emprestito_all...")
            response = requests.get(
                'https://gestorproyectoapi-production.up.railway.app/contratos_emprestito_all',
                timeout=120
            )
            response.raise_for_status()
            data = response.json()
            contratos_data = data.get('data', [])
            print(f"✅ Obtenidos {len(contratos_data)} contratos unificados desde endpoint")
        except Exception as e:
            print(f"⚠️  Error al consultar endpoint: {e}")
            print("📥 Fallback: Obteniendo desde Firestore...")
            contratos_docs = list(db.collection('contratos_emprestito').stream())
            contratos_data = [doc.to_dict() for doc in contratos_docs]
            print(f"✅ Obtenidos {len(contratos_data)} contratos")
        
        # 2. Obtener TODOS los reportes
        print("📥 Obteniendo reportes de contratos...")
        reportes_ref = db.collection('reportes_contratos')
        reportes = list(reportes_ref.stream())
        print(f"✅ Obtenidos {len(reportes)} reportes")
        
        # 3. Agrupar reportes por referencia_contrato
        reportes_por_contrato = defaultdict(list)
        for reporte_doc in reportes:
            reporte = reporte_doc.to_dict()
            ref = reporte.get('referencia_contrato')
            if ref:
                reportes_por_contrato[ref].append(reporte)
        
        # 4. Agrupar contratos por semana
        agregaciones_semanales = defaultdict(lambda: {
            'contratos': [],
            'reportes': []
        })
        
        for contrato in contratos_data:
            # Los contratos ya vienen como diccionarios del endpoint
            
            # Obtener fecha para agrupar (usar fecha_inicio_contrato)
            fecha_inicio = parse_firebase_date(contrato.get('fecha_inicio_contrato'))
            if not fecha_inicio:
                continue
            
            # Calcular semana
            semana_info = get_week_number_and_dates(fecha_inicio)
            
            # Crear clave de agrupación: semana + bp + centro_gestor + banco
            bp = contrato.get('bp', 'Sin BP')
            centro_gestor = contrato.get('nombre_centro_gestor', 'Sin Centro Gestor')
            banco = contrato.get('banco', 'Sin Banco')
            
            clave = f"{semana_info['semana_id']}_{bp}_{centro_gestor}_{banco}"
            
            # Agregar contrato y sus reportes
            agregaciones_semanales[clave]['contratos'].append(contrato)
            agregaciones_semanales[clave]['semana_info'] = semana_info
            agregaciones_semanales[clave]['bp'] = bp
            agregaciones_semanales[clave]['centro_gestor'] = centro_gestor
            agregaciones_semanales[clave]['banco'] = banco
            
            # Agregar reportes relacionados
            ref_contrato = contrato.get('referencia_contrato')
            if ref_contrato and ref_contrato in reportes_por_contrato:
                agregaciones_semanales[clave]['reportes'].extend(reportes_por_contrato[ref_contrato])
        
        print(f"📊 Generadas {len(agregaciones_semanales)} agregaciones semanales únicas")
        
        # 5. Calcular métricas y guardar en Firestore
        collection_ref = db.collection('analytics_contratos_weekly')
        batch = db.batch()
        batch_count = 0
        total_guardados = 0
        
        for clave, datos in agregaciones_semanales.items():
            # Calcular métricas de contratos
            contratos_lista = datos['contratos']
            reportes_lista = datos['reportes']
            
            valor_total = sum(float(c.get('valor_contrato', 0)) for c in contratos_lista)
            # Usar valor_pagado del endpoint
            valor_pagado = sum(float(c.get('valor_pagado', 0) or 0) for c in contratos_lista)
            porcentaje_ejecucion = (valor_pagado / valor_total * 100) if valor_total > 0 else 0
            
            # Calcular métricas de reportes
            if reportes_lista:
                avances_fisicos = [float(r.get('avance_fisico', 0)) for r in reportes_lista]
                avances_financieros = [float(r.get('avance_financiero', 0)) for r in reportes_lista]
                
                avance_fisico_promedio = sum(avances_fisicos) / len(avances_fisicos)
                avance_financiero_promedio = sum(avances_financieros) / len(avances_financieros)
                
                # Último reporte
                fechas_reportes = [parse_firebase_date(r.get('fecha_reporte')) for r in reportes_lista]
                fechas_validas = [f for f in fechas_reportes if f]
                ultimo_reporte = max(fechas_validas).strftime('%Y-%m-%d') if fechas_validas else None
            else:
                avance_fisico_promedio = 0
                avance_financiero_promedio = 0
                ultimo_reporte = None
            
            # Detectar alertas
            contratos_retrasados = sum(1 for c in contratos_lista if c.get('tiene_retrasos', False))
            sin_reportes_semana = len(reportes_lista) == 0
            
            # Crear documento agregado
            doc_data = {
                'anio': datos['semana_info']['anio'],
                'semana': datos['semana_info']['semana'],
                'fecha_inicio_semana': datos['semana_info']['fecha_inicio_semana'],
                'fecha_fin_semana': datos['semana_info']['fecha_fin_semana'],
                'semana_id': datos['semana_info']['semana_id'],
                'bp': datos['bp'],
                'nombre_centro_gestor': datos['centro_gestor'],
                'banco': datos['banco'],
                
                'metricas_contratos': {
                    'cantidad_contratos': len(contratos_lista),
                    'valor_total_contratos': round(valor_total, 2),
                    'valor_pagado_total': round(valor_pagado, 2),
                    'porcentaje_ejecucion': round(porcentaje_ejecucion, 2)
                },
                
                'metricas_reportes': {
                    'cantidad_reportes': len(reportes_lista),
                    'avance_fisico_promedio': round(avance_fisico_promedio, 2),
                    'avance_financiero_promedio': round(avance_financiero_promedio, 2),
                    'ultimo_reporte': ultimo_reporte
                },
                
                'alertas': {
                    'contratos_retrasados': contratos_retrasados,
                    'sin_reportes_semana': sin_reportes_semana
                },
                
                'timestamp': firestore.SERVER_TIMESTAMP,
                'ultima_actualizacion': firestore.SERVER_TIMESTAMP,
                'version': '1.0'
            }
            
            # Agregar al batch
            doc_ref = collection_ref.document(clave)
            batch.set(doc_ref, doc_data, merge=True)
            batch_count += 1
            
            # Commit cada 500 documentos (límite de Firestore)
            if batch_count >= 500:
                batch.commit()
                total_guardados += batch_count
                print(f"💾 Guardados {total_guardados} documentos...")
                batch = db.batch()
                batch_count = 0
        
        # Commit final
        if batch_count > 0:
            batch.commit()
            total_guardados += batch_count
        
        print(f"\n✅ COMPLETADO: {total_guardados} agregaciones semanales guardadas en 'analytics_contratos_weekly'")
        return {
            'success': True,
            'total_agregaciones': total_guardados,
            'mensaje': f'Agregaciones semanales generadas exitosamente'
        }
        
    except Exception as e:
        print(f"\n❌ ERROR en agregación semanal de contratos: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }


# =============================================================================
# FUNCIÓN 2: AGREGACIÓN POR BANCO
# =============================================================================

@secure_log
def aggregate_flujo_caja_banco():
    """
    Genera agregaciones de flujo de caja por banco y trimestre.
    
    Colección destino: analytics_flujo_caja_banco
    Granularidad: Trimestral por Banco
    
    Estructura:
    {
        id: "2025-Q4_Bancolombia",
        anio: 2025,
        trimestre: 4,
        banco: "Bancolombia",
        
        metricas: {
            desembolsos_totales: 500000000000,
            cantidad_proyectos: 25,
            proyectos_activos: 20,
            promedio_desembolso_proyecto: 20000000000
        },
        
        proyectos: [
            {bp: "BP26005260", desembolso: 100000000, sector: "TIC"},
            ...
        ],
        
        por_sector: {
            "TIC": {desembolso: 300000000000, proyectos: 10},
            "Educación": {desembolso: 200000000000, proyectos: 15}
        },
        
        timestamp: Timestamp
    }
    """
    print("\n" + "="*80)
    print("💰 AGREGACIÓN DE FLUJO DE CAJA POR BANCO")
    print("="*80)
    
    try:
        db = get_firestore_client()
        
        # 1. Obtener todos los flujos de caja
        print("📥 Obteniendo flujos de caja...")
        flujos_ref = db.collection('flujo_caja_emprestito')
        flujos = list(flujos_ref.stream())
        print(f"✅ Obtenidos {len(flujos)} registros de flujo de caja")
        
        # 2. Agrupar por banco y trimestre
        agregaciones = defaultdict(lambda: {
            'desembolsos': [],
            'proyectos': set(),
            'por_sector': defaultdict(lambda: {'desembolso': 0, 'proyectos': set()})
        })
        
        for flujo_doc in flujos:
            flujo = flujo_doc.to_dict()
            
            banco = flujo.get('banco', 'Sin Banco')
            bp = flujo.get('bp_proyecto', 'Sin BP')
            desembolso = float(flujo.get('desembolso', 0))
            organismo = flujo.get('organismo', 'Sin Sector')
            
            # Obtener período y calcular trimestre
            periodo_str = flujo.get('periodo')
            if periodo_str:
                try:
                    periodo = datetime.fromisoformat(periodo_str.replace('Z', '+00:00'))
                    anio = periodo.year
                    trimestre = (periodo.month - 1) // 3 + 1
                    
                    clave = f"{anio}-Q{trimestre}_{banco}"
                    
                    # Agregar datos
                    agregaciones[clave]['banco'] = banco
                    agregaciones[clave]['anio'] = anio
                    agregaciones[clave]['trimestre'] = trimestre
                    agregaciones[clave]['desembolsos'].append({
                        'bp': bp,
                        'desembolso': desembolso,
                        'sector': organismo
                    })
                    agregaciones[clave]['proyectos'].add(bp)
                    
                    # Por sector
                    agregaciones[clave]['por_sector'][organismo]['desembolso'] += desembolso
                    agregaciones[clave]['por_sector'][organismo]['proyectos'].add(bp)
                    
                except:
                    continue
        
        print(f"📊 Generadas {len(agregaciones)} agregaciones por banco y trimestre")
        
        # 3. Calcular métricas y guardar
        collection_ref = db.collection('analytics_flujo_caja_banco')
        batch = db.batch()
        batch_count = 0
        total_guardados = 0
        
        for clave, datos in agregaciones.items():
            desembolsos_totales = sum(d['desembolso'] for d in datos['desembolsos'])
            cantidad_proyectos = len(datos['proyectos'])
            promedio_desembolso = desembolsos_totales / cantidad_proyectos if cantidad_proyectos > 0 else 0
            
            # Proyectos top 10 por desembolso
            proyectos_agrupados = defaultdict(float)
            for d in datos['desembolsos']:
                proyectos_agrupados[d['bp']] += d['desembolso']
            
            top_proyectos = sorted(
                [{'bp': bp, 'desembolso': total, 'sector': next((d['sector'] for d in datos['desembolsos'] if d['bp'] == bp), 'N/A')}
                 for bp, total in proyectos_agrupados.items()],
                key=lambda x: x['desembolso'],
                reverse=True
            )[:10]
            
            # Por sector
            por_sector_dict = {
                sector: {
                    'desembolso': round(info['desembolso'], 2),
                    'cantidad_proyectos': len(info['proyectos'])
                }
                for sector, info in datos['por_sector'].items()
            }
            
            doc_data = {
                'anio': datos['anio'],
                'trimestre': datos['trimestre'],
                'banco': datos['banco'],
                
                'metricas': {
                    'desembolsos_totales': round(desembolsos_totales, 2),
                    'cantidad_proyectos': cantidad_proyectos,
                    'promedio_desembolso_proyecto': round(promedio_desembolso, 2)
                },
                
                'top_proyectos': top_proyectos,
                'por_sector': por_sector_dict,
                
                'timestamp': firestore.SERVER_TIMESTAMP,
                'ultima_actualizacion': firestore.SERVER_TIMESTAMP,
                'version': '1.0'
            }
            
            doc_ref = collection_ref.document(clave)
            batch.set(doc_ref, doc_data, merge=True)
            batch_count += 1
            
            if batch_count >= 500:
                batch.commit()
                total_guardados += batch_count
                print(f"💾 Guardados {total_guardados} documentos...")
                batch = db.batch()
                batch_count = 0
        
        if batch_count > 0:
            batch.commit()
            total_guardados += batch_count
        
        print(f"\n✅ COMPLETADO: {total_guardados} agregaciones por banco guardadas en 'analytics_flujo_caja_banco'")
        return {
            'success': True,
            'total_agregaciones': total_guardados
        }
        
    except Exception as e:
        print(f"\n❌ ERROR en agregación de flujo de caja por banco: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }


# =============================================================================
# FUNCIÓN 3: KPIs GLOBALES DEL DASHBOARD
# =============================================================================

@secure_log
def calculate_global_kpis():
    """
    Calcula KPIs globales del dashboard.
    
    Colección destino: analytics_kpi_dashboard
    Documento único: "global_kpis"
    
    Estructura optimizada para EmprestitoAdvancedDashboard.tsx
    """
    print("\n" + "="*80)
    print("📈 CÁLCULO DE KPIs GLOBALES DEL DASHBOARD")
    print("="*80)
    
    try:
        db = get_firestore_client()
        
        # Obtener datos necesarios
        print("📥 Obteniendo datos...")
        
        # Usar endpoint unificado para obtener contratos (incluye órdenes de compra)
        import requests
        try:
            print("📡 Consultando endpoint /contratos_emprestito_all...")
            response = requests.get(
                'https://gestorproyectoapi-production.up.railway.app/contratos_emprestito_all',
                timeout=120
            )
            response.raise_for_status()
            data = response.json()
            contratos_data = data.get('data', [])
            print(f"✅ Obtenidos {len(contratos_data)} contratos unificados desde endpoint")
        except Exception as e:
            print(f"⚠️  Error al consultar endpoint: {e}")
            print("📥 Fallback: Obteniendo desde Firestore...")
            contratos_docs = list(db.collection('contratos_emprestito').stream())
            contratos_data = [doc.to_dict() for doc in contratos_docs]
        
        reportes = list(db.collection('reportes_contratos').stream())
        proyectos = list(db.collection('proyectos_presupuestales').stream())
        unidades = list(db.collection('unidades_proyecto').stream())
        
        print(f"✅ Datos obtenidos: {len(contratos_data)} contratos, {len(reportes)} reportes")
        
        # Calcular KPIs globales
        contratos_activos = [c for c in contratos_data if c.get('estado_contrato') == 'En ejecución']
        
        valor_total_contratos = sum(float(c.get('valor_contrato', 0)) for c in contratos_activos)
        valor_pagado_total = sum(float(c.get('valor_pagado', 0) or 0) for c in contratos_activos)
        porcentaje_global = (valor_pagado_total / valor_total_contratos * 100) if valor_total_contratos > 0 else 0
        
        # Por banco
        por_banco = defaultdict(lambda: {'cantidad': 0, 'valor': 0, 'pagado': 0})
        for c in contratos_activos:
            banco = c.get('banco', 'Sin Banco')
            por_banco[banco]['cantidad'] += 1
            por_banco[banco]['valor'] += float(c.get('valor_contrato', 0))
            por_banco[banco]['pagado'] += float(c.get('valor_pagado', 0) or 0)
        
        # Por organismo (centro gestor)
        por_organismo = defaultdict(lambda: {'cantidad': 0, 'valor': 0})
        for c in contratos_activos:
            organismo = c.get('nombre_centro_gestor', 'Sin Centro Gestor')
            por_organismo[organismo]['cantidad'] += 1
            por_organismo[organismo]['valor'] += float(c.get('valor_contrato', 0))
        
        # Calcular promedio PONDERADO de avances (por valor_contrato)
        # Crear mapa de contratos por referencia
        contrato_map = {c.get('referencia_contrato'): c for c in contratos_data}
        
        # Crear mapa de último reporte por contrato
        ultimo_reporte_por_contrato = {}
        for reporte_doc in reportes:
            reporte = reporte_doc.to_dict()
            ref = reporte.get('referencia_contrato')
            if ref:
                # Importar función de parseo de fecha
                from cloud_functions.emprestito_analytics import parse_firebase_date
                fecha_reporte = parse_firebase_date(reporte.get('fecha_reporte'))
                if ref not in ultimo_reporte_por_contrato or (
                    fecha_reporte and 
                    fecha_reporte > parse_firebase_date(ultimo_reporte_por_contrato[ref].get('fecha_reporte'))
                ):
                    ultimo_reporte_por_contrato[ref] = reporte
        
        # Calcular promedio ponderado por valor_contrato
        total_ponderado_fisico = 0
        total_ponderado_financiero = 0
        total_peso = 0
        
        for ref_contrato, reporte in ultimo_reporte_por_contrato.items():
            contrato = contrato_map.get(ref_contrato)
            if contrato:
                valor_contrato = float(contrato.get('valor_contrato', 0))
                avance_fisico = float(reporte.get('avance_fisico', 0))
                avance_financiero = float(reporte.get('avance_financiero', 0))
                
                total_ponderado_fisico += avance_fisico * valor_contrato
                total_ponderado_financiero += avance_financiero * valor_contrato
                total_peso += valor_contrato
        
        avance_fisico_promedio = (total_ponderado_fisico / total_peso) if total_peso > 0 else 0
        avance_financiero_promedio = (total_ponderado_financiero / total_peso) if total_peso > 0 else 0
        
        # Crear documento de KPIs
        kpis_doc = {
            'fecha_calculo': datetime.now().strftime('%Y-%m-%d'),
            
            'kpis_globales': {
                'total_contratos_activos': len(contratos_activos),
                'valor_total_contratos': round(valor_total_contratos, 2),
                'valor_pagado_total': round(valor_pagado_total, 2),
                'porcentaje_global_ejecucion': round(porcentaje_global, 2),
                'cantidad_proyectos': len(proyectos),
                'cantidad_unidades_proyecto': len(unidades),
                'cantidad_reportes': len(reportes),
                'avance_fisico_promedio': round(avance_fisico_promedio, 2),
                'avance_financiero_promedio': round(avance_financiero_promedio, 2)
            },
            
            'por_banco': {
                banco: {
                    'cantidad_contratos': info['cantidad'],
                    'valor_total': round(info['valor'], 2),
                    'valor_pagado': round(info['pagado'], 2),
                    'porcentaje_ejecucion': round((info['pagado'] / info['valor'] * 100) if info['valor'] > 0 else 0, 2)
                }
                for banco, info in por_banco.items()
            },
            
            'por_organismo': {
                org: {
                    'cantidad_contratos': info['cantidad'],
                    'valor_total': round(info['valor'], 2)
                }
                for org, info in sorted(por_organismo.items(), key=lambda x: x[1]['valor'], reverse=True)[:10]
            },
            
            'timestamp': firestore.SERVER_TIMESTAMP,
            'ultima_actualizacion': firestore.SERVER_TIMESTAMP,
            'version': '1.0'
        }
        
        # Guardar en Firestore
        db.collection('analytics_kpi_dashboard').document('global_kpis').set(kpis_doc, merge=True)
        
        print(f"\n✅ COMPLETADO: KPIs globales guardados en 'analytics_kpi_dashboard/global_kpis'")
        print(f"   - {len(contratos_activos)} contratos activos")
        print(f"   - ${valor_total_contratos:,.0f} valor total")
        print(f"   - {porcentaje_global:.2f}% ejecución global")
        
        return {
            'success': True,
            'kpis': kpis_doc
        }
        
    except Exception as e:
        print(f"\n❌ ERROR en cálculo de KPIs globales: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }


# =============================================================================
# FUNCIÓN PRINCIPAL - EJECUTAR TODAS LAS AGREGACIONES
# =============================================================================

def run_all_aggregations():
    """
    Ejecuta todas las agregaciones analíticas en secuencia.
    """
    print("\n" + "="*80)
    print("🚀 EJECUTANDO TODAS LAS AGREGACIONES ANALÍTICAS")
    print("="*80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    resultados = {}
    
    # 1. Agregación semanal de contratos
    print("\n[1/3] Agregación semanal de contratos...")
    resultados['contratos_weekly'] = aggregate_contratos_weekly()
    
    # 2. Agregación de flujo de caja por banco
    print("\n[2/3] Agregación de flujo de caja por banco...")
    resultados['flujo_caja_banco'] = aggregate_flujo_caja_banco()
    
    # 3. Cálculo de KPIs globales
    print("\n[3/3] Cálculo de KPIs globales...")
    resultados['kpis_globales'] = calculate_global_kpis()
    
    # Resumen final
    print("\n" + "="*80)
    print("📊 RESUMEN DE AGREGACIONES")
    print("="*80)
    
    for nombre, resultado in resultados.items():
        status = "✅ SUCCESS" if resultado.get('success') else "❌ ERROR"
        print(f"{status} - {nombre}")
        if not resultado.get('success'):
            print(f"    Error: {resultado.get('error', 'Unknown')}")
    
    print("="*80 + "\n")
    
    return resultados


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    run_all_aggregations()
