"""
Cloud Functions para Actualización de Colecciones Analíticas
===============================================================

Este módulo contiene Cloud Functions de Firebase que mantienen actualizadas
las colecciones analíticas en tiempo real, basándose en cambios en las colecciones operacionales.

Estrategia: Usar la MISMA base de datos con prefijo 'analytics_' para colecciones analíticas.

Colecciones Analíticas:
- analytics_contratos_monthly: Agregaciones mensuales de contratos y reportes
- analytics_kpi_dashboard: KPIs principales para dashboard empréstito
- analytics_avance_proyectos: Snapshots de avance de proyectos
- analytics_geoanalysis: Análisis geográfico por comuna

Autor: Sistema ETL Proyectos Cali
Fecha: 2025-11-09
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List
from collections import defaultdict

# Agregar path del proyecto
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.config import get_firestore_client, secure_log
from firebase_admin import firestore


@secure_log
def update_analytics_contratos_monthly() -> Dict[str, Any]:
    """
    Actualiza la colección analytics_contratos_monthly con agregaciones mensuales.
    
    Esta función:
    1. Obtiene todos los contratos de empréstito
    2. Obtiene todos los reportes de contratos
    3. Agrupa por mes/año/proyecto/banco
    4. Calcula métricas agregadas
    5. Guarda SOLO los meses/proyectos que han cambiado (carga incremental)
    
    Returns:
        Dict con estadísticas de la actualización
    """
    db = get_firestore_client()
    
    print("📊 Iniciando actualización de analytics_contratos_monthly...")
    
    # Obtener datos source
    contratos = list(db.collection('contratos_emprestito').stream())
    reportes = list(db.collection('reportes_contratos').stream())
    
    print(f"   📥 {len(contratos)} contratos y {len(reportes)} reportes obtenidos")
    
    # Crear mapa de contratos para acceso rápido
    contratos_map = {doc.id: doc.to_dict() for doc in contratos}
    
    # Agrupar por mes/año/BP/banco
    agregaciones = defaultdict(lambda: {
        'cantidad_contratos': 0,
        'valor_total_contratos': 0,
        'valor_pagado': 0,
        'suma_avance_fisico_ponderado': 0,
        'suma_avance_financiero_ponderado': 0,
        'suma_valores': 0,  # Para promedio ponderado
        'contratos_ids': set(),
        'ultima_actualizacion_dato': None
    })
    
    for reporte_doc in reportes:
        reporte = reporte_doc.to_dict()
        ref_contrato = reporte.get('referencia_contrato')
        
        # Buscar contrato correspondiente
        contrato = next((c for c in contratos_map.values() 
                        if c.get('referencia_contrato') == ref_contrato), None)
        
        if not contrato:
            continue
        
        fecha_reporte = reporte.get('fecha_reporte')
        if not fecha_reporte:
            continue
        
        # Parsear fecha
        if isinstance(fecha_reporte, str):
            fecha_dt = datetime.fromisoformat(fecha_reporte.replace('Z', '+00:00'))
        else:
            fecha_dt = fecha_reporte
        
        anio = fecha_dt.year
        mes = fecha_dt.month
        bp = contrato.get('bp', 'Sin BP')
        banco = contrato.get('banco', 'Sin Banco')
        centro_gestor = contrato.get('nombre_centro_gestor', 'Sin Centro Gestor')
        
        # Key de agregación
        key = f"{anio}-{mes:02d}_{bp}_{banco}"
        
        agg = agregaciones[key]
        
        # Si es la primera vez que vemos este contrato en este mes
        if ref_contrato not in agg['contratos_ids']:
            agg['contratos_ids'].add(ref_contrato)
            agg['cantidad_contratos'] += 1
            
            valor_contrato = float(contrato.get('valor_contrato', 0))
            agg['valor_total_contratos'] += valor_contrato
            agg['suma_valores'] += valor_contrato
        
        # Calcular valor pagado/ejecutado
        avance_financiero = float(reporte.get('avance_financiero', 0))
        avance_fisico = float(reporte.get('avance_fisico', 0))
        valor_contrato = float(contrato.get('valor_contrato', 0))
        
        # Valores ponderados para promedios
        agg['suma_avance_fisico_ponderado'] += (avance_fisico * valor_contrato)
        agg['suma_avance_financiero_ponderado'] += (avance_financiero * valor_contrato)
        agg['valor_pagado'] += (valor_contrato * avance_financiero / 100)
        
        # Metadata
        agg['anio'] = anio
        agg['mes'] = mes
        agg['bp'] = bp
        agg['banco'] = banco
        agg['centro_gestor'] = centro_gestor
        agg['nombre_proyecto'] = contrato.get('nombre_resumido_proceso', contrato.get('descripcion_proceso', ''))
        
        # Tracking de última actualización de datos source
        if not agg['ultima_actualizacion_dato'] or fecha_dt > agg['ultima_actualizacion_dato']:
            agg['ultima_actualizacion_dato'] = fecha_dt
    
    # Guardar agregaciones (INCREMENTAL: solo actualizar los que cambiaron)
    batch = db.batch()
    batch_count = 0
    docs_actualizados = 0
    docs_nuevos = 0
    
    analytics_collection = db.collection('analytics_contratos_monthly')
    
    for key, agg in agregaciones.items():
        # Calcular promedios ponderados
        promedio_avance_fisico = (agg['suma_avance_fisico_ponderado'] / agg['suma_valores']) if agg['suma_valores'] > 0 else 0
        promedio_avance_financiero = (agg['suma_avance_financiero_ponderado'] / agg['suma_valores']) if agg['suma_valores'] > 0 else 0
        porcentaje_ejecucion = (agg['valor_pagado'] / agg['valor_total_contratos'] * 100) if agg['valor_total_contratos'] > 0 else 0
        
        # Documento a guardar
        doc_data = {
            'id': key,
            'anio': agg['anio'],
            'mes': agg['mes'],
            'bp': agg['bp'],
            'nombre_proyecto': agg['nombre_proyecto'],
            'centro_gestor': agg['centro_gestor'],
            'banco': agg['banco'],
            'metricas': {
                'cantidad_contratos': agg['cantidad_contratos'],
                'valor_total_contratos': agg['valor_total_contratos'],
                'valor_pagado': agg['valor_pagado'],
                'porcentaje_ejecucion': round(porcentaje_ejecucion, 2),
                'promedio_avance_fisico': round(promedio_avance_fisico, 2),
                'promedio_avance_financiero': round(promedio_avance_financiero, 2)
            },
            'ultima_actualizacion': firestore.SERVER_TIMESTAMP,
            'ultima_actualizacion_dato': agg['ultima_actualizacion_dato']
        }
        
        # Verificar si necesitamos actualizar (INCREMENTAL)
        doc_ref = analytics_collection.document(key)
        existing_doc = doc_ref.get()
        
        if existing_doc.exists:
            # Comparar última actualización de dato
            existing_data = existing_doc.to_dict()
            existing_update = existing_data.get('ultima_actualizacion_dato')
            
            if existing_update and agg['ultima_actualizacion_dato']:
                # Solo actualizar si hay datos más nuevos
                if agg['ultima_actualizacion_dato'] > existing_update:
                    batch.set(doc_ref, doc_data)
                    batch_count += 1
                    docs_actualizados += 1
        else:
            # Documento nuevo
            batch.set(doc_ref, doc_data)
            batch_count += 1
            docs_nuevos += 1
        
        # Commit batch cada 500 documentos
        if batch_count >= 500:
            batch.commit()
            print(f"   💾 Batch de {batch_count} documentos guardado")
            batch = db.batch()
            batch_count = 0
    
    # Commit final
    if batch_count > 0:
        batch.commit()
        print(f"   💾 Batch final de {batch_count} documentos guardado")
    
    resultado = {
        'status': 'success',
        'docs_nuevos': docs_nuevos,
        'docs_actualizados': docs_actualizados,
        'total_agregaciones': len(agregaciones),
        'timestamp': datetime.now().isoformat()
    }
    
    print(f"✅ analytics_contratos_monthly actualizado: {docs_nuevos} nuevos, {docs_actualizados} actualizados")
    
    return resultado


@secure_log
def update_analytics_kpi_dashboard() -> Dict[str, Any]:
    """
    Actualiza la colección analytics_kpi_dashboard con KPIs principales.
    
    Esta función calcula y guarda:
    - KPIs globales (total proyectos, inversión, ejecución)
    - KPIs por organismo
    - KPIs por banco
    - KPIs por estado
    
    Documento único con fecha actual como ID.
    """
    db = get_firestore_client()
    
    print("📊 Iniciando actualización de analytics_kpi_dashboard...")
    
    # Obtener datos source
    contratos = list(db.collection('contratos_emprestito').stream())
    reportes = list(db.collection('reportes_contratos').stream())
    proyectos = list(db.collection('proyectos_presupuestales').stream())
    unidades = list(db.collection('unidades_proyecto').stream())
    
    print(f"   📥 Datos obtenidos: {len(contratos)} contratos, {len(reportes)} reportes, " +
          f"{len(proyectos)} proyectos, {len(unidades)} unidades")
    
    # Mapas para acceso rápido
    contratos_map = {doc.to_dict().get('referencia_contrato'): doc.to_dict() for doc in contratos}
    reportes_map = defaultdict(list)
    for reporte_doc in reportes:
        reporte = reporte_doc.to_dict()
        reportes_map[reporte.get('referencia_contrato')].append(reporte)
    
    # KPIs Globales
    kpis_globales = {
        'total_proyectos_activos': 0,
        'total_contratos_activos': 0,
        'inversion_total': 0,
        'valor_ejecutado': 0,
        'porcentaje_global_ejecucion': 0,
        'cantidad_ordenes_compra': 0,
        'promedio_avance_fisico': 0,
        'promedio_avance_financiero': 0
    }
    
    # KPIs por organismo
    por_organismo = defaultdict(lambda: {
        'proyectos': 0,
        'contratos': 0,
        'inversion': 0,
        'ejecutado': 0,
        'ejecucion': 0
    })
    
    # KPIs por banco
    por_banco = defaultdict(lambda: {
        'contratos': 0,
        'valor_asignado': 0,
        'valor_desembolsado': 0,
        'porcentaje_uso': 0
    })
    
    # KPIs por estado
    por_estado = defaultdict(int)
    
    # Procesar contratos
    suma_avance_fisico_ponderado = 0
    suma_avance_financiero_ponderado = 0
    suma_valores = 0
    
    for contrato in contratos_map.values():
        estado = contrato.get('estado_contrato', 'Sin Estado')
        centro_gestor = contrato.get('nombre_centro_gestor', 'Sin Centro Gestor')
        banco = contrato.get('banco', 'Sin Banco')
        valor_contrato = float(contrato.get('valor_contrato', 0))
        
        # Buscar último reporte
        reportes_contrato = reportes_map.get(contrato.get('referencia_contrato'), [])
        if reportes_contrato:
            ultimo_reporte = max(reportes_contrato, key=lambda r: r.get('fecha_reporte', ''))
            avance_fisico = float(ultimo_reporte.get('avance_fisico', 0))
            avance_financiero = float(ultimo_reporte.get('avance_financiero', 0))
        else:
            avance_fisico = 0
            avance_financiero = 0
        
        valor_ejecutado = valor_contrato * avance_financiero / 100
        
        # KPIs Globales
        if estado == 'En ejecución':
            kpis_globales['total_contratos_activos'] += 1
        kpis_globales['inversion_total'] += valor_contrato
        kpis_globales['valor_ejecutado'] += valor_ejecutado
        
        # Promedios ponderados
        suma_avance_fisico_ponderado += (avance_fisico * valor_contrato)
        suma_avance_financiero_ponderado += (avance_financiero * valor_contrato)
        suma_valores += valor_contrato
        
        # Por organismo
        por_organismo[centro_gestor]['contratos'] += 1
        por_organismo[centro_gestor]['inversion'] += valor_contrato
        por_organismo[centro_gestor]['ejecutado'] += valor_ejecutado
        
        # Por banco
        por_banco[banco]['contratos'] += 1
        por_banco[banco]['valor_asignado'] += valor_contrato
        por_banco[banco]['valor_desembolsado'] += valor_ejecutado
        
        # Por estado
        por_estado[estado] += 1
    
    # Calcular promedios globales ponderados
    if suma_valores > 0:
        kpis_globales['promedio_avance_fisico'] = round(suma_avance_fisico_ponderado / suma_valores, 2)
        kpis_globales['promedio_avance_financiero'] = round(suma_avance_financiero_ponderado / suma_valores, 2)
        kpis_globales['porcentaje_global_ejecucion'] = round(kpis_globales['valor_ejecutado'] / kpis_globales['inversion_total'] * 100, 2)
    
    # Contar proyectos activos
    kpis_globales['total_proyectos_activos'] = len([p for p in proyectos if p.to_dict().get('anio', 0) >= datetime.now().year - 1])
    
    # Contar órdenes de compra
    ordenes_compra = db.collection('ordenes_compra_emprestito').where('estado', '==', 'activo').stream()
    kpis_globales['cantidad_ordenes_compra'] = len(list(ordenes_compra))
    
    # Calcular porcentajes por organismo y banco
    for org_data in por_organismo.values():
        if org_data['inversion'] > 0:
            org_data['ejecucion'] = round(org_data['ejecutado'] / org_data['inversion'] * 100, 2)
    
    for banco_data in por_banco.values():
        if banco_data['valor_asignado'] > 0:
            banco_data['porcentaje_uso'] = round(banco_data['valor_desembolsado'] / banco_data['valor_asignado'] * 100, 2)
    
    # Documento KPI
    fecha_hoy = datetime.now().strftime('%Y-%m-%d')
    doc_data = {
        'fecha': fecha_hoy,
        'kpis_globales': kpis_globales,
        'por_organismo': dict(por_organismo),
        'por_banco': dict(por_banco),
        'por_estado': dict(por_estado),
        'timestamp': firestore.SERVER_TIMESTAMP
    }
    
    # Guardar (sobrescribe el del día si ya existe)
    db.collection('analytics_kpi_dashboard').document(fecha_hoy).set(doc_data)
    
    print(f"✅ analytics_kpi_dashboard actualizado para {fecha_hoy}")
    
    return {
        'status': 'success',
        'fecha': fecha_hoy,
        'kpis_globales': kpis_globales,
        'timestamp': datetime.now().isoformat()
    }


@secure_log
def update_analytics_avance_proyectos() -> Dict[str, Any]:
    """
    Actualiza la colección analytics_avance_proyectos con snapshots de avance.
    
    Crea un snapshot del avance actual de cada unidad de proyecto.
    Útil para tracking histórico de evolución.
    """
    db = get_firestore_client()
    
    print("📊 Iniciando actualización de analytics_avance_proyectos...")
    
    # Obtener unidades de proyecto
    unidades = list(db.collection('unidades_proyecto').stream())
    
    print(f"   📥 {len(unidades)} unidades de proyecto obtenidas")
    
    fecha_hoy = datetime.now().strftime('%Y-%m-%d')
    batch = db.batch()
    batch_count = 0
    docs_procesados = 0
    
    analytics_collection = db.collection('analytics_avance_proyectos')
    
    for unidad_doc in unidades:
        unidad = unidad_doc.to_dict()
        properties = unidad.get('properties', {})
        
        upid = properties.get('upid')
        if not upid:
            continue
        
        estado = properties.get('estado')
        if estado not in ['En ejecución', 'En proceso', 'Activo']:
            continue
        
        avance_fisico = float(properties.get('avance_obra', 0))
        
        # ID único: upid_fecha
        doc_id = f"{upid}_{fecha_hoy}"
        
        # Verificar si ya existe (INCREMENTAL)
        doc_ref = analytics_collection.document(doc_id)
        if doc_ref.get().exists:
            continue  # Ya existe snapshot para hoy
        
        # Documento snapshot
        doc_data = {
            'upid': upid,
            'fecha_snapshot': fecha_hoy,
            'avance_fisico': avance_fisico,
            'estado': estado,
            'nombre_proyecto': properties.get('nombre_up', ''),
            'centro_gestor': properties.get('nombre_centro_gestor', ''),
            'bp': properties.get('bpin', ''),
            'valor_proyecto': float(properties.get('presupuesto_base', 0)),
            'semaforo': 'Verde' if avance_fisico >= 70 else 'Amarillo' if avance_fisico >= 40 else 'Rojo',
            'timestamp': firestore.SERVER_TIMESTAMP
        }
        
        batch.set(doc_ref, doc_data)
        batch_count += 1
        docs_procesados += 1
        
        # Commit batch cada 500
        if batch_count >= 500:
            batch.commit()
            print(f"   💾 Batch de {batch_count} snapshots guardado")
            batch = db.batch()
            batch_count = 0
    
    # Commit final
    if batch_count > 0:
        batch.commit()
        print(f"   💾 Batch final de {batch_count} snapshots guardado")
    
    print(f"✅ analytics_avance_proyectos actualizado: {docs_procesados} snapshots nuevos")
    
    return {
        'status': 'success',
        'snapshots_nuevos': docs_procesados,
        'fecha': fecha_hoy,
        'timestamp': datetime.now().isoformat()
    }


@secure_log
def update_analytics_geoanalysis() -> Dict[str, Any]:
    """
    Actualiza la colección analytics_geoanalysis con análisis geográfico.
    
    Agrupa datos por comuna/corregimiento para análisis geoespacial.
    """
    db = get_firestore_client()
    
    print("📊 Iniciando actualización de analytics_geoanalysis...")
    
    # Obtener unidades de proyecto
    unidades = list(db.collection('unidades_proyecto').stream())
    
    print(f"   📥 {len(unidades)} unidades de proyecto obtenidas")
    
    # Agrupar por comuna
    por_comuna = defaultdict(lambda: {
        'cantidad_proyectos': 0,
        'inversion_total': 0,
        'proyectos_completados': 0,
        'proyectos_en_ejecucion': 0,
        'proyectos_planeados': 0,
        'por_tipo_intervencion': defaultdict(lambda: {'cantidad': 0, 'inversion': 0}),
        'bounds': {'lat_min': float('inf'), 'lat_max': float('-inf'), 
                   'lon_min': float('inf'), 'lon_max': float('-inf')}
    })
    
    anio_actual = datetime.now().year
    
    for unidad_doc in unidades:
        unidad = unidad_doc.to_dict()
        properties = unidad.get('properties', {})
        geometry = unidad.get('geometry', {})
        
        comuna = properties.get('comuna_corregimiento', 'Sin Comuna')
        tipo_intervencion = properties.get('tipo_intervencion', 'Sin Tipo')
        estado = properties.get('estado', 'Sin Estado')
        presupuesto = float(properties.get('presupuesto_base', 0))
        
        agg = por_comuna[comuna]
        
        # Conteos
        agg['cantidad_proyectos'] += 1
        agg['inversion_total'] += presupuesto
        
        if estado == 'Completado':
            agg['proyectos_completados'] += 1
        elif estado in ['En ejecución', 'En proceso']:
            agg['proyectos_en_ejecucion'] += 1
        else:
            agg['proyectos_planeados'] += 1
        
        # Por tipo de intervención
        agg['por_tipo_intervencion'][tipo_intervencion]['cantidad'] += 1
        agg['por_tipo_intervencion'][tipo_intervencion]['inversion'] += presupuesto
        
        # Bounds geográficos
        if geometry and geometry.get('type') == 'Point':
            coords = geometry.get('coordinates', [])
            if len(coords) == 2:
                lon, lat = coords
                agg['bounds']['lat_min'] = min(agg['bounds']['lat_min'], lat)
                agg['bounds']['lat_max'] = max(agg['bounds']['lat_max'], lat)
                agg['bounds']['lon_min'] = min(agg['bounds']['lon_min'], lon)
                agg['bounds']['lon_max'] = max(agg['bounds']['lon_max'], lon)
    
    # Guardar análisis por comuna
    batch = db.batch()
    batch_count = 0
    
    analytics_collection = db.collection('analytics_geoanalysis')
    
    for comuna, agg in por_comuna.items():
        doc_id = f"{anio_actual}_{comuna.replace('/', '_').replace(' ', '_')}"
        
        # Limpiar bounds infinitos
        if agg['bounds']['lat_min'] == float('inf'):
            agg['bounds'] = None
        
        doc_data = {
            'id': doc_id,
            'anio': anio_actual,
            'comuna_corregimiento': comuna,
            'estadisticas': {
                'cantidad_proyectos': agg['cantidad_proyectos'],
                'inversion_total': agg['inversion_total'],
                'proyectos_completados': agg['proyectos_completados'],
                'proyectos_en_ejecucion': agg['proyectos_en_ejecucion'],
                'proyectos_planeados': agg['proyectos_planeados']
            },
            'por_tipo_intervencion': dict(agg['por_tipo_intervencion']),
            'bounds': agg['bounds'],
            'timestamp': firestore.SERVER_TIMESTAMP
        }
        
        batch.set(analytics_collection.document(doc_id), doc_data)
        batch_count += 1
        
        # Commit batch cada 500
        if batch_count >= 500:
            batch.commit()
            print(f"   💾 Batch de {batch_count} comunas guardado")
            batch = db.batch()
            batch_count = 0
    
    # Commit final
    if batch_count > 0:
        batch.commit()
        print(f"   💾 Batch final de {batch_count} comunas guardado")
    
    print(f"✅ analytics_geoanalysis actualizado: {len(por_comuna)} comunas procesadas")
    
    return {
        'status': 'success',
        'comunas_procesadas': len(por_comuna),
        'anio': anio_actual,
        'timestamp': datetime.now().isoformat()
    }


@secure_log
def run_all_analytics_updates() -> Dict[str, Any]:
    """
    Ejecuta todas las actualizaciones analíticas en secuencia.
    
    Esta función puede ser programada para correr diariamente.
    """
    print("\n" + "="*60)
    print("🚀 INICIANDO ACTUALIZACIÓN COMPLETA DE ANALYTICS")
    print("="*60 + "\n")
    
    resultados = {
        'inicio': datetime.now().isoformat(),
        'funciones': {}
    }
    
    try:
        # 1. Contratos Monthly
        resultados['funciones']['contratos_monthly'] = update_analytics_contratos_monthly()
    except Exception as e:
        print(f"❌ Error en contratos_monthly: {e}")
        resultados['funciones']['contratos_monthly'] = {'status': 'error', 'error': str(e)}
    
    try:
        # 2. KPI Dashboard
        resultados['funciones']['kpi_dashboard'] = update_analytics_kpi_dashboard()
    except Exception as e:
        print(f"❌ Error en kpi_dashboard: {e}")
        resultados['funciones']['kpi_dashboard'] = {'status': 'error', 'error': str(e)}
    
    try:
        # 3. Avance Proyectos
        resultados['funciones']['avance_proyectos'] = update_analytics_avance_proyectos()
    except Exception as e:
        print(f"❌ Error en avance_proyectos: {e}")
        resultados['funciones']['avance_proyectos'] = {'status': 'error', 'error': str(e)}
    
    try:
        # 4. Geoanalysis
        resultados['funciones']['geoanalysis'] = update_analytics_geoanalysis()
    except Exception as e:
        print(f"❌ Error en geoanalysis: {e}")
        resultados['funciones']['geoanalysis'] = {'status': 'error', 'error': str(e)}
    
    resultados['fin'] = datetime.now().isoformat()
    
    print("\n" + "="*60)
    print("✅ ACTUALIZACIÓN COMPLETA DE ANALYTICS FINALIZADA")
    print("="*60 + "\n")
    
    return resultados


# Ejecutar si se llama directamente
if __name__ == "__main__":
    resultado = run_all_analytics_updates()
    
    print("\n📋 RESUMEN DE EJECUCIÓN:")
    print("-" * 60)
    for funcion, resultado_func in resultado['funciones'].items():
        status = resultado_func.get('status', 'unknown')
        emoji = "✅" if status == 'success' else "❌"
        print(f"{emoji} {funcion}: {status}")
    print("-" * 60)
