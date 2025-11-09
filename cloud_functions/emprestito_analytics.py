"""
Cloud Functions Adicionales para Análisis de Empréstito
Colecciones optimizadas para EmprestitoAdvancedDashboard

Este módulo complementa analytics_aggregations.py con funciones específicas
para el dashboard de empréstito que replica EXACTAMENTE los cálculos del frontend.

Colecciones generadas:
- analytics_emprestito_por_banco
- analytics_emprestito_por_centro_gestor  
- analytics_emprestito_resumen_anual
- analytics_emprestito_series_temporales_diarias

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

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.config import get_firestore_client, secure_log


def parse_firebase_date(date_value) -> Optional[datetime]:
    """
    Parsea una fecha de Firebase a datetime.
    
    Soporta:
    - DatetimeWithNanoseconds (objeto de Firestore)
    - datetime (objeto Python)
    - str (formato ISO o YYYY-MM-DD)
    """
    if not date_value:
        return None
    
    try:
        # Si ya es datetime (incluyendo DatetimeWithNanoseconds de Firestore)
        if isinstance(date_value, datetime):
            return date_value
        
        # Si es string, parsear
        if isinstance(date_value, str):
            if 'T' in date_value:
                return datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            return datetime.strptime(date_value.split('T')[0], '%Y-%m-%d')
        
        # Intentar convertir a datetime si tiene método similar
        if hasattr(date_value, 'timestamp'):
            return datetime.fromtimestamp(date_value.timestamp())
            
        return None
    except Exception as e:
        print(f"⚠️  Error parseando fecha {date_value}: {e}")
        return None


def normalize_orden_compra_to_contrato(orden: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza una orden de compra al formato de contrato para procesamiento unificado.
    
    Mapeo de campos:
    - numero_orden → referencia_contrato
    - nombre_banco → banco
    - valor_orden → valor_contrato
    - estado_orden → estado_contrato
    - fecha_publicacion_orden → fecha_inicio_contrato
    - sector → sector
    - nombre_centro_gestor → nombre_centro_gestor
    """
    return {
        'referencia_contrato': orden.get('numero_orden', f"OC-{orden.get('solicitud_id', 'UNKNOWN')}"),
        'banco': orden.get('nombre_banco', 'Sin definir'),
        'nombre_centro_gestor': orden.get('nombre_centro_gestor', 'Sin definir'),
        'valor_contrato': float(orden.get('valor_orden', 0)),
        'estado_contrato': orden.get('estado_orden', 'Sin estado'),
        'sector': orden.get('sector', 'Sin definir'),
        'fecha_inicio_contrato': orden.get('fecha_publicacion_orden', ''),
        'tipo_contrato': 'Orden de Compra',  # Identificador del tipo
        'nombre_resumido_proceso': orden.get('nombre_resumido_proceso', orden.get('objeto_orden', 'Orden de Compra')),
        'bp': orden.get('bp', ''),
        # Campos específicos de orden de compra
        '_es_orden_compra': True,
        '_numero_orden': orden.get('numero_orden'),
        '_solicitud_id': orden.get('solicitud_id'),
        '_nombre_proveedor': orden.get('nombre_proveedor'),
        '_modalidad': orden.get('modalidad_contratacion'),
        '_fecha_vencimiento': orden.get('fecha_vencimiento_orden')
    }


def get_contratos_unificados(db) -> List[Dict[str, Any]]:
    """
    Obtiene contratos y órdenes de compra unificados en una sola lista.
    
    OPTIMIZACIÓN: En lugar de hacer 2 queries a Firestore (contratos_emprestito + ordenes_compra_emprestito),
    usamos directamente el endpoint /contratos_emprestito_all que ya unifica ambas colecciones.
    
    Returns:
        Lista de contratos + órdenes de compra ya unificadas
    """
    import requests
    
    try:
        # Usar endpoint que ya unifica contratos y órdenes de compra
        print("📡 Consultando endpoint /contratos_emprestito_all (contratos + órdenes de compra unificados)...")
        response = requests.get(
            'https://gestorproyectoapi-production.up.railway.app/contratos_emprestito_all',
            timeout=120
        )
        response.raise_for_status()
        
        data = response.json()
        contratos = data.get('data', [])
        
        print(f"✅ Obtenidos {len(contratos)} contratos unificados desde endpoint")
        return contratos
        
    except Exception as e:
        print(f"⚠️ Error consultando endpoint, usando Firestore como fallback: {e}")
        
        # Fallback: consultar directamente Firestore
        contratos = []
        for doc in db.collection('contratos_emprestito').stream():
            contrato_data = doc.to_dict()
            contrato_data['_es_orden_compra'] = False
            contratos.append(contrato_data)
        
        # Obtener órdenes de compra y normalizar
        for doc in db.collection('ordenes_compra_emprestito').stream():
            orden_data = doc.to_dict()
            contrato_normalizado = normalize_orden_compra_to_contrato(orden_data)
            contratos.append(contrato_normalizado)
        
        print(f"✅ Obtenidos {len(contratos)} contratos desde Firestore (fallback)")
        return contratos


# =============================================================================
# FUNCIÓN 1: ANÁLISIS POR BANCO (AnalysisByBank del frontend)
# =============================================================================

@secure_log
def aggregate_analysis_by_banco():
    """
    Genera análisis consolidado por banco.
    Replica la lógica de analysisByBank del frontend.
    
    Colección: analytics_emprestito_por_banco
    Document ID: {nombre_banco}
    
    Campos:
    - banco: nombre del banco
    - totalContratos: cantidad de contratos
    - valorAsignadoBanco: suma de valor_asignado_banco de bancos_emprestito
    - valorAdjudicado: suma de valor_contrato de contratos_emprestito
    - valorEjecutado: calculado desde reportes (avance_financiero * valor_contrato)
    - valorPagado: obtenido del endpoint /contratos_emprestito_all (campo valor_pagado)
    - porcentajeEjecucion: (valorEjecutado / valorAdjudicado) * 100
    - promedioAvanceFisico: promedio ponderado por valor_contrato
    - promedioAvanceFinanciero: promedio ponderado por valor_contrato
    - contratos: lista de referencias de contratos
    - centrosGestores: lista de centros gestores únicos
    """
    print("\n" + "="*80)
    print("🏦 ANÁLISIS POR BANCO")
    print("="*80)
    
    try:
        db = get_firestore_client()
        
        # 1. Obtener datos (contratos + órdenes de compra desde endpoint optimizado)
        print("📥 Obteniendo datos...")
        contratos = get_contratos_unificados(db)
        
        # Obtener reportes (TODO: también se puede optimizar con endpoint)
        reportes = list(db.collection('reportes_contratos').stream())
        
        # Obtener bancos (TODO: también se puede optimizar con endpoint)
        bancos_emprestito = list(db.collection('bancos_emprestito').stream())
        
        print(f"✅ Datos obtenidos: {len(contratos)} contratos unificados, {len(reportes)} reportes, {len(bancos_emprestito)} bancos")
        
        # 2. Crear mapa de reportes por contrato (último reporte)
        ultimo_reporte_por_contrato = {}
        for reporte_doc in reportes:
            # Manejar tanto documentos de Firestore como diccionarios del endpoint
            reporte = reporte_doc.to_dict() if hasattr(reporte_doc, 'to_dict') else reporte_doc
            ref = reporte.get('referencia_contrato')
            if ref:
                fecha_reporte = parse_firebase_date(reporte.get('fecha_reporte'))
                if ref not in ultimo_reporte_por_contrato or (
                    fecha_reporte and 
                    fecha_reporte > parse_firebase_date(ultimo_reporte_por_contrato[ref].get('fecha_reporte'))
                ):
                    ultimo_reporte_por_contrato[ref] = reporte
        
        # 3. Agrupar por banco
        analisis_por_banco = defaultdict(lambda: {
            'totalContratos': 0,
            'valorAsignadoBanco': 0,
            'valorAdjudicado': 0,
            'valorEjecutado': 0,
            'valorPagado': 0,
            'total_ponderado_fisico': 0,
            'total_ponderado_financiero': 0,
            'total_peso': 0,
            'contratos': [],
            'contratos_detalle': [],  # NUEVO: array con metadatos completos
            'centros_gestores': set(),
            'estados_disponibles': set(),  # NUEVO: para filtro de estado
            'sectores_disponibles': set()  # NUEVO: para filtro de sector
        })
        
        # Procesar contratos (ya incluye órdenes de compra normalizadas)
        for contrato in contratos:
            banco = contrato.get('banco', 'Sin definir')
            valor_contrato = float(contrato.get('valor_contrato', 0))
            # Usar valor_pagado del endpoint (convertir string a float)
            valor_pagado_contrato = float(contrato.get('valor_pagado', 0) or 0)
            ref_contrato = contrato.get('referencia_contrato')
            estado = contrato.get('estado_contrato', 'Sin estado')
            sector = contrato.get('sector', 'Sin definir')
            fecha_inicio = contrato.get('fecha_inicio_contrato', '')
            centro_gestor = contrato.get('nombre_centro_gestor', 'Sin definir')
            
            analisis = analisis_por_banco[banco]
            analisis['totalContratos'] += 1
            analisis['valorAdjudicado'] += valor_contrato
            analisis['valorPagado'] += valor_pagado_contrato
            analisis['contratos'].append(ref_contrato)
            
            # NUEVO: Agregar contrato con metadatos completos para filtros
            contrato_info = {
                'referencia': ref_contrato,
                'estado': estado,
                'sector': sector,
                'centroGestor': centro_gestor,
                'valor': round(valor_contrato, 2),
                'fecha_inicio': fecha_inicio
            }
            
            # Buscar último reporte para agregar avances
            if ref_contrato in ultimo_reporte_por_contrato:
                reporte = ultimo_reporte_por_contrato[ref_contrato]
                contrato_info['avance_fisico'] = float(reporte.get('avance_fisico', 0))
                contrato_info['avance_financiero'] = float(reporte.get('avance_financiero', 0))
            else:
                contrato_info['avance_fisico'] = 0.0
                contrato_info['avance_financiero'] = 0.0
            
            analisis['contratos_detalle'].append(contrato_info)
            
            # Recolectar valores únicos para filtros
            analisis['estados_disponibles'].add(estado)
            analisis['sectores_disponibles'].add(sector)
            
            if contrato.get('nombre_centro_gestor'):
                analisis['centros_gestores'].add(contrato['nombre_centro_gestor'])
            
            # Calcular métricas solo si hay reporte (para promedio ponderado correcto)
            if ref_contrato in ultimo_reporte_por_contrato:
                reporte = ultimo_reporte_por_contrato[ref_contrato]
                avance_financiero = float(reporte.get('avance_financiero', 0))
                avance_fisico = float(reporte.get('avance_fisico', 0))
                
                # valorEjecutado se calcula con avance_financiero
                analisis['valorEjecutado'] += (valor_contrato * avance_financiero) / 100
                
                # Promedio ponderado: acumular (avance * peso) y el peso total
                # Solo se incluyen contratos con reporte para el cálculo del promedio
                analisis['total_ponderado_fisico'] += avance_fisico * valor_contrato
                analisis['total_ponderado_financiero'] += avance_financiero * valor_contrato
                analisis['total_peso'] += valor_contrato
        
        # Procesar valor_asignado_banco de bancos_emprestito
        for banco_doc in bancos_emprestito:
            # Manejar tanto documentos de Firestore como diccionarios del endpoint
            banco_data = banco_doc.to_dict() if hasattr(banco_doc, 'to_dict') else banco_doc
            nombre_banco = banco_data.get('nombre_banco', 'Sin definir')
            valor_asignado = float(banco_data.get('valor_asignado_banco', 0))
            
            if nombre_banco in analisis_por_banco:
                analisis_por_banco[nombre_banco]['valorAsignadoBanco'] += valor_asignado
        
        # 4. Calcular métricas finales y guardar
        collection_ref = db.collection('analytics_emprestito_por_banco')
        batch = db.batch()
        batch_count = 0
        total_guardados = 0
        
        for banco, analisis in analisis_por_banco.items():
            # Calcular promedios ponderados
            promedioAvanceFisico = (analisis['total_ponderado_fisico'] / analisis['total_peso'] 
                                   if analisis['total_peso'] > 0 else 0)
            promedioAvanceFinanciero = (analisis['total_ponderado_financiero'] / analisis['total_peso'] 
                                        if analisis['total_peso'] > 0 else 0)
            porcentajeEjecucion = ((analisis['valorEjecutado'] / analisis['valorAdjudicado']) * 100 
                                  if analisis['valorAdjudicado'] > 0 else 0)
            
            doc_data = {
                'banco': banco,
                'totalContratos': analisis['totalContratos'],
                'valorAsignadoBanco': round(analisis['valorAsignadoBanco'], 2),
                'valorAdjudicado': round(analisis['valorAdjudicado'], 2),
                'valorEjecutado': round(analisis['valorEjecutado'], 2),
                'valorPagado': round(analisis['valorPagado'], 2),
                'porcentajeEjecucion': round(porcentajeEjecucion, 2),
                'promedioAvanceFisico': round(promedioAvanceFisico, 2),
                'promedioAvanceFinanciero': round(promedioAvanceFinanciero, 2),
                'contratos': analisis['contratos'],
                'contratos_detalle': sorted(analisis['contratos_detalle'], 
                                           key=lambda x: x['valor'], reverse=True),  # NUEVO: ordenado por valor
                'centrosGestores': sorted(list(analisis['centros_gestores'])),
                'estadosDisponibles': sorted(list(analisis['estados_disponibles'])),  # NUEVO: para filtro UI
                'sectoresDisponibles': sorted(list(analisis['sectores_disponibles'])),  # NUEVO: para filtro UI
                'timestamp': firestore.SERVER_TIMESTAMP,
                'ultima_actualizacion': firestore.SERVER_TIMESTAMP,
                'version': '1.1'  # Actualizado versión
            }
            
            # Usar nombre del banco como ID del documento (sanitizar)
            banco_sanitizado = banco.strip().replace('/', '_').replace(' ', '_').replace('\\', '_')
            if not banco_sanitizado or banco_sanitizado == 'Sin_definir':
                banco_sanitizado = f"Sin_Banco_{hash(banco) % 10000}"
            doc_ref = collection_ref.document(banco_sanitizado)
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
        
        print(f"\n✅ COMPLETADO: {total_guardados} análisis por banco guardados")
        return {
            'success': True,
            'total_bancos': total_guardados
        }
        
    except Exception as e:
        print(f"\n❌ ERROR en análisis por banco: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }


# =============================================================================
# FUNCIÓN 2: ANÁLISIS POR CENTRO GESTOR
# =============================================================================

@secure_log
def aggregate_analysis_by_centro_gestor():
    """
    Genera análisis consolidado por centro gestor.
    Replica la lógica de analysisByCentroGestor del frontend.
    
    Colección: analytics_emprestito_por_centro_gestor
    Document ID: {nombre_centro_gestor}
    """
    print("\n" + "="*80)
    print("🏢 ANÁLISIS POR CENTRO GESTOR")
    print("="*80)
    
    try:
        db = get_firestore_client()
        
        # Obtener datos (contratos + órdenes de compra)
        print("📥 Obteniendo datos...")
        contratos = get_contratos_unificados(db)
        reportes = list(db.collection('reportes_contratos').stream())
        bancos_emprestito = list(db.collection('bancos_emprestito').stream())
        
        print(f"✅ Datos obtenidos: {len(contratos)} contratos unificados, {len(reportes)} reportes, {len(bancos_emprestito)} bancos")
        
        # Crear mapa de reportes
        ultimo_reporte_por_contrato = {}
        for reporte_doc in reportes:
            # Manejar tanto documentos de Firestore como diccionarios del endpoint
            reporte = reporte_doc.to_dict() if hasattr(reporte_doc, 'to_dict') else reporte_doc
            ref = reporte.get('referencia_contrato')
            if ref:
                fecha_reporte = parse_firebase_date(reporte.get('fecha_reporte'))
                if ref not in ultimo_reporte_por_contrato or (
                    fecha_reporte and 
                    fecha_reporte > parse_firebase_date(ultimo_reporte_por_contrato[ref].get('fecha_reporte'))
                ):
                    ultimo_reporte_por_contrato[ref] = reporte
        
        # Agrupar por centro gestor
        analisis_por_centro = defaultdict(lambda: {
            'totalContratos': 0,
            'valorAsignadoBanco': 0,
            'valorAdjudicado': 0,
            'valorEjecutado': 0,
            'valorPagado': 0,
            'total_ponderado_fisico': 0,
            'total_ponderado_financiero': 0,
            'total_peso': 0,
            'sectores': set(),
            'estadosContratos': defaultdict(int),
            'contratos_detalle': [],  # NUEVO: array con metadatos completos
            'bancos_disponibles': set(),  # NUEVO: para filtro de banco
            'bancos': defaultdict(lambda: {
                'valorAsignado': 0,
                'valorAdjudicado': 0,
                'valorEjecutado': 0,
                'contratos': 0
            })
        })
        
        # Procesar contratos (ya incluye órdenes de compra normalizadas)
        for contrato in contratos:
            centro_gestor = contrato.get('nombre_centro_gestor', 'Sin definir')
            banco = contrato.get('banco', 'Sin definir')
            sector = contrato.get('sector', 'Sin definir')
            estado = contrato.get('estado_contrato', 'Sin estado')
            valor_contrato = float(contrato.get('valor_contrato', 0))
            # Usar valor_pagado del endpoint
            valor_pagado_contrato = float(contrato.get('valor_pagado', 0) or 0)
            ref_contrato = contrato.get('referencia_contrato')
            fecha_inicio = contrato.get('fecha_inicio_contrato', '')
            
            analisis = analisis_por_centro[centro_gestor]
            analisis['totalContratos'] += 1
            analisis['valorAdjudicado'] += valor_contrato
            analisis['valorPagado'] += valor_pagado_contrato
            analisis['sectores'].add(sector)
            analisis['estadosContratos'][estado] += 1
            analisis['bancos_disponibles'].add(banco)
            
            # NUEVO: Agregar contrato con metadatos completos
            contrato_info = {
                'referencia': ref_contrato,
                'banco': banco,
                'estado': estado,
                'sector': sector,
                'valor': round(valor_contrato, 2),
                'fecha_inicio': fecha_inicio
            }
            
            # Buscar último reporte para agregar avances
            if ref_contrato in ultimo_reporte_por_contrato:
                reporte = ultimo_reporte_por_contrato[ref_contrato]
                contrato_info['avance_fisico'] = float(reporte.get('avance_fisico', 0))
                contrato_info['avance_financiero'] = float(reporte.get('avance_financiero', 0))
            else:
                contrato_info['avance_fisico'] = 0.0
                contrato_info['avance_financiero'] = 0.0
            
            analisis['contratos_detalle'].append(contrato_info)
            
            # Analisis por banco dentro del centro gestor
            analisis['bancos'][banco]['valorAdjudicado'] += valor_contrato
            analisis['bancos'][banco]['contratos'] += 1
            
            # Calcular métricas solo si hay reporte (para promedio ponderado correcto)
            if ref_contrato in ultimo_reporte_por_contrato:
                reporte = ultimo_reporte_por_contrato[ref_contrato]
                avance_financiero = float(reporte.get('avance_financiero', 0))
                avance_fisico = float(reporte.get('avance_fisico', 0))
                valor_ejecutado = (valor_contrato * avance_financiero) / 100
                
                analisis['valorEjecutado'] += valor_ejecutado
                analisis['bancos'][banco]['valorEjecutado'] += valor_ejecutado
                
                # Promedio ponderado: acumular (avance * peso) y el peso total
                analisis['total_ponderado_fisico'] += avance_fisico * valor_contrato
                analisis['total_ponderado_financiero'] += avance_financiero * valor_contrato
                analisis['total_peso'] += valor_contrato
        
        # Procesar valor_asignado_banco
        for banco_doc in bancos_emprestito:
            # Manejar tanto documentos de Firestore como diccionarios del endpoint
            banco_data = banco_doc.to_dict() if hasattr(banco_doc, 'to_dict') else banco_doc
            nombre_banco = banco_data.get('nombre_banco', 'Sin definir')
            nombre_centro = banco_data.get('nombre_centro_gestor', 'Sin definir')
            valor_asignado = float(banco_data.get('valor_asignado_banco', 0))
            
            if nombre_centro in analisis_por_centro:
                analisis_por_centro[nombre_centro]['valorAsignadoBanco'] += valor_asignado
                analisis_por_centro[nombre_centro]['bancos'][nombre_banco]['valorAsignado'] += valor_asignado
        
        # Guardar
        collection_ref = db.collection('analytics_emprestito_por_centro_gestor')
        batch = db.batch()
        batch_count = 0
        total_guardados = 0
        
        for centro_gestor, analisis in analisis_por_centro.items():
            # Calcular promedios ponderados
            promedioAvanceFisico = (analisis['total_ponderado_fisico'] / analisis['total_peso'] 
                                   if analisis['total_peso'] > 0 else 0)
            promedioAvanceFinanciero = (analisis['total_ponderado_financiero'] / analisis['total_peso'] 
                                        if analisis['total_peso'] > 0 else 0)
            porcentajeEjecucion = ((analisis['valorEjecutado'] / analisis['valorAdjudicado']) * 100 
                                  if analisis['valorAdjudicado'] > 0 else 0)
            
            # Convertir bancos a lista
            bancos_list = [
                {
                    'nombre': banco,
                    'valorAsignado': round(datos['valorAsignado'], 2),
                    'valorAdjudicado': round(datos['valorAdjudicado'], 2),
                    'valorEjecutado': round(datos['valorEjecutado'], 2),
                    'contratos': datos['contratos']
                }
                for banco, datos in analisis['bancos'].items()
            ]
            
            doc_data = {
                'centroGestor': centro_gestor,
                'totalContratos': analisis['totalContratos'],
                'valorAsignadoBanco': round(analisis['valorAsignadoBanco'], 2),
                'valorAdjudicado': round(analisis['valorAdjudicado'], 2),
                'valorEjecutado': round(analisis['valorEjecutado'], 2),
                'valorPagado': round(analisis['valorPagado'], 2),
                'porcentajeEjecucion': round(porcentajeEjecucion, 2),
                'promedioAvanceFisico': round(promedioAvanceFisico, 2),
                'promedioAvanceFinanciero': round(promedioAvanceFinanciero, 2),
                'sectores': sorted(list(analisis['sectores'])),
                'estadosContratos': dict(analisis['estadosContratos']),
                'contratos_detalle': sorted(analisis['contratos_detalle'],
                                           key=lambda x: x['valor'], reverse=True),  # NUEVO: ordenado por valor
                'bancosDisponibles': sorted(list(analisis['bancos_disponibles'])),  # NUEVO: para filtro UI
                'bancos': sorted(bancos_list, key=lambda x: x['valorAdjudicado'], reverse=True),
                'timestamp': firestore.SERVER_TIMESTAMP,
                'ultima_actualizacion': firestore.SERVER_TIMESTAMP,
                'version': '1.1'  # Actualizado versión
            }
            
            # Sanitizar nombre del centro gestor para ID de documento
            centro_sanitizado = centro_gestor.strip().replace('/', '_').replace(' ', '_').replace('\\', '_')
            if not centro_sanitizado or centro_sanitizado == 'Sin_definir':
                centro_sanitizado = f"Sin_Centro_{hash(centro_gestor) % 10000}"
            doc_ref = collection_ref.document(centro_sanitizado)
            batch.set(doc_ref, doc_data, merge=True)
            batch_count += 1
            
            if batch_count >= 500:
                batch.commit()
                total_guardados += batch_count
                batch = db.batch()
                batch_count = 0
        
        if batch_count > 0:
            batch.commit()
            total_guardados += batch_count
        
        print(f"\n✅ COMPLETADO: {total_guardados} análisis por centro gestor guardados")
        return {
            'success': True,
            'total_centros': total_guardados
        }
        
    except Exception as e:
        print(f"\n❌ ERROR en análisis por centro gestor: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }


# =============================================================================
# FUNCIÓN 3: RESUMEN ANUAL
# =============================================================================

@secure_log
def aggregate_resumen_anual():
    """
    Genera resumen consolidado por año.
    Replica la lógica de YearlySummary del frontend.
    
    Colección: analytics_emprestito_resumen_anual
    Document ID: {año}
    """
    print("\n" + "="*80)
    print("📅 RESUMEN ANUAL")
    print("="*80)
    
    try:
        db = get_firestore_client()
        
        print("📥 Obteniendo datos...")
        contratos = get_contratos_unificados(db)
        reportes = list(db.collection('reportes_contratos').stream())
        
        print(f"✅ Datos obtenidos: {len(contratos)} contratos unificados, {len(reportes)} reportes")
        
        # Crear mapa de reportes
        ultimo_reporte_por_contrato = {}
        for reporte_doc in reportes:
            # Manejar tanto documentos de Firestore como diccionarios del endpoint
            reporte = reporte_doc.to_dict() if hasattr(reporte_doc, 'to_dict') else reporte_doc
            ref = reporte.get('referencia_contrato')
            if ref:
                fecha_reporte = parse_firebase_date(reporte.get('fecha_reporte'))
                if ref not in ultimo_reporte_por_contrato or (
                    fecha_reporte and 
                    fecha_reporte > parse_firebase_date(ultimo_reporte_por_contrato[ref].get('fecha_reporte'))
                ):
                    ultimo_reporte_por_contrato[ref] = reporte
        
        # Agrupar por año
        resumen_anual = defaultdict(lambda: {
            'totalContratos': 0,
            'valorTotalAsignado': 0,
            'valorTotalEjecutado': 0,
            'valorTotalPagado': 0,
            'valorTotalFisico': 0,
            'total_ponderado_fisico': 0,
            'total_ponderado_financiero': 0,
            'total_peso': 0,
            'contratos_detalle': [],  # NUEVO: array con metadatos
            'bancos_disponibles': set(),  # NUEVO: para filtro
            'centros_disponibles': set(),  # NUEVO: para filtro
            'estados_disponibles': set(),  # NUEVO: para filtro
            'sectores_disponibles': set()  # NUEVO: para filtro
        })
        
        for contrato in contratos:
            fecha_inicio = parse_firebase_date(contrato.get('fecha_inicio_contrato'))
            
            if fecha_inicio:
                year = str(fecha_inicio.year)
            else:
                year = 'Sin Año'
            
            valor_contrato = float(contrato.get('valor_contrato', 0))
            ref_contrato = contrato.get('referencia_contrato')
            banco = contrato.get('banco', 'Sin definir')
            centro_gestor = contrato.get('nombre_centro_gestor', 'Sin definir')
            estado = contrato.get('estado_contrato', 'Sin estado')
            sector = contrato.get('sector', 'Sin definir')
            
            resumen = resumen_anual[year]
            resumen['totalContratos'] += 1
            resumen['valorTotalAsignado'] += valor_contrato
            
            # NUEVO: Agregar contrato con metadatos
            contrato_info = {
                'referencia': ref_contrato,
                'banco': banco,
                'centroGestor': centro_gestor,
                'estado': estado,
                'sector': sector,
                'valor': round(valor_contrato, 2),
                'fecha_inicio': contrato.get('fecha_inicio_contrato', '')
            }
            
            # Recolectar valores únicos
            resumen['bancos_disponibles'].add(banco)
            resumen['centros_disponibles'].add(centro_gestor)
            resumen['estados_disponibles'].add(estado)
            resumen['sectores_disponibles'].add(sector)
            
            if ref_contrato in ultimo_reporte_por_contrato:
                reporte = ultimo_reporte_por_contrato[ref_contrato]
                avance_financiero = float(reporte.get('avance_financiero', 0))
                avance_fisico = float(reporte.get('avance_fisico', 0))
                
                contrato_info['avance_fisico'] = avance_fisico
                contrato_info['avance_financiero'] = avance_financiero
                
                resumen['valorTotalEjecutado'] += (valor_contrato * avance_financiero) / 100
                resumen['valorTotalFisico'] += (valor_contrato * avance_fisico) / 100
                resumen['total_ponderado_fisico'] += avance_fisico * valor_contrato
                resumen['total_ponderado_financiero'] += avance_financiero * valor_contrato
                resumen['total_peso'] += valor_contrato
            else:
                contrato_info['avance_fisico'] = 0.0
                contrato_info['avance_financiero'] = 0.0
            
            resumen['contratos_detalle'].append(contrato_info)
        
        # Guardar
        collection_ref = db.collection('analytics_emprestito_resumen_anual')
        batch = db.batch()
        batch_count = 0
        total_guardados = 0
        
        for year, resumen in resumen_anual.items():
            porcentajeFisicoPromedio = (resumen['total_ponderado_fisico'] / resumen['total_peso'] 
                                        if resumen['total_peso'] > 0 else 0)
            porcentajeFinancieroPromedio = (resumen['total_ponderado_financiero'] / resumen['total_peso'] 
                                            if resumen['total_peso'] > 0 else 0)
            
            doc_data = {
                'anio': year,
                'totalContratos': resumen['totalContratos'],
                'valorTotalAsignado': round(resumen['valorTotalAsignado'], 2),
                'valorTotalEjecutado': round(resumen['valorTotalEjecutado'], 2),
                'valorTotalPagado': round(resumen['valorTotalPagado'], 2),
                'valorTotalFisico': round(resumen['valorTotalFisico'], 2),
                'porcentajeFisicoPromedio': round(porcentajeFisicoPromedio, 2),
                'porcentajeFinancieroPromedio': round(porcentajeFinancieroPromedio, 2),
                'contratos_detalle': sorted(resumen['contratos_detalle'],
                                           key=lambda x: x['valor'], reverse=True),  # NUEVO
                'bancosDisponibles': sorted(list(resumen['bancos_disponibles'])),  # NUEVO
                'centrosDisponibles': sorted(list(resumen['centros_disponibles'])),  # NUEVO
                'estadosDisponibles': sorted(list(resumen['estados_disponibles'])),  # NUEVO
                'sectoresDisponibles': sorted(list(resumen['sectores_disponibles'])),  # NUEVO
                'timestamp': firestore.SERVER_TIMESTAMP,
                'ultima_actualizacion': firestore.SERVER_TIMESTAMP,
                'version': '1.1'  # Actualizado versión
            }
            
            doc_ref = collection_ref.document(year)
            batch.set(doc_ref, doc_data, merge=True)
            batch_count += 1
            
            if batch_count >= 500:
                batch.commit()
                total_guardados += batch_count
                batch = db.batch()
                batch_count = 0
        
        if batch_count > 0:
            batch.commit()
            total_guardados += batch_count
        
        print(f"\n✅ COMPLETADO: {total_guardados} resúmenes anuales guardados")
        return {
            'success': True,
            'total_anos': total_guardados
        }
        
    except Exception as e:
        print(f"\n❌ ERROR en resumen anual: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }


# =============================================================================
# FUNCIÓN 4: SERIES TEMPORALES DIARIAS
# =============================================================================

@secure_log
def aggregate_series_temporales_diarias():
    """
    Genera series temporales con agregación DIARIA.
    Replica la lógica de TimeSeriesData del frontend.
    
    Colección: analytics_emprestito_series_temporales_diarias
    Document ID: {fecha YYYY-MM-DD}
    """
    print("\n" + "="*80)
    print("📈 SERIES TEMPORALES DIARIAS")
    print("="*80)
    
    try:
        db = get_firestore_client()
        
        print("📥 Obteniendo datos...")
        contratos = get_contratos_unificados(db)
        reportes = list(db.collection('reportes_contratos').stream())
        
        print(f"✅ Datos obtenidos: {len(contratos)} contratos unificados, {len(reportes)} reportes")
        
        # Crear mapa de contratos (ya vienen como diccionarios del endpoint)
        contrato_map = {}
        for contrato in contratos:
            ref = contrato.get('referencia_contrato')
            if ref:
                contrato_map[ref] = contrato
        
        # Agrupar reportes por fecha
        series_por_fecha = defaultdict(lambda: {
            'valor_pagado': 0,
            'valor_contrato': 0,
            'contratos_count': 0,
            'total_avance_fisico': 0,
            'total_avance_financiero': 0,
            'total_peso': 0,  # NUEVO: peso total para promedio ponderado
            'contratos_detalle': [],  # NUEVO: contratos que reportaron en esta fecha
            'bancos_disponibles': set(),  # NUEVO: para filtro
            'centros_disponibles': set(),  # NUEVO: para filtro
            'estados_disponibles': set(),  # NUEVO: para filtro
            'sectores_disponibles': set()  # NUEVO: para filtro
        })
        
        for reporte_doc in reportes:
            # Manejar tanto documentos de Firestore como diccionarios del endpoint
            reporte = reporte_doc.to_dict() if hasattr(reporte_doc, 'to_dict') else reporte_doc
            fecha_reporte_raw = reporte.get('fecha_reporte')
            if not fecha_reporte_raw:
                continue
            
            # Convertir DatetimeWithNanoseconds a string YYYY-MM-DD
            fecha_reporte = parse_firebase_date(fecha_reporte_raw)
            if not fecha_reporte:
                continue
            fecha = fecha_reporte.strftime('%Y-%m-%d')
            
            ref_contrato = reporte.get('referencia_contrato')
            contrato = contrato_map.get(ref_contrato)
            
            if contrato:
                valor_contrato = float(contrato.get('valor_contrato', 0))
                # Usar valor_pagado del endpoint
                valor_pagado_contrato = float(contrato.get('valor_pagado', 0) or 0)
                avance_financiero = float(reporte.get('avance_financiero', 0))
                avance_fisico = float(reporte.get('avance_fisico', 0))
                banco = contrato.get('banco', 'Sin definir')
                centro_gestor = contrato.get('nombre_centro_gestor', 'Sin definir')
                estado = contrato.get('estado_contrato', 'Sin estado')
                sector = contrato.get('sector', 'Sin definir')
                
                series = series_por_fecha[fecha]
                series['valor_pagado'] += valor_pagado_contrato
                series['valor_contrato'] += valor_contrato
                series['contratos_count'] += 1
                # Promedio ponderado: acumular (avance * peso)
                series['total_avance_fisico'] += avance_fisico * valor_contrato
                series['total_avance_financiero'] += avance_financiero * valor_contrato
                series['total_peso'] += valor_contrato  # Peso total para dividir
                
                # NUEVO: Agregar información del contrato
                series['contratos_detalle'].append({
                    'referencia': ref_contrato,
                    'banco': banco,
                    'centroGestor': centro_gestor,
                    'estado': estado,
                    'sector': sector,
                    'valor': round(valor_contrato, 2),
                    'avance_fisico': avance_fisico,
                    'avance_financiero': avance_financiero
                })
                
                # Recolectar valores únicos
                series['bancos_disponibles'].add(banco)
                series['centros_disponibles'].add(centro_gestor)
                series['estados_disponibles'].add(estado)
                series['sectores_disponibles'].add(sector)
        
        # Guardar
        collection_ref = db.collection('analytics_emprestito_series_temporales_diarias')
        batch = db.batch()
        batch_count = 0
        total_guardados = 0
        
        for fecha, series in series_por_fecha.items():
            # Calcular promedio ponderado: dividir por el peso total
            avance_fisico_promedio = (series['total_avance_fisico'] / series['total_peso'] 
                                     if series['total_peso'] > 0 else 0)
            avance_financiero_promedio = (series['total_avance_financiero'] / series['total_peso'] 
                                         if series['total_peso'] > 0 else 0)
            
            doc_data = {
                'fecha': fecha,
                'valor_pagado': round(series['valor_pagado'], 2),
                'valor_contrato': round(series['valor_contrato'], 2),
                'contratos_count': series['contratos_count'],
                'avance_fisico_promedio': round(avance_fisico_promedio, 2),
                'avance_financiero_promedio': round(avance_financiero_promedio, 2),
                'contratos_detalle': sorted(series['contratos_detalle'],
                                           key=lambda x: x['valor'], reverse=True),  # NUEVO
                'bancosDisponibles': sorted(list(series['bancos_disponibles'])),  # NUEVO
                'centrosDisponibles': sorted(list(series['centros_disponibles'])),  # NUEVO
                'estadosDisponibles': sorted(list(series['estados_disponibles'])),  # NUEVO
                'sectoresDisponibles': sorted(list(series['sectores_disponibles'])),  # NUEVO
                'timestamp': firestore.SERVER_TIMESTAMP,
                'ultima_actualizacion': firestore.SERVER_TIMESTAMP,
                'version': '1.1'  # Actualizado versión
            }
            
            doc_ref = collection_ref.document(fecha)
            batch.set(doc_ref, doc_data, merge=True)
            batch_count += 1
            
            if batch_count >= 500:
                batch.commit()
                total_guardados += batch_count
                batch = db.batch()
                batch_count = 0
        
        if batch_count > 0:
            batch.commit()
            total_guardados += batch_count
        
        print(f"\n✅ COMPLETADO: {total_guardados} series temporales guardadas")
        return {
            'success': True,
            'total_fechas': total_guardados
        }
        
    except Exception as e:
        print(f"\n❌ ERROR en series temporales: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }


# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def run_emprestito_analytics():
    """Ejecuta todas las agregaciones específicas de empréstito."""
    print("\n" + "="*80)
    print("🚀 AGREGACIONES ANALÍTICAS DE EMPRÉSTITO")
    print("="*80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    resultados = {}
    
    print("\n[1/4] Análisis por banco...")
    resultados['por_banco'] = aggregate_analysis_by_banco()
    
    print("\n[2/4] Análisis por centro gestor...")
    resultados['por_centro_gestor'] = aggregate_analysis_by_centro_gestor()
    
    print("\n[3/4] Resumen anual...")
    resultados['resumen_anual'] = aggregate_resumen_anual()
    
    print("\n[4/4] Series temporales diarias...")
    resultados['series_temporales'] = aggregate_series_temporales_diarias()
    
    # Resumen final
    print("\n" + "="*80)
    print("📊 RESUMEN DE AGREGACIONES DE EMPRÉSTITO")
    print("="*80)
    
    for nombre, resultado in resultados.items():
        status = "✅ SUCCESS" if resultado.get('success') else "❌ ERROR"
        print(f"{status} - {nombre}")
        if not resultado.get('success'):
            print(f"    Error: {resultado.get('error', 'Unknown')}")
    
    print("="*80 + "\n")
    
    return resultados


if __name__ == "__main__":
    run_emprestito_analytics()
