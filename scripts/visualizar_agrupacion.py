# -*- coding: utf-8 -*-
"""
Visualizador de Resultados de Agrupación

Script para analizar y visualizar los resultados de la prueba de agrupación
de unidades de proyecto e intervenciones.
"""

import os
import sys
import json
from typing import Dict, Any
from datetime import datetime

# Agregar rutas
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def cargar_ultimo_resultado() -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Carga el resultado más reciente de la prueba de agrupación.
    
    Returns:
        Tupla de (unidades, estadisticas)
    """
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'app_outputs', 'test_agrupacion')
    
    if not os.path.exists(output_dir):
        print(f"❌ No se encontró el directorio: {output_dir}")
        print("   Primero ejecuta: python scripts/test_agrupacion_unidades_intervenciones.py")
        return None, None
    
    # Buscar archivos más recientes
    unidades_files = [f for f in os.listdir(output_dir) if f.startswith('unidades_agrupadas_')]
    stats_files = [f for f in os.listdir(output_dir) if f.startswith('estadisticas_agrupacion_')]
    
    if not unidades_files or not stats_files:
        print("❌ No se encontraron archivos de resultados")
        print("   Primero ejecuta: python scripts/test_agrupacion_unidades_intervenciones.py")
        return None, None
    
    # Ordenar por nombre (que incluye timestamp) y tomar el más reciente
    unidades_files.sort(reverse=True)
    stats_files.sort(reverse=True)
    
    # Cargar archivos
    unidades_path = os.path.join(output_dir, unidades_files[0])
    stats_path = os.path.join(output_dir, stats_files[0])
    
    print(f"📂 Cargando resultados:")
    print(f"   • {os.path.basename(unidades_path)}")
    print(f"   • {os.path.basename(stats_path)}")
    
    with open(unidades_path, 'r', encoding='utf-8') as f:
        unidades = json.load(f)
    
    with open(stats_path, 'r', encoding='utf-8') as f:
        estadisticas = json.load(f)
    
    return unidades, estadisticas


def buscar_unidad_por_upid(unidades: Dict[str, Any], upid: str) -> Dict[str, Any]:
    """
    Busca una unidad específica por su UPID.
    
    Args:
        unidades: Diccionario de unidades
        upid: UPID a buscar
        
    Returns:
        Diccionario con los datos de la unidad o None
    """
    return unidades.get(upid)


def buscar_unidades_por_nombre(unidades: Dict[str, Any], nombre: str) -> list[tuple[str, Dict]]:
    """
    Busca unidades que contengan un texto en su nombre.
    
    Args:
        unidades: Diccionario de unidades
        nombre: Texto a buscar en el nombre
        
    Returns:
        Lista de tuplas (upid, unidad)
    """
    nombre_lower = nombre.lower()
    resultados = []
    
    for upid, unidad in unidades.items():
        nombre_up = str(unidad.get('nombre_up', '')).lower()
        if nombre_lower in nombre_up:
            resultados.append((upid, unidad))
    
    return resultados


def buscar_unidades_por_comuna(unidades: Dict[str, Any], comuna: str) -> list[tuple[str, Dict]]:
    """
    Busca unidades en una comuna específica.
    
    Args:
        unidades: Diccionario de unidades
        comuna: Comuna a buscar
        
    Returns:
        Lista de tuplas (upid, unidad)
    """
    comuna_lower = comuna.lower()
    resultados = []
    
    for upid, unidad in unidades.items():
        comuna_corr = str(unidad.get('comuna_corregimiento', '')).lower()
        if comuna_lower in comuna_corr:
            resultados.append((upid, unidad))
    
    return resultados


def buscar_unidades_por_tipo_equipamiento(unidades: Dict[str, Any], tipo: str) -> list[tuple[str, Dict]]:
    """
    Busca unidades de un tipo de equipamiento específico.
    
    Args:
        unidades: Diccionario de unidades
        tipo: Tipo de equipamiento a buscar
        
    Returns:
        Lista de tuplas (upid, unidad)
    """
    tipo_lower = tipo.lower()
    resultados = []
    
    for upid, unidad in unidades.items():
        tipo_eq = str(unidad.get('tipo_equipamiento', '')).lower()
        if tipo_lower in tipo_eq:
            resultados.append((upid, unidad))
    
    return resultados


def mostrar_unidad_detallada(upid: str, unidad: Dict[str, Any]):
    """
    Muestra información detallada de una unidad y sus intervenciones.
    
    Args:
        upid: UPID de la unidad
        unidad: Diccionario con los datos de la unidad
    """
    print(f"\n{'='*80}")
    print(f"📍 UNIDAD DE PROYECTO: {upid}")
    print(f"{'='*80}")
    
    # Información de la unidad
    print(f"\n🏢 INFORMACIÓN GENERAL:")
    print(f"   • Nombre: {unidad.get('nombre_up')}")
    print(f"   • Detalle: {unidad.get('nombre_up_detalle')}")
    print(f"   • Dirección: {unidad.get('direccion')}")
    print(f"   • Comuna/Corregimiento: {unidad.get('comuna_corregimiento')}")
    print(f"   • Barrio/Vereda: {unidad.get('barrio_vereda')}")
    print(f"   • Tipo Equipamiento: {unidad.get('tipo_equipamiento')}")
    
    # Intervenciones
    intervenciones = unidad.get('intervenciones', [])
    print(f"\n🔧 INTERVENCIONES ({len(intervenciones)}):")
    
    if not intervenciones:
        print("   (No hay intervenciones)")
        return
    
    for idx, interv in enumerate(intervenciones, 1):
        print(f"\n   {'─'*70}")
        print(f"   {idx}. {interv.get('intervencion_id')}")
        print(f"   {'─'*70}")
        
        # Información básica
        print(f"      📋 Identificación:")
        print(f"         • Contrato: {interv.get('referencia_contrato')}")
        print(f"         • Proceso: {interv.get('referencia_proceso')}")
        print(f"         • BPIN: {interv.get('bpin')}")
        print(f"         • Identificador: {interv.get('identificador')}")
        
        # Tipo y estado
        print(f"\n      🏗️ Tipo y Estado:")
        print(f"         • Tipo: {interv.get('tipo_intervencion')}")
        print(f"         • Estado: {interv.get('estado')}")
        print(f"         • Clase UP: {interv.get('clase_up')}")
        
        # Financiero
        print(f"\n      💰 Información Financiera:")
        print(f"         • Fuente: {interv.get('fuente_financiacion')}")
        print(f"         • Presupuesto: {interv.get('presupuesto_base')}")
        print(f"         • Avance: {interv.get('avance_obra')}")
        
        # Temporal
        print(f"\n      📅 Información Temporal:")
        print(f"         • Año: {interv.get('ano')}")
        print(f"         • Inicio: {interv.get('fecha_inicio')}")
        print(f"         • Fin: {interv.get('fecha_fin')}")
        
        # Cantidad
        if interv.get('cantidad') and interv.get('unidad'):
            print(f"\n      📏 Cantidad:")
            print(f"         • {interv.get('cantidad')} {interv.get('unidad')}")
        
        # Centro gestor
        if interv.get('nombre_centro_gestor'):
            print(f"\n      🏛️ Centro Gestor:")
            print(f"         • {interv.get('nombre_centro_gestor')}")
        
        # Geometría
        geometry = interv.get('geometry')
        if geometry:
            geom_type = geometry.get('type', 'No especificado')
            print(f"\n      🗺️ Geometría:")
            print(f"         • Tipo: {geom_type}")


def analizar_duplicados_potenciales(unidades: Dict[str, Any]) -> list[tuple[str, str, float]]:
    """
    Identifica potenciales duplicados por similitud en nombres.
    
    Args:
        unidades: Diccionario de unidades
        
    Returns:
        Lista de tuplas (upid1, upid2, similitud)
    """
    from difflib import SequenceMatcher
    
    print(f"\n{'='*80}")
    print(f"🔍 ANÁLISIS DE DUPLICADOS POTENCIALES")
    print(f"{'='*80}")
    
    duplicados = []
    upids = list(unidades.keys())
    
    for i in range(len(upids)):
        for j in range(i + 1, len(upids)):
            upid1, upid2 = upids[i], upids[j]
            nombre1 = str(unidades[upid1].get('nombre_up', '')).lower()
            nombre2 = str(unidades[upid2].get('nombre_up', '')).lower()
            
            if not nombre1 or not nombre2:
                continue
            
            # Calcular similitud
            similitud = SequenceMatcher(None, nombre1, nombre2).ratio()
            
            # Si son muy similares pero no idénticos
            if 0.8 < similitud < 1.0:
                duplicados.append((upid1, upid2, similitud))
    
    if duplicados:
        print(f"\n⚠️ Encontrados {len(duplicados)} pares de unidades con nombres similares:")
        for upid1, upid2, sim in sorted(duplicados, key=lambda x: x[2], reverse=True):
            print(f"\n   Similitud: {sim*100:.1f}%")
            print(f"   • {upid1}: {unidades[upid1].get('nombre_up')}")
            print(f"   • {upid2}: {unidades[upid2].get('nombre_up')}")
    else:
        print(f"\n✅ No se encontraron duplicados potenciales")
    
    return duplicados


def generar_reporte_validacion(unidades: Dict[str, Any], estadisticas: Dict[str, Any]):
    """
    Genera un reporte de validación de la estructura.
    
    Args:
        unidades: Diccionario de unidades
        estadisticas: Diccionario de estadísticas
    """
    print(f"\n{'='*80}")
    print(f"✅ REPORTE DE VALIDACIÓN")
    print(f"{'='*80}")
    
    errores = []
    advertencias = []
    
    # Validar UPIDs
    print(f"\n🏷️ Validando UPIDs...")
    upids_vistos = set()
    for upid in unidades.keys():
        if not upid.startswith('UNP-'):
            errores.append(f"UPID inválido: {upid}")
        if upid in upids_vistos:
            errores.append(f"UPID duplicado: {upid}")
        upids_vistos.add(upid)
    
    if not errores:
        print(f"   ✅ Todos los UPIDs son válidos y únicos")
    
    # Validar IDs de intervenciones
    print(f"\n🔧 Validando IDs de intervenciones...")
    interv_ids_vistos = set()
    
    for upid, unidad in unidades.items():
        intervenciones = unidad.get('intervenciones', [])
        
        for idx, interv in enumerate(intervenciones, 1):
            interv_id = interv.get('intervencion_id')
            
            # Validar formato
            if not interv_id or not interv_id.startswith(f"{upid}-"):
                errores.append(f"ID de intervención inválido: {interv_id} (esperado {upid}-##)")
            
            # Validar unicidad global
            if interv_id in interv_ids_vistos:
                errores.append(f"ID de intervención duplicado: {interv_id}")
            interv_ids_vistos.add(interv_id)
            
            # Validar número secuencial
            esperado_num = idx
            actual_num = interv.get('intervencion_num')
            if actual_num != esperado_num:
                advertencias.append(f"{interv_id}: número secuencial incorrecto ({actual_num} vs {esperado_num})")
    
    if not errores:
        print(f"   ✅ Todos los IDs de intervenciones son válidos")
    
    # Validar campos requeridos
    print(f"\n📋 Validando campos requeridos...")
    campos_up_requeridos = ['nombre_up', 'tipo_equipamiento']
    
    for upid, unidad in unidades.items():
        for campo in campos_up_requeridos:
            valor = unidad.get(campo)
            if not valor or valor == '' or valor == 'None':
                advertencias.append(f"{upid}: campo '{campo}' vacío")
    
    # Validar que todas las unidades tengan al menos una intervención
    print(f"\n🔧 Validando intervenciones...")
    unidades_sin_intervenciones = []
    
    for upid, unidad in unidades.items():
        intervenciones = unidad.get('intervenciones', [])
        if not intervenciones:
            unidades_sin_intervenciones.append(upid)
    
    if unidades_sin_intervenciones:
        advertencias.append(f"{len(unidades_sin_intervenciones)} unidades sin intervenciones")
    
    # Mostrar resumen
    print(f"\n{'─'*80}")
    print(f"📊 RESUMEN:")
    print(f"   • Errores: {len(errores)}")
    print(f"   • Advertencias: {len(advertencias)}")
    
    if errores:
        print(f"\n❌ ERRORES ENCONTRADOS:")
        for error in errores[:10]:  # Mostrar máximo 10
            print(f"   • {error}")
        if len(errores) > 10:
            print(f"   ... y {len(errores) - 10} más")
    
    if advertencias:
        print(f"\n⚠️ ADVERTENCIAS:")
        for adv in advertencias[:10]:  # Mostrar máximo 10
            print(f"   • {adv}")
        if len(advertencias) > 10:
            print(f"   ... y {len(advertencias) - 10} más")
    
    if not errores and not advertencias:
        print(f"\n✅ VALIDACIÓN EXITOSA: No se encontraron problemas")


def menu_interactivo(unidades: Dict[str, Any], estadisticas: Dict[str, Any]):
    """
    Menú interactivo para explorar los resultados.
    
    Args:
        unidades: Diccionario de unidades
        estadisticas: Diccionario de estadísticas
    """
    while True:
        print(f"\n{'='*80}")
        print(f"📊 MENÚ DE EXPLORACIÓN - RESULTADOS DE AGRUPACIÓN")
        print(f"{'='*80}")
        print(f"\n1. Ver estadísticas generales")
        print(f"2. Buscar unidad por UPID")
        print(f"3. Buscar unidades por nombre")
        print(f"4. Buscar unidades por comuna")
        print(f"5. Buscar unidades por tipo de equipamiento")
        print(f"6. Ver unidad con más intervenciones")
        print(f"7. Analizar duplicados potenciales")
        print(f"8. Reporte de validación")
        print(f"9. Salir")
        
        opcion = input(f"\n➜ Selecciona una opción (1-9): ").strip()
        
        if opcion == '1':
            # Mostrar estadísticas
            print(f"\n{'='*80}")
            print(f"📊 ESTADÍSTICAS GENERALES")
            print(f"{'='*80}")
            print(json.dumps(estadisticas, indent=2, ensure_ascii=False))
            
        elif opcion == '2':
            # Buscar por UPID
            upid = input(f"\n➜ Ingresa el UPID (ej: UNP-1): ").strip()
            unidad = buscar_unidad_por_upid(unidades, upid)
            
            if unidad:
                mostrar_unidad_detallada(upid, unidad)
            else:
                print(f"\n❌ No se encontró la unidad {upid}")
            
        elif opcion == '3':
            # Buscar por nombre
            nombre = input(f"\n➜ Ingresa texto a buscar en el nombre: ").strip()
            resultados = buscar_unidades_por_nombre(unidades, nombre)
            
            print(f"\n📋 Encontradas {len(resultados)} unidades:")
            for upid, unidad in resultados[:10]:  # Mostrar máximo 10
                print(f"   • {upid}: {unidad.get('nombre_up')}")
            
            if len(resultados) > 10:
                print(f"   ... y {len(resultados) - 10} más")
            
            if resultados:
                ver_detalle = input(f"\n➜ ¿Ver detalle de alguna? (ingresa UPID o Enter para continuar): ").strip()
                if ver_detalle and ver_detalle in [u[0] for u in resultados]:
                    unidad = buscar_unidad_por_upid(unidades, ver_detalle)
                    mostrar_unidad_detallada(ver_detalle, unidad)
            
        elif opcion == '4':
            # Buscar por comuna
            comuna = input(f"\n➜ Ingresa la comuna/corregimiento: ").strip()
            resultados = buscar_unidades_por_comuna(unidades, comuna)
            
            print(f"\n📋 Encontradas {len(resultados)} unidades en {comuna}:")
            for upid, unidad in resultados[:10]:
                print(f"   • {upid}: {unidad.get('nombre_up')} ({len(unidad.get('intervenciones', []))} intervenciones)")
            
            if len(resultados) > 10:
                print(f"   ... y {len(resultados) - 10} más")
            
        elif opcion == '5':
            # Buscar por tipo equipamiento
            tipo = input(f"\n➜ Ingresa el tipo de equipamiento: ").strip()
            resultados = buscar_unidades_por_tipo_equipamiento(unidades, tipo)
            
            print(f"\n📋 Encontradas {len(resultados)} unidades de tipo '{tipo}':")
            for upid, unidad in resultados[:10]:
                print(f"   • {upid}: {unidad.get('nombre_up')} ({len(unidad.get('intervenciones', []))} intervenciones)")
            
            if len(resultados) > 10:
                print(f"   ... y {len(resultados) - 10} más")
            
        elif opcion == '6':
            # Ver unidad con más intervenciones
            u = estadisticas.get('unidad_con_mas_intervenciones')
            if u:
                upid = u['upid']
                unidad = buscar_unidad_por_upid(unidades, upid)
                if unidad:
                    mostrar_unidad_detallada(upid, unidad)
            
        elif opcion == '7':
            # Analizar duplicados
            analizar_duplicados_potenciales(unidades)
            
        elif opcion == '8':
            # Reporte de validación
            generar_reporte_validacion(unidades, estadisticas)
            
        elif opcion == '9':
            print(f"\n👋 ¡Hasta luego!")
            break
            
        else:
            print(f"\n❌ Opción inválida")
        
        input(f"\nPresiona Enter para continuar...")


def main():
    """
    Función principal.
    """
    print(f"\n{'='*80}")
    print(f"🔍 VISUALIZADOR DE RESULTADOS - AGRUPACIÓN DE UNIDADES")
    print(f"{'='*80}")
    
    # Cargar resultados
    unidades, estadisticas = cargar_ultimo_resultado()
    
    if not unidades or not estadisticas:
        return
    
    print(f"\n✅ Resultados cargados exitosamente")
    print(f"   • Unidades: {len(unidades)}")
    print(f"   • Intervenciones: {sum(len(u['intervenciones']) for u in unidades.values())}")
    
    # Menú interactivo
    menu_interactivo(unidades, estadisticas)


if __name__ == "__main__":
    main()
