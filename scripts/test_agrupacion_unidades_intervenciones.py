# -*- coding: utf-8 -*-
"""
Script de Prueba: Agrupación de Unidades de Proyecto e Intervenciones

Prueba la lógica de agrupación de datos en:
- Unidades de Proyecto (nivel superior - ubicación/infraestructura)
- Intervenciones (nivel detalle - contratos/proyectos específicos)

Este script NO afecta la ETL actual, solo prueba la lógica de agrupación.
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import hashlib

# Agregar rutas necesarias
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar módulos para obtener datos
from extraction_app.data_extraction_unidades_proyecto import extract_and_save_unidades_proyecto


# ============================================================================
# DEFINICIÓN DE CAMPOS PARA CADA NIVEL
# ============================================================================

# Campos que definen la UNIDAD DE PROYECTO (ubicación/infraestructura)
UNIDAD_PROYECTO_FIELDS = [
    'nombre_up',
    'nombre_up_detalle',
    'comuna_corregimiento',
    'barrio_vereda',
    'direccion',
    'tipo_equipamiento'
]

# Campos que definen las INTERVENCIONES (contratos/proyectos específicos)
INTERVENCION_FIELDS = [
    'referencia_proceso',
    'referencia_contrato',
    'bpin',
    'identificador',
    'fuente_financiacion',
    'tipo_intervencion',
    'unidad',
    'cantidad',
    'estado',
    'presupuesto_base',
    'avance_obra',
    'ano',
    'fecha_inicio',
    'fecha_fin',
    'geometry',
    'plataforma',
    'url_proceso',
    'clase_up',
    'nombre_centro_gestor'
]


# ============================================================================
# FUNCIONES DE AGRUPACIÓN
# ============================================================================

def generar_clave_agrupacion(row: pd.Series) -> str:
    """
    Genera una clave única para agrupar registros que pertenecen
    a la misma unidad de proyecto (misma ubicación/infraestructura).
    
    Criterios de agrupación:
    - Mismo nombre_up (normalizado)
    - Mismo nombre_up_detalle (normalizado) ← DIFERENCIADOR CLAVE
    - Misma dirección (normalizado)
    - Misma comuna/corregimiento
    - Mismo barrio/vereda
    - Mismo tipo de equipamiento
    
    IMPORTANTE: Si dos registros tienen el mismo nombre_up pero diferente
    nombre_up_detalle, se consideran unidades de proyecto INDEPENDIENTES.
    
    Args:
        row: Fila del DataFrame con los datos
        
    Returns:
        String que representa la clave de agrupación
    """
    # Función auxiliar para normalizar texto
    def normalizar(valor):
        if pd.isna(valor) or valor is None:
            return ""
        return str(valor).strip().lower()
    
    # Crear clave compuesta (nombre_up_detalle es crítico)
    componentes = [
        normalizar(row.get('nombre_up', '')),
        normalizar(row.get('nombre_up_detalle', '')),  # ← AGREGADO
        normalizar(row.get('direccion', '')),
        normalizar(row.get('comuna_corregimiento', '')),
        normalizar(row.get('barrio_vereda', '')),
        normalizar(row.get('tipo_equipamiento', ''))
    ]
    
    # Unir componentes y crear hash para ID corto
    clave_completa = '|'.join(componentes)
    
    # Crear hash MD5 de la clave (para IDs más cortos)
    hash_corto = hashlib.md5(clave_completa.encode('utf-8')).hexdigest()[:8]
    
    return hash_corto


def extraer_campos_unidad_proyecto(row: pd.Series) -> Dict[str, Any]:
    """
    Extrae solo los campos que definen la unidad de proyecto.
    
    Args:
        row: Fila del DataFrame
        
    Returns:
        Diccionario con los campos de unidad de proyecto
    """
    unidad = {}
    
    for campo in UNIDAD_PROYECTO_FIELDS:
        valor = row.get(campo)
        # Convertir valores None/NaN a None explícito
        if pd.isna(valor):
            unidad[campo] = None
        else:
            unidad[campo] = valor
    
    return unidad


def extraer_campos_intervencion(row: pd.Series) -> Dict[str, Any]:
    """
    Extrae los campos que definen una intervención específica.
    
    Args:
        row: Fila del DataFrame
        
    Returns:
        Diccionario con los campos de la intervención
    """
    intervencion = {}
    
    for campo in INTERVENCION_FIELDS:
        valor = row.get(campo)
        # Convertir valores None/NaN a None explícito
        if pd.isna(valor):
            intervencion[campo] = None
        else:
            intervencion[campo] = valor
    
    return intervencion


def asignar_upid_a_unidades(grupos_agrupados: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Asigna UPIDs únicos a las unidades de proyecto.
    Formato: UNP-1, UNP-2, etc.
    
    Args:
        grupos_agrupados: Diccionario con grupos sin UPID
        
    Returns:
        Diccionario con UPIDs asignados
    """
    resultado = {}
    upid_counter = 1
    
    # Ordenar por clave hash para consistencia
    for clave_hash in sorted(grupos_agrupados.keys()):
        grupo = grupos_agrupados[clave_hash]
        
        # Asignar UPID
        upid = f"UNP-{upid_counter}"
        grupo['upid'] = upid
        
        # Agregar número de intervenciones
        grupo['n_intervenciones'] = len(grupo.get('intervenciones', []))
        
        # Usar UPID como clave del diccionario
        resultado[upid] = grupo
        upid_counter += 1
    
    return resultado


def asignar_ids_intervenciones(unidades_con_upid: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Asigna IDs únicos a cada intervención.
    Formato: UNP-155-01, UNP-155-02, etc.
    
    Args:
        unidades_con_upid: Diccionario de unidades con UPID
        
    Returns:
        Diccionario con IDs de intervenciones asignados
    """
    for upid, unidad in unidades_con_upid.items():
        intervenciones = unidad.get('intervenciones', [])
        
        for idx, intervencion in enumerate(intervenciones, start=1):
            # Asignar ID de intervención: UPID-##
            intervencion_id = f"{upid}-{idx:02d}"
            intervencion['intervencion_id'] = intervencion_id
    
    return unidades_con_upid


def agrupar_datos_por_unidad_proyecto(df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Agrupa los datos del DataFrame en unidades de proyecto con intervenciones.
    
    EXCEPCIÓN: Registros con clase_up = "Subsidios" NO se agrupan.
    Cada subsidio se mantiene como unidad individual (1 unidad = 1 intervención).
    
    Args:
        df: DataFrame con los datos originales (sin transformar)
        
    Returns:
        Diccionario estructurado con unidades e intervenciones
        {
            'UNP-1': {
                'upid': 'UNP-1',
                'nombre_up': '...',
                'direccion': '...',
                'intervenciones': [
                    {'intervencion_id': 'UNP-1-01', ...},
                    {'intervencion_id': 'UNP-1-02', ...}
                ]
            },
            'UNP-2': {...}
        }
    """
    print(f"\n{'='*70}")
    print(f"🔄 AGRUPANDO DATOS EN UNIDADES E INTERVENCIONES")
    print(f"{'='*70}")
    
    # Paso 0: Separar subsidios (no se agrupan)
    mask_subsidios = df['clase_up'] == 'Subsidios'
    df_subsidios = df[mask_subsidios].copy()
    df_agrupables = df[~mask_subsidios].copy()
    
    print(f"\n📊 Distribución de datos:")
    print(f"   • Total registros: {len(df)}")
    print(f"   • Subsidios (no agrupables): {len(df_subsidios)}")
    print(f"   • Registros agrupables: {len(df_agrupables)}")
    
    # Paso 1: Agrupar por clave hash (solo agrupables)
    grupos = defaultdict(lambda: {
        'unidad_proyecto': None,
        'intervenciones': []
    })
    
    for idx, row in df_agrupables.iterrows():
        # Generar clave de agrupación
        clave_hash = generar_clave_agrupacion(row)
        
        # Si es la primera intervención de esta unidad, guardar datos de la unidad
        if grupos[clave_hash]['unidad_proyecto'] is None:
            grupos[clave_hash]['unidad_proyecto'] = extraer_campos_unidad_proyecto(row)
        
        # Agregar intervención
        intervencion = extraer_campos_intervencion(row)
        grupos[clave_hash]['intervenciones'].append(intervencion)
    
    print(f"\n✅ Agrupación inicial completada:")
    print(f"   - Total de UNIDADES DE PROYECTO: {len(grupos)}")
    print(f"   - Total de INTERVENCIONES: {len(df)}")
    
    # Paso 2: Reestructurar y asignar UPIDs
    unidades_estructuradas = {}
    
    for clave_hash, grupo_data in grupos.items():
        unidad_proyecto = grupo_data['unidad_proyecto'].copy()
        unidad_proyecto['intervenciones'] = grupo_data['intervenciones']
        
        unidades_estructuradas[clave_hash] = unidad_proyecto
    
    # Paso 2.5: Procesar subsidios (cada uno es una unidad individual)
    print(f"\n💰 Procesando subsidios como unidades individuales...")
    
    for idx, row in df_subsidios.iterrows():
        # Cada subsidio es su propia unidad
        clave_hash = f"SUBSIDIO-{idx}"
        
        unidad_proyecto = extraer_campos_unidad_proyecto(row).copy()
        intervencion = extraer_campos_intervencion(row)
        unidad_proyecto['intervenciones'] = [intervencion]
        
        unidades_estructuradas[clave_hash] = unidad_proyecto
    
    print(f"   ✅ Subsidios procesados: {len(df_subsidios)}")
    
    # Paso 3: Asignar UPIDs a unidades
    print(f"\n🏷️ Asignando UPIDs a unidades de proyecto...")
    unidades_con_upid = asignar_upid_a_unidades(unidades_estructuradas)
    
    # Paso 4: Asignar IDs a intervenciones
    print(f"🏷️ Asignando IDs a intervenciones...")
    resultado_final = asignar_ids_intervenciones(unidades_con_upid)
    
    return resultado_final


def generar_estadisticas(unidades: Dict[str, Dict]) -> Dict[str, Any]:
    """
    Genera estadísticas sobre la agrupación.
    
    Args:
        unidades: Diccionario de unidades con intervenciones
        
    Returns:
        Diccionario con estadísticas
    """
    total_unidades = len(unidades)
    total_intervenciones = sum(len(u['intervenciones']) for u in unidades.values())
    
    intervenciones_por_unidad = [len(u['intervenciones']) for u in unidades.values()]
    max_intervenciones = max(intervenciones_por_unidad) if intervenciones_por_unidad else 0
    min_intervenciones = min(intervenciones_por_unidad) if intervenciones_por_unidad else 0
    promedio_intervenciones = sum(intervenciones_por_unidad) / len(intervenciones_por_unidad) if intervenciones_por_unidad else 0
    
    # Encontrar unidad con más intervenciones
    unidad_mas_intervenciones = None
    max_count = 0
    for upid, unidad in unidades.items():
        count = len(unidad['intervenciones'])
        if count > max_count:
            max_count = count
            unidad_mas_intervenciones = {
                'upid': upid,
                'nombre_up': unidad.get('nombre_up'),
                'total_intervenciones': count
            }
    
    # Distribución por tipo de equipamiento
    tipos_equipamiento = defaultdict(int)
    for unidad in unidades.values():
        tipo = unidad.get('tipo_equipamiento', 'Sin especificar')
        tipos_equipamiento[tipo] += 1
    
    # Distribución por comuna/corregimiento
    comunas = defaultdict(int)
    for unidad in unidades.values():
        comuna = unidad.get('comuna_corregimiento', 'Sin especificar')
        comunas[comuna] += 1
    
    estadisticas = {
        'total_unidades_proyecto': total_unidades,
        'total_intervenciones': total_intervenciones,
        'intervenciones_por_unidad': {
            'max': max_intervenciones,
            'min': min_intervenciones,
            'promedio': round(promedio_intervenciones, 2)
        },
        'unidad_con_mas_intervenciones': unidad_mas_intervenciones,
        'distribucion_por_tipo_equipamiento': dict(tipos_equipamiento),
        'distribucion_por_comuna': dict(comunas),
        'factor_compresion': round(total_intervenciones / total_unidades, 2) if total_unidades > 0 else 0
    }
    
    return estadisticas


def mostrar_ejemplos(unidades: Dict[str, Dict], num_ejemplos: int = 3):
    """
    Muestra ejemplos de unidades con sus intervenciones.
    
    Args:
        unidades: Diccionario de unidades
        num_ejemplos: Número de ejemplos a mostrar
    """
    print(f"\n{'='*70}")
    print(f"📋 EJEMPLOS DE UNIDADES CON INTERVENCIONES")
    print(f"{'='*70}")
    
    # Seleccionar unidades de ejemplo (con más intervenciones primero)
    unidades_ordenadas = sorted(
        unidades.items(),
        key=lambda x: len(x[1]['intervenciones']),
        reverse=True
    )
    
    for idx, (upid, unidad) in enumerate(unidades_ordenadas[:num_ejemplos], 1):
        print(f"\n{'─'*70}")
        print(f"📍 Ejemplo {idx}: {upid}")
        print(f"{'─'*70}")
        
        # Información de la unidad
        print(f"\n🏢 UNIDAD DE PROYECTO:")
        print(f"   • Nombre: {unidad.get('nombre_up')}")
        print(f"   • Detalle: {unidad.get('nombre_up_detalle')}")
        print(f"   • Dirección: {unidad.get('direccion')}")
        print(f"   • Comuna/Corregimiento: {unidad.get('comuna_corregimiento')}")
        print(f"   • Barrio/Vereda: {unidad.get('barrio_vereda')}")
        print(f"   • Tipo Equipamiento: {unidad.get('tipo_equipamiento')}")
        
        # Intervenciones
        intervenciones = unidad.get('intervenciones', [])
        print(f"\n🔧 INTERVENCIONES ({len(intervenciones)}):")
        
        for interv in intervenciones:
            print(f"\n   ├─ {interv.get('intervencion_id')}:")
            print(f"   │  • Contrato: {interv.get('referencia_contrato')}")
            print(f"   │  • BPIN: {interv.get('bpin')}")
            print(f"   │  • Tipo: {interv.get('tipo_intervencion')}")
            print(f"   │  • Estado: {interv.get('estado')}")
            print(f"   │  • Año: {interv.get('ano')}")
            print(f"   │  • Presupuesto: {interv.get('presupuesto_base')}")
            print(f"   │  • Avance: {interv.get('avance_obra')}")


def guardar_resultados(unidades: Dict[str, Dict], estadisticas: Dict[str, Any]):
    """
    Guarda los resultados de la prueba en archivos JSON.
    
    Args:
        unidades: Diccionario de unidades
        estadisticas: Diccionario de estadísticas
    """
    # Crear directorio de salida
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'app_outputs', 'test_agrupacion')
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Guardar unidades estructuradas
    unidades_path = os.path.join(output_dir, f'unidades_agrupadas_{timestamp}.json')
    with open(unidades_path, 'w', encoding='utf-8') as f:
        json.dump(unidades, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n💾 Unidades agrupadas guardadas: {os.path.basename(unidades_path)}")
    
    # Guardar estadísticas
    stats_path = os.path.join(output_dir, f'estadisticas_agrupacion_{timestamp}.json')
    with open(stats_path, 'w', encoding='utf-8') as f:
        json.dump(estadisticas, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"💾 Estadísticas guardadas: {os.path.basename(stats_path)}")
    
    # Guardar ejemplo simplificado (primeras 5 unidades)
    ejemplo_path = os.path.join(output_dir, f'ejemplo_estructura_{timestamp}.json')
    ejemplo = {k: v for k, v in list(unidades.items())[:5]}
    
    with open(ejemplo_path, 'w', encoding='utf-8') as f:
        json.dump(ejemplo, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"💾 Ejemplo simplificado guardado: {os.path.basename(ejemplo_path)}")
    print(f"\n📂 Directorio de salida: {output_dir}")


def mostrar_estadisticas(estadisticas: Dict[str, Any]):
    """
    Muestra las estadísticas de la agrupación.
    
    Args:
        estadisticas: Diccionario con estadísticas
    """
    print(f"\n{'='*70}")
    print(f"📊 ESTADÍSTICAS DE AGRUPACIÓN")
    print(f"{'='*70}")
    
    print(f"\n🔢 Totales:")
    print(f"   • Unidades de Proyecto: {estadisticas['total_unidades_proyecto']}")
    print(f"   • Intervenciones: {estadisticas['total_intervenciones']}")
    print(f"   • Factor de Compresión: {estadisticas['factor_compresion']}x")
    print(f"     (promedio de intervenciones por unidad)")
    
    print(f"\n📈 Intervenciones por Unidad:")
    print(f"   • Máximo: {estadisticas['intervenciones_por_unidad']['max']}")
    print(f"   • Mínimo: {estadisticas['intervenciones_por_unidad']['min']}")
    print(f"   • Promedio: {estadisticas['intervenciones_por_unidad']['promedio']}")
    
    if estadisticas['unidad_con_mas_intervenciones']:
        print(f"\n🏆 Unidad con más intervenciones:")
        u = estadisticas['unidad_con_mas_intervenciones']
        print(f"   • UPID: {u['upid']}")
        print(f"   • Nombre: {u['nombre_up']}")
        print(f"   • Total intervenciones: {u['total_intervenciones']}")
    
    print(f"\n🏗️ Distribución por Tipo de Equipamiento:")
    for tipo, count in sorted(estadisticas['distribucion_por_tipo_equipamiento'].items(), 
                              key=lambda x: x[1], reverse=True)[:10]:
        print(f"   • {tipo}: {count} unidades")
    
    print(f"\n🗺️ Distribución por Comuna/Corregimiento:")
    for comuna, count in sorted(estadisticas['distribucion_por_comuna'].items(), 
                                key=lambda x: x[1], reverse=True)[:10]:
        print(f"   • {comuna}: {count} unidades")


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """
    Función principal que ejecuta la prueba de agrupación.
    """
    print(f"\n{'='*70}")
    print(f"🧪 PRUEBA DE AGRUPACIÓN: UNIDADES E INTERVENCIONES")
    print(f"{'='*70}")
    print(f"\nEsta prueba NO afecta la ETL actual.")
    print(f"Solo analiza cómo se agruparían los datos en el nuevo modelo.")
    
    # Paso 1: Extraer datos
    print(f"\n{'='*70}")
    print(f"📥 PASO 1: EXTRACCIÓN DE DATOS")
    print(f"{'='*70}")
    
    df = extract_and_save_unidades_proyecto()
    
    if df is None or df.empty:
        print("❌ Error: No se pudieron extraer los datos")
        return
    
    print(f"✅ Datos extraídos: {len(df)} registros")
    
    # Paso 2: Agrupar datos
    unidades = agrupar_datos_por_unidad_proyecto(df)
    
    # Paso 3: Generar estadísticas
    estadisticas = generar_estadisticas(unidades)
    
    # Paso 4: Mostrar resultados
    mostrar_estadisticas(estadisticas)
    mostrar_ejemplos(unidades, num_ejemplos=3)
    
    # Paso 5: Guardar resultados
    print(f"\n{'='*70}")
    print(f"💾 GUARDANDO RESULTADOS")
    print(f"{'='*70}")
    
    guardar_resultados(unidades, estadisticas)
    
    print(f"\n{'='*70}")
    print(f"✅ PRUEBA COMPLETADA")
    print(f"{'='*70}")
    print(f"\nRevisa los archivos JSON generados para ver la estructura propuesta.")
    print(f"Si el modelo funciona bien, podemos integrar esta lógica en el pipeline.")


if __name__ == "__main__":
    main()
