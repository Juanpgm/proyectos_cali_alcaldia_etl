# -*- coding: utf-8 -*-
"""
Script de Prueba: Agrupación Geoespacial Avanzada

Implementa clustering inteligente usando:
- DBSCAN para agrupación geoespacial (coordenadas lat/lon)
- Fuzzy matching para registros sin coordenadas
- Normalización de texto para consolidación de nombres

Este script prueba la estrategia antes de integrarla en el pipeline de transformación.
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Tuple
from collections import defaultdict
from sklearn.cluster import DBSCAN
from unidecode import unidecode
from rapidfuzz import fuzz
import warnings

warnings.filterwarnings('ignore')

# Agregar rutas necesarias
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar módulos para obtener datos
from extraction_app.data_extraction_unidades_proyecto import extract_and_save_unidades_proyecto


# ============================================================================
# CONFIGURACIÓN DE PARÁMETROS
# ============================================================================

# Radio de búsqueda para clustering geoespacial (en metros)
CLUSTERING_RADIUS_METERS = 20  # Dos ubicaciones a menos de 20m se consideran la misma

# Umbral de similitud para fuzzy matching (0-100)
FUZZY_THRESHOLD = 85  # 85% de similitud para considerar nombres similares

# Palabras a eliminar en normalización de texto
STOPWORDS = [
    'i.e.', 'ie', 'i.p.s', 'ips', 'institucion educativa', 'institucion',
    'puesto de salud', 'sede', 'centro', 'unidad', 'proyecto',
    'etapa', 'fase', 'seccion'
]


# ============================================================================
# FUNCIONES DE NORMALIZACIÓN DE TEXTO
# ============================================================================

def normalize_text(text: Any) -> str:
    """
    Normaliza texto para comparación:
    - Convierte a minúsculas
    - Elimina acentos
    - Elimina palabras irrelevantes (stopwords)
    - Elimina espacios múltiples
    
    Args:
        text: Texto a normalizar
        
    Returns:
        Texto normalizado
    """
    if pd.isna(text) or text is None:
        return ""
    
    # Convertir a string y normalizar
    text = str(text).strip()
    
    # Eliminar acentos
    text = unidecode(text)
    
    # Convertir a minúsculas
    text = text.lower()
    
    # Eliminar stopwords
    for stopword in STOPWORDS:
        text = text.replace(stopword, '')
    
    # Eliminar caracteres especiales y espacios múltiples
    text = ' '.join(text.split())
    
    return text


def calculate_fuzzy_similarity(text1: str, text2: str) -> float:
    """
    Calcula similitud entre dos textos usando fuzzy matching.
    
    Args:
        text1: Primer texto
        text2: Segundo texto
        
    Returns:
        Valor de similitud (0-100)
    """
    if not text1 or not text2:
        return 0.0
    
    # Usar ratio de Jaro-Winkler que es bueno para nombres
    return fuzz.token_set_ratio(text1, text2)


# ============================================================================
# FUNCIONES DE CLUSTERING GEOESPACIAL
# ============================================================================

def cluster_by_coordinates(df: pd.DataFrame, radius_meters: float = 20) -> pd.DataFrame:
    """
    Agrupa registros por proximidad geográfica usando DBSCAN.
    
    NOTA: Después del clustering espacial, se debe verificar que registros
    en el mismo cluster geoespacial pero con diferente nombre_up_detalle
    se separen en clusters independientes.
    
    Args:
        df: DataFrame con columnas 'lat' y 'lon'
        radius_meters: Radio de búsqueda en metros
        
    Returns:
        DataFrame con columna 'cluster_geo' añadida
    """
    result_df = df.copy()
    
    # Filtrar registros con coordenadas válidas
    mask_has_coords = result_df['lat'].notna() & result_df['lon'].notna()
    df_with_coords = result_df[mask_has_coords].copy()
    
    if len(df_with_coords) == 0:
        print("⚠️ No hay registros con coordenadas válidas")
        result_df['cluster_geo'] = -1
        return result_df
    
    print(f"\n📍 Clustering geoespacial:")
    print(f"   • Registros con coordenadas: {len(df_with_coords)}")
    print(f"   • Radio de búsqueda: {radius_meters} metros")
    
    # Preparar coordenadas para DBSCAN
    coords = df_with_coords[['lat', 'lon']].values
    
    # Convertir radio de metros a radianes
    # La Tierra tiene un radio promedio de 6371 km
    kms_per_radian = 6371.0088
    epsilon = (radius_meters / 1000.0) / kms_per_radian  # Convertir metros a radianes
    
    # Aplicar DBSCAN con métrica haversine (para coordenadas esféricas)
    db = DBSCAN(
        eps=epsilon,
        min_samples=1,  # Mínimo 1 punto por cluster (todos los puntos se agrupan)
        algorithm='ball_tree',
        metric='haversine'
    )
    
    # Fit y predict (convertir a radianes)
    coords_rad = np.radians(coords)
    clusters = db.fit_predict(coords_rad)
    
    # Asignar cluster IDs
    df_with_coords['cluster_geo'] = clusters
    
    # Para los registros sin coordenadas, asignar -1 (sin cluster)
    result_df['cluster_geo'] = -1
    result_df.loc[mask_has_coords, 'cluster_geo'] = df_with_coords['cluster_geo']
    
    num_clusters = len(set(clusters)) - (1 if -1 in clusters else 0)
    print(f"   ✅ Clusters geoespaciales iniciales: {num_clusters}")
    
    # Post-procesamiento: Separar clusters con mismo nombre_up pero diferente nombre_up_detalle
    print(f"   🔍 Verificando nombre_up_detalle como diferenciador...")
    
    max_cluster_id = result_df['cluster_geo'].max()
    next_cluster_id = max_cluster_id + 1 if max_cluster_id >= 0 else 0
    
    for cluster_id in result_df[result_df['cluster_geo'] >= 0]['cluster_geo'].unique():
        cluster_mask = result_df['cluster_geo'] == cluster_id
        cluster_group = result_df[cluster_mask]
        
        # Verificar si hay diferentes nombre_up_detalle en el mismo cluster
        if len(cluster_group) > 1:
            # Agrupar por nombre_up + nombre_up_detalle
            subgroups = cluster_group.groupby(['nombre_up', 'nombre_up_detalle'])
            
            if len(subgroups) > 1:
                # Hay múltiples combinaciones, separar en clusters diferentes
                first_group = True
                for (nombre_up, nombre_up_detalle), subgroup in subgroups:
                    if first_group:
                        # El primer subgrupo mantiene el cluster_id original
                        first_group = False
                    else:
                        # Los demás subgrupos reciben nuevos cluster_ids
                        result_df.loc[subgroup.index, 'cluster_geo'] = next_cluster_id
                        next_cluster_id += 1
    
    num_clusters_final = len(result_df[result_df['cluster_geo'] >= 0]['cluster_geo'].unique())
    if num_clusters_final > num_clusters:
        print(f"   ✅ Clusters ajustados por nombre_up_detalle: {num_clusters_final}")
    
    return result_df


def cluster_by_fuzzy_matching(df: pd.DataFrame, threshold: float = 85) -> pd.DataFrame:
    """
    Agrupa registros sin coordenadas usando fuzzy matching de nombres.
    
    IMPORTANTE: Se considera nombre_up + nombre_up_detalle como identificador único.
    Si dos registros tienen el mismo nombre_up pero diferente nombre_up_detalle,
    NO se agruparán.
    
    Args:
        df: DataFrame con columna 'cluster_geo' = -1 (sin coordenadas)
        threshold: Umbral de similitud (0-100)
        
    Returns:
        DataFrame con columna 'cluster_fuzzy' añadida
    """
    result_df = df.copy()
    
    # Filtrar registros sin cluster geoespacial
    mask_no_geo = result_df['cluster_geo'] == -1
    df_no_geo = result_df[mask_no_geo].copy()
    
    if len(df_no_geo) == 0:
        print("✅ Todos los registros tienen cluster geoespacial")
        result_df['cluster_fuzzy'] = -1
        return result_df
    
    print(f"\n🔤 Clustering por fuzzy matching:")
    print(f"   • Registros sin coordenadas: {len(df_no_geo)}")
    print(f"   • Umbral de similitud: {threshold}%")
    
    # Normalizar nombres (incluir detalle como diferenciador)
    df_no_geo['nombre_norm'] = df_no_geo.apply(
        lambda x: normalize_text(str(x['nombre_up']) + ' ' + str(x['nombre_up_detalle'])),
        axis=1
    )
    df_no_geo['direccion_norm'] = df_no_geo['direccion'].apply(normalize_text)
    
    # Crear clusters manualmente comparando cada par
    cluster_id = 0
    assigned_clusters = {}
    
    for idx, row in df_no_geo.iterrows():
        if idx in assigned_clusters:
            continue
        
        # Crear nuevo cluster con el registro actual
        current_cluster = cluster_id
        assigned_clusters[idx] = current_cluster
        
        # Buscar registros similares
        for idx2, row2 in df_no_geo.iterrows():
            if idx2 in assigned_clusters or idx2 <= idx:
                continue
            
            # Calcular similitud de nombre y dirección
            sim_nombre = calculate_fuzzy_similarity(row['nombre_norm'], row2['nombre_norm'])
            sim_dir = calculate_fuzzy_similarity(row['direccion_norm'], row2['direccion_norm'])
            
            # Si ambos son suficientemente similares, asignar al mismo cluster
            if sim_nombre >= threshold or (sim_nombre >= threshold * 0.7 and sim_dir >= threshold * 0.7):
                assigned_clusters[idx2] = current_cluster
        
        cluster_id += 1
    
    # Asignar clusters al DataFrame
    df_no_geo['cluster_fuzzy'] = df_no_geo.index.map(assigned_clusters)
    
    # Actualizar resultado
    result_df['cluster_fuzzy'] = -1
    result_df.loc[mask_no_geo, 'cluster_fuzzy'] = df_no_geo['cluster_fuzzy']
    
    num_clusters_fuzzy = len(set(assigned_clusters.values()))
    print(f"   ✅ Clusters por fuzzy matching: {num_clusters_fuzzy}")
    
    return result_df


def create_unified_clusters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea un ID de cluster unificado combinando geo y fuzzy.
    
    Args:
        df: DataFrame con columnas 'cluster_geo' y 'cluster_fuzzy'
        
    Returns:
        DataFrame con columna 'cluster_id' unificada
    """
    result_df = df.copy()
    
    print(f"\n🔗 Unificando clusters:")
    
    # Estrategia: priorizar cluster geoespacial, usar fuzzy como fallback
    result_df['cluster_id'] = result_df.apply(
        lambda row: f"GEO-{row['cluster_geo']}" if row['cluster_geo'] >= 0 
        else f"FUZZY-{row['cluster_fuzzy']}" if row['cluster_fuzzy'] >= 0 
        else f"SINGLE-{row.name}",
        axis=1
    )
    
    num_unified = result_df['cluster_id'].nunique()
    print(f"   ✅ Clusters unificados totales: {num_unified}")
    
    return result_df


# ============================================================================
# FUNCIONES DE CONSOLIDACIÓN DE DATOS
# ============================================================================

def aggregate_up_field(series: pd.Series) -> Any:
    """
    Consolida un campo de unidad de proyecto eligiendo el mejor valor.
    
    Estrategia:
    1. Si hay valores no nulos, tomar el más frecuente (moda)
    2. Si hay empate, tomar el más largo (más completo)
    3. Si todo falla, tomar el primero
    
    Args:
        series: Serie de pandas con valores del campo
        
    Returns:
        Valor consolidado
    """
    # Eliminar nulos
    non_null = series.dropna()
    
    if len(non_null) == 0:
        return None
    
    # Calcular moda (valor más frecuente)
    mode_values = non_null.mode()
    
    if len(mode_values) > 0:
        # Si hay empate, elegir el string más largo
        if len(mode_values) > 1:
            return max(mode_values, key=lambda x: len(str(x)))
        return mode_values.iloc[0]
    
    # Fallback: primer valor
    return non_null.iloc[0]


def consolidate_coordinates(lat_series: pd.Series, lon_series: pd.Series) -> Tuple[float, float]:
    """
    Consolida coordenadas tomando el promedio de valores válidos.
    
    Args:
        lat_series: Serie con latitudes
        lon_series: Serie con longitudes
        
    Returns:
        Tupla (lat_promedio, lon_promedio) o (None, None)
    """
    # Filtrar valores válidos
    valid_mask = lat_series.notna() & lon_series.notna()
    
    if not valid_mask.any():
        return None, None
    
    # Calcular promedio
    lat_avg = lat_series[valid_mask].mean()
    lon_avg = lon_series[valid_mask].mean()
    
    return round(lat_avg, 8), round(lon_avg, 8)


# ============================================================================
# DEFINICIÓN DE CAMPOS
# ============================================================================

UNIDAD_PROYECTO_FIELDS = [
    'nombre_up',
    'nombre_up_detalle',
    'comuna_corregimiento',
    'barrio_vereda',
    'direccion',
    'tipo_equipamiento',
    'lat',
    'lon'
]

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
# FUNCIÓN PRINCIPAL DE AGRUPACIÓN
# ============================================================================

def agrupar_datos_geoespacial(df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Agrupa datos usando estrategia híbrida geoespacial + fuzzy matching.
    
    EXCEPCIÓN: Registros con clase_up = "Subsidios" NO se agrupan.
    Cada subsidio se mantiene como unidad individual (1 unidad = 1 intervención).
    
    Args:
        df: DataFrame con datos originales
        
    Returns:
        Diccionario estructurado con unidades e intervenciones
    """
    print(f"\n{'='*80}")
    print(f"🌍 AGRUPACIÓN GEOESPACIAL INTELIGENTE")
    print(f"{'='*80}")
    
    print(f"\n📊 Dataset inicial:")
    print(f"   • Total registros: {len(df)}")
    print(f"   • Registros con lat/lon: {df['lat'].notna().sum()}")
    print(f"   • Registros sin coordenadas: {df['lat'].isna().sum()}")
    
    # Paso 0: Separar subsidios (no se agrupan)
    mask_subsidios = df['clase_up'] == 'Subsidios'
    df_subsidios = df[mask_subsidios].copy()
    df_agrupables = df[~mask_subsidios].copy()
    
    print(f"\n🎯 Filtrado de datos:")
    print(f"   • Subsidios (no agrupables): {len(df_subsidios)}")
    print(f"   • Registros agrupables: {len(df_agrupables)}")
    
    # Paso 1: Asegurar que lat/lon sean numéricos
    df_agrupables['lat'] = pd.to_numeric(df_agrupables['lat'], errors='coerce')
    df_agrupables['lon'] = pd.to_numeric(df_agrupables['lon'], errors='coerce')
    
    # Paso 2: Clustering geoespacial (DBSCAN) - Solo agrupables
    df_agrupables = cluster_by_coordinates(df_agrupables, radius_meters=CLUSTERING_RADIUS_METERS)
    
    # Paso 3: Clustering por fuzzy matching (para registros sin coords) - Solo agrupables
    df_agrupables = cluster_by_fuzzy_matching(df_agrupables, threshold=FUZZY_THRESHOLD)
    
    # Paso 4: Crear clusters unificados - Solo agrupables
    df_agrupables = create_unified_clusters(df_agrupables)
    
    # Paso 5: Agrupar y consolidar AGRUPABLES
    print(f"\n🔨 Consolidando unidades de proyecto agrupables...")
    
    unidades = {}
    
    for cluster_id, group in df_agrupables.groupby('cluster_id'):
        # Consolidar campos de unidad de proyecto
        unidad = {}
        
        for field in UNIDAD_PROYECTO_FIELDS:
            if field in ['lat', 'lon']:
                continue  # Manejar coordenadas por separado
            
            if field in group.columns:
                unidad[field] = aggregate_up_field(group[field])
        
        # Consolidar coordenadas (promedio)
        lat_avg, lon_avg = consolidate_coordinates(group['lat'], group['lon'])
        unidad['lat'] = lat_avg
        unidad['lon'] = lon_avg
        
        # Crear lista de intervenciones
        intervenciones = []
        for idx, row in group.iterrows():
            intervencion = {}
            
            for field in INTERVENCION_FIELDS:
                if field in row.index:
                    valor = row[field]
                    if pd.notna(valor):
                        intervencion[field] = valor
            
            intervenciones.append(intervencion)
        
        unidad['intervenciones'] = intervenciones
        unidades[cluster_id] = unidad
    
    print(f"   ✅ Unidades agrupables consolidadas: {len(unidades)}")
    
    # Paso 6: Procesar SUBSIDIOS (cada uno es una unidad individual)
    print(f"\n💰 Procesando subsidios como unidades individuales...")
    
    subsidios_unidades = {}
    for idx, row in df_subsidios.iterrows():
        # Cada subsidio es su propia unidad
        cluster_id = f"SUBSIDIO-{idx}"
        
        unidad = {}
        for field in UNIDAD_PROYECTO_FIELDS:
            if field in ['lat', 'lon']:
                valor = pd.to_numeric(row.get(field), errors='coerce')
                unidad[field] = None if pd.isna(valor) else valor
            else:
                valor = row.get(field)
                unidad[field] = None if pd.isna(valor) else valor
        
        # Crear intervención única
        intervencion = {}
        for field in INTERVENCION_FIELDS:
            if field in row.index:
                valor = row[field]
                if pd.notna(valor):
                    intervencion[field] = valor
        
        unidad['intervenciones'] = [intervencion]
        subsidios_unidades[cluster_id] = unidad
    
    print(f"   ✅ Subsidios procesados: {len(subsidios_unidades)}")
    
    # Paso 7: Combinar agrupables y subsidios
    print(f"\n🔗 Combinando resultados:")
    print(f"   • Unidades agrupables: {len(unidades)}")
    print(f"   • Subsidios individuales: {len(subsidios_unidades)}")
    
    unidades_combinadas = {**unidades, **subsidios_unidades}
    print(f"   • Total unidades: {len(unidades_combinadas)}")
    
    # Paso 8: Asignar UPIDs
    print(f"\n🏷️ Asignando UPIDs...")
    unidades_con_upid = asignar_upids(unidades_combinadas)
    
    return unidades_con_upid


def asignar_upids(unidades: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Asigna UPIDs secuenciales a las unidades.
    
    Args:
        unidades: Diccionario de unidades con cluster_id como clave
        
    Returns:
        Diccionario con UPIDs como clave
    """
    resultado = {}
    upid_counter = 1
    
    for cluster_id in sorted(unidades.keys()):
        unidad = unidades[cluster_id]
        
        # Asignar UPID
        upid = f"UNP-{upid_counter}"
        unidad['upid'] = upid
        
        # Contar intervenciones
        n_intervenciones = len(unidad['intervenciones'])
        unidad['n_intervenciones'] = n_intervenciones
        
        # Asignar IDs a intervenciones (sin intervencion_num)
        for idx, intervencion in enumerate(unidad['intervenciones'], start=1):
            intervencion['intervencion_id'] = f"{upid}-{idx:02d}"
        
        resultado[upid] = unidad
        upid_counter += 1
    
    print(f"   ✅ UPIDs asignados: UNP-1 a UNP-{upid_counter-1}")
    
    return resultado


# ============================================================================
# FUNCIONES DE ANÁLISIS Y COMPARACIÓN
# ============================================================================

def generar_estadisticas_comparativas(
    unidades_simple: Dict[str, Dict],
    unidades_geo: Dict[str, Dict]
) -> Dict[str, Any]:
    """
    Compara estadísticas entre agrupación simple y geoespacial.
    
    Args:
        unidades_simple: Resultado de agrupación simple (hash)
        unidades_geo: Resultado de agrupación geoespacial
        
    Returns:
        Diccionario con estadísticas comparativas
    """
    stats = {
        'simple': {
            'total_unidades': len(unidades_simple),
            'total_intervenciones': sum(len(u['intervenciones']) for u in unidades_simple.values()),
            'promedio_intervenciones': 0
        },
        'geoespacial': {
            'total_unidades': len(unidades_geo),
            'total_intervenciones': sum(len(u['intervenciones']) for u in unidades_geo.values()),
            'promedio_intervenciones': 0
        },
        'mejora': {}
    }
    
    # Calcular promedios
    if stats['simple']['total_unidades'] > 0:
        stats['simple']['promedio_intervenciones'] = round(
            stats['simple']['total_intervenciones'] / stats['simple']['total_unidades'], 2
        )
    
    if stats['geoespacial']['total_unidades'] > 0:
        stats['geoespacial']['promedio_intervenciones'] = round(
            stats['geoespacial']['total_intervenciones'] / stats['geoespacial']['total_unidades'], 2
        )
    
    # Calcular mejora
    stats['mejora']['reduccion_unidades'] = stats['simple']['total_unidades'] - stats['geoespacial']['total_unidades']
    stats['mejora']['porcentaje_reduccion'] = round(
        (stats['mejora']['reduccion_unidades'] / stats['simple']['total_unidades'] * 100), 2
    ) if stats['simple']['total_unidades'] > 0 else 0
    
    stats['mejora']['incremento_promedio'] = round(
        stats['geoespacial']['promedio_intervenciones'] - stats['simple']['promedio_intervenciones'], 2
    )
    
    return stats


def mostrar_comparacion(stats: Dict[str, Any]):
    """
    Muestra comparación entre métodos de agrupación.
    
    Args:
        stats: Estadísticas comparativas
    """
    print(f"\n{'='*80}")
    print(f"📊 COMPARACIÓN: SIMPLE vs GEOESPACIAL")
    print(f"{'='*80}")
    
    print(f"\n🔹 AGRUPACIÓN SIMPLE (Hash de campos):")
    print(f"   • Unidades de proyecto: {stats['simple']['total_unidades']}")
    print(f"   • Intervenciones totales: {stats['simple']['total_intervenciones']}")
    print(f"   • Promedio intervenciones/unidad: {stats['simple']['promedio_intervenciones']}")
    
    print(f"\n🔹 AGRUPACIÓN GEOESPACIAL (DBSCAN + Fuzzy):")
    print(f"   • Unidades de proyecto: {stats['geoespacial']['total_unidades']}")
    print(f"   • Intervenciones totales: {stats['geoespacial']['total_intervenciones']}")
    print(f"   • Promedio intervenciones/unidad: {stats['geoespacial']['promedio_intervenciones']}")
    
    print(f"\n🎯 MEJORA:")
    print(f"   • Reducción de unidades: {stats['mejora']['reduccion_unidades']} ({stats['mejora']['porcentaje_reduccion']}%)")
    print(f"   • Incremento promedio intervenciones: +{stats['mejora']['incremento_promedio']}")
    
    if stats['mejora']['reduccion_unidades'] > 0:
        print(f"\n✅ El clustering geoespacial agrupó mejor los datos!")
        print(f"   Menos unidades duplicadas = Datos más consolidados")
    elif stats['mejora']['reduccion_unidades'] < 0:
        print(f"\n⚠️ El clustering geoespacial creó más grupos")
        print(f"   Posible causa: datos con coordenadas inexactas o radius muy pequeño")
    else:
        print(f"\n🤔 Ambos métodos produjeron resultados similares")


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """
    Función principal que ejecuta la prueba de agrupación geoespacial.
    """
    print(f"\n{'='*80}")
    print(f"🧪 PRUEBA: CLUSTERING GEOESPACIAL AVANZADO")
    print(f"{'='*80}")
    print(f"\nComparando dos estrategias:")
    print(f"  1️⃣  Agrupación SIMPLE: Hash de campos (método actual)")
    print(f"  2️⃣  Agrupación GEOESPACIAL: DBSCAN + Fuzzy Matching")
    
    # Extraer datos
    print(f"\n{'='*80}")
    print(f"📥 EXTRACCIÓN DE DATOS")
    print(f"{'='*80}")
    
    df = extract_and_save_unidades_proyecto()
    
    if df is None or df.empty:
        print("❌ Error: No se pudieron extraer los datos")
        return
    
    print(f"✅ Datos extraídos: {len(df)} registros")
    
    # Importar función de agrupación simple para comparar
    sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
    from test_agrupacion_unidades_intervenciones import agrupar_datos_por_unidad_proyecto
    
    # Método 1: Agrupación simple
    print(f"\n{'='*80}")
    print(f"1️⃣  MÉTODO SIMPLE (Hash)")
    print(f"{'='*80}")
    unidades_simple = agrupar_datos_por_unidad_proyecto(df.copy())
    
    # Método 2: Agrupación geoespacial
    unidades_geo = agrupar_datos_geoespacial(df.copy())
    
    # Comparar resultados
    stats = generar_estadisticas_comparativas(unidades_simple, unidades_geo)
    mostrar_comparacion(stats)
    
    # Guardar resultados
    print(f"\n{'='*80}")
    print(f"💾 GUARDANDO RESULTADOS")
    print(f"{'='*80}")
    
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'app_outputs', 'test_agrupacion')
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Guardar unidades geoespaciales
    geo_path = os.path.join(output_dir, f'unidades_geoespacial_{timestamp}.json')
    with open(geo_path, 'w', encoding='utf-8') as f:
        json.dump(unidades_geo, f, indent=2, ensure_ascii=False, default=str)
    print(f"💾 Unidades geoespaciales: {os.path.basename(geo_path)}")
    
    # Guardar estadísticas comparativas
    stats_path = os.path.join(output_dir, f'comparacion_metodos_{timestamp}.json')
    with open(stats_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    print(f"💾 Comparación: {os.path.basename(stats_path)}")
    
    print(f"\n{'='*80}")
    print(f"✅ PRUEBA COMPLETADA")
    print(f"{'='*80}")
    print(f"\n📌 Conclusiones:")
    print(f"   • El método geoespacial usa coordenadas GPS como verdad fundamental")
    print(f"   • Agrupa ubicaciones físicamente cercanas (< {CLUSTERING_RADIUS_METERS}m)")
    print(f"   • Usa fuzzy matching para registros sin coordenadas")
    print(f"   • Consolida nombres y datos tomando los valores más completos")
    
    print(f"\n📂 Archivos generados en: {output_dir}")


if __name__ == "__main__":
    main()
