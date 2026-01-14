# -*- coding: utf-8 -*-
"""
Geospatial Clustering Module for Unidades de Proyecto

This module implements intelligent geospatial clustering using DBSCAN and fuzzy matching
to group project interventions into project units based on physical location and textual similarity.

Key Features:
- DBSCAN clustering for geographically close records (< 20 meters)
- Fuzzy matching for records without GPS coordinates
- Subsidios exclusion (each subsidy is an independent unit)
- nombre_up_detalle differentiation (different details = different units)
- Hierarchical structure: Unidades de Proyecto → Intervenciones

Author: AI Assistant
Version: 1.0
Date: December 18, 2025
"""

import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from typing import Dict, List, Any, Tuple
from rapidfuzz import fuzz
from unidecode import unidecode
import hashlib

# ============================================================================
# CONFIGURATION
# ============================================================================

CLUSTERING_RADIUS_METERS = 40  # Radio de agrupación DBSCAN
FUZZY_THRESHOLD = 85  # Umbral de similitud para fuzzy matching
EARTH_RADIUS_KM = 6371  # Radio de la Tierra en km para haversine

# Campos de unidad de proyecto (nivel superior)
UNIDAD_PROYECTO_FIELDS = [
    'nombre_up',
    'nombre_up_detalle',
    'comuna_corregimiento',
    'barrio_vereda',
    'direccion',
    'tipo_equipamiento'
    # Nota: geometry se crea dinámicamente desde lat/lon consolidadas
    # lat/lon son campos temporales para clustering, no se exportan
]

# Campos de intervención (nivel hijo)
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
    'plataforma',
    'url_proceso',
    'clase_up',
    'nombre_centro_gestor'
]

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def normalize_text(text: str) -> str:
    """
    Normaliza texto para comparación: minúsculas, sin acentos, sin espacios extras.
    
    Args:
        text: Texto a normalizar
        
    Returns:
        Texto normalizado
    """
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text).lower()
    text = unidecode(text)
    text = ' '.join(text.split())
    return text


def calculate_fuzzy_similarity(text1: str, text2: str) -> float:
    """
    Calcula similitud entre dos textos usando fuzzy matching.
    Promedia Jaro-Winkler y Token Set Ratio para mayor robustez.
    
    Args:
        text1: Primer texto
        text2: Segundo texto
        
    Returns:
        Score de similitud (0-100)
    """
    if not text1 or not text2:
        return 0.0
    
    # Jaro-Winkler: Bueno para typos y variaciones cortas
    jaro = fuzz.ratio(text1, text2)
    
    # Token Set: Bueno para orden diferente de palabras
    token_set = fuzz.token_set_ratio(text1, text2)
    
    # Promedio ponderado (más peso a token_set)
    return (jaro * 0.4 + token_set * 0.6)


def consolidate_coordinates(lats: pd.Series, lons: pd.Series) -> Tuple[float, float]:
    """
    Consolida coordenadas GPS tomando el promedio de valores no nulos.
    Mejora: Valida que ambas coordenadas sean numéricas antes de promediar.
    
    Args:
        lats: Serie con latitudes
        lons: Serie con longitudes
        
    Returns:
        Tupla (lat_promedio, lon_promedio)
    """
    # Filtrar valores nulos Y asegurar que sean numéricos
    lats_valid = pd.to_numeric(lats, errors='coerce').dropna()
    lons_valid = pd.to_numeric(lons, errors='coerce').dropna()
    
    # Validar que tengamos coordenadas válidas
    if len(lats_valid) == 0 or len(lons_valid) == 0:
        return None, None
    
    # Tomar el promedio y redondear
    lat_avg = round(lats_valid.mean(), 8)
    lon_avg = round(lons_valid.mean(), 8)
    
    # Validar que el resultado esté en rangos razonables (Cali, Colombia)
    # Cali está aproximadamente en lat: 3.4, lon: -76.5
    if not (2.0 <= lat_avg <= 5.0 and -78.0 <= lon_avg <= -75.0):
        # Si está fuera de rango, buscar la primera coordenada válida individual
        for lat, lon in zip(lats_valid, lons_valid):
            if 2.0 <= lat <= 5.0 and -78.0 <= lon <= -75.0:
                return round(lat, 8), round(lon, 8)
        # Si ninguna es válida, devolver None
        return None, None
    
    return lat_avg, lon_avg


def create_geometry_from_coords(lat: float, lon: float) -> Dict[str, Any]:
    """
    Crea un objeto geometry GeoJSON Point desde coordenadas lat/lon.
    
    Args:
        lat: Latitud
        lon: Longitud
        
    Returns:
        Diccionario con geometry en formato GeoJSON o None
    """
    if lat is None or lon is None:
        return None
    
    if pd.isna(lat) or pd.isna(lon):
        return None
    
    # GeoJSON usa [lon, lat] (no [lat, lon])
    return {
        'type': 'Point',
        'coordinates': [float(lon), float(lat)]
    }


def aggregate_up_field(series: pd.Series) -> Any:
    """
    Consolida un campo de unidad de proyecto tomando el valor más completo.
    
    Args:
        series: Serie con valores del campo
        
    Returns:
        Valor consolidado
    """
    # Filtrar valores nulos y vacíos
    valid_values = series.dropna()
    valid_values = valid_values[valid_values != ""]
    
    if len(valid_values) == 0:
        return None
    
    # Si hay un solo valor único, retornarlo
    unique_values = valid_values.unique()
    if len(unique_values) == 1:
        return unique_values[0]
    
    # Tomar el valor más largo (asumiendo que es más completo)
    return max(unique_values, key=lambda x: len(str(x)))


# ============================================================================
# CLUSTERING FUNCTIONS
# ============================================================================

def cluster_by_coordinates(
    df: pd.DataFrame,
    radius_meters: int = CLUSTERING_RADIUS_METERS
) -> pd.DataFrame:
    """
    Agrupa registros por proximidad geográfica usando DBSCAN.
    
    Post-procesamiento: Separa clusters donde hay diferentes combinaciones
    de (nombre_up, nombre_up_detalle) para mantenerlos como unidades independientes.
    
    Args:
        df: DataFrame con coordenadas lat/lon
        radius_meters: Radio de agrupación en metros
        
    Returns:
        DataFrame con columna 'cluster_geo' añadida
    """
    result_df = df.copy()
    
    # Filtrar registros con coordenadas válidas
    mask_coords = (result_df['lat'].notna()) & (result_df['lon'].notna())
    df_coords = result_df[mask_coords].copy()
    
    if len(df_coords) == 0:
        print("⚠️  No hay registros con coordenadas válidas")
        result_df['cluster_geo'] = -1
        return result_df
    
    print(f"\n📍 Clustering geoespacial:")
    print(f"   • Registros con coordenadas: {len(df_coords)}")
    print(f"   • Radio de búsqueda: {radius_meters} metros")
    
    # Convertir radio de metros a radianes
    epsilon = radius_meters / 1000.0 / EARTH_RADIUS_KM
    
    # DBSCAN con métrica haversine (esférica)
    coords_rad = np.radians(df_coords[['lat', 'lon']].values)
    
    dbscan = DBSCAN(
        eps=epsilon,
        min_samples=1,
        metric='haversine',
        algorithm='ball_tree'
    )
    
    clusters = dbscan.fit_predict(coords_rad)
    df_coords['cluster_geo'] = clusters
    
    num_clusters_initial = len(set(clusters)) - (1 if -1 in clusters else 0)
    print(f"   ✅ Clusters geoespaciales iniciales: {num_clusters_initial}")
    
    # POST-PROCESAMIENTO: Separar por nombre_up_detalle
    print(f"   🔍 Verificando nombre_up_detalle como diferenciador...")
    
    max_cluster_id = clusters.max()
    new_cluster_id = max_cluster_id + 1
    
    for cluster_id in set(clusters):
        if cluster_id == -1:
            continue
        
        cluster_mask = df_coords['cluster_geo'] == cluster_id
        cluster_group = df_coords[cluster_mask]
        
        # Verificar si hay diferentes combinaciones de (nombre_up, nombre_up_detalle)
        unique_combinations = cluster_group[['nombre_up', 'nombre_up_detalle']].drop_duplicates()
        
        if len(unique_combinations) > 1:
            # Separar en sub-clusters
            for idx, (_, combo) in enumerate(unique_combinations.iterrows()):
                if idx == 0:
                    # El primero mantiene el cluster_id original
                    continue
                
                # Los demás obtienen nuevos cluster_ids
                combo_mask = (
                    (df_coords['nombre_up'] == combo['nombre_up']) &
                    (df_coords['nombre_up_detalle'] == combo['nombre_up_detalle']) &
                    (df_coords['cluster_geo'] == cluster_id)
                )
                
                df_coords.loc[combo_mask, 'cluster_geo'] = new_cluster_id
                new_cluster_id += 1
    
    num_clusters_adjusted = len(df_coords['cluster_geo'].unique()) - (1 if -1 in df_coords['cluster_geo'].values else 0)
    print(f"   ✅ Clusters ajustados por nombre_up_detalle: {num_clusters_adjusted}")
    
    # Actualizar DataFrame original
    result_df['cluster_geo'] = -1
    result_df.loc[mask_coords, 'cluster_geo'] = df_coords['cluster_geo']
    
    return result_df


def cluster_by_fuzzy_matching(
    df: pd.DataFrame,
    threshold: float = FUZZY_THRESHOLD
) -> pd.DataFrame:
    """
    Agrupa registros sin coordenadas por similitud textual (fuzzy matching).
    Concatena nombre_up + nombre_up_detalle para la comparación.
    
    Args:
        df: DataFrame con columna 'cluster_geo' ya calculada
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
    Unifica clusters geoespaciales y fuzzy en una sola columna 'cluster_id'.
    
    Args:
        df: DataFrame con 'cluster_geo' y 'cluster_fuzzy'
        
    Returns:
        DataFrame con 'cluster_id' unificado
    """
    result_df = df.copy()
    
    print(f"\n🔗 Unificando clusters:")
    
    # Crear cluster_id unificado
    max_geo_cluster = result_df['cluster_geo'].max()
    
    # Registros con cluster geoespacial
    mask_geo = result_df['cluster_geo'] != -1
    result_df.loc[mask_geo, 'cluster_id'] = result_df.loc[mask_geo, 'cluster_geo'].apply(
        lambda x: f"GEO-{int(x)}"
    )
    
    # Registros con cluster fuzzy (ajustar IDs para evitar colisiones)
    mask_fuzzy = (result_df['cluster_geo'] == -1) & (result_df['cluster_fuzzy'] != -1)
    result_df.loc[mask_fuzzy, 'cluster_id'] = result_df.loc[mask_fuzzy, 'cluster_fuzzy'].apply(
        lambda x: f"FUZZY-{int(x)}"
    )
    
    # Registros sin cluster (quedaron aislados)
    mask_no_cluster = (result_df['cluster_geo'] == -1) & (result_df['cluster_fuzzy'] == -1)
    isolated_count = mask_no_cluster.sum()
    
    # Asignar IDs individuales a registros aislados
    for idx, row in result_df[mask_no_cluster].iterrows():
        result_df.at[idx, 'cluster_id'] = f"ISOLATED-{idx}"
    
    total_clusters = result_df['cluster_id'].nunique()
    print(f"   ✅ Clusters unificados totales: {total_clusters}")
    if isolated_count > 0:
        print(f"   ℹ️  Registros aislados (sin cluster): {isolated_count}")
    
    return result_df


# ============================================================================
# MAIN GROUPING FUNCTION
# ============================================================================

def agrupar_datos_geoespacial(df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Agrupa datos usando estrategia híbrida geoespacial + fuzzy matching.
    
    EXCEPCIONES (NO se agrupan): Registros con clase_up en:
    - "Subsidios"
    - "Adquisición predial"
    - "Demarcación vial"
    
    O registros de centro gestor:
    - "Secretaría de Salud Pública" (cada IPS es una unidad independiente)
    
    Cada registro con estas características se mantiene como unidad individual (1 unidad = 1 intervención).
    
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
    
    # Paso 0: Separar registros que NO se agrupan (cada uno es una unidad individual)
    # Por clase_up:
    clases_no_agrupables = ['Subsidios', 'Adquisición predial', 'Demarcación vial']
    mask_clase_no_agrupable = df['clase_up'].isin(clases_no_agrupables)
    
    # Por centro gestor (Salud Pública: cada IPS es independiente)
    centros_no_agrupables = ['Secretaría de Salud Pública']
    mask_centro_no_agrupable = df['nombre_centro_gestor'].isin(centros_no_agrupables)
    
    # Combinar ambas máscaras
    mask_no_agrupables = mask_clase_no_agrupable | mask_centro_no_agrupable
    
    df_no_agrupables = df[mask_no_agrupables].copy()
    df_agrupables = df[~mask_no_agrupables].copy()
    
    print(f"\n🎯 Filtrado de datos:")
    print(f"   • No agrupables (individuales): {len(df_no_agrupables)}")
    for clase in clases_no_agrupables:
        count = (df['clase_up'] == clase).sum()
        if count > 0:
            print(f"      - {clase}: {count}")
    for centro in centros_no_agrupables:
        count = (df['nombre_centro_gestor'] == centro).sum()
        if count > 0:
            print(f"      - {centro}: {count}")
    print(f"   • Registros agrupables: {len(df_agrupables)}")
    
    # Paso 1: Validar y corregir coordenadas usando CoordinateValidator
    # Corrige problemas de formato, inversiones y rangos inválidos
    print(f"\n🔍 Validando coordenadas de registros agrupables...")
    
    from utils.coordinate_validator import CoordinateValidator
    validator = CoordinateValidator(verbose=False)
    
    # Validar todas las coordenadas del DataFrame
    validated_coords = validator.validate_dataframe(
        df_agrupables, 
        lat_col='lat', 
        lon_col='lon'
    )
    
    # Actualizar las coordenadas con las versiones corregidas
    df_agrupables['lat'] = validated_coords['lat']
    df_agrupables['lon'] = validated_coords['lon']
    
    # Mostrar estadísticas de validación
    stats = validator.get_statistics()
    if stats['inverted_coords_fixed'] > 0 or stats['decimal_separator_fixed'] > 0:
        print(f"   🔧 Correcciones aplicadas:")
        if stats['decimal_separator_fixed'] > 0:
            print(f"      • Separadores decimales: {stats['decimal_separator_fixed']}")
        if stats['inverted_coords_fixed'] > 0:
            print(f"      • Coordenadas invertidas: {stats['inverted_coords_fixed']}")
    
    # Paso 1.5: Asegurar que lat/lon sean numéricos
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
        # Mantener lat/lon para que el pipeline cree geometry después
        lat_avg, lon_avg = consolidate_coordinates(group['lat'], group['lon'])
        unidad['lat'] = lat_avg
        unidad['lon'] = lon_avg
        
        # Crear lista de intervenciones (sin geometry, sin lat/lon)
        intervenciones = []
        for idx, row in group.iterrows():
            intervencion = {}
            
            for field in INTERVENCION_FIELDS:
                if field in row.index:
                    valor = row[field]
                    # Manejar correctamente arrays, listas y valores simples
                    if isinstance(valor, (list, np.ndarray)):
                        if len(valor) > 0:
                            intervencion[field] = valor
                    elif pd.notna(valor):
                        intervencion[field] = valor
            
            intervenciones.append(intervencion)
        
        unidad['intervenciones'] = intervenciones
        unidades[cluster_id] = unidad
    
    print(f"   ✅ Unidades agrupables consolidadas: {len(unidades)}")
    
    # Paso 6: Procesar registros NO AGRUPABLES (cada uno es una unidad individual)
    # Incluye:
    # - Por clase_up: Subsidios, Adquisición predial, Demarcación vial
    # - Por centro gestor: Secretaría de Salud Pública
    print(f"\n💰 Procesando registros no agrupables como unidades individuales...")
    
    no_agrupables_unidades = {}
    for idx, row in df_no_agrupables.iterrows():
        # Cada registro no agrupable es su propia unidad
        cluster_id = f"INDIVIDUAL-{idx}"
        
        unidad = {}
        
        # Extraer campos de unidad de proyecto
        for field in UNIDAD_PROYECTO_FIELDS:
            valor = row.get(field)
            unidad[field] = None if pd.isna(valor) else valor
        
        # Extraer lat/lon (mantener para que el pipeline cree geometry después)
        # Normalizar formato: reemplazar comas por puntos antes de convertir
        lat_str = str(row.get('lat', '')).replace(',', '.')
        lon_str = str(row.get('lon', '')).replace(',', '.')
        lat_val = pd.to_numeric(lat_str, errors='coerce')
        lon_val = pd.to_numeric(lon_str, errors='coerce')
        unidad['lat'] = None if pd.isna(lat_val) else lat_val
        unidad['lon'] = None if pd.isna(lon_val) else lon_val
        
        # Crear intervención única (sin geometry, sin lat/lon)
        intervencion = {}
        for field in INTERVENCION_FIELDS:
            if field in row.index:
                valor = row[field]
                # Manejar correctamente arrays, listas y valores simples
                if isinstance(valor, (list, np.ndarray)):
                    if len(valor) > 0:
                        intervencion[field] = valor
                elif pd.notna(valor):
                    intervencion[field] = valor
        
        unidad['intervenciones'] = [intervencion]
        no_agrupables_unidades[cluster_id] = unidad
    
    print(f"   ✅ Unidades individuales procesadas: {len(no_agrupables_unidades)}")
    
    # Paso 7: Combinar agrupables y no agrupables
    print(f"\n🔗 Combinando resultados:")
    print(f"   • Unidades agrupables: {len(unidades)}")
    print(f"   • Unidades individuales: {len(no_agrupables_unidades)}")
    
    unidades_combinadas = {**unidades, **no_agrupables_unidades}
    print(f"   • Total unidades: {len(unidades_combinadas)}")
    
    # Paso 8: Asignar UPIDs
    print(f"\n🏷️ Asignando UPIDs...")
    unidades_con_upid = asignar_upids(unidades_combinadas)
    
    return unidades_con_upid


def asignar_upids(unidades: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Asigna UPIDs secuenciales a las unidades.
    Elimina cluster_original y agrega n_intervenciones.
    
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


def convert_unidades_to_dataframe(unidades: Dict[str, Dict]) -> pd.DataFrame:
    """
    Convierte el diccionario de unidades jerárquico a un DataFrame plano
    para compatibilidad con el pipeline actual.
    
    Args:
        unidades: Diccionario de unidades con intervenciones
        
    Returns:
        DataFrame con una fila por intervención, incluyendo campos de unidad
    """
    records = []
    
    for upid, unidad in unidades.items():
        # Campos de unidad (nivel superior)
        unidad_fields = {
            'upid': unidad.get('upid'),
            'n_intervenciones': unidad.get('n_intervenciones'),
            'nombre_up': unidad.get('nombre_up'),
            'nombre_up_detalle': unidad.get('nombre_up_detalle'),
            'comuna_corregimiento': unidad.get('comuna_corregimiento'),
            'barrio_vereda': unidad.get('barrio_vereda'),
            'direccion': unidad.get('direccion'),
            'tipo_equipamiento': unidad.get('tipo_equipamiento'),
            'lat': unidad.get('lat'),  # Mantener lat/lon para que pipeline cree geometry
            'lon': unidad.get('lon')
        }
        
        # Crear una fila por cada intervención
        for intervencion in unidad.get('intervenciones', []):
            record = {**unidad_fields, **intervencion}
            records.append(record)
    
    df = pd.DataFrame(records)
    
    print(f"\n✅ DataFrame generado:")
    print(f"   • Filas (intervenciones): {len(df)}")
    print(f"   • Columnas: {len(df.columns)}")
    print(f"   • Unidades únicas: {df['upid'].nunique()}")
    
    return df
