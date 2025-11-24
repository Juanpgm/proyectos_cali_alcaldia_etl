# -*- coding: utf-8 -*-
"""
Módulo de transformación de datos de infraestructura vial.
Procesa shapefiles desde el directorio context/ y genera un GeoJSON compatible con Firebase.
"""

import os
import sys
import json
import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

CONTEXT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "context")
OUTPUT_FILE = os.path.join(CONTEXT_DIR, "unidades_proyecto.geojson")

# Nombres de archivos a procesar
SHAPEFILES = {
    "grupo_operativo": "Grupo_Operativo2024_lista.shp",
    "participativo_urbano": "Participativo_Urb2024_lista.shp",
    "puntos_estrategicos": "Puntos_Estrategicos2024_lista.shp"
}

# Estándar de columnas
COLUMNS_ORDER = [
    "FID", "referencia_proceso", "referencia_contrato", "bpin",
    "identificador", "fuente_financiacion", "nombre_up",
    "nombre_up_detalle", "comuna_corregimiento", "clase_obra",
    "unidad", "cantidad", "direccion",
    "barrio_vereda", "tipo_intervencion", "estado",
    "presupuesto_base", "avance_obra", "ano",
    "fecha_inicio", "fecha_fin", "plataforma",
    "url_proceso", "descripcion_intervencion", "nombre_centro_gestor", "geometry"
]

# Tipos de datos
COLUMNS_DTYPES = {
    'referencia_proceso': 'object',
    'referencia_contrato': 'object',
    'bpin': 'int64',
    'identificador': 'object',
    'fuente_financiacion': 'category',
    'nombre_up': 'object',
    'nombre_up_detalle': 'object',
    'comuna_corregimiento': 'category',
    'clase_obra': 'category',
    'unidad': 'category',
    'cantidad': 'float64',
    'direccion': 'object',
    'barrio_vereda': 'category',
    'tipo_intervencion': 'category',
    'estado': 'category',
    'presupuesto_base': 'int64',
    'avance_obra': 'float64',
    'ano': 'object',
    'fecha_inicio': 'object',
    'fecha_fin': 'object',
    'plataforma': 'category',
    'url_proceso': 'object',
    'descripcion_intervencion': 'object',
    'nombre_centro_gestor': 'object'
}


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def format_text_with_bis(text):
    """Formatea texto capitalizando pero manteniendo BIS en mayúsculas"""
    if isinstance(text, str):
        text = text.title()
        text = text.replace('Bis', 'BIS')
        return text
    return text


def format_comuna(text):
    """Formatea comunas añadiendo cero a números de un dígito"""
    if isinstance(text, str):
        if text.startswith("COMUNA ") and len(text) == len("COMUNA ") + 1 and text[-1].isdigit():
            return f"COMUNA 0{text[-1]}"
    return text


def normalize_estado_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza valores de estado a los 3 estándares válidos.
    
    Estados válidos:
    - "En alistamiento"
    - "En ejecución"
    - "Terminado"
    
    Reglas de negocio:
    - Si avance_obra = 0 → "En alistamiento"
    - Si avance_obra >= 100 → "Terminado"
    - Si 0 < avance_obra < 100 → "En ejecución"
    - Si estado es None, aplica lógica según avance_obra
    """
    if 'estado' not in df.columns:
        return df
    
    result_df = df.copy()
    unknown_states = set()
    
    def standardize_estado(row):
        val = row.get('estado')
        avance_obra = row.get('avance_obra')
        
        # REGLA DE NEGOCIO 1: Si avance_obra es cero, establecer "En alistamiento"
        if avance_obra is not None:
            try:
                avance_numeric = float(str(avance_obra).strip().replace(',', '.').replace('cero', '0').replace('(', '').replace(')', ''))
                
                if avance_numeric == 0.0:
                    return 'En alistamiento'
                
                if avance_numeric >= 100.0:
                    return 'Terminado'
                
                if avance_numeric > 0.0 and avance_numeric < 100.0:
                    if pd.isna(val) or val is None or str(val).strip() == '':
                        return 'En ejecución'
                        
            except (ValueError, TypeError):
                pass
        
        # Si no hay valor de estado (None, NaN, o string vacío), determinar por avance_obra
        if pd.isna(val) or val is None or str(val).strip() == '':
            if pd.isna(avance_obra) or avance_obra is None:
                return 'En alistamiento'
            return 'En ejecución'
        
        val_str = str(val).strip().lower()
        
        # Map variations to standard values
        if 'socializaci' in val_str or 'alistamiento' in val_str or 'planeaci' in val_str or 'preparaci' in val_str or 'por iniciar' in val_str:
            return 'En alistamiento'
        elif 'ejecuci' in val_str or 'proceso' in val_str or 'construcci' in val_str or 'desarrollo' in val_str:
            return 'En ejecución'
        elif 'finalizado' in val_str or 'terminado' in val_str or 'completado' in val_str or 'concluido' in val_str or 'entregado' in val_str or 'liquidaci' in val_str:
            return 'Terminado'
        else:
            unknown_states.add(val_str)
            
            # Default to 'En ejecución' for unknown states, but apply avance_obra logic
            try:
                if avance_obra is not None:
                    avance_numeric = float(str(avance_obra).strip().replace(',', '.'))
                    if avance_numeric >= 100:
                        return 'Terminado'
                    elif avance_numeric == 0:
                        return 'En alistamiento'
            except:
                pass
            
            return 'En ejecución'
    
    result_df['estado'] = result_df.apply(standardize_estado, axis=1)
    
    # Report unknown states found
    if unknown_states:
        print(f"⚠️  WARNING: Found {len(unknown_states)} unknown estado values that were normalized:")
        for state in sorted(unknown_states):
            count = (df['estado'].astype(str).str.lower() == state).sum()
            print(f"   - '{state}' ({count} occurrences)")
    
    # Validate that only valid states remain
    valid_states = {'En alistamiento', 'En ejecución', 'Terminado'}
    final_states = set(result_df['estado'].dropna().unique())
    invalid_final = final_states - valid_states
    
    if invalid_final:
        print(f"❌ ERROR: Invalid estados still present after normalization: {invalid_final}")
    else:
        print(f"✓ Estados normalizados exitosamente. Estados válidos: {sorted(final_states)}")
        for state in sorted(final_states):
            count = (result_df['estado'] == state).sum()
            print(f"   - '{state}': {count} registros")
    
    return result_df


# ============================================================================
# PROCESAMIENTO INDIVIDUAL DE SHAPEFILES
# ============================================================================

def process_grupo_operativo(shapefile_path):
    """Procesa el shapefile Grupo Operativo 2024"""
    print("\n" + "="*80)
    print("PROCESANDO: Grupo Operativo 2024")
    print("="*80)
    
    gdf = gpd.read_file(shapefile_path)
    print(f"✓ Cargado: {gdf.shape[0]} registros")
    
    # Eliminar columnas innecesarias
    cols_drop = [
        "X_INICIAL", "Y_INICIAL", "X_FINAL", "Y_FINAL",
        "X_INICIA_1", "Y_INICIA_1", "X_FINAL_1", "Y_FINAL_1",
        "id_barrio", "area", "perimetro", "Shape_Leng", "Shape_Area", "COM_CORRG",
        "estra_moda", "ID", "Longitud", "Barrio", "Id_1", "TIPO_INTER",
        "comuna_2", "Name", "Tipo_Mezcl", "Nompre_BP", "JERARQUIZA",
    ]
    gdf = gdf.drop(columns=[c for c in cols_drop if c in gdf.columns], errors='ignore')
    
    # Eliminar últimas 5 filas (geometrías nulas)
    gdf = gdf[:-5]
    
    # Renombrar columnas
    rename_dict = {
        "PopupInfo": "nombre_up",
        "Long_inter": "cantidad",
        "Dirección": "direccion",
        "Comuna": "comuna_corregimiento",
        "barrio_2": "barrio_vereda",
        "Tipo_Int_1": "tipo_intervencion",
        "ESTADO": "estado",
        "BP": "bp",
        "BPIN": "bpin",
        "VALOR_INTE": "presupuesto_base",
        "Año": "ano",
        "Tipo_Grupo": "identificador",
        "F__AVANCE": "avance_obra",
        "Tipo_Infra": "nombre_up_detalle",
        "FID_1": "FID"
    }
    gdf = gdf.rename(columns=rename_dict)
    
    # Agregar columnas nuevas
    new_cols = ["nombre_centro_gestor", "descripcion_intervencion", "url_proceso", "plataforma",
                "fecha_inicio", "fecha_fin", "referencia_proceso", "referencia_contrato",
                "fuente_financiacion", "clase_obra", "unidad"]
    for col in new_cols:
        gdf[col] = None
    
    # Reordenar columnas
    gdf = gdf[COLUMNS_ORDER]
    
    # Normalizar tipos de datos
    for col, dtype in COLUMNS_DTYPES.items():
        if col in gdf.columns:
            try:
                gdf[col] = gdf[col].astype(dtype)
            except:
                pass
    
    # Estandarizar mayúsculas/minúsculas
    text_cols = ["identificador", "nombre_up_detalle", "tipo_intervencion", "estado", "fuente_financiacion"]
    for col in text_cols:
        if col in gdf.columns:
            gdf[col] = gdf[col].str.title()
    
    # Limpiar presupuesto_base
    gdf['presupuesto_base'] = gdf['presupuesto_base'].astype(str).str.replace(r'[$. ]', '', regex=True)
    gdf['presupuesto_base'] = gdf['presupuesto_base'].str.replace(',', '.', regex=False)
    gdf['presupuesto_base'] = pd.to_numeric(gdf['presupuesto_base'])
    
    # Limpiar avance_obra
    gdf['avance_obra'] = gdf['avance_obra'].astype(str).str.replace('%', '', regex=False)
    gdf['avance_obra'] = gdf['avance_obra'].str.split(',').str[0]
    gdf['avance_obra'] = pd.to_numeric(gdf['avance_obra'])
    
    # Imputar valores
    gdf['clase_obra'] = "Obra Vial"
    gdf['unidad'] = "m"
    gdf['nombre_centro_gestor'] = "Secretaría de Infraestructura"
    gdf['fuente_financiacion'] = "Recursos Propios"
    
    # Limpiar cantidad
    gdf['cantidad'] = gdf['cantidad'].astype(str).str.split('.').str[0]
    gdf['cantidad'] = pd.to_numeric(gdf['cantidad'])
    
    # Formatear textos
    gdf['nombre_up'] = gdf['nombre_up'].apply(format_text_with_bis)
    gdf['direccion'] = gdf['direccion'].apply(format_text_with_bis)
    gdf['comuna_corregimiento'] = gdf['comuna_corregimiento'].apply(format_comuna)
    
    # Normalizar estados
    gdf = normalize_estado_values(gdf)
    
    # Reemplazar NaN por None
    gdf = gdf.replace({np.nan: None})
    
    print(f"✓ Procesado: {gdf.shape[0]} registros")
    return gdf


def process_participativo_urbano(shapefile_path):
    """Procesa el shapefile Participativo Urbano 2024"""
    print("\n" + "="*80)
    print("PROCESANDO: Presupuesto Participativo Urbano 2024")
    print("="*80)
    
    gdf = gpd.read_file(shapefile_path)
    print(f"✓ Cargado: {gdf.shape[0]} registros")
    
    # Eliminar columnas innecesarias
    cols_drop = [
        "X_INICIO", "Y_INICIO", "X_FINAL", "Y_FINAL",
        "X_INICIAL", "Y_INICIAL", "X_FINAL_1", "Y_FINAL_1",
        "Barrio_1", "ID_BARRIO", "COMUNA", "Id", "Tipo_Infra", "FolderPath",
        "estra_moda", "Longitud", "area", "perimetro", "Shape_Leng", "Shape_Area",
        "Tipo_Mezcl", "Nompre_BP", "id_barrio_", "BARRIO", "comuna_2"
    ]
    gdf = gdf.drop(columns=[c for c in cols_drop if c in gdf.columns], errors='ignore')
    
    # Renombrar columnas
    rename_dict = {
        "Name": "nombre_up",
        "Longitud_1": "cantidad",
        "Dirección": "direccion",
        "Comuna": "comuna_corregimiento",
        "barrio_2": "barrio_vereda",
        "Tipo_Inter": "tipo_intervencion",
        "ESTADO": "estado",
        "BP": "bp",
        "BPIN": "bpin",
        "VALOR_INTE": "presupuesto_base",
        "Año": "ano",
        "Tipo_Grupo": "identificador",
        "F__AVANCE": "avance_obra",
        "Comuna_1": "comuna_corregimiento",
        "FID_1": "FID"
    }
    gdf = gdf.rename(columns=rename_dict)
    
    # Agregar columnas nuevas
    new_cols = ["nombre_centro_gestor", "descripcion_intervencion", "url_proceso", "plataforma",
                "nombre_up_detalle", "fecha_inicio", "fecha_fin", "referencia_proceso",
                "referencia_contrato", "fuente_financiacion", "clase_obra", "unidad"]
    for col in new_cols:
        gdf[col] = None
    
    # Reordenar columnas
    gdf = gdf[COLUMNS_ORDER]
    
    # Normalizar tipos de datos (similar a grupo_operativo)
    for col, dtype in COLUMNS_DTYPES.items():
        if col in gdf.columns:
            try:
                gdf[col] = gdf[col].astype(dtype)
            except:
                pass
    
    # Estandarizar mayúsculas/minúsculas
    text_cols = ["identificador", "nombre_up_detalle", "tipo_intervencion", "estado", "fuente_financiacion"]
    for col in text_cols:
        if col in gdf.columns:
            gdf[col] = gdf[col].str.title()
    
    # Limpiar presupuesto_base
    gdf['presupuesto_base'] = gdf['presupuesto_base'].astype(str).str.replace(r'[$. ]', '', regex=True)
    gdf['presupuesto_base'] = gdf['presupuesto_base'].str.replace(',', '.', regex=False)
    gdf['presupuesto_base'] = pd.to_numeric(gdf['presupuesto_base'], errors='coerce').fillna(0).astype('int64')
    
    # Limpiar avance_obra
    gdf['avance_obra'] = gdf['avance_obra'].astype(str).str.replace('%', '', regex=False)
    gdf['avance_obra'] = gdf['avance_obra'].str.split(',').str[0]
    gdf['avance_obra'] = pd.to_numeric(gdf['avance_obra'], errors='coerce').fillna(0).astype('int64')
    
    # Imputar valores (Presupuesto Participativo)
    gdf['clase_obra'] = "Obra Vial"
    gdf['unidad'] = "m"
    gdf['nombre_centro_gestor'] = "Secretaría de Infraestructura"
    gdf['fuente_financiacion'] = "Presupuesto Participativo"
    
    # Limpiar cantidad
    gdf['cantidad'] = gdf['cantidad'].astype(str).str.split('.').str[0]
    gdf['cantidad'] = pd.to_numeric(gdf['cantidad'])
    
    # Formatear textos
    gdf['nombre_up'] = gdf['nombre_up'].apply(format_text_with_bis)
    gdf['direccion'] = gdf['direccion'].apply(format_text_with_bis)
    gdf['comuna_corregimiento'] = gdf['comuna_corregimiento'].apply(format_comuna)
    
    # Normalizar estados
    gdf = normalize_estado_values(gdf)
    
    # Reemplazar NaN por None
    gdf = gdf.replace({np.nan: None})
    
    print(f"✓ Procesado: {gdf.shape[0]} registros")
    return gdf


def process_puntos_estrategicos(shapefile_path):
    """Procesa el shapefile Puntos Estratégicos 2024"""
    print("\n" + "="*80)
    print("PROCESANDO: Puntos Estratégicos 2024")
    print("="*80)
    
    gdf = gpd.read_file(shapefile_path)
    print(f"✓ Cargado: {gdf.shape[0]} registros")
    
    # Eliminar columnas innecesarias
    cols_drop = [
        "X_INICIA_1", "Y_INICIA_1", "Lote", "X_FINAL", "Y_FINAL", "Tipo_de_in",
        "X_INICIAL", "Y_INICIAL", "X_FINAL_1", "Y_FINAL_1",
        "Barrio", "Comuna", "Id", "Tipo_Infra", "estra_moda",
        "Longitud_1", "area", "perimetro", "Shape_Leng", "Shape_Area",
        "Tipo_Mezcl", "Nompre_BP", "id_barrio", "comuna_2"
    ]
    gdf = gdf.drop(columns=[c for c in cols_drop if c in gdf.columns], errors='ignore')
    
    # Crear dirección
    if 'Direccion' in gdf.columns:
        gdf['direccion'] = gdf['Direccion'].copy()
        gdf = gdf.drop(columns=['Direccion'])
    if 'Dirección' in gdf.columns:
        gdf = gdf.drop(columns=['Dirección'])
    
    # Renombrar columnas
    rename_dict = {
        "Longitud": "cantidad",
        "Comuna_1": "comuna_corregimiento",
        "barrio_2": "barrio_vereda",
        "Tipo_Inter": "tipo_intervencion",
        "ESTADO": "estado",
        "BP": "bp",
        "BPIN": "bpin",
        "VALOR_INTE": "presupuesto_base",
        "Año": "ano",
        "Tipo_Grupo": "identificador",
        "F__AVANCE": "avance_obra",
        "FID_1": "FID"
    }
    gdf = gdf.rename(columns=rename_dict)
    
    # Organizar nombre_up y nombre_up_detalle
    gdf['nombre_up'] = gdf['direccion'].copy()
    
    if 'Jerarquiza' in gdf.columns and 'TRAMO' in gdf.columns:
        gdf['nombre_up_detalle'] = gdf['Jerarquiza'].astype(str) + ' - ' + gdf['TRAMO'].astype(str)
        gdf = gdf.drop(columns=['Jerarquiza', 'TRAMO'])
    
    # Agregar columnas nuevas
    new_cols = ["nombre_centro_gestor", "descripcion_intervencion", "url_proceso", "plataforma",
                "fecha_inicio", "fecha_fin", "referencia_proceso", "referencia_contrato",
                "fuente_financiacion", "clase_obra", "unidad"]
    for col in new_cols:
        if col not in gdf.columns:
            gdf[col] = None
    
    # Reordenar columnas
    gdf = gdf[COLUMNS_ORDER]
    
    # Normalizar tipos de datos
    for col, dtype in COLUMNS_DTYPES.items():
        if col in gdf.columns:
            try:
                gdf[col] = gdf[col].astype(dtype)
            except:
                pass
    
    # Estandarizar mayúsculas/minúsculas
    text_cols = ["identificador", "nombre_up_detalle", "tipo_intervencion", "estado", "fuente_financiacion"]
    for col in text_cols:
        if col in gdf.columns:
            gdf[col] = gdf[col].str.title()
    
    # Limpiar presupuesto_base
    gdf['presupuesto_base'] = gdf['presupuesto_base'].astype(str).str.replace(r'[$. ]', '', regex=True)
    gdf['presupuesto_base'] = gdf['presupuesto_base'].str.replace(',', '.', regex=False)
    gdf['presupuesto_base'] = pd.to_numeric(gdf['presupuesto_base'], errors='coerce').fillna(0).astype('int64')
    
    # Limpiar avance_obra
    gdf['avance_obra'] = gdf['avance_obra'].astype(str).str.replace('%', '', regex=False)
    gdf['avance_obra'] = gdf['avance_obra'].str.split(',').str[0]
    gdf['avance_obra'] = pd.to_numeric(gdf['avance_obra'], errors='coerce').fillna(0).astype('int64')
    
    # Imputar valores
    gdf['clase_obra'] = "Obra Vial"
    gdf['unidad'] = "m"
    gdf['nombre_centro_gestor'] = "Secretaría de Infraestructura"
    gdf['fuente_financiacion'] = "Recursos Propios"
    
    # Limpiar cantidad
    gdf['cantidad'] = gdf['cantidad'].astype(str).str.split('.').str[0]
    gdf['cantidad'] = pd.to_numeric(gdf['cantidad'])
    
    # Formatear textos
    gdf['nombre_up'] = gdf['nombre_up'].apply(format_text_with_bis)
    gdf['direccion'] = gdf['direccion'].apply(format_text_with_bis)
    gdf['comuna_corregimiento'] = gdf['comuna_corregimiento'].apply(format_comuna)
    
    # Normalizar estados
    gdf = normalize_estado_values(gdf)
    
    # Reemplazar NaN por None
    gdf = gdf.replace({np.nan: None})
    
    print(f"✓ Procesado: {gdf.shape[0]} registros")
    return gdf


# ============================================================================
# AGREGAR COLUMNA FRENTE_ACTIVO
# ============================================================================

def add_frente_activo(gdf):
    """
    Agrega la columna 'frente_activo' basada en condiciones de estado, clase_obra, 
    tipo_equipamiento y tipo_intervencion.
    
    Lógica:
    - 'Frente activo': registros en 'En ejecución' + clase_obra en ('Obras equipamientos', 'Obra vial', 'Espacio Público')
      y excluyendo tipo_equipamiento ('Vivienda mejoramiento', 'Vivienda nueva', 'Adquisición de predios', 'Señalización vial')
      y excluyendo tipo_intervencion ('Mantenimiento', 'Estudios y diseños', 'Transferencia directa')
    - 'Inactivo': mismas condiciones pero con estado 'Suspendido'
    - 'No aplica': todos los demás casos
    """
    result_gdf = gdf.copy()
    
    # Inicializar columna con 'No aplica' por defecto
    result_gdf['frente_activo'] = 'No aplica'
    
    # Definir listas de valores a excluir
    tipos_equipamiento_excluidos = [
        'Vivienda mejoramiento', 
        'Vivienda nueva', 
        'Adquisición de predios', 
        'Señalización vial'
    ]
    
    tipos_intervencion_excluidos = [
        'Mantenimiento', 
        'Estudios y diseños', 
        'Transferencia directa'
    ]
    
    # Definir clases válidas para frente activo (usar clase_obra en infraestructura)
    clases_validas = ['Obras equipamientos', 'Obra vial', 'Obra Vial', 'Espacio Público']
    
    # Condiciones base para frente activo (sin considerar el estado todavía)
    # Filtro 1: clase_obra debe estar en las clases válidas
    condicion_clase = result_gdf['clase_obra'].isin(clases_validas) if 'clase_obra' in result_gdf.columns else pd.Series([False] * len(result_gdf))
    
    # Filtro 2: tipo_equipamiento NO debe estar en la lista de excluidos
    condicion_tipo_equipamiento = ~result_gdf['tipo_equipamiento'].isin(tipos_equipamiento_excluidos) if 'tipo_equipamiento' in result_gdf.columns else pd.Series([True] * len(result_gdf))
    
    # Filtro 3: tipo_intervencion NO debe estar en la lista de excluidos
    condicion_tipo_intervencion = ~result_gdf['tipo_intervencion'].isin(tipos_intervencion_excluidos) if 'tipo_intervencion' in result_gdf.columns else pd.Series([True] * len(result_gdf))
    
    # Combinar todas las condiciones base
    condiciones_base = condicion_clase & condicion_tipo_equipamiento & condicion_tipo_intervencion
    
    # Aplicar lógica según estado
    if 'estado' in result_gdf.columns:
        # Frente activo: condiciones base + estado 'En ejecución'
        frente_activo_mask = condiciones_base & (result_gdf['estado'] == 'En ejecución')
        result_gdf.loc[frente_activo_mask, 'frente_activo'] = 'Frente activo'
        
        # Inactivo: condiciones base + estado 'Suspendido'
        inactivo_mask = condiciones_base & (result_gdf['estado'] == 'Suspendido')
        result_gdf.loc[inactivo_mask, 'frente_activo'] = 'Inactivo'
    
    # Reportar estadísticas
    frente_activo_count = (result_gdf['frente_activo'] == 'Frente activo').sum()
    inactivo_count = (result_gdf['frente_activo'] == 'Inactivo').sum()
    no_aplica_count = (result_gdf['frente_activo'] == 'No aplica').sum()
    
    print(f"\n✓ Columna 'frente_activo' agregada:")
    print(f"   - Frente activo: {frente_activo_count} registros")
    print(f"   - Inactivo: {inactivo_count} registros")
    print(f"   - No aplica: {no_aplica_count} registros")
    
    return result_gdf


# ============================================================================
# GENERACIÓN DEL GEOJSON
# ============================================================================

def export_to_geojson(gdf_combined, output_path):
    """Exporta el geodataframe combinado a GeoJSON compatible con Firebase"""
    print("\n" + "="*80)
    print("EXPORTANDO GEOJSON COMPATIBLE CON FIREBASE")
    print("="*80)
    
    # Filtrar solo Point y LineString geometries
    gdf_filtered = gdf_combined[gdf_combined['geometry'].geom_type.isin(['Point', 'LineString', 'MultiLineString'])].copy()
    
    print(f"📊 Geometrías filtradas:")
    print(f"  - Total features: {len(gdf_filtered)}")
    print(f"  - LineStrings: {len(gdf_filtered[gdf_filtered.geometry.geom_type == 'LineString'])}")
    print(f"  - MultiLineStrings: {len(gdf_filtered[gdf_filtered.geometry.geom_type == 'MultiLineString'])}")
    print(f"  - Points: {len(gdf_filtered[gdf_filtered.geometry.geom_type == 'Point'])}")
    
    # Formatear tipos de datos como strings para Firebase
    gdf_filtered['ano'] = gdf_filtered['ano'].astype(str)
    gdf_filtered['avance_obra'] = gdf_filtered['avance_obra'].astype(str)
    gdf_filtered['bpin'] = gdf_filtered['bpin'].astype(str)
    gdf_filtered['cantidad'] = gdf_filtered['cantidad'].astype(str)
    gdf_filtered['presupuesto_base'] = gdf_filtered['presupuesto_base'].astype(str)
    
    # GENERAR UPID PARA TODOS LOS REGISTROS
    print(f"\n🔢 Generando UPIDs...")
    gdf_filtered['upid'] = [f"UNP-{i+1:04d}" for i in range(len(gdf_filtered))]
    print(f"✓ UPIDs generados: desde UNP-0001 hasta UNP-{len(gdf_filtered):04d}")
    
    # Agregar campos requeridos
    gdf_filtered['tipo_equipamiento'] = 'Vias'
    gdf_filtered['centros_gravedad'] = False
    gdf_filtered['has_geometry'] = True
    gdf_filtered['geometry_type'] = gdf_filtered['geometry'].geom_type
    
    # Crear estructura GeoJSON
    geojson_data = {
        "type": "FeatureCollection",
        "features": []
    }
    
    print(f"\n🔄 Procesando features...")
    feature_count = 0
    
    for index, row in gdf_filtered.iterrows():
        geom = row.geometry
        geom_type = geom.geom_type
        
        # Convertir geometría a coordenadas 2D
        if geom_type == 'LineString':
            coords = [[round(coord[0], 8), round(coord[1], 8)] for coord in geom.coords if len(coord) >= 2]
        elif geom_type == 'MultiLineString':
            coords = [[[round(coord[0], 8), round(coord[1], 8)] for coord in line.coords if len(coord) >= 2] 
                     for line in geom.geoms]
        elif geom_type == 'Point':
            coords = [round(geom.x, 8), round(geom.y, 8)]
        else:
            continue
        
        # Crear feature
        feature = {
            "type": "Feature",
            "geometry": {
                "type": geom_type,
                "coordinates": coords
            },
            "properties": {
                "upid": row['upid'],
                "geometry_type": geom_type,
                "has_geometry": True,
                "centros_gravedad": False,
                "tipo_equipamiento": "Vias",
                "created_at": None,
                "processed_timestamp": None,
                "ano": row['ano'],
                "avance_obra": row['avance_obra'],
                "barrio_vereda": row['barrio_vereda'],
                "bpin": row['bpin'],
                "cantidad": row['cantidad'],
                "clase_obra": row['clase_obra'],
                "comuna_corregimiento": row['comuna_corregimiento'],
                "descripcion_intervencion": row['descripcion_intervencion'],
                "direccion": row['direccion'],
                "estado": row['estado'],
                "fecha_fin": row['fecha_fin'],
                "fecha_inicio": row['fecha_inicio'],
                "frente_activo": row['frente_activo'],
                "fuente_financiacion": row['fuente_financiacion'],
                "identificador": row['identificador'],
                "nombre_centro_gestor": row['nombre_centro_gestor'],
                "nombre_up": row['nombre_up'],
                "nombre_up_detalle": row['nombre_up_detalle'],
                "plataforma": row['plataforma'],
                "presupuesto_base": row['presupuesto_base'],
                "referencia_contrato": row['referencia_contrato'],
                "referencia_proceso": row['referencia_proceso'],
                "geometry_bounds": None,
                "microtio": None
            }
        }
        
        geojson_data["features"].append(feature)
        feature_count += 1
    
    # Guardar GeoJSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson_data, f, ensure_ascii=False, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    
    print(f"\n✓ GeoJSON exportado: {feature_count} features")
    print(f"✓ Tamaño: {file_size_kb:.2f} KB")
    print(f"✓ Ubicación: {output_path}")
    print(f"✓ Formato: Compatible con Firebase y NextJS")
    print(f"✓ tipo_equipamiento: 'Vias' (todos los registros)")


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal para ejecutar la transformación"""
    print("="*80)
    print("TRANSFORMACIÓN DE DATOS - INFRAESTRUCTURA VIAL")
    print("="*80)
    print(f"\nDirectorio context: {CONTEXT_DIR}")
    
    # Verificar que existen los shapefiles
    missing_files = []
    for name, filename in SHAPEFILES.items():
        filepath = os.path.join(CONTEXT_DIR, filename)
        if not os.path.exists(filepath):
            missing_files.append(filename)
            print(f"✗ No encontrado: {filename}")
        else:
            print(f"✓ Encontrado: {filename}")
    
    if missing_files:
        print(f"\n❌ ERROR: Faltan archivos shapefile en {CONTEXT_DIR}")
        print(f"   Archivos faltantes: {', '.join(missing_files)}")
        return False
    
    # Procesar cada shapefile
    gdfs = {}
    
    # Grupo Operativo
    gdf_grupo = process_grupo_operativo(os.path.join(CONTEXT_DIR, SHAPEFILES["grupo_operativo"]))
    gdfs['grupo_operativo'] = gdf_grupo
    
    # Participativo Urbano
    gdf_participativo = process_participativo_urbano(os.path.join(CONTEXT_DIR, SHAPEFILES["participativo_urbano"]))
    gdfs['participativo_urbano'] = gdf_participativo
    
    # Puntos Estratégicos
    gdf_puntos = process_puntos_estrategicos(os.path.join(CONTEXT_DIR, SHAPEFILES["puntos_estrategicos"]))
    gdfs['puntos_estrategicos'] = gdf_puntos
    
    # Combinar todos los geodataframes
    print("\n" + "="*80)
    print("COMBINANDO GEODATAFRAMES")
    print("="*80)
    
    gdf_combined = pd.concat([gdf_grupo, gdf_participativo, gdf_puntos], ignore_index=True)
    
    # Agregar tipo_equipamiento
    gdf_combined.insert(gdf_combined.columns.get_loc('identificador') + 1, 'tipo_equipamiento', "Infraestructura Vial")
    
    print(f"\n✓ Total combinado: {gdf_combined.shape[0]} registros")
    print(f"  - Grupo Operativo: {len(gdf_grupo)}")
    print(f"  - Participativo Urbano: {len(gdf_participativo)}")
    print(f"  - Puntos Estratégicos: {len(gdf_puntos)}")
    
    # Análisis de completitud
    print("\n📊 Análisis de completitud:")
    completeness = (1 - gdf_combined.isnull().sum() / len(gdf_combined)) * 100
    print(f"  - Campos con >= 90% completos: {(completeness >= 90).sum()}")
    print(f"  - Campos con < 50% completos: {(completeness < 50).sum()}")
    
    # Agregar columna frente_activo antes de exportar
    print("\n" + "="*80)
    print("AGREGANDO COLUMNA FRENTE_ACTIVO")
    print("="*80)
    gdf_combined = add_frente_activo(gdf_combined)
    
    # Exportar a GeoJSON
    export_to_geojson(gdf_combined, OUTPUT_FILE)
    
    print("\n" + "="*80)
    print("✅ TRANSFORMACIÓN COMPLETADA")
    print("="*80)
    print(f"\n📝 Siguiente paso:")
    print(f"   Ejecutar: python load_app/data_loading_unidades_proyecto_infraestructura.py")
    
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
