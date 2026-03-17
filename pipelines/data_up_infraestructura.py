"""
data_up_infraestructura.py
─────────────────────────
Módulo para cargar capas de infraestructura (shapefiles) a Firebase de forma
incremental, configurable y segura.

Cada capa se define con un archivo JSON en  pipelines/capas_config/  que describe
el mapeo de columnas, valores por defecto y reglas de limpieza.  Agregar nuevas
capas es tan simple como crear un nuevo JSON sin modificar este código.

Colecciones destino:
  • unidades_proyecto
  • intervenciones_unidades_proyecto

Uso CLI:
  python pipelines/data_up_infraestructura.py --capas infraestructura_2024 infraestructura_2025
  python pipelines/data_up_infraestructura.py --capas infraestructura_2024 --dry-run
  python pipelines/data_up_infraestructura.py --config ruta/config.json --verbose

Uso como librería:
  from pipelines.data_up_infraestructura import run_pipeline
  result = run_pipeline(["infraestructura_2024", "infraestructura_2025"])
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# ──────────────────────────────────────────────
# Resolución de rutas del proyecto
# ──────────────────────────────────────────────
_THIS_DIR = Path(__file__).resolve().parent          # pipelines/
PROJECT_ROOT = _THIS_DIR.parent                       # raíz del proyecto
CAPAS_CONFIG_DIR = _THIS_DIR / "capas_config"

COLLECTION_UP = "unidades_proyecto"
COLLECTION_INT = "intervenciones_unidades_proyecto"

# Columnas del esquema estándar de unidades de proyecto
STANDARD_UP_COLUMNS = [
    "upid", "nombre_up", "nombre_up_detalle", "direccion",
    "tipo_equipamiento", "comuna_corregimiento", "barrio_vereda",
    "geometry", "proyectos_estrategicos",
]

STANDARD_INT_COLUMNS = [
    "upid", "intervencion_id", "referencia_proceso", "referencia_contrato",
    "bpin", "identificador", "fuente_financiacion", "tipo_intervencion",
    "unidad", "cantidad", "estado", "presupuesto_base", "avance_obra",
    "fecha_inicio", "fecha_fin", "fecha_inauguracion", "url_proceso",
    "descripcion_intervencion", "observaciones", "nombre_centro_gestor",
    "clase_up",
]


# ──────────────────────────────────────────────
# 1. Configuración de capas
# ──────────────────────────────────────────────
def load_layer_config(config_path: str | Path) -> dict:
    """Lee y valida un JSON de configuración de capa.

    Si *config_path* no es absoluto se busca primero en ``CAPAS_CONFIG_DIR``
    y luego relativo a ``PROJECT_ROOT``.
    """
    p = Path(config_path)
    if not p.is_absolute():
        candidate = CAPAS_CONFIG_DIR / p
        if not candidate.exists() and not candidate.suffix:
            candidate = candidate.with_suffix(".json")
        if candidate.exists():
            p = candidate
        else:
            p = PROJECT_ROOT / p

    if not p.exists():
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {p}")

    with open(p, encoding="utf-8") as f:
        cfg = json.load(f)

    required = ("shapefile_path", "column_mapping", "default_values")
    for key in required:
        if key not in cfg:
            raise ValueError(f"Falta campo obligatorio '{key}' en {p.name}")

    # Resolver ruta del shapefile relativa al proyecto
    shp = Path(cfg["shapefile_path"])
    if not shp.is_absolute():
        shp = PROJECT_ROOT / shp
    if not shp.exists():
        raise FileNotFoundError(f"Shapefile no encontrado: {shp}")
    cfg["_resolved_shapefile"] = str(shp)
    cfg["_config_name"] = p.stem

    return cfg


# ──────────────────────────────────────────────
# 2. Lectura y transformación del shapefile
# ──────────────────────────────────────────────
def _capitalizar_texto(texto) -> str | None:
    """Title-case respetando None / NaN."""
    if texto is None or (isinstance(texto, float) and math.isnan(texto)):
        return None
    if pd.isna(texto):
        return None
    return str(texto).title()


def read_and_transform_shapefile(cfg: dict, verbose: bool = False) -> gpd.GeoDataFrame:
    """Lee un shapefile y aplica todas las transformaciones definidas en *cfg*."""

    shp_path = cfg["_resolved_shapefile"]
    if verbose:
        print(f"  Leyendo shapefile: {shp_path}")

    gdf = gpd.read_file(shp_path)
    original_count = len(gdf)

    # 2a – Eliminar columnas no deseadas (solo las que existen)
    cols_to_drop = [c for c in cfg.get("columns_to_drop", []) if c in gdf.columns]
    if cols_to_drop:
        gdf.drop(columns=cols_to_drop, inplace=True)

    # 2b – Renombrar columnas según mapeo (solo las que existen)
    mapping = {k: v for k, v in cfg["column_mapping"].items() if k in gdf.columns}
    gdf.rename(columns=mapping, inplace=True)

    # 2c – Crear columnas con valores por defecto (solo las que no existen aún)
    for col, val in cfg.get("default_values", {}).items():
        if col not in gdf.columns:
            gdf[col] = val

    # 2d – Capitalización de texto
    for col in cfg.get("text_capitalize_columns", []):
        if col in gdf.columns:
            gdf[col] = gdf[col].apply(_capitalizar_texto)

    # 2e – Reemplazos de texto
    for col, rules in cfg.get("replacements", {}).items():
        if col in gdf.columns:
            for old, new in rules.items():
                gdf[col] = gdf[col].replace(old, new)
                gdf[col] = gdf[col].str.replace(old, new, regex=False)

    # 2f – Limpieza numérica
    for col, rules in cfg.get("numeric_cleaning", {}).items():
        if col not in gdf.columns:
            continue
        series = gdf[col].astype(str)
        for char in rules.get("strip", []):
            series = series.str.replace(char, "", regex=False)
        if rules.get("comma_to_dot"):
            series = series.str.replace(",", ".", regex=False)
        gdf[col] = pd.to_numeric(series, errors="coerce").round(2)

    # 2g – Valores forzados post-limpieza
    for col, val in cfg.get("force_values", {}).items():
        if col in gdf.columns:
            gdf[col] = val

    # 2h – Rellenar nulos
    for col, val in cfg.get("fillna_values", {}).items():
        if col in gdf.columns:
            gdf[col] = gdf[col].fillna(val)

    # 2i – Conversión de año a entero
    anio_col = cfg.get("anio_field")
    if anio_col and anio_col in gdf.columns:
        gdf[anio_col] = pd.to_numeric(gdf[anio_col], errors="coerce").astype("Int64")

    # 2j – Reproyección a EPSG:4326 si es necesario
    if cfg.get("needs_crs_transform", False):
        gdf = gdf.to_crs(epsg=4326)

    if verbose:
        print(f"  → {original_count} registros leídos, {len(gdf)} tras transformar  [{cfg['_config_name']}]")

    return gdf


# ──────────────────────────────────────────────
# 3. Joins espaciales  (replica lógica del notebook)
# ──────────────────────────────────────────────
def _coords_to_point(coords):
    """Convierte coordenadas __geo_interface__ al Point del primer vértice.

    Soporta:
      • (lon, lat, ...)            → punto directo
      • ((lon, lat, z), (...))     → primer vértice de LineString
      • [[[lon,lat],[...]],...]    → primer vértice de MultiLineString
    """
    try:
        if coords is None:
            return None
        if isinstance(coords, (list, tuple)) and len(coords) > 0:
            first = coords[0]
            if isinstance(first, (int, float)) and len(coords) >= 2:
                return Point(float(coords[0]), float(coords[1]))
            if isinstance(first, (list, tuple)) and len(first) >= 2:
                if isinstance(first[0], (int, float)):
                    return Point(float(first[0]), float(first[1]))
                # MultiLineString: first ring → first coord
                inner = first[0]
                if isinstance(inner, (list, tuple)) and len(inner) >= 2:
                    return Point(float(inner[0]), float(inner[1]))
        return None
    except Exception:
        return None


def _run_line_spatial_joins(gdf: gpd.GeoDataFrame, verbose: bool = False) -> gpd.GeoDataFrame:
    """Joins ``intersects`` sobre las geometrías de línea — igual que celdas 22-23 / 44-45 del notebook."""
    barrios_path = PROJECT_ROOT / "basemaps" / "barrios_veredas.geojson"
    comunas_path = PROJECT_ROOT / "basemaps" / "comunas_corregimientos.geojson"

    if not barrios_path.exists() or not comunas_path.exists():
        if verbose:
            print("  ⚠ Basemaps no encontrados, se omiten joins espaciales de línea")
        gdf["barrio_vereda"] = None
        gdf["comuna_corregimiento"] = None
        return gdf

    barrios = gpd.read_file(str(barrios_path))
    comunas = gpd.read_file(str(comunas_path))

    # --- barrio_vereda (intersects) ---
    barrios_tmp = barrios.to_crs(gdf.crs) if gdf.crs != barrios.crs else barrios
    gdf_int_b = gpd.sjoin(gdf, barrios_tmp[["barrio_vereda", "geometry"]],
                          how="left", predicate="intersects")
    gdf_int_b = gdf_int_b.drop_duplicates(subset=gdf.columns.tolist(), keep="first")
    gdf["barrio_vereda"] = gdf_int_b["barrio_vereda"].values

    # --- comuna_corregimiento (intersects) ---
    comunas_tmp = comunas.to_crs(gdf.crs) if gdf.crs != comunas.crs else comunas
    gdf_int_c = gpd.sjoin(gdf, comunas_tmp[["comuna_corregimiento", "geometry"]],
                          how="left", predicate="intersects")
    gdf_int_c = gdf_int_c.drop_duplicates(subset=gdf.columns.tolist(), keep="first")
    gdf["comuna_corregimiento"] = gdf_int_c["comuna_corregimiento"].values

    if verbose:
        print(f"  → Line-intersect completado: {gdf['barrio_vereda'].notna().sum()} barrios, "
              f"{gdf['comuna_corregimiento'].notna().sum()} comunas")
    return gdf


def _build_up_dataframe(
    gdf_combined: gpd.GeoDataFrame,
    verbose: bool = False,
) -> pd.DataFrame:
    """Construye ``df_up`` idéntico al notebook (celdas 57-63).

    1. Extrae columnas UP + coordinates del primer vértice
    2. Spatial-join ``within`` sobre **Point** del primer vértice para
       ``comuna_corregimiento`` y ``barrio_vereda``
    3. Restaura la geometría de línea original como ``__geo_interface__``
    4. Verifica Pulmón de Oriente sobre los mismos Points
    """
    # ── 1. Columnas base (celda 57) ──
    df_up = gdf_combined[["upid", "nombre_up", "nombre_up_detalle",
                          "direccion", "tipo_equipamiento", "_action"]].copy()

    # Extraer coordinates (para crear Points) — celda 57
    df_up["_coordinates"] = gdf_combined["geometry"].apply(
        lambda g: g.__geo_interface__["coordinates"] if g is not None and not g.is_empty else None
    )

    # ── 2. Crear GeoDataFrame temporal de Points (celda 60) ──
    df_up["_point"] = df_up["_coordinates"].apply(_coords_to_point)
    pts_gdf = gpd.GeoDataFrame(
        df_up[["upid"]].copy(),
        geometry=df_up["_point"].values,
        crs="EPSG:4326",
    )
    pts_gdf = pts_gdf.reset_index()  # conservar el índice original

    barrios_path = PROJECT_ROOT / "basemaps" / "barrios_veredas.geojson"
    comunas_path = PROJECT_ROOT / "basemaps" / "comunas_corregimientos.geojson"

    if barrios_path.exists() and comunas_path.exists():
        barrios = gpd.read_file(str(barrios_path)).to_crs("EPSG:4326")
        comunas = gpd.read_file(str(comunas_path)).to_crs("EPSG:4326")

        # --- comunas (within) — como celda 60 ---
        pts_valid = pts_gdf.dropna(subset=["geometry"])
        join_comunas = gpd.sjoin(
            pts_valid[["index", "upid", "geometry"]],
            comunas[["comuna_corregimiento", "geometry"]],
            how="left", predicate="within",
        ).reset_index(drop=True).drop_duplicates(subset="index", keep="first").set_index("index")

        df_up["comuna_corregimiento"] = join_comunas["comuna_corregimiento"]

        # --- barrios (within) — como celda 60 ---
        join_barrios = gpd.sjoin(
            pts_valid[["index", "upid", "geometry"]],
            barrios[["barrio_vereda", "geometry"]],
            how="left", predicate="within",
        ).reset_index(drop=True).drop_duplicates(subset="index", keep="first").set_index("index")

        df_up["barrio_vereda"] = join_barrios["barrio_vereda"]
    else:
        df_up["comuna_corregimiento"] = None
        df_up["barrio_vereda"] = None

    # ── 3. Restaurar geometría de línea original (celda 61) ──
    geom_linea = gdf_combined.drop_duplicates(subset="upid", keep="first").set_index("upid")["geometry"]
    df_up["geometry"] = df_up["upid"].map(geom_linea).apply(
        lambda g: g.__geo_interface__ if g is not None and not g.is_empty else None
    )

    # ── 4. Pulmón de Oriente (celda 63) ──
    pulmon_path = PROJECT_ROOT / "basemaps" / "pulmon_oriente" / "PoligonoPropuestoPulmonDeOriente.geojson"
    df_up["proyectos_estrategicos"] = ""

    if pulmon_path.exists():
        pulmon = gpd.read_file(str(pulmon_path)).to_crs("EPSG:4326")
        pts_valid = pts_gdf.dropna(subset=["geometry"])
        if len(pts_valid) > 0:
            join_pulmon = gpd.sjoin(
                pts_valid[["geometry"]],
                pulmon[["geometry"]],
                how="left", predicate="within",
            )
            inside_idx = join_pulmon[join_pulmon["index_right"].notna()].index.unique()
            df_up.loc[inside_idx, "proyectos_estrategicos"] = "Pulmón de Oriente"

    if verbose:
        n_barrios = df_up["barrio_vereda"].notna().sum()
        n_comunas = df_up["comuna_corregimiento"].notna().sum()
        n_pulmon = (df_up["proyectos_estrategicos"] == "Pulmón de Oriente").sum()
        print(f"  → UP spatial: {n_barrios} barrios, {n_comunas} comunas, {n_pulmon} en Pulmón")

    # ── Limpiar columnas auxiliares y deduplicar ──
    df_up.drop(columns=["_coordinates", "_point"], inplace=True, errors="ignore")
    df_up = df_up.drop_duplicates(subset=["upid"], keep="first")

    return df_up


def _build_int_dataframe(
    gdf_combined: gpd.GeoDataFrame,
    verbose: bool = False,
) -> pd.DataFrame:
    """Construye ``df_int`` idéntico al notebook (celdas 67 + 71).

    Columnas exactas del notebook::

        upid, intervencion_id, referencia_proceso, referencia_contrato, bpin,
        identificador, fuente_financiacion, tipo_intervencion, unidad, cantidad,
        estado, presupuesto_base, avance_obra, fecha_inicio, fecha_fin,
        fecha_inauguracion, url_proceso, descripcion_intervencion, observaciones,
        nombre_centro_gestor, clase_up
    """
    requested = [
        "upid", "referencia_proceso", "referencia_contrato", "bpin", "identificador",
        "fuente_financiacion", "tipo_intervencion", "unidad", "cantidad", "estado",
        "presupuesto_base", "avance_obra", "fecha_inicio", "fecha_fin",
        "fecha_inauguracion", "url_proceso", "descripcion_intervencion",
        "observaciones", "nombre_centro_gestor", "clase_up", "_action",
    ]
    available = [c for c in requested if c in gdf_combined.columns]
    df_int = gdf_combined[available].copy()

    # intervencion_id  (celda 67)
    df_int["intervencion_id"] = df_int.groupby("upid").cumcount() + 1
    df_int["intervencion_id"] = df_int["upid"] + "-INT-" + df_int["intervencion_id"].astype(str)

    # Reordenar: upid, intervencion_id primero
    cols = ["upid", "intervencion_id"] + [c for c in df_int.columns if c not in ("upid", "intervencion_id")]
    df_int = df_int[cols]

    # Derivar estado según avance_obra  (celda 71)
    df_int["avance_obra"] = pd.to_numeric(df_int["avance_obra"], errors="coerce")
    df_int["estado"] = df_int.apply(_derive_estado, axis=1)

    if verbose:
        print(f"  → INT: {len(df_int)} intervenciones construidas")

    return df_int


# ──────────────────────────────────────────────
# 4. Fingerprint para detección incremental
# ──────────────────────────────────────────────
def _first_vertex_token(geom) -> str:
    """Extrae el primer vértice de cualquier geometría (Shapely o dict) y devuelve token.

    Soporta Point, LineString, MultiLineString tanto como objetos Shapely
    como dicts de Firebase ``{type, coordinates}``.
    """
    if geom is None:
        return "no_geom"

    # -- Caso 1: Objeto Shapely --
    if hasattr(geom, "geom_type"):
        if geom.is_empty:
            return "no_geom"
        try:
            if geom.geom_type == "Point":
                return f"{geom.x:.6f},{geom.y:.6f}"
            coords = list(geom.coords)
            first = coords[0][:2]
            return f"{first[0]:.6f},{first[1]:.6f}"
        except Exception:
            return "no_geom"

    # -- Caso 2: dict de Firebase {type, coordinates} --
    if isinstance(geom, dict):
        coords_raw = geom.get("coordinates")
        if isinstance(coords_raw, str):
            try:
                coords_raw = json.loads(coords_raw)
            except Exception:
                return "no_geom"
        if not isinstance(coords_raw, list) or len(coords_raw) == 0:
            return "no_geom"

        gtype = str(geom.get("type", "")).lower()
        if gtype == "point":
            # [lon, lat] o [lon, lat, z]
            return f"{float(coords_raw[0]):.6f},{float(coords_raw[1]):.6f}"
        # LineString: [[lon,lat], ...]
        first = coords_raw[0]
        while isinstance(first, list) and isinstance(first[0], list):
            first = first[0]
        if isinstance(first, list) and len(first) >= 2:
            return f"{float(first[0]):.6f},{float(first[1]):.6f}"
        return "no_geom"

    return "no_geom"


def build_fingerprint(row: pd.Series) -> str:
    """Genera un MD5 que identifica unívocamente un registro.

    Usa ``nombre_up + direccion + primer_vertice`` para detectar duplicados
    independientemente del UPID, tipo de geometría (Point vs LineString)
    o presencia de campos opcionales como bpin.
    """
    parts = [
        str(row.get("nombre_up", "") or "").strip().lower(),
        str(row.get("direccion", "") or "").strip().lower(),
        _first_vertex_token(row.get("geometry")),
    ]
    raw = "|".join(parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ──────────────────────────────────────────────
# 5. Firebase – conexión y helpers
# ──────────────────────────────────────────────
def _get_firebase_db():
    """Obtiene un cliente Firestore reutilizando database/config.py cuando esté disponible."""
    # Intentar usar el módulo centralizado del proyecto
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        from database.config import get_firestore_client
        db = get_firestore_client()
        if db:
            return db
    except Exception:
        pass

    # Fallback: inicialización directa (misma lógica del notebook)
    import firebase_admin
    from firebase_admin import credentials as fb_creds, firestore

    if not firebase_admin._apps:
        cred_candidates = [
            PROJECT_ROOT / "env" / "calitrack-secret.json",
            PROJECT_ROOT / "target-credentials-old-calitrack.json",
            PROJECT_ROOT / "target-credentials.json",
        ]
        cred_path = next((p for p in cred_candidates if p.exists()), None)
        if cred_path is None:
            raise FileNotFoundError("No se encontró archivo de credenciales Firebase.")
        cred = fb_creds.Certificate(str(cred_path))
        firebase_admin.initialize_app(cred, {"projectId": "calitrack-44403"})

    return firestore.client()


def get_next_upid(db) -> int:
    """Retorna el siguiente número disponible de UPID escaneando la colección."""
    max_num = 0
    for doc in db.collection(COLLECTION_UP).stream():
        data = doc.to_dict() or {}
        upid = data.get("upid") or data.get("UPID") or ""
        nums = re.findall(r"\d+", str(upid))
        if nums:
            n = int(nums[-1])
            if n > max_num:
                max_num = n
    return max_num + 1


def get_existing_up_fingerprints(db, verbose: bool = False) -> Dict[str, Dict[str, Any]]:
    """Descarga TODOS los registros de unidades_proyecto y calcula fingerprints.

    Escanea todos los docs (no filtra por nombre_centro_gestor porque ese campo
    NO existe en la colección UP).  El fingerprint usa ``nombre_up + direccion +
    primer_vertice`` para ser compatible con cualquier tipo de geometría
    (Point de cargas anteriores o LineString de este pipeline).

    Retorna  ``{fingerprint: {upid, doc_data}}``.
    """
    result: Dict[str, Dict[str, Any]] = {}
    count = 0

    for doc in db.collection(COLLECTION_UP).stream():
        data = doc.to_dict() or {}
        count += 1

        geom = data.get("geometry")
        geom_token = _first_vertex_token(geom)

        parts = [
            str(data.get("nombre_up", "") or "").strip().lower(),
            str(data.get("direccion", "") or "").strip().lower(),
            geom_token,
        ]
        fp = hashlib.md5("|".join(parts).encode("utf-8")).hexdigest()
        result[fp] = {"upid": data.get("upid", doc.id), "doc_data": data}

    if verbose:
        print(f"  → {count} registros existentes en '{COLLECTION_UP}' (todos)")

    return result


def get_existing_int_by_upid(db, upids: set, verbose: bool = False) -> Dict[str, Dict[str, Any]]:
    """Descarga intervenciones existentes para un conjunto de UPIDs.

    Retorna ``{intervencion_id: doc_data}``.
    """
    result: Dict[str, Dict[str, Any]] = {}
    if not upids:
        return result

    for doc in db.collection(COLLECTION_INT).stream():
        data = doc.to_dict() or {}
        if data.get("upid") in upids:
            result[doc.id] = data

    if verbose:
        print(f"  → {len(result)} intervenciones existentes descargadas")
    return result


# ──────────────────────────────────────────────
# 6. Reconciliación incremental
# ──────────────────────────────────────────────
_METADATA_FIELDS = {"created_at", "updated_at", "has_geometry", "geometry_type",
                    "_hash", "processed_timestamp", "report_id"}


def _fields_changed(new_data: dict, existing_data: dict) -> bool:
    """Compara campos (excluyendo metadatos) para detectar diferencias."""
    all_keys = (set(new_data.keys()) | set(existing_data.keys())) - _METADATA_FIELDS
    for k in all_keys:
        v_new = new_data.get(k)
        v_old = existing_data.get(k)
        # Normalizar None vs ausente
        if v_new is None and v_old is None:
            continue
        if v_new != v_old:
            return True
    return False


def reconcile_data(
    gdf: gpd.GeoDataFrame,
    existing_fps: Dict[str, Dict[str, Any]],
    next_upid_num: int,
    verbose: bool = False,
) -> Tuple[gpd.GeoDataFrame, dict]:
    """Clasifica cada fila como NEW / UPDATE / UNCHANGED y asigna UPIDs.

    Agrega columnas ``upid`` y ``_action`` al GeoDataFrame.
    Retorna ``(gdf_con_upid, stats)``.
    """
    stats = {"new": 0, "updated": 0, "unchanged": 0, "total": len(gdf)}

    upids: list[str] = []
    actions: list[str] = []
    upid_counter = next_upid_num

    for _, row in gdf.iterrows():
        fp = build_fingerprint(row)
        existing = existing_fps.get(fp)

        if existing is None:
            upid = f"UNP-{upid_counter}"
            upid_counter += 1
            actions.append("NEW")
            stats["new"] += 1
        else:
            upid = existing["upid"]
            new_flat = _row_to_flat_dict(row, upid)
            if _fields_changed(new_flat, existing["doc_data"]):
                actions.append("UPDATE")
                stats["updated"] += 1
            else:
                actions.append("UNCHANGED")
                stats["unchanged"] += 1

        upids.append(upid)

    gdf = gdf.copy()
    gdf["upid"] = upids
    gdf["_action"] = actions

    if verbose:
        print(f"  → Reconciliación: {stats['new']} nuevos, {stats['updated']} actualizados, "
              f"{stats['unchanged']} sin cambios")

    return gdf, stats


def _row_to_flat_dict(row: pd.Series, upid: str) -> dict:
    """Convierte una fila del GeoDataFrame a un dict plano comparable con Firebase."""
    d = {
        "upid": upid,
        "nombre_up": row.get("nombre_up"),
        "nombre_up_detalle": row.get("nombre_up_detalle"),
        "direccion": row.get("direccion"),
        "tipo_equipamiento": row.get("tipo_equipamiento"),
        "nombre_centro_gestor": row.get("nombre_centro_gestor"),
        "bpin": row.get("bpin"),
        "estado": row.get("estado"),
        "presupuesto_base": row.get("presupuesto_base"),
        "avance_obra": row.get("avance_obra"),
    }
    return {k: v for k, v in d.items() if v is not None and not (isinstance(v, float) and math.isnan(v))}


def _derive_estado(row: pd.Series) -> str:
    """Deriva el estado basado en avance_obra (misma lógica del notebook)."""
    avance = row.get("avance_obra")
    estado = row.get("estado")
    if pd.isna(avance):
        return estado
    if avance == 0 and estado != "Proyectado":
        return "En alistamiento"
    if 1 <= avance <= 99 and estado != "Suspendido":
        return "En ejecución"
    if avance == 100 and estado != "Inaugurado":
        return "Terminado"
    return estado


# ──────────────────────────────────────────────
# 7. Normalización de geometría y limpieza
# ──────────────────────────────────────────────
def normalize_geometry(geom) -> Optional[dict]:
    """Convierte geometría a formato Firestore compatible con el frontend.

    Retorna ``{"type": str, "coordinates": json_string}`` siguiendo RFC 7946.
    Elimina coordenadas Z (solo [lon, lat]).
    """
    if geom is None:
        return None

    if hasattr(geom, "__geo_interface__"):
        geom = geom.__geo_interface__

    if isinstance(geom, str):
        try:
            geom = json.loads(geom)
        except (json.JSONDecodeError, TypeError):
            return None

    if not isinstance(geom, dict):
        return None

    geom_type = geom.get("type")
    coords = geom.get("coordinates")

    if not geom_type or coords is None:
        return None

    def _strip_z(coord_list):
        if isinstance(coord_list, (list, tuple)):
            if len(coord_list) > 0 and isinstance(coord_list[0], (int, float)):
                return [float(coord_list[0]), float(coord_list[1])]
            return [_strip_z(c) for c in coord_list]
        return coord_list

    clean_coords = _strip_z(coords)

    return {
        "type": geom_type,
        "coordinates": json.dumps(clean_coords, separators=(",", ":")),
    }


def _clean_value(v) -> bool:
    """Retorna True si el valor es válido (no None, no NaN, no NaT)."""
    if v is None:
        return False
    if isinstance(v, float) and (pd.isna(v) or math.isnan(v)):
        return False
    try:
        if pd.isnull(v):
            return False
    except (TypeError, ValueError):
        pass
    return True


def _clean_record(record: dict) -> dict:
    """Elimina campos None/NaN y convierte timestamps a ISO."""
    clean = {}
    for k, v in record.items():
        if k.startswith("_"):
            continue
        if not _clean_value(v):
            continue
        if hasattr(v, "isoformat"):
            v = v.isoformat()
        clean[k] = v
    return clean


# ──────────────────────────────────────────────
# 8. Upload a Firebase
# ──────────────────────────────────────────────
def upload_up_to_firebase(
    db,
    df_up: pd.DataFrame,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Sube documentos NUEVOS de unidades_proyecto a Firebase.

    Solo sube registros con ``_action`` = NEW.  Los registros con UPID existente
    (UPDATE/UNCHANGED) se omiten para no sobreescribir documentos de otras
    secretarías que comparten la misma unidad de proyecto.
    """
    to_upload = df_up[df_up["_action"] == "NEW"]
    skipped = len(df_up) - len(to_upload)
    total = len(to_upload)
    stats = {"created": 0, "updated": 0, "skipped": skipped, "errors": 0}

    if total == 0:
        print("  ✓ No hay cambios en unidades_proyecto para subir")
        return stats

    if dry_run:
        print(f"  [DRY-RUN] Se subirían {total} nuevos a '{COLLECTION_UP}' ({skipped} omitidos por ya existir)")
        stats["created"] = total
        return stats

    collection_ref = db.collection(COLLECTION_UP)
    records = to_upload.to_dict("records")

    batch = db.batch()
    batch_count = 0
    processed = 0

    for record in records:
        doc_id = record.get("upid", f"doc_{processed}")
        doc_ref = collection_ref.document(doc_id)
        clean = _clean_record(record)

        if "geometry" in clean:
            clean["geometry"] = normalize_geometry(clean["geometry"])
            if clean["geometry"] is None:
                del clean["geometry"]

        batch.set(doc_ref, clean)
        stats["created"] += 1

        batch_count += 1
        processed += 1

        if batch_count >= 500:
            batch.commit()
            if verbose:
                print(f"    ↑ {processed}/{total} documentos subidos...")
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()

    print(f"  ✓ '{COLLECTION_UP}': {stats['created']} creados, "
          f"{stats['skipped']} omitidos (ya existían)")
    return stats


def upload_int_to_firebase(
    db,
    df_int: pd.DataFrame,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Sube documentos NUEVOS de intervenciones a Firebase.

    Solo sube registros con ``_action`` = NEW.  Las intervenciones de UPIDs
    que ya existían se omiten para evitar duplicados.
    """
    to_upload = df_int[df_int["_action"] == "NEW"]
    skipped = len(df_int) - len(to_upload)
    total = len(to_upload)
    stats = {"created": 0, "updated": 0, "skipped": skipped, "errors": 0}

    if total == 0:
        print("  ✓ No hay intervenciones nuevas para subir")
        return stats

    if dry_run:
        print(f"  [DRY-RUN] Se subirían {total} nuevas a '{COLLECTION_INT}' ({skipped} omitidas por ya existir)")
        stats["created"] = total
        return stats

    collection_ref = db.collection(COLLECTION_INT)
    records = to_upload.to_dict("records")

    batch = db.batch()
    batch_count = 0
    processed = 0

    for record in records:
        doc_id = record.get("intervencion_id", f"doc_{processed}")
        doc_ref = collection_ref.document(doc_id)
        clean = _clean_record(record)

        batch.set(doc_ref, clean)
        stats["created"] += 1

        batch_count += 1
        processed += 1

        if batch_count >= 500:
            batch.commit()
            if verbose:
                print(f"    ↑ {processed}/{total} intervenciones subidas...")
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()

    print(f"  ✓ '{COLLECTION_INT}': {stats['created']} creadas, "
          f"{stats['skipped']} omitidas (ya existían)")
    return stats


# ──────────────────────────────────────────────
# 9. Orquestador principal
# ──────────────────────────────────────────────
def run_pipeline(
    capas: List[str] | None = None,
    config_paths: List[str | Path] | None = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Ejecuta el pipeline completo de infraestructura.

    Parámetros
    ----------
    capas : list[str]
        Nombres de capa (se buscan en ``capas_config/{nombre}.json``).
    config_paths : list[str | Path]
        Rutas directas a archivos JSON de configuración.
    dry_run : bool
        Si True, ejecuta todo excepto la subida a Firebase.
    verbose : bool
        Muestra detalles adicionales.

    Retorna un diccionario con estadísticas del pipeline.
    """
    start = datetime.now()
    result = {
        "success": False,
        "capas_procesadas": [],
        "registros_totales": 0,
        "reconciliacion": {},
        "upload_up": {},
        "upload_int": {},
        "errors": [],
    }

    # ── 9.1 Resolver configuraciones ──
    configs: list[dict] = []
    all_names: list[str] = []

    if capas:
        for name in capas:
            try:
                cfg = load_layer_config(name)
                configs.append(cfg)
                all_names.append(cfg["_config_name"])
            except (FileNotFoundError, ValueError) as e:
                result["errors"].append(str(e))
                print(f"  ✗ Error cargando capa '{name}': {e}")

    if config_paths:
        for cp in config_paths:
            try:
                cfg = load_layer_config(cp)
                configs.append(cfg)
                all_names.append(cfg["_config_name"])
            except (FileNotFoundError, ValueError) as e:
                result["errors"].append(str(e))
                print(f"  ✗ Error cargando config '{cp}': {e}")

    if not configs:
        result["errors"].append("No se cargó ninguna configuración de capa.")
        print("✗ No se cargó ninguna configuración. Nada que hacer.")
        return result

    print(f"{'='*60}")
    print(f"PIPELINE INFRAESTRUCTURA — {', '.join(all_names)}")
    print(f"{'='*60}")

    # ── 9.2 Leer y transformar cada shapefile + line spatial joins ──
    print(f"\n[1/7] Leyendo y transformando {len(configs)} capa(s)...")
    gdfs: list[gpd.GeoDataFrame] = []
    for cfg in configs:
        try:
            gdf = read_and_transform_shapefile(cfg, verbose)
            # Line-intersect spatial joins por capa (celdas 22-23 / 44-45 del notebook)
            gdf = _run_line_spatial_joins(gdf, verbose)
            gdfs.append(gdf)
            result["capas_procesadas"].append(cfg["_config_name"])
        except Exception as e:
            msg = f"Error transformando '{cfg['_config_name']}': {e}"
            result["errors"].append(msg)
            print(f"  ✗ {msg}")

    if not gdfs:
        print("✗ Ninguna capa se pudo transformar correctamente.")
        return result

    # ── 9.3 Concatenar (celda 52 del notebook) ──
    gdf_combined = pd.concat(gdfs, ignore_index=True)
    if not isinstance(gdf_combined, gpd.GeoDataFrame):
        gdf_combined = gpd.GeoDataFrame(gdf_combined, geometry="geometry")
    if gdf_combined.crs is None:
        gdf_combined = gdf_combined.set_crs(epsg=4326)

    result["registros_totales"] = len(gdf_combined)
    print(f"  → {len(gdf_combined)} registros combinados")

    # ── 9.4 Conectar Firebase y obtener estado actual ──
    print(f"\n[2/7] Conectando a Firebase y descargando estado actual...")
    try:
        db = _get_firebase_db()
    except Exception as e:
        msg = f"Error de conexión a Firebase: {e}"
        result["errors"].append(msg)
        print(f"  ✗ {msg}")
        return result

    existing_fps = get_existing_up_fingerprints(db, verbose)
    next_upid = get_next_upid(db)

    print(f"  → {len(existing_fps)} UPs existentes en Firebase, próximo UPID: UNP-{next_upid}")

    # ── 9.5 Reconciliación: asignar UPIDs + clasificar NEW/UPDATE/UNCHANGED ──
    print(f"\n[3/7] Reconciliando datos (detección incremental)...")
    gdf_combined, recon_stats = reconcile_data(
        gdf_combined, existing_fps, next_upid, verbose
    )
    result["reconciliacion"] = recon_stats

    print(f"  → Nuevos: {recon_stats['new']}, Actualizados: {recon_stats['updated']}, "
          f"Sin cambios: {recon_stats['unchanged']}")

    # ── 9.6 Construir df_up con Point-within spatial joins (celdas 57-63) ──
    print(f"\n[4/7] Construyendo unidades de proyecto (spatial joins por punto)...")
    df_up = _build_up_dataframe(gdf_combined, verbose)

    # ── 9.7 Construir df_int (celdas 67 + 71) ──
    print(f"\n[5/7] Construyendo intervenciones...")
    df_int = _build_int_dataframe(gdf_combined, verbose)

    # ── 9.8 Upload ──
    print(f"\n[6/7] Subiendo UP a Firebase{' (DRY-RUN)' if dry_run else ''}...")
    result["upload_up"] = upload_up_to_firebase(db, df_up, dry_run, verbose)

    print(f"\n[7/7] Subiendo INT a Firebase{' (DRY-RUN)' if dry_run else ''}...")
    result["upload_int"] = upload_int_to_firebase(db, df_int, dry_run, verbose)

    # ── Resumen ──
    elapsed = (datetime.now() - start).total_seconds()
    result["success"] = len(result["errors"]) == 0
    result["elapsed_seconds"] = round(elapsed, 1)

    print(f"\n{'='*60}")
    print(f"RESULTADO: {'ÉXITO' if result['success'] else 'CON ERRORES'}")
    print(f"  Capas: {', '.join(result['capas_procesadas'])}")
    print(f"  Total registros: {result['registros_totales']}")
    print(f"  Nuevos: {recon_stats['new']} | Actualizados: {recon_stats['updated']} "
          f"| Sin cambios: {recon_stats['unchanged']}")
    print(f"  Tiempo: {elapsed:.1f}s")
    if result["errors"]:
        print(f"  Errores: {len(result['errors'])}")
        for err in result["errors"][:5]:
            print(f"    - {err}")
    print(f"{'='*60}\n")

    return result


# ──────────────────────────────────────────────
# 10. CLI
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Pipeline incremental de infraestructura → Firebase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Ejemplos:
  python pipelines/data_up_infraestructura.py --capas infraestructura_2024 infraestructura_2025
  python pipelines/data_up_infraestructura.py --capas infraestructura_2024 --dry-run
  python pipelines/data_up_infraestructura.py --config ruta/mi_config.json --verbose
        """,
    )
    parser.add_argument(
        "--capas", nargs="+", metavar="NOMBRE",
        help="Nombres de capa (busca en pipelines/capas_config/{nombre}.json)",
    )
    parser.add_argument(
        "--config", nargs="+", metavar="PATH",
        help="Rutas directas a archivos JSON de configuración",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Ejecuta todo excepto la subida a Firebase",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Muestra información detallada",
    )

    args = parser.parse_args()

    if not args.capas and not args.config:
        parser.error("Debe especificar --capas y/o --config")

    result = run_pipeline(
        capas=args.capas,
        config_paths=args.config,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
