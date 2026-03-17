# -*- coding: utf-8 -*-
"""
auto_config_shapefile.py
────────────────────────
Utilidad que lee cualquier shapefile nuevo, identifica automáticamente qué
columnas corresponden al esquema estándar de unidades de proyecto (aunque se
llamen diferente), realiza la imputación de variables faltantes y genera un
JSON de configuración listo para ``data_up_infraestructura.py``.

Estrategia de detección:
  1. Coincidencia exacta (case-insensitive) con nombres del esquema.
  2. Coincidencia por alias conocidos — diccionario de variantes históricas.
  3. Coincidencia difusa (difflib) sobre nombres de columna.
  4. Heurísticas por contenido — analiza valores de la columna para deducir
     su rol semántico (e.g. columna con "$" y "." → presupuesto).

Uso:
  python pipelines/auto_config_shapefile.py ruta/shapefile.shp
  python pipelines/auto_config_shapefile.py ruta/shapefile.shp --nombre mi_capa
  python pipelines/auto_config_shapefile.py ruta/shapefile.shp --guardar
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import geopandas as gpd
import pandas as pd

# ──────────────────────────────────────────────
# Constantes y paths
# ──────────────────────────────────────────────
_THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _THIS_DIR.parent
CAPAS_CONFIG_DIR = _THIS_DIR / "capas_config"

# ──────────────────────────────────────────────
# Esquema objetivo: columnas que el pipeline espera
# ──────────────────────────────────────────────
# Campos mapeables — se esperan encontrar en el shapefile
MAPPABLE_FIELDS = {
    "direccion":               {"type": "text",    "required": True},
    "nombre_up":               {"type": "text",    "required": True},
    "nombre_up_detalle":       {"type": "text",    "required": False},
    "tipo_intervencion":       {"type": "text",    "required": True},
    "bpin":                    {"type": "text",    "required": False},
    "bp":                      {"type": "text",    "required": False},
    "identificador":           {"type": "text",    "required": False},
    "estado":                  {"type": "text",    "required": True},
    "presupuesto_base":        {"type": "numeric", "required": False},
    "cantidad":                {"type": "numeric", "required": False},
    "avance_obra":             {"type": "numeric", "required": False},
    "anio":                    {"type": "numeric", "required": False},
    "fecha_inicio":            {"type": "date",    "required": False},
    "fecha_fin":               {"type": "date",    "required": False},
    "fecha_inauguracion":      {"type": "date",    "required": False},
}

# Campos que SIEMPRE van como default_values (constantes para infraestructura)
FIXED_DEFAULTS = {
    "unidad": "m",
    "tipo_equipamiento": "Vías",
    "clase_up": "Obra vial",
    "nombre_centro_gestor": "Secretaría de Infraestructura",
}

# Campos que van como null si no se encuentran
NULL_DEFAULTS = [
    "referencia_proceso", "referencia_contrato", "fuente_financiacion",
    "fecha_inauguracion", "plataforma", "url_proceso",
    "descripcion_intervencion", "observaciones",
]

# ──────────────────────────────────────────────
# Alias conocidos — variantes históricas de nombres
# clave = nombre en minúsculas del shapefile, valor = campo destino
# ──────────────────────────────────────────────
KNOWN_ALIASES: Dict[str, str] = {
    # direccion
    "dirección":        "direccion",
    "direccion":        "direccion",
    "direcci_1":        "direccion",
    "dir":              "direccion",
    # nombre_up
    "tipo_infra":       "nombre_up",
    "jerarquiz":        "nombre_up",
    "infraestructura":  "nombre_up",
    "tipo_infraestruc": "nombre_up",
    "jerarquia":        "nombre_up",
    "jerarquizacion":   "nombre_up",
    # nombre_up_detalle
    "tipo_mezcl":       "nombre_up_detalle",
    "mezcla":           "nombre_up_detalle",
    "tipo_mezcla":      "nombre_up_detalle",
    # tipo_intervencion
    "tipo_inter":       "tipo_intervencion",
    "tipo_int_1":       "tipo_intervencion",
    "tipo_int_2":       "tipo_intervencion",
    "intervencion":     "tipo_intervencion",
    # bpin
    "bpin":             "bpin",
    "cod_bpin":         "bpin",
    # bp
    "bp":               "bp",
    "bp_calcula":       "bp",
    "bp_calculado":     "bp",
    # identificador
    "tipo_grupo":       "identificador",
    "grupo":            "identificador",
    # estado
    "estado":           "estado",
    "estado_rep":       "_drop",   # estado de reporte, no es el estado principal
    # presupuesto
    "valor_inte":       "presupuesto_base",
    "valor_interv":     "presupuesto_base",
    "valor_base":       "_drop",
    "valor_metr":       "_drop",
    # cantidad / metros
    "metros":           "cantidad",
    "longitud":         "cantidad",
    "longitud_1":       "_drop",
    "longitud_v":       "_drop",
    # avance
    "f__avance":        "avance_obra",
    "__avance":         "avance_obra",
    "avance":           "avance_obra",
    "porcentaje_avance":"avance_obra",
    # año
    "año":              "anio",
    "anio":             "anio",
    "ano":              "anio",
    # fechas
    "fecha_inci":       "fecha_inicio",
    "fecha_inicio":     "fecha_inicio",
    "fecha_ini":        "fecha_inicio",
    "fecha_fin":        "fecha_fin",
    "fecha_final":      "fecha_fin",
    "fecha_inaug":      "fecha_inauguracion",
    # coordenadas y auxiliares → drop
    "x_inicial":        "_drop",
    "y_inicial":        "_drop",
    "x_final":          "_drop",
    "y_final":          "_drop",
    "x_inicia_1":       "_drop",
    "y_inicia_1":       "_drop",
    "x_final_1":        "_drop",
    "y_final_1":        "_drop",
    "x_centro":         "_drop",
    "y_centro":         "_drop",
    "longitud_v":       "_drop",
    "ancho__via":       "_drop",
    "f__carrile":       "_drop",
    # identificadores genéricos → drop
    "id":               "_drop",
    "id_1":             "_drop",
    "fid_1":            "_drop",
    "fid":              "_drop",
    "objectid":         "_drop",
    # barrio/comuna — se eliminan porque vienen de spatial join
    "barrio":           "_drop",
    "comuna":           "_drop",
    "barrio_1":         "_drop",
    "comuna_1":         "_drop",
    "barrio_vereda":    "_drop",
    "comuna_corregimiento": "_drop",
    # otros conocidos → drop
    "nompre_bp":        "_drop",
    "mes":              "_drop",
}

# ──────────────────────────────────────────────
# Heurísticas por contenido
# ──────────────────────────────────────────────

def _analyze_column_content(series: pd.Series) -> Dict[str, Any]:
    """Analiza el contenido de una columna y devuelve indicadores."""
    info: Dict[str, Any] = {"dtype": str(series.dtype), "n_null": int(series.isna().sum())}
    sample = series.dropna()
    if len(sample) == 0:
        info["empty"] = True
        return info

    info["n_unique"] = int(sample.nunique())
    info["sample"] = sample.head(5).tolist()

    str_vals = sample.astype(str)

    # Patrón monetario:  "$ 1.234.567,89"
    currency_pct = str_vals.str.match(r'^\$\s*[\d.,]+$').mean()
    info["is_currency"] = currency_pct > 0.5

    # Patrón porcentaje:  "85%" or 0-100 integer
    pct_str = str_vals.str.match(r'^\d+%$').mean()
    info["is_percentage_str"] = pct_str > 0.3

    # Patrón fecha
    date_pct = str_vals.str.match(r'\d{4}-\d{2}-\d{2}').mean()
    info["is_date"] = date_pct > 0.3

    # Numérico puro
    numeric = pd.to_numeric(sample, errors="coerce")
    info["numeric_pct"] = float(numeric.notna().mean())
    if info["numeric_pct"] > 0.8:
        info["min"] = float(numeric.min())
        info["max"] = float(numeric.max())
        # 0-100 rango → probablemente porcentaje/avance
        if 0 <= info["min"] and info["max"] <= 100:
            info["range_0_100"] = True
        # Año: rango 2000-2030
        if 2000 <= info["min"] and info["max"] <= 2040 and info["n_unique"] <= 10:
            info["is_year"] = True

    # BPIN pattern: número largo (13+ dígitos)
    bpin_like = str_vals.str.match(r'^\d{10,}\.?0*$').mean()
    info["is_bpin"] = bpin_like > 0.3

    # BP pattern: "BP..." 
    bp_like = str_vals.str.match(r'^BP\d+', case=False).mean()
    info["is_bp"] = bp_like > 0.3

    # Estado keywords
    estado_kw = {"terminado", "finalizado", "ejecución", "ejecucion", "suspendido",
                 "alistamiento", "proyectado", "inaugurado", "en ejecución"}
    val_lower = set(str_vals.str.lower().str.strip().unique())
    info["is_estado"] = len(val_lower & estado_kw) > 0

    # Dirección keywords
    dir_kw = {"calle", "carrera", "avenida", "diagonal", "transversal", "entre"}
    joined = " ".join(str_vals.head(20).str.lower())
    info["is_direccion"] = any(k in joined for k in dir_kw)

    return info


def _guess_by_content(col_name: str, info: Dict[str, Any]) -> Optional[str]:
    """Intenta adivinar el campo destino basándose en el contenido."""
    if info.get("empty"):
        return None

    if info.get("is_currency"):
        return "presupuesto_base"
    if info.get("is_percentage_str"):
        return "avance_obra"
    if info.get("is_bpin"):
        return "bpin"
    if info.get("is_bp"):
        return "bp"
    if info.get("is_estado"):
        return "estado"
    if info.get("is_direccion"):
        return "direccion"
    if info.get("is_year"):
        return "anio"
    if info.get("is_date"):
        # Distinguir inicio vs fin por nombre
        name_low = col_name.lower()
        if "fin" in name_low or "final" in name_low:
            return "fecha_fin"
        if "inaug" in name_low:
            return "fecha_inauguracion"
        return "fecha_inicio"

    return None


# ──────────────────────────────────────────────
# Motor principal de detección
# ──────────────────────────────────────────────

def detect_mappings(gdf: gpd.GeoDataFrame, verbose: bool = False) -> Dict:
    """
    Analiza un GeoDataFrame y devuelve un diccionario con:
      - column_mapping:  {col_origen: col_destino}
      - columns_to_drop: [cols sin mapeo útil]
      - unmapped:        [cols que no se pudieron resolver]
      - analysis:        {col: info_dict}  (detalle del análisis)
    """
    columns = [c for c in gdf.columns if c != "geometry"]
    mapping: Dict[str, str] = {}
    to_drop: List[str] = []
    unmapped: List[str] = []
    analysis: Dict[str, Dict] = {}
    assigned_targets: Set[str] = set()

    # ── Paso 1: coincidencia exacta (case-insensitive) con alias ──
    for col in columns:
        col_low = col.lower().strip()
        if col_low in KNOWN_ALIASES:
            target = KNOWN_ALIASES[col_low]
            if target == "_drop":
                to_drop.append(col)
                if verbose:
                    print(f"  [alias→drop]    {col}")
            elif target not in assigned_targets:
                mapping[col] = target
                assigned_targets.add(target)
                if verbose:
                    print(f"  [alias→map]     {col} → {target}")
            else:
                # Target ya asignado, duplicado → drop
                to_drop.append(col)
                if verbose:
                    print(f"  [alias→dup]     {col} → {target} (ya asignado, drop)")

    remaining = [c for c in columns if c not in mapping and c not in to_drop]

    # ── Paso 2: coincidencia exacta con nombre destino (ya normalizado) ──
    target_names = list(MAPPABLE_FIELDS.keys())
    for col in list(remaining):
        col_norm = col.lower().replace(" ", "_").replace("-", "_")
        if col_norm in MAPPABLE_FIELDS and col_norm not in assigned_targets:
            mapping[col] = col_norm
            assigned_targets.add(col_norm)
            remaining.remove(col)
            if verbose:
                print(f"  [exact→map]     {col} → {col_norm}")

    # ── Paso 3: coincidencia difusa por nombre ──
    for col in list(remaining):
        col_norm = col.lower().replace("_", " ").replace("-", " ")
        candidates = [t for t in target_names if t not in assigned_targets]
        candidate_labels = [t.replace("_", " ") for t in candidates]
        matches = difflib.get_close_matches(col_norm, candidate_labels, n=1, cutoff=0.6)
        if matches:
            idx = candidate_labels.index(matches[0])
            target = candidates[idx]
            mapping[col] = target
            assigned_targets.add(target)
            remaining.remove(col)
            if verbose:
                print(f"  [fuzzy→map]     {col} → {target}  (score≈{difflib.SequenceMatcher(None, col_norm, matches[0]).ratio():.2f})")

    # ── Paso 4: heurísticas por contenido ──
    for col in list(remaining):
        info = _analyze_column_content(gdf[col])
        analysis[col] = info
        guess = _guess_by_content(col, info)
        if guess and guess not in assigned_targets:
            mapping[col] = guess
            assigned_targets.add(guess)
            remaining.remove(col)
            if verbose:
                print(f"  [content→map]   {col} → {guess}")
        elif guess and guess in assigned_targets:
            to_drop.append(col)
            remaining.remove(col)
            if verbose:
                print(f"  [content→dup]   {col} → {guess} (ya asignado, drop)")

    # Las que sobran → unmapped
    unmapped = remaining

    return {
        "column_mapping": mapping,
        "columns_to_drop": to_drop,
        "unmapped": unmapped,
        "assigned_targets": assigned_targets,
        "analysis": analysis,
    }


# ──────────────────────────────────────────────
# Detección de limpieza numérica
# ──────────────────────────────────────────────

def _detect_numeric_cleaning(gdf: gpd.GeoDataFrame, mapping: Dict[str, str]) -> Dict:
    """Detecta reglas de limpieza numérica para columnas mapeadas."""
    rules = {}
    for src_col, target in mapping.items():
        if target not in ("presupuesto_base", "cantidad", "avance_obra"):
            continue
        if src_col not in gdf.columns:
            continue
        sample = gdf[src_col].dropna().astype(str).head(50)
        if len(sample) == 0:
            continue

        strip_chars = []
        needs_comma_dot = False

        if sample.str.contains(r'\$', regex=True).any():
            strip_chars.append("$")
        # Patrón: "1.234.567,89" — puntos como miles, coma decimal
        if sample.str.contains(r'\d\.\d{3}', regex=True).any():
            strip_chars.append(".")
            needs_comma_dot = True
        elif sample.str.contains(",", regex=False).any():
            needs_comma_dot = True
        if sample.str.contains("%", regex=False).any():
            strip_chars.append("%")

        rules[target] = {"strip": strip_chars, "comma_to_dot": needs_comma_dot}

    return rules


# ──────────────────────────────────────────────
# Detección de CRS y forzados
# ──────────────────────────────────────────────

def _detect_needs_crs_transform(gdf: gpd.GeoDataFrame) -> bool:
    """Detecta si el shapefile necesita reproyección a EPSG:4326."""
    if gdf.crs is None:
        return False
    crs_str = str(gdf.crs).lower()
    return "4326" not in crs_str


def _detect_force_values(gdf: gpd.GeoDataFrame, mapping: Dict[str, str]) -> Dict:
    """Detecta si hay campos con valor único que deberían forzarse."""
    forces = {}
    # Estado: si todos son iguales (ej: FINALIZADO → Terminado)
    for src, target in mapping.items():
        if target == "estado" and src in gdf.columns:
            vals = gdf[src].dropna().unique()
            if len(vals) == 1:
                raw = str(vals[0]).strip().lower()
                if raw in ("finalizado", "terminado"):
                    forces["estado"] = "Terminado"

    # Avance: detectar si el avance está en escala 0-1 en lugar de 0-100
    for src, target in mapping.items():
        if target == "avance_obra" and src in gdf.columns:
            numeric = pd.to_numeric(gdf[src].dropna(), errors="coerce")
            if numeric.notna().any():
                mx = numeric.max()
                # Todos iguales a 1 → probablemente 0/1 flag, forzar 100
                if mx <= 1.0 and numeric.nunique() == 1 and mx == 1.0:
                    forces["avance_obra"] = 100
    return forces


def _detect_replacements(gdf: gpd.GeoDataFrame, mapping: Dict[str, str]) -> Dict:
    """Detecta reemplazos comunes necesarios."""
    replacements: Dict[str, Dict[str, str]] = {}
    for src, target in mapping.items():
        if target != "nombre_up" or src not in gdf.columns:
            continue
        vals = gdf[src].dropna().astype(str)
        # "No Reportado" → "Vía Local"
        if vals.str.contains("No Reportado", case=False).any():
            replacements.setdefault(target, {})["No Reportado"] = "Vía Local"
        # "Via" sin tilde → "Vía"
        if vals.str.contains(r'\bVia\b', case=True).any():
            replacements.setdefault(target, {})["Via"] = "Vía"
        # \r\n residual
        if vals.str.contains(r'\r\n', regex=True).any():
            replacements.setdefault(target, {})["\r\n"] = ""

    # tipo_intervencion con \r\n
    for src, target in mapping.items():
        if target != "tipo_intervencion" or src not in gdf.columns:
            continue
        vals = gdf[src].dropna().astype(str)
        if vals.str.contains(r'\r\n', regex=True).any():
            replacements.setdefault(target, {})["\r\n"] = ""

    return replacements


# ──────────────────────────────────────────────
# Generación del config JSON
# ──────────────────────────────────────────────

def generate_config(
    shapefile_path: str,
    config_name: Optional[str] = None,
    verbose: bool = False,
) -> Dict:
    """
    Lee un shapefile y genera automáticamente un dict de configuración
    compatible con data_up_infraestructura.py.

    Returns:
        dict con toda la configuración lista para guardar como JSON.
    """
    abs_path = Path(shapefile_path).resolve()
    if not abs_path.exists():
        raise FileNotFoundError(f"Shapefile no encontrado: {abs_path}")

    # Ruta relativa al proyecto
    try:
        rel_path = abs_path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        rel_path = str(abs_path)

    print(f"\n{'='*70}")
    print(f"AUTO-CONFIG: {abs_path.name}")
    print(f"{'='*70}")

    gdf = gpd.read_file(abs_path)
    print(f"  Registros: {len(gdf)}")
    print(f"  Columnas:  {len(gdf.columns)}")
    print(f"  CRS:       {gdf.crs}")
    print(f"  Geom type: {gdf.geom_type.unique().tolist()}")
    print(f"  Columnas:  {list(gdf.columns)}")

    # ── Detección de mapeos ──
    print(f"\n── Detección de mapeos ──")
    result = detect_mappings(gdf, verbose=verbose)
    col_mapping = result["column_mapping"]
    cols_drop = result["columns_to_drop"]
    unmapped = result["unmapped"]
    assigned = result["assigned_targets"]

    # ── Campos faltantes → defaults ──
    missing_targets = set(MAPPABLE_FIELDS.keys()) - assigned
    default_values = dict(FIXED_DEFAULTS)
    for field in NULL_DEFAULTS:
        if field not in assigned:
            default_values[field] = None
    for field in missing_targets:
        if field not in default_values:
            default_values[field] = None

    # Si no hay año mapeado, intentar extraerlo del nombre del archivo
    if "anio" not in assigned:
        import re as _re
        year_match = _re.search(r'20[12]\d', abs_path.stem)
        if year_match:
            default_values["anio"] = int(year_match.group())
            print(f"  [auto-anio]     Año deducido del nombre: {default_values['anio']}")
        else:
            default_values["anio"] = None

    # ── Detecciones automáticas ──
    numeric_cleaning = _detect_numeric_cleaning(gdf, col_mapping)
    needs_crs = _detect_needs_crs_transform(gdf)
    force_values = _detect_force_values(gdf, col_mapping)
    replacements = _detect_replacements(gdf, col_mapping)

    # ── fillna ──
    fillna = {}
    if "tipo_intervencion" in assigned:
        for src, tgt in col_mapping.items():
            if tgt == "tipo_intervencion" and src in gdf.columns:
                if gdf[src].isna().any():
                    fillna["tipo_intervencion"] = "Pavimento"
                break

    # ── Text capitalize columns ──
    text_cols = [t for t in [
        "tipo_intervencion", "nombre_up", "nombre_up_detalle",
        "identificador", "estado", "direccion",
    ] if t in assigned]

    # ── anio_field ──
    anio_field = None
    if "anio" in assigned:
        anio_field = "anio"

    # ── Construir config ──
    config: Dict[str, Any] = {"shapefile_path": rel_path}
    if cols_drop:
        config["columns_to_drop"] = cols_drop
    config["column_mapping"] = col_mapping
    config["default_values"] = default_values
    if text_cols:
        config["text_capitalize_columns"] = text_cols
    if replacements:
        config["replacements"] = replacements
    if numeric_cleaning:
        config["numeric_cleaning"] = numeric_cleaning
    if force_values:
        config["force_values"] = force_values
    if fillna:
        config["fillna_values"] = fillna
    if anio_field:
        config["anio_field"] = anio_field
    config["needs_crs_transform"] = needs_crs

    # ── Reporte ──
    _print_report(col_mapping, cols_drop, unmapped, missing_targets,
                  default_values, force_values, numeric_cleaning,
                  needs_crs, gdf, replacements)

    return config


def _print_report(
    col_mapping, cols_drop, unmapped, missing_targets,
    default_values, force_values, numeric_cleaning,
    needs_crs, gdf, replacements,
):
    """Imprime un reporte detallado de la detección."""
    print(f"\n── Resultado ──")
    print(f"  Columnas mapeadas:      {len(col_mapping)}")
    print(f"  Columnas a eliminar:    {len(cols_drop)}")
    print(f"  Columnas sin resolver:  {len(unmapped)}")

    if col_mapping:
        print(f"\n  Mapeos detectados:")
        for src, tgt in sorted(col_mapping.items(), key=lambda x: x[1]):
            sample = ""
            if src in gdf.columns:
                s = gdf[src].dropna().head(2).tolist()
                sample = f"  (ej: {s})" if s else ""
            print(f"    {src:25s} → {tgt}{sample}")

    if cols_drop:
        print(f"\n  Columnas a eliminar: {cols_drop}")

    if unmapped:
        print(f"\n  ⚠ Columnas SIN RESOLVER (requieren revisión manual):")
        for col in unmapped:
            sample = gdf[col].dropna().head(3).tolist() if col in gdf.columns else []
            print(f"    {col:25s}  {sample}")

    if missing_targets:
        print(f"\n  Campos del esquema sin mapear (irán como default/null):")
        for t in sorted(missing_targets):
            val = default_values.get(t, "null")
            print(f"    {t:25s} → default: {val}")

    if force_values:
        print(f"\n  Valores forzados: {force_values}")
    if numeric_cleaning:
        print(f"\n  Limpieza numérica: {list(numeric_cleaning.keys())}")
    if replacements:
        print(f"\n  Reemplazos de texto: {list(replacements.keys())}")
    if needs_crs:
        print(f"\n  CRS: necesita reproyección a EPSG:4326")


def save_config(config: Dict, name: str) -> Path:
    """Guarda el config como JSON en capas_config/."""
    CAPAS_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CAPAS_CONFIG_DIR / f"{name}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"\n  ✓ Config guardado en: {out_path}")
    return out_path


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Auto-detecta el mapeo de columnas de un shapefile al esquema de unidades de proyecto.",
    )
    parser.add_argument("shapefile", help="Ruta al archivo .shp")
    parser.add_argument("--nombre", "-n", help="Nombre para la config (sin .json). Si no se da, se deduce del archivo.")
    parser.add_argument("--guardar", "-g", action="store_true", help="Guardar el JSON en pipelines/capas_config/")
    parser.add_argument("--verbose", "-v", action="store_true", help="Mostrar detalle de cada paso de detección")

    args = parser.parse_args()

    config = generate_config(args.shapefile, verbose=args.verbose)

    name = args.nombre or Path(args.shapefile).stem.lower().replace(" ", "_")

    if args.guardar:
        save_config(config, name)
    else:
        print(f"\n── Config JSON generado (--guardar para persistir como '{name}.json') ──")
        print(json.dumps(config, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
