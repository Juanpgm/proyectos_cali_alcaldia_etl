# -*- coding: utf-8 -*-
"""
Test de validación estructural — data_up_infraestructura.py

Ejecuta la transformación completa SIN subir nada a Firebase y verifica que
los DataFrames resultantes (df_up, df_int) tengan exactamente la misma
estructura que producía el notebook ``infraestructura_up_notebook_test.ipynb``.

Uso:
    python pipelines/test_estructura_infraestructura.py
"""

import os
import sys
import json
from pathlib import Path

import pandas as pd
import geopandas as gpd

# ── Paths ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.data_up_infraestructura import (
    load_layer_config,
    read_and_transform_shapefile,
    _run_line_spatial_joins,
    _build_up_dataframe,
    _build_int_dataframe,
    _derive_estado,
    normalize_geometry,
    build_fingerprint,
    _capitalizar_texto,
)

# ════════════════════════════════════════════════
# Columnas esperadas — definidas por el notebook
# ════════════════════════════════════════════════

# Columnas de gdf_combined después de la transformación y antes de asignar UPID
# (celda 24 / 47 del notebook)
EXPECTED_GDF_COMBINED_COLUMNS = {
    "referencia_proceso", "referencia_contrato", "bpin", "bp", "identificador",
    "tipo_equipamiento", "fuente_financiacion", "nombre_up", "nombre_up_detalle",
    "comuna_corregimiento", "barrio_vereda", "tipo_intervencion", "unidad",
    "cantidad", "direccion", "estado", "presupuesto_base", "avance_obra",
    "anio", "fecha_inicio", "fecha_fin", "fecha_inauguracion", "plataforma",
    "url_proceso", "clase_up", "descripcion_intervencion", "observaciones",
    "nombre_centro_gestor", "geometry",
}

# Columnas de df_up  (celda 57 + 60 + 61 + 63)
EXPECTED_UP_COLUMNS = {
    "upid", "nombre_up", "nombre_up_detalle", "direccion",
    "tipo_equipamiento", "comuna_corregimiento", "barrio_vereda",
    "geometry", "proyectos_estrategicos",
}

# Columnas de df_int  (celda 67 + 71)
EXPECTED_INT_COLUMNS = {
    "upid", "intervencion_id", "referencia_proceso", "referencia_contrato",
    "bpin", "identificador", "fuente_financiacion", "tipo_intervencion",
    "unidad", "cantidad", "estado", "presupuesto_base", "avance_obra",
    "fecha_inicio", "fecha_fin", "fecha_inauguracion", "url_proceso",
    "descripcion_intervencion", "observaciones", "nombre_centro_gestor",
    "clase_up",
}

# Valores por defecto que deben estar presentes en TODOS los registros
EXPECTED_DEFAULTS = {
    "unidad": "m",
    "tipo_equipamiento": "Vías",
    "clase_up": "Obra vial",
    "nombre_centro_gestor": "Secretaría de Infraestructura",
}

# Estados válidos tras derivación (celda 71)
VALID_ESTADOS = {
    "En alistamiento", "En ejecución", "Terminado",
    "Suspendido", "Proyectado", "Inaugurado", None,
}

# ════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════
_pass = 0
_fail = 0
_warn = 0


def _check(condition: bool, msg_ok: str, msg_fail: str):
    global _pass, _fail
    if condition:
        _pass += 1
        print(f"  ✓ {msg_ok}")
    else:
        _fail += 1
        print(f"  ✗ {msg_fail}")


def _warn_check(condition: bool, msg_ok: str, msg_warn: str):
    global _pass, _warn
    if condition:
        _pass += 1
        print(f"  ✓ {msg_ok}")
    else:
        _warn += 1
        print(f"  ⚠ {msg_warn}")


# ════════════════════════════════════════════════
# Tests
# ════════════════════════════════════════════════
CONFIG_NAMES = [
    "infraestructura_2024",
    "infraestructura_2025",
    "gobierno_2025",
]


def test_load_configs():
    """Verifica que los JSON de configuración se cargan sin error."""
    print("\n── 1. Carga de configuraciones ──")
    configs = []
    for name in CONFIG_NAMES:
        cfg = load_layer_config(name)
        _check(cfg is not None, f"Config '{name}' cargada", f"Config '{name}' falló")
        _check("_resolved_shapefile" in cfg, f"Shapefile '{name}' resuelto", f"Shapefile '{name}' no resuelto")
        configs.append(cfg)
    return configs


def test_transform_shapefile(cfg, label):
    """Verifica la transformación de un shapefile individual."""
    print(f"\n── 2{label}. Transformación shapefile {cfg['_config_name']} ──")
    gdf = read_and_transform_shapefile(cfg, verbose=True)

    _check(len(gdf) > 0, f"{len(gdf)} registros leídos", "0 registros — shapefile vacío")
    _check(gdf.crs is not None, f"CRS: {gdf.crs}", "CRS ausente")
    _check(str(gdf.crs).endswith("4326") if gdf.crs else False,
           "CRS es EPSG:4326", f"CRS no es 4326: {gdf.crs}")

    # Validar valores por defecto
    for col, expected in EXPECTED_DEFAULTS.items():
        if col in gdf.columns:
            actual_vals = gdf[col].dropna().unique()
            _check(len(actual_vals) == 1 and actual_vals[0] == expected,
                   f"'{col}' = '{expected}'",
                   f"'{col}' tiene valores inesperados: {actual_vals[:5]}")

    # nombre_up no debe tener 'No Reportado' ni 'Via' sin tilde
    if "nombre_up" in gdf.columns:
        has_no_reportado = (gdf["nombre_up"] == "No Reportado").any()
        _check(not has_no_reportado,
               "nombre_up: 'No Reportado' reemplazado",
               "nombre_up: todavía contiene 'No Reportado'")
        has_via_sin_tilde = gdf["nombre_up"].str.contains(r"\bVia\b", case=True, na=False).any()
        _check(not has_via_sin_tilde,
               "nombre_up: 'Via' → 'Vía' correcto",
               "nombre_up: todavía contiene 'Via' sin tilde")

    # tipo_intervencion no debe tener nulos (fillna='Pavimento')
    if "tipo_intervencion" in gdf.columns:
        n_null = gdf["tipo_intervencion"].isna().sum()
        _check(n_null == 0,
               "tipo_intervencion: sin nulos (fillna='Pavimento')",
               f"tipo_intervencion: {n_null} nulos encontrados")

    # Capitalización: Title Case
    for col in ["nombre_up", "nombre_up_detalle", "direccion", "estado", "identificador"]:
        if col in gdf.columns:
            sample = gdf[col].dropna().head(20)
            all_title = all(v == v.title() or v == _capitalizar_texto(v)
                           for v in sample if isinstance(v, str))
            _check(all_title, f"'{col}' en Title Case", f"'{col}' — falla Title Case")

    # Numéricos
    for col in ["presupuesto_base", "cantidad", "avance_obra"]:
        if col in gdf.columns:
            _check(pd.api.types.is_float_dtype(gdf[col]) or pd.api.types.is_numeric_dtype(gdf[col]),
                   f"'{col}' es numérico", f"'{col}' NO es numérico: {gdf[col].dtype}")

    # anio debe ser entero
    if "anio" in gdf.columns:
        _check(pd.api.types.is_integer_dtype(gdf["anio"]),
               f"'anio' es entero ({gdf['anio'].dtype})",
               f"'anio' NO es entero: {gdf['anio'].dtype}")

    # estado forzado a 'Terminado'
    if "estado" in gdf.columns:
        _check((gdf["estado"] == "Terminado").all(),
               "estado forzado a 'Terminado'",
               f"estado tiene valores != 'Terminado': {gdf['estado'].unique()}")

    return gdf


def test_line_spatial_joins(gdf, label):
    """Verifica los spatial joins de línea (intersects)."""
    print(f"\n── 3{label}. Spatial joins de línea ──")
    gdf = _run_line_spatial_joins(gdf, verbose=True)

    _check("barrio_vereda" in gdf.columns, "barrio_vereda presente", "barrio_vereda FALTA")
    _check("comuna_corregimiento" in gdf.columns, "comuna_corregimiento presente", "comuna_corregimiento FALTA")

    n_barrios = gdf["barrio_vereda"].notna().sum()
    n_comunas = gdf["comuna_corregimiento"].notna().sum()
    _warn_check(n_barrios > 0,
                f"{n_barrios}/{len(gdf)} con barrio_vereda",
                f"0 barrios asignados — revisar basemaps")
    _warn_check(n_comunas > 0,
                f"{n_comunas}/{len(gdf)} con comuna_corregimiento",
                f"0 comunas asignadas — revisar basemaps")

    return gdf


def test_combined_columns(gdf_combined):
    """Verifica que gdf_combined tenga las mismas columnas que el notebook (celda 24/47)."""
    print(f"\n── 4. Columnas de gdf_combined ──")
    actual = set(gdf_combined.columns)
    # Excluir columnas internas del módulo que no estaban en el notebook
    internal = {"upid", "_action"}
    actual_clean = actual - internal

    missing = EXPECTED_GDF_COMBINED_COLUMNS - actual_clean
    extra = actual_clean - EXPECTED_GDF_COMBINED_COLUMNS

    _check(len(missing) == 0,
           f"Todas las columnas esperadas presentes ({len(EXPECTED_GDF_COMBINED_COLUMNS)})",
           f"Faltan columnas: {missing}")
    _warn_check(len(extra) == 0,
                "Sin columnas extra",
                f"Columnas extra (no en notebook): {extra}")

    print(f"  → Total columnas: {len(actual)}")


def test_up_structure(df_up):
    """Verifica la estructura exacta de df_up (unidades_proyecto)."""
    print(f"\n── 5. Estructura df_up (unidades_proyecto) ──")
    actual = set(df_up.columns) - {"_action"}

    missing = EXPECTED_UP_COLUMNS - actual
    extra = actual - EXPECTED_UP_COLUMNS

    _check(len(missing) == 0,
           f"Todas las columnas UP presentes ({len(EXPECTED_UP_COLUMNS)})",
           f"Faltan columnas UP: {missing}")
    _warn_check(len(extra) == 0,
                "Sin columnas extra en UP",
                f"Columnas extra en UP: {extra}")

    _check(len(df_up) > 0, f"{len(df_up)} unidades de proyecto", "0 UPs generadas")

    # upid debe tener formato UNP-{N}
    if "upid" in df_up.columns:
        sample = df_up["upid"].head(5)
        all_valid = all(str(v).startswith("UNP-") for v in sample)
        _check(all_valid,
               f"upid formato correcto: {sample.iloc[0]} ... {sample.iloc[-1]}",
               f"upid formato incorrecto: {sample.tolist()}")

        # No duplicados
        n_dup = df_up["upid"].duplicated().sum()
        _check(n_dup == 0, "upid sin duplicados", f"upid tiene {n_dup} duplicados")

    # geometry debe ser dict con type y coordinates
    if "geometry" in df_up.columns:
        geom_sample = df_up["geometry"].dropna().head(3)
        if len(geom_sample) > 0:
            g = geom_sample.iloc[0]
            _check(isinstance(g, dict),
                   "geometry es dict (GeoJSON __geo_interface__)",
                   f"geometry tipo inesperado: {type(g)}")
            if isinstance(g, dict):
                _check("type" in g and "coordinates" in g,
                       f"geometry tiene type='{g.get('type')}' y coordinates",
                       f"geometry carece de type/coordinates: {list(g.keys())}")

    # proyectos_estrategicos presente
    _check("proyectos_estrategicos" in df_up.columns,
           "proyectos_estrategicos presente",
           "proyectos_estrategicos FALTA")

    # comuna_corregimiento y barrio_vereda (from Point-within)
    if "comuna_corregimiento" in df_up.columns:
        n = df_up["comuna_corregimiento"].notna().sum()
        _warn_check(n > 0,
                    f"UP: {n}/{len(df_up)} con comuna (within point)",
                    "UP: 0 comunas asignadas con within")

    if "barrio_vereda" in df_up.columns:
        n = df_up["barrio_vereda"].notna().sum()
        _warn_check(n > 0,
                    f"UP: {n}/{len(df_up)} con barrio (within point)",
                    "UP: 0 barrios asignados con within")


def test_int_structure(df_int, n_combined):
    """Verifica la estructura exacta de df_int (intervenciones)."""
    print(f"\n── 6. Estructura df_int (intervenciones) ──")
    actual = set(df_int.columns) - {"_action"}

    missing = EXPECTED_INT_COLUMNS - actual
    extra = actual - EXPECTED_INT_COLUMNS

    _check(len(missing) == 0,
           f"Todas las columnas INT presentes ({len(EXPECTED_INT_COLUMNS)})",
           f"Faltan columnas INT: {missing}")
    _warn_check(len(extra) == 0,
                "Sin columnas extra en INT",
                f"Columnas extra en INT: {extra}")

    _check(len(df_int) == n_combined,
           f"{len(df_int)} intervenciones (= {n_combined} registros combinados)",
           f"Diferencia: {len(df_int)} INT vs {n_combined} combinados")

    # intervencion_id formato: UNP-{N}-INT-{M}
    if "intervencion_id" in df_int.columns:
        sample = df_int["intervencion_id"].head(5)
        all_valid = all("-INT-" in str(v) for v in sample)
        _check(all_valid,
               f"intervencion_id formato correcto: {sample.iloc[0]}",
               f"intervencion_id formato incorrecto: {sample.tolist()}")

    # estado derivado correcto
    if "estado" in df_int.columns:
        unique_estados = set(df_int["estado"].dropna().unique())
        invalid = unique_estados - VALID_ESTADOS
        _check(len(invalid) == 0,
               f"Estados válidos: {unique_estados}",
               f"Estados inválidos: {invalid}")

    # Verificar derivación de estado
    if "avance_obra" in df_int.columns and "estado" in df_int.columns:
        zero_avance = df_int[df_int["avance_obra"] == 0]
        if len(zero_avance) > 0:
            estados_zero = set(zero_avance["estado"].dropna().unique())
            _check(estados_zero <= {"En alistamiento", "Proyectado"},
                   f"avance=0 → estado correcto: {estados_zero}",
                   f"avance=0 → estado incorrecto: {estados_zero}")

        full_avance = df_int[df_int["avance_obra"] == 100]
        if len(full_avance) > 0:
            estados_full = set(full_avance["estado"].dropna().unique())
            _check(estados_full <= {"Terminado", "Inaugurado"},
                   f"avance=100 → estado correcto: {estados_full}",
                   f"avance=100 → estado incorrecto: {estados_full}")

    # presupuesto_base numérico
    if "presupuesto_base" in df_int.columns:
        _check(pd.api.types.is_numeric_dtype(df_int["presupuesto_base"]),
               f"presupuesto_base numérico ({df_int['presupuesto_base'].dtype})",
               f"presupuesto_base NO numérico: {df_int['presupuesto_base'].dtype}")

    # nombre_centro_gestor siempre 'Secretaría de Infraestructura'
    if "nombre_centro_gestor" in df_int.columns:
        vals = df_int["nombre_centro_gestor"].dropna().unique()
        _check(len(vals) == 1 and vals[0] == "Secretaría de Infraestructura",
               "nombre_centro_gestor constante",
               f"nombre_centro_gestor inesperado: {vals}")


def test_normalize_geometry():
    """Verifica la normalización de geometría para Firestore."""
    print(f"\n── 7. Normalización de geometría ──")

    # LineString simple
    geom = {"type": "LineString", "coordinates": [[-76.5, 3.4, 0], [-76.6, 3.5, 0]]}
    result = normalize_geometry(geom)
    _check(result is not None, "LineString normalizado", "LineString falló")
    if result:
        _check(result["type"] == "LineString", "type=LineString", f"type={result['type']}")
        _check(isinstance(result["coordinates"], str), "coordinates es string JSON", "coordinates no es string")
        coords = json.loads(result["coordinates"])
        _check(len(coords[0]) == 2, "Z eliminado (solo lon, lat)", f"Coordenada: {coords[0]}")

    # None
    _check(normalize_geometry(None) is None, "None → None", "None no retornó None")


def test_fingerprint_consistency():
    """Verifica que fingerprints sean determinísticos."""
    print(f"\n── 8. Consistencia de fingerprints ──")
    row1 = pd.Series({"nombre_up": "Vía Local", "direccion": "Calle 5", "bpin": "12345", "geometry": None})
    row2 = pd.Series({"nombre_up": "Vía Local", "direccion": "Calle 5", "bpin": "12345", "geometry": None})
    row3 = pd.Series({"nombre_up": "Vía Arterial", "direccion": "Calle 5", "bpin": "12345", "geometry": None})

    fp1 = build_fingerprint(row1)
    fp2 = build_fingerprint(row2)
    fp3 = build_fingerprint(row3)

    _check(fp1 == fp2, "Mismos datos → mismo fingerprint", "Fingerprints distintos para mismos datos")
    _check(fp1 != fp3, "Datos distintos → fingerprint distinto", "Fingerprints iguales para datos distintos")


def test_estado_derivation():
    """Verifica la lógica de derivación de estado (celda 71 del notebook)."""
    print(f"\n── 9. Derivación de estado ──")
    cases = [
        ({"avance_obra": 0, "estado": "Otro"}, "En alistamiento"),
        ({"avance_obra": 0, "estado": "Proyectado"}, "Proyectado"),        # excepción
        ({"avance_obra": 50, "estado": "Otro"}, "En ejecución"),
        ({"avance_obra": 50, "estado": "Suspendido"}, "Suspendido"),       # excepción
        ({"avance_obra": 100, "estado": "Otro"}, "Terminado"),
        ({"avance_obra": 100, "estado": "Inaugurado"}, "Inaugurado"),      # excepción
    ]
    for data, expected in cases:
        result = _derive_estado(pd.Series(data))
        _check(result == expected,
               f"avance={data['avance_obra']}, estado='{data['estado']}' → '{result}'",
               f"avance={data['avance_obra']}, estado='{data['estado']}' → '{result}' (esperado '{expected}')")


# ════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════
def main():
    global _pass, _fail, _warn

    print("=" * 70)
    print("TEST ESTRUCTURAL — data_up_infraestructura.py")
    print("Valida que la salida sea idéntica al notebook original")
    print("Sin conexión a Firebase — sin subir datos")
    print("=" * 70)

    # ── 1. Configs ──
    configs = test_load_configs()

    # ── 2 & 3. Transformación + Spatial joins por capa ──
    gdfs = []
    for i, cfg in enumerate(configs):
        label = chr(ord("a") + i)  # a, b, c ...
        gdf = test_transform_shapefile(cfg, label)
        gdf = test_line_spatial_joins(gdf, label)
        gdfs.append(gdf)

    # ── 4. Concatenar ──
    gdf_combined = pd.concat(gdfs, ignore_index=True)
    if not isinstance(gdf_combined, gpd.GeoDataFrame):
        gdf_combined = gpd.GeoDataFrame(gdf_combined, geometry="geometry")
    if gdf_combined.crs is None:
        gdf_combined = gdf_combined.set_crs(epsg=4326)

    total_rows = sum(len(g) for g in gdfs)
    print(f"\n── Concatenación ──")
    parts = " + ".join(str(len(g)) for g in gdfs)
    _check(len(gdf_combined) == total_rows,
           f"Combinado: {len(gdf_combined)} = {parts}",
           f"Combinado inconsistente: {len(gdf_combined)} ≠ {parts}")

    # Simular UPIDs (sin Firebase)
    gdf_combined["upid"] = [f"UNP-{i}" for i in range(1, len(gdf_combined) + 1)]
    gdf_combined["_action"] = "NEW"

    test_combined_columns(gdf_combined)

    # ── 5. Construir df_up ──
    df_up = _build_up_dataframe(gdf_combined, verbose=True)
    test_up_structure(df_up)

    # ── 6. Construir df_int ──
    df_int = _build_int_dataframe(gdf_combined, verbose=True)
    test_int_structure(df_int, len(gdf_combined))

    # ── 7-9. Tests unitarios ──
    test_normalize_geometry()
    test_fingerprint_consistency()
    test_estado_derivation()

    # ── Resumen ──
    total = _pass + _fail + _warn
    print(f"\n{'=' * 70}")
    print(f"RESUMEN: {_pass} pasaron, {_fail} fallaron, {_warn} advertencias  ({total} total)")
    if _fail == 0:
        print("✓ TODOS LOS TESTS PASARON — la estructura es idéntica al notebook")
    else:
        print(f"✗ {_fail} TEST(S) FALLARON — revisar diferencias arriba")
    print(f"{'=' * 70}")

    return _fail == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
