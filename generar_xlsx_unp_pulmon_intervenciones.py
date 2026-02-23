import json
import os
from datetime import datetime
from typing import Any, Dict, List, Set

import geopandas as gpd
import pandas as pd
from shapely.geometry import shape

from database.config import get_firestore_client


POLYGON_PATH = r"basemaps\pulmon_oriente\PoligonoPropuestoPulmonDeOriente.geojson"
OUTPUT_PATH = r"app_outputs\unidades_proyecto_pulmon_oriente_intervenciones_plano.xlsx"


def normalize_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text.upper()


def geometry_to_text(geom: Any) -> str:
    if geom is None:
        return ""
    try:
        return geom.wkt
    except Exception:
        return str(geom)


def flatten_dict(data: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for key, value in data.items():
        full_key = f"{prefix}{key}" if not prefix else f"{prefix}_{key}"
        if isinstance(value, dict):
            flat.update(flatten_dict(value, full_key))
        elif isinstance(value, list):
            flat[full_key] = json.dumps(value, ensure_ascii=False)
        else:
            flat[full_key] = value
    return flat


def sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()

    datetime_tz_cols = cleaned.select_dtypes(include=["datetimetz"]).columns
    for col in datetime_tz_cols:
        cleaned[col] = cleaned[col].dt.tz_localize(None)

    for col in cleaned.columns:
        if cleaned[col].dtype == "object":
            cleaned[col] = cleaned[col].apply(
                lambda x: x.replace(tzinfo=None) if hasattr(x, "tzinfo") and getattr(x, "tzinfo", None) is not None else x
            )

    return cleaned


def load_polygon(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf.set_crs("EPSG:4326", inplace=True)
    elif gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def load_unidades_proyecto(db) -> gpd.GeoDataFrame:
    docs = db.collection("unidades_proyecto").stream()
    rows: List[Dict[str, Any]] = []
    geometries: List[Any] = []

    for doc in docs:
        data = doc.to_dict() or {}
        data["doc_id"] = doc.id

        geom = None
        geom_data = data.get("geometry")
        try:
            if isinstance(geom_data, dict) and geom_data.get("type") and geom_data.get("coordinates"):
                geom = shape(geom_data)
            elif isinstance(geom_data, str) and geom_data.strip().startswith("{"):
                geom = shape(json.loads(geom_data))
        except Exception:
            geom = None

        if geom is None:
            lat = data.get("lat")
            lon = data.get("lon")
            if lat is not None and lon is not None:
                try:
                    geom = shape({"type": "Point", "coordinates": [float(lon), float(lat)]})
                except Exception:
                    geom = None

        geometries.append(geom)
        data.pop("geometry", None)
        rows.append(data)

    gdf = gpd.GeoDataFrame(pd.DataFrame(rows), geometry=geometries, crs="EPSG:4326")
    gdf = gdf[gdf.geometry.notna()].copy()
    return gdf


def find_intersection(gdf_unidades: gpd.GeoDataFrame, gdf_polygon: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    polygon = gdf_polygon.geometry.iloc[0]
    return gdf_unidades[gdf_unidades.geometry.intersects(polygon)].copy()


def build_unp_keys(unidad: Dict[str, Any]) -> Set[str]:
    keys = {
        normalize_value(unidad.get("upid")),
        normalize_value(unidad.get("doc_id")),
        normalize_value(unidad.get("identificador")),
        normalize_value(unidad.get("id")),
        normalize_value(unidad.get("unidad_proyecto_id")),
    }
    return {k for k in keys if k}


def extract_intervention_keys(record: Dict[str, Any]) -> Set[str]:
    keys: Set[str] = set()
    direct_fields = [
        "upid",
        "up_id",
        "unp",
        "doc_id_unidad",
        "unidad_proyecto_id",
        "unidad_id",
        "identificador",
        "doc_id",
    ]
    for field in direct_fields:
        keys.add(normalize_value(record.get(field)))

    for root in ["unidad_proyecto", "unidad", "unp_data"]:
        nested = record.get(root)
        if isinstance(nested, dict):
            keys.add(normalize_value(nested.get("upid")))
            keys.add(normalize_value(nested.get("doc_id")))
            keys.add(normalize_value(nested.get("identificador")))
            keys.add(normalize_value(nested.get("id")))

    intervencion_id = record.get("intervencion_id")
    if isinstance(intervencion_id, str):
        parts = intervencion_id.split("-")
        if len(parts) >= 3 and parts[0].upper() == "UNP":
            keys.add(normalize_value("-".join(parts[:-1])))

    return {k for k in keys if k}


def load_intervenciones_collection(db) -> List[Dict[str, Any]]:
    docs = list(db.collection("intervenciones_unidades_proyecto").stream())
    rows: List[Dict[str, Any]] = []
    for doc in docs:
        record = doc.to_dict() or {}
        record["doc_id_intervencion"] = doc.id
        rows.append(record)
    return rows


def load_intervenciones_from_unidades(gdf_unidades: gpd.GeoDataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _, row in gdf_unidades.iterrows():
        base = row.to_dict()
        intervenciones = base.get("intervenciones")
        if not isinstance(intervenciones, list):
            continue
        for item in intervenciones:
            if not isinstance(item, dict):
                continue
            record = dict(item)
            record["upid"] = base.get("upid")
            record["doc_id_unidad"] = base.get("doc_id")
            record["identificador"] = base.get("identificador")
            rows.append(record)
    return rows


def create_flat_table(gdf_intersected: gpd.GeoDataFrame, intervenciones_raw: List[Dict[str, Any]]) -> pd.DataFrame:
    indexed_intervenciones: List[Dict[str, Any]] = []
    for record in intervenciones_raw:
        indexed_intervenciones.append(
            {
                "keys": extract_intervention_keys(record),
                "flat": flatten_dict(record),
            }
        )

    rows: List[Dict[str, Any]] = []
    for _, unidad_row in gdf_intersected.iterrows():
        unidad = unidad_row.to_dict()
        unidad_keys = build_unp_keys(unidad)

        unidad_base = {
            "unp_doc_id": unidad.get("doc_id"),
            "unp_upid": unidad.get("upid"),
            "unp_identificador": unidad.get("identificador"),
            "unp_nombre": unidad.get("nombre_up"),
            "unp_nombre_detalle": unidad.get("nombre_up_detalle"),
            "unp_comuna_corregimiento": unidad.get("comuna_corregimiento"),
            "unp_barrio_vereda": unidad.get("barrio_vereda"),
            "unp_direccion": unidad.get("direccion"),
            "unp_estado": unidad.get("estado"),
            "unp_geometry_type": unidad.get("geometry_type"),
            "unp_geometry_wkt": geometry_to_text(unidad.get("geometry")),
        }

        matched = [item for item in indexed_intervenciones if unidad_keys.intersection(item["keys"])]

        if not matched:
            row = dict(unidad_base)
            row["interv_sin_match"] = True
            rows.append(row)
            continue

        for match in matched:
            row = dict(unidad_base)
            row["interv_sin_match"] = False
            for key, value in match["flat"].items():
                row[f"interv_{key}"] = value
            rows.append(row)

    return pd.DataFrame(rows)


def main() -> None:
    print("=" * 78)
    print("GENERAR XLSX: UNIDADES_PROYECTO ∩ PULMÓN ORIENTE + INTERVENCIONES")
    print("=" * 78)

    db = get_firestore_client()
    gdf_polygon = load_polygon(POLYGON_PATH)
    gdf_unidades = load_unidades_proyecto(db)
    gdf_intersected = find_intersection(gdf_unidades, gdf_polygon)

    print(f"UNPs con geometría en Firebase: {len(gdf_unidades)}")
    print(f"UNPs que intersectan Pulmón Oriente: {len(gdf_intersected)}")

    intervenciones_collection = load_intervenciones_collection(db)
    source = "intervenciones_unidades_proyecto"

    if not intervenciones_collection:
        intervenciones_collection = load_intervenciones_from_unidades(gdf_unidades)
        source = "unidades_proyecto.intervenciones (fallback)"

    print(f"Intervenciones cargadas desde: {source}")
    print(f"Total intervenciones cargadas: {len(intervenciones_collection)}")

    df_flat = create_flat_table(gdf_intersected, intervenciones_collection)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_flat = sanitize_for_excel(df_flat)
    df_flat.to_excel(OUTPUT_PATH, index=False, engine="openpyxl")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("-" * 78)
    print(f"Archivo generado: {OUTPUT_PATH}")
    print(f"Filas exportadas: {len(df_flat)}")
    print(f"Columnas exportadas: {len(df_flat.columns)}")
    print(f"Fecha/Hora: {timestamp}")


if __name__ == "__main__":
    main()
