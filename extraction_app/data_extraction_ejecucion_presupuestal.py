"""
Scraper de ejecución presupuestal - Alcaldía de Cali.

Objetivo:
- Acceder a la página de programas/proyectos de ejecución.
- Detectar documentos .xlsx agrupados por "Vigencia".
- Descargar todos los archivos al directorio app_inputs/ejecucion_presupuestal.

Tecnologías:
- Selenium (headless) para cargar el HTML final de la página.
- BeautifulSoup para parsear y extraer estructura/links.
"""

from __future__ import annotations

import os
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


TARGET_URL = "https://www.cali.gov.co/planeacion/publicaciones/115111/programas-proyectos-ejecucion-dapm/"
OUTPUT_DIR = Path("app_inputs") / "ejecucion_presupuestal"
REQUEST_TIMEOUT = 90
MIN_YEAR = 2020


@dataclass(frozen=True)
class ArchivoVigencia:
    vigencia: str
    nombre: str
    url: str


@dataclass
class SheetSelection:
    df: pd.DataFrame
    sheet_name: str
    header_row: int
    score: float


class EjecucionPresupuestalScraper:
    def __init__(self, target_url: str = TARGET_URL, output_dir: Path = OUTPUT_DIR):
        self.target_url = target_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "es-ES,es;q=0.9",
                "Referer": self.target_url,
            }
        )
        self.df_ejecucion_presupuestal = pd.DataFrame()

    @staticmethod
    def _extract_year_from_text(text: str) -> Optional[int]:
        match = re.search(r"(20\d{2})", text)
        if not match:
            return None
        return int(match.group(1))

    def _archivo_year(self, archivo: ArchivoVigencia) -> Optional[int]:
        year_from_vigencia = self._extract_year_from_text(archivo.vigencia)
        if year_from_vigencia is not None:
            return year_from_vigencia
        return self._extract_year_from_text(archivo.nombre)

    def _should_include_year(self, year_value: Optional[int]) -> bool:
        return year_value is not None and year_value >= MIN_YEAR

    def _resolve_output_file_path(self, archivo: ArchivoVigencia) -> Path:
        nombre_archivo = archivo.nombre
        if not nombre_archivo.lower().endswith(".xlsx"):
            nombre_archivo += ".xlsx"
        nombre_archivo = self._sanitize_filename(nombre_archivo)
        return self.output_dir / nombre_archivo

    def _build_driver(self) -> webdriver.Chrome:
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )

        return webdriver.Chrome(options=chrome_options)

    def _fetch_html_with_selenium(self) -> str:
        driver: Optional[webdriver.Chrome] = None
        try:
            print("🌐 Abriendo página con Selenium headless...")
            driver = self._build_driver()
            driver.get(self.target_url)

            WebDriverWait(driver, 45).until(
                EC.presence_of_element_located((By.ID, "accordionDocumentos"))
            )

            buttons = driver.find_elements(
                By.CSS_SELECTOR,
                "#accordionDocumentos .accordion-header button.accordion-button",
            )

            for button in buttons:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                    time.sleep(0.2)
                    if button.get_attribute("aria-expanded") == "false":
                        button.click()
                        time.sleep(0.35)
                except Exception:
                    # Si no se puede expandir alguno, continuamos con los demás.
                    continue

            html = driver.page_source
            print("✅ HTML capturado correctamente con Selenium.")
            return html
        finally:
            if driver:
                driver.quit()

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        cleaned = re.sub(r"\s+", " ", name).strip()
        cleaned = cleaned.replace("/", "-").replace("\\", "-")
        cleaned = re.sub(r"[<>:\"|?*]", "", cleaned)
        return cleaned

    @staticmethod
    def _normalize_text(value: str) -> str:
        text = unicodedata.normalize("NFD", value)
        text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
        text = re.sub(r"\s+", " ", text).strip().lower()
        return text

    def _infer_month(self, file_name: str) -> str:
        meses = [
            "enero",
            "febrero",
            "marzo",
            "abril",
            "mayo",
            "junio",
            "julio",
            "agosto",
            "septiembre",
            "octubre",
            "noviembre",
            "diciembre",
        ]
        normalized_name = self._normalize_text(file_name)
        for mes in meses:
            if mes in normalized_name:
                return mes.capitalize()
        return "No identificado"

    def _find_organismo_column(self, df: pd.DataFrame) -> Optional[str]:
        aliases = {
            self._normalize_text("Nombre del Organismo"),
            self._normalize_text("Nombre Organismo"),
            self._normalize_text("Organismo"),
            self._normalize_text("Nombre Centro Gestor"),
            self._normalize_text("Centro Gestor"),
            self._normalize_text("Nombre del Centro Gestor"),
            self._normalize_text("Entidad"),
            self._normalize_text("Nombre Entidad"),
        }
        for col in df.columns:
            if self._normalize_text(str(col)) in aliases:
                return col
        return None

    def _organismo_name_score(self, column_name: str) -> int:
        normalized = self._normalize_text(column_name)
        score_value = 0

        if "nombre" in normalized and "organismo" in normalized:
            score_value += 8
        if "nombre" in normalized and "centro" in normalized and "gestor" in normalized:
            score_value += 8
        if "organismo" in normalized:
            score_value += 4
        if "centro" in normalized and "gestor" in normalized:
            score_value += 4
        if "entidad" in normalized and "nombre" in normalized:
            score_value += 3
        if "entidad" in normalized:
            score_value += 1

        if "codigo" in normalized and "nombre" not in normalized:
            score_value -= 5
        if normalized.startswith("id"):
            score_value -= 3

        return score_value

    @staticmethod
    def _is_numeric_code_like(value: str) -> bool:
        text = value.strip()
        if not text:
            return False
        return bool(re.fullmatch(r"\d{6,16}", text))

    def _organismo_value_quality_score(self, series: pd.Series) -> float:
        non_null = series.dropna().astype(str).str.strip()
        non_null = non_null[non_null != ""]

        if non_null.empty:
            return -10.0

        sample = non_null.head(300)
        total = len(sample)
        if total == 0:
            return -10.0

        code_like = sample.apply(self._is_numeric_code_like).sum()
        long_text = sample.str.len().ge(8).sum()
        with_spaces = sample.str.contains(r"\s", regex=True).sum()

        return (long_text / total) * 3.0 + (with_spaces / total) * 3.0 - (code_like / total) * 4.0

    def _detect_organismo_column_variant(self, df: pd.DataFrame) -> Optional[str]:
        candidate_columns: List[str] = []
        for col in df.columns:
            normalized = self._normalize_text(str(col))
            if (
                "organ" in normalized
                or "organismo" in normalized
                or "entidad" in normalized
                or "centro gestor" in normalized
                or ("centro" in normalized and "gestor" in normalized)
                or "gestor" in normalized
            ):
                candidate_columns.append(str(col))

        if not candidate_columns:
            return None

        scored_candidates: List[tuple[float, str]] = []
        for col in candidate_columns:
            name_score = float(self._organismo_name_score(col))
            value_score = self._organismo_value_quality_score(df[col])
            scored_candidates.append((name_score + value_score, col))

        scored_candidates.sort(key=lambda item: item[0], reverse=True)
        best_score, best_col = scored_candidates[0]

        if best_score < 2.0:
            return None
        return best_col

    def _sheet_structure_score(self, df: pd.DataFrame) -> float:
        if df is None or df.empty:
            return -1000.0

        rows, cols = df.shape
        score = 0.0

        score += min(rows, 50000) / 5000.0
        score += min(cols, 120) / 30.0

        unnamed_count = sum(
            1 for col in df.columns if self._normalize_text(str(col)).startswith("unnamed")
        )
        score -= unnamed_count * 0.8

        target_tokens = [
            "organismo",
            "centro gestor",
            "proyecto",
            "presupuesto",
            "vigencia",
            "mes",
            "programa",
        ]

        normalized_cols = [self._normalize_text(str(col)) for col in df.columns]
        for token in target_tokens:
            if any(token in col for col in normalized_cols):
                score += 2.0

        organismo_col = self._find_organismo_column(df) or self._detect_organismo_column_variant(df)
        if organismo_col:
            score += 12.0

        dense_ratio = float(df.notna().mean().mean()) if rows > 0 and cols > 0 else 0.0
        score += dense_ratio * 2.0

        return score

    def _select_best_sheet(self, file_path: Path) -> Optional[SheetSelection]:
        try:
            excel_file = pd.ExcelFile(file_path)
        except Exception as exc:
            print(f"   ❌ No se pudo abrir libro {file_path.name}: {exc}")
            return None

        best_selection: Optional[SheetSelection] = None
        header_candidates = range(0, 8)

        for sheet_name in excel_file.sheet_names:
            for header_row in header_candidates:
                try:
                    candidate_df = pd.read_excel(
                        file_path,
                        sheet_name=sheet_name,
                        header=header_row,
                        nrows=250,
                    )
                except Exception:
                    continue

                if candidate_df is None or candidate_df.empty:
                    continue

                score = self._sheet_structure_score(candidate_df)

                if best_selection is None or score > best_selection.score:
                    best_selection = SheetSelection(
                        df=candidate_df,
                        sheet_name=str(sheet_name),
                        header_row=int(header_row),
                        score=float(score),
                    )

        if best_selection is None:
            return None

        try:
            full_df = pd.read_excel(
                file_path,
                sheet_name=best_selection.sheet_name,
                header=best_selection.header_row,
            )
        except Exception as exc:
            print(f"   ❌ Error cargando hoja seleccionada en {file_path.name}: {exc}")
            return None

        best_selection.df = full_df
        return best_selection

    def _build_consolidated_dataframe(self) -> pd.DataFrame:
        xlsx_files = sorted(self.output_dir.glob("*.xlsx"))
        xlsx_files = [
            file_path
            for file_path in xlsx_files
            if self._should_include_year(self._extract_year_from_text(file_path.name))
        ]

        if not xlsx_files:
            print(
                f"⚠️ No hay archivos .xlsx (vigencia >= {MIN_YEAR}) "
                "en la carpeta de salida para consolidar."
            )
            return pd.DataFrame()

        dfs: List[pd.DataFrame] = []

        print(
            f"\n🧩 Construyendo DataFrame consolidado (vigencia >= {MIN_YEAR}) "
            "desde la hoja detectada de cada archivo..."
        )
        for file_path in xlsx_files:
            try:
                selected = self._select_best_sheet(file_path)
                if selected is None:
                    print(f"   ❌ No se encontró una hoja válida en: {file_path.name}")
                    continue

                df = selected.df.copy()
                if df is None or df.empty:
                    print(f"⚠️ Hoja vacía en: {file_path.name}")
                    continue

                organismo_col = self._find_organismo_column(df)
                if organismo_col is None:
                    organismo_col = self._detect_organismo_column_variant(df)
                    if organismo_col:
                        df = df.rename(columns={organismo_col: "Nombre del Organismo"})

                vigencia_match = re.search(r"(20\d{2})", file_path.name)
                vigencia_value = vigencia_match.group(1) if vigencia_match else "No identificado"
                mes_value = self._infer_month(file_path.name)

                df["_archivo_origen"] = file_path.name
                df["_mes_asociado"] = mes_value
                df["_vigencia_asociada"] = vigencia_value
                df["_sheet_origen"] = selected.sheet_name
                df["_header_row_detectado"] = selected.header_row

                dfs.append(df)
                print(
                    f"   ✅ {file_path.name}: {len(df):,} registros | "
                    f"mes={mes_value} | sheet='{selected.sheet_name}' | header={selected.header_row}"
                )
            except Exception as exc:
                print(f"   ❌ Error leyendo {file_path.name}: {exc}")

        if not dfs:
            print("⚠️ No fue posible construir el DataFrame consolidado.")
            return pd.DataFrame()

        consolidated = pd.concat(dfs, ignore_index=True)
        return consolidated

    def _print_dataframe_metrics(self, df_ejecucion_presupuestal: pd.DataFrame) -> None:
        if df_ejecucion_presupuestal.empty:
            print("⚠️ df_ejecucion_presupuestal está vacío.")
            return

        print("\n" + "=" * 70)
        print("Métricas de df_ejecucion_presupuestal")
        print("=" * 70)

        resumen_archivo = (
            df_ejecucion_presupuestal.groupby(["_archivo_origen", "_mes_asociado"], dropna=False)
            .size()
            .reset_index(name="cantidad_registros")
            .sort_values(by=["_archivo_origen", "_mes_asociado"])
        )

        print("\n📄 Cantidad de registros y mes asociado por archivo:")
        for _, row in resumen_archivo.iterrows():
            print(
                f"   - {row['_archivo_origen']} | "
                f"mes={row['_mes_asociado']} | registros={int(row['cantidad_registros']):,}"
            )

        total_registros = len(df_ejecucion_presupuestal)
        print(f"\n🔢 Cantidad total de registros: {total_registros:,}")

        organismo_column = "Nombre del Organismo"
        if organismo_column not in df_ejecucion_presupuestal.columns:
            print("⚠️ No se encontró la columna 'Nombre del Organismo' en el DataFrame consolidado.")
            return

        resumen_organismo = (
            df_ejecucion_presupuestal[organismo_column]
            .fillna("SIN DATO")
            .astype(str)
            .str.strip()
            .replace("", "SIN DATO")
            .value_counts(dropna=False)
            .rename_axis("Nombre del Organismo")
            .reset_index(name="cantidad_registros")
        )

        print("\n🏛️ Cantidad total por 'Nombre del Organismo':")
        for _, row in resumen_organismo.iterrows():
            print(f"   - {row['Nombre del Organismo']}: {int(row['cantidad_registros']):,}")

    def _extract_archivos(self, html: str) -> List[ArchivoVigencia]:
        soup = BeautifulSoup(html, "html.parser")
        accordion = soup.select_one("#accordionDocumentos")
        if not accordion:
            raise RuntimeError("No se encontró el acordeón de documentos en la página.")

        encontrados: List[ArchivoVigencia] = []
        dedupe: Set[str] = set()

        items = accordion.select(".accordion-item")

        for item in items:
            header_button = item.select_one(".accordion-header button")
            vigencia = (
                re.sub(r"\s+", " ", header_button.get_text(" ", strip=True))
                if header_button
                else "Sin vigencia"
            )

            for anchor in item.select("a[href]"):
                href = anchor.get("href", "").strip()
                if not href:
                    continue

                nombre = re.sub(r"\s+", " ", anchor.get_text(" ", strip=True))
                href_lower = href.lower()
                nombre_lower = nombre.lower()

                if ".xlsx" not in href_lower and ".xlsx" not in nombre_lower:
                    continue

                full_url = urljoin(self.target_url, href)
                key = f"{vigencia}|{full_url}|{nombre}"
                if key in dedupe:
                    continue

                dedupe.add(key)
                encontrados.append(
                    ArchivoVigencia(
                        vigencia=vigencia,
                        nombre=nombre if nombre else os.path.basename(full_url),
                        url=full_url,
                    )
                )

        return encontrados

    def _cookies_from_selenium_to_requests(self, driver_cookies: List[Dict[str, str]]) -> None:
        for cookie in driver_cookies:
            if not cookie.get("name"):
                continue
            self.session.cookies.set(cookie["name"], cookie.get("value", ""))

    def _download_archivo(self, archivo: ArchivoVigencia) -> Path:
        output_file = self._resolve_output_file_path(archivo)

        if output_file.exists() and output_file.stat().st_size > 0:
            print(f"⏭️  Ya existe en carpeta, se salta descarga: {output_file.name}")
            return output_file

        response = self.session.get(archivo.url, timeout=REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "").lower()
        if "html" in content_type and "excel" not in content_type:
            raise RuntimeError(
                f"Respuesta inesperada para {archivo.nombre}. "
                f"Parece HTML en vez de archivo Excel."
            )

        with open(output_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 64):
                if chunk:
                    f.write(chunk)

        if output_file.stat().st_size == 0:
            raise RuntimeError(f"Descarga vacía para: {archivo.nombre}")

        print(f"✅ Descargado: {output_file.name} ({output_file.stat().st_size / (1024 * 1024):.2f} MB)")
        return output_file

    def run(self) -> Dict[str, int]:
        total_start = time.perf_counter()
        html = self._fetch_html_with_selenium()

        archivos = self._extract_archivos(html)

        if not archivos:
            raise RuntimeError("No se detectaron archivos .xlsx para descargar.")

        print(f"📌 Archivos detectados: {len(archivos)}")

        conteo_por_vigencia: Dict[str, int] = {}
        descargados = 0
        errores = 0

        for archivo in archivos:
            conteo_por_vigencia[archivo.vigencia] = conteo_por_vigencia.get(archivo.vigencia, 0) + 1

        print("\n📊 Distribución por vigencia:")
        for vigencia, count in sorted(conteo_por_vigencia.items(), reverse=True):
            print(f"   - {vigencia}: {count} archivo(s)")

        archivos_elegibles = [
            archivo
            for archivo in archivos
            if self._should_include_year(self._archivo_year(archivo))
        ]

        print(
            f"\n🎯 Archivos elegibles para proceso (vigencia >= {MIN_YEAR}): "
            f"{len(archivos_elegibles)}"
        )

        pendientes_descarga = [
            archivo
            for archivo in archivos_elegibles
            if not self._resolve_output_file_path(archivo).exists()
            or self._resolve_output_file_path(archivo).stat().st_size == 0
        ]

        print(
            f"   - Nuevos por descargar: {len(pendientes_descarga)}\n"
            f"   - Ya disponibles localmente: {len(archivos_elegibles) - len(pendientes_descarga)}"
        )

        download_start = time.perf_counter()

        if pendientes_descarga:
            # Solo si hay pendientes, capturamos cookies de sesión para acelerar ejecución.
            driver = None
            try:
                driver = self._build_driver()
                driver.get(self.target_url)
                time.sleep(1)
                self._cookies_from_selenium_to_requests(driver.get_cookies())
            finally:
                if driver:
                    driver.quit()

        print("\n⬇️ Iniciando descargas (solo nuevos, vigencia >= 2020)...")
        for archivo in archivos_elegibles:
            try:
                self._download_archivo(archivo)
                descargados += 1
            except Exception as exc:
                errores += 1
                print(f"❌ Error descargando '{archivo.nombre}': {exc}")

        download_seconds = time.perf_counter() - download_start

        print("\n" + "=" * 70)
        print("Resumen")
        print("=" * 70)
        print(f"Total detectados: {len(archivos)}")
        print(f"Descargados/omitidos OK: {descargados}")
        print(f"Errores: {errores}")
        print(f"Directorio salida: {self.output_dir.resolve()}")
        print(f"⏱️ Tiempo descarga+validación: {download_seconds:.2f} segundos")

        consolidation_start = time.perf_counter()
        self.df_ejecucion_presupuestal = self._build_consolidated_dataframe()
        consolidation_seconds = time.perf_counter() - consolidation_start

        print(
            f"\n🧾 DataFrame 'df_ejecucion_presupuestal' creado con "
            f"{len(self.df_ejecucion_presupuestal):,} registros."
        )
        print(f"⏱️ Tiempo consolidación: {consolidation_seconds:.2f} segundos")

        total_seconds = time.perf_counter() - total_start
        print(f"⏱️ Tiempo total ejecución: {total_seconds:.2f} segundos")

        self._print_dataframe_metrics(self.df_ejecucion_presupuestal)

        return {
            "total_detectados": len(archivos),
            "total_elegibles_desde_2020": len(archivos_elegibles),
            "descargados_ok": descargados,
            "errores": errores,
            "tiempo_descarga_segundos": round(download_seconds, 2),
            "tiempo_consolidacion_segundos": round(consolidation_seconds, 2),
            "tiempo_total_segundos": round(total_seconds, 2),
        }


if __name__ == "__main__":
    scraper = EjecucionPresupuestalScraper()
    scraper.run()
