"""
Pipeline de Ejecución Presupuestal

Secuencia:
1) Ejecuta transformation_app/data_transformation_ejecucion_presupuestal.py
2) Verifica que existan los JSON procesados esperados
3) Ejecuta load_app/data_loading_bp.py

Uso:
    python ejecucion_presupuestal_pipeline.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List


@dataclass
class StepResult:
    name: str
    ok: bool
    duration_seconds: float
    command: List[str]


def write_pipeline_log(
    root: Path,
    results: List[StepResult],
    collection_name: str,
    total_duration: float,
    status: str,
    error_message: str = "",
) -> Path:
    logs_dir = root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"ejecucion_presupuestal_pipeline_{timestamp}.log"

    lines: List[str] = []
    lines.append("=" * 70)
    lines.append("PIPELINE EJECUCION PRESUPUESTAL")
    lines.append("=" * 70)
    lines.append(f"timestamp: {datetime.now().isoformat()}")
    lines.append(f"status: {status}")
    lines.append(f"coleccion_destino: {collection_name}")
    lines.append(f"tiempo_total_segundos: {total_duration:.2f}")
    lines.append("")
    lines.append("PASOS:")

    for result in results:
        step_status = "OK" if result.ok else "ERROR"
        lines.append(
            f"- {result.name}: {step_status} | {result.duration_seconds:.2f}s | cmd={' '.join(result.command)}"
        )

    if error_message:
        lines.append("")
        lines.append("ERROR:")
        lines.append(error_message)

    lines.append("=" * 70)

    log_path.write_text("\n".join(lines), encoding="utf-8")
    return log_path


def run_python_script(script_path: Path, project_root: Path, step_name: str) -> StepResult:
    if not script_path.exists():
        raise FileNotFoundError(f"No existe el script: {script_path}")

    command = [sys.executable, str(script_path)]

    print("\n" + "=" * 70)
    print(f"▶ Ejecutando paso: {step_name}")
    print(f"📄 Script: {script_path}")
    print("=" * 70)

    start = time.perf_counter()
    completed = subprocess.run(command, cwd=str(project_root), check=False)
    duration = time.perf_counter() - start

    ok = completed.returncode == 0
    if ok:
        print(f"✅ {step_name} completado en {duration:.2f}s")
    else:
        print(f"❌ {step_name} falló con código {completed.returncode} en {duration:.2f}s")

    return StepResult(
        name=step_name,
        ok=ok,
        duration_seconds=duration,
        command=command,
    )


def verify_processed_outputs(output_dir: Path) -> None:
    required_files = [
        output_dir / "datos_caracteristicos_proyectos.json",
        output_dir / "movimientos_presupuestales.json",
        output_dir / "ejecucion_presupuestal.json",
    ]

    print("\n🔎 Verificando archivos procesados de transformación...")
    missing_or_empty: List[Path] = []

    for file_path in required_files:
        if not file_path.exists() or file_path.stat().st_size == 0:
            missing_or_empty.append(file_path)
        else:
            size_mb = file_path.stat().st_size / (1024 * 1024)
            print(f"   ✅ {file_path.name} ({size_mb:.2f} MB)")

    if missing_or_empty:
        details = "\n".join(f" - {path}" for path in missing_or_empty)
        raise RuntimeError(
            "La transformación no produjo todos los archivos procesados requeridos:\n"
            f"{details}"
        )


def main() -> None:
    pipelines_dir = Path(__file__).resolve().parent
    root = pipelines_dir.parent
    collection_name = "ejecucion_presupuestal"

    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from load_app.data_loading_bp import load_budget_projects_data

    transformation_script = root / "transformation_app" / "data_transformation_ejecucion_presupuestal.py"
    loading_script = root / "load_app" / "data_loading_bp.py"

    output_dir = (
        root
        / "transformation_app"
        / "app_outputs"
        / "ejecucion_presupuestal_outputs"
    )

    print("\n" + "=" * 70)
    print("PIPELINE: EJECUCIÓN PRESUPUESTAL")
    print("=" * 70)
    print(f"📁 Proyecto: {root}")

    pipeline_start = time.perf_counter()
    results: List[StepResult] = []

    # Paso 1: Transformación
    transform_result = run_python_script(
        transformation_script,
        root,
        "Transformación de ejecución presupuestal",
    )
    results.append(transform_result)

    if not transform_result.ok:
        raise RuntimeError("Se detiene el pipeline porque falló la transformación.")

    # Verificación intermedia: archivos procesados
    verify_processed_outputs(output_dir)

    # Paso 2: Carga (forzar colección destino)
    print("\n" + "=" * 70)
    print("▶ Ejecutando paso: Carga a Firestore (BP)")
    print(f"📄 Script: {loading_script}")
    print(f"📚 Colección destino: {collection_name}")
    print("=" * 70)

    load_start = time.perf_counter()
    load_output = load_budget_projects_data(collection_name=collection_name)
    load_duration = time.perf_counter() - load_start

    load_ok = bool(load_output) and load_output.get("status") in {"success", "partial_success"}
    if load_ok:
        print(f"✅ Carga a Firestore completada en {load_duration:.2f}s")
    else:
        print(f"❌ Carga a Firestore falló en {load_duration:.2f}s")

    load_result = StepResult(
        name="Carga a Firestore (BP)",
        ok=load_ok,
        duration_seconds=load_duration,
        command=[sys.executable, str(loading_script), f"--collection={collection_name}"],
    )
    results.append(load_result)

    if not load_result.ok:
        raise RuntimeError(
            f"La carga falló para la colección '{collection_name}'. Resultado: {load_output}"
        )

    total_duration = time.perf_counter() - pipeline_start

    print("\n" + "=" * 70)
    print("RESUMEN PIPELINE")
    print("=" * 70)
    for result in results:
        status = "OK" if result.ok else "ERROR"
        print(f"- {result.name}: {status} ({result.duration_seconds:.2f}s)")
    print(f"- Colección destino Firestore: {collection_name}")
    print(f"- Tiempo total pipeline: {total_duration:.2f}s")
    print("✅ Pipeline completado exitosamente. Se cargaron archivos procesados.")

    log_path = write_pipeline_log(
        root=root,
        results=results,
        collection_name=collection_name,
        total_duration=total_duration,
        status="success",
    )
    print(f"📝 Log guardado en: {log_path}")


if __name__ == "__main__":
    pipeline_root = Path(__file__).resolve().parent.parent
    start = time.perf_counter()
    collected_results: List[StepResult] = []
    try:
        main()
    except Exception as exc:
        print(f"\n💥 Error en pipeline: {exc}")
        elapsed = time.perf_counter() - start
        try:
            log_path = write_pipeline_log(
                root=pipeline_root,
                results=collected_results,
                collection_name="ejecucion_presupuestal",
                total_duration=elapsed,
                status="error",
                error_message=str(exc),
            )
            print(f"📝 Log de error guardado en: {log_path}")
        except Exception:
            pass
        sys.exit(1)
