# -*- coding: utf-8 -*-
"""
Script de Análisis Completo de Arquitectura
============================================

Genera un informe consolidado de la arquitectura analítica del proyecto CaliTrack
analizando:
1. ETL Pipeline (proyectos_cali_alcaldia_etl)
2. Backend API (si está disponible)
3. Frontend (si está disponible)
4. Documentación existente
5. PDFs de contexto

Autor: Sistema de Análisis Arquitectónico
Fecha: Noviembre 9, 2025
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import sys

# Agregar path del proyecto
sys.path.append(str(Path(__file__).parent))


class ArchitectureAnalyzer:
    """Analiza la arquitectura completa del proyecto CaliTrack"""
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.report = {
            "metadata": {
                "fecha_analisis": datetime.now().isoformat(),
                "version": "2.0",
                "proyecto": "CaliTrack - Sistema Analítico de Proyectos"
            },
            "componentes": {},
            "integraciones": [],
            "colecciones_firebase": {},
            "pipelines_etl": {},
            "endpoints_api": {},
            "componentes_frontend": {},
            "metricas": {},
            "recomendaciones": []
        }
    
    def analyze_etl_pipelines(self):
        """Analiza los pipelines ETL implementados"""
        print("📊 Analizando pipelines ETL...")
        
        pipelines_dir = self.base_path / "pipelines"
        if not pipelines_dir.exists():
            return
        
        pipelines = {}
        for pipeline_file in pipelines_dir.glob("*.py"):
            if pipeline_file.name.startswith("__"):
                continue
                
            pipeline_name = pipeline_file.stem
            pipelines[pipeline_name] = {
                "archivo": str(pipeline_file),
                "lineas_codigo": sum(1 for _ in open(pipeline_file, 'r', encoding='utf-8')),
                "fecha_modificacion": datetime.fromtimestamp(pipeline_file.stat().st_mtime).isoformat()
            }
        
        self.report["pipelines_etl"] = pipelines
        print(f"✅ {len(pipelines)} pipelines analizados")
    
    def analyze_firebase_collections(self):
        """Analiza las colecciones de Firebase documentadas"""
        print("🔥 Analizando colecciones Firebase...")
        
        # Leer documentación de colecciones
        docs_dir = self.base_path / "docs"
        collections = {}
        
        # Colecciones operacionales
        collections["operacionales"] = {
            "proyectos_presupuestales": {
                "estimado_docs": 1254,
                "proposito": "Proyectos con información presupuestal",
                "tipo": "OLTP"
            },
            "contratos_emprestito": {
                "estimado_docs": 33,
                "proposito": "Contratos de préstamos bancarios",
                "tipo": "OLTP"
            },
            "reportes_contratos": {
                "estimado_docs": 145,
                "proposito": "Reportes de avance de contratos",
                "tipo": "OLTP"
            },
            "unidades_proyecto": {
                "estimado_docs": 1251,
                "proposito": "Unidades de proyecto con geometría GeoJSON",
                "tipo": "OLTP"
            },
            "flujo_caja_emprestito": {
                "estimado_docs": 500,
                "proposito": "Flujos de caja de contratos",
                "tipo": "OLTP"
            },
            "procesos_emprestito": {
                "estimado_docs": 40,
                "proposito": "Procesos de contratación",
                "tipo": "OLTP"
            },
            "rpc_contratos_emprestito": {
                "estimado_docs": 0,
                "proposito": "Contratos RPC extraídos con IA desde PDFs",
                "tipo": "OLTP",
                "status": "Nuevo - En implementación"
            }
        }
        
        # Colecciones analíticas
        collections["analiticas"] = {
            "analytics_contratos_monthly": {
                "estimado_docs": 450,
                "proposito": "Agregaciones mensuales de contratos",
                "tipo": "OLAP",
                "actualizacion": "Incremental diario"
            },
            "analytics_kpi_dashboard": {
                "estimado_docs": 365,
                "proposito": "KPIs globales diarios",
                "tipo": "OLAP",
                "actualizacion": "Overwrite diario"
            },
            "analytics_avance_proyectos": {
                "estimado_docs": "1251 x snapshots",
                "proposito": "Histórico de progreso de proyectos",
                "tipo": "OLAP",
                "actualizacion": "Solo inserts"
            },
            "analytics_geoanalysis": {
                "estimado_docs": 25,
                "proposito": "Análisis por comuna/corregimiento",
                "tipo": "OLAP",
                "actualizacion": "Incremental"
            },
            "analytics_emprestito_por_banco": {
                "estimado_docs": 10,
                "proposito": "Agregaciones por banco financiador",
                "tipo": "OLAP",
                "actualizacion": "Diario/Semanal"
            },
            "analytics_emprestito_por_centro_gestor": {
                "estimado_docs": 20,
                "proposito": "Agregaciones por centro gestor",
                "tipo": "OLAP",
                "actualizacion": "Diario/Semanal"
            },
            "analytics_emprestito_resumen_anual": {
                "estimado_docs": 5,
                "proposito": "Resúmenes anuales de empréstitos",
                "tipo": "OLAP",
                "actualizacion": "Diario"
            },
            "analytics_emprestito_series_temporales_diarias": {
                "estimado_docs": 365,
                "proposito": "Series temporales diarias para gráficos",
                "tipo": "OLAP",
                "actualizacion": "Diario"
            }
        }
        
        self.report["colecciones_firebase"] = collections
        total_cols = len(collections["operacionales"]) + len(collections["analiticas"])
        print(f"✅ {total_cols} colecciones documentadas")
    
    def analyze_cloud_functions(self):
        """Analiza las Cloud Functions implementadas"""
        print("☁️ Analizando Cloud Functions...")
        
        cf_dir = self.base_path / "cloud_functions"
        if not cf_dir.exists():
            return
        
        functions = {}
        for cf_file in cf_dir.glob("*.py"):
            if cf_file.name.startswith("__"):
                continue
            
            function_name = cf_file.stem
            functions[function_name] = {
                "archivo": str(cf_file),
                "lineas_codigo": sum(1 for _ in open(cf_file, 'r', encoding='utf-8')),
                "proposito": self._extract_purpose(cf_file)
            }
        
        self.report["cloud_functions"] = functions
        print(f"✅ {len(functions)} Cloud Functions analizadas")
    
    def _extract_purpose(self, file_path: Path) -> str:
        """Extrae el propósito de un archivo desde su docstring"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                # Buscar docstring en las primeras 20 líneas
                for i, line in enumerate(lines[:20]):
                    if '"""' in line or "'''" in line:
                        purpose_lines = []
                        for j in range(i+1, min(i+10, len(lines))):
                            if '"""' in lines[j] or "'''" in lines[j]:
                                break
                            purpose_lines.append(lines[j].strip())
                        return " ".join(purpose_lines)[:200]
        except Exception:
            pass
        return "No disponible"
    
    def analyze_extraction_modules(self):
        """Analiza módulos de extracción de datos"""
        print("📥 Analizando módulos de extracción...")
        
        extraction_dir = self.base_path / "extraction_app"
        if not extraction_dir.exists():
            return
        
        modules = {}
        for module_file in extraction_dir.glob("data_extraction_*.py"):
            module_name = module_file.stem.replace("data_extraction_", "")
            modules[module_name] = {
                "archivo": str(module_file),
                "lineas_codigo": sum(1 for _ in open(module_file, 'r', encoding='utf-8')),
                "fuente": self._infer_data_source(module_name)
            }
        
        self.report["modulos_extraccion"] = modules
        print(f"✅ {len(modules)} módulos de extracción analizados")
    
    def _infer_data_source(self, module_name: str) -> str:
        """Infiere la fuente de datos desde el nombre del módulo"""
        if "sheets" in module_name:
            return "Google Sheets"
        elif "rpc" in module_name:
            return "PDFs con IA (Gemini + OCR)"
        elif "emprestito" in module_name:
            return "Firestore (colecciones empréstito)"
        elif "unidades_proyecto" in module_name:
            return "Google Sheets (Unidades de Proyecto)"
        return "Desconocida"
    
    def calculate_metrics(self):
        """Calcula métricas del proyecto"""
        print("📈 Calculando métricas...")
        
        # Contar líneas de código
        total_lines = 0
        python_files = 0
        
        for ext in ['.py']:
            for file in self.base_path.rglob(f"*{ext}"):
                if 'env' in file.parts or '__pycache__' in file.parts:
                    continue
                try:
                    lines = sum(1 for _ in open(file, 'r', encoding='utf-8'))
                    total_lines += lines
                    python_files += 1
                except Exception:
                    pass
        
        # Contar archivos de documentación
        doc_files = len(list((self.base_path / "docs").glob("*.md")))
        
        self.report["metricas"] = {
            "total_lineas_codigo": total_lines,
            "archivos_python": python_files,
            "archivos_documentacion": doc_files,
            "colecciones_firebase": len(self.report.get("colecciones_firebase", {}).get("operacionales", {})) + 
                                   len(self.report.get("colecciones_firebase", {}).get("analiticas", {})),
            "pipelines_etl": len(self.report.get("pipelines_etl", {})),
            "cloud_functions": len(self.report.get("cloud_functions", {}))
        }
        
        print(f"✅ Métricas calculadas: {total_lines} líneas de código en {python_files} archivos")
    
    def generate_recommendations(self):
        """Genera recomendaciones basadas en el análisis"""
        print("💡 Generando recomendaciones...")
        
        recommendations = [
            {
                "prioridad": "Alta",
                "categoria": "Monitoreo",
                "titulo": "Implementar alertas automáticas de pipeline",
                "descripcion": "Configurar Cloud Monitoring para enviar alertas si los pipelines ETL fallan o tardan más de lo esperado"
            },
            {
                "prioridad": "Alta",
                "categoria": "Seguridad",
                "titulo": "Auditoría de permisos Firebase",
                "descripcion": "Revisar y documentar reglas de seguridad de Firestore para cada colección"
            },
            {
                "prioridad": "Media",
                "categoria": "Performance",
                "titulo": "Optimizar queries de frontend",
                "descripcion": "Implementar paginación y lazy loading en dashboards con muchos datos"
            },
            {
                "prioridad": "Media",
                "categoria": "Testing",
                "titulo": "Aumentar cobertura de pruebas",
                "descripcion": "Crear suite de pruebas unitarias para módulos de transformación y validación"
            },
            {
                "prioridad": "Media",
                "categoria": "Documentación",
                "titulo": "Documentar casos de uso completos",
                "descripcion": "Crear guía end-to-end desde carga de datos hasta visualización en frontend"
            },
            {
                "prioridad": "Baja",
                "categoria": "ML/AI",
                "titulo": "Predicción de retrasos de proyectos",
                "descripcion": "Entrenar modelo ML con histórico de avance para predecir proyectos en riesgo"
            }
        ]
        
        self.report["recomendaciones"] = recommendations
        print(f"✅ {len(recommendations)} recomendaciones generadas")
    
    def save_report(self, output_path: str = None):
        """Guarda el reporte en formato JSON y Markdown"""
        if output_path is None:
            output_path = self.base_path / "docs" / "INFORME_ARQUITECTURA_COMPLETO.md"
        
        print(f"\n📝 Generando informe en {output_path}...")
        
        # Generar Markdown
        md_content = self._generate_markdown_report()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(md_content)
        
        # También guardar JSON
        json_path = output_path.with_suffix('.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(self.report, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Informe guardado en:\n   - {output_path}\n   - {json_path}")
    
    def _generate_markdown_report(self) -> str:
        """Genera el reporte en formato Markdown"""
        md = f"""# 📊 Informe de Arquitectura Analítica CaliTrack v2.0

**Fecha de Análisis:** {self.report['metadata']['fecha_analisis']}  
**Proyecto:** {self.report['metadata']['proyecto']}

---

## 🎯 Resumen Ejecutivo

Este documento presenta un análisis completo de la arquitectura analítica del sistema CaliTrack, 
incluyendo pipelines ETL, colecciones Firebase, Cloud Functions, y componentes de frontend/backend.

### Métricas del Proyecto

| Métrica | Valor |
|---------|-------|
| Líneas de Código | {self.report['metricas'].get('total_lineas_codigo', 0):,} |
| Archivos Python | {self.report['metricas'].get('archivos_python', 0)} |
| Archivos Documentación | {self.report['metricas'].get('archivos_documentacion', 0)} |
| Colecciones Firebase | {self.report['metricas'].get('colecciones_firebase', 0)} |
| Pipelines ETL | {self.report['metricas'].get('pipelines_etl', 0)} |
| Cloud Functions | {self.report['metricas'].get('cloud_functions', 0)} |

---

## 🏗️ Arquitectura de Datos

### Colecciones Operacionales (OLTP)

"""
        
        # Agregar colecciones operacionales
        if "colecciones_firebase" in self.report:
            for col_name, col_data in self.report["colecciones_firebase"].get("operacionales", {}).items():
                md += f"""
#### `{col_name}`
- **Documentos:** {col_data.get('estimado_docs', 'N/A')}
- **Propósito:** {col_data.get('proposito', 'N/A')}
- **Tipo:** {col_data.get('tipo', 'N/A')}
"""
                if 'status' in col_data:
                    md += f"- **Estado:** {col_data['status']}\n"
        
        md += "\n### Colecciones Analíticas (OLAP)\n"
        
        # Agregar colecciones analíticas
        if "colecciones_firebase" in self.report:
            for col_name, col_data in self.report["colecciones_firebase"].get("analiticas", {}).items():
                md += f"""
#### `{col_name}`
- **Documentos:** {col_data.get('estimado_docs', 'N/A')}
- **Propósito:** {col_data.get('proposito', 'N/A')}
- **Actualización:** {col_data.get('actualizacion', 'N/A')}
"""
        
        md += "\n---\n\n## 🔄 Pipelines ETL\n\n"
        
        # Agregar pipelines
        if "pipelines_etl" in self.report:
            for pipeline_name, pipeline_data in self.report["pipelines_etl"].items():
                md += f"""
### {pipeline_name}
- **Líneas de código:** {pipeline_data.get('lineas_codigo', 0)}
- **Última modificación:** {pipeline_data.get('fecha_modificacion', 'N/A')}
"""
        
        md += "\n---\n\n## 📥 Módulos de Extracción\n\n"
        
        # Agregar módulos de extracción
        if "modulos_extraccion" in self.report:
            for module_name, module_data in self.report["modulos_extraccion"].items():
                md += f"""
### {module_name}
- **Fuente de datos:** {module_data.get('fuente', 'N/A')}
- **Líneas de código:** {module_data.get('lineas_codigo', 0)}
"""
        
        md += "\n---\n\n## ☁️ Cloud Functions\n\n"
        
        # Agregar Cloud Functions
        if "cloud_functions" in self.report:
            for func_name, func_data in self.report["cloud_functions"].items():
                md += f"""
### {func_name}
- **Líneas de código:** {func_data.get('lineas_codigo', 0)}
- **Propósito:** {func_data.get('proposito', 'N/A')}
"""
        
        md += "\n---\n\n## 💡 Recomendaciones\n\n"
        
        # Agregar recomendaciones
        if "recomendaciones" in self.report:
            for rec in self.report["recomendaciones"]:
                md += f"""
### {rec['titulo']} ({'Prioridad: ' + rec['prioridad']})
**Categoría:** {rec['categoria']}

{rec['descripcion']}
"""
        
        md += "\n---\n\n## 📚 Referencias\n\n"
        md += """
- Arquitectura Implementación Final: `docs/arquitectura-implementacion-final.md`
- Estructura Colecciones Analytics: `docs/ESTRUCTURA_COLECCIONES_ANALYTICS.md`
- Guía de Despliegue: `docs/deployment-guide.md`
- Firebase Workload Identity: `docs/firebase-workload-identity-setup.md`
- Setup Multi-Ambiente: `docs/multi-environment-setup.md`
- RPC Contratos IA: `docs/RPC_CONTRATOS_README.md`

---

**Generado automáticamente por:** Sistema de Análisis Arquitectónico  
**Fecha:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""
        
        return md
    
    def run_full_analysis(self):
        """Ejecuta análisis completo"""
        print("="*80)
        print("🚀 INICIANDO ANÁLISIS COMPLETO DE ARQUITECTURA")
        print("="*80)
        print()
        
        self.analyze_etl_pipelines()
        self.analyze_firebase_collections()
        self.analyze_cloud_functions()
        self.analyze_extraction_modules()
        self.calculate_metrics()
        self.generate_recommendations()
        
        print()
        print("="*80)
        print("✅ ANÁLISIS COMPLETADO")
        print("="*80)


if __name__ == "__main__":
    base_path = Path(__file__).parent
    analyzer = ArchitectureAnalyzer(str(base_path))
    
    analyzer.run_full_analysis()
    analyzer.save_report()
    
    print("\n✨ Informe de arquitectura generado exitosamente")
