# -*- coding: utf-8 -*-
"""
Sistema de Reportes de Control de Calidad
==========================================

Genera reportes detallados de calidad de datos a nivel de registro individual
y agregados por centro gestor. Prepara datos para Firebase y S3.

Author: ETL QA Team
Date: November 2025
Version: 1.0
"""

import json
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from pathlib import Path
from collections import defaultdict
import hashlib


class QualityReporter:
    """
    Genera reportes de calidad de datos en múltiples formatos y niveles de agregación.
    """
    
    def __init__(self):
        """Inicializa el generador de reportes."""
        self.report_timestamp = datetime.now().isoformat()
        self.report_id = self._generate_report_id()
    
    def _generate_report_id(self) -> str:
        """Genera un ID único para el reporte."""
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        hash_part = hashlib.md5(self.report_timestamp.encode()).hexdigest()[:8]
        return f"QC_{timestamp_str}_{hash_part}"
    
    def generate_record_level_report(
        self, 
        issues: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Genera reporte detallado a nivel de registro individual.
        
        Args:
            issues: Lista de problemas detectados con información de registro
            
        Returns:
            Lista de registros con problemas para colección Firebase
        """
        # Agrupar problemas por registro
        records_map = defaultdict(lambda: {
            'issues': [],
            'upid': None,
            'nombre_up': None,
            'nombre_centro_gestor': None,
            'record_index': None
        })
        
        for issue in issues:
            record_key = issue.get('upid') or f"IDX_{issue.get('record_index')}"
            
            records_map[record_key]['issues'].append({
                'rule_id': issue['rule_id'],
                'rule_name': issue['rule_name'],
                'dimension': issue['dimension'],
                'severity': issue['severity'],
                'field_name': issue['field_name'],
                'current_value': issue['current_value'],
                'expected_value': issue['expected_value'],
                'details': issue['details'],
                'suggestion': issue['suggestion'],
                'detected_at': issue['detected_at']
            })
            
            # Actualizar metadata del registro
            if not records_map[record_key]['upid']:
                records_map[record_key]['upid'] = issue.get('upid')
            if not records_map[record_key]['nombre_up']:
                records_map[record_key]['nombre_up'] = issue.get('nombre_up')
            if not records_map[record_key]['nombre_centro_gestor']:
                records_map[record_key]['nombre_centro_gestor'] = issue.get('nombre_centro_gestor')
            if records_map[record_key]['record_index'] is None:
                records_map[record_key]['record_index'] = issue.get('record_index')
        
        # Convertir a lista de documentos para Firebase
        record_reports = []
        
        for record_key, data in records_map.items():
            # Calcular estadísticas del registro
            issues_list = data['issues']
            severity_counts = self._count_by_field(issues_list, 'severity')
            dimension_counts = self._count_by_field(issues_list, 'dimension')
            field_counts = self._count_by_field(issues_list, 'field_name')
            
            # Determinar severidad máxima
            max_severity = self._get_max_severity(issues_list)
            
            # Crear documento del registro
            record_doc = {
                'document_id': record_key,
                'upid': data['upid'],
                'nombre_up': data['nombre_up'],
                'nombre_centro_gestor': data['nombre_centro_gestor'],
                'record_index': data['record_index'],
                'report_id': self.report_id,
                'report_timestamp': self.report_timestamp,
                
                # Estadísticas del registro
                'total_issues': len(issues_list),
                'max_severity': max_severity,
                'severity_counts': severity_counts,
                'dimension_counts': dimension_counts,
                'affected_fields': list(field_counts.keys()),
                'affected_fields_count': len(field_counts),
                
                # Problemas detallados
                'issues': issues_list,
                
                # Clasificación para priorización
                'priority': self._calculate_priority(max_severity, len(issues_list)),
                'requires_immediate_action': max_severity in ['CRITICAL', 'HIGH'],
                
                # Metadata
                'created_at': self.report_timestamp,
                'updated_at': self.report_timestamp
            }
            
            record_reports.append(record_doc)
        
        # Ordenar por prioridad (más críticos primero)
        record_reports.sort(
            key=lambda x: (
                ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO'].index(x['max_severity']),
                -x['total_issues']
            )
        )
        
        return record_reports
    
    def generate_centro_gestor_report(
        self, 
        issues: List[Dict[str, Any]],
        total_records_by_centro: Optional[Dict[str, int]] = None
    ) -> List[Dict[str, Any]]:
        """
        Genera reporte agregado por centro gestor.
        
        Args:
            issues: Lista de problemas detectados
            total_records_by_centro: Diccionario con total de registros por centro
            
        Returns:
            Lista de reportes por centro gestor para colección Firebase
        """
        # Agrupar por centro gestor
        centros_map = defaultdict(lambda: {
            'issues': [],
            'records': set(),
            'problems_by_field': defaultdict(int),
            'problems_by_rule': defaultdict(int)
        })
        
        for issue in issues:
            centro = issue.get('nombre_centro_gestor', 'Sin Centro Gestor')
            upid = issue.get('upid')
            
            centros_map[centro]['issues'].append(issue)
            if upid:
                centros_map[centro]['records'].add(upid)
            
            # Contar por campo y regla
            field = issue.get('field_name')
            if field:
                centros_map[centro]['problems_by_field'][field] += 1
            
            rule_id = issue.get('rule_id')
            centros_map[centro]['problems_by_rule'][rule_id] += 1
        
        # Generar reportes por centro
        centro_reports = []
        
        for centro_name, data in centros_map.items():
            issues_list = data['issues']
            affected_records = list(data['records'])
            
            # Estadísticas generales
            severity_counts = self._count_by_field(issues_list, 'severity')
            dimension_counts = self._count_by_field(issues_list, 'dimension')
            
            # Total de registros del centro
            total_records = total_records_by_centro.get(centro_name, len(affected_records)) if total_records_by_centro else len(affected_records)
            
            # Calcular tasas
            records_with_issues = len(affected_records)
            error_rate = (records_with_issues / total_records * 100) if total_records > 0 else 0
            
            # Top problemas
            top_fields = sorted(
                data['problems_by_field'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
            
            top_rules = sorted(
                data['problems_by_rule'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
            
            # Calcular score de calidad
            quality_score = self._calculate_quality_score(
                total_records,
                records_with_issues,
                severity_counts
            )
            
            # Crear documento del centro gestor
            centro_doc = {
                'document_id': self._sanitize_id(centro_name),
                'nombre_centro_gestor': centro_name,
                'report_id': self.report_id,
                'report_timestamp': self.report_timestamp,
                
                # Estadísticas generales
                'total_records': total_records,
                'records_with_issues': records_with_issues,
                'records_without_issues': total_records - records_with_issues,
                'error_rate': round(error_rate, 2),
                'quality_score': round(quality_score, 2),
                
                # Conteo de problemas
                'total_issues': len(issues_list),
                'severity_counts': severity_counts,
                'dimension_counts': dimension_counts,
                
                # Problemas más frecuentes
                'top_problematic_fields': [
                    {'field': field, 'count': count} 
                    for field, count in top_fields
                ],
                'top_violated_rules': [
                    {'rule_id': rule, 'count': count} 
                    for rule, count in top_rules
                ],
                
                # Registros afectados
                'affected_records': affected_records[:100],  # Primeros 100
                'affected_records_count': len(affected_records),
                
                # Clasificación
                'status': self._classify_centro_status(quality_score, error_rate),
                'requires_attention': quality_score < 80 or error_rate > 20,
                
                # Metadata
                'created_at': self.report_timestamp,
                'updated_at': self.report_timestamp
            }
            
            centro_reports.append(centro_doc)
        
        # Ordenar por score de calidad (peores primero)
        centro_reports.sort(key=lambda x: x['quality_score'])
        
        return centro_reports
    
    def generate_summary_report(
        self,
        record_reports: List[Dict[str, Any]],
        centro_reports: List[Dict[str, Any]],
        total_records: int,
        validation_stats: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Genera reporte resumen general del sistema.
        
        Args:
            record_reports: Reportes a nivel de registro
            centro_reports: Reportes por centro gestor
            total_records: Total de registros validados
            validation_stats: Estadísticas de validación
            
        Returns:
            Documento resumen para colección Firebase
        """
        # Calcular estadísticas globales
        total_issues = sum(r['total_issues'] for r in record_reports)
        records_with_issues = len(record_reports)
        records_without_issues = total_records - records_with_issues
        
        # Severidad global
        global_severity = defaultdict(int)
        for report in record_reports:
            for severity, count in report['severity_counts'].items():
                global_severity[severity] += count
        
        # Dimensión global
        global_dimension = defaultdict(int)
        for report in record_reports:
            for dimension, count in report['dimension_counts'].items():
                global_dimension[dimension] += count
        
        # Top centros con problemas
        top_problematic_centros = sorted(
            centro_reports,
            key=lambda x: x['total_issues'],
            reverse=True
        )[:10]
        
        # Top centros con mejor calidad
        top_quality_centros = sorted(
            centro_reports,
            key=lambda x: x['quality_score'],
            reverse=True
        )[:10]
        
        # Calcular score global
        global_quality_score = validation_stats.get('quality_score', 0)
        
        # Recomendaciones automáticas
        recommendations = self._generate_recommendations(
            global_severity,
            global_dimension,
            centro_reports,
            global_quality_score
        )
        
        # Crear documento resumen
        summary_doc = {
            'document_id': f'summary_{self.report_id}',
            'report_id': self.report_id,
            'report_timestamp': self.report_timestamp,
            'report_type': 'QUALITY_CONTROL_SUMMARY',
            
            # Estadísticas globales
            'total_records_validated': total_records,
            'records_with_issues': records_with_issues,
            'records_without_issues': records_without_issues,
            'error_rate': round((records_with_issues / total_records * 100), 2) if total_records > 0 else 0,
            'total_issues_found': total_issues,
            'global_quality_score': round(global_quality_score, 2),
            
            # Distribución por severidad
            'severity_distribution': dict(global_severity),
            
            # Distribución por dimensión ISO 19157
            'dimension_distribution': dict(global_dimension),
            
            # Centros gestores
            'total_centros_gestores': len(centro_reports),
            'centros_require_attention': sum(1 for c in centro_reports if c['requires_attention']),
            
            # Top centros
            'top_problematic_centros': [
                {
                    'nombre': c['nombre_centro_gestor'],
                    'issues': c['total_issues'],
                    'error_rate': c['error_rate'],
                    'quality_score': c['quality_score']
                }
                for c in top_problematic_centros
            ],
            
            'top_quality_centros': [
                {
                    'nombre': c['nombre_centro_gestor'],
                    'issues': c['total_issues'],
                    'error_rate': c['error_rate'],
                    'quality_score': c['quality_score']
                }
                for c in top_quality_centros
            ],
            
            # Recomendaciones
            'recommendations': recommendations,
            
            # Estado general del sistema
            'system_status': self._classify_system_status(global_quality_score),
            'requires_immediate_action': global_severity.get('CRITICAL', 0) > 0,
            
            # Metadata
            'created_at': self.report_timestamp,
            'updated_at': self.report_timestamp,
            
            # Información adicional
            'validation_engine_version': '1.0',
            'iso_standard': 'ISO 19157',
            'dimensions_evaluated': [
                'Consistencia Lógica',
                'Completitud',
                'Exactitud Posicional',
                'Exactitud Temática',
                'Calidad Temporal'
            ]
        }
        
        return summary_doc
    
    def export_to_json(
        self,
        data: Any,
        output_path: str,
        pretty: bool = True
    ) -> bool:
        """
        Exporta datos a archivo JSON.
        
        Args:
            data: Datos a exportar
            output_path: Ruta del archivo de salida
            pretty: Si True, formatea el JSON con indentación
            
        Returns:
            True si se exportó exitosamente
        """
        try:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(data, f, indent=2, ensure_ascii=False, default=str)
                else:
                    json.dump(data, f, ensure_ascii=False, default=str)
            
            file_size = output_file.stat().st_size / 1024
            print(f"  ✓ Exportado: {output_file.name} ({file_size:.1f} KB)")
            return True
            
        except Exception as e:
            print(f"  ✗ Error exportando {output_path}: {e}")
            return False
    
    def export_to_excel(
        self,
        record_reports: List[Dict[str, Any]],
        centro_reports: List[Dict[str, Any]],
        summary_report: Dict[str, Any],
        output_path: str
    ) -> bool:
        """
        Exporta reportes a archivo Excel con múltiples hojas.
        
        Args:
            record_reports: Reportes por registro
            centro_reports: Reportes por centro gestor
            summary_report: Reporte resumen
            output_path: Ruta del archivo de salida
            
        Returns:
            True si se exportó exitosamente
        """
        try:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Hoja 1: Resumen
                summary_df = pd.DataFrame([summary_report])
                summary_df.to_excel(writer, sheet_name='Resumen', index=False)
                
                # Hoja 2: Por Centro Gestor
                centro_df = pd.DataFrame(centro_reports)
                # Simplificar columnas complejas
                if 'top_problematic_fields' in centro_df.columns:
                    centro_df['top_problematic_fields'] = centro_df['top_problematic_fields'].apply(
                        lambda x: ', '.join([f"{f['field']} ({f['count']})" for f in x[:3]]) if x else ''
                    )
                if 'affected_records' in centro_df.columns:
                    centro_df = centro_df.drop(columns=['affected_records'])
                
                centro_df.to_excel(writer, sheet_name='Por Centro Gestor', index=False)
                
                # Hoja 3: Registros con Problemas (simplificado)
                records_simple = []
                for rec in record_reports:
                    records_simple.append({
                        'UPID': rec['upid'],
                        'Nombre UP': rec['nombre_up'],
                        'Centro Gestor': rec['nombre_centro_gestor'],
                        'Total Problemas': rec['total_issues'],
                        'Severidad Máxima': rec['max_severity'],
                        'Prioridad': rec['priority'],
                        'Acción Inmediata': 'SÍ' if rec['requires_immediate_action'] else 'NO',
                        'Campos Afectados': ', '.join(rec['affected_fields'][:5])
                    })
                
                records_df = pd.DataFrame(records_simple)
                records_df.to_excel(writer, sheet_name='Registros con Problemas', index=False)
            
            file_size = output_file.stat().st_size / 1024
            print(f"  ✓ Exportado: {output_file.name} ({file_size:.1f} KB)")
            return True
            
        except Exception as e:
            print(f"  ✗ Error exportando Excel {output_path}: {e}")
            return False
    
    # ==================== MÉTODOS AUXILIARES ====================
    
    def _count_by_field(self, items: List[Dict], field: str) -> Dict[str, int]:
        """Cuenta ocurrencias de valores en un campo."""
        counts = defaultdict(int)
        for item in items:
            value = item.get(field)
            if value:
                counts[value] += 1
        return dict(counts)
    
    def _get_max_severity(self, issues: List[Dict]) -> str:
        """Obtiene la severidad máxima de una lista de problemas."""
        severity_order = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO']
        
        for severity in severity_order:
            if any(issue['severity'] == severity for issue in issues):
                return severity
        
        return 'INFO'
    
    def _calculate_priority(self, max_severity: str, issue_count: int) -> str:
        """
        Calcula la prioridad de corrección con sistema P0-P3.
        
        Sistema de priorización mejorado:
        - P0 (CRITICAL): Problemas críticos que impiden uso del dato
        - P1 (HIGH): Problemas graves con alto volumen o críticos con bajo volumen
        - P2 (MEDIUM): Problemas moderados con volumen considerable
        - P3 (LOW): Problemas menores o de bajo impacto
        """
        # P0: Siempre crítico si hay muchos problemas críticos
        if max_severity == 'CRITICAL' and issue_count >= 5:
            return 'P0'
        
        # P1: Problemas críticos o problemas altos con volumen significativo
        if max_severity == 'CRITICAL':
            return 'P1'
        elif max_severity == 'HIGH' and issue_count >= 10:
            return 'P1'
        
        # P2: Problemas altos o problemas medios con volumen considerable
        if max_severity == 'HIGH':
            return 'P2'
        elif max_severity == 'MEDIUM' and issue_count >= 15:
            return 'P2'
        
        # P3: Resto de casos (problemas medios/bajos con poco volumen)
        if max_severity == 'MEDIUM':
            return 'P3'
        else:
            return 'P3'
    
    def _calculate_quality_score(
        self,
        total: int,
        with_issues: int,
        severity_counts: Dict[str, int]
    ) -> float:
        """Calcula un score de calidad (0-100)."""
        if total == 0:
            return 100.0
        
        # Penalizaciones por severidad
        penalties = {
            'CRITICAL': 10,
            'HIGH': 5,
            'MEDIUM': 2,
            'LOW': 1,
            'INFO': 0
        }
        
        total_penalty = sum(
            severity_counts.get(sev, 0) * pen 
            for sev, pen in penalties.items()
        )
        
        max_penalty = total * 10
        score = max(0, 100 - (total_penalty / max_penalty * 100))
        
        return score
    
    def _classify_centro_status(self, quality_score: float, error_rate: float) -> str:
        """Clasifica el estado de un centro gestor."""
        if quality_score >= 90 and error_rate < 10:
            return 'EXCELLENT'
        elif quality_score >= 75 and error_rate < 20:
            return 'GOOD'
        elif quality_score >= 60 and error_rate < 35:
            return 'FAIR'
        elif quality_score >= 40:
            return 'POOR'
        else:
            return 'CRITICAL'
    
    def _classify_system_status(self, global_quality_score: float) -> str:
        """Clasifica el estado general del sistema."""
        if global_quality_score >= 90:
            return 'EXCELLENT'
        elif global_quality_score >= 75:
            return 'GOOD'
        elif global_quality_score >= 60:
            return 'ACCEPTABLE'
        elif global_quality_score >= 40:
            return 'NEEDS_IMPROVEMENT'
        else:
            return 'CRITICAL'
    
    def _generate_recommendations(
        self,
        severity_counts: Dict[str, int],
        dimension_counts: Dict[str, int],
        centro_reports: List[Dict[str, Any]],
        quality_score: float
    ) -> List[Dict[str, str]]:
        """Genera recomendaciones automáticas basadas en los problemas encontrados."""
        recommendations = []
        
        # Recomendaciones por severidad
        if severity_counts.get('CRITICAL', 0) > 0:
            recommendations.append({
                'priority': 'URGENT',
                'category': 'Problemas Críticos',
                'recommendation': f"Se detectaron {severity_counts['CRITICAL']} problemas CRÍTICOS que deben corregirse inmediatamente antes de usar los datos en producción."
            })
        
        if severity_counts.get('HIGH', 0) > 10:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Problemas Graves',
                'recommendation': f"Hay {severity_counts['HIGH']} problemas de severidad ALTA. Revisar y corregir en orden de prioridad."
            })
        
        # Recomendaciones por dimensión
        max_dimension = max(dimension_counts.items(), key=lambda x: x[1])[0] if dimension_counts else None
        
        if max_dimension:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Área Problemática Principal',
                'recommendation': f"La dimensión '{max_dimension}' tiene más problemas ({dimension_counts[max_dimension]}). Enfocar esfuerzos de corrección en esta área."
            })
        
        # Recomendaciones por centros gestores
        problematic_centros = [c for c in centro_reports if c['quality_score'] < 60]
        
        if problematic_centros:
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Centros Gestores',
                'recommendation': f"{len(problematic_centros)} centro(s) gestor(es) tienen calidad inferior al 60%. Realizar capacitación o revisión de procesos."
            })
        
        # Recomendación general por score
        if quality_score < 70:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Calidad General',
                'recommendation': f"Score de calidad global es {quality_score:.1f}%. Se recomienda implementar proceso sistemático de revisión y corrección de datos."
            })
        
        return recommendations
    
    def generate_categorical_metadata(
        self,
        record_reports: List[Dict[str, Any]],
        centro_reports: List[Dict[str, Any]],
        summary_report: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Genera metadata categórica optimizada para componentes de Next.js.
        
        Esta función proporciona listas de valores únicos, rangos, y configuraciones
        para dropdowns, tabs, filtros, tablas, etc.
        
        Args:
            record_reports: Reportes a nivel de registro
            centro_reports: Reportes por centro gestor
            summary_report: Reporte resumen
            
        Returns:
            Diccionario con metadata categórica estructurada
        """
        # Extraer todos los valores únicos
        all_severities = set()
        all_dimensions = set()
        all_rule_ids = set()
        all_rule_names = set()
        all_field_names = set()
        all_centros_gestores = set()
        all_priorities = set()
        all_statuses = set()
        
        # Recopilar de record_reports
        for record in record_reports:
            all_severities.update(record.get('severity_counts', {}).keys())
            all_dimensions.update(record.get('dimension_counts', {}).keys())
            all_priorities.add(record.get('priority', 'P3'))
            
            # Extraer de issues individuales
            for issue in record.get('issues', []):
                all_rule_ids.add(issue.get('rule_id', ''))
                all_rule_names.add(issue.get('rule_name', ''))
                all_field_names.add(issue.get('field_name', ''))
        
        # Recopilar de centro_reports
        for centro in centro_reports:
            all_centros_gestores.add(centro.get('nombre_centro_gestor', ''))
            all_statuses.add(centro.get('status', ''))
        
        # Calcular rangos numéricos
        quality_scores = [c.get('quality_score', 0) for c in centro_reports]
        error_rates = [c.get('error_rate', 0) for c in centro_reports]
        issue_counts = [c.get('total_issues', 0) for c in centro_reports]
        
        # Crear opciones de filtrado (filtrar None y valores nulos)
        filter_options = {
            'severities': sorted([x for x in all_severities if x], 
                                key=lambda x: ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'].index(x) 
                                if x in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'] else 999),
            'dimensions': sorted([x for x in all_dimensions if x]),
            'priorities': sorted([x for x in all_priorities if x]),
            'statuses': sorted([x for x in all_statuses if x]),
            'centros_gestores': sorted([x for x in all_centros_gestores if x]),
            'rule_ids': sorted([x for x in all_rule_ids if x]),
            'field_names': sorted([x for x in all_field_names if x])
        }
        
        # Crear configuración de rangos (para sliders, gráficas)
        ranges = {
            'quality_score': {
                'min': min(quality_scores) if quality_scores else 0,
                'max': max(quality_scores) if quality_scores else 100,
                'average': sum(quality_scores) / len(quality_scores) if quality_scores else 0,
                'median': sorted(quality_scores)[len(quality_scores)//2] if quality_scores else 0,
                'step': 5
            },
            'error_rate': {
                'min': min(error_rates) if error_rates else 0,
                'max': max(error_rates) if error_rates else 100,
                'average': sum(error_rates) / len(error_rates) if error_rates else 0,
                'median': sorted(error_rates)[len(error_rates)//2] if error_rates else 0,
                'step': 1
            },
            'issue_count': {
                'min': min(issue_counts) if issue_counts else 0,
                'max': max(issue_counts) if issue_counts else 0,
                'average': sum(issue_counts) / len(issue_counts) if issue_counts else 0,
                'median': sorted(issue_counts)[len(issue_counts)//2] if issue_counts else 0,
                'step': 1
            }
        }
        
        # Configuración de tabs/pestañas
        tab_configurations = {
            'main_tabs': [
                {'id': 'overview', 'label': 'Resumen General', 'icon': 'dashboard'},
                {'id': 'by_centro', 'label': 'Por Centro Gestor', 'icon': 'building'},
                {'id': 'by_severity', 'label': 'Por Severidad', 'icon': 'warning'},
                {'id': 'by_dimension', 'label': 'Por Dimensión ISO', 'icon': 'layers'},
                {'id': 'records', 'label': 'Registros Detallados', 'icon': 'list'}
            ],
            'severity_tabs': [
                {'id': 'critical', 'label': 'Críticos', 'color': 'red', 'count': summary_report.get('severity_distribution', {}).get('CRITICAL', 0)},
                {'id': 'high', 'label': 'Altos', 'color': 'orange', 'count': summary_report.get('severity_distribution', {}).get('HIGH', 0)},
                {'id': 'medium', 'label': 'Medios', 'color': 'yellow', 'count': summary_report.get('severity_distribution', {}).get('MEDIUM', 0)},
                {'id': 'low', 'label': 'Bajos', 'color': 'blue', 'count': summary_report.get('severity_distribution', {}).get('LOW', 0)}
            ],
            'priority_tabs': [
                {'id': 'p0', 'label': 'P0 - Crítico', 'color': 'red'},
                {'id': 'p1', 'label': 'P1 - Alto', 'color': 'orange'},
                {'id': 'p2', 'label': 'P2 - Medio', 'color': 'yellow'},
                {'id': 'p3', 'label': 'P3 - Bajo', 'color': 'green'}
            ]
        }
        
        # Configuración de tablas
        table_configurations = {
            'centro_gestor_table': {
                'columns': [
                    {'id': 'nombre_centro_gestor', 'label': 'Centro Gestor', 'type': 'string', 'sortable': True, 'filterable': True},
                    {'id': 'total_records', 'label': 'Total Registros', 'type': 'number', 'sortable': True},
                    {'id': 'records_with_issues', 'label': 'Con Problemas', 'type': 'number', 'sortable': True},
                    {'id': 'error_rate', 'label': 'Tasa Error (%)', 'type': 'number', 'sortable': True, 'format': 'percentage'},
                    {'id': 'quality_score', 'label': 'Score Calidad', 'type': 'number', 'sortable': True, 'colorize': True},
                    {'id': 'priority', 'label': 'Prioridad', 'type': 'badge', 'sortable': True, 'filterable': True},
                    {'id': 'status', 'label': 'Estado', 'type': 'badge', 'sortable': True, 'filterable': True}
                ],
                'default_sort': {'column': 'quality_score', 'direction': 'asc'},
                'items_per_page': 20,
                'exportable': True
            },
            'record_table': {
                'columns': [
                    {'id': 'upid', 'label': 'UPID', 'type': 'string', 'sortable': True, 'filterable': True},
                    {'id': 'nombre_up', 'label': 'Nombre UP', 'type': 'string', 'sortable': True, 'filterable': True},
                    {'id': 'nombre_centro_gestor', 'label': 'Centro Gestor', 'type': 'string', 'sortable': True, 'filterable': True},
                    {'id': 'total_issues', 'label': 'Total Problemas', 'type': 'number', 'sortable': True},
                    {'id': 'max_severity', 'label': 'Severidad Máx.', 'type': 'badge', 'sortable': True, 'filterable': True},
                    {'id': 'priority', 'label': 'Prioridad', 'type': 'badge', 'sortable': True, 'filterable': True}
                ],
                'default_sort': {'column': 'total_issues', 'direction': 'desc'},
                'items_per_page': 50,
                'expandable': True
            }
        }
        
        # Configuración de gráficas
        chart_configurations = {
            'severity_distribution': {
                'type': 'pie',
                'data_key': 'severity_distribution',
                'colors': {
                    'CRITICAL': '#ef4444',
                    'HIGH': '#f97316',
                    'MEDIUM': '#eab308',
                    'LOW': '#3b82f6'
                }
            },
            'dimension_distribution': {
                'type': 'bar',
                'data_key': 'dimension_distribution',
                'orientation': 'horizontal'
            },
            'quality_score_by_centro': {
                'type': 'bar',
                'data_key': 'quality_score',
                'sort': 'ascending',
                'color_thresholds': [
                    {'max': 50, 'color': '#ef4444'},
                    {'max': 70, 'color': '#f97316'},
                    {'max': 85, 'color': '#eab308'},
                    {'max': 100, 'color': '#22c55e'}
                ]
            }
        }
        
        # Agrupaciones disponibles
        grouping_options = [
            {'id': 'by_centro', 'label': 'Por Centro Gestor', 'field': 'nombre_centro_gestor'},
            {'id': 'by_severity', 'label': 'Por Severidad', 'field': 'max_severity'},
            {'id': 'by_priority', 'label': 'Por Prioridad', 'field': 'priority'},
            {'id': 'by_dimension', 'label': 'Por Dimensión ISO', 'field': 'dimension'},
            {'id': 'by_rule', 'label': 'Por Regla Violada', 'field': 'rule_id'}
        ]
        
        # Opciones de ordenamiento
        sort_options = [
            {'id': 'quality_asc', 'label': 'Calidad: Menor a Mayor', 'field': 'quality_score', 'direction': 'asc'},
            {'id': 'quality_desc', 'label': 'Calidad: Mayor a Menor', 'field': 'quality_score', 'direction': 'desc'},
            {'id': 'issues_desc', 'label': 'Problemas: Mayor a Menor', 'field': 'total_issues', 'direction': 'desc'},
            {'id': 'issues_asc', 'label': 'Problemas: Menor a Mayor', 'field': 'total_issues', 'direction': 'asc'},
            {'id': 'error_rate_desc', 'label': 'Tasa Error: Mayor a Menor', 'field': 'error_rate', 'direction': 'desc'},
            {'id': 'name_asc', 'label': 'Nombre: A-Z', 'field': 'nombre_centro_gestor', 'direction': 'asc'}
        ]
        
        # Paleta de colores para consistencia visual
        color_palette = {
            'severities': {
                'CRITICAL': {'bg': '#fee2e2', 'text': '#991b1b', 'border': '#ef4444'},
                'HIGH': {'bg': '#ffedd5', 'text': '#9a3412', 'border': '#f97316'},
                'MEDIUM': {'bg': '#fef3c7', 'text': '#854d0e', 'border': '#eab308'},
                'LOW': {'bg': '#dbeafe', 'text': '#1e40af', 'border': '#3b82f6'}
            },
            'priorities': {
                'P0': {'bg': '#fee2e2', 'text': '#991b1b', 'border': '#ef4444'},
                'P1': {'bg': '#ffedd5', 'text': '#9a3412', 'border': '#f97316'},
                'P2': {'bg': '#fef3c7', 'text': '#854d0e', 'border': '#eab308'},
                'P3': {'bg': '#d1fae5', 'text': '#065f46', 'border': '#22c55e'}
            },
            'statuses': {
                'EXCELENTE': {'bg': '#d1fae5', 'text': '#065f46', 'border': '#22c55e'},
                'BUENO': {'bg': '#dbeafe', 'text': '#1e40af', 'border': '#3b82f6'},
                'ACEPTABLE': {'bg': '#fef3c7', 'text': '#854d0e', 'border': '#eab308'},
                'DEFICIENTE': {'bg': '#ffedd5', 'text': '#9a3412', 'border': '#f97316'},
                'CRITICO': {'bg': '#fee2e2', 'text': '#991b1b', 'border': '#ef4444'}
            }
        }
        
        # Construir metadata completa
        metadata = {
            'report_id': self.report_id,
            'generated_at': self.report_timestamp,
            'version': '2.0',
            
            # Opciones de filtrado
            'filters': filter_options,
            
            # Rangos numéricos
            'ranges': ranges,
            
            # Configuraciones de componentes
            'tabs': tab_configurations,
            'tables': table_configurations,
            'charts': chart_configurations,
            
            # Opciones de UI
            'grouping': grouping_options,
            'sorting': sort_options,
            'colors': color_palette,
            
            # Contadores rápidos para badges
            'counts': {
                'total_centros': len(centro_reports),
                'total_records': summary_report.get('total_records_validated', 0),
                'records_with_issues': summary_report.get('records_with_issues', 0),
                'total_issues': summary_report.get('total_issues_found', 0),
                'centros_require_attention': summary_report.get('centros_require_attention', 0)
            },
            
            # Mapeo de íconos para UI
            'icons': {
                'severities': {
                    'CRITICAL': 'alert-circle',
                    'HIGH': 'alert-triangle',
                    'MEDIUM': 'info',
                    'LOW': 'check-circle'
                },
                'dimensions': {
                    'Consistencia Lógica': 'check-square',
                    'Completitud': 'clipboard-list',
                    'Exactitud Posicional': 'map-pin',
                    'Exactitud Temática': 'tag',
                    'Calidad Temporal': 'clock'
                }
            },
            
            # Textos de ayuda para tooltips
            'tooltips': {
                'quality_score': 'Score de calidad calculado según ISO 19157 (0-100). Mayor es mejor.',
                'error_rate': 'Porcentaje de registros con al menos un problema de calidad.',
                'severity': 'Nivel de impacto del problema: CRITICAL (bloquea uso), HIGH (impacto significativo), MEDIUM (impacto moderado), LOW (mejora recomendada).',
                'priority': 'Prioridad de corrección: P0 (inmediato), P1 (urgente), P2 (programado), P3 (cuando sea posible).',
                'dimensions': 'Dimensiones de calidad según ISO 19157: estándar internacional para calidad de datos geoespaciales.'
            }
        }
        
        return metadata
    
    def _sanitize_id(self, text: str) -> str:
        """Sanitiza un texto para usar como ID de documento."""
        # Manejar None o valores vacíos
        if not text or text is None:
            return "sin_centro_gestor"
        
        # Reemplazar caracteres especiales
        sanitized = str(text).lower()
        sanitized = sanitized.replace(' ', '_')
        sanitized = sanitized.replace('á', 'a').replace('é', 'e').replace('í', 'i')
        sanitized = sanitized.replace('ó', 'o').replace('ú', 'u').replace('ñ', 'n')
        sanitized = ''.join(c for c in sanitized if c.isalnum() or c == '_')
        
        return sanitized[:100]  # Limitar longitud
