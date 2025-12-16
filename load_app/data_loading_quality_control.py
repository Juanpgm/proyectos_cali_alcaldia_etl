# -*- coding: utf-8 -*-
"""
Cargador de Datos de Control de Calidad a Firebase
===================================================

Carga reportes de control de calidad a Firebase Firestore en 3 colecciones:
1. quality_control_records - Detalle por registro individual
2. quality_control_by_centro_gestor - Agregado por centro gestor
3. quality_control_summary - Resumen general del sistema

Author: ETL QA Team
Date: November 2025
Version: 1.0
"""

import sys
import os
from typing import Dict, List, Any, Optional
from datetime import datetime

# Agregar paths necesarios
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.config import get_firestore_client, secure_log


class QualityControlFirebaseLoader:
    """
    Carga reportes de control de calidad a Firebase Firestore.
    Maneja 3 colecciones independientes con upsert inteligente.
    """
    
    # Nombres de colecciones
    COLLECTION_RECORDS = "unidades_proyecto_quality_control_records"
    COLLECTION_CENTROS = "unidades_proyecto_quality_control_by_centro_gestor"
    COLLECTION_SUMMARY = "unidades_proyecto_quality_control_summary"
    COLLECTION_METADATA = "unidades_proyecto_quality_control_metadata"
    COLLECTION_CHANGELOG = "unidades_proyecto_quality_control_changelog"
    
    def __init__(self, batch_size: int = 100):
        """
        Inicializa el cargador.
        
        Args:
            batch_size: Tamaño de lote para operaciones batch
        """
        self.batch_size = batch_size
        self.db = None
        self.load_stats = {
            'records_loaded': 0,
            'centros_loaded': 0,
            'summary_loaded': 0,
            'records_updated': 0,
            'records_created': 0,
            'errors': []
        }
    
    def _initialize_firestore(self) -> bool:
        """Inicializa conexión a Firestore."""
        try:
            self.db = get_firestore_client()
            if not self.db:
                print("❌ No se pudo conectar a Firestore")
                return False
            print("✓ Conexión a Firestore establecida")
            return True
        except Exception as e:
            print(f"❌ Error conectando a Firestore: {e}")
            return False
    
    def load_all_reports(
        self,
        record_reports: List[Dict[str, Any]],
        centro_reports: List[Dict[str, Any]],
        summary_report: Dict[str, Any],
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Carga todos los reportes de control de calidad a Firebase.
        
        Args:
            record_reports: Lista de reportes por registro
            centro_reports: Lista de reportes por centro gestor
            summary_report: Reporte resumen general
            verbose: Si True, imprime progreso detallado
            
        Returns:
            Diccionario con estadísticas de carga
        """
        if verbose:
            print(f"\n{'='*80}")
            print("📤 CARGA DE REPORTES DE CALIDAD A FIREBASE")
            print(f"{'='*80}")
        
        # Inicializar conexión
        if not self._initialize_firestore():
            return {'error': 'No se pudo conectar a Firestore', **self.load_stats}
        
        # Cargar reportes por registro
        if record_reports:
            if verbose:
                print(f"\n📊 Cargando {len(record_reports)} reportes de registros...")
            success = self._load_record_reports(record_reports, verbose)
            if not success:
                print("⚠️ Advertencia: Problemas cargando reportes de registros")
        
        # Cargar reportes por centro gestor
        if centro_reports:
            if verbose:
                print(f"\n🏢 Cargando {len(centro_reports)} reportes de centros gestores...")
            success = self._load_centro_reports(centro_reports, verbose)
            if not success:
                print("⚠️ Advertencia: Problemas cargando reportes de centros")
        
        # Cargar reporte resumen
        if summary_report:
            if verbose:
                print(f"\n📋 Cargando reporte resumen general...")
            success = self._load_summary_report(summary_report, verbose)
            if not success:
                print("⚠️ Advertencia: Problemas cargando reporte resumen")
        
        # Mostrar resultados
        if verbose:
            self._print_load_summary()
        
        return self.load_stats
    
    def _load_record_reports(
        self,
        reports: List[Dict[str, Any]],
        verbose: bool = True
    ) -> bool:
        """
        Carga reportes de registros individuales a Firebase con UPSERT inteligente.
        Detecta cambios y registra en changelog.
        
        Args:
            reports: Lista de reportes por registro
            verbose: Si True, imprime progreso
            
        Returns:
            True si se cargó exitosamente
        """
        try:
            collection_ref = self.db.collection(self.COLLECTION_RECORDS)
            changelog_ref = self.db.collection(self.COLLECTION_CHANGELOG)
            
            # Procesar en lotes
            total_processed = 0
            records_created = 0
            records_updated = 0
            
            for i in range(0, len(reports), self.batch_size):
                batch = self.db.batch()
                batch_reports = reports[i:i + self.batch_size]
                
                for report in batch_reports:
                    doc_id = report['document_id']
                    doc_ref = collection_ref.document(doc_id)
                    
                    # Verificar si existe documento anterior
                    existing_doc = doc_ref.get()
                    
                    # Preparar nuevo documento
                    doc_data = self._prepare_record_document(report)
                    
                    if existing_doc.exists:
                        # UPSERT: Actualizar solo si hay cambios
                        old_data = existing_doc.to_dict()
                        changes = self._detect_changes(old_data, doc_data, ['upid', 'nombre_up'])
                        
                        if changes:
                            # Hay cambios, actualizar
                            batch.set(doc_ref, doc_data)
                            records_updated += 1
                            
                            # Registrar en changelog
                            changelog_entry = {
                                'collection': self.COLLECTION_RECORDS,
                                'document_id': doc_id,
                                'upid': report.get('upid'),
                                'action': 'updated',
                                'changes': changes,
                                'old_report_id': old_data.get('report_id'),
                                'new_report_id': report.get('report_id'),
                                'timestamp': datetime.now().isoformat()
                            }
                            changelog_doc_ref = changelog_ref.document()
                            batch.set(changelog_doc_ref, changelog_entry)
                        # Si no hay cambios, no hacer nada (optimización)
                    else:
                        # Documento nuevo, crear
                        batch.set(doc_ref, doc_data)
                        records_created += 1
                        
                        # Registrar en changelog
                        changelog_entry = {
                            'collection': self.COLLECTION_RECORDS,
                            'document_id': doc_id,
                            'upid': report.get('upid'),
                            'action': 'created',
                            'report_id': report.get('report_id'),
                            'timestamp': datetime.now().isoformat()
                        }
                        changelog_doc_ref = changelog_ref.document()
                        batch.set(changelog_doc_ref, changelog_entry)
                    
                    total_processed += 1
                
                # Commit batch
                batch.commit()
                
                if verbose and (i + self.batch_size) % 500 == 0:
                    print(f"  Procesados: {min(i + self.batch_size, len(reports))}/{len(reports)}")
            
            self.load_stats['records_loaded'] = total_processed
            self.load_stats['records_created'] = records_created
            self.load_stats['records_updated'] = records_updated
            
            if verbose:
                print(f"  ✓ {total_processed} reportes procesados")
                print(f"    • Nuevos: {records_created}")
                print(f"    • Actualizados: {records_updated}")
                print(f"    • Sin cambios: {total_processed - records_created - records_updated}")
            
            return True
            
        except Exception as e:
            error_msg = f"Error cargando reportes de registros: {e}"
            print(f"  ✗ {error_msg}")
            self.load_stats['errors'].append(error_msg)
            import traceback
            traceback.print_exc()
            return False
    
    def _load_centro_reports(
        self,
        reports: List[Dict[str, Any]],
        verbose: bool = True
    ) -> bool:
        """
        Carga reportes por centro gestor a Firebase.
        
        Args:
            reports: Lista de reportes por centro
            verbose: Si True, imprime progreso
            
        Returns:
            True si se cargó exitosamente
        """
        try:
            collection_ref = self.db.collection(self.COLLECTION_CENTROS)
            
            # Procesar en lotes
            total_processed = 0
            
            for i in range(0, len(reports), self.batch_size):
                batch = self.db.batch()
                batch_reports = reports[i:i + self.batch_size]
                
                for report in batch_reports:
                    doc_id = report['document_id']
                    doc_ref = collection_ref.document(doc_id)
                    
                    # Preparar documento
                    doc_data = self._prepare_centro_document(report)
                    
                    # Upsert con batch
                    batch.set(doc_ref, doc_data, merge=True)
                    total_processed += 1
                
                # Commit batch
                batch.commit()
                
                if verbose and total_processed % 50 == 0:
                    print(f"  Procesados: {total_processed}/{len(reports)}")
            
            self.load_stats['centros_loaded'] = total_processed
            
            if verbose:
                print(f"  ✓ {total_processed} reportes de centros gestores cargados")
            
            return True
            
        except Exception as e:
            error_msg = f"Error cargando reportes de centros: {e}"
            print(f"  ✗ {error_msg}")
            self.load_stats['errors'].append(error_msg)
            return False
    
    def _load_summary_report(
        self,
        report: Dict[str, Any],
        verbose: bool = True
    ) -> bool:
        """
        Carga reporte resumen general a Firebase.
        
        Args:
            report: Reporte resumen
            verbose: Si True, imprime progreso
            
        Returns:
            True si se cargó exitosamente
        """
        try:
            collection_ref = self.db.collection(self.COLLECTION_SUMMARY)
            
            # Usar report_id como documento ID
            doc_id = report['document_id']
            doc_ref = collection_ref.document(doc_id)
            
            # Preparar documento
            doc_data = self._prepare_summary_document(report)
            
            # Guardar
            doc_ref.set(doc_data)
            
            # También actualizar documento "latest" para acceso rápido
            latest_ref = collection_ref.document('latest')
            latest_ref.set(doc_data)
            
            self.load_stats['summary_loaded'] = 1
            
            if verbose:
                print(f"  ✓ Reporte resumen cargado (ID: {doc_id})")
            
            return True
            
        except Exception as e:
            error_msg = f"Error cargando reporte resumen: {e}"
            print(f"  ✗ {error_msg}")
            self.load_stats['errors'].append(error_msg)
            return False
    
    def _prepare_record_document(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepara un documento de registro para Firebase.
        Asegura que todos los campos sean compatibles con Firestore.
        """
        doc = {
            # IDs y referencias
            'upid': report.get('upid'),
            'nombre_up': report.get('nombre_up'),
            'nombre_centro_gestor': report.get('nombre_centro_gestor'),
            'record_index': report.get('record_index'),
            
            # Metadata del reporte
            'report_id': report.get('report_id'),
            'report_timestamp': report.get('report_timestamp'),
            
            # Estadísticas
            'total_issues': report.get('total_issues', 0),
            'max_severity': report.get('max_severity'),
            'severity_counts': report.get('severity_counts', {}),
            'dimension_counts': report.get('dimension_counts', {}),
            'affected_fields': report.get('affected_fields', []),
            'affected_fields_count': report.get('affected_fields_count', 0),
            
            # Clasificación
            'priority': report.get('priority'),
            'requires_immediate_action': report.get('requires_immediate_action', False),
            
            # Problemas detallados (limitar a primeros 50 por rendimiento)
            'issues': report.get('issues', [])[:50],
            'has_more_issues': len(report.get('issues', [])) > 50,
            
            # Timestamps
            'created_at': report.get('created_at'),
            'updated_at': datetime.now().isoformat()
        }
        
        return doc
    
    def _prepare_centro_document(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepara un documento de centro gestor para Firebase.
        """
        doc = {
            # Identificación
            'nombre_centro_gestor': report.get('nombre_centro_gestor'),
            
            # Metadata del reporte
            'report_id': report.get('report_id'),
            'report_timestamp': report.get('report_timestamp'),
            
            # Estadísticas generales
            'total_records': report.get('total_records', 0),
            'records_with_issues': report.get('records_with_issues', 0),
            'records_without_issues': report.get('records_without_issues', 0),
            'error_rate': report.get('error_rate', 0),
            'quality_score': report.get('quality_score', 0),
            
            # Conteo de problemas
            'total_issues': report.get('total_issues', 0),
            'severity_counts': report.get('severity_counts', {}),
            'dimension_counts': report.get('dimension_counts', {}),
            
            # Top problemas
            'top_problematic_fields': report.get('top_problematic_fields', [])[:10],
            'top_violated_rules': report.get('top_violated_rules', [])[:10],
            
            # Registros afectados (limitar para no exceder tamaño de documento)
            'affected_records_sample': report.get('affected_records', [])[:50],
            'affected_records_count': report.get('affected_records_count', 0),
            
            # Estado y clasificación
            'status': report.get('status'),
            'requires_attention': report.get('requires_attention', False),
            
            # Timestamps
            'created_at': report.get('created_at'),
            'updated_at': datetime.now().isoformat()
        }
        
        return doc
    
    def _prepare_summary_document(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepara el documento resumen para Firebase.
        """
        doc = {
            # Metadata
            'report_id': report.get('report_id'),
            'report_timestamp': report.get('report_timestamp'),
            'report_type': report.get('report_type'),
            
            # Estadísticas globales
            'total_records_validated': report.get('total_records_validated', 0),
            'records_with_issues': report.get('records_with_issues', 0),
            'records_without_issues': report.get('records_without_issues', 0),
            'error_rate': report.get('error_rate', 0),
            'total_issues_found': report.get('total_issues_found', 0),
            'global_quality_score': report.get('global_quality_score', 0),
            
            # Distribuciones
            'severity_distribution': report.get('severity_distribution', {}),
            'dimension_distribution': report.get('dimension_distribution', {}),
            
            # Centros gestores
            'total_centros_gestores': report.get('total_centros_gestores', 0),
            'centros_require_attention': report.get('centros_require_attention', 0),
            
            # Top centros
            'top_problematic_centros': report.get('top_problematic_centros', [])[:10],
            'top_quality_centros': report.get('top_quality_centros', [])[:10],
            
            # Recomendaciones
            'recommendations': report.get('recommendations', []),
            
            # Estado del sistema
            'system_status': report.get('system_status'),
            'requires_immediate_action': report.get('requires_immediate_action', False),
            
            # Información adicional
            'validation_engine_version': report.get('validation_engine_version'),
            'iso_standard': report.get('iso_standard'),
            'dimensions_evaluated': report.get('dimensions_evaluated', []),
            
            # Comparación con reporte anterior
            'comparison_with_previous': report.get('comparison_with_previous'),
            
            # Timestamps
            'created_at': report.get('created_at'),
            'updated_at': datetime.now().isoformat()
        }
        
        return doc
    
    def _detect_changes(
        self,
        old_data: Dict[str, Any],
        new_data: Dict[str, Any],
        key_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Detecta cambios entre documento antiguo y nuevo.
        
        Args:
            old_data: Datos del documento existente
            new_data: Datos del nuevo documento
            key_fields: Campos clave que identifican el registro
            
        Returns:
            Diccionario con cambios detectados o None si no hay cambios
        """
        changes = {}
        
        # Campos importantes a monitorear
        monitor_fields = [
            'total_issues', 'max_severity', 'priority', 'quality_score',
            'error_rate', 'requires_immediate_action', 'severity_counts'
        ]
        
        for field in monitor_fields:
            old_value = old_data.get(field)
            new_value = new_data.get(field)
            
            # Comparar valores
            if old_value != new_value:
                changes[field] = {
                    'old': old_value,
                    'new': new_value
                }
        
        return changes if changes else None
    
    def _print_load_summary(self):
        """Imprime resumen de la carga."""
        print(f"\n{'='*80}")
        print("📊 RESUMEN DE CARGA A FIREBASE")
        print(f"{'='*80}")
        
        print(f"\n✅ Documentos procesados:")
        print(f"  • Reportes de registros: {self.load_stats['records_loaded']}")
        if self.load_stats.get('records_created') or self.load_stats.get('records_updated'):
            print(f"    - Nuevos: {self.load_stats.get('records_created', 0)}")
            print(f"    - Actualizados: {self.load_stats.get('records_updated', 0)}")
        print(f"  • Reportes de centros gestores: {self.load_stats['centros_loaded']}")
        print(f"  • Reportes resumen: {self.load_stats['summary_loaded']}")
        
        total_loaded = (
            self.load_stats['records_loaded'] + 
            self.load_stats['centros_loaded'] + 
            self.load_stats['summary_loaded']
        )
        print(f"\n  📦 Total: {total_loaded} documentos")
        
        if self.load_stats['errors']:
            print(f"\n⚠️ Errores encontrados:")
            for error in self.load_stats['errors']:
                print(f"  • {error}")
        else:
            print(f"\n🎉 Carga completada sin errores!")
        
        print(f"\n💾 Colecciones actualizadas:")
        print(f"  • {self.COLLECTION_RECORDS}")
        print(f"  • {self.COLLECTION_CENTROS}")
        print(f"  • {self.COLLECTION_SUMMARY}")
        
        print("="*80)


def load_quality_reports_to_firebase(
    record_reports: List[Dict[str, Any]],
    centro_reports: List[Dict[str, Any]],
    summary_report: Dict[str, Any],
    batch_size: int = 100,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Función de conveniencia para cargar reportes de calidad a Firebase.
    
    Args:
        record_reports: Lista de reportes por registro
        centro_reports: Lista de reportes por centro gestor
        summary_report: Reporte resumen general
        batch_size: Tamaño de lote para operaciones batch
        verbose: Si True, imprime progreso
        
    Returns:
        Diccionario con estadísticas de carga
    """
    loader = QualityControlFirebaseLoader(batch_size=batch_size)
    
    stats = loader.load_all_reports(
        record_reports=record_reports,
        centro_reports=centro_reports,
        summary_report=summary_report,
        verbose=verbose
    )
    
    return stats


if __name__ == "__main__":
    """
    Prueba del cargador de control de calidad.
    """
    print("🧪 Prueba del cargador de control de calidad")
    print("="*80)
    print("\nEste módulo debe ser usado a través de la pipeline de calidad.")
    print("No debe ejecutarse directamente.")
    print("\nUso correcto:")
    print("  from load_app.data_loading_quality_control import load_quality_reports_to_firebase")
    print("  load_quality_reports_to_firebase(records, centros, summary)")
