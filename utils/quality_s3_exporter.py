# -*- coding: utf-8 -*-
"""
Exportador de Reportes de Calidad a S3
=======================================

Exporta reportes de control de calidad a AWS S3 en las carpetas:
- /reports/quality-control/ - Reportes detallados en JSON y Excel
- /logs/quality-control/ - Logs de validación y estadísticas

Author: ETL QA Team
Date: November 2025
Version: 1.0
"""

import sys
import os
import json
import gzip
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# Agregar paths necesarios
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.s3_uploader import S3Uploader


class QualityReportsS3Exporter:
    """
    Exporta reportes de control de calidad a AWS S3.
    Organiza los archivos en estructura jerárquica por fecha.
    """
    
    def __init__(self, credentials_file: str = "aws_credentials.json"):
        """
        Inicializa el exportador.
        
        Args:
            credentials_file: Ruta al archivo de credenciales AWS
        """
        self.s3_uploader = None
        self.credentials_file = credentials_file
        self.export_stats = {
            'files_uploaded': 0,
            'total_size_kb': 0,
            'errors': []
        }
    
    def _initialize_s3(self) -> bool:
        """Inicializa el cliente S3."""
        try:
            self.s3_uploader = S3Uploader(self.credentials_file)
            print("✓ Cliente S3 inicializado")
            return True
        except Exception as e:
            print(f"❌ Error inicializando S3: {e}")
            return False
    
    def export_all_reports(
        self,
        record_reports: List[Dict[str, Any]],
        centro_reports: List[Dict[str, Any]],
        summary_report: Dict[str, Any],
        validation_stats: Dict[str, Any],
        report_id: str,
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Exporta todos los reportes de calidad a S3.
        
        Args:
            record_reports: Lista de reportes por registro
            centro_reports: Lista de reportes por centro gestor
            summary_report: Reporte resumen
            validation_stats: Estadísticas de validación
            report_id: ID único del reporte
            verbose: Si True, imprime progreso
            
        Returns:
            Diccionario con estadísticas de exportación
        """
        if verbose:
            print(f"\n{'='*80}")
            print("☁️  EXPORTACIÓN DE REPORTES A S3")
            print(f"{'='*80}")
        
        # Inicializar S3
        if not self._initialize_s3():
            return {'error': 'No se pudo inicializar S3', **self.export_stats}
        
        # Obtener timestamp y paths
        timestamp = datetime.now()
        date_path = timestamp.strftime('%Y/%m/%d')
        time_str = timestamp.strftime('%H%M%S')
        
        # Crear directorio temporal local
        temp_dir = Path("temp_quality_reports")
        temp_dir.mkdir(exist_ok=True)
        
        try:
            # 1. Exportar reporte resumen (JSON)
            if verbose:
                print(f"\n📋 Exportando reporte resumen...")
            
            summary_file = temp_dir / f"summary_{report_id}.json"
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary_report, f, indent=2, ensure_ascii=False, default=str)
            
            s3_key = f"reports/quality-control/{date_path}/summary_{report_id}_{time_str}.json"
            self._upload_file(summary_file, s3_key, compress=True)
            
            # 2. Exportar reportes por registro (JSON completo)
            if verbose:
                print(f"\n📊 Exportando {len(record_reports)} reportes de registros...")
            
            records_file = temp_dir / f"records_{report_id}.json"
            with open(records_file, 'w', encoding='utf-8') as f:
                json.dump(record_reports, f, indent=2, ensure_ascii=False, default=str)
            
            s3_key = f"reports/quality-control/{date_path}/records_{report_id}_{time_str}.json"
            self._upload_file(records_file, s3_key, compress=True)
            
            # 3. Exportar reportes por centro gestor (JSON)
            if verbose:
                print(f"\n🏢 Exportando {len(centro_reports)} reportes de centros...")
            
            centros_file = temp_dir / f"centros_{report_id}.json"
            with open(centros_file, 'w', encoding='utf-8') as f:
                json.dump(centro_reports, f, indent=2, ensure_ascii=False, default=str)
            
            s3_key = f"reports/quality-control/{date_path}/centros_{report_id}_{time_str}.json"
            self._upload_file(centros_file, s3_key, compress=True)
            
            # 4. Exportar estadísticas de validación (log)
            if verbose:
                print(f"\n📈 Exportando estadísticas de validación...")
            
            stats_file = temp_dir / f"validation_stats_{report_id}.json"
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(validation_stats, f, indent=2, ensure_ascii=False, default=str)
            
            s3_key = f"logs/quality-control/{date_path}/validation_stats_{report_id}_{time_str}.json"
            self._upload_file(stats_file, s3_key, compress=True)
            
            # 5. Generar y exportar reporte en texto plano (log legible)
            if verbose:
                print(f"\n📝 Generando log de texto plano...")
            
            log_file = temp_dir / f"quality_report_{report_id}.txt"
            self._generate_text_log(
                log_file,
                summary_report,
                centro_reports,
                record_reports,
                validation_stats
            )
            
            s3_key = f"logs/quality-control/{date_path}/quality_report_{report_id}_{time_str}.txt"
            self._upload_file(log_file, s3_key, compress=False)
            
            # 6. Exportar reporte de registros críticos (CSV para revisión rápida)
            if verbose:
                print(f"\n🚨 Exportando registros críticos...")
            
            critical_records = [r for r in record_reports if r['max_severity'] in ['CRITICAL', 'HIGH']]
            if critical_records:
                csv_file = temp_dir / f"critical_records_{report_id}.csv"
                self._generate_critical_csv(csv_file, critical_records)
                
                s3_key = f"reports/quality-control/{date_path}/critical_records_{report_id}_{time_str}.csv"
                self._upload_file(csv_file, s3_key, compress=False)
            
            # 7. Actualizar archivo "latest" para acceso rápido
            if verbose:
                print(f"\n🔄 Actualizando referencias 'latest'...")
            
            # Summary latest
            s3_key_latest = "reports/quality-control/latest/summary.json"
            self._upload_file(summary_file, s3_key_latest, compress=True)
            
            # Stats latest
            s3_key_latest = "logs/quality-control/latest/validation_stats.json"
            self._upload_file(stats_file, s3_key_latest, compress=True)
            
            if verbose:
                self._print_export_summary()
            
            return self.export_stats
            
        except Exception as e:
            error_msg = f"Error durante exportación: {e}"
            print(f"❌ {error_msg}")
            self.export_stats['errors'].append(error_msg)
            import traceback
            traceback.print_exc()
            return self.export_stats
        
        finally:
            # Limpiar archivos temporales
            if temp_dir.exists():
                import shutil
                try:
                    shutil.rmtree(temp_dir)
                except:
                    pass
    
    def _upload_file(
        self,
        local_path: Path,
        s3_key: str,
        compress: bool = True
    ) -> bool:
        """
        Sube un archivo a S3.
        
        Args:
            local_path: Ruta local del archivo
            s3_key: Key en S3
            compress: Si True, comprime el archivo con gzip
            
        Returns:
            True si se subió exitosamente
        """
        try:
            success = self.s3_uploader.upload_file(
                local_path=local_path,
                s3_key=s3_key,
                compress=compress,
                metadata={
                    'content-type': 'application/json' if s3_key.endswith('.json') else 'text/plain',
                    'report-type': 'quality-control'
                }
            )
            
            if success:
                file_size = local_path.stat().st_size / 1024
                self.export_stats['files_uploaded'] += 1
                self.export_stats['total_size_kb'] += file_size
            
            return success
            
        except Exception as e:
            error_msg = f"Error subiendo {local_path.name}: {e}"
            self.export_stats['errors'].append(error_msg)
            return False
    
    def _generate_text_log(
        self,
        output_file: Path,
        summary: Dict[str, Any],
        centros: List[Dict[str, Any]],
        records: List[Dict[str, Any]],
        stats: Dict[str, Any]
    ):
        """Genera un log en texto plano legible."""
        with open(output_file, 'w', encoding='utf-8') as f:
            # Header
            f.write("="*100 + "\n")
            f.write("REPORTE DE CONTROL DE CALIDAD DE DATOS - ISO 19157\n")
            f.write("="*100 + "\n\n")
            
            f.write(f"Fecha: {summary['report_timestamp']}\n")
            f.write(f"Report ID: {summary['report_id']}\n")
            f.write(f"Estándar: {summary['iso_standard']}\n\n")
            
            # Resumen ejecutivo
            f.write("-" * 100 + "\n")
            f.write("RESUMEN EJECUTIVO\n")
            f.write("-" * 100 + "\n\n")
            
            f.write(f"Total de registros validados: {summary['total_records_validated']}\n")
            f.write(f"Registros con problemas: {summary['records_with_issues']} ({summary['error_rate']}%)\n")
            f.write(f"Registros sin problemas: {summary['records_without_issues']}\n")
            f.write(f"Total de problemas encontrados: {summary['total_issues_found']}\n")
            f.write(f"Score de calidad global: {summary['global_quality_score']:.2f}/100\n")
            f.write(f"Estado del sistema: {summary['system_status']}\n\n")
            
            # Distribución por severidad
            f.write("Distribución por Severidad:\n")
            for severity, count in summary['severity_distribution'].items():
                f.write(f"  {severity}: {count}\n")
            f.write("\n")
            
            # Distribución por dimensión ISO
            f.write("Distribución por Dimensión ISO 19157:\n")
            for dimension, count in summary['dimension_distribution'].items():
                f.write(f"  {dimension}: {count}\n")
            f.write("\n")
            
            # Centros gestores más problemáticos
            f.write("-" * 100 + "\n")
            f.write("CENTROS GESTORES MÁS PROBLEMÁTICOS\n")
            f.write("-" * 100 + "\n\n")
            
            for i, centro in enumerate(summary['top_problematic_centros'][:10], 1):
                f.write(f"{i}. {centro['nombre']}\n")
                f.write(f"   Problemas: {centro['issues']}, ")
                f.write(f"Error Rate: {centro['error_rate']:.1f}%, ")
                f.write(f"Quality Score: {centro['quality_score']:.1f}\n\n")
            
            # Recomendaciones
            f.write("-" * 100 + "\n")
            f.write("RECOMENDACIONES\n")
            f.write("-" * 100 + "\n\n")
            
            for i, rec in enumerate(summary['recommendations'], 1):
                f.write(f"{i}. [{rec['priority']}] {rec['category']}\n")
                f.write(f"   {rec['recommendation']}\n\n")
            
            # Registros críticos
            critical_count = sum(1 for r in records if r['max_severity'] == 'CRITICAL')
            if critical_count > 0:
                f.write("-" * 100 + "\n")
                f.write(f"REGISTROS CRÍTICOS ({critical_count})\n")
                f.write("-" * 100 + "\n\n")
                
                critical_records = [r for r in records if r['max_severity'] == 'CRITICAL'][:20]
                for rec in critical_records:
                    f.write(f"• UPID: {rec['upid']} - {rec['nombre_up']}\n")
                    f.write(f"  Centro: {rec['nombre_centro_gestor']}\n")
                    f.write(f"  Problemas: {rec['total_issues']}\n")
                    f.write(f"  Campos afectados: {', '.join(rec['affected_fields'][:5])}\n\n")
            
            # Footer
            f.write("="*100 + "\n")
            f.write("FIN DEL REPORTE\n")
            f.write("="*100 + "\n")
    
    def _generate_critical_csv(
        self,
        output_file: Path,
        critical_records: List[Dict[str, Any]]
    ):
        """Genera un CSV con registros críticos para revisión rápida."""
        import csv
        
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = [
                'UPID', 'Nombre UP', 'Centro Gestor', 'Total Problemas',
                'Severidad Máxima', 'Prioridad', 'Campos Afectados', 'Acción Inmediata'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for rec in critical_records:
                writer.writerow({
                    'UPID': rec['upid'],
                    'Nombre UP': rec['nombre_up'],
                    'Centro Gestor': rec['nombre_centro_gestor'],
                    'Total Problemas': rec['total_issues'],
                    'Severidad Máxima': rec['max_severity'],
                    'Prioridad': rec['priority'],
                    'Campos Afectados': ', '.join(rec['affected_fields'][:5]),
                    'Acción Inmediata': 'SÍ' if rec['requires_immediate_action'] else 'NO'
                })
    
    def _print_export_summary(self):
        """Imprime resumen de la exportación."""
        print(f"\n{'='*80}")
        print("📊 RESUMEN DE EXPORTACIÓN A S3")
        print(f"{'='*80}")
        
        print(f"\n✅ Archivos exportados: {self.export_stats['files_uploaded']}")
        print(f"📦 Tamaño total: {self.export_stats['total_size_kb']:.1f} KB")
        
        if self.export_stats['errors']:
            print(f"\n⚠️ Errores:")
            for error in self.export_stats['errors']:
                print(f"  • {error}")
        else:
            print(f"\n🎉 Exportación completada sin errores!")
        
        print(f"\n📂 Estructura en S3:")
        print(f"  • reports/quality-control/YYYY/MM/DD/ - Reportes detallados")
        print(f"  • reports/quality-control/latest/ - Acceso rápido al último reporte")
        print(f"  • logs/quality-control/YYYY/MM/DD/ - Logs de validación")
        print(f"  • logs/quality-control/latest/ - Estadísticas actuales")
        
        print("="*80)


def export_quality_reports_to_s3(
    record_reports: List[Dict[str, Any]],
    centro_reports: List[Dict[str, Any]],
    summary_report: Dict[str, Any],
    validation_stats: Dict[str, Any],
    report_id: str,
    credentials_file: str = "aws_credentials.json",
    verbose: bool = True,
    categorical_metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Función de conveniencia para exportar reportes de calidad a S3.
    
    Args:
        record_reports: Lista de reportes por registro
        centro_reports: Lista de reportes por centro gestor
        summary_report: Reporte resumen
        validation_stats: Estadísticas de validación
        report_id: ID único del reporte
        credentials_file: Ruta al archivo de credenciales AWS
        verbose: Si True, imprime progreso
        categorical_metadata: Metadata categórica para componentes Next.js (opcional)
        
    Returns:
        Diccionario con estadísticas de exportación
    """
    exporter = QualityReportsS3Exporter(credentials_file)
    
    stats = exporter.export_all_reports(
        record_reports=record_reports,
        centro_reports=centro_reports,
        summary_report=summary_report,
        validation_stats=validation_stats,
        report_id=report_id,
        verbose=verbose
    )
    
    # Exportar metadata categórica si está disponible
    if categorical_metadata:
        try:
            metadata_key = f"quality-control/reports/{report_id}/categorical_metadata.json"
            metadata_uploaded = exporter.uploader.upload_json_object(
                categorical_metadata,
                metadata_key
            )
            
            if metadata_uploaded and verbose:
                print(f"   ✓ Metadata categórica exportada a S3")
            
            stats['metadata_uploaded'] = metadata_uploaded
        except Exception as e:
            if verbose:
                print(f"   ⚠️ Error exportando metadata: {e}")
            stats['metadata_uploaded'] = False
    
    return stats


if __name__ == "__main__":
    """
    Prueba del exportador de reportes.
    """
    print("🧪 Prueba del exportador de reportes de calidad")
    print("="*80)
    print("\nEste módulo debe ser usado a través de la pipeline de calidad.")
    print("No debe ejecutarse directamente.")
    print("\nUso correcto:")
    print("  from utils.quality_s3_exporter import export_quality_reports_to_s3")
    print("  export_quality_reports_to_s3(records, centros, summary, stats, report_id)")
