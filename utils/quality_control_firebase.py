# -*- coding: utf-8 -*-
"""
Control de Calidad sobre Colección Completa de Firebase
=======================================================

Lee todos los datos de Firebase y ejecuta control de calidad sobre el conjunto completo.
Optimizado para reportes detallados y compatible con Next.js frontend.

Author: ETL QA Team
Date: November 2025
Version: 2.0
"""

import json
import tempfile
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

from database.config import get_firestore_client
from utils.quality_control import validate_geojson
from utils.quality_reporter import QualityReporter
from utils.quality_s3_exporter import export_quality_reports_to_s3
from load_app.data_loading_quality_control import load_quality_reports_to_firebase


def fetch_all_data_from_firebase(collection_name: str = "unidades_proyecto") -> Optional[Dict[str, Any]]:
    """
    Obtiene TODOS los datos de Firebase y los convierte a GeoJSON.
    
    Args:
        collection_name: Nombre de la colección en Firebase
        
    Returns:
        GeoJSON completo con todos los registros
    """
    try:
        print(f"\n📥 Obteniendo datos completos desde Firebase...")
        print(f"   Colección: {collection_name}")
        
        db = get_firestore_client()
        if not db:
            print("❌ No se pudo conectar a Firebase")
            return None
        
        # Obtener todos los documentos
        docs = db.collection(collection_name).stream()
        
        features = []
        for doc in docs:
            data = doc.to_dict()
            
            # Convertir a formato GeoJSON Feature
            feature = {
                "type": "Feature",
                "properties": {k: v for k, v in data.items() if k != 'geometry'},
                "geometry": data.get('geometry')
            }
            features.append(feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        print(f"   ✓ Obtenidos {len(features)} registros")
        return geojson
        
    except Exception as e:
        print(f"❌ Error obteniendo datos de Firebase: {e}")
        import traceback
        traceback.print_exc()
        return None


def get_previous_summary_from_firebase() -> Optional[Dict[str, Any]]:
    """
    Obtiene el summary del reporte de calidad anterior de Firebase.
    
    Returns:
        Summary anterior o None si no existe
    """
    try:
        db = get_firestore_client()
        if not db:
            return None
        
        # Buscar el documento más reciente en la colección de summaries
        summaries = (
            db.collection('unidades_proyecto_quality_control_summary')
            .order_by('created_at', direction='DESCENDING')
            .limit(1)
            .stream()
        )
        
        for doc in summaries:
            return doc.to_dict()
        
        return None
        
    except Exception as e:
        print(f"⚠️ No se pudo obtener summary anterior: {e}")
        return None


def calculate_comparison(current: Dict[str, Any], previous: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calcula la diferencia entre el reporte actual y el anterior.
    
    Args:
        current: Métricas del reporte actual
        previous: Métricas del reporte anterior (puede ser None)
        
    Returns:
        Diccionario con las comparaciones
    """
    if not previous:
        return {
            'has_previous': False,
            'previous_report_id': None,
            'previous_timestamp': None,
            'changes': {}
        }
    
    def calc_change(curr_val, prev_val, higher_is_better=True):
        """Calcula el cambio entre dos valores."""
        if prev_val is None or curr_val is None:
            return {'value': curr_val, 'previous': prev_val, 'change': None, 'trend': 'neutral'}
        
        change = curr_val - prev_val
        if change > 0:
            trend = 'improved' if higher_is_better else 'worsened'
        elif change < 0:
            trend = 'worsened' if higher_is_better else 'improved'
        else:
            trend = 'stable'
        
        return {
            'value': curr_val,
            'previous': prev_val,
            'change': round(change, 2),
            'change_percentage': round((change / prev_val * 100), 2) if prev_val != 0 else 0,
            'trend': trend
        }
    
    # Comparar métricas principales
    comparison = {
        'has_previous': True,
        'previous_report_id': previous.get('report_id'),
        'previous_timestamp': previous.get('created_at'),
        'changes': {
            'quality_score': calc_change(
                current.get('global_quality_score', 0),
                previous.get('global_quality_score', 0),
                higher_is_better=True
            ),
            'error_rate': calc_change(
                current.get('error_rate', 0),
                previous.get('error_rate', 0),
                higher_is_better=False
            ),
            'total_issues': calc_change(
                current.get('total_issues_found', 0),
                previous.get('total_issues_found', 0),
                higher_is_better=False
            ),
            'records_with_issues': calc_change(
                current.get('records_with_issues', 0),
                previous.get('records_with_issues', 0),
                higher_is_better=False
            ),
            'total_records': calc_change(
                current.get('total_records_validated', 0),
                previous.get('total_records_validated', 0),
                higher_is_better=True
            ),
            'centros_require_attention': calc_change(
                current.get('centros_require_attention', 0),
                previous.get('centros_require_attention', 0),
                higher_is_better=False
            )
        },
        'severity_changes': {}
    }
    
    # Comparar distribución por severidad
    curr_severity = current.get('severity_distribution', {})
    prev_severity = previous.get('severity_distribution', {})
    
    for severity in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO']:
        comparison['severity_changes'][severity] = calc_change(
            curr_severity.get(severity, 0),
            prev_severity.get(severity, 0),
            higher_is_better=False
        )
    
    return comparison


def run_quality_control_on_firebase_data(
    collection_name: str = "unidades_proyecto",
    enable_firebase_upload: bool = True,
    enable_s3_upload: bool = True,
    verbose: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Ejecuta control de calidad sobre TODOS los datos en Firebase.
    
    Este método:
    1. Descarga todos los datos de Firebase
    2. Los valida según ISO 19157
    3. Genera reportes optimizados para Next.js
    4. Carga reportes a Firebase y S3
    
    Args:
        collection_name: Colección de Firebase a validar
        enable_firebase_upload: Si True, carga reportes a Firebase
        enable_s3_upload: Si True, exporta reportes a S3
        verbose: Si True, muestra output detallado
        
    Returns:
        Diccionario con estadísticas completas de calidad
    """
    try:
        if verbose:
            print("\n" + "="*100)
            print("🔍 CONTROL DE CALIDAD - DATOS COMPLETOS DE FIREBASE")
            print("="*100)
        
        # 1. Obtener datos completos de Firebase
        geojson_data = fetch_all_data_from_firebase(collection_name)
        
        if not geojson_data or not geojson_data.get('features'):
            print("❌ No se pudieron obtener datos de Firebase")
            return None
        
        # 2. Guardar temporalmente para validación
        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False, encoding='utf-8') as tmp:
            json.dump(geojson_data, tmp, ensure_ascii=False)
            tmp_path = tmp.name
        
        if verbose:
            print(f"\n📋 Ejecutando validaciones ISO 19157...")
            print(f"   Total de registros: {len(geojson_data['features'])}")
        
        # 3. Validar datos
        validation_result = validate_geojson(tmp_path, verbose=False)
        
        # Limpiar archivo temporal
        try:
            Path(tmp_path).unlink()
        except:
            pass
        
        if not validation_result or 'issues' not in validation_result:
            print("⚠️ No se pudieron generar reportes de calidad")
            return None
        
        stats = validation_result.get('statistics', {})
        
        if verbose:
            print(f"   ✓ Validación completada")
            print(f"   ✓ Registros con problemas: {validation_result['records_with_issues']}")
            print(f"   ✓ Total de problemas: {validation_result['total_issues']}")
            print(f"   ✓ Quality Score: {stats.get('quality_score', 0):.2f}/100")
        
        # 4. Generar reportes optimizados para Next.js
        if verbose:
            print(f"\n📊 Generando reportes optimizados para Next.js...")
        
        reporter = QualityReporter()
        
        # Reporte por registro (con paginación en mente)
        record_reports = reporter.generate_record_level_report(validation_result['issues'])
        
        # Contar totales por centro gestor
        total_by_centro = {}
        for feature in geojson_data['features']:
            centro = feature.get('properties', {}).get('nombre_centro_gestor') or 'Sin Centro Gestor'
            total_by_centro[centro] = total_by_centro.get(centro, 0) + 1
        
        # Reporte por centro gestor (para dashboard)
        centro_reports = reporter.generate_centro_gestor_report(
            validation_result['issues'],
            total_by_centro
        )
        
        # Reporte resumen (para métricas globales)
        summary_report = reporter.generate_summary_report(
            record_reports,
            centro_reports,
            validation_result['total_records'],
            validation_result['statistics']
        )
        
        # Obtener reporte anterior y calcular comparación
        try:
            previous_summary = get_previous_summary_from_firebase()
            if previous_summary:
                comparison = calculate_comparison(summary_report, previous_summary)
                summary_report = reporter.add_comparison_to_summary(summary_report, comparison)
                if verbose:
                    print(f"\n📊 Comparación con reporte anterior:")
                    print(f"   - Reporte anterior: {previous_summary.get('report_id', 'N/A')}")
                    print(f"   - Cambio en Quality Score: {comparison.get('quality_score', {}).get('change', 0):+.2f}%")
                    print(f"   - Tendencia: {comparison.get('overall_trend', 'N/A')}")
            else:
                if verbose:
                    print(f"\n📊 Sin reporte anterior para comparar (primer reporte)")
        except Exception as e:
            if verbose:
                print(f"\n⚠️ Error al calcular comparación: {e}")
        
        # Metadata categórica (para componentes Next.js)
        categorical_metadata = reporter.generate_categorical_metadata(
            record_reports,
            centro_reports,
            summary_report
        )
        
        if verbose:
            print(f"   ✓ Reportes por registro: {len(record_reports)}")
            print(f"   ✓ Reportes por centro gestor: {len(centro_reports)}")
            print(f"   ✓ Reporte resumen generado")
            print(f"   ✓ Metadata categórica generada")
            print(f"   ✓ Report ID: {reporter.report_id}")
        
        # 5. Cargar a Firebase (colecciones para Next.js)
        if enable_firebase_upload:
            if verbose:
                print(f"\n🔥 Cargando reportes a Firebase...")
                print(f"   - quality_control_records (registros individuales)")
                print(f"   - quality_control_by_centro_gestor (agregados)")
                print(f"   - quality_control_summary (métricas globales)")
                print(f"   - quality_control_metadata (metadata categórica)")
            
            firebase_stats = load_quality_reports_to_firebase(
                record_reports=record_reports,
                centro_reports=centro_reports,
                summary_report=summary_report,
                batch_size=100,
                verbose=False
            )
            
            # Cargar metadata categórica
            try:
                db = get_firestore_client()
                metadata_doc_id = f"metadata_{reporter.report_id}"
                db.collection('unidades_proyecto_quality_control_metadata').document(metadata_doc_id).set(categorical_metadata)
                
                if verbose:
                    print(f"   ✓ Registros: {firebase_stats.get('records_loaded', 0)} docs")
                    print(f"   ✓ Centros: {firebase_stats.get('centros_loaded', 0)} docs")
                    print(f"   ✓ Summary: {'✓' if firebase_stats.get('summary_loaded') else '✗'}")
                    print(f"   ✓ Metadata: ✓")
            except Exception as e:
                if verbose:
                    print(f"   ⚠️ Error cargando metadata: {e}")
        
        # 6. Exportar a S3 (backup y análisis)
        if enable_s3_upload:
            if verbose:
                print(f"\n☁️  Exportando reportes a S3...")
            
            try:
                s3_stats = export_quality_reports_to_s3(
                    record_reports=record_reports,
                    centro_reports=centro_reports,
                    summary_report=summary_report,
                    validation_stats=validation_result['statistics'],
                    report_id=reporter.report_id,
                    categorical_metadata=categorical_metadata,
                    verbose=False
                )
                
                if verbose and s3_stats:
                    print(f"   ✓ Archivos exportados: {s3_stats.get('files_uploaded', 0)}")
                    if s3_stats.get('metadata_uploaded'):
                        print(f"   ✓ Metadata categórica exportada")
            except Exception as e:
                if verbose:
                    print(f"   ⚠️ Error en S3: {e}")
        
        # 7. Preparar resultado final
        result = {
            'report_id': reporter.report_id,
            'collection_name': collection_name,
            'total_records': validation_result['total_records'],
            'records_with_issues': validation_result['records_with_issues'],
            'records_without_issues': validation_result['total_records'] - validation_result['records_with_issues'],
            'total_issues': validation_result['total_issues'],
            'quality_score': stats.get('quality_score', 0),
            'severity_distribution': stats.get('by_severity', {}),
            'dimension_distribution': stats.get('by_dimension', {}),
            'top_problematic_centros': [
                {
                    'nombre': c['nombre_centro_gestor'],
                    'total_issues': c['total_issues'],
                    'quality_score': c['quality_score']
                }
                for c in sorted(centro_reports, key=lambda x: x['total_issues'], reverse=True)[:10]
            ],
            'timestamp': datetime.now().isoformat(),
            'firebase_collections': {
                'records': 'unidades_proyecto_quality_control_records',
                'centros': 'unidades_proyecto_quality_control_by_centro_gestor',
                'summary': 'unidades_proyecto_quality_control_summary',
                'metadata': 'unidades_proyecto_quality_control_metadata',
                'changelog': 'unidades_proyecto_quality_control_changelog'
            },
            'categorical_metadata': categorical_metadata
        }
        
        if verbose:
            print(f"\n✅ Control de calidad completado exitosamente")
            print(f"   Report ID: {result['report_id']}")
            print(f"   Quality Score: {result['quality_score']:.2f}/100")
            print(f"   Registros analizados: {result['total_records']}")
            print(f"   Problemas encontrados: {result['total_issues']}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error en control de calidad: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    """Test rápido del módulo."""
    import sys
    
    # Configurar encoding UTF-8 para Windows
    if sys.platform == 'win32':
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    
    result = run_quality_control_on_firebase_data(
        collection_name="unidades_proyecto",
        enable_firebase_upload=True,
        enable_s3_upload=True,
        verbose=True
    )
    
    if result:
        print(f"\n{'='*100}")
        print(f"✅ TEST EXITOSO")
        print(f"{'='*100}")
    else:
        print(f"\n{'='*100}")
        print(f"❌ TEST FALLIDO")
        print(f"{'='*100}")
        sys.exit(1)
