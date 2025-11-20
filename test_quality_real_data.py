# -*- coding: utf-8 -*-
"""
Test rápido de control de calidad con datos reales
===================================================

Ejecuta solo el control de calidad sobre los datos transformados existentes.
"""

import sys
import os

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Agregar paths necesarios
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.quality_control import validate_geojson
from utils.quality_reporter import QualityReporter
from utils.quality_s3_exporter import export_quality_reports_to_s3
from load_app.data_loading_quality_control import load_quality_reports_to_firebase
from utils.s3_downloader import S3Downloader
import gzip
import json

def main():
    print("\n" + "="*100)
    print("🔍 TEST DE CONTROL DE CALIDAD CON DATOS REALES")
    print("="*100 + "\n")
    
    # Descargar el archivo GeoJSON desde S3
    geojson_path = "app_outputs/unidades_proyecto_transformed.geojson"
    s3_key = "up-geodata/unidades_proyecto_transformed/current/unidades_proyecto_transformed.geojson.gz"
    
    print(f"📥 Descargando datos desde S3...")
    print(f"   Key: {s3_key}")
    
    try:
        # Descargar archivo comprimido
        downloader = S3Downloader()
        geojson_data = downloader.read_json_from_s3(s3_key)
        
        if not geojson_data:
            print(f"❌ No se pudo descargar el archivo desde S3")
            print("   Ejecuta primero el pipeline de transformación")
            return False
        
        # Guardar localmente
        os.makedirs("app_outputs", exist_ok=True)
        with open(geojson_path, 'w', encoding='utf-8') as f:
            json.dump(geojson_data, f, ensure_ascii=False)
        
        print(f"   ✓ Descargado y descomprimido: {len(geojson_data.get('features', []))} registros\n")
    except Exception as e:
        print(f"❌ Error descargando desde S3: {e}")
        print("   Ejecuta primero el pipeline de transformación")
        import traceback
        traceback.print_exc()
        return False
    
    print(f"📂 Archivo a validar: {geojson_path}")
    print(f"📊 Ejecutando validaciones ISO 19157...\n")
    
    # 1. Validar datos
    validation_result = validate_geojson(geojson_path, verbose=True)
    
    if not validation_result:
        print("\n❌ Validación falló")
        return False
    
    print(f"\n✅ Validación completada:")
    print(f"   - Registros validados: {validation_result['total_records']}")
    print(f"   - Registros con problemas: {validation_result['records_with_issues']}")
    print(f"   - Total de problemas: {validation_result['total_issues']}")
    
    # 2. Generar reportes
    print(f"\n📊 Generando reportes detallados...")
    reporter = QualityReporter()
    issues = validation_result['issues']
    
    record_reports = reporter.generate_record_level_report(issues)
    print(f"   ✓ Reportes por registro: {len(record_reports)}")
    
    centro_reports = reporter.generate_centro_gestor_report(issues)
    print(f"   ✓ Reportes por centro gestor: {len(centro_reports)}")
    
    summary_report = reporter.generate_summary_report(
        record_reports=record_reports,
        centro_reports=centro_reports,
        total_records=validation_result['total_records'],
        validation_stats=validation_result['statistics']
    )
    print(f"   ✓ Reporte resumen generado")
    print(f"   ✓ Quality Score: {summary_report['global_quality_score']:.2f}/100")
    
    # 3. Cargar a Firebase
    print(f"\n📤 Cargando reportes a Firebase...")
    try:
        firebase_result = load_quality_reports_to_firebase(
            record_reports=record_reports,
            centro_reports=centro_reports,
            summary_report=summary_report
        )
        
        if firebase_result:
            print(f"   ✅ Firebase upload exitoso")
            print(f"      - Colección records: {firebase_result.get('records_uploaded', 0)} docs")
            print(f"      - Colección centros: {firebase_result.get('centros_uploaded', 0)} docs")
            print(f"      - Colección summary: {'✓' if firebase_result.get('summary_uploaded') else '✗'}")
        else:
            print(f"   ⚠️  Firebase upload falló")
    except Exception as e:
        print(f"   ❌ Error en Firebase: {e}")
    
    # 4. Exportar a S3
    print(f"\n📦 Exportando reportes a S3...")
    try:
        s3_result = export_quality_reports_to_s3(
            record_reports=record_reports,
            centro_reports=centro_reports,
            summary_report=summary_report,
            report_id=summary_report['report_id']
        )
        
        if s3_result:
            print(f"   ✅ S3 export exitoso")
            print(f"      - Archivos exportados: {s3_result.get('files_uploaded', 0)}")
        else:
            print(f"   ⚠️  S3 export falló")
    except Exception as e:
        print(f"   ❌ Error en S3: {e}")
    
    # Resumen final
    print(f"\n" + "="*100)
    print(f"✅ TEST COMPLETADO")
    print(f"="*100)
    print(f"\n📊 RESUMEN:")
    print(f"   Total registros: {validation_result['total_records']}")
    print(f"   Registros con problemas: {validation_result['records_with_issues']} ({validation_result['records_with_issues']/validation_result['total_records']*100:.1f}%)")
    print(f"   Total problemas: {validation_result['total_issues']}")
    print(f"   Quality Score: {summary_report['global_quality_score']:.2f}/100")
    print(f"   Report ID: {summary_report['report_id']}")
    
    print(f"\n🔍 TOP 5 PROBLEMAS MÁS COMUNES:")
    stats = validation_result.get('statistics', {})
    by_rule = stats.get('by_rule', {})
    top_rules = sorted(by_rule.items(), key=lambda x: x[1], reverse=True)[:5]
    for i, (rule, count) in enumerate(top_rules, 1):
        print(f"   {i}. {rule}: {count} ocurrencias")
    
    print(f"\n🏢 TOP 5 CENTROS GESTORES CON MÁS PROBLEMAS:")
    top_centros = sorted(centro_reports, key=lambda x: x['total_issues'], reverse=True)[:5]
    for i, centro in enumerate(top_centros, 1):
        print(f"   {i}. {centro['nombre_centro_gestor']}: {centro['total_issues']} problemas")
    
    print(f"\n" + "="*100)
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
