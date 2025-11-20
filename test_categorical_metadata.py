# -*- coding: utf-8 -*-
"""
Test de Metadata Categórica para Componentes Next.js
=====================================================

Verifica que la metadata categórica se genera correctamente
y contiene todos los elementos necesarios para componentes UI.

Author: ETL QA Team
Date: November 2025
"""

import sys
import json
from pathlib import Path

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

from utils.quality_reporter import QualityReporter


def create_sample_reports():
    """Crea reportes de ejemplo para testing."""
    
    # Sample record reports
    record_reports = [
        {
            'upid': 'UP-001',
            'nombre_up': 'Unidad Proyecto 1',
            'nombre_centro_gestor': 'Centro A',
            'total_issues': 5,
            'max_severity': 'CRITICAL',
            'priority': 'P0',
            'severity_counts': {'CRITICAL': 3, 'HIGH': 2},
            'dimension_counts': {'Consistencia Lógica': 2, 'Completitud': 3},
            'issues': [
                {
                    'rule_id': 'R001',
                    'rule_name': 'Validar UPID',
                    'dimension': 'Consistencia Lógica',
                    'severity': 'CRITICAL',
                    'field_name': 'upid',
                    'current_value': None,
                    'expected_value': 'UP-XXX',
                    'details': 'UPID faltante',
                    'suggestion': 'Agregar UPID'
                }
            ]
        },
        {
            'upid': 'UP-002',
            'nombre_up': 'Unidad Proyecto 2',
            'nombre_centro_gestor': 'Centro B',
            'total_issues': 2,
            'max_severity': 'HIGH',
            'priority': 'P2',
            'severity_counts': {'HIGH': 2},
            'dimension_counts': {'Exactitud Posicional': 2},
            'issues': []
        }
    ]
    
    # Sample centro reports
    centro_reports = [
        {
            'nombre_centro_gestor': 'Centro A',
            'total_records': 100,
            'records_with_issues': 30,
            'error_rate': 30.0,
            'quality_score': 65.5,
            'total_issues': 50,
            'priority': 'P1',
            'status': 'DEFICIENTE',
            'severity_counts': {'CRITICAL': 10, 'HIGH': 20, 'MEDIUM': 15, 'LOW': 5},
            'dimension_counts': {'Consistencia Lógica': 20, 'Completitud': 30}
        },
        {
            'nombre_centro_gestor': 'Centro B',
            'total_records': 80,
            'records_with_issues': 10,
            'error_rate': 12.5,
            'quality_score': 88.0,
            'total_issues': 15,
            'priority': 'P3',
            'status': 'BUENO',
            'severity_counts': {'HIGH': 5, 'MEDIUM': 8, 'LOW': 2},
            'dimension_counts': {'Exactitud Posicional': 10, 'Exactitud Temática': 5}
        }
    ]
    
    # Sample summary report
    summary_report = {
        'total_records_validated': 180,
        'records_with_issues': 40,
        'total_issues_found': 65,
        'severity_distribution': {'CRITICAL': 10, 'HIGH': 25, 'MEDIUM': 23, 'LOW': 7},
        'dimension_distribution': {
            'Consistencia Lógica': 20,
            'Completitud': 30,
            'Exactitud Posicional': 10,
            'Exactitud Temática': 5
        }
    }
    
    return record_reports, centro_reports, summary_report


def test_categorical_metadata():
    """Test principal de metadata categórica."""
    
    print("\n" + "="*100)
    print("🧪 TEST: METADATA CATEGÓRICA PARA NEXT.JS")
    print("="*100)
    
    # 1. Crear reportes de ejemplo
    print("\n📋 Paso 1: Creando reportes de ejemplo...")
    record_reports, centro_reports, summary_report = create_sample_reports()
    print(f"   ✓ {len(record_reports)} reportes por registro")
    print(f"   ✓ {len(centro_reports)} reportes por centro")
    print(f"   ✓ 1 reporte resumen")
    
    # 2. Generar metadata categórica
    print("\n🔍 Paso 2: Generando metadata categórica...")
    reporter = QualityReporter()
    
    try:
        metadata = reporter.generate_categorical_metadata(
            record_reports=record_reports,
            centro_reports=centro_reports,
            summary_report=summary_report
        )
        print(f"   ✓ Metadata generada exitosamente")
        print(f"   ✓ Report ID: {metadata.get('report_id')}")
    except Exception as e:
        print(f"   ✗ Error generando metadata: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # 3. Verificar estructura de metadata
    print("\n📊 Paso 3: Verificando estructura de metadata...")
    
    required_sections = [
        'filters', 'ranges', 'tabs', 'tables', 'charts',
        'grouping', 'sorting', 'colors', 'counts', 'icons', 'tooltips'
    ]
    
    for section in required_sections:
        if section in metadata:
            print(f"   ✓ Sección '{section}' presente")
        else:
            print(f"   ✗ Sección '{section}' FALTANTE")
            return False
    
    # 4. Verificar filtros
    print("\n🔍 Paso 4: Verificando opciones de filtrado...")
    filters = metadata.get('filters', {})
    
    expected_filters = [
        'severities', 'dimensions', 'priorities', 'statuses',
        'centros_gestores', 'rule_ids', 'field_names'
    ]
    
    for filter_key in expected_filters:
        if filter_key in filters:
            values = filters[filter_key]
            print(f"   ✓ Filtro '{filter_key}': {len(values)} opciones")
        else:
            print(f"   ✗ Filtro '{filter_key}' FALTANTE")
    
    # 5. Verificar rangos
    print("\n📈 Paso 5: Verificando rangos numéricos...")
    ranges = metadata.get('ranges', {})
    
    for range_key, range_data in ranges.items():
        print(f"   ✓ Rango '{range_key}':")
        print(f"      Min: {range_data.get('min')}")
        print(f"      Max: {range_data.get('max')}")
        print(f"      Promedio: {range_data.get('average'):.2f}")
    
    # 6. Verificar configuración de tabs
    print("\n📑 Paso 6: Verificando configuración de tabs/pestañas...")
    tabs = metadata.get('tabs', {})
    
    for tab_group, tab_list in tabs.items():
        print(f"   ✓ Grupo '{tab_group}': {len(tab_list)} tabs")
        for tab in tab_list[:2]:  # Mostrar primeros 2
            print(f"      - {tab.get('id')}: {tab.get('label')}")
    
    # 7. Verificar configuración de tablas
    print("\n📊 Paso 7: Verificando configuración de tablas...")
    tables = metadata.get('tables', {})
    
    for table_key, table_config in tables.items():
        columns = table_config.get('columns', [])
        print(f"   ✓ Tabla '{table_key}': {len(columns)} columnas")
        print(f"      Items por página: {table_config.get('items_per_page')}")
        print(f"      Exportable: {table_config.get('exportable')}")
    
    # 8. Verificar paleta de colores
    print("\n🎨 Paso 8: Verificando paleta de colores...")
    colors = metadata.get('colors', {})
    
    for category, color_map in colors.items():
        print(f"   ✓ Categoría '{category}': {len(color_map)} colores definidos")
    
    # 9. Verificar íconos
    print("\n🎯 Paso 9: Verificando mapeo de íconos...")
    icons = metadata.get('icons', {})
    
    for icon_category, icon_map in icons.items():
        print(f"   ✓ Categoría '{icon_category}': {len(icon_map)} íconos")
    
    # 10. Exportar metadata a JSON para inspección
    print("\n💾 Paso 10: Exportando metadata a JSON...")
    
    output_dir = Path('test_outputs')
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / 'categorical_metadata_sample.json'
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        file_size = output_file.stat().st_size / 1024
        print(f"   ✓ Exportado: {output_file.name} ({file_size:.1f} KB)")
        print(f"   ✓ Ruta completa: {output_file.absolute()}")
    except Exception as e:
        print(f"   ✗ Error exportando: {e}")
        return False
    
    # 11. Mostrar resumen de metadata
    print("\n📋 RESUMEN DE METADATA:")
    print(f"   Report ID: {metadata.get('report_id')}")
    print(f"   Versión: {metadata.get('version')}")
    print(f"   Filtros disponibles: {len(metadata.get('filters', {}))}")
    print(f"   Rangos definidos: {len(metadata.get('ranges', {}))}")
    print(f"   Configuraciones de tabs: {len(metadata.get('tabs', {}))}")
    print(f"   Configuraciones de tablas: {len(metadata.get('tables', {}))}")
    print(f"   Configuraciones de gráficas: {len(metadata.get('charts', {}))}")
    print(f"   Opciones de agrupación: {len(metadata.get('grouping', []))}")
    print(f"   Opciones de ordenamiento: {len(metadata.get('sorting', []))}")
    print(f"   Total centros: {metadata.get('counts', {}).get('total_centros')}")
    print(f"   Total registros: {metadata.get('counts', {}).get('total_records')}")
    print(f"   Total problemas: {metadata.get('counts', {}).get('total_issues')}")
    
    return True


def main():
    """Ejecuta el test."""
    success = test_categorical_metadata()
    
    print("\n" + "="*100)
    if success:
        print("✅ TEST EXITOSO - Metadata categórica generada correctamente")
        print("\nLa metadata incluye:")
        print("  ✓ Opciones para dropdowns y filtros")
        print("  ✓ Rangos para sliders y gráficas")
        print("  ✓ Configuraciones para tabs/pestañas")
        print("  ✓ Esquemas de columnas para tablas")
        print("  ✓ Configuraciones de charts")
        print("  ✓ Paleta de colores consistente")
        print("  ✓ Mapeo de íconos")
        print("  ✓ Textos de ayuda (tooltips)")
        print("\n✨ Lista para usar en componentes Next.js")
    else:
        print("❌ TEST FALLIDO - Revisar errores arriba")
    print("="*100 + "\n")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
