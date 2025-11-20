# -*- coding: utf-8 -*-
"""
Verificar Metadata Categórica en Firebase
=========================================

Verifica que la metadata categórica se haya cargado correctamente
a Firebase y muestra un resumen de su contenido.

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

from database.config import get_firestore_client


def verify_categorical_metadata():
    """Verifica la metadata categórica en Firebase."""
    
    print("\n" + "="*100)
    print("🔍 VERIFICACIÓN DE METADATA CATEGÓRICA EN FIREBASE")
    print("="*100)
    
    try:
        # Conectar a Firebase
        print("\n📡 Conectando a Firebase...")
        db = get_firestore_client()
        
        if not db:
            print("❌ No se pudo conectar a Firebase")
            return False
        
        print("   ✓ Conexión establecida")
        
        # Obtener todos los documentos de metadata
        print("\n📥 Consultando colección 'unidades_proyecto_quality_control_metadata'...")
        metadata_docs = list(db.collection('unidades_proyecto_quality_control_metadata').stream())
        
        if not metadata_docs:
            print("   ⚠️ No se encontraron documentos de metadata")
            return False
        
        print(f"   ✓ Encontrados {len(metadata_docs)} documento(s)")
        
        # Analizar el documento más reciente
        latest_doc = metadata_docs[-1]  # Último documento
        metadata = latest_doc.to_dict()
        
        print(f"\n📋 Analizando documento: {latest_doc.id}")
        print(f"   Report ID: {metadata.get('report_id')}")
        print(f"   Versión: {metadata.get('version')}")
        print(f"   Generado: {metadata.get('generated_at')}")
        
        # Verificar secciones principales
        print("\n✅ Secciones verificadas:")
        
        sections = {
            'filters': 'Opciones de filtrado',
            'ranges': 'Rangos numéricos',
            'tabs': 'Configuración de tabs',
            'tables': 'Configuración de tablas',
            'charts': 'Configuración de gráficas',
            'grouping': 'Opciones de agrupación',
            'sorting': 'Opciones de ordenamiento',
            'colors': 'Paleta de colores',
            'counts': 'Contadores rápidos',
            'icons': 'Mapeo de íconos',
            'tooltips': 'Textos de ayuda'
        }
        
        for section_key, section_name in sections.items():
            if section_key in metadata:
                section_data = metadata[section_key]
                if isinstance(section_data, dict):
                    count = len(section_data)
                elif isinstance(section_data, list):
                    count = len(section_data)
                else:
                    count = 1
                print(f"   ✓ {section_name}: {count} elementos")
            else:
                print(f"   ✗ {section_name}: FALTANTE")
        
        # Detalles de filtros
        print("\n🔍 Detalles de filtros:")
        filters = metadata.get('filters', {})
        for filter_name, filter_values in filters.items():
            print(f"   • {filter_name}: {len(filter_values)} opciones")
            if len(filter_values) <= 5:
                print(f"      → {', '.join(str(v) for v in filter_values)}")
        
        # Detalles de rangos
        print("\n📊 Detalles de rangos:")
        ranges = metadata.get('ranges', {})
        for range_name, range_data in ranges.items():
            print(f"   • {range_name}:")
            print(f"      Min: {range_data.get('min')}, Max: {range_data.get('max')}")
            print(f"      Promedio: {range_data.get('average', 0):.2f}")
        
        # Configuración de tabs
        print("\n📑 Configuración de tabs:")
        tabs = metadata.get('tabs', {})
        for tab_group, tab_list in tabs.items():
            print(f"   • {tab_group}: {len(tab_list)} tabs")
        
        # Configuración de tablas
        print("\n📋 Configuración de tablas:")
        tables = metadata.get('tables', {})
        for table_name, table_config in tables.items():
            columns = table_config.get('columns', [])
            print(f"   • {table_name}:")
            print(f"      Columnas: {len(columns)}")
            print(f"      Items por página: {table_config.get('items_per_page')}")
        
        # Contadores
        print("\n🔢 Contadores rápidos:")
        counts = metadata.get('counts', {})
        for count_name, count_value in counts.items():
            print(f"   • {count_name}: {count_value}")
        
        # Exportar para inspección
        print("\n💾 Exportando metadata para inspección...")
        output_dir = Path('test_outputs')
        output_dir.mkdir(exist_ok=True)
        
        output_file = output_dir / f'firebase_metadata_{latest_doc.id}.json'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        file_size = output_file.stat().st_size / 1024
        print(f"   ✓ Exportado: {output_file.name} ({file_size:.1f} KB)")
        
        # Verificar otras colecciones de calidad
        print("\n📊 Verificando otras colecciones de calidad:")
        
        collections_to_check = [
            ('unidades_proyecto_quality_control_summary', 'Resumen'),
            ('unidades_proyecto_quality_control_by_centro_gestor', 'Por Centro Gestor'),
            ('unidades_proyecto_quality_control_records', 'Registros Detallados'),
            ('unidades_proyecto_quality_control_changelog', 'Changelog')
        ]
        
        for collection_name, label in collections_to_check:
            try:
                docs = list(db.collection(collection_name).limit(1).stream())
                count = len(docs)
                if count > 0:
                    print(f"   ✓ {label}: Colección presente")
                else:
                    print(f"   ⚠️ {label}: Colección vacía")
            except Exception as e:
                print(f"   ✗ {label}: Error - {e}")
        
        print("\n" + "="*100)
        print("✅ VERIFICACIÓN COMPLETADA")
        print("="*100)
        print("\n✨ Metadata categórica lista para usar en Next.js")
        print("\n📚 Componentes que pueden usar esta metadata:")
        print("   • Dropdowns de filtrado (filters)")
        print("   • Sliders de rango (ranges)")
        print("   • Tabs/Pestañas (tabs)")
        print("   • Tablas con configuración (tables)")
        print("   • Gráficas (charts)")
        print("   • Selectores de agrupación (grouping)")
        print("   • Controles de ordenamiento (sorting)")
        print("   • Badges con colores consistentes (colors)")
        print("   • Íconos por categoría (icons)")
        print("   • Tooltips informativos (tooltips)")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error durante verificación: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = verify_categorical_metadata()
    sys.exit(0 if success else 1)
