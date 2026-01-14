# -*- coding: utf-8 -*-
"""
Script de verificación de compatibilidad del GeoJSON de infraestructura
con la estructura de Firebase unidades_proyecto.

Este script NO carga datos, solo valida la estructura y reporta posibles problemas.
"""

import json
import os
import sys

# Agregar rutas necesarias
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def validate_infraestructura_geojson():
    """
    Valida el GeoJSON de infraestructura y reporta compatibilidad con Firebase.
    """
    print("="*80)
    print("VALIDACIÓN DE COMPATIBILIDAD - GEOJSON INFRAESTRUCTURA")
    print("="*80)
    
    # Ruta al GeoJSON
    geojson_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "app_outputs",
        "unidades_proyecto_infraestructura_outputs",
        "unidades_proyecto_infraestructura_2024_2025.geojson"
    )
    
    print(f"\n📁 Archivo: {os.path.basename(geojson_path)}")
    
    # Verificar existencia
    if not os.path.exists(geojson_path):
        print(f"\n❌ ERROR: Archivo no encontrado")
        print(f"   Ruta esperada: {geojson_path}")
        return False
    
    print(f"✅ Archivo encontrado")
    
    # Cargar GeoJSON
    try:
        with open(geojson_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        print(f"✅ GeoJSON cargado correctamente")
    except Exception as e:
        print(f"\n❌ ERROR al cargar GeoJSON: {e}")
        return False
    
    # Validar estructura básica
    if geojson_data.get('type') != 'FeatureCollection':
        print(f"\n❌ ERROR: No es un FeatureCollection válido")
        return False
    
    features = geojson_data.get('features', [])
    if not features:
        print(f"\n❌ ERROR: No hay features en el GeoJSON")
        return False
    
    print(f"✅ Estructura válida: FeatureCollection con {len(features)} features")
    
    # Estadísticas de validación
    stats = {
        'total': len(features),
        'with_geometry': 0,
        'geometry_types': {},
        'with_bpin': 0,
        'with_bp': 0,
        'with_upid': 0,
        'missing_fields': {},
        'estados': {},
        'anios': {},
        'centros_gestores': set()
    }
    
    # Campos requeridos para Firebase
    required_fields = [
        'tipo_equipamiento',
        'clase_up',
        'nombre_centro_gestor',
        'estado'
    ]
    
    recommended_fields = [
        'bpin',
        'bp',
        'nombre_up',
        'comuna_corregimiento',
        'presupuesto_base',
        'avance_obra',
        'anio'
    ]
    
    print(f"\n📊 Analizando {len(features)} features...")
    
    # Analizar cada feature
    for i, feature in enumerate(features):
        # Geometría
        geometry = feature.get('geometry')
        if geometry:
            stats['with_geometry'] += 1
            geom_type = geometry.get('type', 'Unknown')
            stats['geometry_types'][geom_type] = stats['geometry_types'].get(geom_type, 0) + 1
        
        # Properties
        properties = feature.get('properties', {})
        
        # Campos identificadores
        if properties.get('bpin'):
            stats['with_bpin'] += 1
        if properties.get('bp'):
            stats['with_bp'] += 1
        if properties.get('upid'):
            stats['with_upid'] += 1
        
        # Estados
        estado = properties.get('estado')
        if estado:
            stats['estados'][estado] = stats['estados'].get(estado, 0) + 1
        
        # Años
        anio = properties.get('anio')
        if anio:
            stats['anios'][anio] = stats['anios'].get(anio, 0) + 1
        
        # Centros gestores
        centro = properties.get('nombre_centro_gestor')
        if centro:
            stats['centros_gestores'].add(centro)
        
        # Campos faltantes
        for field in required_fields + recommended_fields:
            if not properties.get(field):
                if field not in stats['missing_fields']:
                    stats['missing_fields'][field] = 0
                stats['missing_fields'][field] += 1
    
    # Reporte de validación
    print(f"\n{'='*80}")
    print("REPORTE DE COMPATIBILIDAD")
    print("="*80)
    
    # Geometrías
    print(f"\n🗺️  GEOMETRÍAS:")
    print(f"   Total con geometría: {stats['with_geometry']}/{stats['total']}")
    print(f"   Tipos de geometría:")
    for geom_type, count in stats['geometry_types'].items():
        print(f"     - {geom_type}: {count}")
    
    # Identificadores
    print(f"\n🆔 IDENTIFICADORES:")
    print(f"   Con BPIN: {stats['with_bpin']}/{stats['total']}")
    print(f"   Con BP: {stats['with_bp']}/{stats['total']}")
    print(f"   Con UPID: {stats['with_upid']}/{stats['total']}")
    
    if stats['with_upid'] == 0:
        print(f"   ⚠️  Ningún feature tiene UPID - se generarán automáticamente")
    
    # Estados
    print(f"\n📊 ESTADOS:")
    for estado, count in sorted(stats['estados'].items()):
        print(f"   - {estado}: {count}")
    
    # Años
    print(f"\n📅 AÑOS:")
    for anio, count in sorted(stats['anios'].items()):
        print(f"   - {anio}: {count}")
    
    # Centros gestores
    print(f"\n🏛️  CENTROS GESTORES:")
    for centro in sorted(stats['centros_gestores']):
        print(f"   - {centro}")
    
    # Campos faltantes
    print(f"\n⚠️  CAMPOS FALTANTES:")
    if stats['missing_fields']:
        for field, count in sorted(stats['missing_fields'].items(), key=lambda x: x[1], reverse=True):
            severity = "❌ REQUERIDO" if field in required_fields else "⚠️  RECOMENDADO"
            print(f"   {severity} - {field}: {count} features sin este campo")
    else:
        print(f"   ✅ Todos los campos requeridos están presentes")
    
    # Compatibilidad general
    print(f"\n{'='*80}")
    print("RESUMEN DE COMPATIBILIDAD")
    print("="*80)
    
    issues = []
    warnings = []
    
    # Verificar campos críticos
    critical_missing = [f for f in required_fields if f in stats['missing_fields']]
    if critical_missing:
        issues.append(f"Campos requeridos faltantes: {', '.join(critical_missing)}")
    
    # Verificar geometrías
    if stats['with_geometry'] == 0:
        issues.append("Ningún feature tiene geometría")
    elif stats['with_geometry'] < stats['total']:
        warnings.append(f"{stats['total'] - stats['with_geometry']} features sin geometría")
    
    # Verificar identificadores
    if stats['with_bpin'] == 0 and stats['with_bp'] == 0:
        warnings.append("Ningún feature tiene BPIN ni BP - se usarán UUIDs genéricos")
    
    # Verificar tipo de geometría
    if 'LineString' not in stats['geometry_types']:
        warnings.append("No hay geometrías de tipo LineString (esperado para vías)")
    
    # Mostrar resultados
    if issues:
        print(f"\n❌ PROBLEMAS CRÍTICOS ENCONTRADOS:")
        for i, issue in enumerate(issues, 1):
            print(f"   {i}. {issue}")
        print(f"\n⚠️  El GeoJSON requiere correcciones antes de cargar a Firebase")
        return False
    elif warnings:
        print(f"\n⚠️  ADVERTENCIAS:")
        for i, warning in enumerate(warnings, 1):
            print(f"   {i}. {warning}")
        print(f"\n✅ El GeoJSON es compatible pero tiene advertencias menores")
        print(f"   El pipeline puede proceder - se aplicarán correcciones automáticas")
        return True
    else:
        print(f"\n✅ COMPATIBILIDAD TOTAL")
        print(f"   El GeoJSON cumple con todos los requisitos")
        print(f"   Listo para integración al pipeline")
        return True


if __name__ == "__main__":
    """
    Ejecutar validación de compatibilidad.
    """
    print("\n🔍 Iniciando validación de compatibilidad...\n")
    
    success = validate_infraestructura_geojson()
    
    print(f"\n{'='*80}")
    if success:
        print("✅ VALIDACIÓN EXITOSA")
        print("\n📝 Próximos pasos:")
        print("   1. Revisar el reporte de validación arriba")
        print("   2. Ejecutar el pipeline completo:")
        print("      python pipelines\\unidades_proyecto_pipeline.py")
        print("   3. El pipeline generará UPIDs automáticamente si es necesario")
    else:
        print("❌ VALIDACIÓN FALLIDA")
        print("\n📝 Acciones requeridas:")
        print("   1. Revisar los problemas críticos reportados arriba")
        print("   2. Corregir el GeoJSON de infraestructura")
        print("   3. Volver a ejecutar esta validación")
    print("="*80)
    
    sys.exit(0 if success else 1)
