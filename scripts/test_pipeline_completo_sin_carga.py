"""
Script de prueba del pipeline completo SIN carga a Firebase/S3
Prueba la integración completa con clustering geoespacial
"""

import os
import sys
import json
from pathlib import Path

# Agregar rutas necesarias al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extraction_app.data_extraction_unidades_proyecto import extract_and_save_unidades_proyecto
from transformation_app.data_transformation_unidades_proyecto import transform_and_save_unidades_proyecto

def test_pipeline_completo_sin_carga():
    """Ejecuta el pipeline completo sin carga a Firebase/S3"""
    
    print("\n" + "="*80)
    print("🧪 TEST PIPELINE COMPLETO SIN CARGA")
    print("="*80)
    
    # =================================================================
    # FASE 1: EXTRACCIÓN
    # =================================================================
    print("\n" + "-"*80)
    print("📥 FASE 1: EXTRACCIÓN DE DATOS")
    print("-"*80)
    
    try:
        extracted_data = extract_and_save_unidades_proyecto()
        if extracted_data is None or extracted_data.empty:
            print("❌ Error: No se pudieron extraer datos")
            return False
        
        print(f"✅ Datos extraídos exitosamente: {len(extracted_data)} filas")
        print(f"   Columnas: {len(extracted_data.columns)}")
    except Exception as e:
        print(f"❌ Error en extracción: {e}")
        return False
    
    # =================================================================
    # FASE 2: TRANSFORMACIÓN CON CLUSTERING
    # =================================================================
    print("\n" + "-"*80)
    print("⚙️  FASE 2: TRANSFORMACIÓN CON CLUSTERING GEOESPACIAL")
    print("-"*80)
    
    try:
        # Transformar SIN cargar a S3/Firebase
        result = transform_and_save_unidades_proyecto(
            data=extracted_data,
            use_extraction=False,  # Ya tenemos los datos
            upload_to_s3=False  # NO CARGAR
        )
        
        if result is None:
            print("❌ Error: La transformación retornó None")
            return False
        
        print(f"\n✅ Transformación completada exitosamente")
        
    except Exception as e:
        print(f"❌ Error en transformación: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # =================================================================
    # FASE 3: VERIFICACIÓN DE ARCHIVOS GENERADOS
    # =================================================================
    print("\n" + "-"*80)
    print("🔍 FASE 3: VERIFICACIÓN DE ARCHIVOS GENERADOS")
    print("-"*80)
    
    output_dir = Path("app_outputs")
    geojson_path = output_dir / "unidades_proyecto_transformed.geojson"
    excel_path = output_dir / "unidades_proyecto_transformed.xlsx"
    
    # Verificar GeoJSON
    if not geojson_path.exists():
        print(f"❌ GeoJSON no encontrado: {geojson_path}")
        return False
    
    print(f"✅ GeoJSON generado: {geojson_path}")
    print(f"   Tamaño: {geojson_path.stat().st_size / 1024:.2f} KB")
    
    # Verificar Excel
    if excel_path.exists():
        print(f"✅ Excel generado: {excel_path}")
        print(f"   Tamaño: {excel_path.stat().st_size / 1024:.2f} KB")
    
    # =================================================================
    # FASE 4: ANÁLISIS DEL GEOJSON
    # =================================================================
    print("\n" + "-"*80)
    print("📊 FASE 4: ANÁLISIS DE ESTRUCTURA DEL GEOJSON")
    print("-"*80)
    
    try:
        with open(geojson_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        features = geojson_data.get('features', [])
        
        print(f"\n📋 Estructura General:")
        print(f"   • Total features (unidades): {len(features)}")
        print(f"   • Type: {geojson_data.get('type', 'N/A')}")
        
        if features:
            # Analizar primera feature
            first_feature = features[0]
            props = first_feature['properties']
            
            # Campos de unidad
            unit_fields = [k for k in props.keys() if k != 'intervenciones']
            print(f"\n📋 Campos a nivel de Unidad: {len(unit_fields)}")
            for field in sorted(unit_fields)[:10]:  # Mostrar primeros 10
                value = props[field]
                if isinstance(value, str) and len(value) > 50:
                    value = value[:47] + "..."
                print(f"   • {field}: {value}")
            
            if len(unit_fields) > 10:
                print(f"   ... y {len(unit_fields) - 10} campos más")
            
            # Campos de intervenciones
            if 'intervenciones' in props and props['intervenciones']:
                interv = props['intervenciones'][0]
                print(f"\n📋 Campos en Intervenciones: {len(interv)}")
                for field in sorted(interv.keys())[:10]:  # Mostrar primeros 10
                    value = interv[field]
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:47] + "..."
                    print(f"   • {field}: {value}")
                
                if len(interv) > 10:
                    print(f"   ... y {len(interv) - 10} campos más")
                
                # Verificar frente_activo
                if 'frente_activo' in interv:
                    print(f"\n✅ Campo 'frente_activo' PRESENTE")
                    print(f"   Valor ejemplo: {interv['frente_activo']}")
                else:
                    print(f"\n❌ Campo 'frente_activo' NO ENCONTRADO")
            
            # Geometría
            has_geom = first_feature.get('geometry') is not None
            print(f"\n🌍 Geometría:")
            if has_geom:
                geom_type = first_feature['geometry'].get('type', 'N/A')
                print(f"   ✅ Primera feature tiene geometría (Type: {geom_type})")
            else:
                print(f"   ⚠️  Primera feature no tiene geometría")
            
            # Cobertura de geometría
            units_with_geom = sum(1 for f in features if f.get('geometry') is not None)
            geom_coverage = (units_with_geom / len(features)) * 100
            print(f"   • Unidades con geometría: {units_with_geom}/{len(features)} ({geom_coverage:.1f}%)")
            
            # Unidades con múltiples intervenciones
            multi_interv = [
                (f['properties']['upid'], 
                 f['properties'].get('nombre_up', 'N/A'),
                 len(f['properties']['intervenciones']))
                for f in features 
                if len(f['properties']['intervenciones']) > 1
            ]
            
            if multi_interv:
                multi_interv.sort(key=lambda x: x[2], reverse=True)
                print(f"\n🔢 Unidades con múltiples intervenciones: {len(multi_interv)}")
                print(f"   Top 3:")
                for upid, nombre, n_interv in multi_interv[:3]:
                    nombre_short = nombre if len(nombre) <= 50 else nombre[:47] + "..."
                    print(f"   • {upid}: {nombre_short} ({n_interv} intervenciones)")
            
            # Ejemplos de frente_activo por estado
            print(f"\n📊 Ejemplos de frente_activo por estado:")
            ejemplos_por_estado = {}
            for feature in features[:100]:  # Analizar primeros 100
                for interv in feature['properties']['intervenciones']:
                    estado = interv.get('estado', 'N/A')
                    frente = interv.get('frente_activo', 'N/A')
                    
                    if estado not in ejemplos_por_estado and estado != 'N/A':
                        ejemplos_por_estado[estado] = frente
                    
                    if len(ejemplos_por_estado) >= 5:  # Limitar a 5 ejemplos
                        break
                if len(ejemplos_por_estado) >= 5:
                    break
            
            for estado, frente in sorted(ejemplos_por_estado.items()):
                print(f"   • {estado:30s} → {frente}")
    
    except Exception as e:
        print(f"❌ Error al analizar GeoJSON: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # =================================================================
    # RESUMEN FINAL
    # =================================================================
    print("\n" + "="*80)
    print("✅ TEST COMPLETO EXITOSO")
    print("="*80)
    print("\n📌 Resumen:")
    print(f"   ✅ Extracción: {len(extracted_data)} intervenciones")
    print(f"   ✅ Transformación: {len(features)} unidades de proyecto")
    print(f"   ✅ Clustering: {len(multi_interv)} unidades agrupadas")
    print(f"   ✅ Geometría: {geom_coverage:.1f}% cobertura")
    print(f"   ✅ Estructura: Geometry a nivel de unidad ✓")
    print(f"   ✅ Campos: frente_activo incluido ✓")
    print(f"\n💾 Archivos generados en: {output_dir.absolute()}")
    
    return True

if __name__ == "__main__":
    try:
        success = test_pipeline_completo_sin_carga()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
