# -*- coding: utf-8 -*-
"""
Test script para verificar integración de clustering geoespacial en el pipeline.

Este script prueba que el módulo de transformación puede usar el nuevo
clustering geoespacial sin romper el pipeline existente.
"""

import os
import sys
from pathlib import Path

# Agregar rutas necesarias
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'extraction_app'))
sys.path.insert(0, os.path.join(project_root, 'transformation_app'))

from extraction_app.data_extraction_unidades_proyecto import extract_unidades_proyecto_data
from transformation_app.data_transformation_unidades_proyecto import generate_upid_for_records

def test_clustering_integration():
    """
    Prueba la integración del clustering en el pipeline de transformación.
    """
    print("="*80)
    print("TEST: INTEGRACIÓN DE CLUSTERING GEOESPACIAL EN PIPELINE")
    print("="*80)
    
    # Paso 1: Extraer datos
    print("\n[PASO 1: EXTRACCIÓN]")
    
    # Usar función de extracción que devuelve DataFrame
    df = extract_unidades_proyecto_data()
    
    if df is None or len(df) == 0:
        print("❌ Error: No se pudieron extraer los datos")
        return False
    
    print(f"✅ Datos extraídos: {len(df)} registros")
    
    # Paso 2: Aplicar clustering (nuevo método)
    print("\n[PASO 2: CLUSTERING CON GEOESPACIAL]")
    try:
        df_clustered = generate_upid_for_records(df, use_clustering=True)
        
        print(f"✅ Clustering completado")
        print(f"   • Registros procesados: {len(df_clustered)}")
        print(f"   • UPIDs únicos: {df_clustered['upid'].nunique()}")
        
        if 'n_intervenciones' in df_clustered.columns:
            print(f"   • Campo n_intervenciones: ✅ Presente")
            print(f"   • Promedio intervenciones/unidad: {df_clustered['n_intervenciones'].mean():.2f}")
        else:
            print(f"   • Campo n_intervenciones: ❌ Ausente")
        
        if 'intervencion_id' in df_clustered.columns:
            print(f"   • Campo intervencion_id: ✅ Presente")
        
        # Verificar que no existan campos antiguos
        if 'cluster_original' in df_clustered.columns:
            print(f"   • ⚠️ cluster_original encontrado (debería estar eliminado)")
        else:
            print(f"   • cluster_original: ✅ Correctamente eliminado")
        
        if 'intervencion_num' in df_clustered.columns:
            print(f"   • ⚠️ intervencion_num encontrado (debería estar eliminado)")
        else:
            print(f"   • intervencion_num: ✅ Correctamente eliminado")
        
    except Exception as e:
        print(f"❌ Error en clustering: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Paso 3: Fallback a método simple
    print("\n[PASO 3: FALLBACK A MÉTODO SIMPLE]")
    try:
        df_simple = generate_upid_for_records(df, use_clustering=False)
        
        print(f"✅ Método simple funcional")
        print(f"   • Registros procesados: {len(df_simple)}")
        print(f"   • UPIDs únicos: {df_simple['upid'].nunique()}")
        
    except Exception as e:
        print(f"❌ Error en método simple: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Paso 4: Comparación
    print("\n[PASO 4: COMPARACIÓN DE MÉTODOS]")
    print(f"   • Clustering Geoespacial: {df_clustered['upid'].nunique()} unidades")
    print(f"   • Método Simple: {df_simple['upid'].nunique()} unidades")
    print(f"   • Reducción: {df_simple['upid'].nunique() - df_clustered['upid'].nunique()} unidades")
    print(f"   • Mejora: {((df_simple['upid'].nunique() - df_clustered['upid'].nunique()) / df_simple['upid'].nunique() * 100):.2f}%")
    
    print("\n" + "="*80)
    print("✅ TEST COMPLETADO EXITOSAMENTE")
    print("="*80)
    
    return True


if __name__ == "__main__":
    print("\n🧪 Iniciando prueba de integración...")
    
    success = test_clustering_integration()
    
    if success:
        print("\n✅ La integración funciona correctamente")
        print("   El módulo está listo para usarse en el pipeline principal")
    else:
        print("\n❌ La integración tiene problemas")
        print("   Revisar los errores anteriores")
    
    sys.exit(0 if success else 1)
