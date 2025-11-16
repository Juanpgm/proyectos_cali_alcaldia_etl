"""
Test detallado de intersecciones espaciales con datos reales
"""
import sys
import os
import pandas as pd
import json

# Add transformation_app to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'transformation_app'))

from data_transformation_unidades_proyecto import _process_unidades_proyecto_dataframe

def test_spatial_intersections_real_data():
    """Test spatial intersections with real data"""
    print("\n" + "="*80)
    print("TEST: INTERSECCIONES ESPACIALES CON DATOS REALES")
    print("="*80)
    
    # Load real data
    input_file = "transformation_app/app_inputs/unidades_proyecto_input/unidades_proyecto.json"
    
    print(f"\n✓ Cargando datos desde: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    print(f"  Total de registros: {len(df)}")
    print(f"  Total de columnas: {len(df.columns)}")
    
    # Process data
    print(f"\n{'='*80}")
    print("PROCESANDO DATOS...")
    print("="*80)
    
    result_df = _process_unidades_proyecto_dataframe(df)
    
    print(f"\n{'='*80}")
    print("RESULTADOS DE INTERSECCIONES ESPACIALES")
    print("="*80)
    
    # Analyze comuna_corregimiento_val
    print(f"\n1. COLUMNA 'comuna_corregimiento_val':")
    print("-" * 80)
    
    if 'comuna_corregimiento_val' in result_df.columns:
        comuna_counts = result_df['comuna_corregimiento_val'].value_counts()
        print(f"  Total de valores únicos: {len(comuna_counts)}")
        print(f"\n  Distribución (Top 15):")
        for value, count in comuna_counts.head(15).items():
            pct = (count / len(result_df)) * 100
            print(f"    • {value}: {count} ({pct:.1f}%)")
        
        revisar_count = (result_df['comuna_corregimiento_val'] == 'REVISAR').sum()
        null_count = result_df['comuna_corregimiento_val'].isnull().sum()
        valid_count = len(result_df) - revisar_count - null_count
        
        print(f"\n  Resumen:")
        print(f"    - Intersecciones exitosas: {valid_count}")
        print(f"    - Marcados como REVISAR: {revisar_count}")
        print(f"    - Valores nulos: {null_count}")
        print(f"    - Tasa de éxito: {(valid_count/len(result_df)*100):.1f}%")
    else:
        print("  ⚠️  Columna no encontrada!")
    
    # Analyze barrio_vereda_val
    print(f"\n2. COLUMNA 'barrio_vereda_val':")
    print("-" * 80)
    
    if 'barrio_vereda_val' in result_df.columns:
        barrio_counts = result_df['barrio_vereda_val'].value_counts()
        print(f"  Total de valores únicos: {len(barrio_counts)}")
        print(f"\n  Distribución (Top 15):")
        for value, count in barrio_counts.head(15).items():
            pct = (count / len(result_df)) * 100
            print(f"    • {value}: {count} ({pct:.1f}%)")
        
        revisar_count = (result_df['barrio_vereda_val'] == 'REVISAR').sum()
        null_count = result_df['barrio_vereda_val'].isnull().sum()
        valid_count = len(result_df) - revisar_count - null_count
        
        print(f"\n  Resumen:")
        print(f"    - Intersecciones exitosas: {valid_count}")
        print(f"    - Marcados como REVISAR: {revisar_count}")
        print(f"    - Valores nulos: {null_count}")
        print(f"    - Tasa de éxito: {(valid_count/len(result_df)*100):.1f}%")
    else:
        print("  ⚠️  Columna no encontrada!")
    
    # Analyze exclusion of Infraestructura vial
    print(f"\n3. ANÁLISIS DE EXCLUSIÓN (Infraestructura vial):")
    print("-" * 80)
    
    if 'tipo_equipamiento' in result_df.columns and 'barrio_vereda_val' in result_df.columns:
        # Count infraestructura vial
        infra_vial_mask = result_df['tipo_equipamiento'].str.strip().str.lower() == 'infraestructura vial'
        infra_vial = result_df[infra_vial_mask]
        
        print(f"  Total registros 'Infraestructura vial': {len(infra_vial)}")
        
        if len(infra_vial) > 0:
            # Check barrio_vereda_val for these records
            revisar_in_infra = (infra_vial['barrio_vereda_val'] == 'REVISAR').sum()
            other_in_infra = len(infra_vial) - revisar_in_infra
            
            print(f"\n  Estado de barrio_vereda_val para Infraestructura vial:")
            print(f"    - REVISAR (excluidos correctamente): {revisar_in_infra}")
            print(f"    - Otros valores: {other_in_infra}")
            
            if revisar_in_infra == len(infra_vial):
                print(f"    ✓ Todos los registros de Infraestructura vial fueron excluidos correctamente")
            else:
                print(f"    ⚠️  Algunos registros no fueron excluidos correctamente")
            
            # Show comuna_corregimiento_val for these records (should have values)
            comuna_valid_in_infra = (infra_vial['comuna_corregimiento_val'] != 'REVISAR').sum()
            print(f"\n  Estado de comuna_corregimiento_val para Infraestructura vial:")
            print(f"    - Con intersección exitosa: {comuna_valid_in_infra}")
            print(f"    - REVISAR: {len(infra_vial) - comuna_valid_in_infra}")
            
            # Sample records
            print(f"\n  Muestra de registros (primeros 5):")
            for idx, row in infra_vial.head(5).iterrows():
                nombre = str(row.get('nombre_up', 'N/A'))[:40]
                comuna = row.get('comuna_corregimiento_val', 'N/A')
                barrio = row.get('barrio_vereda_val', 'N/A')
                print(f"    - {nombre}...")
                print(f"      → comuna: {comuna}, barrio: {barrio}")
        else:
            print(f"  ℹ️  No hay registros con tipo_equipamiento = 'Infraestructura vial'")
    else:
        print("  ⚠️  Columnas necesarias no encontradas!")
    
    # Sample comparison
    print(f"\n4. MUESTRA DE REGISTROS (primeros 10):")
    print("-" * 80)
    
    sample_cols = ['nombre_up', 'tipo_equipamiento', 'comuna_corregimiento_val', 'barrio_vereda_val']
    available_cols = [col for col in sample_cols if col in result_df.columns]
    
    if available_cols:
        for idx, row in result_df.head(10).iterrows():
            nombre = str(row.get('nombre_up', 'N/A'))[:40]
            tipo = str(row.get('tipo_equipamiento', 'N/A'))[:30]
            comuna = row.get('comuna_corregimiento_val', 'N/A')
            barrio = str(row.get('barrio_vereda_val', 'N/A'))[:30]
            
            print(f"\n  [{idx+1}] {nombre}...")
            print(f"      Tipo: {tipo}")
            print(f"      Comuna: {comuna}")
            print(f"      Barrio: {barrio}")
    
    print(f"\n{'='*80}")
    print("✓ TEST COMPLETADO")
    print("="*80)


if __name__ == "__main__":
    try:
        test_spatial_intersections_real_data()
    except Exception as e:
        print(f"\n✗ Error durante el test: {e}")
        import traceback
        traceback.print_exc()
