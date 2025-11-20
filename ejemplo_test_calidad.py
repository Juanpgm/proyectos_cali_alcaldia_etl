"""
Ejemplo de uso del script de testing de calidad de datos ETL
=============================================================

Este script demuestra cómo usar el ETLDataQualityTester para verificar
la calidad de los datos transformados.
"""

import pandas as pd
import sys
from pathlib import Path

# Importar el tester
from test_etl_data_quality import ETLDataQualityTester


def ejemplo_basico_con_archivo():
    """Ejemplo básico usando un archivo CSV/Excel."""
    print("="*70)
    print("EJEMPLO 1: Pruebas con archivo CSV")
    print("="*70)
    
    # Crear instancia del tester con ruta al archivo
    tester = ETLDataQualityTester(
        data_path='app_outputs/transformed_data.csv',  # Ajustar ruta según tu caso
        verbose=True
    )
    
    # Cargar datos
    if tester.load_data():
        # Ejecutar todas las pruebas
        resultados = tester.run_all_tests(
            module_path='transformation_app/data_transformation_unidades_proyecto.py'
        )
        
        # Guardar reporte
        tester.save_report('app_outputs/reports/quality_report.json')
        
        return resultados
    else:
        print("No se pudieron cargar los datos")
        return None


def ejemplo_con_dataframe():
    """Ejemplo usando un DataFrame directamente."""
    print("\n" + "="*70)
    print("EJEMPLO 2: Pruebas con DataFrame en memoria")
    print("="*70)
    
    # Crear o cargar DataFrame de ejemplo
    df = pd.DataFrame({
        'estado': ['En Alistamiento', 'En Ejecución', 'Terminado', 'En Ejecución', 'En Alistamiento'],
        'avance_obra': [0.0, 45.5, 100.0, 75.0, 0.0],
        'nickname': ['Proyecto A', 'Proyecto B', 'Proyecto C', 'Proyecto D', 'Proyecto E'],
        'presupuesto_base': [100000, 200000, 150000, 300000, 50000]
    })
    
    # Crear instancia del tester
    tester = ETLDataQualityTester(verbose=True)
    
    # Cargar DataFrame directamente
    tester.load_data(df)
    
    # Ejecutar todas las pruebas
    resultados = tester.run_all_tests()
    
    return resultados


def ejemplo_tests_individuales():
    """Ejemplo ejecutando tests individuales."""
    print("\n" + "="*70)
    print("EJEMPLO 3: Ejecutar tests individuales")
    print("="*70)
    
    # Datos de ejemplo con algunos errores intencionales
    df_con_errores = pd.DataFrame({
        'estado': [
            'En Alistamiento',  # ✓ Correcto: avance=0
            'En Ejecución',     # ✓ Correcto: avance=50
            'En Alistamiento',  # ✗ Error: avance=100, debería ser "Terminado"
            'Estado Inválido',  # ✗ Error: valor no permitido
            'En Ejecución',     # ⚠ Advertencia: avance=0, debería ser "En Alistamiento"
            'Terminado',        # ✓ Correcto: avance=100
        ],
        'avance_obra': [0.0, 50.0, 100.0, 25.0, 0.0, 100.0],
    })
    
    tester = ETLDataQualityTester(verbose=True)
    tester.load_data(df_con_errores)
    
    # Test 1: Congruencia
    print("\n--- TEST 1: Congruencia ---")
    resultado_1 = tester.test_estado_avance_consistency()
    print(f"Inconsistencias encontradas: {len(resultado_1.get('inconsistencies', []))}")
    
    # Test 2: Validación numérica
    print("\n--- TEST 2: Validación numérica ---")
    resultado_2 = tester.test_avance_obra_numeric()
    print(f"Errores numéricos: {len(resultado_2.get('numeric_errors', []))}")
    
    # Test 3: Valores válidos
    print("\n--- TEST 3: Valores válidos ---")
    resultado_3 = tester.test_estado_valid_values()
    print(f"Valores inválidos: {len(resultado_3.get('invalid_values', []))}")
    
    return {
        'congruencia': resultado_1,
        'numerico': resultado_2,
        'valores_validos': resultado_3
    }


def ejemplo_analisis_modulo():
    """Ejemplo de análisis del módulo de transformación."""
    print("\n" + "="*70)
    print("EJEMPLO 4: Análisis de funciones del módulo")
    print("="*70)
    
    tester = ETLDataQualityTester(verbose=True)
    
    # Analizar módulo de transformación
    resultado = tester.test_duplicate_functions(
        module_path='transformation_app/data_transformation_unidades_proyecto.py'
    )
    
    if 'error' not in resultado:
        print(f"\nFunciones duplicadas: {len(resultado.get('duplicate_functions', []))}")
        print(f"Funciones similares: {len(resultado.get('similar_functions', []))}")
        
        print("\nAnálisis de funciones específicas:")
        for func, info in resultado.get('function_analysis', {}).items():
            if info.get('exists'):
                print(f"  ✓ {func}: {info.get('line_count', 0)} líneas")
            else:
                print(f"  ✗ {func}: NO ENCONTRADA")
    
    return resultado


def ejemplo_pipeline_completo():
    """Ejemplo de un pipeline completo de testing."""
    print("\n" + "="*70)
    print("EJEMPLO 5: Pipeline completo de testing")
    print("="*70)
    
    try:
        # 1. Cargar datos transformados
        print("\n1. Cargando datos transformados...")
        data_path = 'app_outputs/transformed_data.csv'
        
        if not Path(data_path).exists():
            print(f"⚠ Archivo no encontrado: {data_path}")
            print("  Usando datos de ejemplo en su lugar...")
            
            # Crear datos de ejemplo
            df = pd.DataFrame({
                'estado': ['En Alistamiento'] * 10 + ['En Ejecución'] * 30 + ['Terminado'] * 10,
                'avance_obra': [0.0] * 10 + [float(x) for x in range(20, 80, 2)] + [100.0] * 10,
                'nickname': [f'Proyecto {i}' for i in range(50)],
                'presupuesto_base': [100000 + (i * 50000) for i in range(50)]
            })
            
            tester = ETLDataQualityTester(verbose=True)
            tester.load_data(df)
        else:
            tester = ETLDataQualityTester(data_path=data_path, verbose=True)
            if not tester.load_data():
                print("Error al cargar datos")
                return None
        
        # 2. Ejecutar suite completa de tests
        print("\n2. Ejecutando suite completa de pruebas...")
        resultados = tester.run_all_tests(
            module_path='transformation_app/data_transformation_unidades_proyecto.py'
        )
        
        # 3. Guardar reporte
        print("\n3. Guardando reporte...")
        output_dir = Path('app_outputs/reports')
        output_dir.mkdir(parents=True, exist_ok=True)
        tester.save_report(str(output_dir / 'quality_report.json'))
        
        # 4. Analizar resultados
        print("\n4. Análisis de resultados:")
        print(f"  Total tests: {tester.test_results['total_tests']}")
        print(f"  Tests pasados: {tester.test_results['passed_tests']}")
        print(f"  Tests fallados: {tester.test_results['failed_tests']}")
        print(f"  Advertencias: {tester.test_results['warnings']}")
        
        # 5. Decisión final
        print("\n5. Decisión final:")
        if tester.test_results['failed_tests'] == 0:
            print("  ✓✓ Los datos están listos para producción")
            return True
        else:
            print("  ✗✗ Los datos necesitan correcciones antes de producción")
            return False
        
    except Exception as e:
        print(f"Error en el pipeline: {str(e)}")
        return None


def main():
    """Ejecutar todos los ejemplos."""
    print("="*70)
    print("EJEMPLOS DE USO DEL TESTER DE CALIDAD ETL")
    print("="*70)
    
    # Descomentar el ejemplo que quieras ejecutar:
    
    # ejemplo_basico_con_archivo()
    # ejemplo_con_dataframe()
    # ejemplo_tests_individuales()
    # ejemplo_analisis_modulo()
    ejemplo_pipeline_completo()
    
    print("\n" + "="*70)
    print("Ejemplos completados")
    print("="*70)


if __name__ == '__main__':
    main()
