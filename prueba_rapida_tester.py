"""
Script de Prueba Rápida - Verificación del Tester de Calidad ETL
================================================================

Este script ejecuta una prueba rápida con datos sintéticos para verificar
que el sistema de testing funciona correctamente.
"""

import pandas as pd
import sys
from pathlib import Path

# Agregar el directorio actual al path para importar el módulo
sys.path.insert(0, str(Path(__file__).parent))

from test_etl_data_quality import ETLDataQualityTester


def crear_datos_prueba():
    """Crea un DataFrame de prueba con casos buenos y malos."""
    
    print("Creando datos de prueba...")
    
    # Datos que PASAN todas las pruebas
    datos_buenos = {
        'estado': [
            'En Alistamiento',  # avance=0 ✓
            'En Alistamiento',  # avance=0 ✓
            'En Ejecución',     # avance=50 ✓
            'En Ejecución',     # avance=75 ✓
            'Terminado',        # avance=100 ✓
            'Terminado',        # avance=100 ✓
        ],
        'avance_obra': [0.0, 0.0, 50.0, 75.0, 100.0, 100.0],
        'nickname': ['Proyecto 1', 'Proyecto 2', 'Proyecto 3', 
                    'Proyecto 4', 'Proyecto 5', 'Proyecto 6'],
        'presupuesto_base': [100000, 200000, 150000, 300000, 250000, 180000]
    }
    
    # Datos con ERRORES para probar detección
    datos_con_errores = {
        'estado': [
            'En Alistamiento',  # avance=0 ✓
            'En Ejecución',     # avance=0 ✗ ERROR CRÍTICO
            'En Alistamiento',  # avance=100 ⚠ ADVERTENCIA
            'Estado Inválido',  # valor no permitido ✗ ERROR CRÍTICO
            'Terminado',        # avance=100 ✓
            'En Ejecución',     # avance=50 ✓
        ],
        'avance_obra': [0.0, 0.0, 100.0, 25.0, 100.0, 50.0],
        'nickname': ['Proyecto A', 'Proyecto B', 'Proyecto C', 
                    'Proyecto D', 'Proyecto E', 'Proyecto F'],
        'presupuesto_base': [100000, 200000, 150000, 300000, 250000, 180000]
    }
    
    df_buenos = pd.DataFrame(datos_buenos)
    df_errores = pd.DataFrame(datos_con_errores)
    
    print(f"✓ Creados {len(df_buenos)} registros de datos buenos")
    print(f"✓ Creados {len(df_errores)} registros con errores intencionados")
    
    return df_buenos, df_errores


def prueba_datos_buenos():
    """Prueba con datos que deberían pasar todas las validaciones."""
    
    print("\n" + "="*70)
    print("PRUEBA 1: Datos que DEBERÍAN PASAR todas las validaciones")
    print("="*70)
    
    df_buenos, _ = crear_datos_prueba()
    
    tester = ETLDataQualityTester(verbose=True)
    tester.load_data(df_buenos)
    
    # Ejecutar solo las pruebas de datos (sin análisis de módulo)
    print("\n--- Test 1: Congruencia ---")
    resultado_1 = tester.test_estado_avance_consistency()
    
    print("\n--- Test 2: Validación Numérica ---")
    resultado_2 = tester.test_avance_obra_numeric()
    
    print("\n--- Test 3: Valores Válidos ---")
    resultado_3 = tester.test_estado_valid_values()
    
    # Verificar resultados
    print("\n" + "-"*70)
    print("RESULTADOS DE PRUEBA 1:")
    print("-"*70)
    
    if (tester.test_results['failed_tests'] == 0 and 
        tester.test_results['warnings'] == 0):
        print("✓✓ EXITOSO: Todos los tests pasaron sin errores ni advertencias")
        return True
    else:
        print("✗✗ FALLÓ: Se esperaba que todos los tests pasaran")
        print(f"   Tests fallados: {tester.test_results['failed_tests']}")
        print(f"   Advertencias: {tester.test_results['warnings']}")
        return False


def prueba_datos_con_errores():
    """Prueba con datos que deberían FALLAR las validaciones."""
    
    print("\n" + "="*70)
    print("PRUEBA 2: Datos que DEBERÍAN FALLAR las validaciones")
    print("="*70)
    
    _, df_errores = crear_datos_prueba()
    
    tester = ETLDataQualityTester(verbose=True)
    tester.load_data(df_errores)
    
    # Ejecutar solo las pruebas de datos
    print("\n--- Test 1: Congruencia ---")
    resultado_1 = tester.test_estado_avance_consistency()
    
    print("\n--- Test 2: Validación Numérica ---")
    resultado_2 = tester.test_avance_obra_numeric()
    
    print("\n--- Test 3: Valores Válidos ---")
    resultado_3 = tester.test_estado_valid_values()
    
    # Verificar resultados
    print("\n" + "-"*70)
    print("RESULTADOS DE PRUEBA 2:")
    print("-"*70)
    
    errores_esperados = {
        'congruencia': len(resultado_1.get('inconsistencies', [])) > 0,
        'valores_invalidos': len(resultado_3.get('invalid_values', [])) > 0
    }
    
    if errores_esperados['congruencia'] and errores_esperados['valores_invalidos']:
        print("✓✓ EXITOSO: Se detectaron los errores esperados")
        print(f"   Inconsistencias de congruencia: {len(resultado_1.get('inconsistencies', []))}")
        print(f"   Valores inválidos detectados: {len(resultado_3.get('invalid_values', []))}")
        return True
    else:
        print("✗✗ FALLÓ: No se detectaron todos los errores esperados")
        print(f"   Congruencia detectada: {errores_esperados['congruencia']}")
        print(f"   Valores inválidos detectados: {errores_esperados['valores_invalidos']}")
        return False


def prueba_analisis_modulo():
    """Prueba el análisis del módulo de transformación."""
    
    print("\n" + "="*70)
    print("PRUEBA 3: Análisis del módulo de transformación")
    print("="*70)
    
    tester = ETLDataQualityTester(verbose=True)
    
    # Buscar el módulo de transformación
    module_path = Path(__file__).parent / 'transformation_app' / 'data_transformation_unidades_proyecto.py'
    
    if not module_path.exists():
        print(f"⚠ ADVERTENCIA: Módulo no encontrado en {module_path}")
        print("  Esta prueba solo funciona si el módulo de transformación existe")
        return None
    
    resultado = tester.test_duplicate_functions(str(module_path))
    
    if 'error' in resultado:
        print(f"✗ Error al analizar módulo: {resultado['error']}")
        return False
    
    # Verificar resultados
    print("\n" + "-"*70)
    print("RESULTADOS DE PRUEBA 3:")
    print("-"*70)
    
    duplicados = len(resultado.get('duplicate_functions', []))
    similares = len(resultado.get('similar_functions', []))
    
    print(f"Funciones duplicadas encontradas: {duplicados}")
    print(f"Funciones similares encontradas: {similares}")
    
    if duplicados == 0:
        print("✓✓ EXITOSO: No se encontraron funciones duplicadas")
        return True
    else:
        print("⚠ ADVERTENCIA: Se encontraron funciones duplicadas")
        return False


def prueba_guardado_reporte():
    """Prueba que el reporte se guarde correctamente."""
    
    print("\n" + "="*70)
    print("PRUEBA 4: Guardado de reporte JSON")
    print("="*70)
    
    df_buenos, _ = crear_datos_prueba()
    
    tester = ETLDataQualityTester(verbose=False)  # Sin verbose para esta prueba
    tester.load_data(df_buenos)
    
    # Ejecutar tests
    tester.test_estado_avance_consistency()
    tester.test_avance_obra_numeric()
    tester.test_estado_valid_values()
    
    # Guardar reporte
    output_dir = Path('test_outputs')
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / 'test_quality_report.json'
    
    tester.save_report(str(output_path))
    
    # Verificar que se creó el archivo
    if output_path.exists():
        print(f"✓✓ EXITOSO: Reporte guardado en {output_path}")
        
        # Verificar que se puede leer
        import json
        with open(output_path, 'r', encoding='utf-8') as f:
            reporte = json.load(f)
        
        print(f"   Reporte contiene {len(reporte.get('details', []))} entradas")
        return True
    else:
        print("✗✗ FALLÓ: No se pudo guardar el reporte")
        return False


def main():
    """Ejecutar todas las pruebas."""
    
    print("="*70)
    print("SUITE DE PRUEBAS RÁPIDAS - TESTER DE CALIDAD ETL")
    print("="*70)
    print("\nEste script verifica que el sistema de testing funcione correctamente")
    print("usando datos sintéticos con casos conocidos.\n")
    
    resultados = []
    
    # Ejecutar pruebas
    try:
        resultados.append(('Datos Buenos', prueba_datos_buenos()))
    except Exception as e:
        print(f"\n✗ Error en Prueba 1: {str(e)}")
        resultados.append(('Datos Buenos', False))
    
    try:
        resultados.append(('Datos con Errores', prueba_datos_con_errores()))
    except Exception as e:
        print(f"\n✗ Error en Prueba 2: {str(e)}")
        resultados.append(('Datos con Errores', False))
    
    try:
        resultado_modulo = prueba_analisis_modulo()
        if resultado_modulo is not None:
            resultados.append(('Análisis de Módulo', resultado_modulo))
    except Exception as e:
        print(f"\n✗ Error en Prueba 3: {str(e)}")
    
    try:
        resultados.append(('Guardado de Reporte', prueba_guardado_reporte()))
    except Exception as e:
        print(f"\n✗ Error en Prueba 4: {str(e)}")
        resultados.append(('Guardado de Reporte', False))
    
    # Resumen final
    print("\n" + "="*70)
    print("RESUMEN DE PRUEBAS")
    print("="*70)
    
    for nombre, resultado in resultados:
        estado = "✓ PASÓ" if resultado else "✗ FALLÓ"
        print(f"{estado:12} | {nombre}")
    
    total = len(resultados)
    exitosas = sum(1 for _, r in resultados if r)
    
    print(f"\nTotal: {exitosas}/{total} pruebas exitosas")
    
    if exitosas == total:
        print("\n🎉 EXCELENTE: Todas las pruebas pasaron!")
        print("El sistema de testing de calidad está funcionando correctamente.")
        return 0
    else:
        print(f"\n⚠ ATENCIÓN: {total - exitosas} prueba(s) fallaron")
        print("Revisar los errores anteriores.")
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
