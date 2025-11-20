"""
Script para ejecutar pruebas de calidad con datos reales de la ETL
==================================================================

Este script busca automáticamente los datos transformados más recientes
y ejecuta la suite completa de pruebas de calidad.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import glob

from test_etl_data_quality import ETLDataQualityTester


def encontrar_archivo_mas_reciente(patron: str) -> str:
    """
    Encuentra el archivo más reciente que coincida con el patrón.
    
    Args:
        patron: Patrón de búsqueda (ej: 'app_outputs/*.csv')
        
    Returns:
        Ruta al archivo más reciente o None
    """
    archivos = glob.glob(patron)
    
    if not archivos:
        return None
    
    # Ordenar por fecha de modificación (más reciente primero)
    archivos_con_fecha = [(f, os.path.getmtime(f)) for f in archivos]
    archivos_ordenados = sorted(archivos_con_fecha, key=lambda x: x[1], reverse=True)
    
    return archivos_ordenados[0][0]


def buscar_datos_transformados():
    """
    Busca automáticamente los datos transformados en las ubicaciones comunes.
    
    Returns:
        Ruta al archivo de datos o None
    """
    print("🔍 Buscando datos transformados...")
    
    # Ubicaciones posibles donde podrían estar los datos
    ubicaciones = [
        'app_outputs/*.csv',
        'app_outputs/transformed_*.csv',
        'app_outputs/reports/*.csv',
        'test_outputs/*.csv',
        'context/*.geojson',
        'output/*.csv',
    ]
    
    for patron in ubicaciones:
        archivo = encontrar_archivo_mas_reciente(patron)
        if archivo:
            print(f"✓ Datos encontrados: {archivo}")
            fecha_modificacion = datetime.fromtimestamp(os.path.getmtime(archivo))
            print(f"  Última modificación: {fecha_modificacion.strftime('%Y-%m-%d %H:%M:%S')}")
            return archivo
    
    print("⚠ No se encontraron datos transformados en las ubicaciones comunes")
    return None


def ejecutar_pruebas_con_datos_reales(data_path: str = None):
    """
    Ejecuta pruebas de calidad con datos reales.
    
    Args:
        data_path: Ruta específica al archivo de datos (opcional)
    """
    print("="*70)
    print("PRUEBAS DE CALIDAD CON DATOS REALES DE LA ETL")
    print("="*70)
    print()
    
    # Si no se especificó ruta, buscar automáticamente
    if data_path is None:
        data_path = buscar_datos_transformados()
        
        if data_path is None:
            print("\n❌ ERROR: No se pudieron encontrar datos para probar")
            print("\nOpciones:")
            print("1. Ejecutar primero la ETL para generar datos")
            print("2. Especificar ruta manualmente:")
            print("   python ejecutar_pruebas_datos_reales.py --data <ruta_al_archivo>")
            return False
    
    # Verificar que el archivo existe
    if not os.path.exists(data_path):
        print(f"\n❌ ERROR: El archivo no existe: {data_path}")
        return False
    
    print(f"\n📊 Archivo a probar: {data_path}")
    
    # Verificar extensión del archivo
    extension = os.path.splitext(data_path)[1].lower()
    if extension not in ['.csv', '.xlsx', '.xls', '.json', '.geojson']:
        print(f"\n⚠ ADVERTENCIA: Extensión de archivo no estándar: {extension}")
        print("  Formatos soportados: .csv, .xlsx, .xls, .json, .geojson")
    
    # Crear directorio de reportes si no existe
    report_dir = Path('app_outputs/reports')
    report_dir.mkdir(parents=True, exist_ok=True)
    
    # Generar nombre de reporte con timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = report_dir / f'quality_report_{timestamp}.json'
    
    try:
        # Crear tester
        print("\n" + "="*70)
        print("INICIANDO PRUEBAS DE CALIDAD")
        print("="*70)
        
        tester = ETLDataQualityTester(data_path=data_path, verbose=True)
        
        # Cargar datos
        print("\n1️⃣ Cargando datos...")
        if not tester.load_data():
            print("❌ ERROR: No se pudieron cargar los datos")
            return False
        
        print(f"   Total de registros: {len(tester.df)}")
        print(f"   Columnas disponibles: {len(tester.df.columns)}")
        
        # Verificar columnas necesarias
        columnas_requeridas = ['estado', 'avance_obra']
        columnas_faltantes = [col for col in columnas_requeridas if col not in tester.df.columns]
        
        if columnas_faltantes:
            print(f"\n⚠ ADVERTENCIA: Columnas faltantes: {columnas_faltantes}")
            print("  Algunas pruebas pueden no ejecutarse")
        
        # Ejecutar todas las pruebas
        print("\n2️⃣ Ejecutando suite completa de pruebas...")
        
        # Buscar módulo de transformación
        module_path = Path(__file__).parent / 'transformation_app' / 'data_transformation_unidades_proyecto.py'
        module_path_str = str(module_path) if module_path.exists() else None
        
        resultados = tester.run_all_tests(module_path=module_path_str)
        
        # Guardar reporte
        print("\n3️⃣ Guardando reporte...")
        tester.save_report(str(report_path))
        print(f"   📄 Reporte guardado en: {report_path}")
        
        # Análisis de resultados
        print("\n" + "="*70)
        print("4️⃣ ANÁLISIS DE RESULTADOS")
        print("="*70)
        
        total_tests = tester.test_results['total_tests']
        passed = tester.test_results['passed_tests']
        failed = tester.test_results['failed_tests']
        warnings = tester.test_results['warnings']
        
        print(f"\n📊 Estadísticas:")
        print(f"   Total de pruebas: {total_tests}")
        print(f"   ✅ Pasadas: {passed} ({(passed/total_tests*100):.1f}%)")
        print(f"   ❌ Falladas: {failed} ({(failed/total_tests*100):.1f}%)")
        print(f"   ⚠️  Advertencias: {warnings}")
        
        # Recomendaciones según resultados
        print("\n" + "="*70)
        print("5️⃣ RECOMENDACIONES")
        print("="*70)
        
        if failed == 0 and warnings == 0:
            print("\n✅ EXCELENTE: Los datos están listos para producción")
            print("   → Puedes proceder con la carga a Firebase/S3")
            print("   → No se requieren correcciones")
            
        elif failed == 0 and warnings > 0:
            print("\n⚠️  BUENO: Los datos pasan todas las pruebas críticas")
            print(f"   → Hay {warnings} advertencia(s) a revisar")
            print("   → Puedes proceder con la carga, pero revisa las advertencias")
            print(f"   → Ver detalles en: {report_path}")
            
        else:
            print(f"\n❌ ATENCIÓN: {failed} prueba(s) crítica(s) fallaron")
            print("   → NO CARGAR datos a producción hasta corregir errores")
            print("   → Revisar logs anteriores para detalles específicos")
            print(f"   → Consultar reporte completo: {report_path}")
            
            # Mostrar detalles de errores críticos
            print("\n   Errores críticos detectados:")
            for detalle in tester.test_results['details']:
                if not detalle['passed']:
                    print(f"     • {detalle['test_name']}")
        
        print("\n" + "="*70)
        
        return failed == 0
        
    except Exception as e:
        print(f"\n❌ ERROR durante las pruebas: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Función principal."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Ejecutar pruebas de calidad con datos reales de la ETL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

  # Buscar y probar datos automáticamente
  python ejecutar_pruebas_datos_reales.py
  
  # Especificar archivo de datos
  python ejecutar_pruebas_datos_reales.py --data app_outputs/transformed_data.csv
  
  # Usar archivo GeoJSON
  python ejecutar_pruebas_datos_reales.py --data context/unidades_proyecto.geojson
        """
    )
    
    parser.add_argument(
        '--data',
        type=str,
        help='Ruta al archivo de datos transformados (opcional, se buscará automáticamente si no se especifica)'
    )
    
    args = parser.parse_args()
    
    # Ejecutar pruebas
    exito = ejecutar_pruebas_con_datos_reales(args.data)
    
    # Código de salida
    if exito:
        print("\n✅ Pruebas completadas exitosamente")
        sys.exit(0)
    else:
        print("\n❌ Pruebas completadas con errores")
        sys.exit(1)


if __name__ == '__main__':
    main()
