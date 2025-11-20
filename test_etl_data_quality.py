"""
ETL Data Quality Testing Script
================================

Este script verifica la calidad de los datos transformados por la ETL, incluyendo:
1. Congruencia entre 'estado' y 'avance_obra'
2. Validación de que 'avance_obra' sea numérico
3. Validación de valores permitidos en 'estado'
4. Detección de funciones duplicadas o intrusas que puedan añadir errores

Author: ETL QA Team
Date: November 2025
"""

import sys
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
import inspect
import importlib.util
from pathlib import Path
import json
from datetime import datetime


class ETLDataQualityTester:
    """Clase para realizar pruebas de calidad de datos de la ETL."""
    
    # Valores permitidos para estado
    VALID_ESTADO_VALUES = {'En alistamiento', 'En ejecución', 'Terminado'}
    
    def __init__(self, data_path: Optional[str] = None, verbose: bool = True):
        """
        Inicializar el tester de calidad de datos.
        
        Args:
            data_path: Ruta al archivo de datos transformados (CSV/Excel/JSON)
            verbose: Si True, imprime información detallada durante las pruebas
        """
        self.data_path = data_path
        self.verbose = verbose
        self.df = None
        self.test_results = {
            'timestamp': datetime.now().isoformat(),
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'warnings': 0,
            'details': []
        }
        
    def log(self, message: str, level: str = 'INFO'):
        """Registrar mensajes en la consola si verbose está activado."""
        if self.verbose:
            prefix = {
                'INFO': '✓',
                'WARNING': '⚠',
                'ERROR': '✗',
                'SUCCESS': '✓✓'
            }.get(level, '•')
            print(f"{prefix} {message}")
    
    def load_data(self, data_source: Optional[Any] = None) -> bool:
        """
        Cargar datos desde archivo o DataFrame.
        
        Args:
            data_source: Ruta al archivo o DataFrame directamente
            
        Returns:
            True si los datos se cargaron correctamente, False en caso contrario
        """
        try:
            if data_source is None and self.data_path is None:
                self.log("No se proporcionó fuente de datos", 'ERROR')
                return False
                
            if isinstance(data_source, pd.DataFrame):
                self.df = data_source.copy()
                self.log(f"DataFrame cargado con {len(self.df)} registros", 'SUCCESS')
                return True
            
            file_path = data_source if data_source else self.data_path
            
            if file_path.endswith('.csv'):
                self.df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                self.df = pd.read_excel(file_path)
            elif file_path.endswith('.json'):
                self.df = pd.read_json(file_path)
            else:
                self.log(f"Formato de archivo no soportado: {file_path}", 'ERROR')
                return False
            
            self.log(f"Datos cargados exitosamente: {len(self.df)} registros", 'SUCCESS')
            return True
            
        except Exception as e:
            self.log(f"Error al cargar datos: {str(e)}", 'ERROR')
            return False
    
    def _record_test(self, test_name: str, passed: bool, details: Dict[str, Any]):
        """Registrar resultado de una prueba."""
        self.test_results['total_tests'] += 1
        if passed:
            self.test_results['passed_tests'] += 1
        else:
            self.test_results['failed_tests'] += 1
        
        self.test_results['details'].append({
            'test_name': test_name,
            'passed': passed,
            'timestamp': datetime.now().isoformat(),
            **details
        })
    
    # ==================================================================
    # TEST 1: Congruencia entre 'estado' y 'avance_obra'
    # ==================================================================
    
    def test_estado_avance_consistency(self) -> Dict[str, Any]:
        """
        Verifica que la variable 'estado' sea congruente con 'avance_obra'.
        
        Reglas de negocio:
        - Si avance_obra = 0, estado debe ser 'En alistamiento'
        - Si avance_obra > 0 y < 100, estado debería ser 'En Ejecución'
        - Si avance_obra = 100, estado debería ser 'Terminado'
        
        Returns:
            Diccionario con resultados de la prueba
        """
        self.log("\n" + "="*70)
        self.log("TEST 1: Congruencia entre 'estado' y 'avance_obra'")
        self.log("="*70)
        
        if self.df is None:
            return {'error': 'No hay datos cargados'}
        
        if 'estado' not in self.df.columns or 'avance_obra' not in self.df.columns:
            self.log("Columnas 'estado' o 'avance_obra' no encontradas", 'ERROR')
            return {'error': 'Columnas requeridas no encontradas'}
        
        results = {
            'total_records': len(self.df),
            'inconsistencies': [],
            'warnings': [],
            'summary': {}
        }
        
        # Crear copia del DataFrame para análisis
        df_test = self.df[['estado', 'avance_obra']].copy()
        
        # Contar casos por categoría
        zero_avance = df_test['avance_obra'] == 0
        partial_avance = (df_test['avance_obra'] > 0) & (df_test['avance_obra'] < 100)
        complete_avance = df_test['avance_obra'] == 100
        
        # Verificar congruencia: avance_obra = 0 → estado = 'En alistamiento'
        inconsistent_zero = zero_avance & (df_test['estado'] != 'En alistamiento')
        if inconsistent_zero.any():
            count = inconsistent_zero.sum()
            indices = df_test[inconsistent_zero].index.tolist()[:10]  # Primeros 10
            results['inconsistencies'].append({
                'rule': 'avance_obra = 0 debe tener estado = "En alistamiento"',
                'count': int(count),
                'sample_indices': indices,
                'severity': 'CRITICAL'
            })
            self.log(f"✗ CRÍTICO: {count} registros con avance_obra=0 pero estado≠'En alistamiento'", 'ERROR')
        else:
            self.log(f"✓ Todos los registros con avance_obra=0 tienen estado='En alistamiento'", 'SUCCESS')
        
        # Advertencia: avance_obra > 0 pero estado = 'En alistamiento'
        warning_alistamiento = (df_test['avance_obra'] > 0) & (df_test['estado'] == 'En alistamiento')
        if warning_alistamiento.any():
            count = warning_alistamiento.sum()
            indices = df_test[warning_alistamiento].index.tolist()[:10]
            results['warnings'].append({
                'rule': 'avance_obra > 0 con estado = "En alistamiento" es sospechoso',
                'count': int(count),
                'sample_indices': indices
            })
            self.log(f"⚠ ADVERTENCIA: {count} registros con avance_obra>0 pero estado='En alistamiento'", 'WARNING')
            self.test_results['warnings'] += 1
        
        # Advertencia: avance_obra = 100 pero estado ≠ 'Terminado'
        warning_terminado = complete_avance & (df_test['estado'] != 'Terminado')
        if warning_terminado.any():
            count = warning_terminado.sum()
            indices = df_test[warning_terminado].index.tolist()[:10]
            results['warnings'].append({
                'rule': 'avance_obra = 100 debería tener estado = "Terminado"',
                'count': int(count),
                'sample_indices': indices
            })
            self.log(f"⚠ ADVERTENCIA: {count} registros con avance_obra=100 pero estado≠'Terminado'", 'WARNING')
            self.test_results['warnings'] += 1
        
        # Resumen estadístico
        results['summary'] = {
            'zero_avance_count': int(zero_avance.sum()),
            'partial_avance_count': int(partial_avance.sum()),
            'complete_avance_count': int(complete_avance.sum()),
            'estado_distribution': df_test['estado'].value_counts().to_dict(),
            'consistency_rate': f"{((1 - inconsistent_zero.sum() / len(df_test)) * 100):.2f}%"
        }
        
        # Registrar resultado
        test_passed = len(results['inconsistencies']) == 0
        self._record_test('estado_avance_consistency', test_passed, results)
        
        if test_passed:
            self.log("\n✓✓ TEST 1 PASADO: Estado y avance_obra son congruentes", 'SUCCESS')
        else:
            self.log(f"\n✗✗ TEST 1 FALLADO: Se encontraron {len(results['inconsistencies'])} inconsistencias críticas", 'ERROR')
        
        return results
    
    # ==================================================================
    # TEST 2: Validación de tipos de datos en 'avance_obra'
    # ==================================================================
    
    def test_avance_obra_numeric(self) -> Dict[str, Any]:
        """
        Verifica que 'avance_obra' solo maneje datos numéricos válidos.
        
        Validaciones:
        - Todos los valores deben ser numéricos (int o float)
        - No debe haber valores NaN o None
        - Los valores deben estar en el rango [0, 100]
        - No debe haber valores negativos
        
        Returns:
            Diccionario con resultados de la prueba
        """
        self.log("\n" + "="*70)
        self.log("TEST 2: Validación de datos numéricos en 'avance_obra'")
        self.log("="*70)
        
        if self.df is None:
            return {'error': 'No hay datos cargados'}
        
        if 'avance_obra' not in self.df.columns:
            self.log("Columna 'avance_obra' no encontrada", 'ERROR')
            return {'error': 'Columna requerida no encontrada'}
        
        results = {
            'total_records': len(self.df),
            'numeric_errors': [],
            'range_errors': [],
            'statistics': {}
        }
        
        avance_serie = self.df['avance_obra']
        
        # Verificar tipos de datos
        non_numeric = []
        for idx, val in enumerate(avance_serie):
            if not isinstance(val, (int, float, np.integer, np.floating)):
                non_numeric.append({
                    'index': idx,
                    'value': str(val),
                    'type': str(type(val))
                })
        
        if non_numeric:
            results['numeric_errors'].append({
                'error': 'Valores no numéricos encontrados',
                'count': len(non_numeric),
                'samples': non_numeric[:10]
            })
            self.log(f"✗ CRÍTICO: {len(non_numeric)} valores no numéricos en 'avance_obra'", 'ERROR')
        else:
            self.log("✓ Todos los valores de 'avance_obra' son numéricos", 'SUCCESS')
        
        # Verificar NaN/None
        null_count = avance_serie.isna().sum()
        if null_count > 0:
            results['numeric_errors'].append({
                'error': 'Valores nulos (NaN/None) encontrados',
                'count': int(null_count)
            })
            self.log(f"✗ CRÍTICO: {null_count} valores nulos en 'avance_obra'", 'ERROR')
        else:
            self.log("✓ No hay valores nulos en 'avance_obra'", 'SUCCESS')
        
        # Verificar rango [0, 100]
        if not non_numeric and null_count == 0:
            out_of_range = (avance_serie < 0) | (avance_serie > 100)
            if out_of_range.any():
                count = out_of_range.sum()
                samples = self.df.loc[out_of_range, 'avance_obra'].head(10).tolist()
                results['range_errors'].append({
                    'error': 'Valores fuera del rango [0, 100]',
                    'count': int(count),
                    'samples': samples
                })
                self.log(f"✗ CRÍTICO: {count} valores fuera del rango [0, 100]", 'ERROR')
            else:
                self.log("✓ Todos los valores están en el rango [0, 100]", 'SUCCESS')
            
            # Estadísticas descriptivas
            results['statistics'] = {
                'mean': float(avance_serie.mean()),
                'median': float(avance_serie.median()),
                'std': float(avance_serie.std()),
                'min': float(avance_serie.min()),
                'max': float(avance_serie.max()),
                'q25': float(avance_serie.quantile(0.25)),
                'q75': float(avance_serie.quantile(0.75))
            }
            
            self.log(f"  Estadísticas: Media={results['statistics']['mean']:.2f}, "
                    f"Mediana={results['statistics']['median']:.2f}, "
                    f"Min={results['statistics']['min']:.2f}, "
                    f"Max={results['statistics']['max']:.2f}")
        
        # Registrar resultado
        test_passed = len(results['numeric_errors']) == 0 and len(results['range_errors']) == 0
        self._record_test('avance_obra_numeric', test_passed, results)
        
        if test_passed:
            self.log("\n✓✓ TEST 2 PASADO: avance_obra contiene solo datos numéricos válidos", 'SUCCESS')
        else:
            total_errors = len(results['numeric_errors']) + len(results['range_errors'])
            self.log(f"\n✗✗ TEST 2 FALLADO: Se encontraron {total_errors} tipos de errores", 'ERROR')
        
        return results
    
    # ==================================================================
    # TEST 3: Validación de valores permitidos en 'estado'
    # ==================================================================
    
    def test_estado_valid_values(self) -> Dict[str, Any]:
        """
        Verifica que 'estado' solo tome los valores permitidos.
        
        Valores permitidos:
        - 'En alistamiento'
        - 'En Ejecución'
        - 'Terminado'
        
        Returns:
            Diccionario con resultados de la prueba
        """
        self.log("\n" + "="*70)
        self.log("TEST 3: Validación de valores permitidos en 'estado'")
        self.log("="*70)
        
        if self.df is None:
            return {'error': 'No hay datos cargados'}
        
        if 'estado' not in self.df.columns:
            self.log("Columna 'estado' no encontrada", 'ERROR')
            return {'error': 'Columna requerida no encontrada'}
        
        results = {
            'total_records': len(self.df),
            'valid_values': list(self.VALID_ESTADO_VALUES),
            'invalid_values': [],
            'distribution': {}
        }
        
        estado_serie = self.df['estado']
        
        # Verificar valores nulos
        null_count = estado_serie.isna().sum()
        if null_count > 0:
            results['invalid_values'].append({
                'value': 'NULL/NaN',
                'count': int(null_count),
                'percentage': f"{(null_count / len(self.df) * 100):.2f}%"
            })
            self.log(f"⚠ ADVERTENCIA: {null_count} valores nulos en 'estado'", 'WARNING')
            self.test_results['warnings'] += 1
        
        # Encontrar valores inválidos
        unique_estados = estado_serie.dropna().unique()
        invalid_estados = set(unique_estados) - self.VALID_ESTADO_VALUES
        
        if invalid_estados:
            for invalid_val in invalid_estados:
                count = (estado_serie == invalid_val).sum()
                results['invalid_values'].append({
                    'value': invalid_val,
                    'count': int(count),
                    'percentage': f"{(count / len(self.df) * 100):.2f}%"
                })
                self.log(f"✗ CRÍTICO: Valor inválido '{invalid_val}' encontrado {count} veces", 'ERROR')
        else:
            self.log("✓ Todos los valores de 'estado' son válidos", 'SUCCESS')
        
        # Distribución de valores
        results['distribution'] = estado_serie.value_counts().to_dict()
        
        self.log("\nDistribución de valores en 'estado':")
        for estado, count in results['distribution'].items():
            percentage = (count / len(self.df)) * 100
            valid_mark = "✓" if estado in self.VALID_ESTADO_VALUES else "✗"
            self.log(f"  {valid_mark} {estado}: {count} ({percentage:.2f}%)")
        
        # Registrar resultado
        test_passed = len(invalid_estados) == 0 and null_count == 0
        self._record_test('estado_valid_values', test_passed, results)
        
        if test_passed:
            self.log("\n✓✓ TEST 3 PASADO: Todos los valores de 'estado' son válidos", 'SUCCESS')
        else:
            self.log(f"\n✗✗ TEST 3 FALLADO: Se encontraron {len(invalid_estados)} valores inválidos", 'ERROR')
        
        return results
    
    # ==================================================================
    # TEST 4: Detección de funciones duplicadas o intrusas
    # ==================================================================
    
    def test_duplicate_functions(self, module_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Revisa que no haya funciones duplicadas o intrusas en los módulos de transformación.
        
        Args:
            module_path: Ruta al módulo de transformación a analizar
            
        Returns:
            Diccionario con resultados de la prueba
        """
        self.log("\n" + "="*70)
        self.log("TEST 4: Detección de funciones duplicadas o intrusas")
        self.log("="*70)
        
        if module_path is None:
            # Buscar módulos de transformación en el directorio actual
            transformation_dir = Path(__file__).parent / 'transformation_app'
            if not transformation_dir.exists():
                self.log("Directorio 'transformation_app' no encontrado", 'WARNING')
                return {'error': 'No se pudo encontrar el módulo de transformación'}
            
            module_path = str(transformation_dir / 'data_transformation_unidades_proyecto.py')
        
        results = {
            'module_path': module_path,
            'duplicate_functions': [],
            'similar_functions': [],
            'function_analysis': {}
        }
        
        try:
            # Cargar el módulo
            spec = importlib.util.spec_from_file_location("transformation_module", module_path)
            if spec is None or spec.loader is None:
                self.log(f"No se pudo cargar el módulo: {module_path}", 'ERROR')
                return {'error': 'No se pudo cargar el módulo'}
            
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Obtener todas las funciones del módulo
            functions = {}
            for name, obj in inspect.getmembers(module):
                if inspect.isfunction(obj) and obj.__module__ == module.__name__:
                    # Obtener el código fuente
                    try:
                        source = inspect.getsource(obj)
                        functions[name] = {
                            'object': obj,
                            'source': source,
                            'source_hash': hash(source),
                            'line_count': len(source.split('\n')),
                            'signature': str(inspect.signature(obj))
                        }
                    except Exception as e:
                        self.log(f"No se pudo obtener el código de {name}: {e}", 'WARNING')
            
            self.log(f"✓ Analizadas {len(functions)} funciones en el módulo")
            
            # Buscar funciones duplicadas (mismo código exacto)
            seen_hashes = {}
            for name, info in functions.items():
                src_hash = info['source_hash']
                if src_hash in seen_hashes:
                    results['duplicate_functions'].append({
                        'function1': seen_hashes[src_hash],
                        'function2': name,
                        'severity': 'CRITICAL'
                    })
                    self.log(f"✗ CRÍTICO: Función duplicada detectada: '{seen_hashes[src_hash]}' y '{name}'", 'ERROR')
                else:
                    seen_hashes[src_hash] = name
            
            if not results['duplicate_functions']:
                self.log("✓ No se encontraron funciones con código idéntico", 'SUCCESS')
            
            # Buscar funciones sospechosamente similares (nombres similares)
            function_names = list(functions.keys())
            for i, name1 in enumerate(function_names):
                for name2 in function_names[i+1:]:
                    # Calcular similitud en nombres
                    if self._similar_names(name1, name2):
                        results['similar_functions'].append({
                            'function1': name1,
                            'function2': name2,
                            'signature1': functions[name1]['signature'],
                            'signature2': functions[name2]['signature']
                        })
                        self.log(f"⚠ ADVERTENCIA: Funciones con nombres similares: '{name1}' y '{name2}'", 'WARNING')
                        self.test_results['warnings'] += 1
            
            # Análisis específico para funciones relacionadas con 'estado' y 'avance_obra'
            target_functions = ['normalize_estado_values', 'standardize_estado', 
                              'clean_numeric_column', 'clean_numeric_column_safe']
            
            results['function_analysis'] = {}
            for target in target_functions:
                if target in functions:
                    results['function_analysis'][target] = {
                        'exists': True,
                        'line_count': functions[target]['line_count'],
                        'signature': functions[target]['signature']
                    }
                    self.log(f"✓ Función '{target}' encontrada ({functions[target]['line_count']} líneas)")
                else:
                    results['function_analysis'][target] = {'exists': False}
                    self.log(f"⚠ Función '{target}' no encontrada", 'WARNING')
            
            # Verificar que solo haya una función para normalizar estado
            estado_functions = [name for name in functions.keys() if 'estado' in name.lower() and 'normalize' in name.lower()]
            if len(estado_functions) > 1:
                self.log(f"⚠ ADVERTENCIA: Múltiples funciones de normalización de estado: {estado_functions}", 'WARNING')
                self.test_results['warnings'] += 1
            
            # Verificar funciones de limpieza numérica
            numeric_clean_functions = [name for name in functions.keys() if 'clean_numeric' in name.lower()]
            if len(numeric_clean_functions) > 2:
                self.log(f"⚠ ADVERTENCIA: Múltiples funciones de limpieza numérica: {numeric_clean_functions}", 'WARNING')
                self.test_results['warnings'] += 1
            
        except Exception as e:
            self.log(f"Error al analizar el módulo: {str(e)}", 'ERROR')
            return {'error': str(e)}
        
        # Registrar resultado
        test_passed = len(results['duplicate_functions']) == 0
        self._record_test('duplicate_functions', test_passed, results)
        
        if test_passed:
            self.log("\n✓✓ TEST 4 PASADO: No se encontraron funciones duplicadas", 'SUCCESS')
        else:
            self.log(f"\n✗✗ TEST 4 FALLADO: Se encontraron {len(results['duplicate_functions'])} funciones duplicadas", 'ERROR')
        
        return results
    
    def _similar_names(self, name1: str, name2: str, threshold: float = 0.9) -> bool:
        """
        Determina si dos nombres de función son sospechosamente similares.
        
        Args:
            name1: Primer nombre
            name2: Segundo nombre
            threshold: Umbral de similitud (0-1, default 0.9 para reducir falsos positivos)
            
        Returns:
            True si los nombres son similares
        """
        # Similitud de Jaccard basada en caracteres
        set1 = set(name1.lower())
        set2 = set(name2.lower())
        
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        if union == 0:
            return False
        
        similarity = intersection / union
        
        # Verificar si uno contiene al otro (pero solo si son muy similares en longitud)
        len_ratio = min(len(name1), len(name2)) / max(len(name1), len(name2))
        contains_similarity = (name1.lower() in name2.lower() or name2.lower() in name1.lower()) and len_ratio > 0.7
        
        return similarity >= threshold or contains_similarity
    
    # ==================================================================
    # Funciones de reporting
    # ==================================================================
    
    def run_all_tests(self, module_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Ejecuta todas las pruebas de calidad de datos.
        
        Args:
            module_path: Ruta opcional al módulo de transformación
            
        Returns:
            Diccionario con todos los resultados
        """
        self.log("\n" + "="*70)
        self.log("INICIANDO SUITE COMPLETA DE PRUEBAS DE CALIDAD ETL")
        self.log("="*70)
        
        all_results = {}
        
        # Test 1: Congruencia estado-avance
        all_results['test_1_consistency'] = self.test_estado_avance_consistency()
        
        # Test 2: Validación numérica de avance_obra
        all_results['test_2_numeric'] = self.test_avance_obra_numeric()
        
        # Test 3: Valores válidos en estado
        all_results['test_3_valid_values'] = self.test_estado_valid_values()
        
        # Test 4: Funciones duplicadas
        all_results['test_4_duplicates'] = self.test_duplicate_functions(module_path)
        
        # Resumen final
        self._print_summary()
        
        return all_results
    
    def _print_summary(self):
        """Imprime un resumen de todos los tests ejecutados."""
        self.log("\n" + "="*70)
        self.log("RESUMEN DE PRUEBAS DE CALIDAD")
        self.log("="*70)
        
        total = self.test_results['total_tests']
        passed = self.test_results['passed_tests']
        failed = self.test_results['failed_tests']
        warnings = self.test_results['warnings']
        
        self.log(f"\nTotal de pruebas ejecutadas: {total}")
        self.log(f"✓ Pruebas pasadas: {passed} ({(passed/total*100):.1f}%)", 'SUCCESS')
        self.log(f"✗ Pruebas falladas: {failed} ({(failed/total*100):.1f}%)", 'ERROR' if failed > 0 else 'INFO')
        self.log(f"⚠ Advertencias: {warnings}", 'WARNING' if warnings > 0 else 'INFO')
        
        if failed == 0 and warnings == 0:
            self.log("\n🎉 EXCELENTE: Todos los tests pasaron sin errores ni advertencias!", 'SUCCESS')
        elif failed == 0:
            self.log("\n✓ BUENO: Todos los tests pasaron, pero hay advertencias a revisar.", 'SUCCESS')
        else:
            self.log(f"\n⚠ ATENCIÓN: {failed} test(s) fallaron. Revisar errores críticos.", 'ERROR')
    
    def save_report(self, output_path: str = 'etl_quality_report.json'):
        """
        Guarda el reporte de pruebas en un archivo JSON.
        
        Args:
            output_path: Ruta donde guardar el reporte
        """
        try:
            # Convertir objetos no serializables a strings
            def convert_to_serializable(obj):
                if isinstance(obj, (int, float, str, bool, type(None))):
                    return obj
                elif isinstance(obj, (list, tuple)):
                    return [convert_to_serializable(item) for item in obj]
                elif isinstance(obj, dict):
                    return {key: convert_to_serializable(value) for key, value in obj.items()}
                else:
                    return str(obj)
            
            serializable_results = convert_to_serializable(self.test_results)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(serializable_results, f, indent=2, ensure_ascii=False)
            self.log(f"\n✓ Reporte guardado en: {output_path}", 'SUCCESS')
        except Exception as e:
            self.log(f"Error al guardar reporte: {str(e)}", 'ERROR')


# ==================================================================
# Función principal para ejecución del script
# ==================================================================

def main():
    """Función principal para ejecutar las pruebas desde línea de comandos."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Script de pruebas de calidad para ETL de datos',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  # Cargar datos desde CSV y ejecutar todas las pruebas
  python test_etl_data_quality.py --data output.csv
  
  # Usar DataFrame existente en código Python
  from test_etl_data_quality import ETLDataQualityTester
  tester = ETLDataQualityTester()
  tester.load_data(my_dataframe)
  tester.run_all_tests()
  
  # Ejecutar solo pruebas específicas
  tester.test_estado_avance_consistency()
  tester.test_avance_obra_numeric()
        """
    )
    
    parser.add_argument('--data', type=str, help='Ruta al archivo de datos transformados (CSV/Excel/JSON)')
    parser.add_argument('--module', type=str, help='Ruta al módulo de transformación a analizar')
    parser.add_argument('--output', type=str, default='etl_quality_report.json', 
                       help='Ruta para guardar el reporte JSON (default: etl_quality_report.json)')
    parser.add_argument('--quiet', action='store_true', help='Modo silencioso (menos output)')
    
    args = parser.parse_args()
    
    # Crear instancia del tester
    tester = ETLDataQualityTester(data_path=args.data, verbose=not args.quiet)
    
    # Cargar datos si se proporcionó ruta
    if args.data:
        if not tester.load_data():
            print("Error: No se pudieron cargar los datos. Saliendo...")
            sys.exit(1)
    else:
        print("⚠ No se proporcionó archivo de datos. Algunas pruebas no se ejecutarán.")
        print("  Usa --data <archivo> para especificar los datos a probar.")
    
    # Ejecutar todas las pruebas
    results = tester.run_all_tests(module_path=args.module)
    
    # Guardar reporte
    tester.save_report(args.output)
    
    # Retornar código de salida apropiado
    if tester.test_results['failed_tests'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
